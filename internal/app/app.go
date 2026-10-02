// Package app provides the main application lifecycle management for tsbridge.
package app

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/jtdowney/tsbridge/internal/config"
	tserrors "github.com/jtdowney/tsbridge/internal/errors"
	"github.com/jtdowney/tsbridge/internal/metrics"
	"github.com/jtdowney/tsbridge/internal/service"
	"github.com/jtdowney/tsbridge/internal/tailscale"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// App encapsulates the tsbridge application lifecycle
type App struct {
	cfg             *config.Config
	provider        config.Provider
	providerName    string
	tsServer        *tailscale.Server
	registry        *service.Registry
	metricsServer   *metrics.Server
	metricsRegistry *prometheus.Registry
	startOnce       sync.Once
	stopOnce        sync.Once
	configWatcher   context.CancelFunc
	mu              sync.RWMutex
}

// Options allows customizing App creation for testing
type Options struct {
	TSServer *tailscale.Server
	Registry *service.Registry
	Provider config.Provider
}

// NewApp creates a new App instance with the given configuration
func NewApp(cfg *config.Config) (*App, error) {
	return NewAppWithOptions(cfg, Options{})
}

// NewAppWithOptions creates a new App instance with custom options
func NewAppWithOptions(cfg *config.Config, opts Options) (*App, error) {
	providerName := "file"
	if opts.Provider != nil {
		loaded, err := opts.Provider.Load(context.Background())
		if err != nil {
			return nil, tserrors.WrapConfig(err, "failed to load config from provider")
		}
		cfg = loaded
		providerName = opts.Provider.Name()
	}

	if cfg == nil {
		return nil, tserrors.NewValidationError("config cannot be nil")
	}

	if err := cfg.Validate(providerName); err != nil {
		// The error from Validate is already a validation error
		return nil, err
	}

	tsServer := opts.TSServer
	if tsServer == nil {
		slog.Debug("creating tailscale server")
		var err error
		tsServer, err = tailscale.NewServer(cfg.Tailscale)
		if err != nil {
			return nil, tserrors.WrapResource(err, "failed to create tailscale server")
		}
	}

	registry := opts.Registry
	if registry == nil {
		registry = service.NewRegistry(cfg, tsServer)
	}

	app := &App{
		cfg:          cfg,
		provider:     opts.Provider,
		providerName: providerName,
		tsServer:     tsServer,
		registry:     registry,
	}

	if cfg.Global.MetricsAddr != "" {
		if err := app.setupMetrics(); err != nil {
			// Clean up tsServer if metrics setup fails and we created it
			if opts.TSServer == nil {
				tsServer.Close()
			}
			return nil, tserrors.WrapResource(err, "failed to setup metrics")
		}
	}

	return app, nil
}

// setupMetrics initializes the metrics collector and server (server is not
// started until Start).
func (a *App) setupMetrics() error {
	slog.Debug("creating metrics collector")
	reg, err := a.ensureMetricsRegistry()
	if err != nil {
		return err
	}

	// Create metrics server (but don't start it yet)
	a.metricsServer = metrics.NewServer(a.cfg.Global.MetricsAddr, reg, metricsReadHeaderTimeout(a.cfg))

	return nil
}

// ensureMetricsRegistry creates (once) the Prometheus registry, collector and
// default runtime collectors and wires the collector into the service
// registry. It is shared by startup metrics and by a later configuration
// generation that enables metrics at runtime, so metric series are
// continuous across metrics_addr migrations.
func (a *App) ensureMetricsRegistry() (*prometheus.Registry, error) {
	if a.metricsRegistry != nil {
		return a.metricsRegistry, nil
	}

	collector := metrics.NewCollector()
	reg := prometheus.NewRegistry()

	// Register default collectors (Go runtime and process metrics)
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	if err := collector.Register(reg); err != nil {
		return nil, tserrors.WrapResource(err, "failed to register metrics")
	}

	a.registry.SetMetricsCollector(collector)
	a.metricsRegistry = reg
	return reg, nil
}

// metricsReadHeaderTimeout resolves the metrics server read-header timeout,
// applying the same default the metrics server uses when the value is unset.
func metricsReadHeaderTimeout(cfg *config.Config) time.Duration {
	if cfg.Global.MetricsReadHeaderTimeout != nil {
		return *cfg.Global.MetricsReadHeaderTimeout
	}
	return 5 * time.Second
}

// Start starts the application and all its services
func (a *App) Start(ctx context.Context) error {
	var startErr error
	a.startOnce.Do(func() {
		// Start watching for configuration changes if provider supports it
		if a.provider != nil {
			watchCtx, cancel := context.WithCancel(ctx)
			a.configWatcher = cancel

			configCh, err := a.provider.Watch(watchCtx)
			if err != nil {
				slog.Warn("failed to start config watcher", "error", err)
			} else if configCh != nil {
				go a.watchConfigChanges(watchCtx, configCh)
			}
		}

		if current := a.currentMetricsServer(); current != nil {
			slog.Debug("starting metrics server", "address", a.cfg.Global.MetricsAddr)
			if err := current.Start(ctx); err != nil {
				startErr = tserrors.WrapResource(err, "failed to start metrics server")
				return
			}
			slog.Info("metrics server listening", "address", current.Addr())
		}

		slog.Info("starting services")
		if err := a.registry.StartServices(); err != nil {
			// Check if this is a partial failure
			if startupErr, ok := tserrors.AsServiceStartupError(err); ok && !startupErr.AllFailed() {
				// Some services started successfully, log the failures but continue
				slog.Warn("some services failed to start",
					"successful", startupErr.Successful,
					"failed", startupErr.Failed,
					"total", startupErr.Total,
					"failures", startupErr.Failures)
				// Continue running with partial services
			} else {
				// All services failed or other error type
				startErr = err
				// If all services fail, shut down metrics server
				if metricsServer := a.currentMetricsServer(); metricsServer != nil {
					if shutdownErr := metricsServer.Shutdown(context.Background()); shutdownErr != nil {
						slog.Error("failed to shutdown metrics server", "error", shutdownErr)
					}
				}
				return
			}
		}
	})

	return startErr
}

// Shutdown gracefully shuts down the application
func (a *App) Shutdown(ctx context.Context) error {
	var shutdownErr error
	a.stopOnce.Do(func() {
		shutdownErr = a.performShutdown(ctx)
	})
	return shutdownErr
}

// performShutdown performs the actual shutdown sequence
func (a *App) performShutdown(ctx context.Context) error {
	var errs []error

	if a.configWatcher != nil {
		a.configWatcher()
	}

	if err := a.registry.Shutdown(ctx); err != nil {
		// The error from Shutdown is already typed
		slog.Error("failed to shutdown services", "error", err)
		errs = append(errs, err)
	}

	if metricsServer := a.currentMetricsServer(); metricsServer != nil {
		if err := metricsServer.Shutdown(ctx); err != nil {
			slog.Error("failed to shutdown metrics server", "error", err)
			errs = append(errs, tserrors.WrapInternal(err, "failed to shutdown metrics server"))
		}
	}

	if err := a.tsServer.Close(); err != nil {
		// Timeout on close is logged but does not fail shutdown
		if tserrors.IsTimeout(err) {
			slog.Warn("tailscale server close timed out but continuing shutdown", "error", err)
		} else {
			slog.Error("failed to close tailscale server", "error", err)
			errs = append(errs, tserrors.WrapResource(err, "failed to close tailscale server"))
		}
	}

	if len(errs) == 0 {
		slog.Info("shutdown complete")
		return nil
	}

	return errors.Join(errs...)
}

// watchConfigChanges monitors for configuration changes from the provider
func (a *App) watchConfigChanges(ctx context.Context, configCh <-chan *config.Config) {
	for {
		select {
		case <-ctx.Done():
			return
		case newCfg, ok := <-configCh:
			if !ok {
				return
			}

			slog.Info("configuration changed, reloading services")
			if err := a.ReloadConfig(newCfg); err != nil {
				slog.Error("failed to reload configuration", "error", err)
			}
		}
	}
}

// ReloadConfig applies a new configuration to the running process.
//
// Updates are serialized on the App lock. Three outcomes are possible:
//   - The candidate is identical to the current configuration: no-op.
//   - Only the service set changed (add/remove/update): the existing dynamic
//     service management path is used.
//   - The global section changed: the candidate is parsed and validated
//     before any runtime resource is touched, then every running service's
//     handler generation and the Prometheus listener switch over together as
//     one atomic generation. On any preparation failure the candidate is
//     discarded in full, the previous generation keeps serving, and the
//     current config is left untouched; the same candidate can be retried
//     once the fault clears.
//
// This method is exported for testing purposes.
func (a *App) ReloadConfig(newCfg *config.Config) error {
	start := time.Now()

	a.mu.Lock()
	defer a.mu.Unlock()

	oldCfg := a.cfg

	// Parse/validate the candidate completely before touching runtime resources.
	if newCfg == nil {
		err := tserrors.NewValidationError("config cannot be nil")
		a.recordConfigReload(false, time.Since(start))
		return err
	}
	if err := newCfg.Validate(a.providerName); err != nil {
		a.recordConfigReload(false, time.Since(start))
		return err
	}

	globalChanged := !config.GlobalEqual(oldCfg.Global, newCfg.Global)

	var reloadErr error
	switch {
	case globalChanged:
		reloadErr = a.applyGlobalGeneration(oldCfg, newCfg)
	case config.ServicesEqual(oldCfg.Services, newCfg.Services):
		slog.Debug("configuration unchanged, skipping reload")
	default:
		// Service-only change: add/remove/update through the dynamic path.
		reloadErr = reloadConfigWithRegistry(oldCfg, newCfg, a.registry)
	}

	a.recordConfigReload(reloadErr == nil, time.Since(start))

	if globalChanged {
		// Publish the candidate as current only after the whole generation
		// committed. A failed generation must never be remembered as current.
		if reloadErr == nil {
			a.cfg = newCfg
		}
		return reloadErr
	}

	// Dynamic path keeps its existing semantics: it tolerates per-operation
	// failures and the latest intended configuration is published either way.
	a.cfg = newCfg
	return reloadErr
}

// recordConfigReload records the reload outcome on the active collector, if any.
func (a *App) recordConfigReload(success bool, duration time.Duration) {
	if a.registry == nil {
		return
	}
	if collector := a.registry.GetMetricsCollector(); collector != nil {
		collector.RecordConfigReload(success, duration)
	}
}

// findServicesToRemove returns names of services in old config not present in new config.
func findServicesToRemove(old, new *config.Config) []string {
	newServices := make(map[string]bool)
	for _, svc := range new.Services {
		newServices[svc.Name] = true
	}

	var toRemove []string
	for _, svc := range old.Services {
		if !newServices[svc.Name] {
			toRemove = append(toRemove, svc.Name)
		}
	}
	return toRemove
}

// findServicesToAdd returns services in new config but not in old.
func findServicesToAdd(old, new *config.Config) []config.Service {
	oldServices := make(map[string]bool)
	for _, svc := range old.Services {
		oldServices[svc.Name] = true
	}

	var toAdd []config.Service
	for _, svc := range new.Services {
		if !oldServices[svc.Name] {
			toAdd = append(toAdd, svc)
		}
	}
	return toAdd
}

// findServicesToUpdate returns services present in both configs with changed configuration.
func findServicesToUpdate(old, new *config.Config) []config.Service {
	oldServices := make(map[string]config.Service)
	for _, svc := range old.Services {
		oldServices[svc.Name] = svc
	}

	var toUpdate []config.Service
	for _, newSvc := range new.Services {
		if oldSvc, exists := oldServices[newSvc.Name]; exists {
			if !config.ServiceConfigEqual(oldSvc, newSvc) {
				toUpdate = append(toUpdate, newSvc)
			}
		}
	}
	return toUpdate
}

// MetricsAddr returns the actual address the metrics server is listening on.
// Returns empty string if metrics server is not running.
func (a *App) MetricsAddr() string {
	metricsServer := a.currentMetricsServer()
	if metricsServer == nil {
		return ""
	}
	return metricsServer.Addr()
}
