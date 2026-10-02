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
	tsnetpkg "github.com/jtdowney/tsbridge/internal/tsnet"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// initialGeneration numbers the tailscale Server/Registry generation created
// when the App starts; every accepted Tailscale configuration rotation
// increments it.
const initialGeneration = 1

// App encapsulates the tsbridge application lifecycle
type App struct {
	cfg           *config.Config
	provider      config.Provider
	tsServer      *tailscale.Server
	registry      *service.Registry
	metricsServer *metrics.Server
	// generation identifies the currently owned tailscale Server generation.
	generation uint64
	// tsNetFactory builds tsnet-backed servers; nil uses the real tsnet
	// implementation.
	tsNetFactory tsnetpkg.TSNetServerFactory
	startOnce    sync.Once
	stopOnce     sync.Once
	configWatcher context.CancelFunc
	mu            sync.RWMutex
	// stopped is set under mu when Shutdown begins; afterwards no reload may
	// create new generations or resources.
	stopped bool
}

// Options allows customizing App creation for testing
type Options struct {
	TSServer *tailscale.Server
	Registry *service.Registry
	Provider config.Provider
	// TSNetFactory overrides the tsnet server factory used for the initial
	// generation and every generation created by a Tailscale config rotation.
	TSNetFactory tsnetpkg.TSNetServerFactory
}

// newTailscaleServer creates a tailscale Server for a generation, honoring an
// injected tsnet factory.
func newTailscaleServer(cfg config.Tailscale, factory tsnetpkg.TSNetServerFactory) (*tailscale.Server, error) {
	if factory != nil {
		return tailscale.NewServerWithFactory(cfg, factory)
	}
	return tailscale.NewServer(cfg)
}

// NewApp creates a new App instance with the given configuration
func NewApp(cfg *config.Config) (*App, error) {
	return NewAppWithOptions(cfg, Options{})
}

// NewAppWithOptions creates a new App instance with custom options
func NewAppWithOptions(cfg *config.Config, opts Options) (*App, error) {
	if opts.Provider != nil {
		loaded, err := opts.Provider.Load(context.Background())
		if err != nil {
			return nil, tserrors.WrapConfig(err, "failed to load config from provider")
		}
		cfg = loaded
	}

	if cfg == nil {
		return nil, tserrors.NewValidationError("config cannot be nil")
	}

	providerName := ""
	if opts.Provider != nil {
		providerName = opts.Provider.Name()
	}
	if err := cfg.Validate(providerName); err != nil {
		// The error from Validate is already a validation error
		return nil, err
	}

	tsServer := opts.TSServer
	if tsServer == nil {
		slog.Debug("creating tailscale server")
		var err error
		tsServer, err = newTailscaleServer(cfg.Tailscale, opts.TSNetFactory)
		if err != nil {
			return nil, tserrors.WrapResource(err, "failed to create tailscale server")
		}
	}

	registry := opts.Registry
	if registry == nil {
		registry = service.NewRegistry(cfg, tsServer)
	}

	app := &App{
		cfg:           cfg,
		provider:      opts.Provider,
		tsServer:      tsServer,
		registry:      registry,
		generation:    initialGeneration,
		tsNetFactory:  opts.TSNetFactory,
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

// setupMetrics initializes the metrics server and collector
func (a *App) setupMetrics() error {
	slog.Debug("creating metrics collector")
	collector := metrics.NewCollector()
	reg := prometheus.NewRegistry()

	// Register default collectors (Go runtime and process metrics)
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	if err := collector.Register(reg); err != nil {
		return tserrors.WrapResource(err, "failed to register metrics")
	}

	a.registry.SetMetricsCollector(collector)

	// Create metrics server (but don't start it yet)
	var metricsTimeout time.Duration
	if a.cfg.Global.MetricsReadHeaderTimeout != nil {
		metricsTimeout = *a.cfg.Global.MetricsReadHeaderTimeout
	}
	a.metricsServer = metrics.NewServer(a.cfg.Global.MetricsAddr, reg, metricsTimeout)

	return nil
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

		if a.metricsServer != nil {
			slog.Debug("starting metrics server", "address", a.cfg.Global.MetricsAddr)
			if err := a.metricsServer.Start(ctx); err != nil {
				startErr = tserrors.WrapResource(err, "failed to start metrics server")
				return
			}
			slog.Info("metrics server listening", "address", a.metricsServer.Addr())
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
				if a.metricsServer != nil {
					if shutdownErr := a.metricsServer.Shutdown(context.Background()); shutdownErr != nil {
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
	// Block new reloads/generations and wait for any in-flight rotation to
	// finish before tearing resources down.
	a.mu.Lock()
	a.stopped = true
	a.mu.Unlock()

	var errs []error

	if a.configWatcher != nil {
		a.configWatcher()
	}

	if err := a.registry.Shutdown(ctx); err != nil {
		// The error from Shutdown is already typed
		slog.Error("failed to shutdown services", "error", err)
		errs = append(errs, err)
	}

	if a.metricsServer != nil {
		if err := a.metricsServer.Shutdown(ctx); err != nil {
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

// ReloadConfig reloads the configuration and restarts affected services
// This method is exported for testing purposes
//
// When the Tailscale control-plane configuration changes, a new tailscale
// Server generation is coordinated atomically: the candidate is validated
// before any online service is interrupted, services are migrated one by one,
// and on any tsnet Up/authentication/Listen failure every candidate node is
// cleaned up and all previously online services are restored on the outgoing
// generation. The committed config only advances when the rotation succeeds,
// so a later event retries the rejected candidate.
func (a *App) ReloadConfig(newCfg *config.Config) error {
	start := time.Now()

	a.mu.Lock()
	defer a.mu.Unlock()

	if a.stopped {
		if a.registry != nil {
			if collector := a.registry.GetMetricsCollector(); collector != nil {
				collector.RecordConfigReload(false, time.Since(start))
			}
		}
		return errAppStopped
	}

	oldCfg := a.cfg
	tailscaleRotated := !config.TailscaleEqual(oldCfg.Tailscale, newCfg.Tailscale)

	var err error
	if tailscaleRotated {
		err = a.rotateTailscaleGeneration(context.Background(), oldCfg, newCfg)
	} else {
		err = reloadConfigWithRegistry(oldCfg, newCfg, a.registry)
	}

	if a.registry != nil {
		if collector := a.registry.GetMetricsCollector(); collector != nil {
			collector.RecordConfigReload(err == nil, time.Since(start))
		}
	}

	switch {
	case err == nil:
		// Commit: registry, services and tsnet nodes all belong to the same
		// (possibly new) generation.
		a.cfg = newCfg
	case tailscaleRotated:
		// The rotation rolled back completely; keep the previous config so
		// the unchanged old generation stays authoritative and the next
		// event can retry the candidate after the external fault is fixed.
		slog.Warn("keeping previous tailscale generation; configuration not advanced",
			"generation", a.generation,
			"error", err)
	default:
		// Legacy semantics for service-only reloads: always work from the
		// latest intended configuration even when individual operations fail.
		a.cfg = newCfg
	}

	return err
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
	if a.metricsServer == nil {
		return ""
	}
	return a.metricsServer.Addr()
}
