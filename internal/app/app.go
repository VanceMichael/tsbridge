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
	cfg           *config.Config
	provider      config.Provider
	tsServer      *tailscale.Server
	registry      *service.Registry
	metricsServer *metrics.Server
	startOnce     sync.Once
	stopOnce      sync.Once

	// lifecycleMu guards state, activeReloads and the watcher handles. It is
	// also the locker for reloadsIdleCond.
	lifecycleMu   sync.Mutex
	state         appState
	activeReloads int
	reloadsIdle   *sync.Cond

	// configWatcher cancels the provider watch context; watcherDone is closed
	// when the watcher goroutine has fully exited. Both are nil when no
	// watcher was started.
	configWatcher context.CancelFunc
	watcherDone   chan struct{}

	// shutdownDone is closed once the single shutdown process finishes;
	// shutdownErr holds its result. Every Shutdown caller waits on the same
	// channel and returns the same error.
	shutdownDone chan struct{}
	shutdownErr  error

	mu sync.RWMutex
}

// appState models the App lifecycle:
//
//	running  → normal operation; configuration reloads are admitted.
//	stopping → shutdown has begun; new reloads are rejected, the watcher is
//	           cancelled and admitted reloads are awaited (or aborted).
//	stopped  → all resources have been shut down; terminal state.
type appState int

const (
	stateRunning appState = iota
	stateStopping
	stateStopped
)

// Reload admission errors. They are wrapped in the typed error package for
// callers that classify failures.
var (
	errAppStopping = errors.New("application is shutting down")
	errAppStopped  = errors.New("application has been shut down")
)

// providerCloser is implemented by providers that own closable resources
// (for example the Docker provider's client, ticker and event stream).
type providerCloser interface {
	Close() error
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
		cfg:      cfg,
		provider: opts.Provider,
		tsServer: tsServer,
		registry: registry,
	}
	app.reloadsIdle = sync.NewCond(&app.lifecycleMu)

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
	// Reject Start once shutdown has begun/finished. This lives outside
	// startOnce so callers after the first (completed) Start observe the
	// terminal state instead of Once's silent no-op.
	a.lifecycleMu.Lock()
	if a.state != stateRunning {
		a.lifecycleMu.Unlock()
		return errAppStopped
	}
	a.lifecycleMu.Unlock()

	var startErr error
	a.startOnce.Do(func() {
		// Start watching for configuration changes if provider supports it
		if a.provider != nil {
			watchCtx, cancel := context.WithCancel(ctx)

			a.lifecycleMu.Lock()
			a.configWatcher = cancel
			a.lifecycleMu.Unlock()

			configCh, err := a.provider.Watch(watchCtx)
			if err != nil {
				slog.Warn("failed to start config watcher", "error", err)
			} else if configCh != nil {
				done := make(chan struct{})
				a.lifecycleMu.Lock()
				a.watcherDone = done
				a.lifecycleMu.Unlock()

				go func() {
					defer close(done)
					a.watchConfigChanges(watchCtx, configCh)
					// The provider contract is that configCh is closed when the
					// watch context is cancelled. Drain late messages without
					// publishing them and only signal watcher exit once the
					// provider-side event loop is gone, so provider resources
					// (event stream, client) are closed after the watcher,
					// never while it is running.
					for range configCh {
						slog.Debug("dropping configuration received during shutdown")
					}
				}()
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

// Shutdown gracefully shuts down the application.
//
// Concurrent or repeated calls converge onto a single shutdown process: they
// all wait for it to finish and return the same result. The context of the
// first caller bounds resource shutdown; later contexts cannot cut another
// caller's wait short, which is what guarantees every caller observes the
// same final error.
func (a *App) Shutdown(ctx context.Context) error {
	a.stopOnce.Do(func() {
		a.shutdownDone = make(chan struct{})
		go func() {
			a.shutdownErr = a.performShutdown(ctx)
			close(a.shutdownDone)
		}()
	})
	<-a.shutdownDone
	return a.shutdownErr
}

// performShutdown performs the actual shutdown sequence:
//  1. enter stopping state and reject new reload admissions,
//  2. cancel the config watcher and wait for its goroutine to exit so queued
//     configs can no longer be picked up,
//  3. close provider resources (Docker ticker/event stream/client) once,
//  4. wait for reloads admitted before shutdown to finish; if the deadline
//     expires, abort blocked service creation and force convergence,
//  5. stop the services, metrics server and tailscale server with the
//     existing best-effort error aggregation.
func (a *App) performShutdown(ctx context.Context) error {
	slog.Info("shutting down")
	var errs []error

	// 1. Close reload admission.
	a.lifecycleMu.Lock()
	a.state = stateStopping
	watcherCancel := a.configWatcher
	watcherDone := a.watcherDone
	a.lifecycleMu.Unlock()

	// 2. Cancel the watcher and wait for its goroutine (and any reload it has
	//    already entered) to stop producing work.
	if watcherCancel != nil {
		watcherCancel()
	}
	if watcherDone != nil {
		select {
		case <-watcherDone:
		case <-ctx.Done():
			slog.Warn("timed out waiting for config watcher to exit; continuing shutdown",
				"error", ctx.Err())
		}
	}

	// 3. Release provider-owned resources exactly once, after the watcher has
	//    exited (or after the timeout fallback above).
	if closer, ok := a.provider.(providerCloser); ok {
		if err := closer.Close(); err != nil {
			slog.Error("failed to close config provider", "error", err)
			errs = append(errs, tserrors.WrapResource(err, "failed to close config provider"))
		}
	}

	// 4. Wait for admitted reloads to complete their compensation sequence.
	//    If the shutdown deadline expires, signal the registry and abort any
	//    tsnet startup blocked in service creation so the candidate can never
	//    be registered and registry shutdown is not deadlocked.
	tailscaleClosedEarly := false
	if !a.waitForReloads(ctx) {
		slog.Warn("timed out waiting for in-flight configuration reload; aborting service creation")
		a.registry.SignalShutdown()
		if err := a.tsServer.Close(); err != nil {
			if tserrors.IsTimeout(err) {
				slog.Warn("tailscale server close timed out while aborting reload", "error", err)
			} else {
				slog.Error("failed to close tailscale server while aborting reload", "error", err)
				errs = append(errs, tserrors.WrapResource(err, "failed to close tailscale server"))
			}
		}
		tailscaleClosedEarly = true
	}

	// 5a. Stop services (registry rejects all mutations once this begins and
	//     waits for any in-flight operation to release its lock).
	if err := a.registry.Shutdown(ctx); err != nil {
		// The error from Shutdown is already typed
		slog.Error("failed to shutdown services", "error", err)
		errs = append(errs, err)
	}

	// 5b. Stop the metrics server.
	if a.metricsServer != nil {
		if err := a.metricsServer.Shutdown(ctx); err != nil {
			slog.Error("failed to shutdown metrics server", "error", err)
			errs = append(errs, tserrors.WrapInternal(err, "failed to shutdown metrics server"))
		}
	}

	// 5c. Close the tailscale server (skipped if the reload-abort path closed
	//     it already; Close itself is idempotent either way).
	if !tailscaleClosedEarly {
		if err := a.tsServer.Close(); err != nil {
			// Timeout on close is logged but does not fail shutdown
			if tserrors.IsTimeout(err) {
				slog.Warn("tailscale server close timed out but continuing shutdown", "error", err)
			} else {
				slog.Error("failed to close tailscale server", "error", err)
				errs = append(errs, tserrors.WrapResource(err, "failed to close tailscale server"))
			}
		}
	}

	a.lifecycleMu.Lock()
	a.state = stateStopped
	a.lifecycleMu.Unlock()

	if len(errs) == 0 {
		slog.Info("shutdown complete")
		return nil
	}

	return errors.Join(errs...)
}

// beginReload admits a configuration reload while the App is running. It
// returns false once shutdown has started or completed.
func (a *App) beginReload() bool {
	a.lifecycleMu.Lock()
	defer a.lifecycleMu.Unlock()
	if a.state != stateRunning {
		return false
	}
	a.activeReloads++
	return true
}

// endReload marks an admitted reload as finished and wakes shutdown waiters
// once no reload is in flight.
func (a *App) endReload() {
	a.lifecycleMu.Lock()
	a.activeReloads--
	if a.activeReloads == 0 {
		a.reloadsIdle.Broadcast()
	}
	a.lifecycleMu.Unlock()
}

// waitForReloads blocks until every admitted reload finishes. It returns
// false if ctx expires before that happens. The zero-reload case returns
// immediately so empty/idle apps keep their fast shutdown path.
func (a *App) waitForReloads(ctx context.Context) bool {
	a.lifecycleMu.Lock()
	if a.activeReloads == 0 {
		a.lifecycleMu.Unlock()
		return true
	}
	a.lifecycleMu.Unlock()

	idle := make(chan struct{})
	go func() {
		a.lifecycleMu.Lock()
		for a.activeReloads > 0 {
			a.reloadsIdle.Wait()
		}
		a.lifecycleMu.Unlock()
		close(idle)
	}()

	select {
	case <-idle:
		return true
	case <-ctx.Done():
		// The waiter goroutine exits once the aborted reloads unwind.
		return false
	}
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
				if errors.Is(err, errAppStopping) {
					// Shutdown started before this config could be published;
					// drop it and let the watcher exit.
					slog.Debug("ignoring configuration change during shutdown")
					return
				}
				slog.Error("failed to reload configuration", "error", err)
			}
		}
	}
}

// ReloadConfig reloads the configuration and restarts affected services.
//
// Reloads are serialized and only admitted while the App is running. Once
// shutdown starts, ReloadConfig returns errAppStopping without touching the
// registry, so queued or late-arriving configurations are never published.
// This method is exported for testing purposes.
func (a *App) ReloadConfig(newCfg *config.Config) error {
	if !a.beginReload() {
		return errAppStopping
	}
	defer a.endReload()

	start := time.Now()

	a.mu.Lock()
	defer a.mu.Unlock()

	oldCfg := a.cfg
	err := reloadConfigWithRegistry(oldCfg, newCfg, a.registry)

	if a.registry != nil {
		if collector := a.registry.GetMetricsCollector(); collector != nil {
			collector.RecordConfigReload(err == nil, time.Since(start))
		}
	}

	// Always update the config, even if some operations failed
	// This ensures we're working with the latest intended configuration
	a.cfg = newCfg

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
