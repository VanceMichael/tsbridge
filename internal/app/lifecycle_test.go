package app

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"tailscale.com/ipn/ipnstate"

	"github.com/jtdowney/tsbridge/internal/config"
	"github.com/jtdowney/tsbridge/internal/service"
	"github.com/jtdowney/tsbridge/internal/tailscale"
	"github.com/jtdowney/tsbridge/internal/testutil"
	"github.com/jtdowney/tsbridge/internal/tsnet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func lifecycleAppConfig(t *testing.T, name string) *config.Config {
	t.Helper()
	socketPath := testutil.CreateTestUnixSocket(t)
	cfg := &config.Config{
		Tailscale: config.Tailscale{
			StateDir: t.TempDir(),
			AuthKey:  "test-auth-key",
		},
		Global: config.Global{
			ShutdownTimeout: new(5 * time.Second),
		},
		Services: []config.Service{
			{Name: name, BackendAddr: "unix://" + socketPath},
		},
	}
	cfg.SetDefaults()
	return cfg
}

func startLifecycleApp(t *testing.T, cfg *config.Config, factory tsnet.TSNetServerFactory) *App {
	t.Helper()
	tsServer, err := tailscale.NewServerWithFactory(cfg.Tailscale, factory)
	require.NoError(t, err)
	application, err := NewAppWithOptions(cfg, Options{TSServer: tsServer})
	require.NoError(t, err)
	require.NoError(t, application.Start(context.Background()))
	t.Cleanup(func() { _ = application.Shutdown(context.Background()) })
	return application
}

func TestAppFastShutdownWithoutStartOrWatcher(t *testing.T) {
	cfg := lifecycleAppConfig(t, "svc")
	tsServer := testutil.CreateMockTailscaleServer(t, cfg.Tailscale)
	app, err := NewAppWithOptions(cfg, Options{TSServer: tsServer})
	require.NoError(t, err)

	start := time.Now()
	require.NoError(t, app.Shutdown(context.Background()))
	assert.Less(t, time.Since(start), time.Second, "shutdown with no watcher and no reloads must stay fast")
}

func TestAppStartRejectedAfterShutdown(t *testing.T) {
	cfg := lifecycleAppConfig(t, "svc")
	app := startLifecycleApp(t, cfg, func(string) tsnet.TSNetServer { return tsnet.NewMockTSNetServer() })

	require.NoError(t, app.Shutdown(context.Background()))
	assert.ErrorIs(t, app.Start(context.Background()), errAppStopped)
}

func TestAppReloadRejectedAfterShutdown(t *testing.T) {
	cfg := lifecycleAppConfig(t, "svc1")
	app := startLifecycleApp(t, cfg, func(string) tsnet.TSNetServer { return tsnet.NewMockTSNetServer() })
	require.NoError(t, app.Shutdown(context.Background()))

	socketPath := testutil.CreateTestUnixSocket(t)
	queued := *cfg
	queued.Services = append(queued.Services, config.Service{
		Name:        "late-service",
		BackendAddr: "unix://" + socketPath,
	})
	queued.SetDefaults()

	err := app.ReloadConfig(&queued)
	require.ErrorIs(t, err, errAppStopping)

	if _, ok := app.registry.GetService("late-service"); ok {
		t.Fatal("reload after shutdown must not register a new service")
	}
	assert.Nil(t, app.tsServer.GetServiceServer("late-service"))
}

func TestAppConcurrentShutdownSharesResult(t *testing.T) {
	cfg := lifecycleAppConfig(t, "svc1")

	closeErr := errors.New("tsnet close boom")
	factory := func(string) tsnet.TSNetServer {
		mock := tsnet.NewMockTSNetServer()
		mock.CloseFunc = func() error { return closeErr }
		return mock
	}
	app := startLifecycleApp(t, cfg, factory)

	const callers = 8
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make([]error, callers)
	for i := range callers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			// Deliberately short contexts: callers must still wait for and
			// observe the shared shutdown result instead of nil or timeout.
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			defer cancel()
			errs[idx] = app.Shutdown(ctx)
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		require.Error(t, err, "caller %d must observe the shared error", i)
		assert.ErrorIs(t, err, closeErr, "caller %d", i)
		assert.Same(t, errs[0], err, "caller %d must observe the exact shared result", i)
	}
}

func TestAppConcurrentShutdownAllNil(t *testing.T) {
	cfg := lifecycleAppConfig(t, "svc1")
	app := startLifecycleApp(t, cfg, func(string) tsnet.TSNetServer { return tsnet.NewMockTSNetServer() })

	const callers = 8
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make([]error, callers)
	for i := range callers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			errs[idx] = app.Shutdown(context.Background())
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		assert.NoError(t, err, "caller %d", i)
	}
}

// TestAppShutdownWaitsForAdmittedReload verifies a reload already in progress
// is allowed to run its compensation sequence to completion before resources
// shut down.
func TestAppShutdownWaitsForAdmittedReload(t *testing.T) {
	cfg := lifecycleAppConfig(t, "svc1")

	upEntered := make(chan struct{})
	factory := func(name string) tsnet.TSNetServer {
		mock := tsnet.NewMockTSNetServer()
		if name == "added-service" {
			mock.UpFunc = func(ctx context.Context) (*ipnstate.Status, error) {
				close(upEntered)
				select {
				case <-time.After(150 * time.Millisecond):
					return &ipnstate.Status{}, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
		}
		return mock
	}
	app := startLifecycleApp(t, cfg, factory)

	socketPath := testutil.CreateTestUnixSocket(t)
	newCfg := *cfg
	newCfg.Services = []config.Service{
		cfg.Services[0],
		{Name: "added-service", BackendAddr: "unix://" + socketPath},
	}
	newCfg.SetDefaults()

	reloadErr := make(chan error, 1)
	go func() { reloadErr <- app.ReloadConfig(&newCfg) }()

	select {
	case <-upEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("reload never entered service creation")
	}

	shutdownStart := time.Now()
	require.NoError(t, app.Shutdown(context.Background()))
	assert.GreaterOrEqual(t, time.Since(shutdownStart), 120*time.Millisecond,
		"shutdown must wait for the admitted reload to finish")

	require.NoError(t, <-reloadErr)
	// The reload published before resource shutdown, so the service exists but
	// has been stopped together with every other service.
	svc, ok := app.registry.GetService("added-service")
	require.True(t, ok)
	assert.NotNil(t, svc)
}

// TestAppShutdownAbortsReloadBlockedOnServiceCreation is the core scenario:
// an in-flight reload is stuck creating a service (tsnet Up blocked); the
// shutdown deadline expires while waiting, so shutdown aborts creation and
// guarantees the candidate is never registered and no node/listener leaks.
func TestAppShutdownAbortsReloadBlockedOnServiceCreation(t *testing.T) {
	cfg := lifecycleAppConfig(t, "svc1")

	upEntered := make(chan struct{})
	longTimeout := 30 * time.Second
	factory := func(name string) tsnet.TSNetServer {
		mock := tsnet.NewMockTSNetServer()
		if name == "blocked-service" {
			mock.UpFunc = func(ctx context.Context) (*ipnstate.Status, error) {
				close(upEntered)
				<-ctx.Done()
				return nil, ctx.Err()
			}
		}
		return mock
	}
	app := startLifecycleApp(t, cfg, factory)

	socketPath := testutil.CreateTestUnixSocket(t)
	blockedCfg := *cfg
	blockedCfg.Services = []config.Service{
		cfg.Services[0],
		{Name: "blocked-service", BackendAddr: "unix://" + socketPath, StartupTimeout: &longTimeout},
	}
	blockedCfg.SetDefaults()

	reloadDone := make(chan error, 1)
	go func() { reloadDone <- app.ReloadConfig(&blockedCfg) }()

	select {
	case <-upEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("blocked reload never entered service creation")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	shutdownStart := time.Now()
	require.NoError(t, app.Shutdown(shutdownCtx))
	elapsed := time.Since(shutdownStart)
	assert.Less(t, elapsed, 5*time.Second,
		"shutdown must abort blocked creation instead of waiting for startup timeout")

	select {
	case err := <-reloadDone:
		require.Error(t, err, "aborted reload must report failure")
	case <-time.After(2 * time.Second):
		t.Fatal("blocked reload did not unwind after shutdown")
	}

	if _, ok := app.registry.GetService("blocked-service"); ok {
		t.Fatal("blocked service candidate must not be registered after shutdown")
	}
	assert.Nil(t, app.tsServer.GetServiceServer("blocked-service"),
		"no tsnet node may survive for the blocked candidate")

	// Post-shutdown mutations stay rejected.
	assert.ErrorIs(t, app.registry.AddService(config.Service{
		Name: "another", BackendAddr: "unix://" + socketPath,
	}), service.ErrRegistryClosed)

	app.lifecycleMu.Lock()
	state := app.state
	app.lifecycleMu.Unlock()
	assert.Equal(t, stateStopped, state)
}

// TestAppWatcherConfigInFlightAtCancelNotPublished ensures configurations
// queued or arriving while shutdown starts can never be published, even if
// the watcher happens to select them from the channel.
func TestAppWatcherConfigInFlightAtCancelNotPublished(t *testing.T) {
	cfg := lifecycleAppConfig(t, "svc1")
	tsServer := testutil.CreateMockTailscaleServer(t, cfg.Tailscale)

	socketPath := testutil.CreateTestUnixSocket(t)
	lateCfg := &config.Config{
		Tailscale: cfg.Tailscale,
		Global:    cfg.Global,
		Services: []config.Service{
			{Name: "queued-service", BackendAddr: "unix://" + socketPath},
		},
	}
	lateCfg.SetDefaults()

	provider := &inflightProvider{
		name: "inflight",
		cfg:  cfg,
		late: lateCfg,
	}

	app, err := NewAppWithOptions(nil, Options{Provider: provider, TSServer: tsServer})
	require.NoError(t, err)
	require.NoError(t, app.Start(context.Background()))

	// Give the watcher a moment to be parked, then shut down. The provider
	// only offers the late config once the watch context is cancelled.
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, app.Shutdown(context.Background()))

	if _, ok := app.registry.GetService("queued-service"); ok {
		t.Fatal("a watcher config arriving at shutdown must not be published")
	}
	assert.Nil(t, tsServer.GetServiceServer("queued-service"))
	assert.Equal(t, 1, provider.closeCalls(), "provider must be closed exactly once")
}

// TestAppProviderClosedAfterWatcherExits verifies ownership ordering: the
// provider close happens only after the watcher goroutine has exited.
func TestAppProviderClosedAfterWatcherExits(t *testing.T) {
	cfg := lifecycleAppConfig(t, "svc1")
	tsServer := testutil.CreateMockTailscaleServer(t, cfg.Tailscale)
	provider := &orderedCloseProvider{
		name: "ordered",
		cfg:  cfg,
	}

	app, err := NewAppWithOptions(nil, Options{Provider: provider, TSServer: tsServer})
	require.NoError(t, err)
	require.NoError(t, app.Start(context.Background()))

	require.NoError(t, app.Shutdown(context.Background()))

	events := provider.eventLog()
	require.Len(t, events, 2)
	assert.Equal(t, "watcher-exit", events[0])
	assert.Equal(t, "provider-close", events[1])
	assert.Equal(t, 1, provider.closeCalls())
}

// inflightProvider offers a late configuration only after its watch context
// is cancelled, modelling an event arriving concurrently with shutdown.
type inflightProvider struct {
	name string
	cfg  *config.Config
	late *config.Config

	closeMu sync.Mutex
	closes  int
}

func (p *inflightProvider) Name() string { return p.name }

func (p *inflightProvider) Load(context.Context) (*config.Config, error) {
	return p.cfg, nil
}

func (p *inflightProvider) Watch(ctx context.Context) (<-chan *config.Config, error) {
	ch := make(chan *config.Config)
	go func() {
		defer close(ch)
		<-ctx.Done()
		// Best-effort delivery after cancellation: either the watcher is
		// already gone or it must reject the config at reload admission.
		select {
		case ch <- p.late:
		default:
		}
	}()
	return ch, nil
}

func (p *inflightProvider) Close() error {
	p.closeMu.Lock()
	p.closes++
	p.closeMu.Unlock()
	return nil
}

func (p *inflightProvider) closeCalls() int {
	p.closeMu.Lock()
	defer p.closeMu.Unlock()
	return p.closes
}

// orderedCloseProvider records that the watcher goroutine exits before Close.
type orderedCloseProvider struct {
	name string
	cfg  *config.Config

	mu     sync.Mutex
	events []string
	closes int
}

func (p *orderedCloseProvider) Name() string { return p.name }

func (p *orderedCloseProvider) Load(context.Context) (*config.Config, error) {
	return p.cfg, nil
}

func (p *orderedCloseProvider) Watch(ctx context.Context) (<-chan *config.Config, error) {
	ch := make(chan *config.Config)
	go func() {
		defer close(ch)
		<-ctx.Done()
		p.mu.Lock()
		p.events = append(p.events, "watcher-exit")
		p.mu.Unlock()
	}()
	return ch, nil
}

func (p *orderedCloseProvider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.events) == 0 || p.events[len(p.events)-1] != "watcher-exit" {
		p.events = append(p.events, "provider-close")
		return fmt.Errorf("provider closed before watcher exited: %v", p.events)
	}
	p.events = append(p.events, "provider-close")
	p.closes++
	return nil
}

func (p *orderedCloseProvider) eventLog() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, len(p.events))
	copy(out, p.events)
	return out
}

func (p *orderedCloseProvider) closeCalls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closes
}
