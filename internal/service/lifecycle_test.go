package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"tailscale.com/ipn/ipnstate"

	"github.com/jtdowney/tsbridge/internal/config"
	"github.com/jtdowney/tsbridge/internal/tailscale"
	"github.com/jtdowney/tsbridge/internal/testutil"
	"github.com/jtdowney/tsbridge/internal/tsnet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func lifecycleRegistryConfig(t *testing.T, services ...config.Service) *config.Config {
	t.Helper()
	cfg := &config.Config{
		Global: config.Global{
			ShutdownTimeout: new(5 * time.Second),
		},
		Tailscale: config.Tailscale{
			AuthKey: "test-key",
		},
		Services: services,
	}
	cfg.SetDefaults()
	return cfg
}

func newLifecycleRegistry(t *testing.T, cfg *config.Config, factory tsnet.TSNetServerFactory) (*Registry, *tailscale.Server) {
	t.Helper()
	tsServer, err := tailscale.NewServerWithFactory(cfg.Tailscale, factory)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tsServer.Close() })
	return NewRegistry(cfg, tsServer), tsServer
}

func defaultMockFactory() tsnet.TSNetServerFactory {
	return func(string) tsnet.TSNetServer { return tsnet.NewMockTSNetServer() }
}

func TestRegistryRejectsMutationsAfterShutdown(t *testing.T) {
	backend := testutil.CreateTestTCPBackend(t)
	svcCfg := config.Service{Name: "svc1", BackendAddr: backend, TLSMode: "off"}
	cfg := lifecycleRegistryConfig(t, svcCfg)

	registry, tsServer := newLifecycleRegistry(t, cfg, defaultMockFactory())
	require.NoError(t, registry.StartServices())
	_, exists := registry.GetService("svc1")
	require.True(t, exists)

	require.NoError(t, registry.Shutdown(context.Background()))

	t.Run("AddService rejected", func(t *testing.T) {
		err := registry.AddService(config.Service{Name: "svc-new", BackendAddr: backend, TLSMode: "off"})
		assert.ErrorIs(t, err, ErrRegistryClosed)
		assert.Nil(t, tsServer.GetServiceServer("svc-new"),
			"rejected add must not create a tsnet node")
	})

	t.Run("UpdateService rejected", func(t *testing.T) {
		err := registry.UpdateService("svc1", config.Service{Name: "svc1", BackendAddr: backend, TLSMode: "off"})
		assert.ErrorIs(t, err, ErrRegistryClosed)
	})

	t.Run("StartServices rejected", func(t *testing.T) {
		assert.ErrorIs(t, registry.StartServices(), ErrRegistryClosed)
	})

	t.Run("RemoveService rejected", func(t *testing.T) {
		assert.ErrorIs(t, registry.RemoveService("svc1"), ErrRegistryClosed)
	})
}

func TestRegistryShutdownConcurrentCallersShareResult(t *testing.T) {
	cfg := lifecycleRegistryConfig(t)
	registry, _ := newLifecycleRegistry(t, cfg, defaultMockFactory())

	const callers = 8
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make([]error, callers)
	for i := range callers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			errs[idx] = registry.Shutdown(context.Background())
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		assert.NoError(t, err, "caller %d must observe the shared nil result", i)
	}
}

// TestAddServiceAbandonsCandidateWhenShutdownSignaled covers the race where
// service creation succeeds but shutdown is signalled before the candidate is
// published: it must be torn down instead of registered.
func TestAddServiceAbandonsCandidateWhenShutdownSignaled(t *testing.T) {
	backend := testutil.CreateTestTCPBackend(t)

	upEntered := make(chan struct{})
	releaseUp := make(chan struct{})
	factory := func(string) tsnet.TSNetServer {
		mock := tsnet.NewMockTSNetServer()
		mock.UpFunc = func(ctx context.Context) (*ipnstate.Status, error) {
			close(upEntered)
			select {
			case <-releaseUp:
				return &ipnstate.Status{}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return mock
	}

	cfg := lifecycleRegistryConfig(t)
	registry, tsServer := newLifecycleRegistry(t, cfg, factory)

	addErr := make(chan error, 1)
	go func() {
		addErr <- registry.AddService(config.Service{
			Name:        "candidate",
			BackendAddr: backend,
			TLSMode:     "off",
		})
	}()

	select {
	case <-upEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("service creation never entered Up")
	}

	// Signal shutdown while creation is still in flight, then let it succeed.
	registry.SignalShutdown()
	close(releaseUp)

	select {
	case err := <-addErr:
		assert.ErrorIs(t, err, ErrRegistryClosed)
	case <-time.After(2 * time.Second):
		t.Fatal("AddService did not return")
	}

	if _, ok := registry.GetService("candidate"); ok {
		t.Fatal("candidate service must not be registered after shutdown signal")
	}
	assert.Nil(t, tsServer.GetServiceServer("candidate"),
		"candidate tsnet node must be closed")
}

// TestAddServiceCreationAbortedByClose covers the race where creation is
// blocked in tsnet startup and the tailscale server is closed to abort it.
func TestAddServiceCreationAbortedByClose(t *testing.T) {
	backend := testutil.CreateTestTCPBackend(t)
	longTimeout := 30 * time.Second

	upEntered := make(chan struct{})
	factory := func(string) tsnet.TSNetServer {
		mock := tsnet.NewMockTSNetServer()
		mock.UpFunc = func(ctx context.Context) (*ipnstate.Status, error) {
			close(upEntered)
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return mock
	}

	cfg := lifecycleRegistryConfig(t)
	registry, tsServer := newLifecycleRegistry(t, cfg, factory)

	addErr := make(chan error, 1)
	go func() {
		addErr <- registry.AddService(config.Service{
			Name:           "blocked",
			BackendAddr:    backend,
			TLSMode:        "off",
			StartupTimeout: &longTimeout,
		})
	}()

	select {
	case <-upEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("service creation never entered Up")
	}

	// App force-shutdown sequence: signal, then close tsnet to cancel Up.
	registry.SignalShutdown()
	start := time.Now()
	require.NoError(t, tsServer.Close())

	select {
	case err := <-addErr:
		require.Error(t, err)
		assert.False(t, errors.Is(err, ErrRegistryClosed),
			"aborted creation reports its start failure; it must not register")
	case <-time.After(2 * time.Second):
		t.Fatal("AddService did not return after tsnet close")
	}
	assert.Less(t, time.Since(start), 2*time.Second, "shutdown must not wait for startup timeout")

	if _, ok := registry.GetService("blocked"); ok {
		t.Fatal("blocked candidate must not be registered after abort")
	}
	assert.Nil(t, tsServer.GetServiceServer("blocked"))
}

func TestSignalShutdownIsIdempotent(t *testing.T) {
	cfg := lifecycleRegistryConfig(t)
	registry, _ := newLifecycleRegistry(t, cfg, defaultMockFactory())

	assert.NotPanics(t, func() {
		registry.SignalShutdown()
		registry.SignalShutdown()
	})

	assert.True(t, registry.isClosing())
}
