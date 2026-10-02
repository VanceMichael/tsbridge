package tailscale

import (
	"context"
	"errors"
	"testing"
	"time"

	"tailscale.com/ipn/ipnstate"

	"github.com/jtdowney/tsbridge/internal/config"
	tsnet "github.com/jtdowney/tsbridge/internal/tsnet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newLifecycleTestServer(t *testing.T, factory tsnet.TSNetServerFactory) *Server {
	t.Helper()
	server, err := NewServerWithFactory(config.Tailscale{AuthKey: "test-key"}, factory)
	require.NoError(t, err)
	t.Cleanup(func() { _ = server.Close() })
	return server
}

func TestListenRejectedAfterClose(t *testing.T) {
	factoryCreated := 0
	server := newLifecycleTestServer(t, func(serviceName string) tsnet.TSNetServer {
		factoryCreated++
		return tsnet.NewMockTSNetServer()
	})

	require.NoError(t, server.Close())

	svc := config.Service{Name: "late-service", BackendAddr: "127.0.0.1:8080", TLSMode: "off"}
	listener, err := server.Listen(svc, config.TLSModeOff, false)
	require.ErrorIs(t, err, ErrServerClosed)
	assert.Nil(t, listener)

	// No tsnet node may be created for a rejected listener.
	assert.Equal(t, 0, factoryCreated, "no tsnet server should be fabricated after Close")
	assert.Nil(t, server.GetServiceServer("late-service"))
}

func TestCloseIdempotentAndAggregates(t *testing.T) {
	mockServer := tsnet.NewMockTSNetServer()
	closeCalls := 0
	mockServer.CloseFunc = func() error {
		closeCalls++
		return errors.New("boom")
	}

	factory := func(string) tsnet.TSNetServer { return tsnet.NewMockTSNetServer() }
	server := newLifecycleTestServer(t, factory)
	server.mu.Lock()
	server.serviceServers["svc"] = mockServer
	server.mu.Unlock()

	err1 := server.Close()
	require.Error(t, err1)
	assert.Contains(t, err1.Error(), "boom")

	// Concurrent/repeated closes must not close the node a second time.
	err2 := server.Close()
	assert.NoError(t, err2, "repeated Close should be a no-op returning nil")
	assert.Equal(t, 1, closeCalls, "tsnet node must be closed exactly once")

	_, err := server.Listen(config.Service{Name: "x", TLSMode: "off"}, config.TLSModeOff, false)
	assert.ErrorIs(t, err, ErrServerClosed)
}

func TestCloseAbortsBlockedStartup(t *testing.T) {
	upStarted := make(chan struct{})
	released := make(chan struct{})

	mockServer := tsnet.NewMockTSNetServer()
	mockServer.UpFunc = func(ctx context.Context) (*ipnstate.Status, error) {
		close(upStarted)
		<-ctx.Done()
		close(released)
		return nil, ctx.Err()
	}

	factory := func(string) tsnet.TSNetServer { return mockServer }
	server := newLifecycleTestServer(t, factory)

	longTimeout := 30 * time.Second
	svc := config.Service{
		Name:           "blocked-service",
		BackendAddr:    "127.0.0.1:8080",
		TLSMode:        "off",
		StartupTimeout: &longTimeout,
	}

	listenErr := make(chan error, 1)
	go func() {
		_, err := server.Listen(svc, config.TLSModeOff, false)
		listenErr <- err
	}()

	select {
	case <-upStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("blocked Up never started")
	}

	// Closing must abort the pending startup promptly rather than waiting 30s.
	require.NoError(t, server.Close())

	select {
	case <-released:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not cancel blocked tsnet startup")
	}

	select {
	case err := <-listenErr:
		require.Error(t, err)
		assert.False(t, errors.Is(err, ErrServerClosed), "in-flight Listen reports its startup failure, rejection applies to later calls")
	case <-time.After(2 * time.Second):
		t.Fatal("in-flight Listen never returned after Close")
	}

	// The failed startup must leave no registered node.
	assert.Nil(t, server.GetServiceServer("blocked-service"))
}
