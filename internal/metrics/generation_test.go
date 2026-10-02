package metrics

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServerListenDoesNotServeYet(t *testing.T) {
	server := NewServer("127.0.0.1:0", prometheus.NewRegistry(), 5*time.Second)
	require.NoError(t, server.Listen())
	t.Cleanup(func() { _ = server.Shutdown(context.Background()) })

	addr := server.Addr()
	require.NotEmpty(t, addr)

	// The listener is bound (a second bind must fail) ...
	occupier, err := net.Listen("tcp", addr)
	require.Error(t, err, "Listen must bind the port before Serve")
	_ = occupier

	// ... but it must not accept connections yet. A connection attempt should
	// queue at most in the backlog; an actual request cannot complete.
	client := http.Client{Timeout: 200 * time.Millisecond}
	resp, err := client.Get("http://" + addr + "/metrics")
	if err == nil {
		resp.Body.Close()
		t.Fatal("candidate metrics endpoint answered before Serve was called")
	}
}

func TestServerListenThenServe(t *testing.T) {
	server := NewServer("127.0.0.1:0", prometheus.NewRegistry(), 5*time.Second)
	require.NoError(t, server.Listen())
	server.Serve()
	t.Cleanup(func() { _ = server.Shutdown(context.Background()) })

	assert.Eventually(t, func() bool {
		resp, err := http.Get("http://" + server.Addr() + "/metrics")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 2*time.Second, 20*time.Millisecond)
}

func TestServerDiscardReleasesBoundPort(t *testing.T) {
	reg := prometheus.NewRegistry()
	server := NewServer("127.0.0.1:0", reg, 5*time.Second)
	require.NoError(t, server.Listen())
	addr := server.Addr()
	require.NotEmpty(t, addr)

	// Abandon the candidate before it enters service.
	server.Discard()

	// Shutdown after Discard must be a safe no-op.
	assert.NoError(t, server.Shutdown(context.Background()))

	// The port is released and the same address can bind again: the failed
	// candidate left no listener behind, enabling a successful retry.
	assert.Eventually(t, func() bool {
		retry := NewServer(addr, reg, 5*time.Second)
		if err := retry.Listen(); err != nil {
			return false
		}
		retry.Discard()
		return true
	}, 2*time.Second, 20*time.Millisecond)

	// A discarded endpoint never served.
	resp, err := http.Get("http://" + addr + "/metrics")
	if err == nil {
		resp.Body.Close()
		t.Fatal("discarded metrics server must not serve")
	}
}

func TestServerListenConflict(t *testing.T) {
	occupier, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = occupier.Close() })

	server := NewServer(occupier.Addr().String(), prometheus.NewRegistry(), 5*time.Second)
	err = server.Listen()
	require.Error(t, err)

	// The failed candidate holds no resources and the old occupier is unaffected.
	server.Discard()
	conn, dialErr := net.DialTimeout("tcp", occupier.Addr().String(), time.Second)
	require.NoError(t, dialErr)
	_ = conn.Close()
}
