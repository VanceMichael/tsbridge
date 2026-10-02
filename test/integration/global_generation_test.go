//go:build integration

package integration

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/jtdowney/tsbridge/internal/app"
	"github.com/jtdowney/tsbridge/internal/config"
	"github.com/jtdowney/tsbridge/internal/testutil"
	"github.com/jtdowney/tsbridge/test/integration/helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// reserveLoopbackAddr binds a loopback port and immediately releases it,
// returning the address for use as a fixed metrics listen address.
func reserveLoopbackAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return addr
}

func expectMetricsServing(t *testing.T, addr string) {
	t.Helper()
	require.Eventually(t, func() bool {
		resp, err := http.Get("http://" + addr + "/metrics")
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 2*time.Second, 25*time.Millisecond)
}

func expectMetricsDown(t *testing.T, addr string) {
	t.Helper()
	require.Eventually(t, func() bool {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err != nil {
			return true
		}
		_ = conn.Close()
		return false
	}, 2*time.Second, 25*time.Millisecond)
}

func startTestApp(t *testing.T, cfg *config.Config) *app.App {
	t.Helper()
	tsServer := testutil.CreateMockTailscaleServer(t, cfg.Tailscale)
	testApp, err := app.NewAppWithOptions(cfg, app.Options{TSServer: tsServer})
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, testApp.Start(ctx))
	waitForServicesReady(t)

	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = testApp.Shutdown(shutdownCtx)
	})
	return testApp
}

// TestGlobalGenerationMigratesMetricsEndpoint verifies a global generation
// moves the Prometheus listener as one unit: the new endpoint serves and the
// old one is retired, with reload succeeding and graceful shutdown still
// working afterwards.
func TestGlobalGenerationMigratesMetricsEndpoint(t *testing.T) {
	addrA := reserveLoopbackAddr(t)
	addrB := reserveLoopbackAddr(t)

	backend := helpers.CreateTestBackend(t)
	cfg := helpers.CreateTestConfig(t, "svc1", backend.Listener.Addr().String())
	cfg.Global.MetricsAddr = addrA
	testApp := startTestApp(t, cfg)
	expectMetricsServing(t, addrA)

	newCfg := helpers.CreateTestConfig(t, "svc1", backend.Listener.Addr().String())
	newCfg.Global.MetricsAddr = addrB
	newCfg.Global.TrustedProxies = []string{"10.0.0.0/8"} // global policy change in the same generation
	require.NoError(t, testApp.ReloadConfig(newCfg))

	assert.Equal(t, addrB, testApp.MetricsAddr())
	expectMetricsServing(t, addrB)
	expectMetricsDown(t, addrA)
}

// TestGlobalGenerationMetricsBindFailureKeepsOldGeneration verifies that a
// failed metrics bind leaves the old scrape endpoint in place, the candidate
// is not published, and the identical configuration succeeds once the fault
// clears.
func TestGlobalGenerationMetricsBindFailureKeepsOldGeneration(t *testing.T) {
	addrA := reserveLoopbackAddr(t)

	backend := helpers.CreateTestBackend(t)
	cfg := helpers.CreateTestConfig(t, "svc1", backend.Listener.Addr().String())
	cfg.Global.MetricsAddr = addrA
	testApp := startTestApp(t, cfg)
	expectMetricsServing(t, addrA)

	// Occupy the target metrics port.
	occupier, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	target := occupier.Addr().String()

	candidate := helpers.CreateTestConfig(t, "svc1", backend.Listener.Addr().String())
	candidate.Global.MetricsAddr = target
	candidate.Global.TrustedProxies = []string{"172.16.0.0/12"}

	require.Error(t, testApp.ReloadConfig(candidate))
	assert.Equal(t, addrA, testApp.MetricsAddr(), "failed generation must not publish the candidate endpoint")
	expectMetricsServing(t, addrA)

	// Free the conflicting port and retry the exact same configuration.
	require.NoError(t, occupier.Close())
	expectMetricsDown(t, target) // occupier's listener closed

	require.NoError(t, testApp.ReloadConfig(candidate))
	assert.Equal(t, target, testApp.MetricsAddr())
	expectMetricsServing(t, target)
	expectMetricsDown(t, addrA)
}

// TestGlobalGenerationNoOp verifies that an identical candidate does not
// disturb the running metrics endpoint.
func TestGlobalGenerationNoOp(t *testing.T) {
	addr := reserveLoopbackAddr(t)

	backend := helpers.CreateTestBackend(t)
	cfg := helpers.CreateTestConfig(t, "svc1", backend.Listener.Addr().String())
	cfg.Global.MetricsAddr = addr
	testApp := startTestApp(t, cfg)
	expectMetricsServing(t, addr)

	require.NoError(t, testApp.ReloadConfig(cfg))
	assert.Equal(t, addr, testApp.MetricsAddr())
	expectMetricsServing(t, addr)
}

// TestGlobalGenerationMixedWithServiceChanges verifies a global policy change
// applied together with service-set changes does not break the dynamic
// management path: the new service comes up while the generation commits.
func TestGlobalGenerationMixedWithServiceChanges(t *testing.T) {
	addrA := reserveLoopbackAddr(t)
	addrB := reserveLoopbackAddr(t)

	backend1 := helpers.CreateTestBackend(t)
	backend2 := helpers.CreateTestBackend(t)

	cfg := helpers.CreateTestConfig(t, "svc1", backend1.Listener.Addr().String())
	cfg.Global.MetricsAddr = addrA
	testApp := startTestApp(t, cfg)
	expectMetricsServing(t, addrA)

	mixed := helpers.CreateMultiServiceConfig(t, map[string]string{
		"svc1": backend1.Listener.Addr().String(),
		"svc2": backend2.Listener.Addr().String(),
	})
	mixed.Global.MetricsAddr = addrB
	mixed.Global.TrustedProxies = []string{"10.1.0.0/16"}

	require.NoError(t, testApp.ReloadConfig(mixed))
	waitForServicesReady(t)

	assert.Equal(t, addrB, testApp.MetricsAddr())
	expectMetricsServing(t, addrB)
	expectMetricsDown(t, addrA)
}
