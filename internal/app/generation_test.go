package app

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jtdowney/tsbridge/internal/config"
	"github.com/jtdowney/tsbridge/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// freeTCPPort reserves a loopback TCP port, releases the listener and
// returns the address. It lets tests use distinct fixed metrics addresses so
// a migration is not deduplicated as ":0" == ":0".
func freeTCPPort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return addr
}

func generationConfig(t *testing.T, g config.Global, services ...config.Service) *config.Config {
	t.Helper()
	cfg := &config.Config{
		Tailscale: config.Tailscale{
			AuthKey:  config.RedactedString("test-auth-key"),
			StateDir: t.TempDir(),
		},
		Global:   g,
		Services: services,
	}
	cfg.SetDefaults()
	cfg.Normalize()
	return cfg
}

func generationService(name, backendAddr string) config.Service {
	return config.Service{
		Name:         name,
		BackendAddr:  backendAddr,
		TLSMode:      config.TLSModeOff,
		WhoisEnabled: new(false),
	}
}

func startGenerationApp(t *testing.T, cfg *config.Config) *App {
	t.Helper()
	tsServer := testutil.CreateMockTailscaleServer(t, cfg.Tailscale)
	a, err := NewAppWithOptions(cfg, Options{TSServer: tsServer})
	require.NoError(t, err)
	require.NoError(t, a.Start(context.Background()))
	t.Cleanup(func() {
		_ = a.Shutdown(context.Background())
	})
	return a
}

// headerRecordingBackend records the latest X-Real-IP it received.
func headerRecordingBackend(t *testing.T) (*httptest.Server, func() string) {
	t.Helper()
	var mu sync.Mutex
	xRealIP := ""
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		xRealIP = r.Header.Get("X-Real-IP")
		mu.Unlock()
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	return server, func() string {
		mu.Lock()
		defer mu.Unlock()
		return xRealIP
	}
}

// serveThrough invokes the named service's in-process handler chain.
func serveThrough(t *testing.T, a *App, serviceName string, req *http.Request) *httptest.ResponseRecorder {
	t.Helper()
	svc, ok := a.registry.GetService(serviceName)
	require.True(t, ok)
	rec := httptest.NewRecorder()
	svc.Handler().ServeHTTP(rec, req)
	return rec
}

func proxiedRequest(method string, body io.Reader) *http.Request {
	req := httptest.NewRequest(method, "/", body)
	req.RemoteAddr = "10.0.0.1:51234"
	req.Header.Set("X-Forwarded-For", "203.0.113.9")
	return req
}

func TestGlobalGeneration_TrustedProxiesAndBodyLimitSwitchTogether(t *testing.T) {
	backend, lastXRealIP := headerRecordingBackend(t)

	oldCfg := generationConfig(t, config.Global{
		MetricsAddr:        freeTCPPort(t),
		TrustedProxies:     []string{"10.0.0.0/8"},
		MaxRequestBodySize: new(int64(50 * 1024 * 1024)),
	}, generationService("svc", backend.Listener.Addr().String()))

	a := startGenerationApp(t, oldCfg)

	// Old generation: trusted 10.0.0.0/8 -> XFF honored; large body allowed.
	rec := serveThrough(t, a, "svc", proxiedRequest(http.MethodPost, strings.NewReader(strings.Repeat("x", 100))))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Eventually(t, func() bool { return lastXRealIP() == "203.0.113.9" },
		time.Second, 10*time.Millisecond)

	svc, _ := a.registry.GetService("svc")
	oldHandler := svc.Handler()

	// New generation tightens trusted proxies and shrinks the body limit in
	// one candidate (metrics address unchanged).
	newCfg := generationConfig(t, config.Global{
		MetricsAddr:        oldCfg.Global.MetricsAddr,
		TrustedProxies:     []string{"172.16.0.0/12"},
		MaxRequestBodySize: new(int64(10)),
	}, generationService("svc", backend.Listener.Addr().String()))

	require.NoError(t, a.ReloadConfig(newCfg))

	// Candidate is published as current only after commit.
	assert.Same(t, newCfg, a.currentConfig())
	assert.NotSame(t, oldHandler, svc.Handler())

	// New policy: peer no longer trusted -> X-Real-IP is the direct peer.
	rec = serveThrough(t, a, "svc", proxiedRequest(http.MethodGet, nil))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Eventually(t, func() bool { return lastXRealIP() == "10.0.0.1" },
		time.Second, 10*time.Millisecond)

	// Body limit from the same generation is enforced.
	rec = serveThrough(t, a, "svc", proxiedRequest(http.MethodPost, strings.NewReader(strings.Repeat("x", 100))))
	assert.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
}

func TestGlobalGeneration_TransportTimeoutApplies(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(800 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(backend.Close)

	oldCfg := generationConfig(t, config.Global{}, generationService("svc", backend.Listener.Addr().String()))
	a := startGenerationApp(t, oldCfg)

	newCfg := generationConfig(t, config.Global{
		ResponseHeaderTimeout: new(150 * time.Millisecond),
	}, generationService("svc", backend.Listener.Addr().String()))
	require.NoError(t, a.ReloadConfig(newCfg))

	rec := serveThrough(t, a, "svc", proxiedRequest(http.MethodGet, nil))
	assert.Equal(t, http.StatusGatewayTimeout, rec.Code, "new transport timeout must apply to new requests")
}

func TestGlobalGeneration_MetricsMigration(t *testing.T) {
	addrA := freeTCPPort(t)
	addrB := freeTCPPort(t)
	backend, _ := headerRecordingBackend(t)

	oldCfg := generationConfig(t, config.Global{MetricsAddr: addrA},
		generationService("svc", backend.Listener.Addr().String()))
	a := startGenerationApp(t, oldCfg)

	require.Eventually(t, func() bool {
		resp, err := http.Get("http://" + addrA + "/metrics")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 2*time.Second, 20*time.Millisecond)
	assert.Equal(t, addrA, a.MetricsAddr())

	newCfg := generationConfig(t, config.Global{MetricsAddr: addrB},
		generationService("svc", backend.Listener.Addr().String()))
	require.NoError(t, a.ReloadConfig(newCfg))

	// New endpoint is live...
	assert.Equal(t, addrB, a.MetricsAddr())
	resp, err := http.Get("http://" + addrB + "/metrics")
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// ...and the old listener is gone (no duplicate, no leak).
	assert.Eventually(t, func() bool {
		resp, err := http.Get("http://" + addrA + "/metrics")
		if err == nil {
			resp.Body.Close()
		}
		return err != nil
	}, 2*time.Second, 20*time.Millisecond)
}

func TestGlobalGeneration_MetricsBindFailureRollsBackAndRetries(t *testing.T) {
	addrA := freeTCPPort(t)
	backend, lastXRealIP := headerRecordingBackend(t)

	oldCfg := generationConfig(t, config.Global{
		MetricsAddr:    addrA,
		TrustedProxies: []string{"10.0.0.0/8"},
	}, generationService("svc", backend.Listener.Addr().String()))
	a := startGenerationApp(t, oldCfg)

	// Occupy the candidate metrics address before attempting the generation.
	occupier, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	target := occupier.Addr().String()

	candidateCfg := generationConfig(t, config.Global{
		MetricsAddr:    target,
		TrustedProxies: []string{"172.16.0.0/12"},
	}, generationService("svc", backend.Listener.Addr().String()))

	svc, _ := a.registry.GetService("svc")
	handlerBefore := svc.Handler()

	err = a.ReloadConfig(candidateCfg)
	require.Error(t, err, "generation must fail when the metrics address cannot bind")

	// Failed candidate is not published; old scrape endpoint and proxy policy survive.
	assert.Same(t, oldCfg, a.currentConfig(), "failed candidate must not be recorded as current")
	assert.Equal(t, addrA, a.MetricsAddr())
	assert.Same(t, handlerBefore, svc.Handler(), "old handlers must remain in place")

	resp, httpErr := http.Get("http://" + addrA + "/metrics")
	require.NoError(t, httpErr)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	rec := serveThrough(t, a, "svc", proxiedRequest(http.MethodGet, nil))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Eventually(t, func() bool { return lastXRealIP() == "203.0.113.9" },
		time.Second, 10*time.Millisecond, "old trusted proxy policy must still apply")

	// Fault clears: the exact same candidate now succeeds.
	require.NoError(t, occupier.Close())
	assert.Eventually(t, func() bool {
		// Wait for the kernel to release the port.
		ln, lerr := net.Listen("tcp", target)
		if lerr != nil {
			return false
		}
		_ = ln.Close()
		return true
	}, 2*time.Second, 20*time.Millisecond)

	require.NoError(t, a.ReloadConfig(candidateCfg))
	assert.Same(t, candidateCfg, a.currentConfig())
	assert.Equal(t, target, a.MetricsAddr())
	resp, httpErr = http.Get("http://" + target + "/metrics")
	require.NoError(t, httpErr)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestGlobalGeneration_NoOpLeavesStateUntouched(t *testing.T) {
	backend, _ := headerRecordingBackend(t)
	cfg := generationConfig(t, config.Global{MetricsAddr: freeTCPPort(t)},
		generationService("svc", backend.Listener.Addr().String()))
	a := startGenerationApp(t, cfg)

	svc, _ := a.registry.GetService("svc")
	before := svc.Handler()
	addrBefore := a.MetricsAddr()

	require.NoError(t, a.ReloadConfig(cfg)) // identical candidate

	assert.Same(t, before, svc.Handler(), "identical config must not rebuild handlers")
	assert.Equal(t, addrBefore, a.MetricsAddr())
}

func TestGlobalGeneration_ConcurrentUpdatesSerialize(t *testing.T) {
	backend, _ := headerRecordingBackend(t)
	startCfg := generationConfig(t, config.Global{
		MetricsAddr:    freeTCPPort(t),
		TrustedProxies: []string{"10.0.0.0/8"},
	}, generationService("svc", backend.Listener.Addr().String()))
	a := startGenerationApp(t, startCfg)

	const generations = 12
	ports := make([]string, generations)
	for i := range generations {
		ports[i] = freeTCPPort(t)
	}

	errCh := make(chan error, generations)
	var wg sync.WaitGroup
	for i := range generations {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cfg := generationConfig(t, config.Global{
				MetricsAddr:    ports[i],
				TrustedProxies: []string{fmt.Sprintf("10.%d.0.0/16", i)},
			}, generationService("svc", backend.Listener.Addr().String()))
			errCh <- a.ReloadConfig(cfg)
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		assert.NoError(t, err)
	}

	// Exactly one metrics listener remains: the committed one.
	currentAddr := a.MetricsAddr()
	resp, err := http.Get("http://" + currentAddr + "/metrics")
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	dead := 0
	for _, addr := range ports {
		if addr == currentAddr {
			continue
		}
		resp, err := http.Get("http://" + addr + "/metrics")
		if err != nil {
			dead++
			continue
		}
		resp.Body.Close()
	}
	assert.Equal(t, generations-1, dead, "only the latest generation's listener may remain")

	// Service still serves with a coherent policy.
	rec := serveThrough(t, a, "svc", proxiedRequest(http.MethodGet, nil))
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestGlobalGeneration_MixedWithServiceAddition(t *testing.T) {
	backend1, lastXRealIP1 := headerRecordingBackend(t)
	backend2, _ := headerRecordingBackend(t)

	oldCfg := generationConfig(t, config.Global{
		MetricsAddr:    freeTCPPort(t),
		TrustedProxies: []string{"10.0.0.0/8"},
	}, generationService("svc1", backend1.Listener.Addr().String()))
	a := startGenerationApp(t, oldCfg)

	newCfg := generationConfig(t, config.Global{
		MetricsAddr:    freeTCPPort(t),
		TrustedProxies: []string{"172.16.0.0/12"},
	},
		generationService("svc1", backend1.Listener.Addr().String()),
		generationService("svc2", backend2.Listener.Addr().String()),
	)
	require.NoError(t, a.ReloadConfig(newCfg))

	// svc1 handler generation switched ...
	rec := serveThrough(t, a, "svc1", proxiedRequest(http.MethodGet, nil))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Eventually(t, func() bool { return lastXRealIP1() == "10.0.0.1" },
		time.Second, 10*time.Millisecond)

	// ... while svc2 was brought up via the dynamic add path.
	svc2, ok := a.registry.GetService("svc2")
	require.True(t, ok)
	rec2 := httptest.NewRecorder()
	svc2.Handler().ServeHTTP(rec2, proxiedRequest(http.MethodGet, nil))
	assert.Equal(t, http.StatusOK, rec2.Code)
}

// currentConfig returns the App's published config under the reload lock.
func (a *App) currentConfig() *config.Config {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.cfg
}
