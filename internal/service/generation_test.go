package service

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/jtdowney/tsbridge/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newGenerationTestConfig builds a normalized config with a single service
// proxying to backendAddr and the given trusted proxies.
func newGenerationTestConfig(name, backendAddr string, trustedProxies ...string) *config.Config {
	cfg := &config.Config{
		Tailscale: config.Tailscale{AuthKey: "test-key"},
		Global: config.Global{
			TrustedProxies: trustedProxies,
		},
		Services: []config.Service{
			{
				Name:         name,
				BackendAddr:  backendAddr,
				TLSMode:      config.TLSModeOff,
				WhoisEnabled: new(bool),
			},
		},
	}
	cfg.SetDefaults()
	cfg.Normalize()
	return cfg
}

// recordedHeaders captures headers seen by the test backend.
type recordedHeaders struct {
	mu      sync.Mutex
	xRealIP string
}

func (r *recordedHeaders) snapshot() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.xRealIP
}

func newHeaderRecordingBackend(t *testing.T) (*httptest.Server, *recordedHeaders) {
	t.Helper()
	rec := &recordedHeaders{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.mu.Lock()
		rec.xRealIP = r.Header.Get("X-Real-IP")
		rec.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	return server, rec
}

func TestAtomicHandler(t *testing.T) {
	first := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("first"))
	})
	second := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("second"))
	})

	ref := newAtomicHandler(first)

	rec := httptest.NewRecorder()
	ref.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
	assert.Equal(t, "first", rec.Body.String())

	ref.Store(second)
	rec = httptest.NewRecorder()
	ref.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
	assert.Equal(t, "second", rec.Body.String())
}

func TestGeneration_TrustedProxiesSwitchAtomically(t *testing.T) {
	backend, seen := newHeaderRecordingBackend(t)

	oldCfg := newGenerationTestConfig("svc", backend.Listener.Addr().String(), "10.0.0.0/8")
	tsServer, err := testTailscaleServerFactory()
	require.NoError(t, err)
	registry := NewRegistry(oldCfg, tsServer)
	require.NoError(t, registry.StartServices())
	t.Cleanup(func() { _ = registry.Shutdown(t.Context()) })

	svc, ok := registry.GetService("svc")
	require.True(t, ok)

	proxyRequest := func() {
		req := httptest.NewRequest("GET", "/", nil)
		req.RemoteAddr = "10.0.0.1:51234"
		req.Header.Set("X-Forwarded-For", "203.0.113.9")
		rec := httptest.NewRecorder()
		svc.Handler().ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Old generation trusts 10.0.0.0/8, so the XFF chain is honored.
	proxyRequest()
	assert.Eventually(t, func() bool { return seen.snapshot() == "203.0.113.9" },
		time.Second, 10*time.Millisecond)

	// Prepare a generation that tightens trusted proxies to a different range.
	newCfg := newGenerationTestConfig("svc", backend.Listener.Addr().String(), "172.16.0.0/12")
	candidates, err := registry.BuildGenerationHandlers(newCfg)
	require.NoError(t, err)
	require.Len(t, candidates, 1)

	// Before commit the running handler still applies the old policy.
	proxyRequest()
	assert.Eventually(t, func() bool { return seen.snapshot() == "203.0.113.9" },
		time.Second, 10*time.Millisecond)

	// Commit swaps the handler without recreating the listener.
	registry.CommitGeneration(newCfg, candidates)
	assert.Equal(t, newCfg, registryCurrentConfig(registry))

	// New generation: 10.0.0.1 is no longer trusted; X-Real-IP is the peer.
	proxyRequest()
	assert.Eventually(t, func() bool { return seen.snapshot() == "10.0.0.1" },
		time.Second, 10*time.Millisecond)
}

func TestGeneration_BuildFailureKeepsOldHandlers(t *testing.T) {
	backend, seen := newHeaderRecordingBackend(t)

	oldCfg := newGenerationTestConfig("svc", backend.Listener.Addr().String(), "10.0.0.0/8")
	tsServer, err := testTailscaleServerFactory()
	require.NoError(t, err)
	registry := NewRegistry(oldCfg, tsServer)
	require.NoError(t, registry.StartServices())
	t.Cleanup(func() { _ = registry.Shutdown(t.Context()) })

	svc, ok := registry.GetService("svc")
	require.True(t, ok)
	oldHandler := svc.Handler()

	// An invalid trusted proxy cannot be parsed into a handler. The registry
	// method is called directly to simulate a build failure that slips past
	// upstream validation (the App validates the candidate beforehand).
	badCfg := newGenerationTestConfig("svc", backend.Listener.Addr().String())
	badCfg.Global.TrustedProxies = []string{"not-an-ip-or-cidr"}

	candidates, err := registry.BuildGenerationHandlers(badCfg)
	require.Error(t, err)
	assert.Nil(t, candidates)

	// The running service keeps its previous handler and policy.
	assert.Same(t, oldHandler, svc.Handler())
	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "10.0.0.1:51234"
	req.Header.Set("X-Forwarded-For", "203.0.113.9")
	rec := httptest.NewRecorder()
	svc.Handler().ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Eventually(t, func() bool { return seen.snapshot() == "203.0.113.9" },
		time.Second, 10*time.Millisecond)

	// A corrected candidate builds successfully: the failed attempt left no
	// poisoned state.
	goodCfg := newGenerationTestConfig("svc", backend.Listener.Addr().String(), "10.0.0.0/8")
	candidates, err = registry.BuildGenerationHandlers(goodCfg)
	require.NoError(t, err)
	require.Len(t, candidates, 1)
	registry.CommitGeneration(goodCfg, candidates)
}

func TestGeneration_ShapeChangedServicesAreNotCandidates(t *testing.T) {
	backend, _ := newHeaderRecordingBackend(t)

	oldCfg := newGenerationTestConfig("svc", backend.Listener.Addr().String())
	tsServer, err := testTailscaleServerFactory()
	require.NoError(t, err)
	registry := NewRegistry(oldCfg, tsServer)
	require.NoError(t, registry.StartServices())
	t.Cleanup(func() { _ = registry.Shutdown(t.Context()) })

	// Service backend changed: this is a dynamic update, not a handler
	// generation candidate.
	newCfg := newGenerationTestConfig("svc", "127.0.0.1:59999")
	candidates, err := registry.BuildGenerationHandlers(newCfg)
	require.NoError(t, err)
	assert.Empty(t, candidates, "shape-changed services must go through the dynamic update path")
}

// registryCurrentConfig exposes the registry's live config for assertions.
func registryCurrentConfig(r *Registry) *config.Config {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.config
}
