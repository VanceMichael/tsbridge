package app

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"

	"tailscale.com/ipn/ipnstate"

	"github.com/jtdowney/tsbridge/internal/config"
	"github.com/jtdowney/tsbridge/internal/tailscale"
	"github.com/jtdowney/tsbridge/internal/tsnet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// trackedNode records the lifecycle of one tsnet node created by a test
// factory.
type trackedNode struct {
	name       string
	mock       *tsnet.MockTSNetServer
	sequence   int64
	upCalls    atomic.Int32
	closeCalls atomic.Int32
	plain      atomic.Int32
	tls        atomic.Int32
	funnel     atomic.Int32
}

// nodeHarness builds tracked tsnet nodes and can inject Up/Listen failures.
type nodeHarness struct {
	mu sync.Mutex

	nodes []*trackedNode
	seq   int64
	// upFailNTimes causes the next n Up() calls for a node name to fail.
	upFailNTimes map[string]int
	upFail       map[string]error
}

func newNodeHarness() *nodeHarness {
	return &nodeHarness{
		upFailNTimes: make(map[string]int),
		upFail:       make(map[string]error),
	}
}

func (h *nodeHarness) failUpNTimes(name string, n int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.upFailNTimes[name] = n
}

func (h *nodeHarness) failUp(name string, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.upFail[name] = err
}

func (h *nodeHarness) clearFailUp(name string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.upFail, name)
}

func (h *nodeHarness) factory(serviceName string) tsnet.TSNetServer {
	h.mu.Lock()
	h.seq++
	node := &trackedNode{name: serviceName, sequence: h.seq}
	h.nodes = append(h.nodes, node)
	h.mu.Unlock()

	listenOK := func() (net.Listener, error) {
		return net.Listen("tcp", "127.0.0.1:0")
	}

	mock := tsnet.NewMockTSNetServer()
	mock.UpFunc = func(ctx context.Context) (*ipnstate.Status, error) {
		node.upCalls.Add(1)
		h.mu.Lock()
		if h.upFailNTimes[serviceName] > 0 {
			h.upFailNTimes[serviceName]--
			h.mu.Unlock()
			return nil, errors.New("simulated candidate tsnet Up failure")
		}
		err := h.upFail[serviceName]
		h.mu.Unlock()
		if err != nil {
			return nil, err
		}
		return &ipnstate.Status{}, nil
	}
	mock.CloseFunc = func() error {
		node.closeCalls.Add(1)
		return nil
	}
	mock.ListenFunc = func(network, addr string) (net.Listener, error) {
		node.plain.Add(1)
		return listenOK()
	}
	mock.ListenTLSFunc = func(network, addr string) (net.Listener, error) {
		node.tls.Add(1)
		return listenOK()
	}
	mock.ListenFunnelFunc = func(network, addr string) (net.Listener, error) {
		node.funnel.Add(1)
		return listenOK()
	}
	node.mock = mock
	return mock
}

func (h *nodeHarness) createdCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.nodes)
}

func (h *nodeHarness) currentSeq() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.seq
}

func (h *nodeHarness) nodesCreatedAfter(seq int64) []*trackedNode {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []*trackedNode
	for _, n := range h.nodes {
		if n.sequence > seq {
			out = append(out, n)
		}
	}
	return out
}

func (h *nodeHarness) findNodes(name string) []*trackedNode {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []*trackedNode
	for _, n := range h.nodes {
		if n.name == name {
			out = append(out, n)
		}
	}
	return out
}

// lastNode returns the most recently created node with the given name.
func (h *nodeHarness) lastNode(name string) *trackedNode {
	nodes := h.findNodes(name)
	if len(nodes) == 0 {
		return nil
	}
	return nodes[len(nodes)-1]
}

func plainService(name, port string) config.Service {
	return config.Service{
		Name:        name,
		BackendAddr: "127.0.0.1:" + port,
		TLSMode:     config.TLSModeOff,
	}
}

func rotationConfig(stateDir, authKey, controlURL string, svcs ...config.Service) *config.Config {
	cfg := &config.Config{
		Tailscale: config.Tailscale{
			AuthKey:    config.RedactedString(authKey),
			ControlURL: controlURL,
			StateDir:   stateDir,
		},
		Services: svcs,
	}
	cfg.SetDefaults()
	return cfg
}

func startGenerationApp(t *testing.T, h *nodeHarness, cfg *config.Config) *App {
	t.Helper()
	a, err := NewAppWithOptions(cfg, Options{TSNetFactory: h.factory})
	require.NoError(t, err)
	require.NoError(t, a.Start(context.Background()))
	t.Cleanup(func() {
		_ = a.Shutdown(context.Background())
	})
	return a
}

// activeNode returns the tsnet node mock currently registered for a service
// on the given server.
func activeNode(t *testing.T, server *tailscale.Server, name string) *tsnet.MockTSNetServer {
	t.Helper()
	node := server.GetServiceServer(name)
	require.NotNil(t, node, "no active tsnet node for service %q", name)
	mock, ok := node.(*tsnet.MockTSNetServer)
	require.True(t, ok)
	return mock
}

// TestRotationMigratesAllServicesToNewGeneration verifies the happy path:
// preflight, per-service migration, commit and retirement.
func TestRotationMigratesAllServicesToNewGeneration(t *testing.T) {
	h := newNodeHarness()
	dirA := t.TempDir()
	cfg1 := rotationConfig(dirA, "key-a", "",
		plainService("svc-a", "8081"),
		plainService("svc-b", "8082"),
		plainService("svc-c", "8083"),
	)
	a := startGenerationApp(t, h, cfg1)

	oldServer := a.tsServer
	require.Equal(t, uint64(initialGeneration), a.generation)
	oldNodes := map[string]*tsnet.MockTSNetServer{}
	for _, name := range []string{"svc-a", "svc-b", "svc-c"} {
		oldNodes[name] = activeNode(t, oldServer, name)
	}

	// Rotate credentials, control URL and state directory. Services unchanged.
	cfg2 := rotationConfig(t.TempDir(), "key-b", "https://headscale.example.net",
		plainService("svc-a", "8081"),
		plainService("svc-b", "8082"),
		plainService("svc-c", "8083"),
	)
	require.NoError(t, a.ReloadConfig(cfg2))

	// New generation owns the registry pointer, every service, nodes/listeners.
	assert.Equal(t, uint64(2), a.generation)
	require.NotSame(t, oldServer, a.tsServer)
	assert.True(t, oldServer.Closed(), "outgoing generation must be retired")
	assert.False(t, a.tsServer.Closed())
	for _, svc := range cfg2.Services {
		running, ok := a.registry.GetService(svc.Name)
		require.True(t, ok, "service %q must exist after rotation", svc.Name)
		assert.Same(t, a.tsServer, running.TSServer(), "service %q must be owned by the new generation", svc.Name)
	}

	// Old nodes closed; new nodes for every service are up on the new server.
	for _, name := range []string{"svc-a", "svc-b", "svc-c"} {
		newNode := activeNode(t, a.tsServer, name)
		assert.NotSame(t, oldNodes[name], newNode, "service %q needs a new-generation node", name)
		assert.GreaterOrEqual(t, h.lastNode(name).upCalls.Load(), int32(1))
		assert.Equal(t, int32(0), h.lastNode(name).closeCalls.Load(), "active node must not be closed")

		oldTracked := h.findNodes(name)
		var oldClosed bool
		for _, n := range oldTracked {
			if n.mock == oldNodes[name] {
				oldClosed = n.closeCalls.Load() > 0
			}
		}
		assert.True(t, oldClosed, "old node for %q must be closed", name)
	}

	// The preflight probe was ephemeral and closed.
	probe := h.lastNode(tailscale.PreflightProbeName)
	require.NotNil(t, probe)
	assert.GreaterOrEqual(t, probe.closeCalls.Load(), int32(1))

	// Subsequent service additions only use the new generation. cfg3 keeps
	// the exact committed Tailscale configuration and adds one service.
	cfg3 := &config.Config{
		Tailscale: cfg2.Tailscale,
		Services: []config.Service{
			plainService("svc-a", "8081"),
			plainService("svc-b", "8082"),
			plainService("svc-c", "8083"),
			plainService("svc-d", "8084"),
		},
	}
	cfg3.SetDefaults()
	require.True(t, config.TailscaleEqual(cfg2.Tailscale, cfg3.Tailscale))
	require.NoError(t, a.ReloadConfig(cfg3))
	assert.Equal(t, uint64(2), a.generation, "service add must not rotate generations")
	assert.True(t, oldServer.Closed())
	assert.NotNil(t, a.tsServer.GetServiceServer("svc-d"))
	assert.Nil(t, oldServer.GetServiceServer("svc-d"))
}

// TestRotationServiceOnlyChangeDoesNotRotate ensures ordinary service label
// changes don't churn unrelated nodes or start a new generation.
func TestRotationServiceOnlyChangeDoesNotRotate(t *testing.T) {
	h := newNodeHarness()
	dir := t.TempDir()
	cfg1 := rotationConfig(dir, "key-a", "",
		plainService("svc-a", "8081"),
		plainService("svc-b", "8082"),
	)
	a := startGenerationApp(t, h, cfg1)
	oldServer := a.tsServer
	nodeB := activeNode(t, oldServer, "svc-b")
	nodeA := activeNode(t, oldServer, "svc-a")
	createdBefore := h.createdCount()

	cfg2 := rotationConfig(dir, "key-a", "",
		plainService("svc-a", "9091"), // backend label changed
		plainService("svc-b", "8082"),
	)
	require.NoError(t, a.ReloadConfig(cfg2))

	assert.Same(t, oldServer, a.tsServer, "same tailscale config must keep the generation")
	assert.Equal(t, uint64(1), a.generation)
	assert.False(t, oldServer.Closed())
	// Only svc-a's node is recreated; svc-b is untouched.
	assert.Same(t, nodeB, a.tsServer.GetServiceServer("svc-b"), "unrelated service node must not rotate")
	assert.NotSame(t, nodeA, a.tsServer.GetServiceServer("svc-a"), "changed service gets a replacement node")
	// No preflight probe should have been created.
	assert.Nil(t, h.lastNode(tailscale.PreflightProbeName))
	assert.Greater(t, h.createdCount(), createdBefore, "svc-a still needs a replacement node")
}

// TestRotationIdenticalConfigDoesNothing verifies an identical resolved
// Tailscale config creates no nodes or generations.
func TestRotationIdenticalConfigDoesNothing(t *testing.T) {
	h := newNodeHarness()
	dir := t.TempDir()
	cfg1 := rotationConfig(dir, "key-a", "",
		plainService("svc-a", "8081"),
	)
	a := startGenerationApp(t, h, cfg1)
	before := h.createdCount()

	require.NoError(t, a.ReloadConfig(rotationConfig(dir, "key-a", "", plainService("svc-a", "8081"))))
	assert.Equal(t, uint64(1), a.generation)
	assert.Equal(t, before, h.createdCount(), "identical config must not create nodes")
	assert.Nil(t, h.lastNode(tailscale.PreflightProbeName))
}

// TestRotationInvalidCandidateRejectedAndRetriable verifies a failed
// preflight leaves every online node untouched, and the candidate retries
// after the external fault is fixed.
func TestRotationInvalidCandidateRejectedAndRetriable(t *testing.T) {
	h := newNodeHarness()
	dirA := t.TempDir()
	cfg1 := rotationConfig(dirA, "key-a", "",
		plainService("svc-a", "8081"),
		plainService("svc-b", "8082"),
	)
	a := startGenerationApp(t, h, cfg1)
	oldServer := a.tsServer
	oldA := activeNode(t, oldServer, "svc-a")
	oldB := activeNode(t, oldServer, "svc-b")

	probeErr := errors.New("control plane unreachable")
	h.failUp(tailscale.PreflightProbeName, probeErr)

	cfg2 := rotationConfig(t.TempDir(), "key-b", "https://headscale.example.net",
		plainService("svc-a", "8081"),
		plainService("svc-b", "8082"),
	)
	err := a.ReloadConfig(cfg2)
	require.Error(t, err)
	assert.ErrorIs(t, err, probeErr)

	// Old generation entirely untouched.
	assert.Same(t, oldServer, a.tsServer)
	assert.Equal(t, uint64(1), a.generation)
	assert.False(t, oldServer.Closed())
	assert.Same(t, oldA, a.tsServer.GetServiceServer("svc-a"))
	assert.Same(t, oldB, a.tsServer.GetServiceServer("svc-b"))
	assert.Equal(t, "key-a", a.cfg.Tailscale.AuthKey.Value(), "config must not advance on rejected candidate")
	for _, name := range []string{"svc-a", "svc-b"} {
		for _, n := range h.findNodes(name) {
			assert.Equal(t, int32(0), n.closeCalls.Load(), "no service node may close when preflight fails")
		}
	}
	// No service nodes were created during preflight (only the probe).
	assert.Len(t, h.findNodes("svc-a"), 1)
	assert.Len(t, h.findNodes("svc-b"), 1)

	// Fix the external fault and retry: rotation now commits.
	h.clearFailUp(tailscale.PreflightProbeName)
	require.NoError(t, a.ReloadConfig(cfg2))
	assert.Equal(t, uint64(2), a.generation)
	assert.True(t, oldServer.Closed())
	assert.NotSame(t, oldServer, a.tsServer)
	assert.Equal(t, "key-b", a.cfg.Tailscale.AuthKey.Value())
}

// TestRotationMidMigrationFailureRollsBack verifies that after several
// services migrated, a candidate Up failure cleans candidate nodes and
// restores every old service on the outgoing generation with no orphans or
// deleted services; a later retry succeeds.
func TestRotationMidMigrationFailureRollsBack(t *testing.T) {
	h := newNodeHarness()
	dirA := t.TempDir()
	cfg1 := rotationConfig(dirA, "key-a", "",
		plainService("svc-a", "8081"),
		plainService("svc-b", "8082"),
		plainService("svc-c", "8083"),
	)
	a := startGenerationApp(t, h, cfg1)
	oldServer := a.tsServer
	originalA := activeNode(t, oldServer, "svc-a")
	originalB := activeNode(t, oldServer, "svc-b")
	originalC := activeNode(t, oldServer, "svc-c")
	markerSeq := h.currentSeq()

	// The candidate attempt for svc-b fails once; the rollback restore of
	// svc-b then succeeds, modelling a transient external fault.
	h.failUpNTimes("svc-b", 1)

	cfg2 := rotationConfig(t.TempDir(), "key-b", "https://headscale.example.net",
		plainService("svc-a", "8081"),
		plainService("svc-b", "8082"),
		plainService("svc-c", "8083"),
	)
	err := a.ReloadConfig(cfg2)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "svc-b")

	// Application remains owned by the old generation with old config.
	assert.Same(t, oldServer, a.tsServer)
	assert.False(t, oldServer.Closed())
	assert.Equal(t, uint64(1), a.generation)
	assert.Equal(t, "key-a", a.cfg.Tailscale.AuthKey.Value())

	// No service deleted: all three are present and reachable on old gen.
	for _, name := range []string{"svc-a", "svc-b", "svc-c"} {
		_, ok := a.registry.GetService(name)
		assert.True(t, ok, "service %q must be restored, not deleted", name)
		assert.NotNil(t, oldServer.GetServiceServer(name), "service %q must have an old-generation node", name)
	}

	// svc-c was never touched: its original node is still the registered one
	// and was never closed.
	assert.Same(t, originalC, oldServer.GetServiceServer("svc-c"),
		"services not reached by migration keep their original nodes")
	for _, n := range h.findNodes("svc-c") {
		if n.mock == originalC {
			assert.Equal(t, int32(0), n.closeCalls.Load(), "untouched node must stay open")
		}
	}

	// svc-a/svc-b original nodes were stopped during cutover; their restored
	// replacements are distinct, healthy, and registered on old gen.
	for name, original := range map[string]*tsnet.MockTSNetServer{
		"svc-a": originalA, "svc-b": originalB,
	} {
		current := activeNode(t, oldServer, name)
		assert.NotSame(t, original, current, "%s must be restored with a fresh node", name)
		assert.Equal(t, int32(0), h.lastNode(name).closeCalls.Load(), "restored node must be open")
	}

	// Every candidate-created node not active on the restored old generation
	// (probe, candidate svc-a, failed candidate svc-b) must be closed: no
	// orphan tsnet nodes.
	for _, n := range h.nodesCreatedAfter(markerSeq) {
		if n.name == tailscale.PreflightProbeName || oldServer.GetServiceServer(n.name) != n.mock {
			assert.GreaterOrEqual(t, n.closeCalls.Load(), int32(1),
				"candidate node %q (seq %d) must be closed", n.name, n.sequence)
		}
	}

	// Retry after the external fault clears commits the new generation.
	require.NoError(t, a.ReloadConfig(cfg2))
	assert.Equal(t, uint64(2), a.generation)
	assert.True(t, oldServer.Closed())
	for _, name := range []string{"svc-a", "svc-b", "svc-c"} {
		assert.NotNil(t, a.tsServer.GetServiceServer(name))
	}
}

// TestRotationRemovedAndAddedServices verifies services exist only in one
// generation are owned by the right server after commit.
func TestRotationRemovedAndAddedServices(t *testing.T) {
	h := newNodeHarness()
	cfg1 := rotationConfig(t.TempDir(), "key-a", "",
		plainService("svc-a", "8081"),
		plainService("svc-gone", "8099"),
	)
	a := startGenerationApp(t, h, cfg1)
	oldServer := a.tsServer

	cfg2 := rotationConfig(t.TempDir(), "key-b", "https://control.example",
		plainService("svc-a", "8081"),
		plainService("svc-new", "8090"),
	)
	require.NoError(t, a.ReloadConfig(cfg2))

	_, hasGone := a.registry.GetService("svc-gone")
	assert.False(t, hasGone)
	_, hasNew := a.registry.GetService("svc-new")
	assert.True(t, hasNew)
	assert.NotNil(t, a.tsServer.GetServiceServer("svc-new"))
	assert.Nil(t, oldServer.GetServiceServer("svc-new"))
	assert.True(t, oldServer.Closed(), "retirement closes the removed service's old node too")
}

// TestRotationPreservesFunnelAndTLS verifies listener types survive the
// generation switch.
func TestRotationPreservesFunnelAndTLS(t *testing.T) {
	h := newNodeHarness()

	funnel := plainService("svc-funnel", "8081")
	funnel.FunnelEnabled = boolPtr(true)
	funnel.TLSMode = config.TLSModeAuto

	tlsSvc := plainService("svc-tls", "8082")
	tlsSvc.TLSMode = config.TLSModeAuto

	cfg1 := rotationConfig(t.TempDir(), "key-a", "", funnel, tlsSvc)
	a := startGenerationApp(t, h, cfg1)

	cfg2 := rotationConfig(t.TempDir(), "key-b", "https://control.example", funnel, tlsSvc)
	require.NoError(t, a.ReloadConfig(cfg2))

	newFunnel := h.lastNode("svc-funnel")
	newTLS := h.lastNode("svc-tls")
	require.NotNil(t, newFunnel)
	require.NotNil(t, newTLS)
	assert.Equal(t, int32(1), newFunnel.funnel.Load(), "funnel listener must be recreated on new generation")
	assert.Equal(t, int32(1), newTLS.tls.Load(), "TLS listener must be recreated on new generation")
	assert.Equal(t, int32(0), newFunnel.plain.Load())
}

// TestRotationRejectedAfterShutdown verifies a closing app never creates a
// fresh generation or candidate resources.
func TestRotationRejectedAfterShutdown(t *testing.T) {
	h := newNodeHarness()
	cfg1 := rotationConfig(t.TempDir(), "key-a", "", plainService("svc-a", "8081"))
	a := startGenerationApp(t, h, cfg1)
	before := h.createdCount()

	require.NoError(t, a.Shutdown(context.Background()))

	cfg2 := rotationConfig(t.TempDir(), "key-b", "https://control.example", plainService("svc-a", "8081"))
	err := a.ReloadConfig(cfg2)
	require.Error(t, err)
	assert.ErrorIs(t, err, errAppStopped)
	assert.Equal(t, before, h.createdCount(), "shutdown must block candidate resource creation")
	assert.Equal(t, uint64(1), a.generation)
}

// TestRotationSerializesConcurrentEvents runs many rotations concurrently and
// asserts a single coherent generation survives (run with -race).
func TestRotationSerializesConcurrentEvents(t *testing.T) {
	h := newNodeHarness()
	svcs := []config.Service{plainService("svc-a", "8081")}
	cfg1 := rotationConfig(t.TempDir(), "key-0", "", svcs...)
	a := startGenerationApp(t, h, cfg1)

	const n = 8
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 1; i <= n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cfg := rotationConfig(t.TempDir(), fmt.Sprintf("key-%d", i),
				fmt.Sprintf("https://control-%d.example", i), svcs...)
			errs <- a.ReloadConfig(cfg)
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		assert.NoError(t, err)
	}

	assert.Equal(t, uint64(1+n), a.generation)
	assert.False(t, a.tsServer.Closed(), "active generation must be open")
	node := activeNode(t, a.tsServer, "svc-a")
	assert.NotNil(t, node)
}

func boolPtr(v bool) *bool { return &v }
