package tailscale

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"tailscale.com/ipn/ipnstate"

	"github.com/jtdowney/tsbridge/internal/config"
	tsnet "github.com/jtdowney/tsbridge/internal/tsnet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPreflightSuccessWithAuthKey verifies the ephemeral probe authenticates
// against the control plane and is removed afterwards.
func TestPreflightSuccessWithAuthKey(t *testing.T) {
	stateDir := t.TempDir()

	var upCalls, closeCalls int32
	var createdNames []string
	factory := func(serviceName string) tsnet.TSNetServer {
		createdNames = append(createdNames, serviceName)
		mock := tsnet.NewMockTSNetServer()
		mock.UpFunc = func(ctx context.Context) (*ipnstate.Status, error) {
			atomic.AddInt32(&upCalls, 1)
			return &ipnstate.Status{}, nil
		}
		mock.CloseFunc = func() error {
			atomic.AddInt32(&closeCalls, 1)
			return nil
		}
		return mock
	}

	cfg := config.Tailscale{
		AuthKey:  config.RedactedString("test-auth-key"),
		StateDir: stateDir,
	}
	server, err := NewServerWithFactory(cfg, factory)
	require.NoError(t, err)

	require.NoError(t, server.Preflight(context.Background()))
	require.Contains(t, createdNames, PreflightProbeName)
	assert.Equal(t, int32(1), atomic.LoadInt32(&upCalls), "probe Up should run once")
	assert.Equal(t, int32(1), atomic.LoadInt32(&closeCalls), "probe node should close")

	// The throwaway probe state must not remain in the state directory.
	_, err = os.Stat(filepath.Join(stateDir, PreflightProbeName))
	assert.True(t, os.IsNotExist(err), "probe state directory should be removed")

	// The validated server itself stays open and can still serve services.
	assert.False(t, server.Closed())
	listener, err := server.Listen(config.Service{
		Name:        "svc",
		BackendAddr: "localhost:8080",
	}, config.TLSModeOff, false)
	require.NoError(t, err)
	require.NotNil(t, listener)
}

// TestPreflightControlPlaneAuthFailureRejectsCandidate ensures an invalid
// candidate is rejected while the server itself is not marked closed.
func TestPreflightControlPlaneAuthFailureRejectsCandidate(t *testing.T) {
	stateDir := t.TempDir()
	controlErr := errors.New("control plane rejected auth key")

	var upCalls, closeCalls int32
	factory := func(serviceName string) tsnet.TSNetServer {
		mock := tsnet.NewMockTSNetServer()
		mock.UpFunc = func(ctx context.Context) (*ipnstate.Status, error) {
			atomic.AddInt32(&upCalls, 1)
			return nil, controlErr
		}
		mock.CloseFunc = func() error {
			atomic.AddInt32(&closeCalls, 1)
			return nil
		}
		return mock
	}

	cfg := config.Tailscale{
		AuthKey:    config.RedactedString("bad-key"),
		ControlURL: "https://headscale.example.net",
		StateDir:  stateDir,
	}
	server, err := NewServerWithFactory(cfg, factory)
	require.NoError(t, err)

	err = server.Preflight(context.Background())
	require.Error(t, err)
	assert.ErrorIs(t, err, controlErr)
	assert.Equal(t, int32(1), atomic.LoadInt32(&upCalls))
	assert.Equal(t, int32(1), atomic.LoadInt32(&closeCalls), "failed probe must close")
	assert.False(t, server.Closed(), "rejected candidate must not retire the server")
}

// TestPreflightAuthFreeSkipsLiveProbe verifies deployments without
// credentials (existing-state mode) only validate the state directory.
func TestPreflightAuthFreeSkipsLiveProbe(t *testing.T) {
	stateDir := t.TempDir()

	var created int32
	factory := func(serviceName string) tsnet.TSNetServer {
		atomic.AddInt32(&created, 1)
		return tsnet.NewMockTSNetServer()
	}

	server, err := NewServerWithFactory(config.Tailscale{StateDir: stateDir}, factory)
	require.NoError(t, err)

	require.NoError(t, server.Preflight(context.Background()))
	assert.Equal(t, int32(0), atomic.LoadInt32(&created), "no probe node without credentials")
}

// TestPreflightOAuthWithoutDefaultTagsSkipsProbe ensures tag-scoped OAuth
// clients are not falsely rejected by an untagged probe key attempt.
func TestPreflightOAuthWithoutDefaultTagsSkipsProbe(t *testing.T) {
	stateDir := t.TempDir()

	var created int32
	factory := func(serviceName string) tsnet.TSNetServer {
		atomic.AddInt32(&created, 1)
		return tsnet.NewMockTSNetServer()
	}

	cfg := config.Tailscale{
		OAuthClientID:     "oauth-client-id",
		OAuthClientSecret: config.RedactedString("oauth-secret"),
		StateDir:          stateDir,
	}
	server, err := NewServerWithFactory(cfg, factory)
	require.NoError(t, err)

	require.NoError(t, server.Preflight(context.Background()))
	assert.Equal(t, int32(0), atomic.LoadInt32(&created))
}

// TestPreflightRejectsUnwritableStateDir verifies state directory validation.
func TestPreflightRejectsUnwritableStateDir(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "not-a-directory")
	require.NoError(t, os.WriteFile(blocker, []byte("x"), 0o600))

	factory := func(serviceName string) tsnet.TSNetServer {
		t.Fatal("probe node must not be created when the state directory is invalid")
		return nil
	}
	server, err := NewServerWithFactory(config.Tailscale{
		AuthKey:  config.RedactedString("test-key"),
		StateDir: blocker,
	}, factory)
	require.NoError(t, err)

	err = server.Preflight(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "state directory")
}

// TestPreflightAfterCloseRejected verifies a retired server never validates or
// creates resources again.
func TestPreflightAfterCloseRejected(t *testing.T) {
	server, err := NewServerWithFactory(config.Tailscale{StateDir: t.TempDir()}, nil)
	require.NoError(t, err)
	require.NoError(t, server.Close())

	err = server.Preflight(context.Background())
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrServerClosed)

	// Close must be idempotent.
	require.NoError(t, server.Close())
}

// TestListenAfterCloseRejected verifies a retired server cannot create a new
// tsnet node/listener.
func TestListenAfterCloseRejected(t *testing.T) {
	server, err := NewServerWithFactory(config.Tailscale{
		AuthKey:  config.RedactedString("test-key"),
		StateDir: t.TempDir(),
	}, nil)
	require.NoError(t, err)
	require.NoError(t, server.Close())

	_, err = server.Listen(config.Service{
		Name:        "svc",
		BackendAddr: "localhost:8080",
	}, config.TLSModeOff, false)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrServerClosed)
}
