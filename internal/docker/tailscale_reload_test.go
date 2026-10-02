package docker

import (
	"context"
	"testing"

	"github.com/jtdowney/tsbridge/internal/config"
	"github.com/moby/moby/api/types/container"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubHostname makes findSelfContainer resolve the tsbridge container by the
// returned container ID, so tests can omit the oauth_client_id label.
func stubHostname(t *testing.T, hostname string) {
	t.Helper()
	old := readFile
	readFile = func(string) ([]byte, error) {
		return []byte(hostname), nil
	}
	t.Cleanup(func() { readFile = old })
}

func tsbridgeContainerWithLabels(id string, labels map[string]string) container.Summary {
	if labels == nil {
		labels = map[string]string{}
	}
	return container.Summary{
		ID:     id,
		Names:  []string{"/tsbridge"},
		Labels: labels,
		State:  "running",
	}
}

// TestLoadReparsesTailscaleLabelsEveryTime verifies rotated control_url and
// state_dir labels are observed on every Load instead of being frozen at
// process start.
func TestLoadReparsesTailscaleLabelsEveryTime(t *testing.T) {
	mockClient := newMockDockerClient()
	self := tsbridgeContainerWithLabels("tsbridge123", map[string]string{
		"tsbridge.tailscale.oauth_client_id":     "tskey-client",
		"tsbridge.tailscale.oauth_client_secret": "secret-a",
		"tsbridge.tailscale.control_url":         "https://control-a.example.net",
		"tsbridge.tailscale.state_dir":           "/var/lib/tsbridge-a",
		"tsbridge.tailscale.default_tags":        "tag:server",
	})
	mockClient.containers = []container.Summary{self}

	provider := &Provider{client: mockClient, labelPrefix: "tsbridge"}

	cfg1, err := provider.Load(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "https://control-a.example.net", cfg1.Tailscale.ControlURL)
	assert.Equal(t, "/var/lib/tsbridge-a", cfg1.Tailscale.StateDir)

	// Rotate labels on the running container (e.g. via container recreate).
	self.Labels["tsbridge.tailscale.control_url"] = "https://control-b.example.net"
	self.Labels["tsbridge.tailscale.state_dir"] = "/var/lib/tsbridge-b"

	cfg2, err := provider.Load(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "https://control-b.example.net", cfg2.Tailscale.ControlURL)
	assert.Equal(t, "/var/lib/tsbridge-b", cfg2.Tailscale.StateDir)
	assert.False(t, config.TailscaleEqual(cfg1.Tailscale, cfg2.Tailscale))
}

// TestLoadRotatesInlineAuthKey verifies a new auth key label replaces the
// previous one and surfaces as a real difference.
func TestLoadRotatesInlineAuthKey(t *testing.T) {
	stubHostname(t, "tsbridge123")

	mockClient := newMockDockerClient()
	self := tsbridgeContainerWithLabels("tsbridge123", map[string]string{
		"tsbridge.tailscale.auth_key":  "key-a",
		"tsbridge.tailscale.state_dir": "/var/lib/tsbridge",
	})
	mockClient.containers = []container.Summary{self}

	provider := &Provider{client: mockClient, labelPrefix: "tsbridge"}

	cfg1, err := provider.Load(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "key-a", cfg1.Tailscale.AuthKey.Value())

	self.Labels["tsbridge.tailscale.auth_key"] = "key-b"
	cfg2, err := provider.Load(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "key-b", cfg2.Tailscale.AuthKey.Value())
	assert.False(t, config.TailscaleEqual(cfg1.Tailscale, cfg2.Tailscale))

	// Re-loading the same labels stays stable.
	cfg3, err := provider.Load(context.Background())
	require.NoError(t, err)
	assert.True(t, config.TailscaleEqual(cfg2.Tailscale, cfg3.Tailscale))
}

// TestLoadPreservesProcessEnvironmentAuthKey verifies the only cached value
// is the auth key sourced from the process environment (TS_AUTHKEY), which the
// first resolution unsets. Labels must never be masked by the cache.
func TestLoadPreservesProcessEnvironmentAuthKey(t *testing.T) {
	t.Setenv("TS_AUTHKEY", "env-auth-key")
	stubHostname(t, "tsbridge123")

	mockClient := newMockDockerClient()
	// No credential labels at all: the key comes solely from process env.
	self := tsbridgeContainerWithLabels("tsbridge123", map[string]string{
		"tsbridge.tailscale.state_dir": "/var/lib/tsbridge",
	})
	mockClient.containers = []container.Summary{self}

	provider := &Provider{client: mockClient, labelPrefix: "tsbridge"}

	cfg1, err := provider.Load(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "env-auth-key", cfg1.Tailscale.AuthKey.Value())

	// The first resolution unsets TS_AUTHKEY; the second load must still
	// report the same configuration without spurious differences.
	cfg2, err := provider.Load(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "env-auth-key", cfg2.Tailscale.AuthKey.Value())
	assert.True(t, config.TailscaleEqual(cfg1.Tailscale, cfg2.Tailscale),
		"process-env credential fallback must not look like a rotation")
	assert.True(t, provider.configEqual(cfg1, cfg2))
}

// TestLoadProcessEnvFallbackDoesNotMaskLabelRotation verifies an inline label
// credential always wins over the process-environment fallback cache.
func TestLoadProcessEnvFallbackDoesNotMaskLabelRotation(t *testing.T) {
	t.Setenv("TS_AUTHKEY", "env-auth-key")
	stubHostname(t, "tsbridge123")

	mockClient := newMockDockerClient()
	self := tsbridgeContainerWithLabels("tsbridge123", map[string]string{
		"tsbridge.tailscale.state_dir": "/var/lib/tsbridge",
	})
	mockClient.containers = []container.Summary{self}

	provider := &Provider{client: mockClient, labelPrefix: "tsbridge"}

	cfg1, err := provider.Load(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "env-auth-key", cfg1.Tailscale.AuthKey.Value())

	// A label-declared rotation takes effect even though the first credential
	// came from the process environment.
	self.Labels["tsbridge.tailscale.auth_key"] = "rotated-label-key"
	cfg2, err := provider.Load(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "rotated-label-key", cfg2.Tailscale.AuthKey.Value())
	assert.False(t, config.TailscaleEqual(cfg1.Tailscale, cfg2.Tailscale))
}

// TestProviderConfigEqualDetectsTailscaleChanges covers the Tailscale arm of
// configEqual directly.
func TestProviderConfigEqualDetectsTailscaleChanges(t *testing.T) {
	provider := &Provider{}

	base := &config.Config{
		Tailscale: config.Tailscale{
			AuthKey:    config.RedactedString("key-a"),
			ControlURL: "https://control-a.example.net",
			StateDir:   "/var/lib/tsbridge-a",
		},
		Services: []config.Service{
			{Name: "svc", BackendAddr: "localhost:8080"},
		},
	}

	t.Run("identical tailscale config is equal", func(t *testing.T) {
		other := *base
		other.Tailscale = base.Tailscale
		assert.True(t, provider.configEqual(base, &other))
	})

	t.Run("rotated auth key detected", func(t *testing.T) {
		other := *base
		other.Tailscale.AuthKey = config.RedactedString("key-b")
		assert.False(t, provider.configEqual(base, &other))
	})

	t.Run("rotated control url detected", func(t *testing.T) {
		other := *base
		other.Tailscale.ControlURL = "https://control-b.example.net"
		assert.False(t, provider.configEqual(base, &other))
	})

	t.Run("rotated state dir detected", func(t *testing.T) {
		other := *base
		other.Tailscale.StateDir = "/var/lib/tsbridge-b"
		assert.False(t, provider.configEqual(base, &other))
	})

	t.Run("service-only change still detected", func(t *testing.T) {
		other := *base
		other.Services = []config.Service{{Name: "svc", BackendAddr: "localhost:9090"}}
		assert.False(t, provider.configEqual(base, &other))
	})
}
