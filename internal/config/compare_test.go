package config

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestServiceConfigEqual(t *testing.T) {
	tests := []struct {
		name     string
		a        Service
		b        Service
		expected bool
	}{
		{
			name: "identical basic configs",
			a: Service{
				Name:        "test-service",
				BackendAddr: "http://localhost:8080",
				TLSMode:     "strict",
			},
			b: Service{
				Name:        "test-service",
				BackendAddr: "http://localhost:8080",
				TLSMode:     "strict",
			},
			expected: true,
		},
		{
			name: "different names",
			a: Service{
				Name:        "service-a",
				BackendAddr: "http://localhost:8080",
			},
			b: Service{
				Name:        "service-b",
				BackendAddr: "http://localhost:8080",
			},
			expected: false,
		},
		{
			name: "different backend addresses",
			a: Service{
				Name:        "test-service",
				BackendAddr: "http://localhost:8080",
			},
			b: Service{
				Name:        "test-service",
				BackendAddr: "http://localhost:8081",
			},
			expected: false,
		},
		{
			name: "different listen addrs",
			a: Service{
				Name:        "test-service",
				BackendAddr: "http://localhost:8080",
				ListenAddr:  ":443",
			},
			b: Service{
				Name:        "test-service",
				BackendAddr: "http://localhost:8080",
				ListenAddr:  ":8443",
			},
			expected: false,
		},
		{
			name: "same listen addrs",
			a: Service{
				Name:        "test-service",
				BackendAddr: "http://localhost:8080",
				ListenAddr:  ":443",
			},
			b: Service{
				Name:        "test-service",
				BackendAddr: "http://localhost:8080",
				ListenAddr:  ":443",
			},
			expected: true,
		},
		{
			name: "different TLS modes",
			a: Service{
				Name:        "test-service",
				BackendAddr: "http://localhost:8080",
				TLSMode:     "strict",
			},
			b: Service{
				Name:        "test-service",
				BackendAddr: "http://localhost:8080",
				TLSMode:     "",
			},
			expected: false,
		},
		{
			name: "different funnel enabled state",
			a: Service{
				Name:          "test-service",
				BackendAddr:   "http://localhost:8080",
				FunnelEnabled: new(true),
			},
			b: Service{
				Name:          "test-service",
				BackendAddr:   "http://localhost:8080",
				FunnelEnabled: new(false),
			},
			expected: false,
		},
		{
			name: "equal pointer values at different addresses",
			a: Service{
				Name:          "test-service",
				BackendAddr:   "http://localhost:8080",
				FunnelEnabled: new(true),
			},
			b: Service{
				Name:          "test-service",
				BackendAddr:   "http://localhost:8080",
				FunnelEnabled: new(true),
			},
			expected: true,
		},
		{
			name: "nil vs non-nil funnel enabled",
			a: Service{
				Name:          "test-service",
				BackendAddr:   "http://localhost:8080",
				FunnelEnabled: nil,
			},
			b: Service{
				Name:          "test-service",
				BackendAddr:   "http://localhost:8080",
				FunnelEnabled: new(false),
			},
			expected: false,
		},
		{
			name: "both nil funnel enabled",
			a: Service{
				Name:          "test-service",
				BackendAddr:   "http://localhost:8080",
				FunnelEnabled: nil,
			},
			b: Service{
				Name:          "test-service",
				BackendAddr:   "http://localhost:8080",
				FunnelEnabled: nil,
			},
			expected: true,
		},
		{
			name: "different oauth preauthorized setting",
			a: Service{
				Name:               "test-service",
				BackendAddr:        "http://localhost:8080",
				OAuthPreauthorized: new(true),
			},
			b: Service{
				Name:               "test-service",
				BackendAddr:        "http://localhost:8080",
				OAuthPreauthorized: new(false),
			},
			expected: false,
		},
		{
			name: "nil vs non-nil oauth preauthorized",
			a: Service{
				Name:               "test-service",
				BackendAddr:        "http://localhost:8080",
				OAuthPreauthorized: nil,
			},
			b: Service{
				Name:               "test-service",
				BackendAddr:        "http://localhost:8080",
				OAuthPreauthorized: new(false),
			},
			expected: false,
		},
		{
			name: "both nil oauth preauthorized",
			a: Service{
				Name:               "test-service",
				BackendAddr:        "http://localhost:8080",
				OAuthPreauthorized: nil,
			},
			b: Service{
				Name:               "test-service",
				BackendAddr:        "http://localhost:8080",
				OAuthPreauthorized: nil,
			},
			expected: true,
		},
		{
			name: "different ephemeral setting",
			a: Service{
				Name:        "test-service",
				BackendAddr: "http://localhost:8080",
				Ephemeral:   true,
			},
			b: Service{
				Name:        "test-service",
				BackendAddr: "http://localhost:8080",
				Ephemeral:   false,
			},
			expected: false,
		},
		{
			name: "different tags",
			a: Service{
				Name:        "test-service",
				BackendAddr: "http://localhost:8080",
				Tags:        []string{"prod", "api"},
			},
			b: Service{
				Name:        "test-service",
				BackendAddr: "http://localhost:8080",
				Tags:        []string{"dev", "api"},
			},
			expected: false,
		},
		{
			name: "same tags different order",
			a: Service{
				Name:        "test-service",
				BackendAddr: "http://localhost:8080",
				Tags:        []string{"api", "prod"},
			},
			b: Service{
				Name:        "test-service",
				BackendAddr: "http://localhost:8080",
				Tags:        []string{"prod", "api"},
			},
			expected: true, // Order doesn't matter for tags
		},
		{
			name: "different upstream headers",
			a: Service{
				Name:        "test-service",
				BackendAddr: "http://localhost:8080",
				UpstreamHeaders: map[string]string{
					"X-Custom": "value1",
				},
			},
			b: Service{
				Name:        "test-service",
				BackendAddr: "http://localhost:8080",
				UpstreamHeaders: map[string]string{
					"X-Custom": "value2",
				},
			},
			expected: false,
		},
		{
			name: "different downstream headers",
			a: Service{
				Name:        "test-service",
				BackendAddr: "http://localhost:8080",
				DownstreamHeaders: map[string]string{
					"X-Response": "value1",
				},
			},
			b: Service{
				Name:        "test-service",
				BackendAddr: "http://localhost:8080",
				DownstreamHeaders: map[string]string{
					"X-Response": "value2",
				},
			},
			expected: false,
		},
		{
			name: "different response header timeouts",
			a: Service{
				Name:                  "test-service",
				BackendAddr:           "http://localhost:8080",
				ResponseHeaderTimeout: new(30 * time.Second),
			},
			b: Service{
				Name:                  "test-service",
				BackendAddr:           "http://localhost:8080",
				ResponseHeaderTimeout: new(60 * time.Second),
			},
			expected: false,
		},
		{
			name: "different access log settings",
			a: Service{
				Name:        "test-service",
				BackendAddr: "http://localhost:8080",
				AccessLog:   new(true),
			},
			b: Service{
				Name:        "test-service",
				BackendAddr: "http://localhost:8080",
				AccessLog:   new(false),
			},
			expected: false,
		},
		{
			name: "nil vs empty maps",
			a: Service{
				Name:            "test-service",
				BackendAddr:     "http://localhost:8080",
				UpstreamHeaders: nil,
			},
			b: Service{
				Name:            "test-service",
				BackendAddr:     "http://localhost:8080",
				UpstreamHeaders: map[string]string{},
			},
			expected: true, // Treat nil and empty maps as equal
		},
		{
			name: "nil vs empty slices",
			a: Service{
				Name:        "test-service",
				BackendAddr: "http://localhost:8080",
				Tags:        nil,
			},
			b: Service{
				Name:        "test-service",
				BackendAddr: "http://localhost:8080",
				Tags:        []string{},
			},
			expected: true, // Treat nil and empty slices as equal
		},
		{
			name: "different whois enabled",
			a: Service{
				Name:         "test-service",
				BackendAddr:  "http://localhost:8080",
				WhoisEnabled: new(true),
			},
			b: Service{
				Name:         "test-service",
				BackendAddr:  "http://localhost:8080",
				WhoisEnabled: new(false),
			},
			expected: false,
		},
		{
			name: "different remove upstream headers",
			a: Service{
				Name:           "test-service",
				BackendAddr:    "http://localhost:8080",
				RemoveUpstream: []string{"X-Header-1"},
			},
			b: Service{
				Name:           "test-service",
				BackendAddr:    "http://localhost:8080",
				RemoveUpstream: []string{"X-Header-2"},
			},
			expected: false,
		},
		{
			name: "different remove downstream headers",
			a: Service{
				Name:             "test-service",
				BackendAddr:      "http://localhost:8080",
				RemoveDownstream: []string{"X-Response-1"},
			},
			b: Service{
				Name:             "test-service",
				BackendAddr:      "http://localhost:8080",
				RemoveDownstream: []string{"X-Response-2"},
			},
			expected: false,
		},
		{
			name: "different flush intervals",
			a: Service{
				Name:          "test-service",
				BackendAddr:   "http://localhost:8080",
				FlushInterval: new(1 * time.Second),
			},
			b: Service{
				Name:          "test-service",
				BackendAddr:   "http://localhost:8080",
				FlushInterval: new(2 * time.Second),
			},
			expected: false,
		},
		{
			name: "different read header timeouts",
			a: Service{
				Name:              "test-service",
				BackendAddr:       "http://localhost:8080",
				ReadHeaderTimeout: new(10 * time.Second),
			},
			b: Service{
				Name:              "test-service",
				BackendAddr:       "http://localhost:8080",
				ReadHeaderTimeout: new(20 * time.Second),
			},
			expected: false,
		},
		{
			name: "different write timeouts",
			a: Service{
				Name:         "test-service",
				BackendAddr:  "http://localhost:8080",
				WriteTimeout: new(30 * time.Second),
			},
			b: Service{
				Name:         "test-service",
				BackendAddr:  "http://localhost:8080",
				WriteTimeout: new(60 * time.Second),
			},
			expected: false,
		},
		{
			name: "different idle timeouts",
			a: Service{
				Name:        "test-service",
				BackendAddr: "http://localhost:8080",
				IdleTimeout: new(120 * time.Second),
			},
			b: Service{
				Name:        "test-service",
				BackendAddr: "http://localhost:8080",
				IdleTimeout: new(240 * time.Second),
			},
			expected: false,
		},
		{
			name: "different whois timeouts",
			a: Service{
				Name:         "test-service",
				BackendAddr:  "http://localhost:8080",
				WhoisTimeout: new(5 * time.Second),
			},
			b: Service{
				Name:         "test-service",
				BackendAddr:  "http://localhost:8080",
				WhoisTimeout: new(10 * time.Second),
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ServiceConfigEqual(tt.a, tt.b)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// durationPtrEqual compares two time.Duration pointers
func durationPtrEqual(a, b *time.Duration) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func TestTailscaleEqual(t *testing.T) {
	base := Tailscale{
		OAuthClientID:         "client-id",
		OAuthClientSecret:     RedactedString("client-secret"),
		AuthKey:               RedactedString("auth-key"),
		StateDir:              "/var/lib/tsbridge",
		DefaultTags:           []string{"tag:server", "tag:proxy"},
		ControlURL:            "https://headscale.example.net",
		OAuthPreauthorized:    new(true),
		OAuthClientIDEnv:      "",
		OAuthClientIDFile:     "",
		OAuthClientSecretEnv:  "",
		OAuthClientSecretFile: "",
		AuthKeyEnv:            "",
		AuthKeyFile:           "",
		StateDirEnv:           "",
	}

	tests := []struct {
		name     string
		mutate   func(*Tailscale)
		expected bool
	}{
		{name: "identical", expected: true},
		{name: "default tags reorder", mutate: func(ts *Tailscale) {
			ts.DefaultTags = []string{"tag:proxy", "tag:server"}
		}, expected: true},
		{name: "rotated auth key", mutate: func(ts *Tailscale) {
			ts.AuthKey = RedactedString("rotated-key")
		}, expected: false},
		{name: "rotated oauth secret", mutate: func(ts *Tailscale) {
			ts.OAuthClientSecret = RedactedString("rotated-secret")
		}, expected: false},
		{name: "rotated oauth client id", mutate: func(ts *Tailscale) {
			ts.OAuthClientID = "other-client"
		}, expected: false},
		{name: "control url changed", mutate: func(ts *Tailscale) {
			ts.ControlURL = "https://control.other.example"
		}, expected: false},
		{name: "state dir changed", mutate: func(ts *Tailscale) {
			ts.StateDir = "/srv/tsbridge"
		}, expected: false},
		{name: "default tags changed", mutate: func(ts *Tailscale) {
			ts.DefaultTags = []string{"tag:server", "tag:edge"}
		}, expected: false},
		{name: "preauthorized changed", mutate: func(ts *Tailscale) {
			ts.OAuthPreauthorized = new(false)
		}, expected: false},
		{name: "auth key env source changed", mutate: func(ts *Tailscale) {
			ts.AuthKey = ""
			ts.AuthKeyEnv = "ROTATED_AUTHKEY"
		}, expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := base
			b := base
			if tt.mutate != nil {
				tt.mutate(&b)
			}
			assert.Equal(t, tt.expected, TailscaleEqual(a, b))
		})
	}
}

// TestTailscaleEqualCoversAllFields ensures every Tailscale field participates
// in the generation-difference comparison.
func TestTailscaleEqualCoversAllFields(t *testing.T) {
	tailscaleType := reflect.TypeFor[Tailscale]()
	comparedFields := map[string]bool{
		"OAuthClientID":         true,
		"OAuthClientIDEnv":      true,
		"OAuthClientIDFile":     true,
		"OAuthClientSecret":     true,
		"OAuthClientSecretEnv":  true,
		"OAuthClientSecretFile": true,
		"AuthKey":               true,
		"AuthKeyEnv":            true,
		"AuthKeyFile":           true,
		"StateDir":              true,
		"StateDirEnv":           true,
		"DefaultTags":           true,
		"ControlURL":            true,
		"OAuthPreauthorized":    true,
	}

	for field := range tailscaleType.Fields() {
		if !comparedFields[field.Name] {
			t.Errorf("Field %s is not compared in TailscaleEqual", field.Name)
		}
	}
}

// TestServiceConfigEqualCoversAllFields uses reflection to ensure all fields
// of the Service struct are compared in ServiceConfigEqual function.
// This test prevents bugs when new fields are added but not included in comparison.
func TestServiceConfigEqualCoversAllFields(t *testing.T) {
	// Use reflection to get all fields of Service struct
	serviceType := reflect.TypeFor[Service]()

	// List of fields we expect to be compared in ServiceConfigEqual
	comparedFields := map[string]bool{
		"Name":                  true,
		"BackendAddr":           true,
		"ListenAddr":            true,
		"WhoisEnabled":          true,
		"WhoisTimeout":          true,
		"TLSMode":               true,
		"Tags":                  true,
		"StartupTimeout":        true,
		"ReadHeaderTimeout":     true,
		"WriteTimeout":          true,
		"IdleTimeout":           true,
		"ResponseHeaderTimeout": true,
		"AccessLog":             true,
		"FunnelEnabled":         true,
		"Ephemeral":             true,
		"FlushInterval":         true,
		"InsecureSkipVerify":    true,
		"UpstreamHeaders":       true,
		"DownstreamHeaders":     true,
		"RemoveUpstream":        true,
		"RemoveDownstream":      true,
		"MaxRequestBodySize":    true,
		"OAuthPreauthorized":    true,
	}

	// Check that all struct fields are in our comparison
	for field := range serviceType.Fields() {
		if !comparedFields[field.Name] {
			t.Errorf("Field %s is not compared in ServiceConfigEqual", field.Name)
		}
	}

	// Also check that we don't have extra fields in our expected list
	// that don't actually exist in the struct
	actualFields := make(map[string]bool)
	for field := range serviceType.Fields() {
		actualFields[field.Name] = true
	}

	for fieldName := range comparedFields {
		if !actualFields[fieldName] {
			t.Errorf("Expected field %s does not exist in Service struct", fieldName)
		}
	}
}
