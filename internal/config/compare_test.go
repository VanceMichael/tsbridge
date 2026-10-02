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

func TestGlobalEqual(t *testing.T) {
	t.Run("empty globals are equal", func(t *testing.T) {
		assert.True(t, GlobalEqual(Global{}, Global{}))
	})

	t.Run("identical global sections are equal", func(t *testing.T) {
		d := 5 * time.Second
		enabled := true
		size := int64(1024)
		a := Global{
			MetricsAddr:              ":9090",
			TrustedProxies:           []string{"10.0.0.0/8", "1.2.3.4"},
			AccessLog:                &enabled,
			MaxRequestBodySize:       &size,
			DialTimeout:              &d,
			MetricsReadHeaderTimeout: &d,
		}
		b := a
		assert.True(t, GlobalEqual(a, b))
	})

	t.Run("metrics addr differs", func(t *testing.T) {
		assert.False(t, GlobalEqual(Global{MetricsAddr: ":9090"}, Global{MetricsAddr: ":9091"}))
	})

	t.Run("trusted proxies differ in content", func(t *testing.T) {
		assert.False(t, GlobalEqual(
			Global{TrustedProxies: []string{"10.0.0.0/8"}},
			Global{TrustedProxies: []string{"10.1.0.0/16"}},
		))
	})

	t.Run("trusted proxies differ in order", func(t *testing.T) {
		// Order is significant because it is operator-authored configuration.
		assert.False(t, GlobalEqual(
			Global{TrustedProxies: []string{"10.0.0.0/8", "1.2.3.4"}},
			Global{TrustedProxies: []string{"1.2.3.4", "10.0.0.0/8"}},
		))
	})

	t.Run("scalar pointer values differ", func(t *testing.T) {
		short := time.Second
		long := 2 * time.Second
		assert.False(t, GlobalEqual(Global{DialTimeout: &short}, Global{DialTimeout: &long}))
		assert.False(t, GlobalEqual(Global{DialTimeout: &short}, Global{}))
		assert.False(t, GlobalEqual(Global{}, Global{DialTimeout: &long}))
	})
}

func TestServiceShapeEqual(t *testing.T) {
	base := func() Service {
		return Service{
			Name:        "api",
			BackendAddr: "http://localhost:8080",
			TLSMode:     "auto",
		}
	}

	t.Run("identical services are shape equal", func(t *testing.T) {
		assert.True(t, ServiceShapeEqual(base(), base()))
	})

	t.Run("inherited field changes are ignored", func(t *testing.T) {
		a := base()
		b := base()
		timeout := 5 * time.Second
		enabled := true
		disabled := false
		size := int64(1024)
		a.StartupTimeout = &timeout
		a.ReadHeaderTimeout = &timeout
		a.WriteTimeout = &timeout
		a.IdleTimeout = &timeout
		a.ResponseHeaderTimeout = &timeout
		a.AccessLog = &enabled
		a.FlushInterval = &timeout
		a.MaxRequestBodySize = &size

		b.AccessLog = &disabled
		b.MaxRequestBodySize = nil
		assert.True(t, ServiceShapeEqual(a, b),
			"changes to globally inherited fields must not count as a service shape change")
	})

	t.Run("backend change changes the shape", func(t *testing.T) {
		a := base()
		b := base()
		b.BackendAddr = "http://localhost:9090"
		assert.False(t, ServiceShapeEqual(a, b))
	})

	t.Run("non-inherited per-service timeout still changes shape", func(t *testing.T) {
		a := base()
		b := base()
		d1 := time.Second
		d2 := 2 * time.Second
		a.WhoisTimeout = &d1
		b.WhoisTimeout = &d2
		assert.False(t, ServiceShapeEqual(a, b))
	})
}

func TestServicesShapeEqual(t *testing.T) {
	t.Run("same services with different inherited values", func(t *testing.T) {
		enabled := true
		a := []Service{{
			Name:      "api",
			AccessLog: &enabled,
		}}
		b := []Service{{
			Name: "api",
		}}
		assert.True(t, ServicesShapeEqual(a, b))
		assert.False(t, ServicesEqual(a, b), "full equality must still detect the inherited value difference")
	})

	t.Run("service added", func(t *testing.T) {
		a := []Service{{Name: "api"}}
		b := []Service{{Name: "api"}, {Name: "web"}}
		assert.False(t, ServicesShapeEqual(a, b))
	})

	t.Run("order independent", func(t *testing.T) {
		a := []Service{{Name: "api"}, {Name: "web"}}
		b := []Service{{Name: "web"}, {Name: "api"}}
		assert.True(t, ServicesShapeEqual(a, b))
		assert.True(t, ServicesEqual(a, b))
	})
}

func TestEqual(t *testing.T) {
	t.Run("nil handling", func(t *testing.T) {
		assert.True(t, Equal(nil, nil))
		assert.False(t, Equal(nil, &Config{}))
		assert.False(t, Equal(&Config{}, nil))
	})

	t.Run("global-only difference is not equal", func(t *testing.T) {
		a := &Config{
			Global:   Global{MetricsAddr: ":9090"},
			Services: []Service{{Name: "api"}},
		}
		b := &Config{
			Global:   Global{MetricsAddr: ":9091"},
			Services: []Service{{Name: "api"}},
		}
		assert.False(t, Equal(a, b))
	})

	t.Run("identical normalized configs are equal", func(t *testing.T) {
		a := &Config{
			Global:   Global{MetricsAddr: ":9090"},
			Services: []Service{{Name: "api"}, {Name: "web"}},
		}
		b := &Config{
			Global:   Global{MetricsAddr: ":9090"},
			Services: []Service{{Name: "web"}, {Name: "api"}},
		}
		assert.True(t, Equal(a, b))
	})
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
