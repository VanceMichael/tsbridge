// Package config handles configuration parsing and validation for tsbridge.
package config

import (
	"maps"
	"slices"

	"github.com/google/go-cmp/cmp"
)

// serviceComparerOptions builds the cmp options shared by all service
// comparisons. Tags are treated as an unordered set, while ordered header
// removal lists and header maps are compared element-wise.
func serviceComparerOptions() []cmp.Option {
	stringSliceComparer := cmp.Comparer(slices.Equal[[]string])

	tagsComparer := cmp.Comparer(func(x, y []string) bool {
		xCopy := slices.Clone(x)
		yCopy := slices.Clone(y)
		slices.Sort(xCopy)
		slices.Sort(yCopy)
		return slices.Equal(xCopy, yCopy)
	})

	stringMapComparer := cmp.Comparer(maps.Equal[map[string]string, map[string]string])

	return []cmp.Option{
		cmp.FilterPath(func(p cmp.Path) bool {
			return p.String() == "Tags"
		}, tagsComparer),
		cmp.FilterPath(func(p cmp.Path) bool {
			field := p.String()
			return field == "RemoveUpstream" || field == "RemoveDownstream"
		}, stringSliceComparer),
		cmp.FilterPath(func(p cmp.Path) bool {
			field := p.String()
			return field == "UpstreamHeaders" || field == "DownstreamHeaders"
		}, stringMapComparer),
	}
}

// ServiceConfigEqual compares two service configurations and returns true if they are equal.
// This function is used to determine if a service needs to be restarted when configuration changes.
// It compares all fields that would require a service restart if changed.
func ServiceConfigEqual(a, b Service) bool {
	return cmp.Equal(a, b, serviceComparerOptions()...)
}

// globallyInheritedServiceFields are Service fields that Normalize copies from
// the global section when a service does not set an explicit override. A
// change confined to [Global] therefore shows up in the normalized service
// configurations even though the service itself was not edited. Comparisons
// that need to tell a global-only generation apart from real service
// deltas must ignore exactly these fields.
var globallyInheritedServiceFields = map[string]bool{
	"StartupTimeout":        true,
	"ReadHeaderTimeout":     true,
	"WriteTimeout":          true,
	"IdleTimeout":           true,
	"ResponseHeaderTimeout": true,
	"AccessLog":             true,
	"FlushInterval":         true,
	"MaxRequestBodySize":    true,
}

// ServiceShapeEqual reports whether two service configurations define the
// same running service, ignoring fields that services inherit from the
// global section during normalization. It is used to recognize a
// global-only configuration generation: when every service is shape-equal
// to its previous self, no tsnet listener has to be recreated and the new
// global policy can be installed by atomically rebuilding handlers.
func ServiceShapeEqual(a, b Service) bool {
	opts := append(serviceComparerOptions(), cmp.FilterPath(func(p cmp.Path) bool {
		// p.String() for a struct field comparison is the field name.
		return globallyInheritedServiceFields[p.String()]
	}, cmp.Ignore()))
	return cmp.Equal(a, b, opts...)
}

// servicesByKey indexes services by name.
func servicesByKey(services []Service) map[string]Service {
	m := make(map[string]Service, len(services))
	for _, svc := range services {
		m[svc.Name] = svc
	}
	return m
}

// compareServiceSets compares two service slices as name-keyed sets using
// the provided per-service comparison.
func compareServiceSets(a, b []Service, equal func(Service, Service) bool) bool {
	if len(a) != len(b) {
		return false
	}
	bServices := servicesByKey(b)
	for _, aSvc := range a {
		bSvc, ok := bServices[aSvc.Name]
		if !ok || !equal(aSvc, bSvc) {
			return false
		}
	}
	return true
}

// ServicesEqual reports whether both slices contain the same services,
// comparing every normalized field. Service order is irrelevant.
func ServicesEqual(a, b []Service) bool {
	return compareServiceSets(a, b, ServiceConfigEqual)
}

// ServicesShapeEqual reports whether both slices describe the same service
// set using [ServiceShapeEqual] for each matching name.
func ServicesShapeEqual(a, b []Service) bool {
	return compareServiceSets(a, b, ServiceShapeEqual)
}

// GlobalEqual reports whether two global configuration sections are
// equivalent. Slice fields (e.g. trusted_proxies) are compared in order,
// matching how they are parsed from configuration.
func GlobalEqual(a, b Global) bool {
	return cmp.Equal(a, b)
}

// Equal reports whether two configurations carry the same global policy and
// the same service set. The Tailscale section is excluded because its values
// are resolved once at startup (and cached by the Docker provider) and are
// not part of any reloadable generation.
func Equal(a, b *Config) bool {
	if a == nil || b == nil {
		return a == b
	}
	return GlobalEqual(a.Global, b.Global) && ServicesEqual(a.Services, b.Services)
}
