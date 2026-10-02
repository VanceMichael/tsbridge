package app

import (
	"context"
	"log/slog"
	"time"

	"github.com/jtdowney/tsbridge/internal/config"
	tserrors "github.com/jtdowney/tsbridge/internal/errors"
	"github.com/jtdowney/tsbridge/internal/metrics"
)

// metricsGenerationShutdownTimeout bounds how long a replaced metrics server
// waits for in-flight scrapes when a generation retires it.
const metricsGenerationShutdownTimeout = 5 * time.Second

// applyGlobalGeneration installs a new process configuration generation.
//
// All candidate resources (service handlers and, when changed, the Prometheus
// listener) are built and verified before anything running is touched. The
// commit then publishes every service handler and the metrics endpoint as one
// generation, and only afterwards does ReloadConfig publish the new config.
//
// On failure during preparation every candidate resource is released, the
// previous generation keeps serving in full, and the error is returned so the
// caller does not record the candidate as current. The same candidate can be
// retried once the fault (e.g. an occupied metrics port) clears.
//
// The caller must hold a.mu, which also serializes concurrent generations.
func (a *App) applyGlobalGeneration(oldCfg, newCfg *config.Config) error {
	slog.Info("global configuration generation preparing",
		"old_metrics_addr", metricsAddrOrDisabled(oldCfg),
		"new_metrics_addr", metricsAddrOrDisabled(newCfg))

	// Ensure metrics plumbing exists before building candidate handlers so
	// that a generation enabling metrics produces handlers with the metrics
	// middleware. If metrics stays disabled this is a no-op; attaching a
	// collector never affects already-running handlers.
	if newCfg.Global.MetricsAddr != "" {
		if _, err := a.ensureMetricsRegistry(); err != nil {
			return err
		}
	}

	// Phase 1: build the complete set of candidate service handlers without
	// mutating any running state.
	candidates, err := a.registry.BuildGenerationHandlers(newCfg)
	if err != nil {
		slog.Error("configuration generation aborted while building handlers", "error", err)
		return tserrors.WrapConfig(err, "preparing configuration generation")
	}

	// Phase 2: bind the candidate metrics listener (if metrics changed)
	// before committing. A bind failure here aborts the generation while the
	// old scrape endpoint and proxy policy keep serving.
	metricsCandidate, metricsChanged, err := a.prepareMetricsGeneration(newCfg)
	if err != nil {
		slog.Error("configuration generation aborted while preparing metrics", "error", err)
		a.registry.DiscardGenerationHandlers(candidates)
		return err
	}

	// Phase 3: every candidate is ready — commit the generation. First swap
	// all service handlers atomically, then publish the new metrics endpoint
	// and retire the previous one. External observers can only see the
	// complete old generation or the complete new one.
	a.registry.CommitGeneration(newCfg, candidates)
	if metricsChanged {
		a.commitMetricsGeneration(metricsCandidate)
	}

	// Phase 4: reconcile service-set deltas (new/removed services and
	// per-service edits requiring listener recreation) via the dynamic
	// management path. For a global-only generation this is empty.
	if deltas := serviceDeltasForGeneration(oldCfg, newCfg); deltas.nonEmpty() {
		deltas.apply(a)
	}

	slog.Info("global configuration generation committed",
		"metrics_addr", metricsAddrOrDisabled(newCfg))
	return nil
}

// metricsAddrOrDisabled renders the metrics address for logs.
func metricsAddrOrDisabled(cfg *config.Config) string {
	if cfg.Global.MetricsAddr == "" {
		return "disabled"
	}
	return cfg.Global.MetricsAddr
}

// metricsGeneration describes the candidate metrics endpoint for a
// configuration generation.
type metricsGeneration struct {
	server  *metrics.Server
	enabled bool // false means the generation disables metrics
}

// prepareMetricsGeneration builds and binds the candidate metrics server when
// the listen address or read-header timeout changes. It returns
// (nil, false, nil) when the current server already matches the candidate.
// The returned listener is bound but not yet serving.
func (a *App) prepareMetricsGeneration(newCfg *config.Config) (*metricsGeneration, bool, error) {
	desiredAddr := newCfg.Global.MetricsAddr

	if desiredAddr == "" {
		// Candidate disables metrics: only a change if one is currently running.
		if a.metricsServer == nil {
			return nil, false, nil
		}
		return &metricsGeneration{enabled: false}, true, nil
	}

	desiredTimeout := metricsReadHeaderTimeout(newCfg)
	if a.metricsServer != nil &&
		a.cfg.Global.MetricsAddr == desiredAddr &&
		metricsReadHeaderTimeout(a.cfg) == desiredTimeout {
		// Endpoint and timeout unchanged: keep the existing listener.
		return nil, false, nil
	}

	reg, err := a.ensureMetricsRegistry()
	if err != nil {
		return nil, false, err
	}

	candidate := metrics.NewServer(desiredAddr, reg, desiredTimeout)
	if err := candidate.Listen(); err != nil {
		candidate.Discard()
		return nil, false, tserrors.WrapResource(err, "binding candidate metrics listener")
	}
	return &metricsGeneration{server: candidate, enabled: true}, true, nil
}

// commitMetricsGeneration publishes the candidate metrics endpoint and shuts
// the previous one down. The candidate starts serving before the old endpoint
// closes so scrapes never lose availability, and exactly one listener remains
// afterwards.
func (a *App) commitMetricsGeneration(gen *metricsGeneration) {
	old := a.metricsServer
	if gen == nil || !gen.enabled {
		a.metricsServer = nil
	} else {
		gen.server.Serve()
		a.metricsServer = gen.server
		slog.Info("metrics server generation committed", "address", gen.server.Addr())
	}

	if old != nil {
		ctx, cancel := context.WithTimeout(context.Background(), metricsGenerationShutdownTimeout)
		defer cancel()
		if err := old.Shutdown(ctx); err != nil {
			slog.Warn("failed to shutdown previous metrics server", "error", err)
		}
	}
}

// currentMetricsServer returns the live metrics server, safe against a
// concurrent generation swap.
func (a *App) currentMetricsServer() *metrics.Server {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.metricsServer
}

// generationServiceDeltas lists services that keep their identity across the
// generation but require the dynamic management path (listener/tsnet
// recreation) because their shape changed, plus adds and removes.
type generationServiceDeltas struct {
	remove []string
	add    []config.Service
	update []config.Service
}

func (d generationServiceDeltas) nonEmpty() bool {
	return len(d.remove) > 0 || len(d.add) > 0 || len(d.update) > 0
}

// serviceDeltasForGeneration computes add/remove/update deltas using shape
// equality, so services whose only differences are values inherited from the
// (already switched) global section are not needlessly restarted.
func serviceDeltasForGeneration(oldCfg, newCfg *config.Config) generationServiceDeltas {
	var deltas generationServiceDeltas

	oldByName := make(map[string]config.Service, len(oldCfg.Services))
	for _, svc := range oldCfg.Services {
		oldByName[svc.Name] = svc
	}

	newNames := make(map[string]bool, len(newCfg.Services))
	for _, svc := range newCfg.Services {
		newNames[svc.Name] = true
		if oldSvc, ok := oldByName[svc.Name]; ok {
			if !config.ServiceShapeEqual(oldSvc, svc) {
				deltas.update = append(deltas.update, svc)
			}
		} else {
			deltas.add = append(deltas.add, svc)
		}
	}

	for name := range oldByName {
		if !newNames[name] {
			deltas.remove = append(deltas.remove, name)
		}
	}

	return deltas
}

// apply executes the deltas using the existing dynamic service operations.
// Like reloadConfigWithRegistry it is best effort: individual failures are
// logged and counted but never unwind the already committed global policy.
func (d generationServiceDeltas) apply(a *App) {
	failures := 0

	for _, name := range d.remove {
		if err := a.registry.RemoveService(name); err != nil {
			slog.Error("generation: failed to remove service", "service", name, "error", err)
			failures++
		}
	}

	for _, svc := range d.add {
		if err := a.registry.AddService(svc); err != nil {
			slog.Error("generation: failed to add service", "service", svc.Name, "error", err)
			failures++
		}
	}

	for _, svc := range d.update {
		if err := a.registry.UpdateService(svc.Name, svc); err != nil {
			slog.Error("generation: failed to update service", "service", svc.Name, "error", err)
			failures++
		}
	}

	if failures > 0 {
		slog.Warn("generation completed with service reconciliation errors",
			"failed_operations", failures,
			"removes", len(d.remove),
			"adds", len(d.add),
			"updates", len(d.update))
	}
}
