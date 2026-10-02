package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jtdowney/tsbridge/internal/config"
	tserrors "github.com/jtdowney/tsbridge/internal/errors"
	"github.com/jtdowney/tsbridge/internal/service"
)

// rotationOperationTimeout bounds best-effort shutdown operations performed
// while committing or rolling back a tailscale generation rotation.
const rotationOperationTimeout = 10 * time.Second

// errAppStopped is the sentinel returned when a reload races with shutdown.
var errAppStopped = tserrors.NewResourceError("application is shutting down; reload rejected")

// serviceRegistryOps is the minimal interface for dynamic config reloads.
// It supports adding, removing, and updating services at runtime.
type serviceRegistryOps interface {
	AddService(svcCfg config.Service) error
	RemoveService(name string) error
	UpdateService(name string, newCfg config.Service) error
}

// reloadConfigWithRegistry reloads services in the registry to match newCfg.
// Removes, adds, and updates services as needed, in that order.
// Continues on errors and joins every failure in the returned error.
func reloadConfigWithRegistry(oldCfg, newCfg *config.Config, registry serviceRegistryOps) error {
	toRemove := findServicesToRemove(oldCfg, newCfg)
	toAdd := findServicesToAdd(oldCfg, newCfg)
	toUpdate := findServicesToUpdate(oldCfg, newCfg)

	if len(toRemove) > 0 || len(toAdd) > 0 || len(toUpdate) > 0 {
		slog.Info("configuration changes detected",
			"services_to_remove", len(toRemove),
			"services_to_add", len(toAdd),
			"services_to_update", len(toUpdate))
	} else {
		slog.Info("no service configuration changes detected")
		return nil
	}

	var failures []error
	successful := 0
	removeFailures := 0
	addFailures := 0
	updateFailures := 0

	for _, name := range toRemove {
		if err := registry.RemoveService(name); err != nil {
			slog.Error("failed to remove service",
				"service", name,
				"error", err,
				"operation", "reload_remove")
			failures = append(failures, fmt.Errorf("remove service %q: %w", name, err))
			removeFailures++
		} else {
			slog.Info("removed service during reload",
				"service", name,
				"operation", "reload_remove")
			successful++
		}
	}

	for _, svc := range toAdd {
		if err := registry.AddService(svc); err != nil {
			slog.Error("failed to add service",
				"service", svc.Name,
				"error", err,
				"operation", "reload_add",
				"backend", svc.BackendAddr)
			failures = append(failures, fmt.Errorf("add service %q: %w", svc.Name, err))
			addFailures++
		} else {
			slog.Info("added service during reload",
				"service", svc.Name,
				"operation", "reload_add",
				"backend", svc.BackendAddr)
			successful++
		}
	}

	for _, svc := range toUpdate {
		if err := registry.UpdateService(svc.Name, svc); err != nil {
			slog.Error("failed to update service",
				"service", svc.Name,
				"error", err,
				"operation", "reload_update",
				"backend", svc.BackendAddr)
			failures = append(failures, fmt.Errorf("update service %q: %w", svc.Name, err))
			updateFailures++
		} else {
			slog.Info("updated service during reload",
				"service", svc.Name,
				"operation", "reload_update",
				"backend", svc.BackendAddr)
			successful++
		}
	}

	if len(failures) > 0 {
		slog.Warn("configuration reload completed with errors",
			"successful_operations", successful,
			"failed_operations", len(failures),
			"add_errors", addFailures,
			"remove_errors", removeFailures,
			"update_errors", updateFailures)
	} else {
		slog.Info("configuration reload completed successfully",
			"operations", successful)
	}

	return errors.Join(failures...)
}

// rotateTailscaleGeneration coordinates migration to a fresh tailscale
// Server generation after the Tailscale control-plane configuration changes.
//
// Invariants:
//
//   - The candidate (credentials, control URL, state directory) is validated
//     by Preflight before any online service is interrupted. An invalid
//     candidate is discarded and never affects outgoing nodes.
//   - Services shared by both configs are cut over one at a time: the old
//     node is stopped, then the candidate node is brought up and starts
//     listening. Two tsnet nodes cannot share one on-disk state directory.
//   - If any candidate tsnet Up, authentication or Listen fails, every
//     candidate node/listener is torn down and all interrupted old services
//     are restarted on the outgoing generation with their original
//     configuration; no service is deleted and no orphan node is left.
//   - On success the active registry/server pointers are switched to the new
//     generation (Registry, services, tsnet nodes and listeners share one
//     owner), the outgoing generation is retired, and subsequent service
//     additions/updates only use the new generation.
//
// The caller (ReloadConfig) holds a.mu, so rotations and ordinary reload
// events are serialized.
func (a *App) rotateTailscaleGeneration(ctx context.Context, oldCfg, newCfg *config.Config) error {
	slog.Info("tailscale control-plane configuration changed; preparing new generation",
		"outgoing_generation", a.generation)

	candidateServer, err := newTailscaleServer(newCfg.Tailscale, a.tsNetFactory)
	if err != nil {
		return fmt.Errorf("creating candidate tailscale generation: %w", err)
	}

	// Validate credentials, control plane and state directory while every
	// online service is still served by the outgoing generation.
	if err := candidateServer.Preflight(ctx); err != nil {
		_ = candidateServer.Close()
		return fmt.Errorf("candidate tailscale configuration rejected, keeping generation %d: %w", a.generation, err)
	}

	candidateRegistry := service.NewRegistry(newCfg, candidateServer)
	if collector := a.registry.GetMetricsCollector(); collector != nil {
		candidateRegistry.SetMetricsCollector(collector)
	}

	oldServices := make(map[string]config.Service, len(oldCfg.Services))
	for _, svc := range oldCfg.Services {
		oldServices[svc.Name] = svc
	}

	// interruptedOld records the original configuration of services that were
	// stopped on the outgoing generation; they are restarted exactly as they
	// were if the migration rolls back.
	var interruptedOld []config.Service

	rollback := func(cause error) error {
		slog.Error("aborting tailscale generation migration; restoring previous generation",
			"outgoing_generation", a.generation,
			"services_to_restore", len(interruptedOld),
			"error", cause)

		closeCtx, cancel := context.WithTimeout(context.Background(), rotationOperationTimeout)
		if err := candidateRegistry.Shutdown(closeCtx); err != nil {
			slog.Error("failed to stop candidate services during rollback", "error", err)
		}
		cancel()
		// Close every candidate tsnet node before restarting old nodes; when
		// both generations share a state directory this also releases the
		// on-disk node locks.
		if err := candidateServer.Close(); err != nil {
			slog.Error("failed to close candidate tailscale nodes during rollback", "error", err)
		}

		var restoreErrs []error
		for _, oldSvc := range interruptedOld {
			if err := a.registry.AddService(oldSvc); err != nil {
				slog.Error("failed to restore service on previous tailscale generation",
					"service", oldSvc.Name, "error", err)
				restoreErrs = append(restoreErrs, fmt.Errorf("restore service %q: %w", oldSvc.Name, err))
				continue
			}
			slog.Info("restored service on previous tailscale generation", "service", oldSvc.Name)
		}

		if len(restoreErrs) > 0 {
			return errors.Join(
				fmt.Errorf("tailscale generation migration failed: %w", cause),
				fmt.Errorf("partial restoration of previous generation: %w", errors.Join(restoreErrs...)),
			)
		}
		return fmt.Errorf("tailscale generation migration failed and previous generation restored: %w", cause)
	}

	// Cut over services present in both generations.
	for _, newSvc := range newCfg.Services {
		oldSvc, shared := oldServices[newSvc.Name]
		if !shared {
			continue
		}
		name := newSvc.Name

		if _, running := a.registry.GetService(name); running {
			if err := a.registry.RemoveService(name); err != nil {
				return rollback(fmt.Errorf("stop service %q on outgoing generation: %w", name, err))
			}
			interruptedOld = append(interruptedOld, oldSvc)
		}

		if err := candidateRegistry.AddService(newSvc); err != nil {
			return rollback(fmt.Errorf("start service %q on candidate generation: %w", name, err))
		}
		slog.Info("migrated service to candidate tailscale generation", "service", name)
	}

	// Start services that exist only in the new configuration.
	for _, newSvc := range findServicesToAdd(oldCfg, newCfg) {
		if err := candidateRegistry.AddService(newSvc); err != nil {
			return rollback(fmt.Errorf("add service %q on candidate generation: %w", newSvc.Name, err))
		}
	}

	// Commit: Registry, every Service, tsnet node and listener now belong to
	// the new generation.
	outgoingServer := a.tsServer
	outgoingRegistry := a.registry
	a.generation++
	a.registry = candidateRegistry
	a.tsServer = candidateServer
	slog.Info("committed new tailscale generation",
		"generation", a.generation,
		"services", len(newCfg.Services))

	// Retire the outgoing generation: stop services absent from the new
	// config and close every old tsnet node. Afterwards new service events
	// can only be served by the committed generation.
	retireCtx, cancel := context.WithTimeout(context.Background(), rotationOperationTimeout)
	defer cancel()
	if err := outgoingRegistry.Shutdown(retireCtx); err != nil {
		slog.Error("retiring previous generation: failed to stop outgoing services",
			"generation", a.generation-1, "error", err)
	}
	if err := outgoingServer.Close(); err != nil {
		slog.Error("retiring previous generation: failed to close outgoing tsnet nodes",
			"generation", a.generation-1, "error", err)
	}
	slog.Info("previous tailscale generation retired", "generation", a.generation-1)
	return nil
}
