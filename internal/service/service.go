// Package service provides service registry and management capabilities.
package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/jtdowney/tsbridge/internal/config"
	tserrors "github.com/jtdowney/tsbridge/internal/errors"
	"github.com/jtdowney/tsbridge/internal/funnel"
	"github.com/jtdowney/tsbridge/internal/metrics"
	"github.com/jtdowney/tsbridge/internal/middleware"
	"github.com/jtdowney/tsbridge/internal/proxy"
	"github.com/jtdowney/tsbridge/internal/tailscale"
)

const (
	defaultWhoisCacheSize = 1000
	defaultWhoisCacheTTL  = 5 * time.Minute
	serviceStopTimeout    = 5 * time.Second
)

// ErrRegistryClosed is returned by registry mutations after shutdown has
// started or completed. It guarantees that callers cannot create or change
// services (and therefore listeners or tsnet nodes) during shutdown, even
// when bypassing the App.
var ErrRegistryClosed = errors.New("service registry is closed")

// Registry manages all services
type Registry struct {
	config           *config.Config
	tsServer         *tailscale.Server
	services         map[string]*Service
	metricsCollector *metrics.Collector
	mu               sync.RWMutex
	// closed is set under mu by Shutdown; when true all mutations are rejected.
	closed bool
	// closing is closed at most once via SignalShutdown/Shutdown. It lets
	// in-flight service creation observe shutdown even while it holds mu, so
	// candidates are torn down instead of being registered.
	closing   chan struct{}
	closeOnce sync.Once
	// shutdownOnce/shutdownDone/shutdownErr converge concurrent or repeated
	// Shutdown calls onto a single stop process with one shared result.
	shutdownOnce sync.Once
	shutdownDone chan struct{}
	shutdownErr  error
}

// Service represents a single service instance
type Service struct {
	Name             string
	Config           config.Service
	globalConfig     *config.Config
	listener         net.Listener
	server           *http.Server
	tsServer         *tailscale.Server // Reference to Tailscale server for WhoIs
	metricsCollector *metrics.Collector
	handler          http.Handler // Pre-created handler to catch config errors early
}

// handlerWithClose wraps an http.Handler and preserves the Close method from the underlying proxy.Handler
type handlerWithClose struct {
	http.Handler
	closeHandler proxy.Handler
}

// Close delegates to the underlying proxy handler's Close method
func (h *handlerWithClose) Close() error {
	if h.closeHandler != nil {
		return h.closeHandler.Close()
	}
	return nil
}

// NewRegistry creates a new service registry
func NewRegistry(cfg *config.Config, tsServer *tailscale.Server) *Registry {
	return &Registry{
		config:   cfg,
		tsServer: tsServer,
		services: make(map[string]*Service, len(cfg.Services)),
		closing:  make(chan struct{}),
	}
}

// SignalShutdown marks the registry as shutting down without blocking. It is
// idempotent and safe to call while another goroutine holds the registry lock
// (for example one blocked creating a service). Mutations already past their
// admission checks must observe isClosing before registering new state.
func (r *Registry) SignalShutdown() {
	r.closeOnce.Do(func() {
		// The channel is allocated by NewRegistry and never reassigned, so it
		// is safe to read here without r.mu. A nil channel means the registry
		// was created as a zero value (e.g. in tests); Shutdown's r.closed
		// flag still provides rejection for those instances.
		if r.closing != nil {
			close(r.closing)
		}
	})
}

// isClosing reports whether shutdown has been signalled. Callers must hold
// r.mu (or otherwise be serialized against Shutdown) when reading r.closed;
// the channel check is lock-free and safe on a nil channel.
func (r *Registry) isClosing() bool {
	if r.closed {
		return true
	}
	if r.closing == nil {
		return false
	}
	select {
	case <-r.closing:
		return true
	default:
		return false
	}
}

// SetMetricsCollector sets the metrics collector for the registry
func (r *Registry) SetMetricsCollector(collector *metrics.Collector) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.metricsCollector = collector
}

// GetMetricsCollector returns the metrics collector for the registry
func (r *Registry) GetMetricsCollector() *metrics.Collector {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.metricsCollector
}

// GetService returns a service by name
func (r *Registry) GetService(name string) (*Service, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	svc, exists := r.services[name]
	return svc, exists
}

// StartServices starts all configured services
func (r *Registry) StartServices() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.isClosing() {
		return ErrRegistryClosed
	}

	startTime := time.Now()
	totalServices := len(r.config.Services)
	failedServices := make(map[string]error)
	successfulCount := 0

	slog.Debug("starting all configured services",
		"total_services", totalServices,
	)

	for i, svcCfg := range r.config.Services {
		// Stop admitting work if shutdown began while starting services.
		if r.isClosing() {
			slog.Info("skipping remaining service starts because registry is shutting down",
				"started", i,
				"total", totalServices,
			)
			break
		}

		serviceStart := time.Now()
		slog.Debug("starting service",
			"service", svcCfg.Name,
			"index", i+1,
			"of", totalServices,
		)

		svc, err := r.startService(svcCfg)
		if err != nil {
			slog.Debug("service start failed",
				"service", svcCfg.Name,
				"duration", time.Since(serviceStart),
				"error", err,
			)
			failedServices[svcCfg.Name] = err
			continue // Skip failed services as per spec
		}
		r.services[svcCfg.Name] = svc
		slog.Info("started service", "service", svcCfg.Name)
		slog.Debug("service started successfully",
			"service", svcCfg.Name,
			"duration", time.Since(serviceStart),
		)
		successfulCount++
	}

	// Update active services count if metrics collector is available
	if r.metricsCollector != nil {
		r.metricsCollector.SetActiveServices(len(r.services))
	}

	// Create ServiceStartupError if any services failed
	failedCount := len(failedServices)
	slog.Debug("service startup complete",
		"total_services", totalServices,
		"successful", successfulCount,
		"failed", failedCount,
		"total_duration", time.Since(startTime),
	)

	if failedCount > 0 {
		return tserrors.NewServiceStartupError(totalServices, successfulCount, failedCount, failedServices)
	}

	return nil
}

// startService starts a single service
func (r *Registry) startService(svcCfg config.Service) (*Service, error) {
	funnelEnabled := svcCfg.FunnelEnabled != nil && *svcCfg.FunnelEnabled

	phaseStart := time.Now()
	slog.Debug("creating listener for service",
		"service", svcCfg.Name,
		"tls_mode", svcCfg.TLSMode,
		"funnel_enabled", funnelEnabled,
	)

	// Create listener for this service
	listener, err := r.tsServer.Listen(svcCfg, svcCfg.TLSMode, funnelEnabled)
	if err != nil {
		return nil, tserrors.WrapResource(err, "creating listener")
	}
	slog.Debug("listener created for service",
		"service", svcCfg.Name,
		"duration", time.Since(phaseStart),
		"address", listener.Addr(),
	)

	// Create service instance
	svc := &Service{
		Name:             svcCfg.Name,
		Config:           svcCfg,
		globalConfig:     r.config,
		listener:         listener,
		tsServer:         r.tsServer,
		metricsCollector: r.metricsCollector,
	}

	// Create handler early to catch configuration errors
	handlerStart := time.Now()
	slog.Debug("creating handler for service",
		"service", svcCfg.Name,
		"backend_addr", svcCfg.BackendAddr,
	)
	handler, err := svc.CreateHandler()
	if err != nil {
		_ = listener.Close()
		if r.tsServer != nil {
			if closeErr := r.tsServer.CloseService(svcCfg.Name); closeErr != nil {
				slog.Debug("failed to close tsnet server after handler failure",
					"service", svcCfg.Name,
					"error", closeErr,
				)
			}
		}
		slog.Debug("handler creation failed",
			"service", svcCfg.Name,
			"duration", time.Since(handlerStart),
			"error", err,
		)
		return nil, err
	}
	svc.handler = handler
	slog.Debug("handler created for service",
		"service", svcCfg.Name,
		"duration", time.Since(handlerStart),
	)

	// Create HTTP server with timeouts
	svc.server = &http.Server{
		Handler:           svc.handler,
		ReadHeaderTimeout: config.DefaultReadHeaderTimeout,
	}

	// For Funnel-enabled services, extract the real client IP from FunnelConn
	if funnelEnabled {
		svc.server.ConnContext = funnel.ConnContext
	}

	// Override with configured values if provided
	if svcCfg.ReadHeaderTimeout != nil {
		svc.server.ReadHeaderTimeout = *svcCfg.ReadHeaderTimeout
	}
	if svcCfg.WriteTimeout != nil {
		svc.server.WriteTimeout = *svcCfg.WriteTimeout
	}
	if svcCfg.IdleTimeout != nil {
		svc.server.IdleTimeout = *svcCfg.IdleTimeout
	}

	slog.Debug("HTTP server configured",
		"service", svcCfg.Name,
		"read_header_timeout", svc.server.ReadHeaderTimeout,
		"write_timeout", svc.server.WriteTimeout,
		"idle_timeout", svc.server.IdleTimeout,
	)

	// Track active connections per service when metrics are enabled
	if svc.metricsCollector != nil {
		svc.server.ConnState = svc.metricsCollector.ConnStateHook(svcCfg.Name)
	}

	// Start serving in background
	go func() {
		slog.Debug("service listening", "service", svcCfg.Name, "address", listener.Addr())
		if err := svc.server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("service serve error", "service", svcCfg.Name, "error", err)
		}
	}()

	return svc, nil
}

// Handler returns the HTTP handler for this service
func (s *Service) Handler() http.Handler {
	return s.handler
}

// SetHandler sets the handler for the service (used for testing)
func (s *Service) SetHandler(h http.Handler) {
	s.handler = h
}

// CreateHandler creates the HTTP handler for the service, returning an error if configuration is invalid
func (s *Service) CreateHandler() (http.Handler, error) {
	// Create transport config from global settings
	transportConfig := &proxy.TransportConfig{}
	if s.Config.ResponseHeaderTimeout != nil {
		transportConfig.ResponseHeaderTimeout = *s.Config.ResponseHeaderTimeout
	}

	// Get trusted proxies from global config
	var trustedProxies []string
	if s.globalConfig != nil {
		trustedProxies = s.globalConfig.Global.TrustedProxies
		// Set transport timeouts from global config
		if s.globalConfig.Global.DialTimeout != nil {
			transportConfig.DialTimeout = *s.globalConfig.Global.DialTimeout
		}
		if s.globalConfig.Global.KeepAliveTimeout != nil {
			transportConfig.KeepAliveTimeout = *s.globalConfig.Global.KeepAliveTimeout
		}
		if s.globalConfig.Global.IdleConnTimeout != nil {
			transportConfig.IdleConnTimeout = *s.globalConfig.Global.IdleConnTimeout
		}
		if s.globalConfig.Global.TLSHandshakeTimeout != nil {
			transportConfig.TLSHandshakeTimeout = *s.globalConfig.Global.TLSHandshakeTimeout
		}
		if s.globalConfig.Global.ExpectContinueTimeout != nil {
			transportConfig.ExpectContinueTimeout = *s.globalConfig.Global.ExpectContinueTimeout
		}
	}

	// Create proxy handler with unified configuration
	insecureSkipVerify := s.Config.InsecureSkipVerify != nil && *s.Config.InsecureSkipVerify
	handler, err := proxy.NewHandler(&proxy.HandlerConfig{
		BackendAddr:        s.Config.BackendAddr,
		TransportConfig:    transportConfig,
		TrustedProxies:     trustedProxies,
		MetricsCollector:   s.metricsCollector,
		ServiceName:        s.Config.Name,
		UpstreamHeaders:    s.Config.UpstreamHeaders,
		DownstreamHeaders:  s.Config.DownstreamHeaders,
		RemoveUpstream:     s.Config.RemoveUpstream,
		RemoveDownstream:   s.Config.RemoveDownstream,
		FlushInterval:      s.Config.FlushInterval,
		InsecureSkipVerify: insecureSkipVerify,
	})
	if err != nil {
		return nil, err
	}

	// Log security warning if TLS verification is disabled
	if insecureSkipVerify {
		slog.Warn("TLS certificate verification disabled for service connection to backend",
			"service", s.Config.Name,
			"backend", s.Config.BackendAddr)
	}

	// Wrap with middleware - convert to http.Handler for middleware chaining
	var httpHandler http.Handler = handler

	// Wrap with request ID middleware - this should be early in the chain
	httpHandler = middleware.RequestID(httpHandler)

	// Apply request body size limit
	maxBodySize := s.getMaxRequestBodySize()
	if maxBodySize > 0 {
		httpHandler = middleware.MaxBytesHandler(maxBodySize)(httpHandler)
	}

	// Wrap with whois middleware if enabled
	whoisEnabled := s.Config.WhoisEnabled != nil && *s.Config.WhoisEnabled
	if whoisEnabled && s.tsServer != nil {
		// Get the tsnet server instance for this service
		serviceServer := s.tsServer.GetServiceServer(s.Config.Name)
		if serviceServer != nil {
			var whoisTimeout time.Duration
			if s.Config.WhoisTimeout != nil {
				whoisTimeout = *s.Config.WhoisTimeout
			} else {
				whoisTimeout = config.DefaultWhoisTimeout
			}
			// Create a whois client adapter for the tsnet server
			whoisClient := tailscale.NewWhoisClientAdapter(serviceServer, s.metricsCollector, s.Config.Name)
			// Use the whois middleware with internalized cache
			httpHandler = middleware.Whois(whoisClient, whoisEnabled, whoisTimeout, defaultWhoisCacheSize, defaultWhoisCacheTTL)(httpHandler)
		}
	}

	// Strip X-Tailscale-* headers unconditionally to prevent identity spoofing.
	// This wraps the whois middleware so spoofed headers are removed before
	// reaching any handler, regardless of whether whois lookup is enabled.
	httpHandler = middleware.StripTailscaleHeaders(httpHandler)

	// Wrap with metrics middleware if collector is available
	if s.metricsCollector != nil {
		httpHandler = s.metricsCollector.Middleware(s.Config.Name, httpHandler)
	}

	// Wrap with access logging middleware if enabled
	if s.isAccessLogEnabled() {
		httpHandler = middleware.AccessLog(slog.Default(), s.Config.Name)(httpHandler)
	}

	// Create a wrapper that preserves the Close method from the original handler
	return &handlerWithClose{
		Handler:      httpHandler,
		closeHandler: handler,
	}, nil
}

// isAccessLogEnabled returns whether access logging is enabled for this service
func (s *Service) isAccessLogEnabled() bool {
	// First check service-specific setting
	if s.Config.AccessLog != nil {
		return *s.Config.AccessLog
	}
	// Then check global setting
	if s.globalConfig != nil && s.globalConfig.Global.AccessLog != nil {
		return *s.globalConfig.Global.AccessLog
	}
	// Default to true
	return true
}

// getMaxRequestBodySize returns the max request body size for this service
// It returns the service-specific override if set, otherwise the global value
// A negative value means no limit should be applied
func (s *Service) getMaxRequestBodySize() int64 {
	if s.Config.MaxRequestBodySize != nil {
		// If explicitly set, use the value as-is. 0 is a valid explicit limit.
		return *s.Config.MaxRequestBodySize
	}
	if s.globalConfig != nil && s.globalConfig.Global.MaxRequestBodySize != nil {
		// If explicitly set globally, use the value as-is.
		return *s.globalConfig.Global.MaxRequestBodySize
	}
	// Default if no config available or not explicitly set anywhere
	return config.DefaultMaxRequestBodySize
}

// Stop gracefully stops the service
func (s *Service) Stop(ctx context.Context) error {
	if s.server != nil {
		if err := s.server.Shutdown(ctx); err != nil {
			return err
		}
	}

	// Close the listener (may already be closed by server.Shutdown)
	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			// Only log if it's not an "already closed" error
			if !errors.Is(err, net.ErrClosed) {
				slog.Warn("failed to close listener", "service", s.Config.Name, "error", err)
			}
		}
	}

	// Close the handler if it implements Close
	if s.handler != nil {
		if closer, ok := s.handler.(io.Closer); ok {
			if err := closer.Close(); err != nil {
				slog.Warn("failed to close handler", "service", s.Config.Name, "error", err)
			}
		}
	}

	return nil
}

// Shutdown gracefully shuts down all services.
//
// It is idempotent: the first call signals shutdown, rejects further
// mutations, stops every service and aggregates stop errors; concurrent or
// repeated calls wait for the same shutdown process and observe its result.
func (r *Registry) Shutdown(ctx context.Context) error {
	r.shutdownOnce.Do(func() {
		r.shutdownDone = make(chan struct{})
		go func() {
			r.shutdownErr = r.doShutdown(ctx)
			close(r.shutdownDone)
		}()
	})
	<-r.shutdownDone
	return r.shutdownErr
}

// doShutdown performs the single, serialized service stop sequence.
func (r *Registry) doShutdown(ctx context.Context) error {
	// Signal without holding the lock first so an in-flight
	// AddService/UpdateService blocked on service creation can observe
	// shutdown as soon as callers (or the App) abort the creation.
	r.SignalShutdown()

	r.mu.Lock()
	r.closed = true
	defer r.mu.Unlock()

	var wg sync.WaitGroup
	errCh := make(chan error, len(r.services))

	for _, svc := range r.services {
		wg.Add(1)
		go func(s *Service) {
			defer wg.Done()
			if err := s.Stop(ctx); err != nil {
				errCh <- tserrors.WrapInternal(err, fmt.Sprintf("shutting down service %q", s.Config.Name))
			}
		}(svc)
	}

	wg.Wait()
	close(errCh)

	// Collect any errors
	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}

// AddService dynamically starts and registers a new service.
// Returns ErrRegistryClosed if the registry is shutting down or closed,
// in which case the candidate service is never registered.
// Thread-safe.
func (r *Registry) AddService(svcCfg config.Service) error {
	start := time.Now()

	r.mu.Lock()
	defer r.mu.Unlock()

	// Reject new services once shutdown has been signalled.
	if r.isClosing() {
		if r.metricsCollector != nil {
			r.metricsCollector.RecordServiceOperation("add", false, time.Since(start))
		}
		return ErrRegistryClosed
	}

	// Check if service already exists
	if _, exists := r.services[svcCfg.Name]; exists {
		if r.metricsCollector != nil {
			r.metricsCollector.RecordServiceOperation("add", false, time.Since(start))
		}
		return fmt.Errorf("service %s already exists", svcCfg.Name)
	}

	// Start the service (this may block on tsnet startup)
	svc, err := r.startService(svcCfg)
	if err != nil {
		if r.metricsCollector != nil {
			r.metricsCollector.RecordServiceOperation("add", false, time.Since(start))
		}
		return fmt.Errorf("failed to start service %s: %w", svcCfg.Name, err)
	}

	// Shutdown can begin while service creation is in flight. Tear the
	// candidate down instead of registering it so no listener or tsnet node
	// survives the shutdown process.
	if r.isClosing() {
		r.discardCandidate(svc, start)
		return ErrRegistryClosed
	}

	// Add to registry
	r.services[svcCfg.Name] = svc

	// Record metrics
	if r.metricsCollector != nil {
		r.metricsCollector.RecordServiceOperation("add", true, time.Since(start))
		r.metricsCollector.SetActiveServices(len(r.services))
	}

	slog.Info("added service", "service", svcCfg.Name)
	return nil
}

// discardCandidate stops a service that was created during a reload but can
// no longer be registered because shutdown started. The caller must hold
// r.mu.
func (r *Registry) discardCandidate(svc *Service, startedAt time.Time) {
	ctx, cancel := context.WithTimeout(context.Background(), serviceStopTimeout)
	defer cancel()

	if err := svc.Stop(ctx); err != nil {
		slog.Debug("failed to stop discarded service candidate",
			"service", svc.Config.Name,
			"error", err)
	}
	if r.tsServer != nil {
		if err := r.tsServer.CloseService(svc.Config.Name); err != nil {
			slog.Debug("failed to close tsnet server for discarded candidate",
				"service", svc.Config.Name,
				"error", err)
		}
	}
	if r.metricsCollector != nil {
		r.metricsCollector.RecordServiceOperation("add", false, time.Since(startedAt))
	}
	slog.Info("discarded service candidate because registry is shutting down",
		"service", svc.Config.Name)
}

// RemoveService stops and removes a service from the registry.
// Returns ErrRegistryClosed if the registry is shutting down or closed.
// Returns an error if the service is not found or fails to stop.
func (r *Registry) RemoveService(name string) error {
	start := time.Now()

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.isClosing() {
		if r.metricsCollector != nil {
			r.metricsCollector.RecordServiceOperation("remove", false, time.Since(start))
		}
		return ErrRegistryClosed
	}

	svc, exists := r.services[name]
	if !exists {
		if r.metricsCollector != nil {
			r.metricsCollector.RecordServiceOperation("remove", false, time.Since(start))
		}
		return fmt.Errorf("service %s not found", name)
	}

	// Stop the service (this will close listener and handler)
	ctx, cancel := context.WithTimeout(context.Background(), serviceStopTimeout)
	defer cancel()

	if err := svc.Stop(ctx); err != nil {
		if r.metricsCollector != nil {
			r.metricsCollector.RecordServiceOperation("remove", false, time.Since(start))
		}
		return fmt.Errorf("failed to stop service %s: %w", name, err)
	}

	// Close the tsnet server for this service
	if r.tsServer != nil {
		if err := r.tsServer.CloseService(name); err != nil {
			slog.Error("failed to close tsnet server for service", "service", name, "error", err)
			// Continue with removal even if tsnet close fails
		}
	}

	// Remove from registry
	delete(r.services, name)

	// Record metrics
	if r.metricsCollector != nil {
		r.metricsCollector.RecordServiceOperation("remove", true, time.Since(start))
		r.metricsCollector.SetActiveServices(len(r.services))
	}

	slog.Info("removed service", "service", name)
	return nil
}

// validateServiceConfig checks service config for common errors before updating.
//
// Validates:
//   - Non-empty service name and backend address
//   - Absolute unix socket paths (unix://)
//   - TLS mode is one of "off" or "auto"
//   - Non-negative timeouts
//
// Returns error if invalid.
func (r *Registry) validateServiceConfig(cfg config.Service) error {
	// Validate service name
	if cfg.Name == "" {
		return fmt.Errorf("service name is required")
	}

	// Validate backend address
	if cfg.BackendAddr == "" {
		return fmt.Errorf("backend address is required")
	}

	// Validate backend address format
	path, unix := strings.CutPrefix(cfg.BackendAddr, "unix://")
	if unix {
		// Unix socket path validation
		if !filepath.IsAbs(path) {
			return fmt.Errorf("unix socket path must be absolute: %s", path)
		}
	} else {
		// TCP address validation
		// Add scheme if missing, consistent with proxy.parseBackendURL
		addr := cfg.BackendAddr
		if !strings.Contains(addr, "://") {
			addr = "http://" + addr
		}
		if _, err := url.Parse(addr); err != nil {
			return fmt.Errorf("invalid backend address: %w", err)
		}
	}

	// Validate TLS mode
	switch cfg.TLSMode {
	case config.TLSModeOff, config.TLSModeAuto:
		// Valid modes
	default:
		return fmt.Errorf("invalid TLS mode: %s (must be '%s' or '%s')", cfg.TLSMode, config.TLSModeOff, config.TLSModeAuto)
	}

	// Validate timeout values
	if cfg.ReadHeaderTimeout != nil && *cfg.ReadHeaderTimeout < 0 {
		return fmt.Errorf("read header timeout must be non-negative")
	}
	if cfg.WriteTimeout != nil && *cfg.WriteTimeout < 0 {
		return fmt.Errorf("write timeout must be non-negative")
	}
	if cfg.IdleTimeout != nil && *cfg.IdleTimeout < 0 {
		return fmt.Errorf("idle timeout must be non-negative")
	}
	if cfg.ResponseHeaderTimeout != nil && *cfg.ResponseHeaderTimeout < 0 {
		return fmt.Errorf("response header timeout must be non-negative")
	}

	return nil
}

// UpdateService updates an existing service with new configuration at runtime.
// Minimizes downtime by validating config before stopping the old service.
// Thread-safe. Returns ErrRegistryClosed if the registry is shutting down,
// in which case no replacement service is registered.
// Returns error if service not found, config invalid, stop/start fails.
func (r *Registry) UpdateService(name string, newCfg config.Service) error {
	start := time.Now()

	r.mu.Lock()
	defer r.mu.Unlock()

	// Reject updates once shutdown has been signalled.
	if r.isClosing() {
		if r.metricsCollector != nil {
			r.metricsCollector.RecordServiceOperation("update", false, time.Since(start))
		}
		return ErrRegistryClosed
	}

	// Check if service exists
	oldSvc, exists := r.services[name]
	if !exists {
		if r.metricsCollector != nil {
			r.metricsCollector.RecordServiceOperation("update", false, time.Since(start))
		}
		return fmt.Errorf("service %s not found", name)
	}

	// Validate the new configuration as much as possible before stopping the old service
	// This helps minimize downtime by catching configuration errors early
	if err := r.validateServiceConfig(newCfg); err != nil {
		if r.metricsCollector != nil {
			r.metricsCollector.RecordServiceOperation("update", false, time.Since(start))
		}
		return fmt.Errorf("invalid service configuration: %w", err)
	}

	// Store old service config for logging/debugging
	oldConfig := oldSvc.Config

	// Stop the old service
	ctx, cancel := context.WithTimeout(context.Background(), serviceStopTimeout)
	defer cancel()

	if err := oldSvc.Stop(ctx); err != nil {
		if r.metricsCollector != nil {
			r.metricsCollector.RecordServiceOperation("update", false, time.Since(start))
		}
		return fmt.Errorf("failed to stop service %s: %w", name, err)
	}

	// Close the tsnet server for this service
	if r.tsServer != nil {
		if err := r.tsServer.CloseService(name); err != nil {
			slog.Error("failed to close tsnet server for service", "service", name, "error", err)
			// Continue with update even if tsnet close fails
		}
	}

	// Shutdown may have started while the old service was being stopped.
	// Do not create a replacement that would outlive the shutdown process.
	if r.isClosing() {
		delete(r.services, name)
		if r.metricsCollector != nil {
			r.metricsCollector.RecordServiceOperation("update", false, time.Since(start))
			r.metricsCollector.SetActiveServices(len(r.services))
		}
		return ErrRegistryClosed
	}

	// Start the new service configuration
	newSvc, err := r.startService(newCfg)
	if err != nil {
		// If we fail to start the new service, remove it from registry
		// to avoid leaving a stopped service in the registry
		delete(r.services, name)

		// Record failure metric
		if r.metricsCollector != nil {
			r.metricsCollector.RecordServiceOperation("update", false, time.Since(start))
			r.metricsCollector.SetActiveServices(len(r.services))
		}

		return fmt.Errorf("failed to start updated service %s: %w", name, err)
	}

	// Re-check before publishing: shutdown can begin during service creation.
	if r.isClosing() {
		r.discardCandidate(newSvc, start)
		delete(r.services, name)
		if r.metricsCollector != nil {
			r.metricsCollector.SetActiveServices(len(r.services))
		}
		return ErrRegistryClosed
	}

	// Replace in registry
	r.services[name] = newSvc

	// Record success metric
	if r.metricsCollector != nil {
		r.metricsCollector.RecordServiceOperation("update", true, time.Since(start))
		// Active services count doesn't change on update
	}

	slog.Info("updated service",
		"service", name,
		"old_backend", oldConfig.BackendAddr,
		"new_backend", newCfg.BackendAddr,
	)
	return nil
}
