// Package metrics handles Prometheus metrics collection and exposition.
package metrics

import (
	"bufio"
	"context"
	stderrors "errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/jtdowney/tsbridge/internal/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Collector holds all prometheus metrics for tsbridge
type Collector struct {
	RequestsTotal   *prometheus.CounterVec
	RequestDuration *prometheus.HistogramVec
	ErrorsTotal     *prometheus.CounterVec

	// Enhanced metrics
	ConnectionCount      *prometheus.GaugeVec
	WhoisDuration        *prometheus.HistogramVec
	ConnectionPoolActive *prometheus.GaugeVec

	// Service lifecycle metrics
	ServiceOperations    *prometheus.CounterVec
	ServiceOpDuration    *prometheus.HistogramVec
	ServicesActive       prometheus.Gauge
	ConfigReloads        *prometheus.CounterVec
	ConfigReloadDuration prometheus.Histogram
}

// NewCollector creates a new metrics collector with all required metrics
func NewCollector() *Collector {
	return &Collector{
		RequestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "tsbridge_requests_total",
				Help: "Total number of requests processed",
			},
			[]string{"service", "status"},
		),
		RequestDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "tsbridge_request_duration_seconds",
				Help:    "Request duration in seconds",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"service"},
		),
		ErrorsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "tsbridge_errors_total",
				Help: "Total number of errors",
			},
			[]string{"service", "type"},
		),
		ConnectionCount: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "tsbridge_connections_active",
				Help: "Number of connections managed by the HTTP server per service; excludes hijacked connections",
			},
			[]string{"service"},
		),
		WhoisDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "tsbridge_whois_duration_seconds",
				Help:    "Successful Whois lookup duration in seconds",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"service"},
		),
		ConnectionPoolActive: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "tsbridge_connection_pool_active",
				Help: "Number of active connections in the pool",
			},
			[]string{"service"},
		),
		ServiceOperations: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "tsbridge_service_operations_total",
				Help: "Total number of service lifecycle operations",
			},
			[]string{"operation", "status"},
		),
		ServiceOpDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "tsbridge_service_operation_duration_seconds",
				Help:    "Duration of service lifecycle operations in seconds",
				Buckets: []float64{0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10},
			},
			[]string{"operation"},
		),
		ServicesActive: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "tsbridge_services_active",
				Help: "Number of active services",
			},
		),
		ConfigReloads: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "tsbridge_config_reloads_total",
				Help: "Total number of configuration reloads",
			},
			[]string{"status"},
		),
		ConfigReloadDuration: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "tsbridge_config_reload_duration_seconds",
				Help:    "Duration of configuration reloads in seconds",
				Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 30},
			},
		),
	}
}

// Register registers all metrics with the provided registry
func (c *Collector) Register(reg prometheus.Registerer) error {
	collectors := []prometheus.Collector{
		c.RequestsTotal,
		c.RequestDuration,
		c.ErrorsTotal,
		c.ConnectionCount,
		c.WhoisDuration,
		c.ConnectionPoolActive,
		c.ServiceOperations,
		c.ServiceOpDuration,
		c.ServicesActive,
		c.ConfigReloads,
		c.ConfigReloadDuration,
	}

	for _, collector := range collectors {
		if err := reg.Register(collector); err != nil {
			return errors.WrapResource(err, "failed to register collector")
		}
	}

	return nil
}

// RecordError increments the error counter for a service and error type
func (c *Collector) RecordError(service, errorType string) {
	c.ErrorsTotal.WithLabelValues(service, errorType).Inc()
}

// RecordWhoisDuration records the duration of a successful whois lookup.
func (c *Collector) RecordWhoisDuration(service string, duration time.Duration) {
	c.WhoisDuration.WithLabelValues(service).Observe(duration.Seconds())
}

// ConnStateHook returns an http.Server.ConnState callback that tracks
// connections until they close or leave HTTP server control through hijacking.
func (c *Collector) ConnStateHook(service string) func(net.Conn, http.ConnState) {
	return func(_ net.Conn, state http.ConnState) {
		switch state {
		case http.StateNew:
			c.ConnectionCount.WithLabelValues(service).Inc()
		case http.StateClosed, http.StateHijacked:
			c.ConnectionCount.WithLabelValues(service).Dec()
		}
	}
}

// UpdateConnectionPoolMetrics updates connection pool metrics for a service
func (c *Collector) UpdateConnectionPoolMetrics(service string, active int) {
	c.ConnectionPoolActive.WithLabelValues(service).Set(float64(active))
}

// RecordServiceOperation records a service lifecycle operation
func (c *Collector) RecordServiceOperation(operation string, success bool, duration time.Duration) {
	status := "success"
	if !success {
		status = "failure"
	}
	c.ServiceOperations.WithLabelValues(operation, status).Inc()
	c.ServiceOpDuration.WithLabelValues(operation).Observe(duration.Seconds())
}

// SetActiveServices sets the number of active services
func (c *Collector) SetActiveServices(count int) {
	c.ServicesActive.Set(float64(count))
}

// RecordConfigReload records a configuration reload operation
func (c *Collector) RecordConfigReload(success bool, duration time.Duration) {
	status := "success"
	if !success {
		status = "failure"
	}
	c.ConfigReloads.WithLabelValues(status).Inc()
	c.ConfigReloadDuration.Observe(duration.Seconds())
}

// responseWriter wraps http.ResponseWriter to capture status code
type responseWriter struct {
	http.ResponseWriter
	statusCode int
	written    bool
}

func (rw *responseWriter) WriteHeader(code int) {
	if !rw.written {
		rw.statusCode = code
		rw.written = true
		rw.ResponseWriter.WriteHeader(code)
	}
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	if !rw.written {
		rw.WriteHeader(http.StatusOK)
	}
	return rw.ResponseWriter.Write(b)
}

// Hijack implements the http.Hijacker interface for WebSocket support
func (rw *responseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hijacker, ok := rw.ResponseWriter.(http.Hijacker); ok {
		return hijacker.Hijack()
	}
	return nil, nil, fmt.Errorf("ResponseWriter does not support hijacking")
}

// Flush implements the http.Flusher interface for streaming support
func (rw *responseWriter) Flush() {
	if flusher, ok := rw.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

// Middleware returns HTTP middleware that records metrics for requests
func (c *Collector) Middleware(serviceName string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Wrap response writer to capture status
		wrapped := &responseWriter{
			ResponseWriter: w,
			statusCode:     http.StatusOK,
		}

		// Recover from panics
		defer func() {
			if err := recover(); err != nil {
				// Write error response if not already written
				if !wrapped.written {
					wrapped.WriteHeader(http.StatusInternalServerError)
				}
				// Record error
				c.RecordError(serviceName, "panic")
			}

			// Record metrics
			duration := time.Since(start)
			c.RequestDuration.WithLabelValues(serviceName).Observe(duration.Seconds())
			c.RequestsTotal.WithLabelValues(serviceName, strconv.Itoa(wrapped.statusCode)).Inc()
		}()

		// Call next handler
		next.ServeHTTP(wrapped, r)
	})
}

// Server represents a metrics HTTP server
type Server struct {
	addr              string
	server            *http.Server
	listener          net.Listener
	registry          *prometheus.Registry
	readHeaderTimeout time.Duration
	mu                sync.RWMutex
}

// NewServer creates a new metrics server with a custom registry
func NewServer(addr string, registry *prometheus.Registry, readHeaderTimeout time.Duration) *Server {
	return &Server{
		addr:              addr,
		registry:          registry,
		readHeaderTimeout: readHeaderTimeout,
	}
}

// Listen binds the TCP listener and prepares the HTTP server without
// accepting connections. It lets a configuration generation verify that a
// new metrics address is bindable before any running resource is touched.
// Call Serve to start accepting traffic, or Discard to release a candidate
// that never entered service.
func (s *Server) Listen() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.listener != nil {
		return errors.NewResourceError(fmt.Sprintf("metrics server already listening on %s", s.addr))
	}

	// Create listener
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return errors.WrapResource(err, fmt.Sprintf("failed to listen on %s", s.addr))
	}
	s.listener = listener

	// Create prometheus handler
	handler := promhttp.HandlerFor(s.registry, promhttp.HandlerOpts{})

	timeout := s.readHeaderTimeout
	if timeout == 0 {
		timeout = 5 * time.Second // Default if not set
	}
	s.server = &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: timeout,
	}

	return nil
}

// Serve starts accepting connections on the listener created by Listen.
func (s *Server) Serve() {
	s.mu.RLock()
	listener := s.listener
	server := s.server
	s.mu.RUnlock()

	if listener == nil || server == nil {
		slog.Error("metrics server called Serve before Listen", "address", s.addr)
		return
	}

	// Start serving in background
	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			// Log error but don't crash
			slog.Error("metrics server error", "error", err)
		}
	}()
}

// Start binds the listener and starts the metrics server.
func (s *Server) Start(ctx context.Context) error {
	if err := s.Listen(); err != nil {
		return err
	}
	s.Serve()
	return nil
}

// Discard releases a candidate metrics server that was bound with Listen but
// never published via Serve. After Discard the server no longer holds its
// port and Shutdown is a no-op.
func (s *Server) Discard() {
	s.mu.Lock()
	listener := s.listener
	s.listener = nil
	s.server = nil
	s.mu.Unlock()

	if listener != nil {
		if err := listener.Close(); err != nil && !stderrors.Is(err, net.ErrClosed) {
			slog.Debug("error closing discarded metrics listener", "error", err)
		}
	}
}

// Addr returns the actual address the server is listening on
func (s *Server) Addr() string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

// Shutdown gracefully shuts down the metrics server
func (s *Server) Shutdown(ctx context.Context) error {
	if s.server == nil {
		return nil
	}
	return s.server.Shutdown(ctx)
}
