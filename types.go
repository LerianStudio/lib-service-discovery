package libsd

import (
	"context"
	"errors"
	"fmt"
)

// ── Errors ────────────────────────────────────────────────────────────────────

var (
	// ErrNilManager is returned when a method is called on a nil *Manager.
	ErrNilManager = errors.New("lib-service-discovery: manager is nil")

	// ErrNilRegistry is returned when a method is called on a nil registry.
	ErrNilRegistry = errors.New("lib-service-discovery: registry is nil")

	// ErrEmptyAdvertiseAddr is returned when SERVICE_DISCOVERY_ENABLED=true but
	// SERVICE_ADVERTISE_ADDR is not set.
	ErrEmptyAdvertiseAddr = errors.New("lib-service-discovery: SERVICE_ADVERTISE_ADDR is required when discovery is enabled")

	// ErrEmptyConsulAddr is returned when the Consul address is empty.
	ErrEmptyConsulAddr = errors.New("lib-service-discovery: consul address must not be empty")

	// ErrNoHealthyInstances is returned by Resolve when Consul finds no healthy
	// instances for the requested service name.
	ErrNoHealthyInstances = errors.New("lib-service-discovery: no healthy instances found")

	// ErrDiscoveryDisabledNoFallback is returned by Resolve when discovery is
	// disabled and no fallback address was provided.
	ErrDiscoveryDisabledNoFallback = errors.New("lib-service-discovery: discovery disabled and no fallback address provided")
)

// ── Registry interface ────────────────────────────────────────────────────────

// Registry is the interface for a service registry backend.
// The only provided implementation is consulRegistry (consul.go).
// Custom backends (e.g. in-memory for tests) can satisfy this interface.
type Registry interface {
	Register(ctx context.Context, svc Service) error
	Deregister(ctx context.Context, serviceID string) error
	// Resolve returns a healthy instance of name. tag narrows the result to
	// instances carrying that exact tag; pass "" to match any instance.
	Resolve(ctx context.Context, name, tag string) (Service, error)
	Watch(ctx context.Context, name string) (<-chan Event, error)
}

// ── Domain types ──────────────────────────────────────────────────────────────

// Service represents an instance registered in the service registry.
type Service struct {
	ID          string
	Name        string
	Address     string
	Port        int
	// Scheme is the URL scheme used to reach this service (e.g. "https", "http").
	// Stored in Consul Meta under the key "scheme". When empty, callers should
	// default to their own scheme convention.
	Scheme      string
	Tags        []string
	Meta        map[string]string
	HealthCheck *HealthCheck
}

// HealthCheck configures how Consul tracks the liveness of the service.
//
// Two mutually exclusive modes:
//   - HTTP check (Interval/Timeout): Consul polls an HTTP endpoint on the
//     service. Requires Consul to REACH the service — only works when they share
//     a network (e.g. client agent on the same node).
//   - TTL check (TTL): the registry pushes a heartbeat ("pass") every TTL/2 from
//     inside the process, so Consul never needs to reach the service. Required for
//     agentless/remote workloads behind NAT (the central-Consul model).
type HealthCheck struct {
	// HTTP is set automatically by Manager.Register from the service scheme,
	// address, port and Path. Set it manually only when using the Registry
	// interface directly.
	HTTP     string
	Interval string
	Timeout  string

	// Path is the HTTP path Consul probes for the health check (e.g. "/healthz").
	// Defaults to "/health" when empty. A leading slash is added if missing.
	// Ignored for TTL checks.
	Path string

	// TTL, when non-empty (e.g. "30s"), registers a TTL check instead of HTTP.
	// The registry emits a heartbeat every TTL/2 and stops it on Deregister.
	// Mutually exclusive with HTTP; when set, HTTP is ignored.
	TTL string
}

// EventType classifies a Watch event.
type EventType string

const (
	// EventRegistered is emitted when a service instance becomes healthy.
	EventRegistered EventType = "registered"

	// EventDeregistered is emitted when a service instance becomes critical or is removed.
	EventDeregistered EventType = "deregistered"
)

// Event is emitted by Watch when a service's health state changes.
type Event struct {
	Type    EventType
	Service Service
}

// Addr returns the host:port address of the service.
func (s Service) Addr() string {
	return fmt.Sprintf("%s:%d", s.Address, s.Port)
}
