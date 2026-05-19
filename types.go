package libsd

import (
	"context"
	"errors"
	"fmt"
)

// ── Errors ────────────────────────────────────────────────────────────────────

var (
	// ErrNilManager is returned when a method is called on a nil *Manager.
	ErrNilManager = errors.New("lib-sd: manager is nil")

	// ErrNilRegistry is returned when a method is called on a nil registry.
	ErrNilRegistry = errors.New("lib-sd: registry is nil")

	// ErrEmptyAdvertiseAddr is returned when SERVICE_DISCOVERY_ENABLED=true but
	// SERVICE_ADVERTISE_ADDR is not set.
	ErrEmptyAdvertiseAddr = errors.New("lib-sd: SERVICE_ADVERTISE_ADDR is required when discovery is enabled")

	// ErrEmptyConsulAddr is returned when the Consul address is empty.
	ErrEmptyConsulAddr = errors.New("lib-sd: consul address must not be empty")

	// ErrNoHealthyInstances is returned by Resolve when Consul finds no healthy
	// instances for the requested service name.
	ErrNoHealthyInstances = errors.New("lib-sd: no healthy instances found")

	// ErrDiscoveryDisabledNoFallback is returned by Resolve when discovery is
	// disabled and no fallback address was provided.
	ErrDiscoveryDisabledNoFallback = errors.New("lib-sd: discovery disabled and no fallback address provided")
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

// HealthCheck configures the HTTP health endpoint Consul polls.
type HealthCheck struct {
	// HTTP is set automatically by Manager.Register from the service address and port.
	// Set it manually only when using the Registry interface directly.
	HTTP     string
	Interval string
	Timeout  string
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
