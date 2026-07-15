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
	//
	// Deprecated: superseded by ErrNoEndpoint. Register returns ErrNoEndpoint when
	// the service to register has neither an external nor an internal endpoint.
	ErrEmptyAdvertiseAddr = errors.New("lib-service-discovery: SERVICE_ADVERTISE_ADDR is required when discovery is enabled")

	// ErrNoEndpoint is returned by Register when discovery is enabled but the
	// service to register has no reachable endpoint — neither an external
	// (AdvertiseAddr) nor an internal (AdvertiseInternalAddr) advertise address is
	// configured, and the service carries no endpoint of its own. Registering
	// requires at least one endpoint; resolving and watching do not (a consumer-only
	// Manager is valid). It is NOT returned by New or Validate.
	ErrNoEndpoint = errors.New("lib-service-discovery: at least one advertise address (external or internal) is required to register")

	// ErrEndpointViewUnavailable is returned by Service.EndpointFor when the
	// requested view cannot be satisfied: an External view against a provider that
	// advertised only an internal endpoint, or a request against a Service that
	// advertised no endpoint at all.
	ErrEndpointViewUnavailable = errors.New("lib-service-discovery: requested endpoint view is unavailable")

	// ErrEmptyConsulAddr is returned when the Consul address is empty.
	ErrEmptyConsulAddr = errors.New("lib-service-discovery: consul address must not be empty")

	// ErrNoHealthyInstances is returned by Resolve when Consul finds no healthy
	// instances for the requested service name.
	ErrNoHealthyInstances = errors.New("lib-service-discovery: no healthy instances found")

	// ErrDiscoveryDisabledNoFallback is returned by Resolve when discovery is
	// disabled and no fallback address was provided.
	ErrDiscoveryDisabledNoFallback = errors.New("lib-service-discovery: discovery disabled and no fallback address provided")

	// ErrInvalidTTL is returned by Register when a TTL-mode health check carries a
	// TTL string that cannot be parsed as a time.Duration. It is a hard
	// configuration error (unlike a below-floor TTL, which is silently raised to
	// the floor). Callers match it with errors.Is.
	ErrInvalidTTL = errors.New("lib-service-discovery: invalid health-check TTL")
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

// Endpoint groups a single reachable address for a service.
//
// Scheme is the URL scheme used to reach the endpoint (e.g. "https", "http").
// When empty, callers should default to their own scheme convention.
type Endpoint struct {
	Address string
	Port    int
	Scheme  string
}

// Addr returns the host:port address of the endpoint. A Port of 0 renders as
// "host:0" (no special handling), mirroring Service.Addr.
func (e Endpoint) Addr() string {
	return fmt.Sprintf("%s:%d", e.Address, e.Port)
}

// EndpointView selects which advertised endpoint a consumer wants to reach.
type EndpointView string

const (
	// External is the ingress host advertised by the instance (Address/Port/Scheme
	// on Service). This is the default view.
	External EndpointView = "external"

	// Internal is the in-cluster Kubernetes service DNS endpoint, when advertised.
	Internal EndpointView = "internal"
)

// Service represents an instance registered in the service registry.
//
// Endpoints are modeled symmetrically: External and Internal are both optional
// *Endpoint fields, and External is the authoritative external endpoint (nil means
// none was advertised). The flat Address/Port/Scheme fields are a deprecated
// mirror of the ROOT ROUTABLE endpoint — External when advertised, otherwise
// Internal — so legacy Resolve/ResolveService/Addr callers always get a routable
// address (never ":0" for a valid instance, including an internal-only provider).
// They are NOT a mirror of External specifically: EndpointFor reads the External
// pointer directly and never synthesizes an external view from these fields.
type Service struct {
	ID   string
	Name string

	// Address is a deprecated mirror of the root routable endpoint's address
	// (External.Address when advertised, else Internal.Address).
	//
	// Deprecated: use External/Internal; retained as a root-routable mirror for
	// back-compat.
	Address string
	// Port is a deprecated mirror of the root routable endpoint's port
	// (External.Port when advertised, else Internal.Port).
	//
	// Deprecated: use External/Internal; retained as a root-routable mirror for
	// back-compat.
	Port int
	// Scheme is a deprecated mirror of the root routable endpoint's scheme
	// (External.Scheme when advertised, else Internal.Scheme).
	//
	// Deprecated: use External/Internal; retained as a root-routable mirror for
	// back-compat.
	Scheme string

	// External is the ingress endpoint advertised by the instance. A nil value
	// means no external endpoint was advertised (e.g. an internal-only provider).
	// Serialized to Consul Meta under external_address/external_port/external_scheme
	// (and mirrored into the "scheme" key for back-compat).
	External *Endpoint
	// Internal is the in-cluster (Kubernetes service DNS) endpoint. A nil value
	// means no internal endpoint was advertised. Serialized to Consul Meta under
	// internal_address/internal_port/internal_scheme.
	Internal *Endpoint

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

// externalEndpoint synthesizes an external endpoint from the deprecated flat
// fields. It is a WRITE-PATH ONLY helper: it returns s.External verbatim when set,
// otherwise synthesizes one from the flat fields when any is non-zero, otherwise
// nil. It is used only by normalizeEndpoints to promote a legacy flat-only caller
// into the External pointer; the READ path (EndpointFor) never calls it, so a
// flat mirror of an internal root can never be mistaken for an external endpoint.
func (s Service) externalEndpoint() *Endpoint {
	if s.External != nil {
		return s.External
	}

	if s.Address != "" || s.Port != 0 || s.Scheme != "" {
		return &Endpoint{Address: s.Address, Port: s.Port, Scheme: s.Scheme}
	}

	return nil
}

// rootEndpoint returns the routable "root" endpoint: External when advertised,
// otherwise Internal, otherwise nil. It is what the deprecated flat mirror and the
// registrable Consul address reflect, and what legacy Resolve/Addr callers read.
func (s Service) rootEndpoint() *Endpoint {
	if s.External != nil {
		return s.External
	}

	return s.Internal
}

// mirrorFlat overwrites the deprecated flat Address/Port/Scheme fields with the
// root routable endpoint (External if set, else Internal). This is a ONE-DIRECTION
// mirror (root -> flat): it never promotes the flat fields back into External, so
// an internal-only provider keeps a routable flat address while External stays nil.
func (s *Service) mirrorFlat() {
	if s == nil {
		return
	}

	root := s.rootEndpoint()
	if root == nil {
		s.Address, s.Port, s.Scheme = "", 0, ""

		return
	}

	s.Address, s.Port, s.Scheme = root.Address, root.Port, root.Scheme
}

// normalizeEndpoints is the WRITE-PATH reconciliation between the endpoint pointers
// and the deprecated flat mirror:
//   - When neither External nor Internal is set but a flat field is non-zero, the
//     caller is a legacy external registration: promote the flat fields into
//     External.
//   - Always mirror the root routable endpoint (External if set, else Internal)
//     back into the flat fields.
//
// It is called by Register (after config is applied), serviceMeta (on its copy),
// and consulRegistry.Register (for direct callers). It is NOT called on the read
// path: serviceFromEntry uses mirrorFlat directly so a reconstructed internal-only
// root is never re-promoted into External.
func (s *Service) normalizeEndpoints() {
	if s == nil {
		return
	}

	// Write-path promotion: a legacy caller that supplied ONLY the flat fields
	// (neither External nor Internal) is an external registration.
	if s.External == nil && s.Internal == nil {
		s.External = s.externalEndpoint()
	}

	// Mirror the root routable endpoint into the deprecated flat fields.
	s.mirrorFlat()
}

// EndpointFor returns the endpoint for view. The mapping is ASYMMETRIC and reads
// the endpoint POINTERS directly — it never synthesizes a view from the deprecated
// flat fields:
//
//   - External (and any unknown value): returns *s.External when advertised,
//     otherwise ErrEndpointViewUnavailable. It NEVER degrades to the internal
//     endpoint, and it NEVER derives an external view from the flat mirror — an
//     internal-only provider (External nil) is unavailable for the External view
//     even though its flat fields carry a routable internal address.
//   - Internal: returns *s.Internal when set; otherwise degrades to *s.External
//     WITHOUT error (the degrade warning is logged by the caller that holds a
//     logger, not here); otherwise ErrEndpointViewUnavailable.
func (s Service) EndpointFor(view EndpointView) (Endpoint, error) {
	if view == Internal {
		if s.Internal != nil {
			return *s.Internal, nil
		}

		if s.External != nil {
			return *s.External, nil // degrade to external; caller logs the warning
		}

		return Endpoint{}, ErrEndpointViewUnavailable
	}

	// External and any unknown view: the authoritative External pointer only.
	if s.External != nil {
		return *s.External, nil
	}

	return Endpoint{}, ErrEndpointViewUnavailable
}
