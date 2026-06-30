package libsd

import (
	"context"
	"fmt"
	"strings"

	"github.com/LerianStudio/lib-observability/log"
)

// Manager is the entry point for lib-service-discovery.
//
// When SERVICE_DISCOVERY_ENABLED=false, Register and Deregister are no-ops,
// and Resolve returns the fallback address directly.
//
// When SERVICE_DISCOVERY_ENABLED=true, Register publishes this service to Consul
// and Resolve queries Consul for healthy instances, falling back to the static
// address only when a fallback is provided.
//
// When a workload is configured (via Config.Workload or WithWorkload), Register
// automatically tags services with "workload=<id>" and Resolve filters results
// to instances carrying that tag, isolating each deployment group.
type Manager struct {
	config   Config
	registry Registry
	logger   log.Logger
	workload string
}

// Option configures a Manager after construction.
type Option func(*Manager)

// WithLogger sets the structured logger used by the Manager and its registry.
// A nil logger is silently ignored; the Config.Logger (or log.NewNop()) is used instead.
func WithLogger(l log.Logger) Option {
	return func(m *Manager) {
		if m == nil || l == nil {
			return
		}

		m.logger = l
	}
}

// WithRegistry overrides the Registry backend used for service discovery.
// Useful for alternative providers (etcd, K8s) or in-memory stubs in tests.
// A nil registry is silently ignored; the default Consul backend is used instead.
func WithRegistry(r Registry) Option {
	return func(m *Manager) {
		if m == nil || r == nil {
			return
		}

		m.registry = r
	}
}

// WithWorkload overrides the workload scope set in Config.Workload.
// An empty string clears the workload filter (all healthy instances match).
func WithWorkload(id string) Option {
	return func(m *Manager) {
		if m == nil {
			return
		}

		m.workload = id
	}
}

// New creates a Manager from cfg. Options are applied before the default Consul
// registry is created, so WithRegistry can override the backend entirely.
// Returns ErrEmptyAdvertiseAddr when discovery is enabled but AdvertiseAddr is not set.
func New(cfg Config, opts ...Option) (*Manager, error) {
	cfg = cfg.withDefaults()

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	m := &Manager{
		config:   cfg,
		logger:   cfg.Logger,
		workload: cfg.Workload,
	}

	for _, opt := range opts {
		if opt != nil {
			opt(m)
		}
	}

	if !cfg.Enabled {
		m.logger.Log(context.Background(), log.LevelInfo,
			"service discovery disabled — using direct addresses")

		return m, nil
	}

	if m.registry == nil {
		registry, err := newConsulRegistry(cfg, m.logger)
		if err != nil {
			return nil, fmt.Errorf("lib-service-discovery: %w", err)
		}

		m.registry = registry

		m.logger.Log(context.Background(), log.LevelInfo, "service discovery enabled",
			log.String("consul", cfg.ConsulAddr),
			log.String("advertise", cfg.AdvertiseAddr))
	} else {
		m.logger.Log(context.Background(), log.LevelInfo, "service discovery enabled",
			log.String("registry", "custom"),
			log.String("advertise", cfg.AdvertiseAddr))
	}

	return m, nil
}

// Register publishes svc to the service registry using the configured AdvertiseAddr.
// When a workload is set, it is appended to svc.Tags as "workload=<id>" so that
// Resolve calls from managers with the same workload find this instance.
// No-op when discovery is disabled.
func (m *Manager) Register(ctx context.Context, svc Service) error {
	if m == nil {
		return ErrNilManager
	}

	if !m.config.Enabled {
		return nil
	}

	svc.Address = m.config.AdvertiseAddr
	if m.config.AdvertisePort > 0 {
		svc.Port = m.config.AdvertisePort
	}

	if svc.Scheme == "" && m.config.AdvertiseScheme != "" {
		svc.Scheme = m.config.AdvertiseScheme
	}

	if m.workload != "" {
		// Copy before appending so we never mutate the caller's backing array.
		tags := make([]string, len(svc.Tags), len(svc.Tags)+1)
		copy(tags, svc.Tags)
		svc.Tags = append(tags, "workload="+m.workload)
	}

	// TTL checks need no reachable endpoint; only build the HTTP URL for HTTP checks.
	if svc.HealthCheck != nil && svc.HealthCheck.TTL == "" {
		// Copy the HealthCheck before mutating: svc is a value, but HealthCheck is
		// a pointer shared with the caller, so writing HTTP in place would leak back.
		hc := *svc.HealthCheck
		hc.HTTP = healthCheckURL(svc.Scheme, svc.Address, svc.Port, hc.Path)
		svc.HealthCheck = &hc
	}

	return m.registry.Register(ctx, svc)
}

// RegisterAsync registers svc without blocking the caller and without failing
// startup: it retries in the background with exponential backoff until Register
// succeeds or ctx is cancelled. Use it when the service must come up even if the
// discovery server is briefly unavailable at boot — boot no longer depends on
// Consul being reachable. Pass an app-lifetime ctx (the one cancelled on
// shutdown), not a request-scoped one. No-op when discovery is disabled.
func (m *Manager) RegisterAsync(ctx context.Context, svc Service) {
	if m == nil || !m.config.Enabled {
		return
	}

	go func() {
		for attempt := 0; ; attempt++ {
			if err := m.Register(ctx, svc); err == nil {
				if attempt > 0 {
					m.logger.Log(ctx, log.LevelInfo, "service registered after retry",
						log.String("name", svc.Name),
						log.Int("attempts", attempt))
				}

				return
			} else {
				m.logger.Log(ctx, log.LevelWarn, "register attempt failed; will retry",
					log.String("name", svc.Name),
					log.Int("attempt", attempt),
					log.Err(err))
			}

			if !sleepCtx(ctx, backoffDuration(attempt)) {
				return
			}
		}
	}()
}

// healthCheckURL builds the URL Consul probes for an HTTP health check, honoring
// the service scheme (defaulting to "http") and path (defaulting to "/health").
func healthCheckURL(scheme, addr string, port int, path string) string {
	if scheme == "" {
		scheme = "http"
	}

	if path == "" {
		path = "/health"
	}

	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	return fmt.Sprintf("%s://%s:%d%s", scheme, addr, port, path)
}

// Resolve returns the host:port address of name.
//
//   - Discovery disabled: returns fallback, or ErrDiscoveryDisabledNoFallback when empty.
//   - Discovery enabled, Consul succeeds: returns the first healthy instance address
//     (filtered by workload tag when a workload is configured).
//   - Discovery enabled, Consul fails, fallback provided: returns fallback (logs warning).
//   - Discovery enabled, Consul fails, no fallback: returns the Consul error.
func (m *Manager) Resolve(ctx context.Context, name, fallback string) (string, error) {
	if m == nil {
		return "", ErrNilManager
	}

	if !m.config.Enabled {
		if fallback == "" {
			return "", fmt.Errorf("%w: %q", ErrDiscoveryDisabledNoFallback, name)
		}

		m.logger.Log(ctx, log.LevelInfo, "service resolved",
			log.String("service", name),
			log.String("addr", fallback),
			log.String("source", "fallback"),
			log.String("reason", "discovery disabled"))

		return fallback, nil
	}

	tag := ""
	if m.workload != "" {
		tag = "workload=" + m.workload
	}

	svc, err := m.registry.Resolve(ctx, name, tag)
	if err == nil {
		m.logger.Log(ctx, log.LevelInfo, "service resolved",
			log.String("service", name),
			log.String("addr", svc.Addr()),
			log.String("source", "consul"),
			log.String("workload", m.workload))

		return svc.Addr(), nil
	}

	if fallback != "" {
		m.logger.Log(ctx, log.LevelWarn, "service resolved",
			log.String("service", name),
			log.String("addr", fallback),
			log.String("source", "fallback"),
			log.String("reason", "consul resolve failed"),
			log.Err(err))

		return fallback, nil
	}

	return "", err
}

// ResolveService is like Resolve but returns the full Service struct, giving
// callers access to Scheme and other fields beyond host:port.
// fallback is returned as-is when discovery is disabled or Consul fails;
// an empty fallback.Address is treated as "no fallback".
func (m *Manager) ResolveService(ctx context.Context, name string, fallback Service) (Service, error) {
	if m == nil {
		return Service{}, ErrNilManager
	}

	if !m.config.Enabled {
		if fallback.Address == "" {
			return Service{}, fmt.Errorf("%w: %q", ErrDiscoveryDisabledNoFallback, name)
		}

		m.logger.Log(ctx, log.LevelInfo, "service resolved",
			log.String("service", name),
			log.String("addr", fallback.Addr()),
			log.String("source", "fallback"),
			log.String("reason", "discovery disabled"))

		return fallback, nil
	}

	tag := ""
	if m.workload != "" {
		tag = "workload=" + m.workload
	}

	svc, err := m.registry.Resolve(ctx, name, tag)
	if err == nil {
		m.logger.Log(ctx, log.LevelInfo, "service resolved",
			log.String("service", name),
			log.String("addr", svc.Addr()),
			log.String("source", "consul"),
			log.String("scheme", svc.Scheme),
			log.String("workload", m.workload))

		return svc, nil
	}

	if fallback.Address != "" {
		m.logger.Log(ctx, log.LevelWarn, "service resolved",
			log.String("service", name),
			log.String("addr", fallback.Addr()),
			log.String("source", "fallback"),
			log.String("reason", "consul resolve failed"),
			log.Err(err))

		return fallback, nil
	}

	return Service{}, err
}

// Deregister removes serviceID from the registry.
// No-op when discovery is disabled.
func (m *Manager) Deregister(ctx context.Context, serviceID string) error {
	if m == nil {
		return ErrNilManager
	}

	if !m.config.Enabled {
		return nil
	}

	return m.registry.Deregister(ctx, serviceID)
}

// Watch returns a channel that emits events when the health state of name changes.
// Returns a closed channel (no error) when discovery is disabled.
func (m *Manager) Watch(ctx context.Context, name string) (<-chan Event, error) {
	if m == nil {
		ch := make(chan Event)
		close(ch)

		return ch, ErrNilManager
	}

	if !m.config.Enabled {
		ch := make(chan Event)
		close(ch)

		return ch, nil
	}

	return m.registry.Watch(ctx, name)
}
