package libsd

import (
	"context"
	"fmt"

	"github.com/LerianStudio/lib-commons/v5/commons/log"
)

// Manager is the entry point for lib-sd.
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
		registry, err := newConsulRegistry(cfg.ConsulAddr, m.logger)
		if err != nil {
			return nil, fmt.Errorf("lib-sd: %w", err)
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

	if m.workload != "" {
		svc.Tags = append(svc.Tags, "workload="+m.workload)
	}

	if svc.HealthCheck != nil {
		svc.HealthCheck.HTTP = fmt.Sprintf("http://%s:%d/health", svc.Address, svc.Port)
	}

	return m.registry.Register(ctx, svc)
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

		m.logger.Log(ctx, log.LevelDebug, "discovery disabled — using fallback",
			log.String("service", name),
			log.String("fallback", fallback))

		return fallback, nil
	}

	tag := ""
	if m.workload != "" {
		tag = "workload=" + m.workload
	}

	svc, err := m.registry.Resolve(ctx, name, tag)
	if err == nil {
		m.logger.Log(ctx, log.LevelDebug, "consul resolved",
			log.String("service", name),
			log.String("addr", svc.Addr()),
			log.String("workload", m.workload))

		return svc.Addr(), nil
	}

	if fallback != "" {
		m.logger.Log(ctx, log.LevelWarn, "consul resolve failed — using fallback",
			log.String("service", name),
			log.String("fallback", fallback),
			log.Err(err))

		return fallback, nil
	}

	return "", err
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
