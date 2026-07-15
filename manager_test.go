//go:build unit

package libsd

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/LerianStudio/lib-observability/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// captureLogger is a log.Logger that records every Log call, so tests can assert
// that a specific message (e.g. the internal-view degrade warning) was emitted.
type captureLogger struct {
	mu   sync.Mutex
	msgs []string
}

func (c *captureLogger) Log(_ context.Context, _ log.Level, msg string, _ ...log.Field) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.msgs = append(c.msgs, msg)
}

func (c *captureLogger) With(_ ...log.Field) log.Logger { return c }
func (c *captureLogger) WithGroup(_ string) log.Logger  { return c }
func (c *captureLogger) Enabled(_ log.Level) bool       { return true }
func (c *captureLogger) Sync(_ context.Context) error   { return nil }

func (c *captureLogger) has(msg string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, m := range c.msgs {
		if m == msg {
			return true
		}
	}

	return false
}

// stubRegistry is a minimal in-memory Registry for unit tests.
type stubRegistry struct {
	resolveResult Service
	resolveErr    error
	registerErr   error
	deregisterErr error
}

func (s *stubRegistry) Register(_ context.Context, _ Service) error  { return s.registerErr }
func (s *stubRegistry) Deregister(_ context.Context, _ string) error { return s.deregisterErr }
func (s *stubRegistry) Resolve(_ context.Context, _, _ string) (Service, error) {
	return s.resolveResult, s.resolveErr
}
func (s *stubRegistry) Watch(_ context.Context, _ string) (<-chan Event, error) {
	ch := make(chan Event)
	close(ch)
	return ch, nil
}

func disabledManager(t *testing.T) *Manager {
	t.Helper()

	m, err := New(Config{Enabled: false})
	require.NoError(t, err)

	return m
}

func enabledManager(t *testing.T, reg Registry) *Manager {
	t.Helper()

	m, err := New(Config{
		Enabled:       true,
		ConsulAddr:    "localhost:8500",
		AdvertiseAddr: "127.0.0.1",
		Logger:        log.NewNop(),
	}, WithRegistry(reg))
	require.NoError(t, err)

	// Stop any managed-resolver watch goroutines started by resolve calls.
	t.Cleanup(func() { _ = m.Close() })

	return m
}

// ── New ──────────────────────────────────────────────────────────────────────

func TestNew_DisabledSucceeds(t *testing.T) {
	t.Parallel()

	m, err := New(Config{Enabled: false})
	require.NoError(t, err)
	assert.NotNil(t, m)
}

func TestNew_EnabledMissingAdvertiseAddr(t *testing.T) {
	t.Parallel()

	_, err := New(Config{Enabled: true, ConsulAddr: "localhost:8500"})
	assert.ErrorIs(t, err, ErrNoEndpoint)
}

func TestNew_EnabledMissingConsulAddr(t *testing.T) {
	t.Parallel()

	_, err := New(Config{Enabled: true, AdvertiseAddr: "127.0.0.1", ConsulAddr: ""})
	// ConsulAddr gets default "localhost:8500" from withDefaults — validation passes,
	// but NewConsulRegistry returns an error only when the agent is unreachable.
	// This test verifies that withDefaults fills the blank ConsulAddr.
	assert.NotErrorIs(t, err, ErrEmptyConsulAddr)
}

func TestNew_WithRegistryOption(t *testing.T) {
	t.Parallel()

	stub := &stubRegistry{resolveResult: Service{Address: "10.0.0.1", Port: 9000}}

	m, err := New(Config{
		Enabled:       true,
		ConsulAddr:    "localhost:8500",
		AdvertiseAddr: "127.0.0.1",
		Logger:        log.NewNop(),
	}, WithRegistry(stub))
	require.NoError(t, err)
	assert.Equal(t, stub, m.registry)
}

func TestNew_WithRegistryNilIsIgnored(t *testing.T) {
	t.Parallel()

	m, err := New(Config{Enabled: false}, WithRegistry(nil))
	require.NoError(t, err)
	assert.Nil(t, m.registry)
}

func TestNew_WithLoggerOption(t *testing.T) {
	t.Parallel()

	nop := log.NewNop()

	m, err := New(Config{Enabled: false}, WithLogger(nop))
	require.NoError(t, err)
	assert.Equal(t, nop, m.logger)
}

func TestNew_NilOptionIsIgnored(t *testing.T) {
	t.Parallel()

	m, err := New(Config{Enabled: false}, nil)
	require.NoError(t, err)
	assert.NotNil(t, m)
}

// ── Nil receiver ─────────────────────────────────────────────────────────────

func TestNilReceiver_Register(t *testing.T) {
	t.Parallel()

	var m *Manager
	err := m.Register(context.Background(), Service{})
	assert.ErrorIs(t, err, ErrNilManager)
}

func TestNilReceiver_Resolve(t *testing.T) {
	t.Parallel()

	var m *Manager
	_, err := m.Resolve(context.Background(), "svc", "")
	assert.ErrorIs(t, err, ErrNilManager)
}

func TestNilReceiver_Deregister(t *testing.T) {
	t.Parallel()

	var m *Manager
	err := m.Deregister(context.Background(), "svc-1")
	assert.ErrorIs(t, err, ErrNilManager)
}

func TestNilReceiver_Watch(t *testing.T) {
	t.Parallel()

	var m *Manager
	ch, err := m.Watch(context.Background(), "svc")
	assert.ErrorIs(t, err, ErrNilManager)
	assert.NotNil(t, ch)
}

// ── Disabled mode ─────────────────────────────────────────────────────────────

func TestDisabled_RegisterIsNoop(t *testing.T) {
	t.Parallel()

	m := disabledManager(t)
	assert.NoError(t, m.Register(context.Background(), Service{Name: "svc"}))
}

func TestDisabled_DeregisterIsNoop(t *testing.T) {
	t.Parallel()

	m := disabledManager(t)
	assert.NoError(t, m.Deregister(context.Background(), "svc-1"))
}

func TestDisabled_ResolveReturnsFallback(t *testing.T) {
	t.Parallel()

	m := disabledManager(t)
	addr, err := m.Resolve(context.Background(), "svc-b", "svc-b:8082")
	require.NoError(t, err)
	assert.Equal(t, "svc-b:8082", addr)
}

func TestDisabled_ResolveNoFallbackErrors(t *testing.T) {
	t.Parallel()

	m := disabledManager(t)
	_, err := m.Resolve(context.Background(), "svc-b", "")
	assert.ErrorIs(t, err, ErrDiscoveryDisabledNoFallback)
}

func TestDisabled_WatchReturnsClosedChannel(t *testing.T) {
	t.Parallel()

	m := disabledManager(t)
	ch, err := m.Watch(context.Background(), "svc")
	require.NoError(t, err)

	_, open := <-ch
	assert.False(t, open, "channel must be closed when discovery is disabled")
}

// ── Enabled mode ──────────────────────────────────────────────────────────────

func TestEnabled_ResolveUsesRegistry(t *testing.T) {
	t.Parallel()

	stub := &stubRegistry{resolveResult: Service{Address: "10.0.0.1", Port: 8082}}
	m := enabledManager(t, stub)

	addr, err := m.Resolve(context.Background(), "svc-b", "")
	require.NoError(t, err)
	assert.Equal(t, "10.0.0.1:8082", addr)
}

func TestEnabled_ResolveFallsBackOnRegistryError(t *testing.T) {
	t.Parallel()

	stub := &stubRegistry{resolveErr: errors.New("consul down")}
	m := enabledManager(t, stub)

	addr, err := m.Resolve(context.Background(), "svc-b", "svc-b:8082")
	require.NoError(t, err)
	assert.Equal(t, "svc-b:8082", addr)
}

func TestEnabled_ResolveErrorsWhenNoFallback(t *testing.T) {
	t.Parallel()

	stub := &stubRegistry{resolveErr: ErrNoHealthyInstances}
	m := enabledManager(t, stub)

	_, err := m.Resolve(context.Background(), "svc-b", "")
	assert.ErrorIs(t, err, ErrNoHealthyInstances)
}

func TestEnabled_RegisterSetsAdvertiseAddr(t *testing.T) {
	t.Parallel()

	var registered Service

	stub := &stubRegistry{}
	stub.registerErr = nil

	m := enabledManager(t, &captureRegistry{onRegister: func(svc Service) { registered = svc }})
	m.config.AdvertiseAddr = "10.0.0.2"

	err := m.Register(context.Background(), Service{Name: "svc-a", Port: 8081})
	require.NoError(t, err)
	assert.Equal(t, "10.0.0.2", registered.Address)
}

// captureRegistry records the last Register call for assertion.
type captureRegistry struct {
	onRegister func(Service)
}

func (c *captureRegistry) Register(_ context.Context, svc Service) error {
	if c.onRegister != nil {
		c.onRegister(svc)
	}
	return nil
}
func (c *captureRegistry) Deregister(_ context.Context, _ string) error { return nil }
func (c *captureRegistry) Resolve(_ context.Context, _, _ string) (Service, error) {
	return Service{}, nil
}
func (c *captureRegistry) Watch(_ context.Context, _ string) (<-chan Event, error) {
	ch := make(chan Event)
	close(ch)
	return ch, nil
}

// captureResolveRegistry records the tag argument passed to Resolve.
type captureResolveRegistry struct {
	capturedTag   string
	resolveResult Service
}

func (r *captureResolveRegistry) Register(_ context.Context, _ Service) error  { return nil }
func (r *captureResolveRegistry) Deregister(_ context.Context, _ string) error { return nil }
func (r *captureResolveRegistry) Resolve(_ context.Context, _, tag string) (Service, error) {
	r.capturedTag = tag
	return r.resolveResult, nil
}
func (r *captureResolveRegistry) Watch(_ context.Context, _ string) (<-chan Event, error) {
	ch := make(chan Event)
	close(ch)
	return ch, nil
}

// ── Workload filtering ────────────────────────────────────────────────────────

func TestWorkload_RegisterAddsTag(t *testing.T) {
	t.Parallel()

	var registered Service

	m := enabledManager(t, &captureRegistry{onRegister: func(svc Service) { registered = svc }})
	m.workload = "tenant-a"

	err := m.Register(context.Background(), Service{Name: "svc-a", Port: 8081})
	require.NoError(t, err)
	assert.Contains(t, registered.Tags, "workload=tenant-a")
}

func TestWorkload_RegisterNoTagWhenEmpty(t *testing.T) {
	t.Parallel()

	var registered Service

	m := enabledManager(t, &captureRegistry{onRegister: func(svc Service) { registered = svc }})

	err := m.Register(context.Background(), Service{Name: "svc-a", Port: 8081})
	require.NoError(t, err)

	for _, tag := range registered.Tags {
		assert.NotContains(t, tag, "workload=")
	}
}

func TestWorkload_ResolvePassesTag(t *testing.T) {
	t.Parallel()

	cap := &captureResolveRegistry{resolveResult: Service{Address: "10.0.0.1", Port: 8080}}
	m := enabledManager(t, cap)
	m.workload = "tenant-a"

	_, err := m.Resolve(context.Background(), "svc-b", "")
	require.NoError(t, err)
	assert.Equal(t, "workload=tenant-a", cap.capturedTag)
}

func TestWorkload_ResolveNoTagWhenEmpty(t *testing.T) {
	t.Parallel()

	cap := &captureResolveRegistry{resolveResult: Service{Address: "10.0.0.1", Port: 8080}}
	m := enabledManager(t, cap)

	_, err := m.Resolve(context.Background(), "svc-b", "")
	require.NoError(t, err)
	assert.Equal(t, "", cap.capturedTag)
}

func TestWorkload_WithWorkloadOption(t *testing.T) {
	t.Parallel()

	m, err := New(Config{Enabled: false}, WithWorkload("tenant-b"))
	require.NoError(t, err)
	assert.Equal(t, "tenant-b", m.workload)
}

func TestWorkload_WithWorkloadClearsWhenEmpty(t *testing.T) {
	t.Parallel()

	m, err := New(Config{Enabled: false, Workload: "tenant-a"}, WithWorkload(""))
	require.NoError(t, err)
	assert.Equal(t, "", m.workload)
}

// ── Health check URL (#2: honor scheme + configurable path) ─────────────────────

// registerCapture registers svc on a captureRegistry manager and returns the
// Service the registry actually received.
func registerCapture(t *testing.T, m *Manager, svc Service) Service {
	t.Helper()

	var got Service

	m.registry = &captureRegistry{onRegister: func(s Service) { got = s }}
	require.NoError(t, m.Register(context.Background(), svc))

	return got
}

func TestRegister_HealthCheckURLDefaultSchemeAndPath(t *testing.T) {
	t.Parallel()

	m := enabledManager(t, &captureRegistry{})
	m.config.AdvertiseAddr = "10.0.0.2"

	got := registerCapture(t, m, Service{Name: "svc-a", Port: 8081,
		HealthCheck: &HealthCheck{Interval: "2s", Timeout: "1s"}})

	assert.Equal(t, "http://10.0.0.2:8081/health", got.HealthCheck.HTTP)
}

func TestRegister_HealthCheckURLHonorsHTTPSScheme(t *testing.T) {
	t.Parallel()

	m := enabledManager(t, &captureRegistry{})
	m.config.AdvertiseAddr = "10.0.0.2"
	m.config.AdvertiseScheme = "https"

	got := registerCapture(t, m, Service{Name: "svc-a", Port: 8081,
		HealthCheck: &HealthCheck{Interval: "2s", Timeout: "1s"}})

	assert.Equal(t, "https://10.0.0.2:8081/health", got.HealthCheck.HTTP)
}

func TestRegister_HealthCheckURLCustomPath(t *testing.T) {
	t.Parallel()

	m := enabledManager(t, &captureRegistry{})
	m.config.AdvertiseAddr = "10.0.0.2"

	got := registerCapture(t, m, Service{Name: "svc-a", Port: 8081,
		HealthCheck: &HealthCheck{Interval: "2s", Timeout: "1s", Path: "/healthz"}})

	assert.Equal(t, "http://10.0.0.2:8081/healthz", got.HealthCheck.HTTP)
}

func TestRegister_HealthCheckURLAddsLeadingSlashToPath(t *testing.T) {
	t.Parallel()

	m := enabledManager(t, &captureRegistry{})
	m.config.AdvertiseAddr = "10.0.0.2"

	got := registerCapture(t, m, Service{Name: "svc-a", Port: 8081,
		HealthCheck: &HealthCheck{Interval: "2s", Timeout: "1s", Path: "ping"}})

	assert.Equal(t, "http://10.0.0.2:8081/ping", got.HealthCheck.HTTP)
}

func TestRegister_TTLCheckSkipsHTTPURL(t *testing.T) {
	t.Parallel()

	m := enabledManager(t, &captureRegistry{})
	m.config.AdvertiseAddr = "10.0.0.2"

	got := registerCapture(t, m, Service{Name: "svc-a", Port: 8081,
		HealthCheck: &HealthCheck{TTL: "30s"}})

	assert.Equal(t, "", got.HealthCheck.HTTP, "TTL checks need no HTTP endpoint")
}

// ── Internal endpoint population + health-check retargeting (Task 1.3.1) ─────────

func TestRegister_InternalEndpointPopulatedFromConfig(t *testing.T) {
	t.Parallel()

	m := enabledManager(t, &captureRegistry{})
	m.config.AdvertiseAddr = "10.0.0.2"
	m.config.AdvertiseInternalAddr = "svc.ns.svc.cluster.local"
	m.config.AdvertiseInternalPort = 9090
	m.config.AdvertiseInternalScheme = "https"

	got := registerCapture(t, m, Service{Name: "svc-a", Port: 8081,
		HealthCheck: &HealthCheck{Interval: "2s", Timeout: "1s"}})

	require.NotNil(t, got.Internal, "internal endpoint must be populated when advertised")
	assert.Equal(t, &Endpoint{Address: "svc.ns.svc.cluster.local", Port: 9090, Scheme: "https"}, got.Internal)

	// Health check must target the internal endpoint when present.
	assert.Equal(t, "https://svc.ns.svc.cluster.local:9090/health", got.HealthCheck.HTTP)
}

func TestRegister_InternalEndpointDefaultsPortAndScheme(t *testing.T) {
	t.Parallel()

	m := enabledManager(t, &captureRegistry{})
	m.config.AdvertiseAddr = "10.0.0.2"
	m.config.AdvertiseInternalAddr = "svc.ns.svc.cluster.local"
	// No internal port (0) → defaults to external Register port.
	// No internal scheme ("") → defaults to "http" (in-cluster plaintext).

	got := registerCapture(t, m, Service{Name: "svc-a", Port: 8081,
		HealthCheck: &HealthCheck{Interval: "2s", Timeout: "1s"}})

	require.NotNil(t, got.Internal)
	assert.Equal(t, &Endpoint{Address: "svc.ns.svc.cluster.local", Port: 8081, Scheme: "http"}, got.Internal)
	assert.Equal(t, "http://svc.ns.svc.cluster.local:8081/health", got.HealthCheck.HTTP)
}

func TestRegister_NoInternalConfigKeepsExternalHealthCheck(t *testing.T) {
	t.Parallel()

	m := enabledManager(t, &captureRegistry{})
	m.config.AdvertiseAddr = "10.0.0.2"
	// No internal config at all.

	got := registerCapture(t, m, Service{Name: "svc-a", Port: 8081,
		HealthCheck: &HealthCheck{Interval: "2s", Timeout: "1s"}})

	assert.Nil(t, got.Internal, "internal endpoint must stay nil when not advertised")
	// Regression: health check still targets the external endpoint.
	assert.Equal(t, "http://10.0.0.2:8081/health", got.HealthCheck.HTTP)
}

func TestRegister_TTLCheckStillPopulatesInternalWithoutHTTP(t *testing.T) {
	t.Parallel()

	m := enabledManager(t, &captureRegistry{})
	m.config.AdvertiseAddr = "10.0.0.2"
	m.config.AdvertiseInternalAddr = "svc.ns.svc.cluster.local"
	m.config.AdvertiseInternalPort = 9090
	m.config.AdvertiseInternalScheme = "https"

	got := registerCapture(t, m, Service{Name: "svc-a", Port: 8081,
		HealthCheck: &HealthCheck{TTL: "30s"}})

	// Internal is populated regardless of health-check mode.
	require.NotNil(t, got.Internal)
	assert.Equal(t, &Endpoint{Address: "svc.ns.svc.cluster.local", Port: 9090, Scheme: "https"}, got.Internal)
	// TTL checks carry no HTTP URL — unchanged behavior.
	assert.Equal(t, "", got.HealthCheck.HTTP, "TTL checks need no HTTP endpoint")
}

func TestRegister_InternalPortDefaultsToAdvertisedExternalPort(t *testing.T) {
	t.Parallel()

	m := enabledManager(t, &captureRegistry{})
	m.config.AdvertiseAddr = "10.0.0.2"
	// Advertised external port overrides the caller's Register port (5000 below).
	m.config.AdvertisePort = 8081
	m.config.AdvertiseInternalAddr = "svc.ns.svc.cluster.local"
	// No internal port (0) → must default to the ADVERTISED external port (8081),
	// not the caller's original svc.Port (5000).
	m.config.AdvertiseInternalPort = 0

	got := registerCapture(t, m, Service{Name: "svc-a", Port: 5000,
		HealthCheck: &HealthCheck{Interval: "2s", Timeout: "1s"}})

	require.NotNil(t, got.Internal)
	assert.Equal(t, m.config.AdvertisePort, got.Internal.Port,
		"internal port must default to the advertised external port, not the caller's svc.Port")
	assert.NotEqual(t, 5000, got.Internal.Port,
		"internal port must not use the caller's original Register port")
	assert.Equal(t, "http://svc.ns.svc.cluster.local:8081/health", got.HealthCheck.HTTP)
}

// TestRegister_ConfigInternalOnlyDropsCallerExternal proves #14: the library
// derives External/Internal SOLELY from config. A caller-supplied External must
// NOT survive an internal-only config — otherwise a service could publish an
// unauthorized external endpoint it was never configured to advertise.
func TestRegister_ConfigInternalOnlyDropsCallerExternal(t *testing.T) {
	t.Parallel()

	m := enabledManager(t, &captureRegistry{})
	m.config.AdvertiseAddr = "" // internal-only config: no external endpoint
	m.config.AdvertiseScheme = ""
	m.config.AdvertiseInternalAddr = "svc.ns.svc.cluster.local"
	m.config.AdvertiseInternalPort = 9090
	m.config.AdvertiseInternalScheme = "http"

	// The caller tries to smuggle in an External (and an Internal) endpoint.
	got := registerCapture(t, m, Service{
		Name: "svc-a", Port: 8081,
		External:    &Endpoint{Address: "attacker.example.com", Port: 443, Scheme: "https"},
		Internal:    &Endpoint{Address: "caller.internal", Port: 1, Scheme: "https"},
		HealthCheck: &HealthCheck{Interval: "2s", Timeout: "1s"},
	})

	assert.Nil(t, got.External,
		"an internal-only config must not publish a caller-supplied External")

	require.NotNil(t, got.Internal, "internal endpoint must be derived from config")
	assert.Equal(t, &Endpoint{Address: "svc.ns.svc.cluster.local", Port: 9090, Scheme: "http"}, got.Internal,
		"Internal must come from config, not the caller's smuggled pointer")
}

// TestRegister_ConfigExternalOnlyDropsCallerInternal is the mirror of #14: a
// caller-supplied Internal must not survive an external-only config.
func TestRegister_ConfigExternalOnlyDropsCallerInternal(t *testing.T) {
	t.Parallel()

	m := enabledManager(t, &captureRegistry{})
	m.config.AdvertiseAddr = "10.0.0.2"
	// No internal config advertised.

	got := registerCapture(t, m, Service{
		Name: "svc-a", Port: 8081,
		Internal:    &Endpoint{Address: "caller.internal", Port: 9090, Scheme: "http"},
		HealthCheck: &HealthCheck{Interval: "2s", Timeout: "1s"},
	})

	assert.Nil(t, got.Internal,
		"an external-only config must not publish a caller-supplied Internal")
	require.NotNil(t, got.External)
	assert.Equal(t, "10.0.0.2", got.External.Address)
}

// ── Caller aliasing (#5: Register must not mutate the caller's inputs) ───────────

func TestRegister_DoesNotMutateCallerHealthCheck(t *testing.T) {
	t.Parallel()

	m := enabledManager(t, &captureRegistry{})
	m.config.AdvertiseAddr = "10.0.0.2"

	hc := &HealthCheck{Interval: "2s", Timeout: "1s"}
	got := registerCapture(t, m, Service{Name: "svc-a", Port: 8081, HealthCheck: hc})

	require.Equal(t, "http://10.0.0.2:8081/health", got.HealthCheck.HTTP)
	assert.Equal(t, "", hc.HTTP, "caller's HealthCheck pointer must not be mutated")
}

func TestRegister_DoesNotMutateCallerTags(t *testing.T) {
	t.Parallel()

	m := enabledManager(t, &captureRegistry{})
	m.config.AdvertiseAddr = "10.0.0.2"
	m.workload = "tenant-a"

	// A slice with spare capacity is where an in-place append would leak.
	orig := make([]string, 1, 4)
	orig[0] = "region=us"

	got := registerCapture(t, m, Service{Name: "svc-a", Port: 8081, Tags: orig})

	assert.Contains(t, got.Tags, "workload=tenant-a")
	assert.Equal(t, []string{"region=us"}, orig, "caller's Tags slice must be unchanged")

	// The workload tag must not have leaked into the caller's backing array.
	for _, tag := range orig[:cap(orig)] {
		assert.NotContains(t, tag, "workload=")
	}
}

// ── ResolveEndpoint (Task 1.4.2) ────────────────────────────────────────────────

func TestResolveEndpoint(t *testing.T) {
	t.Parallel()

	// extSvc advertises only the external endpoint (Internal nil).
	extSvc := Service{External: &Endpoint{Address: "10.0.0.1", Port: 8082, Scheme: "https"}}
	// intSvc advertises both external and in-cluster endpoints.
	intSvc := Service{
		External: &Endpoint{Address: "10.0.0.1", Port: 8082, Scheme: "https"},
		Internal: &Endpoint{Address: "svc.ns.svc.cluster.local", Port: 9090, Scheme: "http"},
	}

	tests := []struct {
		name       string
		enabled    bool
		resolveRes Service
		resolveErr error
		view       EndpointView
		fallback   string
		wantAddr   string
		wantErrIs  error
	}{
		{ // (a)
			name:     "disabled with fallback returns fallback",
			enabled:  false,
			view:     External,
			fallback: "svc-b:8082",
			wantAddr: "svc-b:8082",
		},
		{ // (b)
			name:      "disabled without fallback errors",
			enabled:   false,
			view:      External,
			fallback:  "",
			wantErrIs: ErrDiscoveryDisabledNoFallback,
		},
		{ // (c)
			name:       "enabled internal view returns internal addr",
			enabled:    true,
			resolveRes: intSvc,
			view:       Internal,
			wantAddr:   "svc.ns.svc.cluster.local:9090",
		},
		{ // (d)
			name:       "enabled internal view falls back to external when internal nil",
			enabled:    true,
			resolveRes: extSvc,
			view:       Internal,
			wantAddr:   "10.0.0.1:8082",
		},
		{ // (e)
			name:       "enabled external view returns external addr",
			enabled:    true,
			resolveRes: intSvc,
			view:       External,
			wantAddr:   "10.0.0.1:8082",
		},
		{ // (f)
			name:       "enabled registry error with fallback returns fallback",
			enabled:    true,
			resolveErr: errors.New("consul down"),
			view:       Internal,
			fallback:   "svc-b:8082",
			wantAddr:   "svc-b:8082",
		},
		{ // (g)
			name:       "enabled registry error without fallback returns error",
			enabled:    true,
			resolveErr: ErrNoHealthyInstances,
			view:       Internal,
			wantErrIs:  ErrNoHealthyInstances,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var m *Manager
			if tt.enabled {
				m = enabledManager(t, &stubRegistry{resolveResult: tt.resolveRes, resolveErr: tt.resolveErr})
			} else {
				m = disabledManager(t)
			}

			addr, err := m.ResolveEndpoint(context.Background(), "svc-b", tt.view, tt.fallback)

			if tt.wantErrIs != nil {
				require.ErrorIs(t, err, tt.wantErrIs)
				assert.Empty(t, addr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantAddr, addr)
		})
	}
}

func TestResolveService(t *testing.T) {
	t.Parallel()

	hit := Service{Address: "10.0.0.1", Port: 8082, Scheme: "https"}
	fb := Service{Address: "svc-b", Port: 8082, Scheme: "http"}

	tests := []struct {
		name       string
		enabled    bool
		resolveRes Service
		resolveErr error
		fallback   Service
		wantAddr   string
		wantScheme string
		wantErrIs  error
	}{
		{
			name:       "disabled with fallback returns fallback verbatim",
			enabled:    false,
			fallback:   fb,
			wantAddr:   "svc-b:8082",
			wantScheme: "http",
		},
		{
			name:      "disabled without fallback errors",
			enabled:   false,
			fallback:  Service{},
			wantErrIs: ErrDiscoveryDisabledNoFallback,
		},
		{
			name:       "enabled hit returns resolved service",
			enabled:    true,
			resolveRes: hit,
			wantAddr:   "10.0.0.1:8082",
			wantScheme: "https",
		},
		{
			name:       "enabled registry error with fallback returns fallback",
			enabled:    true,
			resolveErr: errors.New("consul down"),
			fallback:   fb,
			wantAddr:   "svc-b:8082",
			wantScheme: "http",
		},
		{
			name:       "enabled registry error without fallback returns error",
			enabled:    true,
			resolveErr: ErrNoHealthyInstances,
			fallback:   Service{},
			wantErrIs:  ErrNoHealthyInstances,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var m *Manager
			if tt.enabled {
				m = enabledManager(t, &stubRegistry{resolveResult: tt.resolveRes, resolveErr: tt.resolveErr})
			} else {
				m = disabledManager(t)
			}

			svc, err := m.ResolveService(context.Background(), "svc-b", tt.fallback)

			if tt.wantErrIs != nil {
				require.ErrorIs(t, err, tt.wantErrIs)
				assert.Equal(t, Service{}, svc)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantAddr, svc.Addr())
			assert.Equal(t, tt.wantScheme, svc.Scheme)
		})
	}
}

func TestNilReceiver_ResolveService(t *testing.T) {
	t.Parallel()

	var m *Manager
	_, err := m.ResolveService(context.Background(), "svc", Service{})
	assert.ErrorIs(t, err, ErrNilManager)
}

// ── ResolvePreferredEndpoint (Epic 2.2) ─────────────────────────────────────────

func TestResolvePreferredEndpoint(t *testing.T) {
	t.Parallel()

	// extSvc advertises only the external endpoint (Internal nil).
	extSvc := Service{External: &Endpoint{Address: "10.0.0.1", Port: 8082, Scheme: "https"}}
	// intSvc advertises both external and in-cluster endpoints.
	intSvc := Service{
		External: &Endpoint{Address: "10.0.0.1", Port: 8082, Scheme: "https"},
		Internal: &Endpoint{Address: "svc.ns.svc.cluster.local", Port: 9090, Scheme: "http"},
	}

	tests := []struct {
		name       string
		preferView EndpointView
		resolveRes Service
		wantAddr   string
	}{
		{
			name:       "prefer internal returns internal addr",
			preferView: Internal,
			resolveRes: intSvc,
			wantAddr:   "svc.ns.svc.cluster.local:9090",
		},
		{
			name:       "prefer external returns external addr",
			preferView: External,
			resolveRes: intSvc,
			wantAddr:   "10.0.0.1:8082",
		},
		{
			name:       "prefer internal degrades to external when internal nil",
			preferView: Internal,
			resolveRes: extSvc,
			wantAddr:   "10.0.0.1:8082",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m, err := New(Config{
				Enabled:       true,
				ConsulAddr:    "localhost:8500",
				AdvertiseAddr: "127.0.0.1",
				PreferView:    tt.preferView,
				Logger:        log.NewNop(),
			}, WithRegistry(&stubRegistry{resolveResult: tt.resolveRes}))
			require.NoError(t, err)

			addr, err := m.ResolvePreferredEndpoint(context.Background(), "svc-b", "")
			require.NoError(t, err)
			assert.Equal(t, tt.wantAddr, addr)
		})
	}
}

func TestNilReceiver_ResolvePreferredEndpoint(t *testing.T) {
	t.Parallel()

	var m *Manager
	_, err := m.ResolvePreferredEndpoint(context.Background(), "svc", "")
	assert.ErrorIs(t, err, ErrNilManager)
}

// ── Symmetric endpoints: register external-only / internal-only (Epic 3.5) ───────

// internalOnlyManager builds an enabled Manager whose config advertises ONLY an
// internal endpoint (no external), registering svc on a captureRegistry.
func internalOnlyManager(t *testing.T, onRegister func(Service)) *Manager {
	t.Helper()

	m, err := New(Config{
		Enabled:                 true,
		ConsulAddr:              "localhost:8500",
		AdvertiseInternalAddr:   "svc.ns.svc.cluster.local",
		AdvertiseInternalPort:   9090,
		AdvertiseInternalScheme: "http",
		Logger:                  log.NewNop(),
	}, WithRegistry(&captureRegistry{onRegister: onRegister}))
	require.NoError(t, err)

	return m
}

func TestRegister_InternalOnly_ExternalNilRootRoutable(t *testing.T) {
	t.Parallel()

	var got Service

	m := internalOnlyManager(t, func(s Service) { got = s })

	require.NoError(t, m.Register(context.Background(), Service{Name: "svc-a", Port: 8081,
		HealthCheck: &HealthCheck{Interval: "2s", Timeout: "1s"}}))

	assert.Nil(t, got.External, "internal-only registration must leave External nil")
	require.NotNil(t, got.Internal)
	assert.Equal(t, &Endpoint{Address: "svc.ns.svc.cluster.local", Port: 9090, Scheme: "http"}, got.Internal)

	// The deprecated flat mirror reflects the root routable endpoint. With no
	// external endpoint the root is Internal, so the flat address is the routable
	// in-cluster address (never ":0"), and the legacy Resolve path stays usable.
	assert.Equal(t, "svc.ns.svc.cluster.local", got.Address)
	assert.Equal(t, 9090, got.Port)
	assert.Equal(t, "http", got.Scheme)

	// The health check probes the internal endpoint.
	assert.Equal(t, "http://svc.ns.svc.cluster.local:9090/health", got.HealthCheck.HTTP)
}

func TestRegister_ExternalOnly_InternalNil(t *testing.T) {
	t.Parallel()

	var got Service

	m := enabledManager(t, &captureRegistry{onRegister: func(s Service) { got = s }})
	m.config.AdvertiseAddr = "fees.example.net"
	m.config.AdvertiseScheme = "https"

	require.NoError(t, m.Register(context.Background(), Service{Name: "svc-a", Port: 8081,
		HealthCheck: &HealthCheck{Interval: "2s", Timeout: "1s"}}))

	assert.Nil(t, got.Internal, "external-only registration must leave Internal nil")
	require.NotNil(t, got.External)
	assert.Equal(t, &Endpoint{Address: "fees.example.net", Port: 8081, Scheme: "https"}, got.External)
	// The flat mirror reflects the external endpoint.
	assert.Equal(t, "fees.example.net", got.Address)
	// Health check degrades to the external endpoint (no internal advertised).
	assert.Equal(t, "https://fees.example.net:8081/health", got.HealthCheck.HTTP)
}

// ── ResolveEndpoint: External view against internal-only provider (Epic 3.4) ─────

func TestResolveEndpoint_ExternalViewInternalOnlyIsUnavailable(t *testing.T) {
	t.Parallel()

	// Provider advertised ONLY an internal endpoint (External nil, flat zero).
	internalOnly := Service{Internal: &Endpoint{Address: "svc.ns.svc.cluster.local", Port: 9090, Scheme: "http"}}

	m := enabledManager(t, &stubRegistry{resolveResult: internalOnly})

	_, err := m.ResolveEndpoint(context.Background(), "svc-b", External, "")
	assert.ErrorIs(t, err, ErrEndpointViewUnavailable,
		"External view against an internal-only provider must be unavailable")
}

func TestResolveEndpoint_ExternalViewInternalOnlyUsesFallback(t *testing.T) {
	t.Parallel()

	internalOnly := Service{Internal: &Endpoint{Address: "svc.ns.svc.cluster.local", Port: 9090, Scheme: "http"}}

	m := enabledManager(t, &stubRegistry{resolveResult: internalOnly})

	addr, err := m.ResolveEndpoint(context.Background(), "svc-b", External, "fallback:8082")
	require.NoError(t, err)
	assert.Equal(t, "fallback:8082", addr, "view-unavailable must degrade to the fallback when provided")
}

// An internal-only provider REBUILT through the real read path (serviceMeta ->
// serviceFromEntry, not a hand-built literal) must be unavailable for the External
// view, while the legacy Resolve returns the routable internal address (never ":0").
func TestResolveEndpoint_ExternalViewInternalOnlyReconstructed(t *testing.T) {
	t.Parallel()

	meta := serviceMeta(Service{Internal: &Endpoint{Address: "svc.ns.svc.cluster.local", Port: 9090, Scheme: "http"}})
	reconstructed := serviceFromEntry(entryWithMeta(meta))

	require.Nil(t, reconstructed.External, "internal-only round-trip must leave External nil")
	require.NotNil(t, reconstructed.Internal)

	m := enabledManager(t, &stubRegistry{resolveResult: reconstructed})

	// External view: unavailable, no fallback -> surfaces the view error.
	_, err := m.ResolveEndpoint(context.Background(), "svc-a", External, "")
	assert.ErrorIs(t, err, ErrEndpointViewUnavailable,
		"External view against a real internal-only provider must be unavailable")

	// Legacy Resolve reads the flat mirror -> routable internal address, never ":0".
	addr, err := m.Resolve(context.Background(), "svc-a", "")
	require.NoError(t, err)
	assert.Equal(t, "svc.ns.svc.cluster.local:9090", addr,
		"legacy Resolve against internal-only must return the internal address, never :0")
}

func TestResolveEndpoint_InternalDegradeEmitsWarn(t *testing.T) {
	t.Parallel()

	// External advertised, no internal: Internal view degrades to external.
	extSvc := Service{External: &Endpoint{Address: "10.0.0.1", Port: 8082, Scheme: "https"}}

	cap := &captureLogger{}

	m, err := New(Config{
		Enabled:       true,
		ConsulAddr:    "localhost:8500",
		AdvertiseAddr: "127.0.0.1",
		Logger:        cap,
	}, WithRegistry(&stubRegistry{resolveResult: extSvc}))
	require.NoError(t, err)

	addr, err := m.ResolveEndpoint(context.Background(), "svc-b", Internal, "")
	require.NoError(t, err)
	assert.Equal(t, "10.0.0.1:8082", addr)
	assert.True(t, cap.has("internal view degraded to external"),
		"a real internal->external degrade must emit a warning")
}

func TestResolveEndpoint_InternalHitDoesNotWarn(t *testing.T) {
	t.Parallel()

	// Provider advertises a genuine internal endpoint: no degrade, no warning.
	intSvc := Service{
		Address: "10.0.0.1", Port: 8082, Scheme: "https",
		Internal: &Endpoint{Address: "svc.ns.svc.cluster.local", Port: 9090, Scheme: "http"},
	}

	cap := &captureLogger{}

	m, err := New(Config{
		Enabled:       true,
		ConsulAddr:    "localhost:8500",
		AdvertiseAddr: "127.0.0.1",
		Logger:        cap,
	}, WithRegistry(&stubRegistry{resolveResult: intSvc}))
	require.NoError(t, err)

	addr, err := m.ResolveEndpoint(context.Background(), "svc-b", Internal, "")
	require.NoError(t, err)
	assert.Equal(t, "svc.ns.svc.cluster.local:9090", addr)
	assert.False(t, cap.has("internal view degraded to external"),
		"a genuine internal hit must not emit the degrade warning")
}
