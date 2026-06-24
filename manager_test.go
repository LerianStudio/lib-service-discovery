//go:build unit

package libsd

import (
	"context"
	"errors"
	"testing"

	"github.com/LerianStudio/lib-observability/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubRegistry is a minimal in-memory Registry for unit tests.
type stubRegistry struct {
	resolveResult Service
	resolveErr    error
	registerErr   error
	deregisterErr error
}

func (s *stubRegistry) Register(_ context.Context, _ Service) error        { return s.registerErr }
func (s *stubRegistry) Deregister(_ context.Context, _ string) error       { return s.deregisterErr }
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
	assert.ErrorIs(t, err, ErrEmptyAdvertiseAddr)
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
func (c *captureRegistry) Deregister(_ context.Context, _ string) error      { return nil }
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
	capturedTag    string
	resolveResult  Service
}

func (r *captureResolveRegistry) Register(_ context.Context, _ Service) error { return nil }
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
