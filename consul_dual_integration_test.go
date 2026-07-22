//go:build integration

package libsd

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// externalHost is the fictitious ingress host advertised as the EXTERNAL view.
// Nothing listens on it — the health check always targets the INTERNAL endpoint
// (host.docker.internal:<internalPort>), so the external view is only metadata.
// This makes the two views trivially distinguishable in assertions.
const externalHost = "ext.example.com"

// externalPort is the fictitious ingress port advertised as the EXTERNAL view.
// It is never bound or probed (the check runs against the internal endpoint), so
// its value only has to differ from the internal port to keep views distinct.
const externalPort = 8443

// integrationManagerDual builds a Manager configured for the dual-endpoint
// feature: an EXTERNAL (ingress) view that is only advertised (ext.example.com)
// and an INTERNAL view that is the real, reachable health server
// (host.docker.internal:<internalPort>). Because Register points the health
// check at the internal endpoint, internalPort MUST be a port a startHealthServer
// is actually listening on for the service to become healthy.
func integrationManagerDual(t *testing.T, internalPort int, preferView EndpointView) *Manager {
	t.Helper()

	m, err := New(Config{
		Enabled:               true,
		ConsulAddr:            consulAddr,
		AdvertiseAddr:         externalHost,  // external (ingress) view — advertised only
		AdvertiseInternalAddr: advertiseAddr, // internal view — real/reachable (host.docker.internal)
		AdvertiseInternalPort: internalPort,
		PreferView:            preferView,
		Logger:                nopLogger(),
	})
	require.NoError(t, err)

	// Stop the managed-resolver watch goroutines started by resolve calls.
	t.Cleanup(func() { _ = m.Close() })

	return m
}

// dualService builds the Service registered in the dual-endpoint tests. Port is
// the fictitious EXTERNAL port (Resolve external returns externalHost:externalPort);
// the internal endpoint and the health-check target come from the Manager config.
func dualService(name string) Service {
	return Service{
		ID:          name,
		Name:        name,
		Port:        externalPort,
		Tags:        []string{"integration", "dual"},
		HealthCheck: &HealthCheck{Interval: "2s", Timeout: "1s"},
	}
}

// heldDeadPort binds a TCP listener on a free port and KEEPS it open (closed only
// at test end), returning the port. Holding the listener is deliberate: if we
// released the port (the old freePort behavior) the OS could reuse it for another
// test's health server between release and the Consul probe, making the "dead"
// endpoint accidentally serve 200 and flaking the negative case. Nothing ever
// calls Accept, so an HTTP health check against it connects but never receives a
// response → the check stays critical, exactly as the negative case requires.
func heldDeadPort(t *testing.T) int {
	t.Helper()

	ln, err := net.Listen("tcp", "0.0.0.0:0")
	require.NoError(t, err)

	t.Cleanup(func() { _ = ln.Close() })

	return ln.Addr().(*net.TCPAddr).Port
}

// TestIntegration_DualEndpoint_RegisterSerializesInternalMeta verifies that
// Register serializes the internal endpoint into Consul Meta and that
// ResolveService reconstructs svc.Internal (address + port) from it.
func TestIntegration_DualEndpoint_RegisterSerializesInternalMeta(t *testing.T) {
	internalPort := startHealthServer(t)
	m := integrationManagerDual(t, internalPort, External)
	ctx := context.Background()

	svc := dualService(fmt.Sprintf("dual-meta-%d", internalPort))

	require.NoError(t, m.Register(ctx, svc))
	t.Cleanup(func() { _ = m.Deregister(context.Background(), svc.ID) })

	waitHealthy(t, m, svc.Name, 10*time.Second)

	got, err := m.ResolveService(ctx, svc.Name, Service{})
	require.NoError(t, err)

	require.NotNil(t, got.Internal, "internal endpoint should be reconstructed from Consul Meta")
	assert.Equal(t, advertiseAddr, got.Internal.Address)
	assert.Equal(t, internalPort, got.Internal.Port)

	// The external endpoint must also be reconstructed from Consul Meta, distinct
	// from the internal one.
	require.NotNil(t, got.External, "external endpoint should be reconstructed from Consul Meta")
	assert.Equal(t, externalHost, got.External.Address)
	assert.Equal(t, externalPort, got.External.Port)

	// Both endpoint families must be serialized into Consul Meta.
	assert.Equal(t, externalHost, got.Meta["external_address"])
	assert.Equal(t, strconv.Itoa(externalPort), got.Meta["external_port"])
	assert.Equal(t, advertiseAddr, got.Meta["internal_address"])
	assert.Equal(t, strconv.Itoa(internalPort), got.Meta["internal_port"])

	// reg.Address (the root routable / external view) must stay external, never
	// the internal host.
	assert.Equal(t, externalHost, got.Address)
	assert.Equal(t, externalPort, got.Port)
}

// TestIntegration_DualEndpoint_ResolveEndpointInternalVsExternal verifies that
// ResolveEndpoint returns the external ingress endpoint for External and the
// in-cluster endpoint for Internal, from the same registered instance.
func TestIntegration_DualEndpoint_ResolveEndpointInternalVsExternal(t *testing.T) {
	internalPort := startHealthServer(t)
	m := integrationManagerDual(t, internalPort, External)
	ctx := context.Background()

	svc := dualService(fmt.Sprintf("dual-view-%d", internalPort))

	require.NoError(t, m.Register(ctx, svc))
	t.Cleanup(func() { _ = m.Deregister(context.Background(), svc.ID) })

	waitHealthy(t, m, svc.Name, 10*time.Second)

	ext, err := m.ResolveEndpoint(ctx, svc.Name, External, "")
	require.NoError(t, err)
	assert.Equal(t, fmt.Sprintf("%s:%d", externalHost, externalPort), ext)

	internal, err := m.ResolveEndpoint(ctx, svc.Name, Internal, "")
	require.NoError(t, err)
	assert.Equal(t, fmt.Sprintf("%s:%d", advertiseAddr, internalPort), internal)
}

// TestIntegration_DualEndpoint_ResolvePreferredEndpoint verifies that
// ResolvePreferredEndpoint honors the Manager's configured PreferView: an
// Internal-preferring Manager returns the internal endpoint, an External-preferring
// one returns the external endpoint, for the same registered instance.
func TestIntegration_DualEndpoint_ResolvePreferredEndpoint(t *testing.T) {
	internalPort := startHealthServer(t)
	ctx := context.Background()

	name := fmt.Sprintf("dual-prefer-%d", internalPort)

	// Register once with an Internal-preferring Manager. The registration is
	// PreferView-independent; PreferView only affects the resolve side.
	mInternal := integrationManagerDual(t, internalPort, Internal)
	svc := dualService(name)

	require.NoError(t, mInternal.Register(ctx, svc))
	t.Cleanup(func() { _ = mInternal.Deregister(context.Background(), svc.ID) })

	waitHealthy(t, mInternal, name, 10*time.Second)

	// PreferView: Internal → internal endpoint.
	preferInternal, err := mInternal.ResolvePreferredEndpoint(ctx, name, "")
	require.NoError(t, err)
	assert.Equal(t, fmt.Sprintf("%s:%d", advertiseAddr, internalPort), preferInternal)

	// A second Manager with PreferView: External resolves the same instance's
	// external endpoint. (integrationManagerDual registers its own Close cleanup.)
	mExternal := integrationManagerDual(t, internalPort, External)

	preferExternal, err := mExternal.ResolvePreferredEndpoint(ctx, name, "")
	require.NoError(t, err)
	assert.Equal(t, fmt.Sprintf("%s:%d", externalHost, externalPort), preferExternal)
}

// TestIntegration_DualEndpoint_WatchResolveWithViewInternal verifies that a
// DynamicResolver created with WithView(Internal) tracks the internal endpoint.
func TestIntegration_DualEndpoint_WatchResolveWithViewInternal(t *testing.T) {
	internalPort := startHealthServer(t)
	m := integrationManagerDual(t, internalPort, External)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)

	t.Cleanup(cancel)

	svc := dualService(fmt.Sprintf("dual-watch-%d", internalPort))

	require.NoError(t, m.Register(ctx, svc))
	t.Cleanup(func() { _ = m.Deregister(context.Background(), svc.ID) })

	waitHealthy(t, m, svc.Name, 10*time.Second)

	dr, err := m.WatchResolve(ctx, svc.Name, "", WithView(Internal))
	require.NoError(t, err)
	t.Cleanup(dr.Stop)

	want := fmt.Sprintf("%s:%d", advertiseAddr, internalPort)

	require.Eventually(t, func() bool {
		return dr.Address() == want
	}, 10*time.Second, 200*time.Millisecond, "resolver should track the internal endpoint")
}

// TestIntegration_DualEndpoint_HealthCheckTargetsInternal proves the health check
// is pointed at the INTERNAL endpoint (not the external ingress host, which nothing
// serves): when the internal endpoint serves /health the service becomes healthy.
//
// The negative sub-case registers a service whose internal port has no listener
// and asserts it never becomes healthy within a short window — if the check were
// (incorrectly) targeting the never-served external host it would also stay
// unhealthy, but paired with the positive case above this demonstrates the check
// follows the internal endpoint.
func TestIntegration_DualEndpoint_HealthCheckTargetsInternal(t *testing.T) {
	ctx := context.Background()

	t.Run("internal reachable becomes healthy", func(t *testing.T) {
		internalPort := startHealthServer(t)
		m := integrationManagerDual(t, internalPort, External)

		svc := dualService(fmt.Sprintf("dual-hc-ok-%d", internalPort))

		require.NoError(t, m.Register(ctx, svc))
		t.Cleanup(func() { _ = m.Deregister(context.Background(), svc.ID) })

		waitHealthy(t, m, svc.Name, 10*time.Second)

		_, err := m.Resolve(ctx, svc.Name, "")
		require.NoError(t, err)
	})

	t.Run("internal unreachable never becomes healthy", func(t *testing.T) {
		// No server is started on this port, so the health check (which targets the
		// internal endpoint) can never pass. The listener is held open for the test's
		// lifetime so the OS cannot reuse the port for a real health server.
		deadPort := heldDeadPort(t)
		m := integrationManagerDual(t, deadPort, External)

		svc := dualService(fmt.Sprintf("dual-hc-dead-%d", deadPort))

		require.NoError(t, m.Register(ctx, svc))
		t.Cleanup(func() { _ = m.Deregister(context.Background(), svc.ID) })

		// The check interval is 2s; poll across several intervals and assert the
		// service never reports healthy, proving the check is on the (dead) internal
		// endpoint. If this proves flaky in CI, replace with t.Skip.
		require.Never(t, func() bool {
			_, err := m.Resolve(ctx, svc.Name, "")
			return err == nil
		}, 8*time.Second, 500*time.Millisecond, "service must not become healthy while its internal endpoint is unreachable")
	})
}

// integrationManagerInternalOnly builds a Manager that advertises ONLY an internal
// endpoint (AdvertiseInternalAddr) with no external address — an internal-only
// provider. The internal endpoint is the real, reachable health server
// (host.docker.internal:<internalPort>), so Register's health check (which targets
// the internal endpoint) can pass and the service becomes healthy.
func integrationManagerInternalOnly(t *testing.T, internalPort int) *Manager {
	t.Helper()

	m, err := New(Config{
		Enabled:               true,
		ConsulAddr:            consulAddr,
		AdvertiseInternalAddr: advertiseAddr, // internal view — real/reachable
		AdvertiseInternalPort: internalPort,
		// AdvertiseAddr intentionally empty: no external endpoint is advertised.
		Logger: nopLogger(),
	})
	require.NoError(t, err)

	// Stop the managed-resolver watch goroutines started by resolve calls.
	t.Cleanup(func() { _ = m.Close() })

	return m
}

// TestIntegration_InternalOnly_RegisterAndResolve proves the internal-only
// deployment shape end-to-end: a provider that advertises only an internal
// endpoint (Service.External == nil) registers, becomes healthy, and resolves as
// follows — the Internal view and the legacy Resolve return the internal address,
// while the External view is genuinely unavailable (ErrEndpointViewUnavailable),
// never synthesized from the deprecated flat mirror.
func TestIntegration_InternalOnly_RegisterAndResolve(t *testing.T) {
	internalPort := startHealthServer(t)
	m := integrationManagerInternalOnly(t, internalPort)
	ctx := context.Background()

	svc := Service{
		ID:          fmt.Sprintf("internal-only-%d", internalPort),
		Name:        fmt.Sprintf("internal-only-%d", internalPort),
		Tags:        []string{"integration", "internal-only"},
		HealthCheck: &HealthCheck{Interval: "2s", Timeout: "1s"},
	}

	require.NoError(t, m.Register(ctx, svc))
	t.Cleanup(func() { _ = m.Deregister(context.Background(), svc.ID) })

	waitHealthy(t, m, svc.Name, 10*time.Second)

	want := fmt.Sprintf("%s:%d", advertiseAddr, internalPort)

	// Internal view: resolves to the internal endpoint.
	internal, err := m.ResolveEndpoint(ctx, svc.Name, Internal, "")
	require.NoError(t, err)
	assert.Equal(t, want, internal)

	// External view: unavailable — the provider advertised no external endpoint,
	// and ErrEndpointViewUnavailable must not be papered over by the flat mirror.
	_, err = m.ResolveEndpoint(ctx, svc.Name, External, "")
	require.ErrorIs(t, err, ErrEndpointViewUnavailable)

	// Legacy Resolve: returns the root routable (internal) address, never ":0".
	legacy, err := m.Resolve(ctx, svc.Name, "")
	require.NoError(t, err)
	assert.Equal(t, want, legacy)
}
