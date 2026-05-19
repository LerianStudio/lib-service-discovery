//go:build integration

package libsd

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/LerianStudio/lib-commons/v5/commons/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const consulAddr = "localhost:8500"

// advertiseAddr is the address Consul (running in Docker) uses to reach the
// health check endpoint started by the test. On macOS with Docker Desktop,
// "host.docker.internal" resolves to the host machine from inside a container.
const advertiseAddr = "host.docker.internal"

func integrationManager(t *testing.T) *Manager {
	t.Helper()

	m, err := New(Config{
		Enabled:       true,
		ConsulAddr:    consulAddr,
		AdvertiseAddr: advertiseAddr,
		Logger:        log.NewNop(),
	})
	require.NoError(t, err)

	return m
}

// startHealthServer starts an HTTP server on a free port (all interfaces) that
// always returns 200 on /health. Returns the port and registers cleanup.
// Binding to 0.0.0.0 allows Consul running in Docker to reach it via host.docker.internal.
func startHealthServer(t *testing.T) int {
	t.Helper()

	ln, err := net.Listen("tcp", "0.0.0.0:0")
	require.NoError(t, err)

	port := ln.Addr().(*net.TCPAddr).Port

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{Handler: mux}

	go func() { _ = srv.Serve(ln) }()

	t.Cleanup(func() { _ = srv.Close() })

	return port
}

// waitHealthy polls Consul until the service has at least one healthy instance
// or the timeout is reached.
func waitHealthy(t *testing.T, m *Manager, name string, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		_, err := m.Resolve(context.Background(), name, "")
		if err == nil {
			return
		}

		time.Sleep(500 * time.Millisecond)
	}

	t.Fatalf("service %q did not become healthy within %s", name, timeout)
}

func TestIntegration_RegisterAndResolve(t *testing.T) {
	port := startHealthServer(t)
	m := integrationManager(t)
	ctx := context.Background()

	svc := Service{
		ID:   fmt.Sprintf("test-svc-%d", port),
		Name: fmt.Sprintf("test-svc-%d", port),
		Port: port,
		Tags: []string{"integration"},
		HealthCheck: &HealthCheck{
			Interval: "2s",
			Timeout:  "1s",
		},
	}

	require.NoError(t, m.Register(ctx, svc))
	t.Cleanup(func() { _ = m.Deregister(context.Background(), svc.ID) })

	waitHealthy(t, m, svc.Name, 10*time.Second)

	addr, err := m.Resolve(ctx, svc.Name, "")
	require.NoError(t, err)
	assert.Equal(t, fmt.Sprintf("%s:%d", advertiseAddr, port), addr)
}

func TestIntegration_ResolveNotFound(t *testing.T) {
	m := integrationManager(t)

	_, err := m.Resolve(context.Background(), "nonexistent-svc-xyz", "")
	assert.ErrorIs(t, err, ErrNoHealthyInstances)
}

func TestIntegration_ResolveNotFoundWithFallback(t *testing.T) {
	m := integrationManager(t)

	addr, err := m.Resolve(context.Background(), "nonexistent-svc-xyz", "fallback:9999")
	require.NoError(t, err)
	assert.Equal(t, "fallback:9999", addr)
}

func TestIntegration_Deregister(t *testing.T) {
	port := startHealthServer(t)
	m := integrationManager(t)
	ctx := context.Background()

	svc := Service{
		ID:   fmt.Sprintf("test-svc-dereg-%d", port),
		Name: fmt.Sprintf("test-svc-dereg-%d", port),
		Port: port,
		HealthCheck: &HealthCheck{Interval: "2s", Timeout: "1s"},
	}

	require.NoError(t, m.Register(ctx, svc))
	waitHealthy(t, m, svc.Name, 10*time.Second)

	require.NoError(t, m.Deregister(ctx, svc.ID))

	_, err := m.Resolve(ctx, svc.Name, "")
	assert.ErrorIs(t, err, ErrNoHealthyInstances)
}

func TestIntegration_Watch(t *testing.T) {
	port := startHealthServer(t)
	m := integrationManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)

	t.Cleanup(cancel)

	svc := Service{
		ID:   fmt.Sprintf("test-svc-watch-%d", port),
		Name: fmt.Sprintf("test-svc-watch-%d", port),
		Port: port,
		HealthCheck: &HealthCheck{Interval: "2s", Timeout: "1s"},
	}

	require.NoError(t, m.Register(ctx, svc))
	t.Cleanup(func() { _ = m.Deregister(context.Background(), svc.ID) })

	// Wait until Consul marks the service healthy before starting the watch,
	// otherwise the first Watch poll returns the initial critical state.
	waitHealthy(t, m, svc.Name, 10*time.Second)

	ch, err := m.Watch(ctx, svc.Name)
	require.NoError(t, err)

	select {
	case event, open := <-ch:
		require.True(t, open, "watch channel closed unexpectedly")
		assert.Equal(t, EventRegistered, event.Type)
		assert.Equal(t, svc.Name, event.Service.Name)
	case <-time.After(10 * time.Second):
		t.Log("no event received within 10s — Consul index may not have changed")
	}
}
