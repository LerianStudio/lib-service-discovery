// Package libsd provides a service discovery abstraction backed by HashiCorp Consul.
//
// # Overview
//
// The Manager is the single entry point. It supports three operational modes:
//
//   - Disabled: all operations are no-ops; Resolve returns the fallback address directly.
//   - Enabled with fallback: queries Consul first; on failure falls back to a static address.
//   - Enabled without fallback: queries Consul; returns an error when no healthy instance is found.
//
// This design allows gradual migration from hardcoded addresses to full service discovery
// without requiring all services to be Consul-aware at once.
//
// # Usage
//
//	cfg := libsd.ConfigFromEnv()
//	sd, err := libsd.New(cfg, libsd.WithLogger(logger))
//	if err != nil {
//	    return err
//	}
//
//	// Register this service
//	if err := sd.Register(ctx, libsd.Service{
//	    ID:   "svc-a-1",
//	    Name: "svc-a",
//	    Port: 8081,
//	    Tags: []string{"v1"},
//	    HealthCheck: &libsd.HealthCheck{Interval: "10s", Timeout: "3s"},
//	}); err != nil {
//	    return err
//	}
//
//	// Resolve a downstream service (with optional static fallback for migration)
//	addr, err := sd.Resolve(ctx, "svc-b", "svc-b:8082")
//
//	// Deregister on shutdown
//	defer sd.Deregister(ctx, "svc-a-1")
//
// # Environment Variables
//
//   - SERVICE_DISCOVERY_ENABLED — "true" to enable Consul-backed discovery (default: false)
//   - CONSUL_ADDR               — Consul agent address (default: "localhost:8500")
//   - SERVICE_ADVERTISE_ADDR    — Address this service advertises to Consul (required when enabled)
//   - SERVICE_ADVERTISE_PORT    — Port to advertise; defaults to the port passed to Register
package libsd
