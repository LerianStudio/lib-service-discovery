# lib-service-discovery

Service discovery library (`lib-service-discovery`) backed by HashiCorp Consul, following Lerian's `lib-commons` conventions.

## Requirements

- Go `1.26` or newer
- HashiCorp Consul `1.19` or newer (only when `SERVICE_DISCOVERY_ENABLED=true`)

## Installation

```bash
go get github.com/LerianStudio/lib-service-discovery
```

## What is in this library

### `lib-service-discovery`

A service discovery abstraction with three operational modes:

| Mode | Behaviour |
|---|---|
| Disabled | All operations are no-ops. `Resolve` returns the fallback address directly. |
| Enabled with fallback | Queries Consul first; falls back to a static address on failure. |
| Enabled without fallback | Queries Consul; returns an error when no healthy instance is found. |

**Key types:**

- `Manager` — entry point; created with `New(cfg, opts...)`.
- `Registry` — interface for the Consul backend; can be replaced by an in-memory stub in tests.
- `Service` / `HealthCheck` / `Event` — domain types.

**Functional options:**

```go
libsd.WithLogger(logger)   // inject a lib-commons log.Logger
```

## Usage

```go
cfg := libsd.ConfigFromEnv()

sd, err := libsd.New(cfg, libsd.WithLogger(logger))
if err != nil {
    return err
}

// Register this service
if err := sd.Register(ctx, libsd.Service{
    ID:   "svc-a-1",
    Name: "svc-a",
    Port: 8081,
    Tags: []string{"v1"},
    HealthCheck: &libsd.HealthCheck{Interval: "10s", Timeout: "3s"},
}); err != nil {
    return err
}

// Resolve a downstream service (static fallback for gradual migration)
addr, err := sd.Resolve(ctx, "svc-b", "svc-b:8082")

// Deregister on shutdown
defer sd.Deregister(ctx, "svc-a-1")
```

## Environment variables

Canonical names use the backend-agnostic `SD_` prefix. Legacy names are still
accepted as a fallback (`SD_` takes precedence when both are set).

| Variable | Legacy (fallback) | Default | Description |
|---|---|---|---|
| `SD_ENABLED` | `SERVICE_DISCOVERY_ENABLED` | `false` | Set to `"true"` to enable discovery |
| `SD_ADDRESS` | `CONSUL_ADDR` | `localhost:8500` | Discovery server address (host:port) |
| `SD_ADVERTISE_ADDRESS` | `SERVICE_ADVERTISE_ADDR` | — | Address this service advertises (hostname or full URL); required when enabled |
| `SD_ADVERTISE_PORT` | `SERVICE_ADVERTISE_PORT` | `0` | Port override (defaults to the port passed to `Register`) |
| `SD_WORKLOAD` | `WORKLOAD_ID` | — | Workload scope for tag-based isolation |
| `SD_TLS` | — | `false` | `"true"` to use HTTPS to the server |
| `SD_TLS_SKIP_VERIFY` | — | `false` | `"true"` to skip server certificate verification |
| `SD_TOKEN` | — | — | ACL token sent to the server |

## Running the demo

```bash
make up    # starts consul + svc-a + svc-b + svc-c
make down  # stops and removes containers
```

Then:

```bash
curl http://localhost:8081/ping   # svc-a → svc-b → svc-c chain
curl http://localhost:8081/whoami # shows discovery config
```

## Development

```bash
make test-unit          # unit tests
make test-integration   # requires running Consul
make lint               # golangci-lint
make ci                 # full pipeline
```
