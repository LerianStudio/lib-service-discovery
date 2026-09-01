# AGENTS

This file provides repository-specific guidance for coding agents working on `lib-service-discovery`.

## Project snapshot

- Module: `github.com/LerianStudio/lib-service-discovery/v2` (the `/v2` suffix is
  mandatory for the v2 major line; self-imports inside the module must carry it too)
- Language: Go
- Go version: `1.26` (see `go.mod`)
- Lerian library dependency: `github.com/LerianStudio/lib-observability/v4` (internal only — the
  `log.Field` constructors at call sites and `runtime.SafeGo`). It must never appear on the
  public API: the logger contract is `obs.Logger`, declared in `obs/` over stdlib types only,
  and `obs/boundary` fails the build if a lib-observability type reaches an exported field,
  parameter, return or alias. There is **no** lib-commons dependency. There is currently ONE
  replace directive, for the unpublished lib-observability v4 (PR #61); it must be removed
  before tagging. Do not add another without a reason.

## Primary objective for changes

- Follow the same patterns as `lib-commons/v6` (the reference implementation for all Lerian Go libraries).
- Keep the public API nil-safe and concurrency-safe by default.
- Prefer explicit error returns over panics.

## Repository shape

```
lib-service-discovery/
├── lib-service-discovery/          # Service discovery library (main deliverable)
│   ├── doc.go       # Package-level godoc
│   ├── types.go     # Errors + Registry interface + domain types (Service, HealthCheck, Event)
│   ├── config.go    # Config struct, ConfigFromEnv(), Validate(), withDefaults()
│   ├── manager.go   # Manager struct, New(), Option, public methods
│   └── consul.go    # consulRegistry — Consul API implementation of Registry
├── services/
│   └── main.go      # Demo services (svc-a, svc-b, svc-c) sharing one binary
└── docker-compose.yml  # consul:1.19 + svc-a/b/c
```

## Coding standards

All standards mirror `lib-commons/v6`:

1. **`types.go`** holds all exported types: errors (`var Err* = errors.New(...)`), the `Registry`
   interface, and domain models (`Service`, `HealthCheck`, `Event`, `EventType`).
2. **`config.go`** holds `Config` with an exported `Validate() error` and a private `withDefaults() Config`.
3. **`manager.go`** declares `Manager`, `Option`, `New()`, and all exported methods.
4. **Nil-receiver guards** — every exported method checks `if m == nil { return ErrNilManager }`.
5. **Functional options** — `type Option func(*Manager)`; guard nil receiver and nil value inside each option.
6. **Structured logging** — internal fields are typed `libsd.Logger` (alias of `obs.Logger`,
   stdlib types only). Field constructors come from `lib-observability/v4/log`, which is an
   INTERNAL dependency: never name a `lib-observability` type on an exported field, parameter,
   return or alias — `obs/boundary` fails the build if you do. Never `fmt.Sprintf` inside a log
   call; use `log.String`, `log.Err`, `log.Int`, `log.Bool`.
7. **Sentinel errors** — defined in `types.go`; callers use `errors.Is()`.
8. **Context first** — all blocking methods take `ctx context.Context` as first parameter.

## Testing standards

- Build tag `//go:build unit` on all unit test files.
- Build tag `//go:build integration` on integration tests (require a live Consul agent).
- `t.Parallel()` in every test and subtest.
- Use stub types in `_test.go` files; no mocking frameworks for unit tests.
- Integration tests use `docker-compose.yml` or `testcontainers-go`.

## Running tests

```bash
make test-unit          # unit tests (no external deps)
make test-integration   # requires running Consul (docker compose up consul)
make lint               # golangci-lint
make ci                 # tidy + vet + lint + test-unit
```
