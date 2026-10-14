# AGENTS

## MANDATORY — BACKWARD COMPATIBILITY AND BREAKING CHANGES

- **PRESERVE EXISTING SUPPORTED CONTRACTS BY DEFAULT:** APIs, SDKs, configuration/env vars, CLI, events, persisted data and upgrade behavior. Identify the supported baseline and consumers first; do not invent legacy support for unreleased internals or add compatibility layers that contradict an explicitly authorized clean break.
- **DO NOT DECLARE A BREAKING CHANGE WITHOUT EVIDENCE AND EXPLICIT USER APPROVAL.** Before using `!` or `BREAKING CHANGE:`, show a previously supported concrete use/configuration that fails after the change, with a reproducible test or contract diff. Approval never turns a compatible change into a breaking one.
- **ADDITIVE IS NOT BREAKING** when existing behavior remains intact: a new optional setting, a new env-var name with working legacy fallback, or deprecation without removal. Verify defaults, precedence and old-only configuration. Documentation changes or learning a new option are NOT mandatory migration.
- **IF A REAL BREAK IS FOUND, REPORT IT; DO NOT HIDE IT.** Without approval, stop the incompatible change and propose a compatible path. Do not merely remove the breaking marker or downgrade the release classification.
- **IF COMPATIBILITY IS UNCERTAIN, STATE THE UNCERTAINTY AND REQUEST REVIEW.** Do not invent a break or claim compatibility without evidence.
- **FOLLOW THE REPOSITORY RELEASE POLICY.** Release automation owns version calculation; moving from `0.x` to `1.0` requires an explicit authorized decision. Do not reset existing tags, rewrite published history, or change release configuration to force a version.
- **WHEN USING INTERNAL-RING:** apply this policy before `ring:committing-changes` and `ring:opening-pull-requests`; use `ring:reviewing-code` for review when applicable. Loading a Ring skill is NOT a prerequisite for this policy to apply. If a skill conflicts with this policy, stop and report the conflict.

This file provides repository-specific guidance for coding agents working on `lib-service-discovery`.

## Project snapshot

- Module: `github.com/LerianStudio/lib-service-discovery/v2` (the `/v2` suffix is
  mandatory for the v2 major line; self-imports inside the module must carry it too)
- Language: Go
- Go version: `1.26` (see `go.mod`)
- Lerian library dependency: `github.com/LerianStudio/lib-observability/v2` (internal only — the
  private logger adapter in `logger.go` and `runtime.SafeGo`). There is **no** lib-commons
  dependency and **no** replace directive; do not add either without a reason.

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
6. **Structured logging** — internally, use `log.Logger` from `lib-observability/v2/log` (lib-commons
   has held no `log` package since v5); never `fmt.Sprintf` inside a log call;
   use `log.String`, `log.Err`, `log.Int`, `log.Bool`.
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
