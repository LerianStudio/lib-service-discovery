# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [v0.6.0] - 2026-06-30

### Added
- **`Manager.RegisterAsync`**: non-blocking registration that retries in the
  background with exponential backoff until it succeeds or the context is
  cancelled. Startup no longer depends on the discovery server being reachable.
- **Self-healing TTL heartbeat**: when a heartbeat finds its check unknown
  (HTTP 404 — the registration was lost after a Consul restart, or dropped by a
  server-only/agentless catalog), the registry re-registers the service to
  recreate the check and resumes, instead of failing every TTL/2 forever.
- **Unit coverage for the Consul backend**: the wire paths (register, resolve,
  watch, TTL heartbeat, deregister, and the self-heal path) are now exercised
  against an `httptest` fake of the Consul HTTP API, so they run in CI without a
  live Consul agent.

### Fixed
- Reconciled this changelog with the repository's real tags (`v0.1.0`–`v0.5.0`).
  Prior entries (`v1.x`/`v2.x`, dated 2025-07) were release-template residue and
  did not correspond to any tag.

## [v0.5.0] - 2026-06-24

### Changed
- Standardized environment variables under the backend-agnostic `SD_` prefix
  (`SD_ENABLED`, `SD_ADDRESS`, `SD_ADVERTISE_ADDRESS`, `SD_ADVERTISE_PORT`,
  `SD_WORKLOAD`). Legacy names (`SERVICE_DISCOVERY_ENABLED`, `CONSUL_ADDR`,
  `SERVICE_ADVERTISE_ADDR`/`_PORT`, `WORKLOAD_ID`) remain accepted as a fallback,
  with `SD_*` taking precedence.

### Added
- `SD_TLS`, `SD_TLS_SKIP_VERIFY` and `SD_TOKEN` for HTTPS and ACL-protected
  discovery servers.

## [v0.4.0] - 2026-06-24

### Added
- `Resolve`/`ResolveService` log their outcome at Info with an explicit `source`
  field (`consul` or `fallback`) for clearer operational signal.

## [v0.3.0] - 2026-05-19

### Added
- Scheme is parsed from `SERVICE_ADVERTISE_ADDR` (e.g. `https://host`) and
  propagated to Consul via service Meta, so resolvers recover the URL scheme.

## [v0.2.0] - 2026-05-19

### Added
- `Scheme` field on `Service` and the `ResolveService` method, exposing the full
  resolved service (scheme and metadata) beyond `host:port`.

## [v0.1.1] - 2026-05-19

### Added
- Successful Consul lookups log the resolved address.

## [v0.1.0] - 2026-05-19

### Added
- Initial public release: `Manager` over a Consul `Registry` with `Register`,
  `Resolve`, `Deregister` and `Watch`; three operating modes (disabled,
  enabled-with-fallback, enabled-strict); TTL heartbeat and HTTP health checks;
  workload tag isolation; configuration via environment variables.

[Unreleased]: https://github.com/LerianStudio/lib-service-discovery/compare/v0.6.0...HEAD
[v0.6.0]: https://github.com/LerianStudio/lib-service-discovery/compare/v0.5.0...v0.6.0
[v0.5.0]: https://github.com/LerianStudio/lib-service-discovery/compare/v0.4.0...v0.5.0
[v0.4.0]: https://github.com/LerianStudio/lib-service-discovery/compare/v0.3.0...v0.4.0
[v0.3.0]: https://github.com/LerianStudio/lib-service-discovery/compare/v0.2.0...v0.3.0
[v0.2.0]: https://github.com/LerianStudio/lib-service-discovery/compare/v0.1.1...v0.2.0
[v0.1.1]: https://github.com/LerianStudio/lib-service-discovery/compare/v0.1.0...v0.1.1
[v0.1.0]: https://github.com/LerianStudio/lib-service-discovery/releases/tag/v0.1.0
