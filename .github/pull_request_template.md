<table border="0" cellspacing="0" cellpadding="0">
  <tr>
    <td><img src="https://github.com/LerianStudio.png" width="72" alt="Lerian" /></td>
    <td><h1>Lib Service Discovery</h1></td>
  </tr>
</table>

---

## Description

<!-- Summarize what this PR changes and why. Mention the area(s) affected
     (Manager, Registry/Consul backend, resolvers, config, demo services). -->

## Type of Change

- [ ] `feat`: New feature or capability
- [ ] `fix`: Bug fix
- [ ] `perf`: Performance improvement
- [ ] `refactor`: Internal restructuring with no behavior change
- [ ] `docs`: Documentation only (README, docs/, llms.txt, inline comments)
- [ ] `style`: Formatting, whitespace, naming (no logic change)
- [ ] `test`: Adding or updating tests
- [ ] `ci`: CI pipeline or workflow changes
- [ ] `build`: Build system, Dockerfile, Go module dependencies
- [ ] `chore`: Maintenance, config, tooling
- [ ] `revert`: Reverts a previous commit
- [ ] `BREAKING CHANGE`: Consumers must update their integration

## Breaking Changes

<!-- If applicable, describe exactly what breaks (env vars, route paths,
     persistence schema, public ports/adapters) and how downstream services
     should migrate. Remove this section if not applicable. -->

None.

## Testing

- [ ] `make test-unit` passes
- [ ] `make test-integration` passes if Consul paths are exercised (`docker compose up consul`)
- [ ] `make lint` and `make vet` pass
- [ ] Coverage threshold respected (`make coverage-unit`)

**Test evidence / Actions run:** <!-- Optional: link to a CI run or screenshot -->

## Architectural Checklist

- [ ] Every exported method is nil-receiver safe (returns `ErrNilManager` or equivalent)
- [ ] No `panic()` in library paths — invalid input returns a sentinel or wrapped error (`%w`)
- [ ] Blocking methods take `ctx context.Context` as the first parameter
- [ ] Logging goes through the internal `lib-observability` adapter with typed fields (no `fmt.Sprintf` in log calls)
- [ ] Goroutines start via `runtime.SafeGo` and stop on `Close`/context cancellation
- [ ] No `lib-commons` dependency added
- [ ] Public API additions documented (godoc + README if user-facing)

## Related Issues

Closes #
