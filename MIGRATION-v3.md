# Migrating to lib-service-discovery v3

v3 has one job: **stop lib-observability's major version from propagating
through this library.**

Nothing was added to what the library discovers, registers or watches. What
changed is the shape of its public boundary.

---

## Why v3 exists

Go types have **nominal** identity. `lib-observability/v2/log.Logger` and
`lib-observability/v4/log.Logger` are different types even when the source is
byte-for-byte identical. So while this library named a `lib-observability` type
on its public API, every major over there crossed the boundary and forced a
major here.

That is not a theoretical cost. It stalled the fleet: midaz could not adopt
lib-observability v4 because lib-service-discovery, lib-auth and lib-streaming
did not compile against it.

Worse, the coupling is silent in one place. `lib-observability` v1.1.0 declares
its own `var ContextKey = contextKey("custom_context")`. A logger stored in a
context by one major is invisible to another, which falls through to
`NopLogger` — no error, no panic, just missing logs. midaz pins
`lib-service-discovery v1.1.0`, which drags exactly that v1.1.0 in.

v3 removes the mechanism. The contract now lives in the `obs` subpackage, which
imports **nothing but the stdlib**:

```go
package obs

type Logger interface {
	Log(ctx context.Context, level int, msg string, fields ...any)
	Enabled(level int) bool
	Sync(ctx context.Context) error
}

const (
	LevelError = 0
	LevelWarn  = 1
	LevelInfo  = 2
	LevelDebug = 3
)
```

This is the same contract lib-commons v7 declares in `commons/obs`, and the
same shape `lib-observability` v4 accepts in every logger parameter position.

Note what is absent: a self-returning method. `With(...) Logger` is
unsatisfiable from outside the declaring package by construction — a foreign
package has no way to name the return type — which is precisely what made the
old contract impossible to reimplement locally.

A regression test, `obs/boundary`, walks every non-test file in the module with
`go/ast` and fails if a `lib-observability` type appears in an exported field,
parameter, return or type alias. The defect class cannot come back silently.

---

## From / to

| v2 | v3 | breaking? |
|---|---|---|
| `libsd.Logger` = `InfoContext`/`WarnContext`/`ErrorContext`/`DebugContext` | `libsd.Logger` = `obs.Logger` (`Log`/`Enabled`/`Sync`) | **yes — implementers** |
| `Config.Logger Logger` | unchanged spelling, new contract | **yes — implementers** |
| `WithLogger(l Logger) Option` | unchanged spelling, new contract | **yes — implementers** |
| — | `libsd.LevelError/Warn/Info/Debug` | new |
| — | package `.../v2/obs` | new |
| `lib-observability/v2` (internal) | `lib-observability/v4` (internal) | no — not on the API |
| module `…/v2` | module `…/v3` | **yes — import path** |

Everything that is not a logger is untouched: `Manager`, `Config`, `Registry`,
`Service`, `HealthCheck`, `Event`, `DynamicResolver` and every method on them
keep their signatures.

---

## What consumers do

### You pass a lib-observability v4 logger

Nothing to do beyond the import path. v4's `log.Logger` satisfies
`libsd.Logger` as it is — no adapter, in either direction.

```go
sd, err := libsd.New(cfg, libsd.WithLogger(obsLogger))
```

### You pass a lib-commons `obs.Logger`

Also nothing to do. `commons/obs.Logger` and `libsd/obs.Logger` declare the
same three methods over stdlib types, so a value of one is assignable to the
other.

### You pass a `*slog.Logger`

This is the one real break. `*slog.Logger` satisfied the old contract directly;
it does not satisfy the new one, because the level scales are inverted
(`libsd` Error=0 … Debug=3; `slog` Debug=-4 … Error=8) and slog has no `Sync`.
Write the shim once, in your own package — it imports nothing from any
observability library:

```go
type slogLogger struct{ l *slog.Logger }

func (s slogLogger) Log(ctx context.Context, level int, msg string, fields ...any) {
	s.l.Log(ctx, slogLevel(level), msg, fields...)
}

func (s slogLogger) Enabled(level int) bool {
	return s.l.Enabled(context.Background(), slogLevel(level))
}

func (s slogLogger) Sync(context.Context) error { return nil }

func slogLevel(level int) slog.Level {
	switch level {
	case libsd.LevelError:
		return slog.LevelError
	case libsd.LevelWarn:
		return slog.LevelWarn
	case libsd.LevelDebug:
		return slog.LevelDebug
	default:
		return slog.LevelInfo
	}
}
```

`services/main.go` carries a complete version of this, including the field
flattening that renders `lib-observability` `log.Field` values as slog
key/value pairs without importing `lib-observability` to name the type.

### You are on `lib-service-discovery v1.1.0`

Bump to v3 directly. v1.1.0 drags `lib-observability v1.1.0` and its
independent `ContextKey`, which is the silent-logger-loss hazard described
above. There is no reason to stop at v2.

---

## Cutting the module to /v3

The module path is still `github.com/LerianStudio/lib-service-discovery/v2` on
this branch, deliberately: the rename is a separate, purely mechanical commit,
so the API change can be reviewed on its own. To cut it, from the repository
root:

```sh
sed -i 's#github.com/LerianStudio/lib-service-discovery/v2#github.com/LerianStudio/lib-service-discovery/v3#g' \
  $(git ls-files '*.go' '*.md' 'go.mod')
go mod tidy
go build ./... && go test -tags unit -count=1 ./...
```

Then tag `v3.0.0`.

---

## Releasing

`lib-observability v4` is not published yet (PR #61), so `go.mod` reaches it
through a local `replace`:

```
replace github.com/LerianStudio/lib-observability/v4 => /home/rodrigodh/Development/lo-v4
```

**That directive must be deleted, and the requirement pinned to the published
`v4.0.0`, before this is tagged.** It is the one blocker on release.
