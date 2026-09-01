// Package obs defines the minimal observability contract that
// lib-service-discovery exposes on its public API.
//
// The interface here is deliberately expressed with stdlib types only.
// lib-service-discovery MUST NOT import
// github.com/LerianStudio/lib-observability from this package: an interface
// that names a type defined by a versioned module stays nominally bound to
// that module, which is exactly the coupling this package exists to remove.
// While libsd.Logger named lib-observability's log.Logger, every major of
// lib-observability crossed the boundary and forced a major here — and,
// transitively, blocked the fleet: midaz could not move to
// lib-observability v4 until this library compiled against it.
//
// Because this contract mentions nothing but stdlib types, a logger written
// against ANY major of lib-observability satisfies it, and so does a type
// declared in a package that has never imported lib-observability at all.
//
// Since lib-observability v4 no adapter is needed in either direction: its
// loggers satisfy Logger as they are, and every lib-observability entry point
// that takes a logger accepts a Logger declared here. See MIGRATION-v3.md.
package obs

import "context"

// Log severity levels.
//
// The numeric scale is identical to lib-observability's log.Level: lower
// values are MORE severe. A logger configured at LevelInfo (2) emits Error,
// Warn and Info and suppresses Debug. Note this is INVERTED from log/slog,
// where Debug is -4 and Error is 8.
const (
	// LevelError reports failures. Most severe.
	LevelError = 0
	// LevelWarn reports recoverable anomalies.
	LevelWarn = 1
	// LevelInfo reports normal operational events.
	LevelInfo = 2
	// LevelDebug reports diagnostic detail. Least severe.
	LevelDebug = 3
)

// Logger is the logging contract required by lib-service-discovery.
//
// Structured attributes are passed as alternating key/value pairs in fields,
// or as values carrying their own key (lib-observability's log.Field and
// []log.Field are recognized by its loggers on arrival). Implementations must
// be safe for concurrent use and must never panic on malformed fields.
//
// Note the absence of a self-returning method: a method such as
// With(...) Logger cannot be declared by a foreign package, because that
// package has no way to name the return type, which would make this contract
// unsatisfiable from outside and defeat its purpose.
type Logger interface {
	// Log emits msg at the given level with the fields attached.
	Log(ctx context.Context, level int, msg string, fields ...any)
	// Enabled reports whether events at level would be emitted.
	Enabled(level int) bool
	// Sync flushes any buffered log entries.
	Sync(ctx context.Context) error
}
