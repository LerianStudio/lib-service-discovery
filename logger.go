package libsd

import (
	"context"

	"github.com/LerianStudio/lib-observability/log"
)

// Logger is the minimal structured-logging interface required by
// lib-service-discovery.
//
// It intentionally mirrors the context-aware methods of the standard library's
// *slog.Logger, so a consumer can supply any structured logger — including
// *slog.Logger — without being forced to import and version-match a specific
// logging library. This keeps the lib-observability logging types out of the
// public API and avoids the type incompatibilities that arise when a consumer
// pins a different lib-observability version than this library does.
//
// Any value implementing these four methods satisfies the interface via Go's
// structural typing, so no concrete adapter type is required on the caller side.
type Logger interface {
	InfoContext(ctx context.Context, msg string, args ...any)
	WarnContext(ctx context.Context, msg string, args ...any)
	ErrorContext(ctx context.Context, msg string, args ...any)
	DebugContext(ctx context.Context, msg string, args ...any)
}

// nopLogger is a Logger that discards everything.
type nopLogger struct{}

func (nopLogger) InfoContext(context.Context, string, ...any)  {}
func (nopLogger) WarnContext(context.Context, string, ...any)  {}
func (nopLogger) ErrorContext(context.Context, string, ...any) {}
func (nopLogger) DebugContext(context.Context, string, ...any) {}

// NewNopLogger returns a Logger that discards all output. It is the default used
// when no logger is supplied via Config.Logger or WithLogger.
func NewNopLogger() Logger { return nopLogger{} }

// observabilityAdapter adapts a public Logger to the internal lib-observability
// log.Logger consumed by the registry and resolvers. It lets the internal
// logging call sites keep using the log.Log(ctx, level, msg, fields...) style
// while the public API only ever exposes the version-agnostic Logger interface.
type observabilityAdapter struct {
	inner  Logger
	fields []log.Field
}

// toInternalLogger wraps a public Logger in the internal log.Logger contract.
// A nil Logger yields a lib-observability no-op logger.
func toInternalLogger(l Logger) log.Logger {
	if l == nil {
		return log.NewNop()
	}

	return &observabilityAdapter{inner: l}
}

// Log routes an internal log event to the appropriate context-aware method of
// the wrapped Logger, flattening the typed fields into slog-style key/value
// arguments.
func (a *observabilityAdapter) Log(ctx context.Context, level log.Level, msg string, fields ...log.Field) {
	all := fields
	if len(a.fields) > 0 {
		all = make([]log.Field, 0, len(a.fields)+len(fields))
		all = append(all, a.fields...)
		all = append(all, fields...)
	}

	args := make([]any, 0, len(all)*2)
	for _, f := range all {
		args = append(args, f.Key, f.Value)
	}

	switch level {
	case log.LevelError:
		a.inner.ErrorContext(ctx, msg, args...)
	case log.LevelWarn:
		a.inner.WarnContext(ctx, msg, args...)
	case log.LevelDebug:
		a.inner.DebugContext(ctx, msg, args...)
	default: // LevelInfo and any unknown level map to Info.
		a.inner.InfoContext(ctx, msg, args...)
	}
}

// With returns a child adapter carrying additional persistent fields.
//
//nolint:ireturn // must satisfy the lib-observability log.Logger contract.
func (a *observabilityAdapter) With(fields ...log.Field) log.Logger {
	merged := make([]log.Field, 0, len(a.fields)+len(fields))
	merged = append(merged, a.fields...)
	merged = append(merged, fields...)

	return &observabilityAdapter{inner: a.inner, fields: merged}
}

// WithGroup is a no-op: the public Logger interface has no grouping concept.
//
//nolint:ireturn // must satisfy the lib-observability log.Logger contract.
func (a *observabilityAdapter) WithGroup(_ string) log.Logger { return a }

// Enabled always reports true and defers level filtering to the wrapped Logger.
func (a *observabilityAdapter) Enabled(_ log.Level) bool { return true }

// Sync is a no-op: flushing is the responsibility of the wrapped Logger.
func (a *observabilityAdapter) Sync(_ context.Context) error { return nil }
