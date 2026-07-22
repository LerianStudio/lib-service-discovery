package libsd

import (
	"context"
	"strings"

	"github.com/LerianStudio/lib-observability/log"
)

// Logger is the minimal, version-agnostic structured-logging contract that
// lib-service-discovery accepts from consumers. It uses only stdlib types and
// mirrors the context-aware methods of *slog.Logger, so any slog-compatible
// logger (including the stdlib *slog.Logger) satisfies it directly — with no
// dependency on lib-observability and no adapter wrapper on the caller side.
//
// The variadic args follow slog semantics: alternating key/value pairs
// (e.g. "consul", addr) or slog.Attr values.
type Logger interface {
	InfoContext(ctx context.Context, msg string, args ...any)
	WarnContext(ctx context.Context, msg string, args ...any)
	ErrorContext(ctx context.Context, msg string, args ...any)
	DebugContext(ctx context.Context, msg string, args ...any)
}

// obsLoggerAdapter bridges a consumer-supplied libsd.Logger to the
// lib-observability log.Logger contract used internally by the registry and
// resolvers, so no internal call site changes. lib-observability remains a
// private implementation detail and never appears in the public API.
type obsLoggerAdapter struct {
	l      Logger
	attrs  []any
	groups []string
}

// toObsLogger wraps a public libsd.Logger as an internal log.Logger. A nil
// logger yields the lib-observability no-op logger, preserving the prior
// "nil logger is silent" behaviour.
func toObsLogger(l Logger) log.Logger {
	if l == nil {
		return log.NewNop()
	}

	return &obsLoggerAdapter{l: l}
}

// fieldsToArgs converts lib-observability Fields into slog-style key/value
// args, qualifying keys with the active group prefix.
func fieldsToArgs(prefix string, fields []log.Field) []any {
	out := make([]any, 0, len(fields)*2)

	for _, f := range fields {
		key := f.Key
		if prefix != "" {
			key = prefix + "." + f.Key
		}

		out = append(out, key, f.Value)
	}

	return out
}

func (a *obsLoggerAdapter) Log(ctx context.Context, level log.Level, msg string, fields ...log.Field) {
	prefix := strings.Join(a.groups, ".")
	args := append(append([]any{}, a.attrs...), fieldsToArgs(prefix, fields)...)

	switch level {
	case log.LevelError:
		a.l.ErrorContext(ctx, msg, args...)
	case log.LevelWarn:
		a.l.WarnContext(ctx, msg, args...)
	case log.LevelDebug:
		a.l.DebugContext(ctx, msg, args...)
	default: // LevelInfo and any unknown level default to info.
		a.l.InfoContext(ctx, msg, args...)
	}
}

func (a *obsLoggerAdapter) With(fields ...log.Field) log.Logger {
	prefix := strings.Join(a.groups, ".")

	return &obsLoggerAdapter{
		l:      a.l,
		attrs:  append(append([]any{}, a.attrs...), fieldsToArgs(prefix, fields)...),
		groups: a.groups,
	}
}

func (a *obsLoggerAdapter) WithGroup(name string) log.Logger {
	if name == "" {
		return a
	}

	return &obsLoggerAdapter{
		l:      a.l,
		attrs:  append([]any{}, a.attrs...),
		groups: append(append([]string{}, a.groups...), name),
	}
}

// Enabled always reports true: level filtering is delegated to the underlying
// slog-compatible logger supplied by the consumer.
func (a *obsLoggerAdapter) Enabled(_ log.Level) bool { return true }

// Sync is a no-op: the underlying slog-compatible logger manages its own
// flushing, and the libsd.Logger contract does not expose a sync hook.
func (a *obsLoggerAdapter) Sync(_ context.Context) error { return nil }
