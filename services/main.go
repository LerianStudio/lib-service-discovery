// Command services is the lib-service-discovery demo. One binary behaves as
// svc-a, svc-b or svc-c (selected by SERVICE_NAME). Each instance registers
// itself in Consul through the library and, when NEXT is set, resolves the next
// service by name and calls it. A single request to svc-a therefore walks the
// svc-a -> svc-b -> svc-c chain using discovery only, with no hardcoded addresses.
//
// Watch-and-cache: instead of querying Consul on every request, each instance
// with a downstream (NEXT set) builds ONE DynamicResolver at startup. A
// background goroutine watches Consul and keeps the cached address fresh; the
// request path only reads that in-memory address. So the hot path never touches
// Consul, and if Consul goes down the last known-good address keeps being served.
//
//	make up                          # consul + svc-a + svc-b + svc-c
//	curl http://localhost:8081/ping  # svc-a -> svc-b -> svc-c
//	curl http://localhost:8081/whoami
package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"reflect"
	"syscall"
	"time"

	libsd "github.com/LerianStudio/lib-service-discovery/v2"
)

func main() {
	name := getenv("SERVICE_NAME", "svc-a")
	next := os.Getenv("NEXT") // downstream service name; empty for the leaf
	// Derive the listen port from the advertised port. An internal-only service
	// sets only SD_INTERNAL_PORT (no SD_ADVERTISE_PORT/SD_EXTERNAL_PORT), so fall
	// back to it; otherwise the internal-only instance would bind the wrong port
	// and callers resolving its internal endpoint could not reach it.
	listen := ":" + getenv("SD_ADVERTISE_PORT", getenv("SD_INTERNAL_PORT", "8080"))

	// libsd.Logger is built from stdlib types only, so a consumer can satisfy it
	// with a type it declares itself — slogLogger below imports nothing from any
	// observability library. A lib-observability v4 logger would be accepted just
	// as directly, with no adapter.
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	sdLogger := slogLogger{l: logger}
	ctx := context.Background()

	cfg := libsd.ConfigFromEnv()

	sd, err := libsd.New(cfg, libsd.WithLogger(sdLogger))
	if err != nil {
		logger.ErrorContext(ctx, "init service discovery", "error", err)
		os.Exit(1)
	}

	id := name + "-1"

	// RegisterAsync so the service still starts if Consul is briefly unavailable.
	sd.RegisterAsync(ctx, libsd.Service{
		ID:          id,
		Name:        name,
		HealthCheck: &libsd.HealthCheck{TTL: "15s"},
	})

	// Build ONE DynamicResolver per downstream. The leaf (svc-c, no NEXT) resolves
	// nobody, so it never creates one — resolver stays nil there.
	//
	// No WithView here on purpose: WatchResolve inherits the Manager's configured
	// SD_PREFER_VIEW (internal for this demo), matching the old one-shot behaviour.
	// Empty fallback keeps strict mode: with no Consul instance the cached address
	// stays empty and the handler fails open rather than dialling a bogus addr.
	//
	// In enabled mode WatchResolve is fail-open (a bad seed is logged, not fatal),
	// so a non-nil err here is a programmer error only — log it and carry on with a
	// nil resolver (the /ping handler treats an empty address as "not resolved yet").
	var resolver *libsd.DynamicResolver

	if next != "" {
		resolver, err = sd.WatchResolve(ctx, next, "" /* fallback */)
		if err != nil {
			logger.ErrorContext(ctx, "start dynamic resolver",
				"next", next, "error", err)
		}
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		if next == "" {
			fmt.Fprint(w, name)

			return
		}

		// Hot path never touches Consul: resolver.Address() reads the in-memory
		// cache kept fresh by the background watch goroutine. Consul is queried only
		// by that watcher, never here — so if Consul is down the last known-good
		// address keeps being served. resolver.Address() is nil-safe (returns "").
		addr := resolver.Address()
		if addr == "" {
			// Degraded: seed hasn't populated (Consul unreachable at boot with no
			// fallback) or the resolver failed to start. Fail open with 503 instead
			// of dialling a bogus address, so the degraded state is observable.
			http.Error(w, fmt.Sprintf("downstream %s not resolved yet", next), http.StatusServiceUnavailable)

			return
		}

		req, _ := http.NewRequestWithContext(r.Context(), http.MethodGet, "http://"+addr+"/ping", nil)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			http.Error(w, fmt.Sprintf("call %s: %v", next, err), http.StatusBadGateway)

			return
		}

		defer func() { _ = resp.Body.Close() }()

		body, _ := io.ReadAll(resp.Body)
		fmt.Fprintf(w, "%s -> %s", name, body)
	})

	mux.HandleFunc("/whoami", func(w http.ResponseWriter, _ *http.Request) {
		// resolver.Address() is nil-safe, so the leaf (nil resolver) prints "".
		// Exposing the cached address makes it observable that the hot path serves
		// the address from the DynamicResolver cache, not a per-request Consul query.
		fmt.Fprintf(w, "service=%s listen=%s next=%q prefer_view=%s next_addr=%q\n",
			name, listen, next, cfg.PreferView, resolver.Address())
	})

	srv := &http.Server{Addr: listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.ErrorContext(ctx, "http server", "error", err)
		}
	}()

	logger.InfoContext(ctx, "demo service up",
		"name", name,
		"listen", listen,
		"next", next)

	// Graceful shutdown: deregister so the instance leaves the catalog cleanly.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Stop the background watch goroutine so it does not leak (nil-safe on the leaf).
	resolver.Stop()

	_ = sd.Deregister(shutCtx, id)
	// Close stops the Manager's own background goroutines (TTL heartbeats).
	_ = sd.Close()
	_ = srv.Shutdown(shutCtx)
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}

	return def
}

// slogLogger adapts a *slog.Logger to libsd.Logger.
//
// It is declared here, in the consumer, and imports nothing from
// lib-observability. That is the whole point of the libsd.Logger contract: it
// names only stdlib types, so any package can satisfy it without inheriting a
// versioned dependency — and without waiting on a major bump anywhere.
type slogLogger struct{ l *slog.Logger }

func (s slogLogger) Log(ctx context.Context, level int, msg string, fields ...any) {
	s.l.Log(ctx, slogLevel(level), msg, slogArgs(fields)...)
}

func (s slogLogger) Enabled(level int) bool {
	return s.l.Enabled(context.Background(), slogLevel(level))
}

// Sync is a no-op: slog handlers flush on write.
func (s slogLogger) Sync(context.Context) error { return nil }

// slogLevel maps the libsd severity scale (Error=0 .. Debug=3, lower is more
// severe) onto slog's, which runs the other way (Debug=-4 .. Error=8).
func slogLevel(level int) slog.Level {
	switch level {
	case libsd.LevelError:
		return slog.LevelError
	case libsd.LevelWarn:
		return slog.LevelWarn
	case libsd.LevelDebug:
		return slog.LevelDebug
	case libsd.LevelInfo:
		return slog.LevelInfo
	default:
		return slog.LevelInfo
	}
}

// slogArgs flattens the fields variadic into slog key/value pairs.
//
// The library emits lib-observability log.Field values (a struct with exported
// Key and Value). This consumer will not import that module just to name the
// type, so it recognizes the SHAPE by reflection instead — which also accepts
// the equivalent field type of any other logging library. Anything else is
// passed through untouched, so plain alternating key/value pairs work too.
func slogArgs(fields []any) []any {
	out := make([]any, 0, len(fields)*2)

	for _, f := range fields {
		out = appendField(out, reflect.ValueOf(f))
	}

	return out
}

func appendField(out []any, v reflect.Value) []any {
	for v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface {
		if v.IsNil() {
			return append(out, nil)
		}

		v = v.Elem()
	}

	// A []Field arrives as ONE variadic element; flatten it in place.
	if v.Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Struct {
		for i := range v.Len() {
			out = appendField(out, v.Index(i))
		}

		return out
	}

	if v.Kind() == reflect.Struct {
		key := v.FieldByName("Key")
		val := v.FieldByName("Value")

		if key.Kind() == reflect.String && val.IsValid() && val.CanInterface() {
			return append(out, key.String(), val.Interface())
		}
	}

	if !v.IsValid() {
		return append(out, nil)
	}

	return append(out, v.Interface())
}
