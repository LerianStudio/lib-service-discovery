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
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/LerianStudio/lib-observability/log"
	libsd "github.com/LerianStudio/lib-service-discovery"
)

func main() {
	name := getenv("SERVICE_NAME", "svc-a")
	next := os.Getenv("NEXT") // downstream service name; empty for the leaf
	// Derive the listen port from the advertised port. An internal-only service
	// sets only SD_INTERNAL_PORT (no SD_ADVERTISE_PORT/SD_EXTERNAL_PORT), so fall
	// back to it; otherwise the internal-only instance would bind the wrong port
	// and callers resolving its internal endpoint could not reach it.
	listen := ":" + getenv("SD_ADVERTISE_PORT", getenv("SD_INTERNAL_PORT", "8080"))

	logger := &log.GoLogger{Level: log.LevelDebug}
	ctx := context.Background()

	cfg := libsd.ConfigFromEnv()

	sd, err := libsd.New(cfg, libsd.WithLogger(logger))
	if err != nil {
		logger.Log(ctx, log.LevelError, "init service discovery", log.Err(err))
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
			logger.Log(ctx, log.LevelError, "start dynamic resolver",
				log.String("next", next), log.Err(err))
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
			logger.Log(ctx, log.LevelError, "http server", log.Err(err))
		}
	}()

	logger.Log(ctx, log.LevelInfo, "demo service up",
		log.String("name", name),
		log.String("listen", listen),
		log.String("next", next))

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
