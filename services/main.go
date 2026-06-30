// Command services is the lib-service-discovery demo. One binary behaves as
// svc-a, svc-b or svc-c (selected by SERVICE_NAME). Each instance registers
// itself in Consul through the library and, when NEXT is set, resolves the next
// service by name and calls it. A single request to svc-a therefore walks the
// svc-a -> svc-b -> svc-c chain using discovery only, with no hardcoded addresses.
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
	listen := ":" + getenv("SD_ADVERTISE_PORT", "8080")

	logger := &log.GoLogger{Level: log.LevelDebug}
	ctx := context.Background()

	sd, err := libsd.New(libsd.ConfigFromEnv(), libsd.WithLogger(logger))
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

	mux := http.NewServeMux()

	mux.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		if next == "" {
			fmt.Fprint(w, name)

			return
		}

		addr, err := sd.Resolve(r.Context(), next, "")
		if err != nil {
			http.Error(w, fmt.Sprintf("resolve %s: %v", next, err), http.StatusBadGateway)

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
		fmt.Fprintf(w, "service=%s listen=%s next=%q\n", name, listen, next)
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

	_ = sd.Deregister(shutCtx, id)
	_ = srv.Shutdown(shutCtx)
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}

	return def
}
