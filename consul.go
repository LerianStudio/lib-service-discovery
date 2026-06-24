package libsd

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LerianStudio/lib-observability/log"
	"github.com/hashicorp/consul/api"
)

const (
	// ttlHeartbeatFloor is the minimum interval between TTL heartbeats, so a very
	// small TTL never busy-loops the heartbeat goroutine.
	ttlHeartbeatFloor = time.Second

	// watchWaitTime caps how long a single blocking catalog query parks before
	// returning. Cancellation is driven by ctx (WithContext), so this only bounds
	// the long-poll itself.
	watchWaitTime = 5 * time.Minute

	// watchBackoffBase/Max bound the exponential backoff applied between failed
	// catalog polls, so a Consul outage never busy-loops the watch goroutine.
	watchBackoffBase = 100 * time.Millisecond
	watchBackoffMax  = 30 * time.Second
)

type consulRegistry struct {
	client *api.Client
	logger log.Logger

	// rr is a round-robin cursor used by Resolve to spread load across healthy
	// instances instead of always returning the first one.
	rr atomic.Uint64

	// mu guards heartbeats. Each registered TTL service has a cancel func that
	// stops its heartbeat goroutine; Deregister calls it.
	mu         sync.Mutex
	heartbeats map[string]context.CancelFunc
}

func newConsulRegistry(c Config, logger log.Logger) (Registry, error) {
	if logger == nil {
		logger = log.NewNop()
	}

	// DefaultConfig still honors the SDK's own CONSUL_HTTP_* env vars as a
	// fallback; explicit SD_* values (TLS/Token) take precedence below.
	cfg := api.DefaultConfig()
	cfg.Address = c.ConsulAddr

	if c.TLS {
		cfg.Scheme = "https"
	}

	if c.TLSSkipVerify {
		cfg.TLSConfig.InsecureSkipVerify = true
	}

	if c.Token != "" {
		cfg.Token = c.Token
	}

	client, err := api.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("consul: create client: %w", err)
	}

	return &consulRegistry{
		client:     client,
		logger:     logger,
		heartbeats: make(map[string]context.CancelFunc),
	}, nil
}

func (r *consulRegistry) Register(ctx context.Context, svc Service) error {
	if r == nil {
		return ErrNilRegistry
	}

	reg := &api.AgentServiceRegistration{
		ID:      svc.ID,
		Name:    svc.Name,
		Address: svc.Address,
		Port:    svc.Port,
		Tags:    svc.Tags,
		Meta:    metaWithScheme(svc),
	}

	ttl := ""

	if svc.HealthCheck != nil {
		if svc.HealthCheck.TTL != "" {
			// TTL check: the process pushes heartbeats; Consul never reaches the
			// service. Works for agentless/remote workloads behind NAT.
			ttl = svc.HealthCheck.TTL
			reg.Check = &api.AgentServiceCheck{
				CheckID:                        ttlCheckID(svc.ID),
				TTL:                            ttl,
				DeregisterCriticalServiceAfter: "1m",
			}
		} else {
			reg.Check = &api.AgentServiceCheck{
				HTTP:                           svc.HealthCheck.HTTP,
				Interval:                       svc.HealthCheck.Interval,
				Timeout:                        svc.HealthCheck.Timeout,
				DeregisterCriticalServiceAfter: "30s",
			}
		}
	}

	if err := r.client.Agent().ServiceRegisterOpts(reg, (api.ServiceRegisterOpts{}).WithContext(ctx)); err != nil {
		return fmt.Errorf("consul: register %s: %w", svc.Name, err)
	}

	r.logger.Log(ctx, log.LevelDebug, "service registered",
		log.String("id", svc.ID),
		log.String("name", svc.Name),
		log.String("addr", svc.Addr()))

	if ttl != "" {
		r.startHeartbeat(svc.ID, ttl)
	}

	return nil
}

// ttlCheckID returns the Consul check ID for a service's TTL check.
func ttlCheckID(serviceID string) string {
	return "service:" + serviceID
}

// startHeartbeat marks the TTL check passing immediately and then re-passes it
// every TTL/2 from a background goroutine until Deregister cancels it. The
// heartbeat is an OUTBOUND call to Consul, so it works from any network.
func (r *consulRegistry) startHeartbeat(serviceID, ttl string) {
	checkID := ttlCheckID(serviceID)

	interval := ttlHeartbeatFloor

	if d, err := time.ParseDuration(ttl); err == nil && d/2 > ttlHeartbeatFloor {
		interval = d / 2
	}

	ctx, cancel := context.WithCancel(context.Background())

	pass := func() {
		opts := (&api.QueryOptions{}).WithContext(ctx)
		if err := r.client.Agent().UpdateTTLOpts(checkID, "lib-service-discovery heartbeat", api.HealthPassing, opts); err != nil {
			r.logger.Log(ctx, log.LevelWarn, "ttl heartbeat failed",
				log.String("check", checkID),
				log.Err(err))
		}
	}

	pass() // first pass moves the check from critical to passing right away

	r.mu.Lock()
	// Replace any pre-existing heartbeat for the same service.
	if prev, ok := r.heartbeats[serviceID]; ok {
		prev()
	}

	r.heartbeats[serviceID] = cancel
	r.mu.Unlock()

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				pass()
			}
		}
	}()
}

func (r *consulRegistry) Deregister(ctx context.Context, serviceID string) error {
	if r == nil {
		return ErrNilRegistry
	}

	// Stop the TTL heartbeat goroutine (if any) before deregistering.
	r.mu.Lock()
	if cancel, ok := r.heartbeats[serviceID]; ok {
		cancel()
		delete(r.heartbeats, serviceID)
	}
	r.mu.Unlock()

	if err := r.client.Agent().ServiceDeregisterOpts(serviceID, (&api.QueryOptions{}).WithContext(ctx)); err != nil {
		return fmt.Errorf("consul: deregister %s: %w", serviceID, err)
	}

	r.logger.Log(ctx, log.LevelDebug, "service deregistered", log.String("id", serviceID))

	return nil
}

func (r *consulRegistry) Resolve(ctx context.Context, name, tag string) (Service, error) {
	if r == nil {
		return Service{}, ErrNilRegistry
	}

	entries, _, err := r.client.Health().Service(name, tag, true, (&api.QueryOptions{}).WithContext(ctx))
	if err != nil {
		return Service{}, fmt.Errorf("consul: resolve %s: %w", name, err)
	}

	if len(entries) == 0 {
		return Service{}, fmt.Errorf("%w: %s", ErrNoHealthyInstances, name)
	}

	// Spread load across healthy instances instead of always returning the first.
	return serviceFromEntry(entries[r.nextIndex(len(entries))]), nil
}

func (r *consulRegistry) Watch(ctx context.Context, name string) (<-chan Event, error) {
	if r == nil {
		ch := make(chan Event)
		close(ch)

		return ch, ErrNilRegistry
	}

	ch := make(chan Event, 16)

	go func() {
		defer close(ch)

		var (
			lastIndex uint64
			attempt   int
		)

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			entries, meta, err := r.client.Health().Service(name, "", false, (&api.QueryOptions{
				WaitIndex: lastIndex,
				WaitTime:  watchWaitTime,
			}).WithContext(ctx))
			if err != nil {
				// ctx cancelled mid-poll: exit quietly, no backoff/log needed.
				if ctx.Err() != nil {
					return
				}

				r.logger.Log(ctx, log.LevelWarn, "consul watch poll failed",
					log.String("service", name),
					log.Int("attempt", attempt),
					log.Err(err))

				// Backoff so a Consul outage doesn't busy-loop the goroutine.
				if !sleepCtx(ctx, backoffDuration(attempt)) {
					return
				}

				attempt++

				continue
			}

			attempt = 0

			// Consul restarted and the index rewound; re-baseline from scratch.
			if meta.LastIndex < lastIndex {
				lastIndex = 0

				continue
			}

			if meta.LastIndex == lastIndex {
				continue
			}

			lastIndex = meta.LastIndex

			for _, e := range entries {
				eventType := EventRegistered

				for _, check := range e.Checks {
					if check.Status == api.HealthCritical {
						eventType = EventDeregistered

						break
					}
				}

				select {
				case ch <- Event{Type: eventType, Service: serviceFromEntry(e)}:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return ch, nil
}

// serviceFromEntry maps a Consul health entry to a Service, recovering the
// advertised scheme from Meta["scheme"]. Shared by Resolve and Watch so both
// surface the same fields (including Scheme).
func serviceFromEntry(e *api.ServiceEntry) Service {
	return Service{
		ID:      e.Service.ID,
		Name:    e.Service.Service,
		Address: e.Service.Address,
		Port:    e.Service.Port,
		Scheme:  e.Service.Meta["scheme"],
		Tags:    e.Service.Tags,
		Meta:    e.Service.Meta,
	}
}

// nextIndex returns the next round-robin index for n healthy instances.
func (r *consulRegistry) nextIndex(n int) int {
	if n <= 1 {
		return 0
	}

	return int(r.rr.Add(1)-1) % n
}

// metaWithScheme returns svc.Meta augmented with the "scheme" key, copying the
// map first so the caller's Meta is never mutated. Returns svc.Meta unchanged
// when no scheme is set.
func metaWithScheme(svc Service) map[string]string {
	if svc.Scheme == "" {
		return svc.Meta
	}

	meta := make(map[string]string, len(svc.Meta)+1)
	for k, v := range svc.Meta {
		meta[k] = v
	}

	meta["scheme"] = svc.Scheme

	return meta
}

// backoffDuration returns the exponential backoff for a given retry attempt,
// capped at watchBackoffMax.
func backoffDuration(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}

	if attempt >= 9 { // watchBackoffBase << 9 == 51.2s, already past the cap
		return watchBackoffMax
	}

	d := watchBackoffBase << uint(attempt)
	if d > watchBackoffMax {
		return watchBackoffMax
	}

	return d
}

// sleepCtx sleeps for d or until ctx is done. It returns true if the full
// duration elapsed, false if ctx was cancelled first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
