package libsd

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LerianStudio/lib-observability/v2/log"
	obsruntime "github.com/LerianStudio/lib-observability/v2/runtime"
	"github.com/hashicorp/consul/api"
)

// reservedMetaKeys are the Meta keys serviceMeta derives authoritatively from the
// Service's endpoints. Any caller-supplied copy is stripped before the derived
// values are written, so a stale reserved key can never reconstruct a phantom
// endpoint on the read path (serviceFromEntry).
var reservedMetaKeys = []string{
	"scheme",
	"external_address", "external_port", "external_scheme",
	"internal_address", "internal_port", "internal_scheme",
}

const (
	// ttlHeartbeatFloor is the minimum interval between TTL heartbeats, so a very
	// small TTL never busy-loops the heartbeat goroutine.
	ttlHeartbeatFloor = time.Second

	// watchPollMargin is the safety headroom added on top of watchWaitTime for the
	// per-poll client-side deadline. A healthy blocking query returns at or before
	// watchWaitTime, so this ceiling never truncates it; it only bounds a poll whose
	// connection wedged (server-side WaitTime alone cannot rescue a stuck socket),
	// so a hung connection surfaces as an error, backs off, and retries instead of
	// freezing the watcher forever.
	watchPollMargin = 15 * time.Second

	// watchBackoffBase/Max bound the exponential backoff applied between failed
	// catalog polls, so a Consul outage never busy-loops the watch goroutine.
	watchBackoffBase = 100 * time.Millisecond
	watchBackoffMax  = 30 * time.Second
)

type consulRegistry struct {
	// client is the "fast" client for short request/response exchanges
	// (Register/Deregister/Resolve/heartbeat). Its transport carries a
	// ResponseHeaderTimeout so a hung Consul fails fast.
	client *api.Client
	// watchClient is the "watch" client for blocking catalog queries. Its
	// transport deliberately omits ResponseHeaderTimeout: a blocking query
	// withholds headers until the catalog index advances (up to watchWaitTime), so
	// a response-header deadline would abort healthy long-polls.
	watchClient *api.Client
	logger      log.Logger

	// allowStale opts catalog reads (Resolve/Watch) into Consul stale mode via
	// QueryOptions.AllowStale. Derived from Config.AllowStale (a *bool, resolved to
	// a concrete bool by withDefaults, which defaults nil → true / stale reads).
	allowStale bool

	// watchWaitTime is the blocking-query wait for the catalog watch long-poll.
	// From Config.WatchWaitTime (defaulted by withDefaults). Kept below any Consul
	// reverse-proxy read timeout so the long-poll returns before the proxy 504s.
	watchWaitTime time.Duration

	// rr is a round-robin cursor used by Resolve to spread load across healthy
	// instances instead of always returning the first one.
	rr atomic.Uint64

	// mu guards heartbeats and closed. Each registered TTL service has a cancel func
	// that stops its heartbeat goroutine; Deregister calls it.
	mu         sync.Mutex
	heartbeats map[string]context.CancelFunc
	// closed reports whether Close has run. Once true, startHeartbeat refuses to
	// insert a new heartbeat, so a Register that races shutdown (e.g. a pending
	// RegisterAsync retry) cannot resurrect a background goroutine that escapes
	// Close's cleanup. Guarded by mu.
	closed bool
}

// newTunedConfig builds an *api.Config wired to a connection-hardened
// *http.Transport. The dial and TLS-handshake timeouts bound connection
// establishment against a dead single-node Consul without ever truncating a
// blocking query (those are transport-level, not whole-request, deadlines).
//
// When fast is true the transport also carries a ResponseHeaderTimeout — safe
// for the short request/response client. When fast is false (the watch client)
// the ResponseHeaderTimeout is left zero, because a Consul blocking query
// legitimately withholds response headers until the catalog index advances.
//
// TLS options (scheme, InsecureSkipVerify, token) flow through cfg the same way
// the SDK expects: Transport.TLSClientConfig is left nil so consul's
// NewHttpClient applies cfg.TLSConfig to the transport it wraps.
func newTunedConfig(c Config, fast bool) *api.Config {
	// DefaultConfig still honors the SDK's own CONSUL_HTTP_* env vars as a
	// fallback; explicit SD_* values (Address/TLS/Token) take precedence below.
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

	// A fresh transport mirroring http.DefaultTransport's pooling, with the tuned
	// connection-establishment timeouts. TLSClientConfig stays nil on purpose (see
	// the doc comment) so consul applies cfg.TLSConfig.
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   c.DialTimeout,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2: true,
		MaxIdleConns:      100,
		// All traffic (fast + watch) targets a single Consul host, so the Go
		// default of 2 idle conns per host caps reuse and forces handshake churn
		// under concurrent resolves. Match MaxIdleConns so the pool can actually
		// hold the connections it is allowed to keep open.
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   c.TLSHandshakeTimeout,
		ExpectContinueTimeout: 1 * time.Second,
	}

	if fast {
		transport.ResponseHeaderTimeout = c.ResponseHeaderTimeout
	}

	cfg.Transport = transport

	return cfg
}

func newConsulRegistry(c Config, logger log.Logger) (Registry, error) {
	if logger == nil {
		logger = log.NewNop()
	}

	client, err := api.NewClient(newTunedConfig(c, true))
	if err != nil {
		return nil, fmt.Errorf("consul: create client: %w", err)
	}

	watchClient, err := api.NewClient(newTunedConfig(c, false))
	if err != nil {
		return nil, fmt.Errorf("consul: create watch client: %w", err)
	}

	// c is expected to have passed through withDefaults (New always applies it),
	// so AllowStale is non-nil; the nil-guard keeps a direct caller that bypassed
	// withDefaults safe and mirrors the withDefaults default (nil → true / stale).
	allowStale := true
	if c.AllowStale != nil {
		allowStale = *c.AllowStale
	}

	return &consulRegistry{
		client:        client,
		watchClient:   watchClient,
		allowStale:    allowStale,
		watchWaitTime: c.WatchWaitTime,
		logger:        logger,
		heartbeats:    make(map[string]context.CancelFunc),
	}, nil
}

// queryOpts returns the QueryOptions used for catalog reads, carrying the
// configured AllowStale mode and bound to ctx. It is nil-receiver safe like the
// rest of the library: a nil registry yields strong-read defaults (AllowStale
// false) still bound to ctx.
func (r *consulRegistry) queryOpts(ctx context.Context) *api.QueryOptions {
	if r == nil {
		return (&api.QueryOptions{}).WithContext(ctx)
	}

	return (&api.QueryOptions{AllowStale: r.allowStale}).WithContext(ctx)
}

func (r *consulRegistry) Register(ctx context.Context, svc Service) error {
	if r == nil {
		return ErrNilRegistry
	}

	// Write-path normalization: promote a legacy flat-only caller into External and
	// mirror the root routable endpoint back into the flat fields, so both the
	// registrable address and the serialized Meta are derived consistently.
	svc.normalizeEndpoints()

	reg := &api.AgentServiceRegistration{
		ID:   svc.ID,
		Name: svc.Name,
		Tags: svc.Tags,
		Meta: serviceMeta(svc),
	}

	// The registrable (root) address is the external endpoint when advertised, else
	// the internal endpoint, else zero. Validate guarantees at least one is present
	// for Manager callers; the nil-guard is defensive for direct Registry callers.
	root := svc.rootEndpoint()
	if root != nil {
		reg.Address = root.Address
		reg.Port = root.Port
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

	rootAddr := ""
	if root != nil {
		rootAddr = root.Addr()
	}

	r.logger.Log(ctx, log.LevelDebug, "service registered",
		log.String("id", svc.ID),
		log.String("name", svc.Name),
		log.String("addr", rootAddr))

	if ttl != "" {
		r.startHeartbeat(reg, ttl)
	}

	return nil
}

// ttlCheckID returns the Consul check ID for a service's TTL check.
func ttlCheckID(serviceID string) string {
	return "service:" + serviceID
}

// isUnknownCheck reports whether err means the TTL check no longer exists in
// Consul (HTTP 404 from UpdateTTL) — the trigger for self-healing re-registration.
// The Consul SDK returns a plain error for this, so we match on the response text.
func isUnknownCheck(err error) bool {
	if err == nil {
		return false
	}

	s := err.Error()

	return strings.Contains(s, "404") ||
		strings.Contains(s, "does not exist") ||
		strings.Contains(s, "Unknown check")
}

// startHeartbeat marks the TTL check passing immediately and then re-passes it
// every TTL/2 from a background goroutine until Deregister cancels it. The
// heartbeat is an OUTBOUND call to Consul, so it works from any network.
//
// Self-heal: when a heartbeat finds the check unknown (HTTP 404 — the
// registration vanished because Consul restarted, or a server-only/agentless
// catalog dropped it), it re-registers the service to recreate the check and
// resumes, instead of failing every TTL/2 forever.
func (r *consulRegistry) startHeartbeat(reg *api.AgentServiceRegistration, ttl string) {
	// Refuse a new heartbeat once Closed: a Register that raced shutdown must not
	// resurrect a background goroutine that escapes Close's cleanup.
	r.mu.Lock()
	closed := r.closed
	r.mu.Unlock()

	if closed {
		return
	}

	serviceID := reg.ID
	checkID := ttlCheckID(serviceID)

	interval := ttlHeartbeatFloor

	if d, err := time.ParseDuration(ttl); err == nil && d/2 > ttlHeartbeatFloor {
		interval = d / 2
	}

	ctx, cancel := context.WithCancel(context.Background())

	pass := func() {
		opts := (&api.QueryOptions{}).WithContext(ctx)

		err := r.client.Agent().UpdateTTLOpts(checkID, "lib-service-discovery heartbeat", api.HealthPassing, opts)
		if err == nil {
			return
		}

		if !isUnknownCheck(err) {
			r.logger.Log(ctx, log.LevelWarn, "ttl heartbeat failed",
				log.String("check", checkID),
				log.Err(err))

			return
		}

		// The check is gone — re-register to recreate it, then re-pass.
		r.logger.Log(ctx, log.LevelWarn, "ttl check unknown; re-registering service",
			log.String("id", serviceID))

		if regErr := r.client.Agent().ServiceRegisterOpts(reg, (api.ServiceRegisterOpts{}).WithContext(ctx)); regErr != nil {
			r.logger.Log(ctx, log.LevelWarn, "self-heal re-register failed",
				log.String("id", serviceID),
				log.Err(regErr))

			return
		}

		if err2 := r.client.Agent().UpdateTTLOpts(checkID, "lib-service-discovery heartbeat", api.HealthPassing, opts); err2 != nil {
			r.logger.Log(ctx, log.LevelWarn, "ttl heartbeat failed after re-register",
				log.String("check", checkID),
				log.Err(err2))
		}
	}

	pass() // first pass moves the check from critical to passing right away

	r.mu.Lock()
	// Re-check under the storing lock: Close may have run during pass(). When it
	// did, drop this heartbeat (cancel the context, store nothing) so no goroutine
	// outlives Close.
	if r.closed {
		r.mu.Unlock()
		cancel()

		return
	}

	// Replace any pre-existing heartbeat for the same service.
	if prev, ok := r.heartbeats[serviceID]; ok {
		prev()
	}

	r.heartbeats[serviceID] = cancel
	r.mu.Unlock()

	// SafeGo wraps the heartbeat loop with panic recovery (KeepRunning): a panic
	// in a single TTL pass must not tear down the process — parity with the managed
	// watch (runManagedUpdates) and RegisterAsync goroutines. Lifecycle is
	// unchanged: ctx.Done() (cancel stored in r.heartbeats) still stops the loop.
	obsruntime.SafeGo(r.logger, "libsd.ttl-heartbeat:"+serviceID, obsruntime.KeepRunning, func() {
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
	})
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

// Close stops every active TTL heartbeat goroutine started by Register, so a
// consumer that forgets to Deregister does not leak them. It cancels each
// tracked heartbeat context and drains the map, making it idempotent: a second
// call finds an empty map and is a no-op. Nil-receiver safe.
//
// Close does NOT deregister services from Consul (a heartbeat simply stops, and
// Consul reaps the instance after DeregisterCriticalServiceAfter); call
// Deregister to remove an instance eagerly.
func (r *consulRegistry) Close() error {
	if r == nil {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Mark closed under the same lock that guards heartbeat insertion, so a
	// startHeartbeat racing this Close either observes closed and refuses to
	// insert, or was inserted before this and is cancelled/drained here.
	r.closed = true

	for serviceID, cancel := range r.heartbeats {
		cancel()
		delete(r.heartbeats, serviceID)
	}

	return nil
}

func (r *consulRegistry) Resolve(ctx context.Context, name, tag string) (Service, error) {
	if r == nil {
		return Service{}, ErrNilRegistry
	}

	entries, _, err := r.client.Health().Service(name, tag, true, r.queryOpts(ctx))
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

	go r.watchLoop(ctx, name, ch)

	return ch, nil
}

// watchLoop drives a Consul blocking query for name, emitting an Event per entry
// each time the catalog index advances, until ctx is cancelled. A failed poll
// backs off exponentially so an outage never busy-loops the goroutine.
func (r *consulRegistry) watchLoop(ctx context.Context, name string, ch chan<- Event) {
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

		// Reads honor AllowStale via queryOpts; the watch client omits the
		// response-header deadline so this blocking long-poll survives. A per-poll
		// client-side deadline (watchWaitTime + margin) is layered on top as a safety
		// ceiling: the server-side WaitTime cannot rescue a wedged connection, so
		// without this a stuck socket would freeze the watcher forever. The ceiling
		// sits above watchWaitTime, so a healthy long-poll (which returns by
		// watchWaitTime) is never truncated. cancel runs before the next iteration so
		// the timer is released each poll (no leak).
		pollCtx, cancel := context.WithTimeout(ctx, r.watchWaitTime+watchPollMargin)
		opts := r.queryOpts(pollCtx)
		opts.WaitIndex = lastIndex
		opts.WaitTime = r.watchWaitTime

		entries, meta, err := r.watchClient.Health().Service(name, "", false, opts)

		cancel()

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

		if !emitEntries(ctx, ch, entries) {
			return
		}
	}
}

// emitEntries sends one Event per entry to ch, returning false if ctx is
// cancelled mid-send so the watch loop can stop.
func emitEntries(ctx context.Context, ch chan<- Event, entries []*api.ServiceEntry) bool {
	for _, e := range entries {
		select {
		case ch <- Event{Type: eventTypeFor(e), Service: serviceFromEntry(e)}:
		case <-ctx.Done():
			return false
		}
	}

	return true
}

// eventTypeFor classifies an entry as deregistered when any of its checks is
// critical, otherwise registered.
func eventTypeFor(e *api.ServiceEntry) EventType {
	for _, check := range e.Checks {
		if check.Status == api.HealthCritical {
			return EventDeregistered
		}
	}

	return EventRegistered
}

// serviceFromEntry maps a Consul health entry to a Service, recovering the
// advertised scheme from Meta["scheme"]. Shared by Resolve and Watch so both
// surface the same fields (including Scheme).
func serviceFromEntry(e *api.ServiceEntry) Service {
	meta := e.Service.Meta

	svc := Service{
		ID:   e.Service.ID,
		Name: e.Service.Service,
		Tags: e.Service.Tags,
		Meta: meta,
	}

	// Reconstruct the AUTHORITATIVE external endpoint. Three cases, distinguished so
	// an internal-only provider is never handed a synthetic external endpoint:
	//   - external_* keys present: the explicit external endpoint this build writes.
	//   - no external_* AND no internal_* keys: a legacy v0.6.0 registration whose
	//     entry Address/Port + Meta["scheme"] WAS the external ingress — reconstruct
	//     it from the root so back-compat readers keep working.
	//   - no external_* but internal_* present: a new internal-only provider; it
	//     never advertised an external endpoint, so External stays nil.
	// A nil Meta reads as zero values, so guarding on the string keys is enough.
	switch {
	case meta["external_address"] != "":
		svc.External = &Endpoint{
			Address: meta["external_address"],
			Port:    atoiSafe(meta["external_port"]),
			Scheme:  meta["external_scheme"],
		}
	case meta["internal_address"] == "":
		svc.External = &Endpoint{
			Address: e.Service.Address,
			Port:    e.Service.Port,
			Scheme:  meta["scheme"],
		}
	}

	// Recover the in-cluster endpoint that Register serialized into Meta
	// (internal_address/internal_port/internal_scheme). When absent/empty the
	// provider never advertised an internal endpoint, so svc.Internal stays nil.
	if meta["internal_address"] != "" {
		svc.Internal = &Endpoint{
			Address: meta["internal_address"],
			Port:    atoiSafe(meta["internal_port"]),
			Scheme:  meta["internal_scheme"],
		}
	}

	// Mirror the root routable endpoint (External if set, else Internal) into the
	// deprecated flat fields for legacy readers. This is a READ path, so mirror
	// ONLY root -> flat (never promote flat -> External): an internal-only provider
	// keeps External nil while its flat mirror stays routable.
	svc.mirrorFlat()

	return svc
}

// atoiSafe parses s as an int, returning 0 on any parse error. It is the
// tolerant counterpart to serviceMeta's strconv.Itoa on the read side: a
// malformed internal_port must never fail this pure mapping path.
func atoiSafe(s string) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}

	return n
}

// nextIndex returns the next round-robin index for n healthy instances.
func (r *consulRegistry) nextIndex(n int) int {
	if n <= 1 {
		return 0
	}

	return int(r.rr.Add(1)-1) % n
}

// serviceMeta returns svc.Meta augmented with the derived registry keys: the
// external endpoint under external_address/external_port/external_scheme (with
// the "scheme" key retained as a mirror of the external scheme for back-compat),
// and, when svc.Internal is set, the in-cluster endpoint under
// internal_address/internal_port/internal_scheme.
//
// svc is normalized on this copy first (svc is passed by value), so a legacy
// flat-only caller is promoted into External and the flat mirror agrees. The map
// is copied before any write, so the caller's Meta is never mutated
// (copy-on-write). Derived keys take precedence over caller-supplied keys of the
// same name. Returns svc.Meta unchanged (nil stays nil) when there is nothing to
// add — neither External nor Internal.
func serviceMeta(svc Service) map[string]string {
	svc.normalizeEndpoints()

	if svc.External == nil && svc.Internal == nil {
		return svc.Meta
	}

	// Room for scheme + the three external_* and three internal_* keys.
	meta := make(map[string]string, len(svc.Meta)+7)
	for k, v := range svc.Meta {
		meta[k] = v
	}

	// Strip EVERY reserved key the caller may have copied in before writing the
	// authoritative values below. Otherwise a stale caller-supplied external_* /
	// internal_* / scheme key for an endpoint svc does NOT have would survive and
	// make serviceFromEntry reconstruct a phantom endpoint on the read path. Only
	// the keys svc actually advertises are re-added afterwards.
	for _, k := range reservedMetaKeys {
		delete(meta, k)
	}

	if svc.External != nil {
		meta["external_address"] = svc.External.Address
		meta["external_port"] = strconv.Itoa(svc.External.Port)

		if svc.External.Scheme != "" {
			meta["external_scheme"] = svc.External.Scheme
			// "scheme" mirrors the EXTERNAL scheme for back-compat with old readers;
			// it is never derived from an internal-only root.
			meta["scheme"] = svc.External.Scheme
		}
	}

	if svc.Internal != nil {
		meta["internal_address"] = svc.Internal.Address
		meta["internal_port"] = strconv.Itoa(svc.Internal.Port)

		// Symmetric with external_scheme: only write internal_scheme when non-empty.
		if svc.Internal.Scheme != "" {
			meta["internal_scheme"] = svc.Internal.Scheme
		}
	}

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

// sleepCtxAny sleeps for d, returning false as soon as EITHER context is done, or
// true when the full duration elapsed. It lets the RegisterAsync retry loop honor
// both the caller ctx and the Manager-lifetime baseCtx, so Manager.Close aborts a
// pending backoff at once.
func sleepCtxAny(a, b context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-a.Done():
		return false
	case <-b.Done():
		return false
	case <-t.C:
		return true
	}
}
