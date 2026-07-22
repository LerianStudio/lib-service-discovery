package libsd

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	// defaultDialTimeout bounds TCP connection establishment to the discovery
	// server. Conservative for a single-node BYOC Consul: a dead node fails fast
	// instead of parking a Resolve at cleanhttp's 30s default.
	defaultDialTimeout = 5 * time.Second

	// defaultTLSHandshakeTimeout bounds the TLS handshake to the discovery server.
	defaultTLSHandshakeTimeout = 5 * time.Second

	// defaultResponseHeaderTimeout bounds time-to-first-response-header for the
	// short request/response client (Resolve/Register/Deregister/heartbeat). It is
	// deliberately NOT applied to the watch client, whose blocking queries withhold
	// headers for up to watchWaitTime.
	defaultResponseHeaderTimeout = 10 * time.Second

	// ttlFloor is the minimum accepted TTL for a TTL-mode health check. A TTL below
	// this is raised to the floor to avoid a false deregistration caused by a GC
	// stop-the-world pause or a brief network blip (both well under a second to a
	// few seconds); 15s leaves room for ≥2 heartbeat attempts (heartbeat runs every
	// TTL/2) before Consul marks the check critical, while still detecting a genuine
	// crash promptly.
	ttlFloor = 15 * time.Second

	// defaultTTL is the safe TTL applied when a TTL-mode health check is left empty.
	defaultTTL = 30 * time.Second

	// defaultSeedTimeout bounds the DynamicResolver's initial (seed) resolve. The
	// seed is fail-open: a slow or dead discovery server must not hang boot for the
	// full defaultResponseHeaderTimeout (10s) per resolver, so the seed runs under a
	// short, separate deadline and degrades to the fallback (or empty) when it
	// expires. The long-lived watch is unaffected — it keeps its own lifetime
	// context and populates last-known-good on its first success. It also bounds the
	// managed resolvers' lazy seed (the single per-name Consul touch on the resolve
	// path); the background watch that follows is never truncated by it.
	defaultSeedTimeout = 3 * time.Second

	// defaultWatchWaitTime caps how long a single blocking catalog watch query
	// parks before returning. It bounds only IDLE re-polling — a catalog change
	// returns immediately via WaitIndex, so this never adds change-detection
	// latency. Kept short so the long-poll fits within common reverse-proxy read
	// timeouts (nginx/openresty default ~60s) in front of Consul: a longer wait
	// behind such a proxy surfaces as 504 Gateway Timeout on the watch. A direct
	// in-cluster Consul can raise it via SD_WATCH_WAIT_TIME to reduce idle polling.
	defaultWatchWaitTime = 30 * time.Second
)

// Config holds the configuration for the Manager.
// Populate it manually or call ConfigFromEnv to load from environment variables.
type Config struct {
	// Enabled controls whether Consul-backed discovery is active.
	// When false all operations are no-ops and Resolve returns the fallback.
	Enabled bool

	// ConsulAddr is the address of the Consul agent (host:port).
	// Defaults to "localhost:8500".
	ConsulAddr string

	// AdvertiseAddr is the address this service advertises to Consul.
	// Required when Enabled is true. Accepts either a plain hostname
	// ("fees.dev.example.net") or a full URL ("https://fees.dev.example.net").
	// When a full URL is provided, the scheme is extracted into AdvertiseScheme
	// automatically by ConfigFromEnv and withDefaults.
	AdvertiseAddr string

	// AdvertiseScheme is the URL scheme ("https", "http") stored in Consul
	// Meta["scheme"] when this service registers. Set automatically when
	// AdvertiseAddr contains a scheme prefix.
	AdvertiseScheme string

	// AdvertisePort overrides the port reported to Consul.
	// When zero the port passed to Register is used as-is.
	AdvertisePort int

	// AdvertiseInternalAddr is the cluster-internal address this service advertises
	// to Consul, mirroring AdvertiseAddr but for the in-cluster (K8s DNS) view.
	// Optional — leave empty when no distinct internal endpoint exists. Accepts a
	// plain hostname ("svc.ns.svc.cluster.local") or a full URL
	// ("https://svc.ns.svc.cluster.local"); when a full URL is provided the scheme
	// is extracted into AdvertiseInternalScheme automatically by ConfigFromEnv and
	// withDefaults.
	AdvertiseInternalAddr string

	// AdvertiseInternalScheme is the URL scheme ("https", "http") for the
	// cluster-internal endpoint, mirroring AdvertiseScheme. Set automatically when
	// AdvertiseInternalAddr contains a scheme prefix.
	AdvertiseInternalScheme string

	// AdvertiseInternalPort overrides the internal port reported to Consul,
	// mirroring AdvertisePort. When zero the port passed to Register is used as-is.
	AdvertiseInternalPort int

	// PreferView is the default view used by view-aware resolvers when the caller
	// does not pass an explicit view. Maps to SD_PREFER_VIEW (internal/external),
	// default external.
	PreferView EndpointView

	// Workload scopes this Manager to a logical deployment group (e.g. "tenant-a").
	// When set, Register automatically tags services with "workload=<id>" and
	// Resolve filters Consul results to instances carrying that same tag.
	// Leave empty to disable workload filtering (all healthy instances are candidates).
	Workload string

	// TLS enables HTTPS to the discovery server. Maps to SD_TLS.
	TLS bool

	// TLSSkipVerify disables server certificate verification (dev/self-signed).
	// Maps to SD_TLS_SKIP_VERIFY.
	TLSSkipVerify bool

	// Token is the ACL token sent to the discovery server. Maps to SD_TOKEN.
	Token string

	// DialTimeout bounds TCP connection establishment to the discovery server.
	// Maps to SD_DIAL_TIMEOUT. Zero applies defaultDialTimeout via withDefaults.
	// This is a transport-level dial timeout, not a whole-request deadline, so it
	// never truncates the watch's long-poll blocking queries.
	DialTimeout time.Duration

	// TLSHandshakeTimeout bounds the TLS handshake to the discovery server.
	// Maps to SD_TLS_HANDSHAKE_TIMEOUT. Zero applies defaultTLSHandshakeTimeout.
	TLSHandshakeTimeout time.Duration

	// ResponseHeaderTimeout bounds time-to-first-response-header for the short
	// request/response client (Resolve/Register/Deregister/heartbeat). Maps to
	// SD_RESPONSE_HEADER_TIMEOUT. Zero applies defaultResponseHeaderTimeout.
	// It is applied ONLY to that client — never to the watch client, whose
	// blocking queries legitimately withhold headers for up to watchWaitTime.
	ResponseHeaderTimeout time.Duration

	// SeedTimeout bounds the DynamicResolver's initial (seed) resolve in
	// WatchResolve/WatchResolveService. Maps to SD_SEED_TIMEOUT. Zero applies
	// defaultSeedTimeout via withDefaults. The seed is fail-open: when it exceeds
	// this deadline (or the discovery server errors) the resolver is still built and
	// its watch still starts — it just degrades to the fallback (or an empty
	// address) until the watch's first success. This is a seed-only deadline derived
	// from the caller's context; it never truncates the long-lived watch.
	SeedTimeout time.Duration

	// WatchWaitTime caps how long a single blocking catalog watch query parks
	// before returning. It bounds only IDLE re-polling — a catalog change returns
	// immediately via WaitIndex, so this never adds change-detection latency. Maps
	// to SD_WATCH_WAIT_TIME. Zero applies defaultWatchWaitTime via withDefaults.
	// Keep it below any reverse proxy's read timeout in front of Consul (else the
	// long-poll 504s); raise it for a direct in-cluster Consul to cut idle polling.
	WatchWaitTime time.Duration

	// AllowStale opts reads (Resolve and Watch) into Consul stale mode
	// (QueryOptions.AllowStale). Maps to SD_ALLOW_STALE. It is a *bool so the
	// unset state (nil) is distinguishable from an explicit false: nil defaults to
	// true (stale reads) via withDefaults, while &false forces strongly consistent
	// leader reads and &true forces stale reads explicitly.
	//
	// Default (nil → true): stale reads keep resolution available when the leader is
	// briefly unavailable, at the cost of possibly serving a slightly out-of-date
	// catalog view from a follower. On a single-node Consul there is no follower, so
	// this mainly future-proofs an HA upgrade and relaxes the leader-liveness
	// requirement during a leadership blip. Set &false for strong leader reads.
	AllowStale *bool

	// Logger receives structured log output from the Manager and registry.
	// It is a version-agnostic, slog-compatible interface (see libsd.Logger):
	// pass any *slog.Logger or equivalent directly. A nil Logger silences output.
	Logger Logger
}

// ConfigFromEnv returns a Config populated from environment variables.
//
// Canonical, backend-agnostic SD_* names (legacy names accepted as fallback):
//
//	SD_ENABLED          — "true" to enable               (legacy: SERVICE_DISCOVERY_ENABLED)
//	SD_ADDRESS          — discovery server addr host:port (legacy: CONSUL_ADDR; default "localhost:8500")
//	SD_EXTERNAL_ADDRESS — hostname or full URL this instance advertises (aliases: SD_ADVERTISE_ADDRESS, SERVICE_ADVERTISE_ADDR)
//	SD_EXTERNAL_PORT    — port override, 0 = use Register port (aliases: SD_ADVERTISE_PORT, SERVICE_ADVERTISE_PORT)
//	SD_INTERNAL_ADDRESS — in-cluster hostname or full URL for the internal endpoint (no legacy fallback)
//	SD_INTERNAL_PORT    — internal port override, 0 = use Register port (no legacy fallback)
//	SD_INTERNAL_SCHEME  — scheme for the internal endpoint (no legacy fallback)
//	SD_PREFER_VIEW      — default view for view-aware resolvers: internal/external, default external (no legacy fallback)
//	SD_WORKLOAD         — workload scope for tag filtering (legacy: WORKLOAD_ID)
//	SD_TLS              — "true" for HTTPS to the server
//	SD_TLS_SKIP_VERIFY  — "true" to skip server cert verification
//	SD_TOKEN            — ACL token
//	SD_DIAL_TIMEOUT            — TCP dial timeout to the server (duration, e.g. "5s"); empty → default in New()
//	SD_TLS_HANDSHAKE_TIMEOUT  — TLS handshake timeout (duration); empty → default in New()
//	SD_RESPONSE_HEADER_TIMEOUT — response-header timeout for the fast client (duration); empty → default in New()
//	SD_SEED_TIMEOUT           — bound for the resolver seed resolve, DynamicResolver and managed (duration); empty → default in New()
//	SD_WATCH_WAIT_TIME        — blocking-query wait for the catalog watch (duration); empty → 30s. Keep below a reverse proxy's read timeout in front of Consul
//	SD_ALLOW_STALE            — "true"/"false" to opt reads into/out of Consul stale mode; unset → default (stale reads)
func ConfigFromEnv() Config {
	c := Config{
		Enabled:       anyEnvTrue("SD_ENABLED", "SERVICE_DISCOVERY_ENABLED"),
		ConsulAddr:    firstEnv("localhost:8500", "SD_ADDRESS", "CONSUL_ADDR"),
		AdvertiseAddr: firstEnv("", "SD_EXTERNAL_ADDRESS", "SD_ADVERTISE_ADDRESS", "SERVICE_ADVERTISE_ADDR"),
		AdvertisePort: firstEnvInt(0, "SD_EXTERNAL_PORT", "SD_ADVERTISE_PORT", "SERVICE_ADVERTISE_PORT"),

		AdvertiseInternalAddr:   firstEnv("", "SD_INTERNAL_ADDRESS"),
		AdvertiseInternalPort:   firstEnvInt(0, "SD_INTERNAL_PORT"),
		AdvertiseInternalScheme: os.Getenv("SD_INTERNAL_SCHEME"),

		PreferView: parsePreferView(os.Getenv("SD_PREFER_VIEW")),

		Workload:      firstEnv("", "SD_WORKLOAD", "WORKLOAD_ID"),
		TLS:           os.Getenv("SD_TLS") == "true",
		TLSSkipVerify: os.Getenv("SD_TLS_SKIP_VERIFY") == "true",
		Token:         os.Getenv("SD_TOKEN"),

		DialTimeout:           firstEnvDuration("SD_DIAL_TIMEOUT"),
		TLSHandshakeTimeout:   firstEnvDuration("SD_TLS_HANDSHAKE_TIMEOUT"),
		ResponseHeaderTimeout: firstEnvDuration("SD_RESPONSE_HEADER_TIMEOUT"),
		SeedTimeout:           firstEnvDuration("SD_SEED_TIMEOUT"),
		WatchWaitTime:         firstEnvDuration("SD_WATCH_WAIT_TIME"),
		AllowStale:            envBoolPtr("SD_ALLOW_STALE"),
	}

	c = c.parseAdvertiseAddrs()

	return c
}

// Validate returns an error when the Config is inconsistent. When Enabled is true,
// only ConsulAddr is required (ErrEmptyConsulAddr otherwise).
//
// An advertise address is deliberately NOT required here. A Manager that is Enabled
// with no advertise address (neither AdvertiseAddr nor AdvertiseInternalAddr) is a
// valid consumer-only Manager: it can resolve and watch services, it just cannot
// register one. The advertise requirement is enforced at Register time instead
// (ErrNoEndpoint), so a pure consumer no longer has to set a dummy advertise
// address just to pass Validate.
func (c Config) Validate() error {
	if !c.Enabled {
		return nil
	}

	if c.ConsulAddr == "" {
		return ErrEmptyConsulAddr
	}

	return nil
}

func (c Config) withDefaults() Config {
	if c.ConsulAddr == "" {
		c.ConsulAddr = "localhost:8500"
	}

	// Fill the transport-level timeout knobs with conservative defaults when the
	// caller left them zero.
	if c.DialTimeout <= 0 {
		c.DialTimeout = defaultDialTimeout
	}

	if c.TLSHandshakeTimeout <= 0 {
		c.TLSHandshakeTimeout = defaultTLSHandshakeTimeout
	}

	if c.ResponseHeaderTimeout <= 0 {
		c.ResponseHeaderTimeout = defaultResponseHeaderTimeout
	}

	if c.SeedTimeout <= 0 {
		c.SeedTimeout = defaultSeedTimeout
	}

	if c.WatchWaitTime <= 0 {
		c.WatchWaitTime = defaultWatchWaitTime
	}

	// AllowStale is a *bool so the unset state (nil) is distinguishable from an
	// explicit false: nil defaults to true (stale reads); an explicit &false/&true
	// is left untouched.
	if c.AllowStale == nil {
		stale := true
		c.AllowStale = &stale
	}

	return c.parseAdvertiseAddrs()
}

// ttlWithDefaults validates and normalizes a TTL-mode health-check TTL, blinding
// the caller against two footguns that cause false deregistration on a single-node
// Consul:
//
//   - An empty TTL is replaced with defaultTTL (30s). Callers reach this branch
//     only when they are in TTL mode (a non-empty TTL selects TTL mode in the
//     registry); the empty default is a defensive, directly-tested guarantee of
//     the helper.
//   - A TTL below ttlFloor (15s) is RAISED to the floor rather than rejected. This
//     is the resilience-first choice (and the objective of this change): a service
//     that asked for an unsafe 5s TTL still registers and stays discoverable, but
//     can no longer be falsely evicted by a sub-second GC pause. It diverges from
//     Config.Validate's reject-on-error style deliberately — a too-small TTL is
//     recoverable by clamping, whereas a missing address is not.
//
// An unparseable TTL is a genuine configuration mistake and returns ErrInvalidTTL.
func ttlWithDefaults(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return defaultTTL.String(), nil
	}

	d, err := time.ParseDuration(raw)
	if err != nil {
		return "", fmt.Errorf("%w: %q", ErrInvalidTTL, raw)
	}

	if d < ttlFloor {
		return ttlFloor.String(), nil
	}

	// Canonicalize via Duration.String() so the output is normalized (e.g. "1m"
	// -> "1m0s") regardless of how the caller spelled the input.
	return d.String(), nil
}

// parseAdvertiseAddrs extracts the scheme from both the external (AdvertiseAddr)
// and internal (AdvertiseInternalAddr) endpoints when they contain "://", so
// callers can use "https://fees.example.net" or plain "fees.example.net" for
// either. For each pair the parsed scheme is written to the matching *Scheme
// field only when it is currently empty, and the addr is replaced with the bare
// hostname only when parsing succeeded (i.e. a scheme was present).
func (c Config) parseAdvertiseAddrs() Config {
	if scheme, host, port := splitSchemeHost(c.AdvertiseAddr); scheme != "" {
		if c.AdvertiseScheme == "" {
			c.AdvertiseScheme = scheme
		}

		// The port embedded in the URL ("https://svc:8443" -> 8443) seeds the port
		// only when no explicit AdvertisePort was supplied — an explicit env/config
		// port always wins. Without this the URL port would be silently dropped and
		// Register would fall back to its own port.
		if c.AdvertisePort == 0 {
			if p, err := strconv.Atoi(port); err == nil {
				c.AdvertisePort = p
			}
		}

		c.AdvertiseAddr = host
	}

	if scheme, host, port := splitSchemeHost(c.AdvertiseInternalAddr); scheme != "" {
		if c.AdvertiseInternalScheme == "" {
			c.AdvertiseInternalScheme = scheme
		}

		// Same precedence as the external endpoint: an explicit AdvertiseInternalPort
		// wins; otherwise the URL's port is preserved instead of dropped.
		if c.AdvertiseInternalPort == 0 {
			if p, err := strconv.Atoi(port); err == nil {
				c.AdvertiseInternalPort = p
			}
		}

		c.AdvertiseInternalAddr = host
	}

	return c
}

// splitSchemeHost splits addr into its URL scheme, bare hostname, and port. When
// addr is empty, cannot be parsed, or carries no scheme/host, it returns
// ("", addr, "") so the caller leaves the original value untouched. The port is
// the URL's port when present ("https://svc:8443" -> "8443"), else "".
func splitSchemeHost(addr string) (scheme, host, port string) {
	if addr == "" {
		return "", addr, ""
	}

	u, err := url.Parse(addr)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "", addr, ""
	}

	return u.Scheme, u.Hostname(), u.Port()
}

// parsePreferView normalizes a raw SD_PREFER_VIEW value into an EndpointView.
// It lowercases and trims the input; "internal" maps to Internal, while
// "external" and the empty string map to External. Any other value degrades to
// External — this is a tolerant, pure mapping that never errors and never logs,
// mirroring atoiSafe/serviceFromEntry on the parse side.
func parsePreferView(raw string) EndpointView {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case string(Internal):
		return Internal
	default:
		// "external", "", and any unrecognized value all resolve to External.
		return External
	}
}

// firstEnv returns the first non-empty value among keys (precedence order),
// or fallback when none is set. Used to prefer SD_* over legacy names.
func firstEnv(fallback string, keys ...string) string {
	for _, k := range keys {
		if v := os.Getenv(k); v != "" {
			return v
		}
	}

	return fallback
}

// firstEnvInt is firstEnv parsed as an int.
func firstEnvInt(fallback int, keys ...string) int {
	for _, k := range keys {
		if v := os.Getenv(k); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				return n
			}
		}
	}

	return fallback
}

// firstEnvDuration is firstEnv parsed as a time.Duration (e.g. "5s", "1m"). A
// value that fails time.ParseDuration is skipped, so a malformed env var degrades
// to the next key or zero instead of failing config loading.
func firstEnvDuration(keys ...string) time.Duration {
	for _, k := range keys {
		if v := os.Getenv(k); v != "" {
			if d, err := time.ParseDuration(v); err == nil {
				return d
			}
		}
	}

	return 0
}

// envBoolPtr reads key as an optional bool. An unset, empty, or unparseable
// value returns nil (leaving the default to withDefaults); a parseable value
// (via strconv.ParseBool: "true"/"false"/"1"/"0"/…) returns a pointer to it.
// This lets a *bool config knob distinguish "unset" from an explicit false.
func envBoolPtr(key string) *bool {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return nil
	}

	b, err := strconv.ParseBool(v)
	if err != nil {
		return nil
	}

	return &b
}

// anyEnvTrue reports whether any of keys is set to the string "true".
func anyEnvTrue(keys ...string) bool {
	for _, k := range keys {
		if os.Getenv(k) == "true" {
			return true
		}
	}

	return false
}
