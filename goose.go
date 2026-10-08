// Package goose is the server-side Go SDK for the Goose config service. It reads
// feature flags and app configs by polling, a live SSE stream, and/or webhooks,
// evaluates canary rollouts locally, and surfaces change events.
//
// Like the Python SDK (and unlike the browser SDK), it is server-side: it can
// hold a client secret and therefore supports secret-bearing features — app
// configs and webhook registration — in addition to flag reads.
package goose

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// FlagValue is a flag value as delivered to the SDK. Boolean flags resolve to
// bool, number flags to float64, and string / list-of-values flags to their
// stored string verbatim. The zero value (nil) means "unset".
type FlagValue = any

// ConnectionType selects how flag changes reach the SDK.
type ConnectionType string

const (
	// Polling refreshes flags (and watched configs) every PollInterval.
	Polling ConnectionType = "polling"
	// SSE keeps a live stream open and applies flag deltas as they happen.
	SSE ConnectionType = "sse"
	// Webhook registers a target URL with the server and runs an embedded
	// listener that applies deltas; secret-bearing, so it needs a client secret.
	Webhook ConnectionType = "webhook"
)

// StorageAdapter mirrors flag values into an external store as they change. The
// SDK calls CreateOrUpdate on the initial load and on every delta
// (polling / SSE / webhook). Errors are logged, never propagated, so a store
// outage never breaks flag reads.
type StorageAdapter interface {
	CreateOrUpdate(flagset, flagKey string, value FlagValue)
}

// FlagChange is a single flag value change delivered to OnChange listeners.
// PreviousValue is nil when the flag had no prior value.
type FlagChange struct {
	Flagset       string
	FlagKey       string
	Value         FlagValue
	PreviousValue FlagValue
}

// FlagChangeListener is invoked for every flag value change.
type FlagChangeListener func(FlagChange)

// ConfigChangeEvent is a change to a watched config document delivered to On
// listeners. Name is the watched config (document) name. OldValue and NewValue
// are the full config documents (the {"configs": {...}} map) before and after
// the change; OldValue is nil for a newly seen config. ApplyStrategy is
// "requires_restart" when any changed inner entry requires a restart, else
// "immediate".
type ConfigChangeEvent struct {
	Name          string
	OldValue      map[string]any
	NewValue      map[string]any
	ApplyStrategy string
}

// ConfigChangeListener is invoked when a watched config document changes.
type ConfigChangeListener func(ConfigChangeEvent)

// Error is the base SDK error.
type Error struct{ msg string }

func (e *Error) Error() string { return e.msg }

func newError(format string, args ...any) *Error {
	return &Error{msg: fmt.Sprintf(format, args...)}
}

// HTTPError is returned for non-2xx HTTP responses.
type HTTPError struct {
	StatusCode   int
	Message      string
	ResponseText string
	// RetryAfter is the server's Retry-After hint (e.g. on a 429), or 0 when
	// absent. Reconnect/poll backoff waits at least this long.
	RetryAfter time.Duration
}

func (e *HTTPError) Error() string { return e.Message }

// ConnectionState describes whether the client is currently receiving updates.
type ConnectionState int

const (
	// StateConnecting is the initial state before the first successful sync.
	StateConnecting ConnectionState = iota
	// StateLive means the last poll succeeded or the SSE stream is connected.
	StateLive
	// StateDegraded means the client is retrying after failures and is serving
	// last-known-good (cached) values in the meantime.
	StateDegraded
	// StateClosed means Close has been called; no further healing happens.
	StateClosed
)

func (s ConnectionState) String() string {
	switch s {
	case StateConnecting:
		return "connecting"
	case StateLive:
		return "live"
	case StateDegraded:
		return "degraded"
	case StateClosed:
		return "closed"
	default:
		return "unknown"
	}
}

// StateChangeListener is invoked when the client's ConnectionState transitions.
type StateChangeListener func(ConnectionState)

// ErrorListener observes background errors (transport failures, invalid
// payloads, recovered read panics). Errors are informational: they never affect
// what a flag/config read returns, which always falls back to last-known-good.
type ErrorListener func(error)

// Options configures a Client. ClientID, ServerURL, and at least one flagset are
// required; everything else has a sensible default.
type Options struct {
	// ClientID is the SDK client id (gsc_…). Always sent; identifies the org.
	ClientID string
	// Thumbprint is a stable caller identifier sent on every request for
	// analytics. When empty, the SDK derives a best-effort host/process-based
	// value.
	Thumbprint string
	// ClientSecret authenticates secret-bearing endpoints (app configs and
	// webhook registration). Flag reads (polling / SSE) never send it, so a
	// flag-only client may leave it empty.
	ClientSecret string
	// ServerURL is the base URL of the config service, e.g. http://localhost:8080.
	ServerURL string
	// Flagsets is one or more flagset names to read.
	Flagsets []string
	// ConnectionTypes selects delivery modes; defaults to [Polling]. Modes combine.
	ConnectionTypes []ConnectionType
	// NamespaceName is the default namespace applied to every flagset (and the
	// namespace watched configs live in). Required when Configs is set.
	NamespaceName string
	// FlagsetNamespaces overrides the namespace per flagset.
	FlagsetNamespaces map[string]string
	// Configs is the set of config document names to watch (requires NamespaceName).
	Configs []string
	// AppVersion is this application's own version ("2.4.0", "v2.4.1-rc.1").
	// Optional. When set, config entries carrying min_app_version /
	// max_app_version are gated against it: an entry whose range excludes this
	// version resolves to its "default" instead of its "value", and changes to
	// it never trigger a requires_restart. When unset (the default) no gating
	// happens and every entry applies, so existing clients are unaffected.
	AppVersion string
	// RestartOnRequiredChange raises SIGINT (after drains run) when a
	// requires_restart config changes, so an orchestrator restarts the process.
	RestartOnRequiredChange bool
	// DefaultTargetingKey is the stable per-user key used to bucket canary
	// rollouts when GetFlag is called without a targeting key.
	DefaultTargetingKey string
	// StorageAdapter mirrors flag values into an external store as they change.
	StorageAdapter StorageAdapter
	// Cache persists last-known-good state and is read back on Connect (before
	// any network call), so a restart during an outage warm-starts with real
	// values. Use NewFileCache for a built-in file-backed implementation.
	Cache PersistentCache
	// RequireInitialConnect restores fail-fast startup: when true, Connect (and
	// New with AutoConnect) returns an error if the first fetch fails. The
	// default (false) is graceful — the client starts in StateDegraded, serves
	// cached values or defaults, and heals in the background.
	RequireInitialConnect bool
	// OnError observes background errors without affecting reads (see ErrorListener).
	OnError ErrorListener
	// PollInterval is the polling cadence (default 5s, floored at 1s).
	PollInterval time.Duration
	// RequestTimeout is the per-request timeout for non-streaming calls (default 10s).
	RequestTimeout time.Duration
	// SSEReadTimeout caps the idle gap between SSE reads (default 65s, floored at 5s).
	SSEReadTimeout time.Duration
	// WebhookTargetURL is the public URL the server posts deltas to (Webhook mode).
	WebhookTargetURL string
	// WebhookSecret is the shared secret for webhook delivery (auto-generated if empty).
	WebhookSecret string
	// WebhookNamePrefix names registered webhooks (default "goose-go-sdk").
	WebhookNamePrefix string
	// WebhookListenerEnabled starts the embedded webhook HTTP listener (default true).
	WebhookListenerEnabled *bool
	// WebhookListenerHost is the embedded listener bind host (default "0.0.0.0").
	WebhookListenerHost string
	// WebhookListenerPort is the embedded listener port (default 8091).
	WebhookListenerPort int
	// WebhookListenerPath is the embedded listener path (default derived from
	// WebhookTargetURL, else "/webhook").
	WebhookListenerPath string
	// AutoConnect connects on New unless explicitly disabled.
	AutoConnect *bool
	// Logger receives best-effort warnings; defaults to slog.Default().
	Logger *slog.Logger
}

const maxSeenEventIDs = 2048

type rolloutConfig struct {
	percentage *int
	salt       string
	value      FlagValue
}

// Client is a Goose server-side SDK client. It is safe for concurrent use.
type Client struct {
	clientID     string
	clientSecret string
	thumbprint   string
	serverURL    string
	flagsets     []string

	connectionTypes   map[ConnectionType]bool
	namespaceName     string
	flagsetNamespaces map[string]string
	configNames       []string

	defaultTargetingKey     string
	storageAdapter          StorageAdapter
	cache                   PersistentCache
	requireInitialConnect   bool
	restartOnRequiredChange bool

	// appVersionRaw is Options.AppVersion as supplied (for log messages);
	// appVersion is its parsed form, nil when unset or unparseable — in both
	// cases app-version gating is skipped and every config entry applies.
	appVersionRaw string
	appVersion    []int

	pollInterval   time.Duration
	requestTimeout time.Duration
	sseReadTimeout time.Duration

	webhookTargetURL       string
	webhookSecret          string
	webhookNamePrefix      string
	webhookListenerEnabled bool
	webhookListenerHost    string
	webhookListenerPort    int
	webhookListenerPath    string

	log *slog.Logger

	mu    sync.RWMutex
	flags map[string]map[string]FlagValue
	// Per-flagset, per-flag segment overrides, kept beside rollouts because
	// both are evaluated locally on the read path.
	segments       map[string]map[string]segmentTargeting
	flagDataTypes  map[string]map[string]string
	rollouts       map[string]map[string]rolloutConfig
	pollCursors    map[string]int64
	configs        map[string]watchedConfig
	configListener map[string][]ConfigChangeListener
	// warnedDeprecated keys are "<config name>\x00<entry key>", so a deprecated
	// entry is warned about once per process rather than on every read.
	warnedDeprecated map[string]bool
	restartHooks     []ConfigChangeListener
	changeListener   map[int]FlagChangeListener
	nextListenerID   int
	seenEventIDs     *lruSet

	resolvedOrgID        string
	webhookEventsCount   int
	connected            bool
	initialized          bool
	rootCtx              context.Context
	cancel               context.CancelFunc
	wg                   sync.WaitGroup
	webhookServerCloseFn func()

	// self-healing state (all guarded by mu)
	state               ConnectionState
	lastSyncAt          time.Time
	consecutiveFailures int
	stateListeners      map[int]StateChangeListener
	nextStateListenerID int
	errorListeners      map[int]ErrorListener
	nextErrorListenerID int

	// SSE→polling fallback: flagsets whose SSE stream is currently down, and the
	// cancel for the temporary polling loop that keeps them fresh meanwhile.
	sseDownFlagsets map[string]bool
	fallbackCancel  context.CancelFunc

	// cacheDirty signals the debounced flusher that state changed.
	cacheDirty chan struct{}
}

type watchedConfig struct {
	document map[string]any
	revision int
}

// New constructs a Client. By default it connects immediately; set
// Options.AutoConnect to a pointer to false to connect later with Connect.
func New(opts Options) (*Client, error) {
	clientID := strings.TrimSpace(opts.ClientID)
	serverURL := strings.TrimSpace(opts.ServerURL)
	if clientID == "" || serverURL == "" {
		return nil, newError("ClientID and ServerURL are required")
	}

	flagsets := normalizeStrings(opts.Flagsets)
	if len(flagsets) == 0 {
		return nil, newError("at least one flagset is required")
	}

	connectionTypes, err := normalizeConnectionTypes(opts.ConnectionTypes)
	if err != nil {
		return nil, err
	}

	namespace := strings.TrimSpace(opts.NamespaceName)
	flagsetNamespaces, err := buildFlagsetNamespaces(opts.FlagsetNamespaces, namespace, flagsets)
	if err != nil {
		return nil, err
	}

	configNames := normalizeStrings(opts.Configs)
	if len(configNames) > 0 && namespace == "" {
		return nil, newError("NamespaceName is required when watching configs")
	}

	secret := strings.TrimSpace(opts.ClientSecret)
	wantsWebhook := connectionTypes[Webhook]
	if (len(configNames) > 0 || wantsWebhook) && secret == "" {
		return nil, newError("ClientSecret is required when watching configs or using webhooks")
	}

	webhookPath := strings.TrimSpace(opts.WebhookListenerPath)
	if wantsWebhook {
		target := strings.TrimSpace(opts.WebhookTargetURL)
		if target == "" {
			return nil, newError("WebhookTargetURL is required when ConnectionTypes includes Webhook")
		}
		parsedPath, err := webhookTargetPath(target)
		if err != nil {
			return nil, err
		}
		if webhookPath == "" {
			webhookPath = parsedPath
		}
	}
	if webhookPath == "" {
		webhookPath = "/webhook"
	}
	if !strings.HasPrefix(webhookPath, "/") {
		webhookPath = "/" + webhookPath
	}

	webhookSecret := strings.TrimSpace(opts.WebhookSecret)
	if webhookSecret == "" {
		webhookSecret = randomSecret()
	}

	c := &Client{
		clientID:                clientID,
		clientSecret:            secret,
		thumbprint:              resolveThumbprint(opts.Thumbprint, clientID),
		serverURL:               strings.TrimRight(serverURL, "/"),
		flagsets:                flagsets,
		connectionTypes:         connectionTypes,
		namespaceName:           namespace,
		flagsetNamespaces:       flagsetNamespaces,
		configNames:             configNames,
		defaultTargetingKey:     strings.TrimSpace(opts.DefaultTargetingKey),
		storageAdapter:          opts.StorageAdapter,
		cache:                   opts.Cache,
		requireInitialConnect:   opts.RequireInitialConnect,
		restartOnRequiredChange: opts.RestartOnRequiredChange,
		pollInterval:            defaultDuration(opts.PollInterval, 5*time.Second, time.Second),
		requestTimeout:          defaultDuration(opts.RequestTimeout, 10*time.Second, time.Second),
		sseReadTimeout:          defaultDuration(opts.SSEReadTimeout, 65*time.Second, 5*time.Second),
		webhookTargetURL:        strings.TrimSpace(opts.WebhookTargetURL),
		webhookSecret:           webhookSecret,
		webhookNamePrefix:       firstNonEmpty(strings.TrimSpace(opts.WebhookNamePrefix), "goose-go-sdk"),
		webhookListenerEnabled:  opts.WebhookListenerEnabled == nil || *opts.WebhookListenerEnabled,
		webhookListenerHost:     firstNonEmpty(strings.TrimSpace(opts.WebhookListenerHost), "0.0.0.0"),
		webhookListenerPort:     firstNonZero(opts.WebhookListenerPort, 8091),
		webhookListenerPath:     webhookPath,
		log:                     firstNonNilLogger(opts.Logger),
		flags:                   map[string]map[string]FlagValue{},
		segments:                map[string]map[string]segmentTargeting{},
		flagDataTypes:           map[string]map[string]string{},
		rollouts:                map[string]map[string]rolloutConfig{},
		pollCursors:             map[string]int64{},
		configs:                 map[string]watchedConfig{},
		configListener:          map[string][]ConfigChangeListener{},
		warnedDeprecated:        map[string]bool{},
		appVersionRaw:           strings.TrimSpace(opts.AppVersion),
		changeListener:          map[int]FlagChangeListener{},
		seenEventIDs:            newLRUSet(maxSeenEventIDs),
		state:                   StateConnecting,
		stateListeners:          map[int]StateChangeListener{},
		errorListeners:          map[int]ErrorListener{},
		sseDownFlagsets:         map[string]bool{},
		cacheDirty:              make(chan struct{}, 1),
	}

	if opts.OnError != nil {
		c.errorListeners[c.nextErrorListenerID] = opts.OnError
		c.nextErrorListenerID++
	}

	// An unparseable AppVersion fails open: gating is skipped entirely rather
	// than silently resolving every gated entry to its default.
	if c.appVersionRaw != "" {
		parsed, ok := parseAppVersion(c.appVersionRaw)
		if !ok {
			c.log.Warn("ignoring unparseable AppVersion; config app-version gating is disabled",
				"app_version", c.appVersionRaw)
		} else {
			c.appVersion = parsed
		}
	}

	for _, flagset := range flagsets {
		c.flags[flagset] = map[string]FlagValue{}
		c.segments[flagset] = map[string]segmentTargeting{}
		c.flagDataTypes[flagset] = map[string]string{}
		c.rollouts[flagset] = map[string]rolloutConfig{}
		c.pollCursors[flagset] = 0
	}

	if opts.AutoConnect == nil || *opts.AutoConnect {
		if err := c.Connect(); err != nil {
			return nil, err
		}
	}

	return c, nil
}

// OrganizationID returns the org id resolved from the client id, best-effort.
// It is informational; data calls resolve the org server-side from the client id.
func (c *Client) OrganizationID() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.resolvedOrgID
}

// WebhookListenerURL is the local URL the embedded webhook listener serves.
func (c *Client) WebhookListenerURL() string {
	return fmt.Sprintf("http://%s:%d%s", c.webhookListenerHost, c.webhookListenerPort, c.webhookListenerPath)
}

// WebhookEventsReceived counts every webhook POST processed, including duplicates.
func (c *Client) WebhookEventsReceived() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.webhookEventsCount
}

// Connect fetches the initial snapshot and starts the configured loops. It is a
// no-op if already connected.
//
// By default Connect is graceful: if the initial fetch fails it does not return
// an error but starts in StateDegraded, serving warm-started cache values (or
// defaults) while the background loops heal. Set Options.RequireInitialConnect
// to restore fail-fast behavior.
func (c *Client) Connect() error {
	c.mu.Lock()
	if c.connected {
		c.mu.Unlock()
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	c.rootCtx = ctx
	c.mu.Unlock()

	// Warm start: hydrate last-known-good from the persistent cache before any
	// network call, so reads are answerable immediately even if the server is down.
	c.loadFromCache(ctx)

	c.resolveOrganizationID(ctx)

	if err := c.Refresh(); err != nil {
		if c.requireInitialConnect {
			cancel()
			return err
		}
		c.log.Warn("initial refresh failed; starting in degraded mode and healing in background", "err", err)
		c.recordFailure(err)
	} else {
		c.recordSuccess()
	}

	if c.connectionTypes[Webhook] {
		if c.webhookListenerEnabled {
			if err := c.startWebhookListener(); err != nil {
				if c.requireInitialConnect {
					cancel()
					return err
				}
				c.log.Warn("webhook listener failed to start", "err", err)
				c.emitError(err)
			}
		}
		if err := c.RegisterWebhooks(ctx); err != nil {
			if c.requireInitialConnect {
				cancel()
				c.stopWebhookListener()
				return err
			}
			c.log.Warn("webhook registration failed", "err", err)
			c.emitError(err)
		}
	}

	if c.connectionTypes[Polling] {
		c.wg.Add(1)
		go c.pollingLoop(ctx)
	}
	if c.connectionTypes[SSE] {
		for _, flagset := range c.flagsets {
			c.wg.Add(1)
			go c.sseLoop(ctx, flagset)
		}
	}
	if c.cache != nil {
		c.wg.Add(1)
		go c.cacheFlushLoop(ctx)
	}

	c.mu.Lock()
	c.connected = true
	c.mu.Unlock()
	return nil
}

// Close stops all loops, aborts in-flight streams, and shuts down the embedded
// webhook listener. Safe to call more than once.
func (c *Client) Close() {
	c.mu.Lock()
	cancel := c.cancel
	c.cancel = nil
	c.connected = false
	prev := c.state
	c.state = StateClosed
	listeners := c.snapshotStateListeners()
	c.mu.Unlock()

	if prev != StateClosed {
		notifyState(listeners, StateClosed)
	}
	if cancel != nil {
		cancel()
	}
	c.stopWebhookListener()
	c.wg.Wait()
}

// Refresh loads (or, once initialized in polling mode, re-polls) every flagset's
// snapshot and any watched configs.
func (c *Client) Refresh() error {
	ctx, cancel := context.WithTimeout(context.Background(), c.requestTimeout*4)
	defer cancel()

	c.mu.RLock()
	initialized := c.initialized
	c.mu.RUnlock()

	if initialized && c.connectionTypes[Polling] {
		for _, flagset := range c.flagsets {
			if err := c.pollOnce(ctx, flagset); err != nil {
				return err
			}
		}
		if len(c.configNames) > 0 {
			if err := c.pollConfigsOnce(ctx); err != nil {
				return err
			}
		}
		return nil
	}

	for _, flagset := range c.flagsets {
		flags, err := c.fetchConfigSnapshot(ctx, flagset)
		if err != nil {
			return err
		}
		c.applySnapshot(flagset, flags)
		cursor := maxUpdatedTimestamp(flags)
		if cursor == 0 {
			cursor = time.Now().Unix()
		}
		c.mu.Lock()
		if cursor > c.pollCursors[flagset] {
			c.pollCursors[flagset] = cursor
		}
		c.mu.Unlock()
	}

	if len(c.configNames) > 0 {
		if err := c.fetchConfigsSnapshot(ctx); err != nil {
			return err
		}
	}

	c.mu.Lock()
	c.initialized = true
	c.mu.Unlock()
	return nil
}

// Snapshot returns a {flagset: {flagKey: value}} copy of all known flags
// (baseline values, no rollout applied).
func (c *Client) Snapshot() map[string]map[string]FlagValue {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]map[string]FlagValue, len(c.flags))
	for flagset, values := range c.flags {
		inner := make(map[string]FlagValue, len(values))
		for k, v := range values {
			inner[k] = v
		}
		out[flagset] = inner
	}
	return out
}

// OnChange subscribes to flag value changes (polling and SSE/webhook both feed
// it). It returns an unsubscribe function.
func (c *Client) OnChange(listener FlagChangeListener) func() {
	c.mu.Lock()
	id := c.nextListenerID
	c.nextListenerID++
	c.changeListener[id] = listener
	c.mu.Unlock()
	return func() {
		c.mu.Lock()
		delete(c.changeListener, id)
		c.mu.Unlock()
	}
}

func defaultDuration(value, fallback, floor time.Duration) time.Duration {
	if value <= 0 {
		value = fallback
	}
	if value < floor {
		return floor
	}
	return value
}

func firstNonEmpty(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func firstNonZero(value, fallback int) int {
	if value == 0 {
		return fallback
	}
	return value
}

func firstNonNilLogger(logger *slog.Logger) *slog.Logger {
	if logger == nil {
		return slog.Default()
	}
	return logger
}

func normalizeStrings(values []string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]bool{}
	for _, v := range values {
		v = strings.TrimSpace(v)
		if v == "" || seen[v] {
			continue
		}
		seen[v] = true
		out = append(out, v)
	}
	return out
}

func normalizeConnectionTypes(types []ConnectionType) (map[ConnectionType]bool, error) {
	if len(types) == 0 {
		return map[ConnectionType]bool{Polling: true}, nil
	}
	out := map[ConnectionType]bool{}
	for _, t := range types {
		normalized := ConnectionType(strings.ToLower(strings.TrimSpace(string(t))))
		switch normalized {
		case Polling, SSE, Webhook:
			out[normalized] = true
		case "on_demand":
			out[Polling] = true
		default:
			return nil, newError("unsupported connection type %q", string(t))
		}
	}
	if len(out) == 0 {
		out[Polling] = true
	}
	return out, nil
}

func buildFlagsetNamespaces(mapping map[string]string, namespace string, flagsets []string) (map[string]string, error) {
	known := map[string]bool{}
	for _, f := range flagsets {
		known[f] = true
	}
	out := map[string]string{}
	for rawFlagset, rawNamespace := range mapping {
		flagset := strings.TrimSpace(rawFlagset)
		ns := strings.TrimSpace(rawNamespace)
		if flagset == "" || ns == "" {
			continue
		}
		if !known[flagset] {
			return nil, newError("FlagsetNamespaces includes unknown flagset %q", flagset)
		}
		out[flagset] = ns
	}
	if namespace != "" {
		for _, flagset := range flagsets {
			if _, ok := out[flagset]; !ok {
				out[flagset] = namespace
			}
		}
	}
	return out, nil
}

func (c *Client) namespaceForFlagset(flagset string) string {
	return c.flagsetNamespaces[flagset]
}
