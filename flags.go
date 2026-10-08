package goose

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// getFlagOptions holds the resolved options for a GetFlag call.
type getFlagOptions struct {
	// context holds attributes for segment targeting; nil means no attributes,
	// so every segment condition simply fails to match.
	context      EvaluationContext
	flagset      string
	hasFlagset   bool
	def          FlagValue
	targetingKey string
	hasTargeting bool
}

// GetFlagOption configures a single GetFlag call.
type GetFlagOption func(*getFlagOptions)

// WithFlagset selects which flagset to read from (required when multiple
// flagsets are configured).
func WithFlagset(flagset string) GetFlagOption {
	return func(o *getFlagOptions) {
		o.flagset = flagset
		o.hasFlagset = true
	}
}

// WithDefault sets the value returned when the flag is unknown.
func WithDefault(value FlagValue) GetFlagOption {
	return func(o *getFlagOptions) { o.def = value }
}

// WithTargetingKey sets the stable per-user key used to bucket canary rollouts.
func WithTargetingKey(key string) GetFlagOption {
	return func(o *getFlagOptions) {
		o.targetingKey = key
		o.hasTargeting = true
	}
}

// GetFlag returns a flag's value. When the flag has a canary rollout configured,
// the value is evaluated locally by deterministically bucketing the targeting
// key (falling back to DefaultTargetingKey): in-bucket users get the candidate
// value, everyone else the baseline. Flags without a rollout return their stored
// value unchanged. Unknown flags return the WithDefault value (nil if unset).
// WithContext supplies the attributes used for segment targeting. Separate
// from the targeting key: the key decides which side of a canary a subject
// lands on, the context decides whether they are in the audience at all.
func WithContext(attributes EvaluationContext) GetFlagOption {
	return func(o *getFlagOptions) { o.context = attributes }
}

func (c *Client) GetFlag(flagKey string, opts ...GetFlagOption) (value FlagValue, err error) {
	o := getFlagOptions{}
	for _, opt := range opts {
		opt(&o)
	}

	flagset, err := c.resolveFlagset(o)
	if err != nil {
		return nil, err
	}

	c.mu.RLock()
	stored, ok := c.flags[flagset][flagKey]
	rollout, hasRollout := c.rollouts[flagset][flagKey]
	dataType := c.flagDataTypes[flagset][flagKey]
	targeting, hasTargeting := c.segments[flagset][flagKey]
	c.mu.RUnlock()

	// Fallback ladder: last-known-good stored value → caller default.
	fallback := o.def
	if ok {
		fallback = stored
	}

	// A read must never surface a fault to the caller. If rollout evaluation or
	// coercion panics on unexpected data, recover and return last-known-good.
	defer func() {
		if r := recover(); r != nil {
			c.log.Warn("flag resolution recovered; returning last-known-good", "flag", flagKey, "err", r)
			c.emitError(newError("flag resolution panicked for %q: %v", flagKey, r))
			value, err = fallback, nil
		}
	}()

	// Segment targeting is resolved before the canary: being in the audience is
	// a statement about who you are, while a canary is a statement about what
	// fraction of traffic sees something. A subject in the segment gets the
	// targeted value outright rather than being re-diced.
	if hasTargeting && matchesSegment(targeting.definition, o.context) {
		return coerceValue(targeting.value, dataType), nil
	}

	if hasRollout && rollout.percentage != nil {
		return c.evaluateRollout(flagKey, rollout, o, fallback, dataType), nil
	}
	return fallback, nil
}

// GetBool reads a flag as a bool, returning def when unknown or not a bool.
func (c *Client) GetBool(flagKey string, def bool, opts ...GetFlagOption) bool {
	value, err := c.GetFlag(flagKey, append(opts, WithDefault(def))...)
	if err != nil {
		return def
	}
	if b, ok := value.(bool); ok {
		return b
	}
	return def
}

// GetString reads a flag as a string, returning def when unknown or not a string.
func (c *Client) GetString(flagKey string, def string, opts ...GetFlagOption) string {
	value, err := c.GetFlag(flagKey, append(opts, WithDefault(def))...)
	if err != nil {
		return def
	}
	if s, ok := value.(string); ok {
		return s
	}
	return def
}

// GetFloat reads a numeric flag as a float64, returning def when unknown or not numeric.
func (c *Client) GetFloat(flagKey string, def float64, opts ...GetFlagOption) float64 {
	value, err := c.GetFlag(flagKey, append(opts, WithDefault(def))...)
	if err != nil {
		return def
	}
	if f, ok := toFloat(value); ok {
		return f
	}
	return def
}

// GetInt reads a numeric flag as an int64 (truncating fractional parts),
// returning def when unknown or not numeric.
func (c *Client) GetInt(flagKey string, def int64, opts ...GetFlagOption) int64 {
	value, err := c.GetFlag(flagKey, append(opts, WithDefault(def))...)
	if err != nil {
		return def
	}
	if f, ok := toFloat(value); ok {
		return int64(f)
	}
	return def
}

func (c *Client) resolveFlagset(o getFlagOptions) (string, error) {
	if o.hasFlagset {
		return o.flagset, nil
	}
	if len(c.flagsets) != 1 {
		return "", newError("flagset is required when multiple flagsets are configured")
	}
	return c.flagsets[0], nil
}

func (c *Client) evaluateRollout(flagKey string, rollout rolloutConfig, o getFlagOptions, baseline FlagValue, dataType string) FlagValue {
	key := o.targetingKey
	if !o.hasTargeting {
		key = c.defaultTargetingKey
	}
	if key == "" {
		c.log.Warn("flag has a canary rollout but no targeting key was provided; returning the baseline value", "flag", flagKey)
		return baseline
	}
	bucket := bucketUser(rollout.salt, flagKey, key)
	if bucket < *rollout.percentage {
		if rollout.value == nil {
			return baseline
		}
		return coerceValue(rollout.value, dataType)
	}
	return baseline
}

// bucketUser deterministically maps a user to a bucket in [0, 99] by hashing
// salt:flagKey:targetingKey with SHA-256 and reducing the first 8 digest bytes
// mod 100. Matches the Python/JS SDKs so every SDK buckets identically; the salt
// is stable per flag, so ramps are sticky and monotonic.
func bucketUser(salt, flagKey, targetingKey string) int {
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s:%s:%s", salt, flagKey, targetingKey)))
	return int(binary.BigEndian.Uint64(digest[:8]) % 100)
}

func (c *Client) pollingLoop(ctx context.Context) {
	defer c.wg.Done()
	ticker := time.NewTicker(c.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		for _, flagset := range c.flagsets {
			if err := c.pollOnce(ctx, flagset); err != nil {
				if ctx.Err() == nil {
					c.log.Warn("poll failed", "flagset", flagset, "err", err)
					c.recordFailure(err)
					c.honorRetryAfter(ctx, err)
				}
			} else {
				c.recordSuccess()
			}
		}
		if len(c.configNames) > 0 {
			if err := c.pollConfigsOnce(ctx); err != nil && ctx.Err() == nil {
				c.log.Warn("config poll failed", "err", err)
				c.recordFailure(err)
				c.honorRetryAfter(ctx, err)
			}
		}
	}
}

// honorRetryAfter blocks for a 429's Retry-After hint (bounded by ctx) so the
// client does not immediately re-hit a rate-limited server on the next tick.
func (c *Client) honorRetryAfter(ctx context.Context, err error) {
	he := httpErrorOf(err)
	if he == nil || he.StatusCode != http.StatusTooManyRequests || he.RetryAfter <= 0 {
		return
	}
	select {
	case <-ctx.Done():
	case <-time.After(he.RetryAfter):
	}
}

func httpErrorOf(err error) *HTTPError {
	var he *HTTPError
	if asHTTPError(err, &he) {
		return he
	}
	return nil
}

func (c *Client) fetchConfigSnapshot(ctx context.Context, flagset string) ([]map[string]any, error) {
	query := map[string]string{
		"client_id": c.clientID,
		"flagset":   flagset,
	}
	if ns := c.namespaceForFlagset(flagset); ns != "" {
		query["namespace_name"] = ns
	}
	payload, err := c.requestJSON(ctx, http.MethodGet, "/api/v1/config", query, nil)
	if err != nil {
		return nil, err
	}
	return extractFlags(payload), nil
}

func (c *Client) pollOnce(ctx context.Context, flagset string) error {
	c.mu.RLock()
	cursor := c.pollCursors[flagset]
	c.mu.RUnlock()

	body := map[string]any{
		"client_id": c.clientID,
		"flagset":   flagset,
		"timestamp": cursor,
	}
	if ns := c.namespaceForFlagset(flagset); ns != "" {
		body["namespace_name"] = ns
	}

	payload, err := c.requestJSON(ctx, http.MethodPost, "/api/v1/poll", nil, body)
	if err != nil {
		return err
	}

	maxTimestamp := cursor
	for _, raw := range extractFlags(payload) {
		flagKey := strings.TrimSpace(asString(raw["flag_key"]))
		if flagKey == "" {
			continue
		}
		if ts := toUnixSeconds(asString(raw["updated_at"])); ts > maxTimestamp {
			maxTimestamp = ts
		}
		dataType := strings.TrimSpace(asString(raw["flag_data_type"]))
		targeting, hasTargeting := parseSegmentTargeting(raw)
		c.commitFlag(flagset, flagKey, dataType, raw["flag_value"], extractRollout(raw), targeting, hasTargeting)
	}

	c.mu.Lock()
	c.pollCursors[flagset] = maxTimestamp
	c.mu.Unlock()
	return nil
}

// commitFlag validates an incoming value against its declared data type and,
// only if usable, updates the flag's data type, rollout, and value. A malformed
// value is rejected — logged and reported to OnError — so the last-known-good
// value is retained rather than overwritten with garbage.
func (c *Client) commitFlag(flagset, flagKey, dataType string, raw any, rollout rolloutConfig, targeting segmentTargeting, hasTargeting bool) {
	value, ok := coerceAndValidate(raw, dataType)
	if !ok {
		c.log.Warn("rejecting malformed flag value; retaining last-known-good",
			"flagset", flagset, "flag", flagKey, "dataType", dataType)
		c.emitError(newError("malformed value for flag %q in flagset %q (data type %q)", flagKey, flagset, dataType))
		return
	}
	c.mu.Lock()
	c.flagDataTypes[flagset][flagKey] = dataType
	c.rollouts[flagset][flagKey] = rollout
	if c.segments[flagset] == nil {
		c.segments[flagset] = map[string]segmentTargeting{}
	}
	if hasTargeting {
		c.segments[flagset][flagKey] = targeting
	} else {
		// A payload with no segment means targeting was removed; clearing it
		// mirrors how a delta with no percentage clears a canary.
		delete(c.segments[flagset], flagKey)
	}
	c.mu.Unlock()
	c.setFlag(flagset, flagKey, value)
}

// coerceAndValidate coerces raw to the type implied by dataType and reports
// whether the result is usable. A nil value, or one that fails to match its
// declared type (e.g. a non-numeric string for a "number" flag), is rejected.
func coerceAndValidate(raw any, dataType string) (FlagValue, bool) {
	if raw == nil {
		return nil, false
	}
	value := coerceValue(raw, dataType)
	switch strings.ToLower(strings.TrimSpace(dataType)) {
	case "bool":
		_, ok := value.(bool)
		return value, ok
	case "number", "int", "integer", "float", "double":
		_, ok := value.(float64)
		return value, ok
	}
	// string / list_of_values / unknown: accept any non-nil coerced value.
	return value, true
}

func (c *Client) applySnapshot(flagset string, flags []map[string]any) {
	for _, raw := range flags {
		flagKey := strings.TrimSpace(asString(raw["flag_key"]))
		if flagKey == "" {
			continue
		}
		dataType := strings.TrimSpace(asString(raw["flag_data_type"]))
		targeting, hasTargeting := parseSegmentTargeting(raw)
		c.commitFlag(flagset, flagKey, dataType, raw["flag_value"], extractRollout(raw), targeting, hasTargeting)
	}
}

const (
	// sseStableInterval is how long a stream must stay connected to be judged
	// healthy, resetting reconnect backoff and clearing the SSE-down marker.
	sseStableInterval = 30 * time.Second
	// sseFallbackThreshold is the consecutive reconnect failure count (SSE-only)
	// after which a temporary polling loop is started to keep flags fresh.
	sseFallbackThreshold = 3
)

func (c *Client) sseLoop(ctx context.Context, flagset string) {
	defer c.wg.Done()

	payload := map[string]any{
		"clientId": c.clientID,
		"flagSet":  flagset,
	}
	if ns := c.namespaceForFlagset(flagset); ns != "" {
		payload["namespaceName"] = ns
	}

	bo := newBackoff(time.Second, 30*time.Second)
	failures := 0
	for ctx.Err() == nil {
		start := time.Now()
		err := c.consumeSSEStream(ctx, payload, flagset)
		if ctx.Err() != nil {
			return
		}

		// A stream that stayed up for a healthy interval counts as recovered:
		// reset backoff and stop any fallback polling.
		if time.Since(start) >= sseStableInterval {
			bo.reset()
			failures = 0
			c.markSSEUp(flagset)
		}

		if err != nil {
			c.recordFailure(err)
			c.log.Warn("sse loop disconnected", "flagset", flagset, "err", err)
		}

		failures++
		if failures >= sseFallbackThreshold {
			c.markSSEDown(flagset)
		}

		delay := bo.durationAtLeast(retryAfterOf(err))
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}

// retryAfterOf returns a 429's Retry-After hint from err, or 0.
func retryAfterOf(err error) time.Duration {
	if he := httpErrorOf(err); he != nil {
		return he.RetryAfter
	}
	return 0
}

func (c *Client) consumeSSEStream(ctx context.Context, payload map[string]any, fallbackFlagset string) error {
	body, err := encodeJSON(payload)
	if err != nil {
		return err
	}

	// Idle watchdog: the stream is long-lived and carries no client-level timeout,
	// so if the server sends nothing (not even a ~20s keepalive) within
	// sseReadTimeout, cancel it. Otherwise a silently half-open connection would
	// hang forever instead of triggering a reconnect.
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	watchdog := time.AfterFunc(c.sseReadTimeout, cancel)
	defer watchdog.Stop()

	req, err := http.NewRequestWithContext(streamCtx, http.MethodPost, c.serverURL+"/api/v1/sse", body)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Goose-Client-Thumbprint", c.thumbprint)

	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		return c.streamError(ctx, streamCtx, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		text, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
		return &HTTPError{
			StatusCode:   resp.StatusCode,
			Message:      fmt.Sprintf("POST /api/v1/sse failed (%d)", resp.StatusCode),
			ResponseText: string(text),
			RetryAfter:   parseRetryAfter(resp.Header.Get("Retry-After")),
		}
	}

	// Connected: the client is receiving updates again.
	c.recordSuccess()

	reader := bufio.NewReader(resp.Body)
	var block []string
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return c.streamError(ctx, streamCtx, err)
		}
		watchdog.Reset(c.sseReadTimeout)
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			c.handleSSEBlock(block, fallbackFlagset)
			block = block[:0]
			continue
		}
		block = append(block, line)
	}
}

// streamError classifies why an SSE read or dial ended. A parent-context
// cancellation (Close) is a clean shutdown and returns nil. An idle-watchdog
// cancellation or a server-side close (EOF) is a disconnect: it returns a
// non-nil error so the loop backs off and reconnects.
func (c *Client) streamError(parent, stream context.Context, err error) error {
	if parent.Err() != nil {
		return nil
	}
	if stream.Err() != nil {
		return newError("sse idle timeout after %s", c.sseReadTimeout)
	}
	if errors.Is(err, io.EOF) {
		return newError("sse stream closed by server")
	}
	return err
}

func (c *Client) handleSSEBlock(block []string, fallbackFlagset string) {
	if len(block) == 0 {
		return
	}
	var dataLines []string
	for _, line := range block {
		if strings.HasPrefix(line, "data:") {
			dataLines = append(dataLines, strings.TrimSpace(line[5:]))
		}
	}
	if len(dataLines) == 0 {
		return
	}
	payload, err := decodeJSONObject(strings.Join(dataLines, "\n"))
	if err != nil {
		c.log.Warn("skipping invalid sse payload")
		return
	}
	c.applyDelta(payload, fallbackFlagset)
}

func (c *Client) applyDelta(payload map[string]any, fallbackFlagset string) {
	eventID := strings.TrimSpace(firstString(payload, "eventId", "event_id"))
	if eventID != "" && c.seenEventIDs.seen(eventID) {
		return
	}

	flagset := strings.TrimSpace(firstString(payload, "flagSet", "flag_set"))
	if flagset == "" {
		flagset = fallbackFlagset
	}
	flagKey := strings.TrimSpace(firstString(payload, "flagKey", "flag_key"))
	if flagset == "" || flagKey == "" {
		return
	}

	c.mu.RLock()
	_, known := c.flags[flagset]
	dataType := c.flagDataTypes[flagset][flagKey]
	c.mu.RUnlock()
	if !known {
		return
	}

	// A delta with no percentage means canary was disabled, so this clears it.
	targeting, hasTargeting := parseSegmentTargeting(payload)
	c.commitFlag(flagset, flagKey, dataType, firstValue(payload, "flagValue", "flag_value"), extractRollout(payload), targeting, hasTargeting)
	c.recordSuccess()
}

func (c *Client) setFlag(flagset, flagKey string, value FlagValue) {
	c.mu.Lock()
	values, ok := c.flags[flagset]
	if !ok {
		c.mu.Unlock()
		return
	}
	previous, had := values[flagKey]
	values[flagKey] = value
	listeners := make([]FlagChangeListener, 0, len(c.changeListener))
	changed := !had || !valuesEqual(previous, value)
	if changed {
		for _, l := range c.changeListener {
			listeners = append(listeners, l)
		}
	}
	c.mu.Unlock()

	c.persist(flagset, flagKey, value)
	c.markDirty()

	if !changed {
		return
	}
	var prev FlagValue
	if had {
		prev = previous
	}
	change := FlagChange{Flagset: flagset, FlagKey: flagKey, Value: value, PreviousValue: prev}
	for _, l := range listeners {
		c.safeNotify(l, change)
	}
}

func (c *Client) safeNotify(listener FlagChangeListener, change FlagChange) {
	defer func() {
		if r := recover(); r != nil {
			c.log.Warn("onChange listener panicked", "err", r)
		}
	}()
	listener(change)
}

func (c *Client) persist(flagset, flagKey string, value FlagValue) {
	if c.storageAdapter == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			c.log.Warn("storage adapter panicked", "err", r)
		}
	}()
	c.storageAdapter.CreateOrUpdate(flagset, flagKey, value)
}

func extractFlags(payload map[string]any) []map[string]any {
	raw, ok := payload["flags"].([]any)
	if !ok {
		return nil
	}
	out := make([]map[string]any, 0, len(raw))
	for _, item := range raw {
		if m, ok := item.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out
}

func extractRollout(raw map[string]any) rolloutConfig {
	rawPercentage := firstValue(raw, "rollout_percentage", "rolloutPercentage")
	salt := firstString(raw, "rollout_salt", "rolloutSalt")
	value := firstValue(raw, "rollout_value", "rolloutValue")

	var percentage *int
	if f, ok := toFloat(rawPercentage); ok {
		p := int(f)
		percentage = &p
	}
	return rolloutConfig{percentage: percentage, salt: salt, value: value}
}

// coerceValue coerces a raw stored value to the type implied by the flag's data
// type. Numbers become float64; bools become bool; everything else is returned
// unchanged.
func coerceValue(value any, dataType string) FlagValue {
	switch strings.ToLower(strings.TrimSpace(dataType)) {
	case "bool":
		switch v := value.(type) {
		case bool:
			return v
		case string:
			return strings.EqualFold(strings.TrimSpace(v), "true")
		}
	case "number", "int", "integer", "float", "double":
		if f, ok := toFloat(value); ok {
			return f
		}
	}
	return value
}

func toFloat(value any) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case bool:
		return 0, false
	case string:
		text := strings.TrimSpace(v)
		if text == "" {
			return 0, false
		}
		f, err := strconv.ParseFloat(text, 64)
		if err != nil {
			return 0, false
		}
		return f, true
	}
	return 0, false
}

func maxUpdatedTimestamp(flags []map[string]any) int64 {
	var max int64
	for _, raw := range flags {
		if ts := toUnixSeconds(asString(raw["updated_at"])); ts > max {
			max = ts
		}
	}
	return max
}
