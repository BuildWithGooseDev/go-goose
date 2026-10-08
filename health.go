package goose

import (
	"context"
	"time"
)

// State returns the client's current connection state.
func (c *Client) State() ConnectionState {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state
}

// LastSync returns the time of the last successful poll or applied SSE delta.
// The zero time means no successful sync has happened yet.
func (c *Client) LastSync() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lastSyncAt
}

// IsStale reports whether the served values may be out of date because no
// successful sync has happened within the staleness window
// (max(3×PollInterval, SSEReadTimeout)). A client that has never synced is stale.
func (c *Client) IsStale() bool {
	c.mu.RLock()
	last := c.lastSyncAt
	c.mu.RUnlock()
	if last.IsZero() {
		return true
	}
	return time.Since(last) > c.stalenessWindow()
}

func (c *Client) stalenessWindow() time.Duration {
	return max(3*c.pollInterval, c.sseReadTimeout)
}

// OnStateChange subscribes to connection-state transitions and returns an
// unsubscribe function.
func (c *Client) OnStateChange(listener StateChangeListener) func() {
	c.mu.Lock()
	id := c.nextStateListenerID
	c.nextStateListenerID++
	c.stateListeners[id] = listener
	c.mu.Unlock()
	return func() {
		c.mu.Lock()
		delete(c.stateListeners, id)
		c.mu.Unlock()
	}
}

// OnError subscribes to background errors (see ErrorListener) and returns an
// unsubscribe function.
func (c *Client) OnError(listener ErrorListener) func() {
	c.mu.Lock()
	id := c.nextErrorListenerID
	c.nextErrorListenerID++
	c.errorListeners[id] = listener
	c.mu.Unlock()
	return func() {
		c.mu.Lock()
		delete(c.errorListeners, id)
		c.mu.Unlock()
	}
}

// recordSuccess marks a successful sync, clears the failure count, and moves the
// client to StateLive (unless it has been Closed).
func (c *Client) recordSuccess() {
	c.mu.Lock()
	c.lastSyncAt = time.Now()
	c.consecutiveFailures = 0
	listeners, changed, newState := c.setStateLocked(StateLive)
	c.mu.Unlock()
	if changed {
		notifyState(listeners, newState)
	}
}

// recordFailure counts a failed sync, moves the client to StateDegraded (unless
// Closed), and reports the error to OnError listeners.
func (c *Client) recordFailure(err error) {
	c.mu.Lock()
	c.consecutiveFailures++
	listeners, changed, newState := c.setStateLocked(StateDegraded)
	c.mu.Unlock()
	if changed {
		notifyState(listeners, newState)
	}
	c.emitError(err)
}

// setStateLocked transitions to next unless the client is Closed. The caller
// must hold mu. It returns the listener snapshot plus whether the state changed.
func (c *Client) setStateLocked(next ConnectionState) (listeners []StateChangeListener, changed bool, state ConnectionState) {
	if c.state == StateClosed {
		return nil, false, c.state
	}
	prev := c.state
	c.state = next
	return c.snapshotStateListeners(), prev != next, next
}

// snapshotStateListeners copies the current state listeners. Caller holds mu.
func (c *Client) snapshotStateListeners() []StateChangeListener {
	out := make([]StateChangeListener, 0, len(c.stateListeners))
	for _, l := range c.stateListeners {
		out = append(out, l)
	}
	return out
}

func notifyState(listeners []StateChangeListener, state ConnectionState) {
	for _, l := range listeners {
		safeNotifyState(l, state)
	}
}

func safeNotifyState(listener StateChangeListener, state ConnectionState) {
	defer func() { _ = recover() }()
	listener(state)
}

// emitError reports err to every OnError listener, recovering from panics.
func (c *Client) emitError(err error) {
	if err == nil {
		return
	}
	c.mu.RLock()
	listeners := make([]ErrorListener, 0, len(c.errorListeners))
	for _, l := range c.errorListeners {
		listeners = append(listeners, l)
	}
	c.mu.RUnlock()
	for _, l := range listeners {
		c.safeErrorNotify(l, err)
	}
}

func (c *Client) safeErrorNotify(listener ErrorListener, err error) {
	defer func() {
		if r := recover(); r != nil {
			c.log.Warn("onError listener panicked", "err", r)
		}
	}()
	listener(err)
}

// --- persistent cache (warm start + debounced save) ---

// loadFromCache hydrates in-memory state from the persistent cache. Only
// configured flagsets are loaded. Failures are logged and reported, never fatal.
func (c *Client) loadFromCache(ctx context.Context) {
	if c.cache == nil {
		return
	}
	snap, err := c.cache.Load(ctx)
	if err != nil {
		c.log.Warn("cache load failed", "err", err)
		c.emitError(err)
		return
	}
	if snap == nil {
		return
	}

	c.mu.Lock()
	for flagset := range c.flags {
		for k, v := range snap.Flags[flagset] {
			c.flags[flagset][k] = v
		}
		for k, dt := range snap.FlagDataTypes[flagset] {
			c.flagDataTypes[flagset][k] = dt
		}
		for k, r := range snap.Rollouts[flagset] {
			c.rollouts[flagset][k] = rolloutConfig{percentage: r.Percentage, salt: r.Salt, value: r.Value}
		}
		if cursor, ok := snap.PollCursors[flagset]; ok && cursor > c.pollCursors[flagset] {
			c.pollCursors[flagset] = cursor
		}
	}
	for _, name := range c.configNames {
		if entry, ok := snap.Configs[name]; ok {
			c.configs[name] = watchedConfig{document: entry.Document, revision: entry.Revision}
		}
	}
	c.mu.Unlock()
	c.log.Info("warm-started from persistent cache")
}

// markDirty signals the flusher that persisted state changed. Non-blocking.
func (c *Client) markDirty() {
	if c.cache == nil {
		return
	}
	select {
	case c.cacheDirty <- struct{}{}:
	default:
	}
}

// cacheFlushLoop debounces dirty signals and writes at most one snapshot per
// second, plus a final flush on shutdown, to avoid write-amplifying on delta storms.
func (c *Client) cacheFlushLoop(ctx context.Context) {
	defer c.wg.Done()
	const debounce = time.Second
	timer := time.NewTimer(debounce)
	if !timer.Stop() {
		<-timer.C
	}
	dirty := false
	for {
		select {
		case <-ctx.Done():
			if dirty {
				c.flushCache(context.Background())
			}
			return
		case <-c.cacheDirty:
			if !dirty {
				dirty = true
				timer.Reset(debounce)
			}
		case <-timer.C:
			if dirty {
				c.flushCache(context.Background())
				dirty = false
			}
		}
	}
}

func (c *Client) flushCache(ctx context.Context) {
	if c.cache == nil {
		return
	}
	snap := c.buildSnapshot()
	if err := c.cache.Save(ctx, snap); err != nil {
		c.log.Warn("cache save failed", "err", err)
		c.emitError(err)
	}
}

// buildSnapshot captures current state into a serializable Snapshot.
func (c *Client) buildSnapshot() *Snapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	snap := &Snapshot{
		Flags:         make(map[string]map[string]FlagValue, len(c.flags)),
		FlagDataTypes: make(map[string]map[string]string, len(c.flagDataTypes)),
		Rollouts:      make(map[string]map[string]RolloutSnapshot, len(c.rollouts)),
		PollCursors:   make(map[string]int64, len(c.pollCursors)),
		Configs:       make(map[string]ConfigSnapshot, len(c.configs)),
	}
	for flagset, values := range c.flags {
		inner := make(map[string]FlagValue, len(values))
		for k, v := range values {
			inner[k] = v
		}
		snap.Flags[flagset] = inner
	}
	for flagset, types := range c.flagDataTypes {
		inner := make(map[string]string, len(types))
		for k, v := range types {
			inner[k] = v
		}
		snap.FlagDataTypes[flagset] = inner
	}
	for flagset, rollouts := range c.rollouts {
		inner := make(map[string]RolloutSnapshot, len(rollouts))
		for k, r := range rollouts {
			inner[k] = RolloutSnapshot{Percentage: r.percentage, Salt: r.salt, Value: r.value}
		}
		snap.Rollouts[flagset] = inner
	}
	for flagset, cursor := range c.pollCursors {
		snap.PollCursors[flagset] = cursor
	}
	for name, entry := range c.configs {
		snap.Configs[name] = ConfigSnapshot{
			Document: redactSensitiveConfigEntries(entry.document),
			Revision: entry.revision,
		}
	}
	return snap
}

// redactSensitiveConfigEntries returns a copy of doc with the "value" and
// "default" of every entry marked sensitive dropped. Config values arrive with
// ${secret} references already resolved server-side, so persisting a document
// verbatim would write plaintext secrets to the cache file; marking an entry
// sensitive keeps it in memory only. Such an entry simply reads as absent after
// a warm start, until the first successful fetch repopulates it.
//
// The copy is shallow except along the path it rewrites: untouched entries are
// shared with the live document, which is safe because the SDK replaces
// documents wholesale rather than mutating them in place.
func redactSensitiveConfigEntries(doc map[string]any) map[string]any {
	entries, ok := doc["configs"].(map[string]any)
	if !ok {
		return doc
	}

	var redactedEntries map[string]any
	for key, rawEntry := range entries {
		entry, ok := rawEntry.(map[string]any)
		if !ok {
			continue
		}
		if sensitive, _ := entry["sensitive"].(bool); !sensitive {
			continue
		}
		if redactedEntries == nil {
			redactedEntries = make(map[string]any, len(entries))
			for k, v := range entries {
				redactedEntries[k] = v
			}
		}
		stripped := make(map[string]any, len(entry))
		for k, v := range entry {
			if k == "value" || k == "default" {
				continue
			}
			stripped[k] = v
		}
		redactedEntries[key] = stripped
	}
	if redactedEntries == nil {
		return doc
	}

	redactedDoc := make(map[string]any, len(doc))
	for k, v := range doc {
		redactedDoc[k] = v
	}
	redactedDoc["configs"] = redactedEntries
	return redactedDoc
}

// --- SSE→polling fallback ---

// markSSEDown records that a flagset's SSE stream is down and, in SSE-only mode,
// starts a temporary polling loop so flags keep refreshing while SSE recovers.
func (c *Client) markSSEDown(flagset string) {
	if c.connectionTypes[Polling] {
		return // steady polling already keeps flags fresh
	}
	c.mu.Lock()
	c.sseDownFlagsets[flagset] = true
	if c.fallbackCancel == nil && c.rootCtx != nil {
		fctx, fcancel := context.WithCancel(c.rootCtx)
		c.fallbackCancel = fcancel
		c.wg.Add(1)
		go c.fallbackPollingLoop(fctx)
		c.log.Warn("sse unavailable; starting fallback polling", "flagset", flagset)
	}
	c.mu.Unlock()
}

// markSSEUp clears a flagset's down marker and stops fallback polling once every
// SSE stream is healthy again.
func (c *Client) markSSEUp(flagset string) {
	c.mu.Lock()
	delete(c.sseDownFlagsets, flagset)
	if len(c.sseDownFlagsets) == 0 && c.fallbackCancel != nil {
		c.fallbackCancel()
		c.fallbackCancel = nil
		c.log.Info("sse recovered; stopping fallback polling")
	}
	c.mu.Unlock()
}

// fallbackPollingLoop polls every flagset (and any watched configs) on the poll
// interval until its context is cancelled.
func (c *Client) fallbackPollingLoop(ctx context.Context) {
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
			if err := c.pollOnce(ctx, flagset); err != nil && ctx.Err() == nil {
				c.recordFailure(err)
			} else if err == nil {
				c.recordSuccess()
			}
		}
		if len(c.configNames) > 0 {
			if err := c.pollConfigsOnce(ctx); err != nil && ctx.Err() == nil {
				c.recordFailure(err)
			}
		}
	}
}
