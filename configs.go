package goose

import (
	"context"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"syscall"
)

// GetConfig returns a watched config's current document, or def if unset.
// A config is a named JSON document shaped {"configs": {"<entry>": {"value":
// ..., "apply_strategy": ...}}}. The whole document is returned. Reading an
// unwatched name logs a warning and returns def.
func (c *Client) GetConfig(name string, def map[string]any) map[string]any {
	if !c.isWatchedConfig(name) {
		c.log.Warn("GetConfig called for an unwatched config name", "name", name)
		return def
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry, ok := c.configs[name]
	if !ok {
		return def
	}
	return entry.document
}

// GetConfigValue returns a single entry's value from within a watched config
// document (e.g. GetConfigValue("frontend", "dashboard_layout", nil)). Returns
// def when the config is not watched, not yet loaded, the key is absent, or the
// document is malformed.
//
// This is where per-entry metadata is applied. When the client was built with
// an AppVersion and the entry's min/max_app_version range excludes it, the
// entry's "default" is returned instead of its "value" (and def when it has no
// default). A "deprecated" entry logs a warning the first time it is read.
// Reading an entry straight out of GetConfig's document bypasses all of this.
func (c *Client) GetConfigValue(name, key string, def any) any {
	document := c.GetConfig(name, nil)
	if document == nil {
		return def
	}
	entries, ok := document["configs"].(map[string]any)
	if !ok {
		return def
	}
	entry, ok := entries[key].(map[string]any)
	if !ok {
		return def
	}
	c.warnDeprecatedOnce(name, key, entry)
	value, ok := resolveConfigEntry(entry, c.appVersion)
	if !ok {
		return def
	}
	return value
}

// warnDeprecatedOnce logs the first read of an entry marked deprecated, naming
// its replaced_by successor when one is set. Subsequent reads stay quiet so a
// hot path does not flood the log.
func (c *Client) warnDeprecatedOnce(name, key string, entry map[string]any) {
	deprecated, _ := entry["deprecated"].(bool)
	if !deprecated {
		return
	}
	cacheKey := name + "\x00" + key
	c.mu.Lock()
	warned := c.warnedDeprecated[cacheKey]
	c.warnedDeprecated[cacheKey] = true
	c.mu.Unlock()
	if warned {
		return
	}
	if replacedBy := strings.TrimSpace(asString(entry["replaced_by"])); replacedBy != "" {
		c.log.Warn("config entry is deprecated", "config", name, "entry", key, "replaced_by", replacedBy)
		return
	}
	c.log.Warn("config entry is deprecated", "config", name, "entry", key)
}

// ConfigsSnapshot returns a {name: document} copy of the watched configs.
func (c *Client) ConfigsSnapshot() map[string]map[string]any {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]map[string]any, len(c.configs))
	for name, entry := range c.configs {
		out[name] = entry.document
	}
	return out
}

// On registers a callback fired when the watched config name changes.
func (c *Client) On(name string, callback ConfigChangeListener) {
	c.mu.Lock()
	c.configListener[name] = append(c.configListener[name], callback)
	c.mu.Unlock()
}

// OnRestartRequired registers a drain callback run when a requires_restart
// config changes (before any opt-in SIGINT).
func (c *Client) OnRestartRequired(callback ConfigChangeListener) {
	c.mu.Lock()
	c.restartHooks = append(c.restartHooks, callback)
	c.mu.Unlock()
}

func (c *Client) isWatchedConfig(name string) bool {
	for _, n := range c.configNames {
		if n == name {
			return true
		}
	}
	return false
}

func (c *Client) fetchConfigsSnapshot(ctx context.Context) error {
	query := url.Values{}
	query.Set("client_id", c.clientID)
	query.Set("client_secret", c.clientSecret)
	query.Set("namespace_name", c.namespaceName)
	for _, name := range c.configNames {
		query.Add("names", name)
	}
	text, err := c.request(ctx, http.MethodGet, "/api/v1/configs?"+query.Encode(), nil, nil)
	if err != nil {
		return err
	}
	payload, _ := decodeJSONObject(text)
	c.applyConfigs(extractConfigEntries(payload), true)
	return nil
}

func (c *Client) pollConfigsOnce(ctx context.Context) error {
	body := map[string]any{
		"client_id":      c.clientID,
		"client_secret":  c.clientSecret,
		"namespace_name": c.namespaceName,
		"names":          c.configNames,
	}
	payload, err := c.requestJSON(ctx, http.MethodPost, "/api/v1/configs/poll", nil, body)
	if err != nil {
		return err
	}
	c.applyConfigs(extractConfigEntries(payload), false)
	return nil
}

func extractConfigEntries(payload map[string]any) map[string]any {
	entries, ok := payload["configs"].(map[string]any)
	if !ok {
		return map[string]any{}
	}
	return entries
}

// applyConfigs applies a batch of named config documents. For each watched name
// present, the incoming document is diffed against the cached one; a difference
// (or a newly seen name) produces a ConfigChangeEvent whose apply_strategy is
// requires_restart when any changed inner entry requires a restart. On initial
// the cache is seeded without dispatching listeners.
func (c *Client) applyConfigs(entries map[string]any, initial bool) {
	var changes []ConfigChangeEvent
	var unsatisfied []error
	batchRequiresRestart := false

	c.mu.Lock()
	for _, name := range c.configNames {
		incoming, ok := entries[name].(map[string]any)
		if !ok {
			continue
		}
		newDoc, ok := incoming["document"].(map[string]any)
		if !ok {
			continue
		}
		revision := asInt(incoming["revision_number"])

		current, exists := c.configs[name]
		var oldDoc map[string]any
		if exists {
			oldDoc = current.document
		}

		if !exists || !reflect.DeepEqual(oldDoc, newDoc) {
			// Reported on first sight and on each change, not on every poll, so an
			// unsatisfied entry does not flood OnError once per interval.
			if missing := unsatisfiedRequiredEntries(newDoc, c.appVersion); len(missing) > 0 {
				unsatisfied = append(unsatisfied, newError(
					"config %q: required entries resolve to nothing for app version %q: %s",
					name, c.appVersionRaw, strings.Join(missing, ", ")))
			}

			requiresRestart := false
			for _, strategy := range changedConfigEntries(oldDoc, newDoc, c.appVersion) {
				if strategy == "requires_restart" {
					requiresRestart = true
					break
				}
			}
			if requiresRestart {
				batchRequiresRestart = true
			}
			applyStrategy := "immediate"
			if requiresRestart {
				applyStrategy = "requires_restart"
			}
			changes = append(changes, ConfigChangeEvent{
				Name:          name,
				OldValue:      oldDoc,
				NewValue:      newDoc,
				ApplyStrategy: applyStrategy,
			})
		}
		c.configs[name] = watchedConfig{document: newDoc, revision: revision}
	}
	c.mu.Unlock()

	for _, err := range unsatisfied {
		c.log.Error("required config entry is unsatisfied", "err", err)
		c.emitError(err)
	}

	if len(changes) > 0 {
		c.markDirty()
	}

	if initial {
		return
	}

	c.mu.Lock()

	listeners := map[string][]ConfigChangeListener{}
	for _, change := range changes {
		listeners[change.Name] = append([]ConfigChangeListener(nil), c.configListener[change.Name]...)
	}
	restartListeners := append([]ConfigChangeListener(nil), c.restartHooks...)
	c.mu.Unlock()

	for _, change := range changes {
		for _, callback := range listeners[change.Name] {
			c.safeConfigNotify(callback, change)
		}
	}

	if !batchRequiresRestart {
		return
	}
	var restartChange ConfigChangeEvent
	for _, change := range changes {
		if change.ApplyStrategy == "requires_restart" {
			restartChange = change
			break
		}
	}
	for _, callback := range restartListeners {
		c.safeConfigNotify(callback, restartChange)
	}
	if c.restartOnRequiredChange {
		c.log.Info("requires_restart config changed; raising SIGINT for orchestrator restart")
		_ = syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	}
}

func (c *Client) safeConfigNotify(callback ConfigChangeListener, change ConfigChangeEvent) {
	defer func() {
		if r := recover(); r != nil {
			c.log.Warn("config listener panicked", "name", change.Name, "err", r)
		}
	}()
	callback(change)
}

// changedConfigEntries returns the apply_strategy of every inner entry in newDoc
// whose value or apply_strategy differs from oldDoc (or is newly added). A nil
// oldDoc treats every inner entry as changed.
//
// Entries gated out by appVersion are skipped: a change to an entry this build
// never reads must not drag the process through a requires_restart. With a nil
// appVersion every entry applies, which is the pre-AppVersion behaviour.
func changedConfigEntries(oldDoc, newDoc map[string]any, appVersion []int) []string {
	newEntries, ok := newDoc["configs"].(map[string]any)
	if !ok {
		return nil
	}
	oldEntries, _ := oldDoc["configs"].(map[string]any)

	var changed []string
	for key, rawNew := range newEntries {
		newEntry, _ := rawNew.(map[string]any)
		if !configEntryApplies(newEntry, appVersion) {
			continue
		}
		strategy := strings.TrimSpace(asString(newEntry["apply_strategy"]))
		oldEntry, ok := oldEntries[key].(map[string]any)
		if !ok ||
			!reflect.DeepEqual(oldEntry["value"], newEntry["value"]) ||
			strings.TrimSpace(asString(oldEntry["apply_strategy"])) != strategy {
			changed = append(changed, strategy)
		}
	}
	return changed
}

// parseAppVersion parses a lenient dotted version ("3", "2.4", "2.4.1",
// "v2.4.1-rc.1") into its numeric components. A leading "v" is dropped and any
// pre-release or build suffix is ignored: app-version gating is coarse by
// design. Reports false when a component is not a number. Mirrors
// parseAppVersion in core_service/configs.go.
func parseAppVersion(raw string) ([]int, bool) {
	trimmed := strings.TrimPrefix(strings.TrimSpace(raw), "v")
	if idx := strings.IndexAny(trimmed, "-+"); idx >= 0 {
		trimmed = trimmed[:idx]
	}
	trimmed = strings.TrimSpace(trimmed)
	if trimmed == "" {
		return nil, false
	}
	parts := strings.Split(trimmed, ".")
	components := make([]int, 0, len(parts))
	for _, part := range parts {
		n, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || n < 0 {
			return nil, false
		}
		components = append(components, n)
	}
	return components, true
}

// compareAppVersions orders two parsed versions, padding the shorter one with
// zeroes so "2.4" and "2.4.0" compare equal.
func compareAppVersions(a, b []int) int {
	for i := 0; i < len(a) || i < len(b); i++ {
		var left, right int
		if i < len(a) {
			left = a[i]
		}
		if i < len(b) {
			right = b[i]
		}
		if left != right {
			if left < right {
				return -1
			}
			return 1
		}
	}
	return 0
}

// configEntryApplies reports whether an entry's min_app_version /
// max_app_version range (both inclusive) includes appVersion. Every unknown
// fails open — a nil appVersion, an absent bound, or an unparseable one leaves
// the entry applying — so bad metadata degrades to today's behaviour rather
// than silently hiding a value.
func configEntryApplies(entry map[string]any, appVersion []int) bool {
	if len(appVersion) == 0 {
		return true
	}
	if lower, ok := parseAppVersion(asString(entry["min_app_version"])); ok {
		if compareAppVersions(appVersion, lower) < 0 {
			return false
		}
	}
	if upper, ok := parseAppVersion(asString(entry["max_app_version"])); ok {
		if compareAppVersions(appVersion, upper) > 0 {
			return false
		}
	}
	return true
}

// resolveConfigEntry returns what an entry resolves to for appVersion: its
// "value" when the entry applies, otherwise its "default". Reports false when
// neither is present, leaving the caller to use its own fallback.
func resolveConfigEntry(entry map[string]any, appVersion []int) (any, bool) {
	if configEntryApplies(entry, appVersion) {
		value, ok := entry["value"]
		return value, ok
	}
	fallback, ok := entry["default"]
	return fallback, ok
}

// unsatisfiedRequiredEntries names the entries in doc marked required that
// resolve to nothing for appVersion — typically an entry gated out of this
// build's version range with no default to fall back on. Reported through
// OnError so the mistake surfaces at startup rather than at the first read.
func unsatisfiedRequiredEntries(doc map[string]any, appVersion []int) []string {
	entries, ok := doc["configs"].(map[string]any)
	if !ok {
		return nil
	}
	var unsatisfied []string
	for key, rawEntry := range entries {
		entry, ok := rawEntry.(map[string]any)
		if !ok {
			continue
		}
		if required, _ := entry["required"].(bool); !required {
			continue
		}
		if value, ok := resolveConfigEntry(entry, appVersion); !ok || value == nil {
			unsatisfied = append(unsatisfied, key)
		}
	}
	sort.Strings(unsatisfied)
	return unsatisfied
}

func asInt(value any) int {
	switch v := value.(type) {
	case float64:
		return int(v)
	case int:
		return v
	case string:
		if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
			return n
		}
	}
	return 0
}
