package goose

import (
	"reflect"
	"testing"
)

// parseAppVersion must agree with the Python/JS SDKs and with the two Go
// services, so the same metadata gates identically everywhere.
func TestParseAppVersionCrossLanguage(t *testing.T) {
	cases := []struct {
		raw  string
		want []int
		ok   bool
	}{
		{"2.4.1", []int{2, 4, 1}, true},
		{"v2.4.1", []int{2, 4, 1}, true},
		{"2.4", []int{2, 4}, true},
		{"3", []int{3}, true},
		{"2.4.1-rc.1", []int{2, 4, 1}, true},
		{"2.4.1+build.7", []int{2, 4, 1}, true},
		{"1.2.3.4", []int{1, 2, 3, 4}, true},
		{"", nil, false},
		{"latest", nil, false},
		{"2.x", nil, false},
		{"-1.0", nil, false},
	}
	for _, tc := range cases {
		got, ok := parseAppVersion(tc.raw)
		if ok != tc.ok || (ok && !reflect.DeepEqual(got, tc.want)) {
			t.Errorf("parseAppVersion(%q) = %v,%v; want %v,%v", tc.raw, got, ok, tc.want, tc.ok)
		}
	}
}

func TestCompareAppVersionsPadsShorter(t *testing.T) {
	cases := []struct {
		a, b []int
		want int
	}{
		{[]int{2, 4}, []int{2, 4, 0}, 0},
		{[]int{2, 4, 1}, []int{2, 4}, 1},
		{[]int{2, 3, 9}, []int{2, 4}, -1},
		{[]int{10}, []int{9, 9, 9}, 1},
	}
	for _, tc := range cases {
		if got := compareAppVersions(tc.a, tc.b); got != tc.want {
			t.Errorf("compareAppVersions(%v,%v) = %d, want %d", tc.a, tc.b, got, tc.want)
		}
	}
}

func TestConfigEntryAppliesBoundsAreInclusive(t *testing.T) {
	entry := map[string]any{"min_app_version": "2.4.0", "max_app_version": "3.0.0"}
	cases := []struct {
		version string
		want    bool
	}{
		{"2.3.9", false},
		{"2.4.0", true}, // lower bound is inclusive
		{"2.7.1", true},
		{"3.0.0", true}, // upper bound is inclusive
		{"3.0.1", false},
	}
	for _, tc := range cases {
		parsed, _ := parseAppVersion(tc.version)
		if got := configEntryApplies(entry, parsed); got != tc.want {
			t.Errorf("configEntryApplies(%s) = %v, want %v", tc.version, got, tc.want)
		}
	}
}

// Every unknown must fail open, so bad metadata degrades to the ungated
// behaviour instead of silently hiding a value.
func TestConfigEntryAppliesFailsOpen(t *testing.T) {
	v240, _ := parseAppVersion("2.4.0")
	if !configEntryApplies(map[string]any{"min_app_version": "9.0.0"}, nil) {
		t.Error("a client with no app version must see every entry")
	}
	if !configEntryApplies(map[string]any{}, v240) {
		t.Error("an entry with no bounds must always apply")
	}
	if !configEntryApplies(map[string]any{"min_app_version": "not-a-version"}, v240) {
		t.Error("an unparseable bound must not gate the entry out")
	}
	if !configEntryApplies(map[string]any{"min_app_version": nil}, v240) {
		t.Error("a null bound must not gate the entry out")
	}
}

func TestResolveConfigEntryFallsBackToDefault(t *testing.T) {
	entry := map[string]any{"value": "grid-v2", "default": "grid", "min_app_version": "2.4.0"}

	current, _ := parseAppVersion("2.6.0")
	if got, ok := resolveConfigEntry(entry, current); !ok || got != "grid-v2" {
		t.Errorf("in-range entry resolved to %v,%v; want grid-v2,true", got, ok)
	}

	old, _ := parseAppVersion("2.1.0")
	if got, ok := resolveConfigEntry(entry, old); !ok || got != "grid" {
		t.Errorf("gated-out entry resolved to %v,%v; want grid,true", got, ok)
	}

	// Gated out with no default: the caller's own fallback wins.
	if _, ok := resolveConfigEntry(map[string]any{"value": "x", "max_app_version": "1.0.0"}, old); ok {
		t.Error("gated-out entry with no default must report not-found")
	}
}

// A requires_restart change to an entry this build never reads must not restart
// the process.
func TestChangedConfigEntriesSkipsGatedOutEntries(t *testing.T) {
	oldDoc := map[string]any{"configs": map[string]any{
		"future": map[string]any{"value": "a", "apply_strategy": "requires_restart", "min_app_version": "9.0.0"},
		"here":   map[string]any{"value": "a", "apply_strategy": "immediate"},
	}}
	newDoc := map[string]any{"configs": map[string]any{
		"future": map[string]any{"value": "b", "apply_strategy": "requires_restart", "min_app_version": "9.0.0"},
		"here":   map[string]any{"value": "b", "apply_strategy": "immediate"},
	}}

	v240, _ := parseAppVersion("2.4.0")
	got := changedConfigEntries(oldDoc, newDoc, v240)
	if !reflect.DeepEqual(got, []string{"immediate"}) {
		t.Errorf("gated client saw %v; want only the immediate change", got)
	}

	// Without an app version the pre-gating behaviour is preserved: both change.
	if ungated := changedConfigEntries(oldDoc, newDoc, nil); len(ungated) != 2 {
		t.Errorf("ungated client saw %v; want both entries", ungated)
	}
}

func TestUnsatisfiedRequiredEntries(t *testing.T) {
	v240, _ := parseAppVersion("2.4.0")
	doc := map[string]any{"configs": map[string]any{
		"ok":            map[string]any{"value": "x", "required": true},
		"gated_default": map[string]any{"value": "x", "default": "y", "required": true, "min_app_version": "9.0.0"},
		"gated_bare":    map[string]any{"value": "x", "required": true, "min_app_version": "9.0.0"},
		"explicit_null": map[string]any{"value": nil, "required": true},
		"not_required":  map[string]any{"min_app_version": "9.0.0"},
	}}
	want := []string{"explicit_null", "gated_bare"}
	if got := unsatisfiedRequiredEntries(doc, v240); !reflect.DeepEqual(got, want) {
		t.Errorf("unsatisfiedRequiredEntries = %v, want %v", got, want)
	}
}

// Config values arrive with ${secret} references already resolved, so a
// sensitive entry must never reach the on-disk cache.
func TestRedactSensitiveConfigEntries(t *testing.T) {
	doc := map[string]any{"configs": map[string]any{
		"db_url": map[string]any{"value": "postgres://app:hunter2@db", "default": "postgres://localhost", "sensitive": true, "apply_strategy": "requires_restart"},
		"layout": map[string]any{"value": "grid", "apply_strategy": "immediate"},
	}}

	redacted := redactSensitiveConfigEntries(doc)
	entries := redacted["configs"].(map[string]any)

	secret := entries["db_url"].(map[string]any)
	if _, ok := secret["value"]; ok {
		t.Error("sensitive value was persisted")
	}
	if _, ok := secret["default"]; ok {
		t.Error("sensitive default was persisted")
	}
	if secret["apply_strategy"] != "requires_restart" {
		t.Error("non-value metadata should survive redaction")
	}
	if entries["layout"].(map[string]any)["value"] != "grid" {
		t.Error("non-sensitive entry must be untouched")
	}

	// The live document must not be mutated by building a snapshot of it.
	live := doc["configs"].(map[string]any)["db_url"].(map[string]any)
	if live["value"] != "postgres://app:hunter2@db" {
		t.Error("redaction mutated the in-memory document")
	}

	// A document with nothing sensitive is returned as-is.
	plain := map[string]any{"configs": map[string]any{"a": map[string]any{"value": 1}}}
	if got := redactSensitiveConfigEntries(plain); !reflect.DeepEqual(got, plain) {
		t.Error("a document with no sensitive entries should be unchanged")
	}
}

// End-to-end through the public read path: a client built with an AppVersion
// serves the default for an entry its version is gated out of.
func TestGetConfigValueAppliesAppVersionGating(t *testing.T) {
	no := false
	client, err := New(Options{
		ClientID:      "gsc_test",
		ClientSecret:  "secret",
		ServerURL:     "http://localhost:0",
		Flagsets:      []string{"main"},
		NamespaceName: "production",
		Configs:       []string{"frontend"},
		AppVersion:    "2.1.0",
		AutoConnect:   &no,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	client.configs["frontend"] = watchedConfig{document: map[string]any{"configs": map[string]any{
		"layout":     map[string]any{"value": "grid-v2", "default": "grid", "min_app_version": "2.4.0"},
		"no_default": map[string]any{"value": "on", "min_app_version": "2.4.0"},
		"ungated":    map[string]any{"value": "always"},
	}}, revision: 1}

	if got := client.GetConfigValue("frontend", "layout", "caller-fallback"); got != "grid" {
		t.Errorf("gated entry returned %v; want the entry default \"grid\"", got)
	}
	if got := client.GetConfigValue("frontend", "no_default", "caller-fallback"); got != "caller-fallback" {
		t.Errorf("gated entry with no default returned %v; want the caller fallback", got)
	}
	if got := client.GetConfigValue("frontend", "ungated", nil); got != "always" {
		t.Errorf("ungated entry returned %v; want \"always\"", got)
	}
}

// A client with no AppVersion must behave exactly as it did before gating existed.
func TestGetConfigValueWithoutAppVersionIgnoresBounds(t *testing.T) {
	no := false
	client, err := New(Options{
		ClientID:      "gsc_test",
		ClientSecret:  "secret",
		ServerURL:     "http://localhost:0",
		Flagsets:      []string{"main"},
		NamespaceName: "production",
		Configs:       []string{"frontend"},
		AutoConnect:   &no,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	client.configs["frontend"] = watchedConfig{document: map[string]any{"configs": map[string]any{
		"layout": map[string]any{"value": "grid-v2", "default": "grid", "min_app_version": "9.9.9"},
	}}, revision: 1}

	if got := client.GetConfigValue("frontend", "layout", nil); got != "grid-v2" {
		t.Errorf("ungated client returned %v; want the live value \"grid-v2\"", got)
	}
}

// An unparseable AppVersion must disable gating rather than gate everything out.
func TestUnparseableAppVersionDisablesGating(t *testing.T) {
	no := false
	client, err := New(Options{
		ClientID:      "gsc_test",
		ClientSecret:  "secret",
		ServerURL:     "http://localhost:0",
		Flagsets:      []string{"main"},
		NamespaceName: "production",
		Configs:       []string{"frontend"},
		AppVersion:    "nightly",
		AutoConnect:   &no,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if client.appVersion != nil {
		t.Fatalf("expected gating to be disabled, got %v", client.appVersion)
	}
	client.configs["frontend"] = watchedConfig{document: map[string]any{"configs": map[string]any{
		"layout": map[string]any{"value": "grid-v2", "default": "grid", "min_app_version": "9.9.9"},
	}}, revision: 1}
	if got := client.GetConfigValue("frontend", "layout", nil); got != "grid-v2" {
		t.Errorf("got %v; want the live value", got)
	}
}
