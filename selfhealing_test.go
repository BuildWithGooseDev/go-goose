package goose

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestBackoffBounds(t *testing.T) {
	b := newBackoff(time.Second, 10*time.Second)
	var maxSeen time.Duration
	for range 200 {
		d := b.duration()
		if d < time.Second || d > 10*time.Second {
			t.Fatalf("backoff out of bounds: %s", d)
		}
		if d > maxSeen {
			maxSeen = d
		}
	}
	// After many attempts the window must have climbed to the cap region.
	if maxSeen < 5*time.Second {
		t.Errorf("backoff never approached cap; max seen %s", maxSeen)
	}
	// durationAtLeast honors a floor.
	if d := b.durationAtLeast(30 * time.Second); d != 30*time.Second {
		t.Errorf("durationAtLeast floor not honored: %s", d)
	}
	// reset returns to base region.
	b.reset()
	if d := b.duration(); d > 2*time.Second {
		t.Errorf("after reset expected small delay, got %s", d)
	}
}

func TestParseRetryAfter(t *testing.T) {
	cases := []struct {
		in   string
		want time.Duration
	}{
		{"5", 5 * time.Second},
		{"0", 0},
		{"-3", 0},
		{"", 0},
		{"garbage", 0},
	}
	for _, tc := range cases {
		if got := parseRetryAfter(tc.in); got != tc.want {
			t.Errorf("parseRetryAfter(%q) = %s, want %s", tc.in, got, tc.want)
		}
	}
}

func TestCoerceAndValidate(t *testing.T) {
	if v, ok := coerceAndValidate("true", "bool"); !ok || v != true {
		t.Errorf("valid bool rejected: %v %v", v, ok)
	}
	if _, ok := coerceAndValidate("abc", "number"); ok {
		t.Error("non-numeric string accepted for number flag")
	}
	if v, ok := coerceAndValidate("5", "number"); !ok || v != float64(5) {
		t.Errorf("valid number rejected: %v %v", v, ok)
	}
	if _, ok := coerceAndValidate(nil, "bool"); ok {
		t.Error("nil value accepted")
	}
	if v, ok := coerceAndValidate("grid", "list_of_values"); !ok || v != "grid" {
		t.Errorf("valid string rejected: %v %v", v, ok)
	}
}

func TestFileCacheRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.json")
	fc := NewFileCache(path)

	if snap, err := fc.Load(context.Background()); err != nil || snap != nil {
		t.Fatalf("expected empty load, got %v %v", snap, err)
	}

	pct := 25
	want := &Snapshot{
		Flags:         map[string]map[string]FlagValue{"main": {"a": true, "b": float64(3), "c": "x"}},
		FlagDataTypes: map[string]map[string]string{"main": {"a": "bool", "b": "number", "c": "string"}},
		Rollouts:      map[string]map[string]RolloutSnapshot{"main": {"b": {Percentage: &pct, Salt: "s", Value: float64(9)}}},
		PollCursors:   map[string]int64{"main": 1234},
		Configs:       map[string]ConfigSnapshot{"frontend": {Document: map[string]any{"configs": map[string]any{}}, Revision: 2}},
	}
	if err := fc.Save(context.Background(), want); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err := fc.Load(context.Background())
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("round-trip mismatch:\n got %#v\nwant %#v", got, want)
	}
}

func TestStateTransitions(t *testing.T) {
	no := false
	c, err := New(Options{ClientID: "gsc", ServerURL: "http://x", Flagsets: []string{"main"}, AutoConnect: &no})
	if err != nil {
		t.Fatal(err)
	}
	if c.State() != StateConnecting {
		t.Errorf("initial state = %s, want connecting", c.State())
	}

	got := make(chan ConnectionState, 8)
	unsub := c.OnStateChange(func(s ConnectionState) { got <- s })

	c.recordSuccess()
	if c.State() != StateLive {
		t.Errorf("after success state = %s, want live", c.State())
	}
	c.recordFailure(newError("boom"))
	if c.State() != StateDegraded {
		t.Errorf("after failure state = %s, want degraded", c.State())
	}
	unsub()
	c.recordSuccess() // should not notify after unsubscribe

	// Expect exactly the live, degraded transitions delivered before unsubscribe.
	assertNext(t, got, StateLive)
	assertNext(t, got, StateDegraded)
	select {
	case s := <-got:
		t.Errorf("unexpected notification after unsubscribe: %s", s)
	default:
	}
}

func assertNext(t *testing.T, ch <-chan ConnectionState, want ConnectionState) {
	t.Helper()
	select {
	case s := <-ch:
		if s != want {
			t.Errorf("state notification = %s, want %s", s, want)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", want)
	}
}

func TestIsStale(t *testing.T) {
	no := false
	c, _ := New(Options{ClientID: "gsc", ServerURL: "http://x", Flagsets: []string{"main"}, AutoConnect: &no})
	if !c.IsStale() {
		t.Error("a client that never synced should be stale")
	}
	c.recordSuccess()
	if c.IsStale() {
		t.Error("a just-synced client should not be stale")
	}
}

// TestMalformedDeltaRetainsLastGood verifies the read never returns garbage: a
// delta whose value fails its declared type is rejected, so GetFlag keeps
// returning the last-known-good value and OnError fires.
func TestMalformedDeltaRetainsLastGood(t *testing.T) {
	no := false
	errs := make(chan error, 4)
	c, _ := New(Options{
		ClientID: "gsc", ServerURL: "http://x", Flagsets: []string{"main"},
		AutoConnect: &no,
		OnError:     func(e error) { errs <- e },
	})

	// Seed a good numeric value.
	c.commitFlag("main", "limit", "number", float64(10), rolloutConfig{}, segmentTargeting{}, false)
	if v, _ := c.GetFlag("limit"); v != float64(10) {
		t.Fatalf("seed failed: %v", v)
	}

	// A malformed delta (string for a number flag) must be rejected.
	c.applyDelta(map[string]any{"flagKey": "limit", "flagValue": "not-a-number"}, "main")
	if v, _ := c.GetFlag("limit"); v != float64(10) {
		t.Errorf("malformed delta corrupted value: got %v, want 10", v)
	}
	select {
	case <-errs:
	case <-time.After(time.Second):
		t.Error("expected OnError for malformed value")
	}

	// A well-formed delta still applies.
	c.applyDelta(map[string]any{"flagKey": "limit", "flagValue": float64(20)}, "main")
	if v, _ := c.GetFlag("limit"); v != float64(20) {
		t.Errorf("valid delta not applied: got %v, want 20", v)
	}
}

// TestWarmStartDegraded verifies a process that restarts while the server is
// down warm-starts from the file cache and comes up in StateDegraded, serving
// last-known-good values without returning an error from Connect.
func TestWarmStartDegraded(t *testing.T) {
	cachePath := filepath.Join(t.TempDir(), "cache.json")

	// Server A: serves a snapshot so the first client populates + persists it.
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/sdk/resolve":
			writeJSON(w, map[string]any{"organization_id": "org1"})
		case "/api/v1/config":
			writeJSON(w, map[string]any{"flags": []any{
				map[string]any{"flag_key": "feature_x", "flag_data_type": "bool", "flag_value": true, "updated_at": "2026-01-01T00:00:00Z"},
			}})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer up.Close()

	yes := true
	a, err := New(Options{
		ClientID: "gsc", ServerURL: up.URL, Flagsets: []string{"main"},
		Cache: NewFileCache(cachePath), PollInterval: time.Minute, AutoConnect: &yes,
	})
	if err != nil {
		t.Fatalf("connect A: %v", err)
	}
	if a.State() != StateLive {
		t.Errorf("client A state = %s, want live", a.State())
	}
	if v, _ := a.GetFlag("feature_x"); v != true {
		t.Fatalf("client A did not load flag: %v", v)
	}
	a.flushCache(context.Background()) // deterministic persist
	a.Close()

	// Server B: always fails, simulating an outage during the restart.
	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer down.Close()

	b, err := New(Options{
		ClientID: "gsc", ServerURL: down.URL, Flagsets: []string{"main"},
		Cache: NewFileCache(cachePath), PollInterval: time.Minute, AutoConnect: &yes,
	})
	if err != nil {
		t.Fatalf("graceful connect B returned error: %v", err)
	}
	defer b.Close()

	if b.State() != StateDegraded {
		t.Errorf("client B state = %s, want degraded", b.State())
	}
	if v, _ := b.GetFlag("feature_x"); v != true {
		t.Errorf("warm start failed: got %v, want cached true", v)
	}
}

// TestSSEIdleWatchdog verifies a silently half-open stream (connected but no
// data, not even keepalives) is torn down by the idle watchdog so the loop can
// reconnect, instead of hanging forever.
func TestSSEIdleWatchdog(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Error("response writer is not a flusher")
			return
		}
		_, _ = io.WriteString(w, ": connected\n\n")
		flusher.Flush()
		<-r.Context().Done() // then stall until the client gives up
	}))
	defer srv.Close()

	no := false
	c, _ := New(Options{ClientID: "gsc", ServerURL: srv.URL, Flagsets: []string{"main"}, AutoConnect: &no})
	c.sseReadTimeout = 200 * time.Millisecond

	start := time.Now()
	err := c.consumeSSEStream(context.Background(), map[string]any{"clientId": "gsc", "flagSet": "main"}, "main")
	if err == nil {
		t.Fatal("expected an idle-timeout error, got nil")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("watchdog fired too slowly: %s", elapsed)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
