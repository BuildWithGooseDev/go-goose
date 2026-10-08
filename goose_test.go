package goose

import "testing"

// bucketUser must match the Python/JS SDKs exactly so a user buckets identically
// across languages. These expected values are computed with the shared formula
// sha256(salt:flag_key:targeting_key)[:8] big-endian mod 100.
func TestBucketUserCrossLanguage(t *testing.T) {
	cases := []struct {
		salt, flagKey, key string
		want               int
	}{
		{"s1", "new_checkout", "user-123", 73},
		{"s1", "new_checkout", "user-456", 68},
		{"", "edge_ui", "user-123", 39},
	}
	for _, tc := range cases {
		if got := bucketUser(tc.salt, tc.flagKey, tc.key); got != tc.want {
			t.Errorf("bucketUser(%q,%q,%q) = %d, want %d", tc.salt, tc.flagKey, tc.key, got, tc.want)
		}
	}
}

func TestCoerceValue(t *testing.T) {
	if v := coerceValue("true", "bool"); v != true {
		t.Errorf("bool from string: got %v", v)
	}
	if v := coerceValue(false, "bool"); v != false {
		t.Errorf("bool passthrough: got %v", v)
	}
	if v := coerceValue("5", "number"); v != float64(5) {
		t.Errorf("number from string: got %v (%T)", v, v)
	}
	if v := coerceValue(float64(3), "int"); v != float64(3) {
		t.Errorf("number passthrough: got %v", v)
	}
	if v := coerceValue("grid", "list_of_values"); v != "grid" {
		t.Errorf("string passthrough: got %v", v)
	}
}

func TestExtractRolloutCamelAndSnake(t *testing.T) {
	snake := extractRollout(map[string]any{"rollout_percentage": float64(25), "rollout_salt": "abc", "rollout_value": "on"})
	if snake.percentage == nil || *snake.percentage != 25 || snake.salt != "abc" || snake.value != "on" {
		t.Errorf("snake_case rollout parsed wrong: %+v", snake)
	}
	camel := extractRollout(map[string]any{"rolloutPercentage": float64(10), "rolloutSalt": "xyz"})
	if camel.percentage == nil || *camel.percentage != 10 || camel.salt != "xyz" {
		t.Errorf("camelCase rollout parsed wrong: %+v", camel)
	}
	none := extractRollout(map[string]any{"flag_key": "x"})
	if none.percentage != nil {
		t.Errorf("expected nil percentage, got %v", *none.percentage)
	}
}

func TestNewValidation(t *testing.T) {
	no := false
	if _, err := New(Options{ClientID: "", ServerURL: "http://x", Flagsets: []string{"a"}, AutoConnect: &no}); err == nil {
		t.Error("expected error for missing client id")
	}
	if _, err := New(Options{ClientID: "gsc", ServerURL: "http://x", Flagsets: nil, AutoConnect: &no}); err == nil {
		t.Error("expected error for missing flagsets")
	}
	if _, err := New(Options{ClientID: "gsc", ServerURL: "http://x", Flagsets: []string{"a"}, ConnectionTypes: []ConnectionType{Webhook}, WebhookTargetURL: "http://h/webhook", AutoConnect: &no}); err == nil {
		t.Error("expected error for webhook without client secret")
	}
}
