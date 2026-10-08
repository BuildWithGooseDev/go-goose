package goose

import (
	"encoding/json"
	"os"
	"testing"
)

// Conformance against the shared segment contract, read by the server and the
// Python and JavaScript SDKs too.
const segmentVectorFile = "../../tests/fixtures/segment_vectors.json"

type segmentVectors struct {
	Operators []string                     `json:"operators"`
	Segments  map[string]segmentDefinition `json:"segments"`
	Contexts  map[string]EvaluationContext `json:"contexts"`
	Matches   []struct {
		Segment string `json:"segment"`
		Context string `json:"context"`
		Matches bool   `json:"matches"`
	} `json:"matches"`
	AttributeStrings []struct {
		Value  any    `json:"value"`
		String string `json:"string"`
	} `json:"attribute_strings"`
}

func loadSegmentVectors(t *testing.T) segmentVectors {
	t.Helper()
	raw, err := os.ReadFile(segmentVectorFile)
	if err != nil {
		t.Fatalf("read %s: %v", segmentVectorFile, err)
	}
	var out segmentVectors
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("parse %s: %v", segmentVectorFile, err)
	}
	return out
}

func TestSegmentVectorsMatchSharedContract(t *testing.T) {
	vectors := loadSegmentVectors(t)
	if len(vectors.Matches) == 0 {
		t.Fatal("no match vectors")
	}
	for _, v := range vectors.Matches {
		segment, ok := vectors.Segments[v.Segment]
		if !ok {
			t.Fatalf("unknown segment %q", v.Segment)
		}
		context, ok := vectors.Contexts[v.Context]
		if !ok {
			t.Fatalf("unknown context %q", v.Context)
		}
		if got := matchesSegment(segment, context); got != v.Matches {
			t.Errorf("segment %q against %q = %v, want %v", v.Segment, v.Context, got, v.Matches)
		}
	}
}

func TestSegmentAttributeStringVectors(t *testing.T) {
	for _, v := range loadSegmentVectors(t).AttributeStrings {
		if got := segmentAttributeString(v.Value); got != v.String {
			t.Errorf("segmentAttributeString(%#v) = %q, want %q", v.Value, got, v.String)
		}
	}
}

func segmentTestClient(t *testing.T) *Client {
	t.Helper()
	no := false
	client, err := New(Options{
		ClientID: "gsc_test", ServerURL: "http://localhost:0",
		Flagsets: []string{"main"}, AutoConnect: &no,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return client
}

// A matching subject gets the targeted value; everyone else falls through.
func TestGetFlagServesSegmentValueToMatchingContext(t *testing.T) {
	client := segmentTestClient(t)
	client.applySnapshot("main", []map[string]any{{
		"flag_key":           "new_checkout",
		"flag_data_type":     "bool",
		"flag_value":         false,
		"segment_key":        "enterprise",
		"segment_value":      true,
		"segment_definition": `{"rules":[{"conditions":[{"attribute":"plan","operator":"equals","values":["enterprise"]}]}]}`,
	}})

	inSegment, err := client.GetFlag("new_checkout",
		WithContext(EvaluationContext{"plan": "enterprise"}))
	if err != nil {
		t.Fatalf("GetFlag: %v", err)
	}
	if inSegment != true {
		t.Errorf("a matching subject got %v, want the targeted value true", inSegment)
	}

	outOfSegment, _ := client.GetFlag("new_checkout", WithContext(EvaluationContext{"plan": "free"}))
	if outOfSegment != false {
		t.Errorf("a non-matching subject got %v, want the baseline false", outOfSegment)
	}

	// No context at all behaves like a non-matching subject rather than
	// erroring: a caller that has not adopted targeting yet keeps working.
	noContext, _ := client.GetFlag("new_checkout")
	if noContext != false {
		t.Errorf("a caller with no context got %v, want the baseline false", noContext)
	}
}

// Being in the audience is a statement about who you are; a canary is about
// what fraction of traffic sees something. A segment hit must win outright
// rather than being re-diced.
func TestSegmentTargetingWinsOverCanary(t *testing.T) {
	client := segmentTestClient(t)
	zero := 0
	client.applySnapshot("main", []map[string]any{{
		"flag_key":           "new_checkout",
		"flag_data_type":     "bool",
		"flag_value":         false,
		"segment_key":        "enterprise",
		"segment_value":      true,
		"segment_definition": `{"rules":[{"conditions":[{"attribute":"plan","operator":"equals","values":["enterprise"]}]}]}`,
		// A canary that includes nobody. A targeted subject must still get the
		// targeted value.
		"rollout_percentage": zero,
		"rollout_salt":       "s1",
		"rollout_value":      true,
	}})

	got, _ := client.GetFlag("new_checkout",
		WithContext(EvaluationContext{"plan": "enterprise"}),
		WithTargetingKey("user-123"))
	if got != true {
		t.Errorf("segment hit returned %v despite a 0%% canary; targeting should win", got)
	}
}

// Removing targeting server-side must clear it locally, the same way a delta
// with no percentage clears a canary.
func TestSegmentTargetingIsClearedWhenRemoved(t *testing.T) {
	client := segmentTestClient(t)
	targeted := map[string]any{
		"flag_key": "f", "flag_data_type": "bool", "flag_value": false,
		"segment_key": "s", "segment_value": true,
		"segment_definition": `{"rules":[{"conditions":[{"attribute":"plan","operator":"equals","values":["enterprise"]}]}]}`,
	}
	client.applySnapshot("main", []map[string]any{targeted})

	context := EvaluationContext{"plan": "enterprise"}
	if got, _ := client.GetFlag("f", WithContext(context)); got != true {
		t.Fatalf("setup: expected the targeted value, got %v", got)
	}

	client.applySnapshot("main", []map[string]any{{
		"flag_key": "f", "flag_data_type": "bool", "flag_value": false,
	}})
	if got, _ := client.GetFlag("f", WithContext(context)); got != false {
		t.Errorf("targeting survived its removal: got %v, want false", got)
	}
}

// A flag whose segment rules are corrupt is served untargeted rather than not
// at all — losing an override is much less harmful than failing the read.
func TestUnparseableSegmentDefinitionFallsBackToBaseline(t *testing.T) {
	client := segmentTestClient(t)
	client.applySnapshot("main", []map[string]any{{
		"flag_key": "f", "flag_data_type": "bool", "flag_value": false,
		"segment_key": "s", "segment_value": true,
		"segment_definition": `{not json`,
	}})

	got, err := client.GetFlag("f", WithContext(EvaluationContext{"plan": "enterprise"}))
	if err != nil {
		t.Fatalf("GetFlag should not error: %v", err)
	}
	if got != false {
		t.Errorf("got %v, want the baseline false", got)
	}
}
