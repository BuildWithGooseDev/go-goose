package goose

import (
	"encoding/json"
	"strconv"
	"strings"
)

// Segment evaluation — a port of utils/segments.go on the server.
//
// A segment answers "is this subject in this audience", from attributes the
// caller supplies. Evaluated here rather than server-side because only the
// caller has the attributes, and a read must not cost a round trip.
//
// Checked against tests/fixtures/segment_vectors.json, the same file the
// server and the other SDKs read. There is no regex operator on purpose: a
// pattern that backtracks badly would be a denial of service, and in three
// different regex engines it would not even fail consistently.

// Segment condition operators.
const (
	opEquals     = "equals"
	opNotEquals  = "not_equals"
	opContains   = "contains"
	opStartsWith = "starts_with"
	opEndsWith   = "ends_with"
	opIn         = "in"
	opNotIn      = "not_in"

	opGreaterThan      = "gt"
	opGreaterThanEqual = "gte"
	opLessThan         = "lt"
	opLessThanEqual    = "lte"

	opVersionGreaterThan      = "version_gt"
	opVersionGreaterThanEqual = "version_gte"
	opVersionLessThan         = "version_lt"
	opVersionLessThanEqual    = "version_lte"

	opExists    = "exists"
	opNotExists = "not_exists"
)

type segmentCondition struct {
	Attribute string   `json:"attribute"`
	Operator  string   `json:"operator"`
	Values    []string `json:"values"`
}

type segmentRule struct {
	Conditions []segmentCondition `json:"conditions"`
}

type segmentDefinition struct {
	Rules []segmentRule `json:"rules"`
}

// EvaluationContext is the attribute bag used for segment targeting. Values may
// be strings, numbers, booleans or lists; anything else is stringified.
//
// This is separate from the targeting key, which identifies the subject for
// bucketing. A typical call supplies both: the key decides which side of a
// canary you land on, the context decides whether you are in the audience at
// all.
type EvaluationContext map[string]any

// matchesSegment reports whether the context satisfies the segment. An empty
// definition matches nobody — an unfinished audience must not become a full
// rollout.
func matchesSegment(definition segmentDefinition, context EvaluationContext) bool {
	if len(definition.Rules) == 0 {
		return false
	}
	for _, rule := range definition.Rules {
		if segmentRuleMatches(rule, context) {
			return true
		}
	}
	return false
}

func segmentRuleMatches(rule segmentRule, context EvaluationContext) bool {
	if len(rule.Conditions) == 0 {
		return false
	}
	for _, condition := range rule.Conditions {
		if !segmentConditionMatches(condition, context) {
			return false
		}
	}
	return true
}

// segmentConditionMatches evaluates one condition.
//
// A missing attribute makes every operator false except not_exists. In
// particular not_equals is false, not true: the vacuous reading would make a
// negative condition silently match every subject whose context omitted the
// attribute, which during a rollout is usually most of them.
func segmentConditionMatches(condition segmentCondition, context EvaluationContext) bool {
	raw, present := context[condition.Attribute]
	if raw == nil {
		present = false
	}

	switch condition.Operator {
	case opExists:
		return present
	case opNotExists:
		return !present
	}
	if !present {
		return false
	}

	operand := ""
	if len(condition.Values) > 0 {
		operand = condition.Values[0]
	}

	switch condition.Operator {
	case opEquals:
		return anySegmentString(raw, func(s string) bool { return s == operand })
	case opNotEquals:
		return !anySegmentString(raw, func(s string) bool { return s == operand })
	case opContains:
		return anySegmentString(raw, func(s string) bool { return strings.Contains(s, operand) })
	case opStartsWith:
		return anySegmentString(raw, func(s string) bool { return strings.HasPrefix(s, operand) })
	case opEndsWith:
		return anySegmentString(raw, func(s string) bool { return strings.HasSuffix(s, operand) })
	case opIn:
		return anySegmentString(raw, func(s string) bool { return segmentValuesContain(condition.Values, s) })
	case opNotIn:
		return !anySegmentString(raw, func(s string) bool { return segmentValuesContain(condition.Values, s) })

	case opGreaterThan, opGreaterThanEqual, opLessThan, opLessThanEqual:
		return segmentNumericCompare(condition.Operator, raw, operand)

	case opVersionGreaterThan, opVersionGreaterThanEqual,
		opVersionLessThan, opVersionLessThanEqual:
		return segmentVersionCompare(condition.Operator, raw, operand)
	}

	// An operator this SDK does not know must never match: a definition from a
	// newer dashboard must not accidentally include everyone.
	return false
}

func segmentValuesContain(values []string, needle string) bool {
	for _, value := range values {
		if value == needle {
			return true
		}
	}
	return false
}

// anySegmentString applies a predicate to the attribute. A list attribute
// matches when any element does, which is what makes
// {"roles": ["admin","billing"]} work with `in`.
func anySegmentString(raw any, predicate func(string) bool) bool {
	switch typed := raw.(type) {
	case []string:
		for _, item := range typed {
			if predicate(item) {
				return true
			}
		}
		return false
	case []any:
		for _, item := range typed {
			if predicate(segmentAttributeString(item)) {
				return true
			}
		}
		return false
	default:
		return predicate(segmentAttributeString(raw))
	}
}

// segmentAttributeString is the canonical string form of an attribute. Pinned
// explicitly because default float formatting differs between Go, Python and
// JavaScript, and a segment matching in one SDK but not another is exactly the
// failure the shared vectors exist to prevent. An integral float renders "42",
// never "42.0".
func segmentAttributeString(raw any) string {
	switch typed := raw.(type) {
	case nil:
		return ""
	case string:
		return typed
	case bool:
		if typed {
			return "true"
		}
		return "false"
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(typed), 'f', -1, 32)
	case int:
		return strconv.Itoa(typed)
	case int32:
		return strconv.FormatInt(int64(typed), 10)
	case int64:
		return strconv.FormatInt(typed, 10)
	case json.Number:
		return typed.String()
	default:
		encoded, err := json.Marshal(typed)
		if err != nil {
			return ""
		}
		return string(encoded)
	}
}

// segmentAttributeFloat parses an attribute as a number. A non-numeric value
// simply does not match a numeric operator, rather than falling back to a
// string compare — "10" < "9" is true as text and is the classic targeting bug.
func segmentAttributeFloat(raw any) (float64, bool) {
	switch typed := raw.(type) {
	case float64:
		return typed, true
	case float32:
		return float64(typed), true
	case int:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case json.Number:
		parsed, err := typed.Float64()
		return parsed, err == nil
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		return parsed, err == nil
	default:
		return 0, false
	}
}

func segmentNumericCompare(operator string, raw any, operand string) bool {
	left, ok := segmentAttributeFloat(raw)
	if !ok {
		return false
	}
	right, err := strconv.ParseFloat(strings.TrimSpace(operand), 64)
	if err != nil {
		return false
	}
	switch operator {
	case opGreaterThan:
		return left > right
	case opGreaterThanEqual:
		return left >= right
	case opLessThan:
		return left < right
	case opLessThanEqual:
		return left <= right
	}
	return false
}

func segmentVersionCompare(operator string, raw any, operand string) bool {
	// Reuses the same lenient parser the config app-version gating uses, so a
	// version means one thing across the whole product.
	left, ok := parseAppVersion(segmentAttributeString(raw))
	if !ok {
		return false
	}
	right, ok := parseAppVersion(operand)
	if !ok {
		return false
	}
	comparison := compareAppVersions(left, right)
	switch operator {
	case opVersionGreaterThan:
		return comparison > 0
	case opVersionGreaterThanEqual:
		return comparison >= 0
	case opVersionLessThan:
		return comparison < 0
	case opVersionLessThanEqual:
		return comparison <= 0
	}
	return false
}

// segmentTargeting is the per-flag override delivered alongside the value.
type segmentTargeting struct {
	key        string
	value      FlagValue
	definition segmentDefinition
}

// parseSegmentTargeting reads the segment fields off a flag payload. Returns
// false when the flag is not targeted, or when the rules are unparseable — in
// which case the flag is served untargeted rather than not at all.
func parseSegmentTargeting(raw map[string]any) (segmentTargeting, bool) {
	key := strings.TrimSpace(asString(firstValue(raw, "segment_key", "segmentKey")))
	if key == "" {
		return segmentTargeting{}, false
	}
	encoded := strings.TrimSpace(asString(firstValue(raw, "segment_definition", "segmentDefinition")))
	if encoded == "" {
		return segmentTargeting{}, false
	}
	var definition segmentDefinition
	if err := json.Unmarshal([]byte(encoded), &definition); err != nil {
		return segmentTargeting{}, false
	}
	return segmentTargeting{
		key:        key,
		value:      firstValue(raw, "segment_value", "segmentValue"),
		definition: definition,
	}, true
}
