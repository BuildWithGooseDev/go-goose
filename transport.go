package goose

import (
	"bytes"
	"container/list"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// requestJSON performs a request and decodes the JSON object response. A 2xx
// with an empty body yields an empty map; non-2xx yields an *HTTPError.
func (c *Client) requestJSON(ctx context.Context, method, path string, query map[string]string, body any) (map[string]any, error) {
	text, err := c.request(ctx, method, path, query, body)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(text) == "" {
		return map[string]any{}, nil
	}
	parsed, err := decodeJSONObject(text)
	if err != nil {
		return map[string]any{}, nil
	}
	return parsed, nil
}

func (c *Client) request(ctx context.Context, method, path string, query map[string]string, body any) (string, error) {
	reqURL := c.serverURL + path
	if len(query) > 0 {
		values := url.Values{}
		for k, v := range query {
			values.Set(k, v)
		}
		reqURL += "?" + values.Encode()
	}

	var reader io.Reader
	if body != nil {
		encoded, err := encodeJSON(body)
		if err != nil {
			return "", err
		}
		reader = encoded
	}

	reqCtx, cancel := context.WithTimeout(ctx, c.requestTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, method, reqURL, reader)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Goose-Client-Thumbprint", c.thumbprint)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	text := string(raw)

	if resp.StatusCode >= 400 {
		return "", &HTTPError{
			StatusCode:   resp.StatusCode,
			Message:      extractErrorMessage(method, path, resp.StatusCode, text),
			ResponseText: text,
			RetryAfter:   parseRetryAfter(resp.Header.Get("Retry-After")),
		}
	}
	return text, nil
}

// parseRetryAfter interprets a Retry-After header (delta-seconds or HTTP-date),
// returning 0 when it is absent, malformed, or in the past.
func parseRetryAfter(value string) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if secs, err := strconv.Atoi(value); err == nil {
		if secs <= 0 {
			return 0
		}
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(value); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}

func extractErrorMessage(method, path string, statusCode int, responseText string) string {
	fallback := fmt.Sprintf("%s %s failed (%d)", method, path, statusCode)
	if strings.TrimSpace(responseText) == "" {
		return fallback
	}
	parsed, err := decodeJSONObject(responseText)
	if err != nil {
		return responseText
	}
	if msg, ok := parsed["error"].(string); ok && msg != "" {
		return msg
	}
	return fallback
}

func encodeJSON(value any) (*bytes.Reader, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(raw), nil
}

func decodeJSONObject(text string) (map[string]any, error) {
	var out map[string]any
	decoder := json.NewDecoder(strings.NewReader(text))
	decoder.UseNumber()
	if err := decoder.Decode(&out); err != nil {
		return nil, err
	}
	return normalizeNumbers(out).(map[string]any), nil
}

// normalizeNumbers converts json.Number values to float64 throughout a decoded
// payload so downstream coercion sees a single numeric type.
func normalizeNumbers(value any) any {
	switch v := value.(type) {
	case map[string]any:
		for k, inner := range v {
			v[k] = normalizeNumbers(inner)
		}
		return v
	case []any:
		for i, inner := range v {
			v[i] = normalizeNumbers(inner)
		}
		return v
	case json.Number:
		if f, err := v.Float64(); err == nil {
			return f
		}
		return v.String()
	}
	return value
}

func asString(value any) string {
	if value == nil {
		return ""
	}
	if s, ok := value.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", value)
}

func firstValue(payload map[string]any, keys ...string) any {
	for _, key := range keys {
		if v, ok := payload[key]; ok && v != nil {
			return v
		}
	}
	return nil
}

func firstString(payload map[string]any, keys ...string) string {
	return asString(firstValue(payload, keys...))
}

// valuesEqual reports whether two flag values are equal for change detection.
func valuesEqual(a, b FlagValue) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	switch av := a.(type) {
	case string:
		bv, ok := b.(string)
		return ok && av == bv
	case bool:
		bv, ok := b.(bool)
		return ok && av == bv
	}
	if af, ok := toFloat(a); ok {
		if bf, ok := toFloat(b); ok {
			return af == bf
		}
	}
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

// toUnixSeconds parses a server timestamp to unix seconds, handling RFC3339 and
// Go's time.String() form. Returns 0 when unparseable.
func toUnixSeconds(raw string) int64 {
	value := strings.TrimSpace(raw)
	if value == "" {
		return 0
	}
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05.999999999 -0700 MST",
		"2006-01-02 15:04:05 -0700 MST",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, value); err == nil {
			return t.Unix()
		}
	}
	return 0
}

func webhookTargetPath(target string) (string, error) {
	parsed, err := url.Parse(target)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "", newError("WebhookTargetURL must be an absolute URL")
	}
	return parsed.Path, nil
}

func randomSecret() string {
	buf := make([]byte, 24)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("goose-%d", time.Now().UnixNano())
	}
	return base64.RawURLEncoding.EncodeToString(buf)
}

func (c *Client) resolveOrganizationID(ctx context.Context) {
	payload, err := c.requestJSON(ctx, http.MethodGet, "/api/v1/sdk/resolve", map[string]string{"client_id": c.clientID}, nil)
	if err != nil {
		var httpErr *HTTPError
		if asHTTPError(err, &httpErr) && (httpErr.StatusCode == 404 || httpErr.StatusCode == 405) {
			return
		}
		c.log.Debug("sdk org resolve failed", "err", err)
		return
	}
	orgID := strings.TrimSpace(asString(payload["organization_id"]))
	c.mu.Lock()
	c.resolvedOrgID = orgID
	c.mu.Unlock()
}

func asHTTPError(err error, target **HTTPError) bool {
	if httpErr, ok := err.(*HTTPError); ok {
		*target = httpErr
		return true
	}
	return false
}

// lruSet is a bounded set with least-recently-used eviction, used to dedupe
// at-least-once delta deliveries by eventId.
type lruSet struct {
	mu    sync.Mutex
	max   int
	items map[string]*list.Element
	order *list.List
}

func newLRUSet(max int) *lruSet {
	return &lruSet{max: max, items: map[string]*list.Element{}, order: list.New()}
}

// seen records id and reports whether it had already been seen recently. A
// repeat refreshes recency so a later redelivery cannot clobber a newer value.
func (s *lruSet) seen(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if el, ok := s.items[id]; ok {
		s.order.MoveToBack(el)
		return true
	}
	s.items[id] = s.order.PushBack(id)
	for s.order.Len() > s.max {
		front := s.order.Front()
		if front == nil {
			break
		}
		s.order.Remove(front)
		delete(s.items, front.Value.(string))
	}
	return false
}
