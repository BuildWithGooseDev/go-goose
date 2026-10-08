package goose

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
)

// ErrInvalidWebhookSecret is returned by ProcessWebhookEvent when the supplied
// secret does not match the configured webhook secret.
var ErrInvalidWebhookSecret = errors.New("invalid webhook secret")

// RegisterWebhooks registers (or re-registers) a webhook per flagset with the
// server. Already-existing registrations are treated as success.
func (c *Client) RegisterWebhooks(ctx context.Context) error {
	for _, flagset := range c.flagsets {
		body := map[string]any{
			"client_id":     c.clientID,
			"client_secret": c.clientSecret,
			"webhook_name":  fmt.Sprintf("%s-%s", c.webhookNamePrefix, flagset),
			"flagset":       flagset,
			"target_url":    c.webhookTargetURL,
			"secret":        c.webhookSecret,
		}
		if ns := c.namespaceForFlagset(flagset); ns != "" {
			body["namespace_name"] = ns
		}

		_, err := c.requestJSON(ctx, http.MethodPost, "/api/v1/webhook", nil, body)
		if err == nil {
			continue
		}
		var httpErr *HTTPError
		if asHTTPError(err, &httpErr) {
			if httpErr.StatusCode == 409 {
				continue
			}
			if httpErr.StatusCode == 400 &&
				(strings.Contains(strings.ToLower(httpErr.ResponseText), "already exists") ||
					strings.TrimSpace(httpErr.ResponseText) == "") {
				continue
			}
		}
		return err
	}
	return nil
}

// ProcessWebhookEvent applies a webhook delta. When receivedSecret is non-nil it
// is compared (in constant time) against the configured webhook secret. Use this
// directly only if you run your own HTTP handler instead of the embedded
// listener; deltas are deduped by eventId, so repeated deliveries are safe.
func (c *Client) ProcessWebhookEvent(payload map[string]any, receivedSecret *string) error {
	if receivedSecret != nil &&
		subtle.ConstantTimeCompare([]byte(*receivedSecret), []byte(c.webhookSecret)) != 1 {
		return ErrInvalidWebhookSecret
	}
	c.applyDelta(payload, "")
	c.mu.Lock()
	c.webhookEventsCount++
	c.mu.Unlock()
	return nil
}

func (c *Client) startWebhookListener() error {
	c.mu.Lock()
	if c.webhookServerCloseFn != nil {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	mux := http.NewServeMux()
	mux.HandleFunc(c.webhookListenerPath, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		raw, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		payload, err := decodeJSONObject(string(raw))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte("invalid json payload"))
			return
		}

		var receivedSecret *string
		if header := r.Header.Get("X-Webhook-Secret"); header != "" {
			receivedSecret = &header
		}
		if err := c.ProcessWebhookEvent(payload, receivedSecret); err != nil {
			if errors.Is(err, ErrInvalidWebhookSecret) {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte("invalid webhook secret"))
				return
			}
			c.log.Warn("embedded webhook processing failed", "err", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	addr := fmt.Sprintf("%s:%d", c.webhookListenerHost, c.webhookListenerPort)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return newError("failed to start embedded webhook listener on %s: %v", addr, err)
	}

	server := &http.Server{Handler: mux}
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			c.log.Warn("embedded webhook listener stopped", "err", err)
		}
	}()

	c.mu.Lock()
	c.webhookServerCloseFn = func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), c.requestTimeout)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}
	c.mu.Unlock()

	c.log.Info("embedded webhook listener started", "url", c.WebhookListenerURL())
	return nil
}

func (c *Client) stopWebhookListener() {
	c.mu.Lock()
	closeFn := c.webhookServerCloseFn
	c.webhookServerCloseFn = nil
	c.mu.Unlock()
	if closeFn != nil {
		closeFn()
	}
}
