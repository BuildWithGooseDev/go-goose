// Command example is a consolidated demo of the Goose Go SDK: it spins up a
// polling client, an SSE client, and a webhook client (plus an optional config
// watcher), prints their initial snapshots, demonstrates a canary read, and then
// logs live deltas until interrupted. Configure it with environment variables or
// an .env file beside this program (see example/README.md).
package main

import (
	"bufio"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	goose "github.com/BuildWithGooseDev/go-goose"
)

// deltaLogger is a StorageAdapter that prints flag deltas once enabled, used to
// show SSE and webhook updates as they arrive.
type deltaLogger struct {
	label   string
	enabled bool
}

func (d *deltaLogger) CreateOrUpdate(flagset, flagKey string, value goose.FlagValue) {
	if d.enabled {
		fmt.Printf("[%s] delta %s.%s=%v\n", d.label, flagset, flagKey, value)
	}
}

func main() {
	loadDotEnv()

	serverURL := env("GOOSE_SERVER_URL", "http://localhost:8080")
	clientID := env("GOOSE_SDK_CLIENT_ID", "")
	clientSecret := env("GOOSE_SDK_CLIENT_SECRET", "")
	if clientID == "" {
		exit("Set GOOSE_SDK_CLIENT_ID before running the example.")
	}

	pollFlagset := env("GOOSE_FLAGSET_POLLTEST", "PollTest")
	sseFlagset := env("GOOSE_FLAGSET_SSETEST", "SSETest")
	hookFlagset := env("GOOSE_FLAGSET_HOOKTEST", "HookTest")
	pollInterval := time.Duration(envFloat("GOOSE_POLL_INTERVAL_SECONDS", 3) * float64(time.Second))

	no := false

	// Flag-only clients (polling, SSE) authenticate with the client id alone.
	pollingClient, err := goose.New(goose.Options{
		ClientID:        clientID,
		ServerURL:       serverURL,
		Flagsets:        []string{pollFlagset},
		ConnectionTypes: []goose.ConnectionType{goose.Polling},
		NamespaceName:   env("GOOSE_NAMESPACE_POLLTEST", ""),
		PollInterval:    pollInterval,
		AutoConnect:     &no,
	})
	must(err)

	sseLogger := &deltaLogger{label: "SSE"}
	sseClient, err := goose.New(goose.Options{
		ClientID:        clientID,
		ServerURL:       serverURL,
		Flagsets:        []string{sseFlagset},
		ConnectionTypes: []goose.ConnectionType{goose.SSE},
		NamespaceName:   env("GOOSE_NAMESPACE_SSETEST", ""),
		StorageAdapter:  sseLogger,
		AutoConnect:     &no,
	})
	must(err)

	clients := []struct {
		name   string
		client *goose.Client
	}{
		{"polling", pollingClient},
		{"sse", sseClient},
	}

	// Webhook mode is secret-bearing, so it is only set up when both a secret and
	// a public target URL are configured.
	webhookLogger := &deltaLogger{label: "WEBHOOK"}
	webhookTarget := env("GOOSE_WEBHOOK_TARGET_URL", "")
	var webhookClient *goose.Client
	if clientSecret != "" && webhookTarget != "" {
		webhookClient, err = goose.New(goose.Options{
			ClientID:            clientID,
			ClientSecret:        clientSecret,
			ServerURL:           serverURL,
			Flagsets:            []string{hookFlagset},
			ConnectionTypes:     []goose.ConnectionType{goose.Webhook},
			NamespaceName:       env("GOOSE_NAMESPACE_HOOKTEST", ""),
			StorageAdapter:      webhookLogger,
			WebhookTargetURL:    webhookTarget,
			WebhookSecret:       env("GOOSE_WEBHOOK_SECRET", "goose-local-webhook-secret"),
			WebhookListenerHost: env("GOOSE_WEBHOOK_LISTENER_HOST", "0.0.0.0"),
			WebhookListenerPort: envInt("GOOSE_WEBHOOK_LISTENER_PORT", 8091),
			WebhookListenerPath: env("GOOSE_WEBHOOK_LISTENER_PATH", "/webhook"),
			AutoConnect:         &no,
		})
		must(err)
		clients = append(clients, struct {
			name   string
			client *goose.Client
		}{"webhook", webhookClient})
	} else {
		fmt.Println("(webhook client skipped: set GOOSE_SDK_CLIENT_SECRET and GOOSE_WEBHOOK_TARGET_URL to enable)")
	}

	// Optional config watcher: each name is a JSON document within one namespace.
	var configClient *goose.Client
	configNamespace := env("GOOSE_CONFIG_NAMESPACE", "")
	configNames := splitCSV(env("GOOSE_CONFIG_NAMES", "frontend,database"))
	// Optional: this app's own version. When set, config entries carrying
	// min_app_version/max_app_version are gated against it — an entry out of
	// range resolves to its "default" instead of its "value", and a change to
	// it never triggers a restart. Leave it empty and every entry applies.
	appVersion := env("GOOSE_APP_VERSION", "")
	if clientSecret != "" && configNamespace != "" && len(configNames) > 0 {
		configClient, err = goose.New(goose.Options{
			ClientID:        clientID,
			ClientSecret:    clientSecret,
			ServerURL:       serverURL,
			Flagsets:        []string{pollFlagset},
			ConnectionTypes: []goose.ConnectionType{goose.Polling},
			NamespaceName:   configNamespace,
			Configs:         configNames,
			AppVersion:      appVersion,
			PollInterval:    pollInterval,
			AutoConnect:     &no,
		})
		must(err)

		onChange := func(e goose.ConfigChangeEvent) {
			fmt.Printf("[CONFIG] '%s' changed (%s): %v -> %v\n", e.Name, e.ApplyStrategy, e.OldValue, e.NewValue)
		}
		for _, name := range configNames {
			configClient.On(name, onChange)
		}
		configClient.OnRestartRequired(func(e goose.ConfigChangeEvent) {
			fmt.Printf("[CONFIG] draining for restart; '%s' has a requires_restart entry\n", e.Name)
		})
	}

	for _, c := range clients {
		if err := c.client.Connect(); err != nil {
			exit(fmt.Sprintf("failed connecting %s client: %v", c.name, err))
		}
		defer c.client.Close()
	}
	if configClient != nil {
		must(configClient.Connect())
		defer configClient.Close()
	}

	fmt.Println("POLLING initial:", pollingClient.Snapshot())
	fmt.Println("SSE initial:", sseClient.Snapshot())
	if webhookClient != nil {
		fmt.Println("WEBHOOK initial:", webhookClient.Snapshot())
	}
	if configClient != nil {
		fmt.Println("CONFIGS initial:", configClient.ConfigsSnapshot())
	}

	// Canary read: flags with a rollout bucket on the targeting key; flags without
	// one ignore it and return their stored value.
	canary, _ := pollingClient.GetFlag("canary_demo", goose.WithTargetingKey("user-123"))
	fmt.Println("canary_demo for user-123:", canary)

	sseLogger.enabled = true
	webhookLogger.enabled = true

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	fmt.Println("Live watch started. Polling snapshot logs every 10s. Press Ctrl+C to stop.")
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			fmt.Println("Stopping...")
			return
		case <-ticker.C:
			fmt.Println("POLLING check:", pollingClient.Snapshot())
			if configClient != nil {
				fmt.Println("CONFIGS check:", configClient.ConfigsSnapshot())
			}
		}
	}
}

func env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v, err := strconv.Atoi(env(key, "")); err == nil {
		return v
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v, err := strconv.ParseFloat(env(key, ""), 64); err == nil {
		return v
	}
	return def
}

func splitCSV(raw string) []string {
	var out []string
	for _, part := range strings.Split(raw, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

// loadDotEnv loads KEY=VALUE lines from an .env file beside this program, without
// overriding variables already set in the environment.
func loadDotEnv() {
	exe, err := os.Executable()
	candidates := []string{".env", filepath.Join("example", ".env")}
	if err == nil {
		candidates = append(candidates, filepath.Join(filepath.Dir(exe), ".env"))
	}
	for _, path := range candidates {
		file, err := os.Open(path)
		if err != nil {
			continue
		}
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" || strings.HasPrefix(line, "#") || !strings.Contains(line, "=") {
				continue
			}
			key, value, _ := strings.Cut(line, "=")
			key = strings.TrimSpace(key)
			if _, ok := os.LookupEnv(key); !ok {
				os.Setenv(key, strings.TrimSpace(value))
			}
		}
		file.Close()
		return
	}
}

func must(err error) {
	if err != nil {
		exit(err.Error())
	}
}

func exit(msg string) {
	slog.Error(msg)
	os.Exit(1)
}
