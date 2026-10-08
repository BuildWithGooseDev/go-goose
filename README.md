# Goose Go SDK

Server-side Go SDK for the Goose config service. It reads feature flags and app
configs by **polling**, a live **SSE** stream, and/or **webhooks**, evaluates
**canary rollouts** locally, and surfaces change events.

Like the Python SDK (and unlike the browser SDK), it is **server-side**: it can
hold a client secret and therefore supports secret-bearing features — app
configs and webhook registration — in addition to flag reads.

It has **no third-party dependencies** (standard library only).

## Install

```bash
go get github.com/BuildWithGooseDev/go-goose
```

Import it as `goose`:

```go
import goose "github.com/BuildWithGooseDev/go-goose"
```

> The module lives in this monorepo and is intentionally **not** part of the Go
> workspace (`go.work`), so it stays decoupled and distributable. When building
> it from inside the repo, use `GOWORK=off go build ./...`.

## Quickstart

```go
client, err := goose.New(goose.Options{
    ClientID:     "<your-client-id>",     // gsc_… from SDK Access
    ClientSecret: "<your-client-secret>", // only needed for configs/webhooks
    ServerURL:    "https://goose.example.com",
    Flagsets:     []string{"checkout"},
    NamespaceName: "production",
})
if err != nil {
    log.Fatal(err)
}
defer client.Close()

if client.GetBool("edge_ui", false) {
    renderNewCheckout()
} else {
    renderClassicCheckout()
}
```

`New` connects immediately by default (loading the initial snapshot before it
returns). To connect later yourself, set `AutoConnect` to a pointer to `false`
and call `client.Connect()`.

By default connecting is **graceful**: if the config service is unreachable at
startup, `Connect` does not fail — the client comes up in `StateDegraded`,
serves warm-started cache values (or your defaults), and heals in the
background. Set `RequireInitialConnect: true` to restore fail-fast startup
(`Connect`/`New` return an error when the first fetch fails). See
[Resilience & self-healing](#resilience--self-healing).

## Reading flags

`GetFlag` returns the raw `any` value; typed helpers coerce and fall back to a
default when a flag is unknown or the wrong type:

```go
raw, _ := client.GetFlag("edge_ui")                 // any (nil if unset)
on    := client.GetBool("edge_ui", false)           // bool
theme := client.GetString("checkout_theme", "classic")
retries := client.GetInt("max_retries", 3)          // int64
timeout := client.GetFloat("api_timeout_ms", 250)   // float64
```

Per-call options:

```go
client.GetBool("edge_ui", false,
    goose.WithFlagset("checkout"),       // required if multiple flagsets
    goose.WithTargetingKey(user.ID),     // canary bucketing key
)
```

Values are typed by the flag's data type: **bool** flags resolve to `bool`,
**number** flags to `float64` (use `GetInt` to truncate), and **string** /
list-of-values flags to their stored string verbatim.

## Connection modes

Modes can be combined; the default is polling.

```go
// Polling (default): refreshes every PollInterval.
goose.Options{ConnectionTypes: []goose.ConnectionType{goose.Polling}, PollInterval: 5 * time.Second}

// SSE: a live stream pushes flag deltas as they happen.
goose.Options{ConnectionTypes: []goose.ConnectionType{goose.SSE}}

// Webhook: the server posts deltas to an embedded listener (secret-bearing).
goose.Options{
    ConnectionTypes:  []goose.ConnectionType{goose.Webhook},
    WebhookTargetURL: "https://app.example.com/webhook",
}

// Combined:
goose.Options{ConnectionTypes: []goose.ConnectionType{goose.Polling, goose.SSE}}
```

## Namespaces

```go
// One namespace for every flagset:
goose.Options{Flagsets: []string{"checkout", "search"}, NamespaceName: "production"}

// Or per-flagset namespaces:
goose.Options{
    Flagsets:          []string{"checkout", "search"},
    FlagsetNamespaces: map[string]string{"checkout": "production", "search": "staging"},
}
```

## Canary rollouts

When a flag has a canary rollout configured, the SDK buckets each user
**locally** so the percentage is deterministic and sticky (a user enabled at 10%
stays enabled at 50%). Pass a stable per-user key:

```go
if client.GetBool("new_checkout", false, goose.WithTargetingKey(user.ID)) {
    // this user is in the canary cohort
}
```

- Flags **without** a rollout ignore the targeting key and return their value.
- Set `DefaultTargetingKey` on `Options` to avoid passing it on every call.
- A canaried flag evaluated with no targeting key returns the baseline value and
  logs a warning (the feature is never leaked to unidentified users).
- Bucketing is `sha256(salt:flag_key:targeting_key)[:8] % 100 < percentage`,
  matching the Python/JS SDKs so every SDK buckets identically.

## Change listeners

```go
unsubscribe := client.OnChange(func(c goose.FlagChange) {
    log.Printf("[%s] %s: %v -> %v", c.Flagset, c.FlagKey, c.PreviousValue, c.Value)
})
defer unsubscribe()

snap := client.Snapshot() // map[flagset]map[flagKey]value (baseline values)
```

## Storage adapters

Implement `StorageAdapter` to mirror flag values into an external store as they
change. The SDK calls it on the initial load and on every delta (polling / SSE /
webhook); errors/panics are logged, never propagated.

```go
type StorageAdapter interface {
    CreateOrUpdate(flagset, flagKey string, value goose.FlagValue)
}

goose.Options{StorageAdapter: myRedisAdapter}
```

> `StorageAdapter` is **write-only** mirroring for your own consumers. To
> persist last-known-good state that the SDK reads back on restart, use `Cache`
> instead — see [Resilience & self-healing](#resilience--self-healing).

## Configs

A **config** is a named JSON **document** inside the client-level
`NamespaceName`, and a namespace can hold many. The client watches a set of
config **names**; each has its own document and revision. Configs are delivered
on the **polling** loop. Each document has the shape:

```json
{
  "configs": {
    "<entryKey>": { "value": <any>, "apply_strategy": "immediate" | "requires_restart" }
  }
}
```

```go
client, _ := goose.New(goose.Options{
    ClientID:      "<your-client-id>",
    ClientSecret:  "<your-client-secret>",   // required: configs are secret-bearing
    ServerURL:     "https://goose.example.com",
    Flagsets:      []string{"checkout"},
    NamespaceName: "production",              // required when watching configs
    Configs:       []string{"frontend", "database"},
})

// Per-name listener; fires when that config's document changes.
client.On("frontend", func(e goose.ConfigChangeEvent) {
    // OldValue/NewValue are full documents (OldValue is nil on first sighting).
    log.Printf("%s changed (%s)", e.Name, e.ApplyStrategy)
})

// Drain hook: always runs before a requires_restart change triggers a restart.
client.OnRestartRequired(func(e goose.ConfigChangeEvent) {
    finishInFlight()
})

frontend := client.GetConfig("frontend", nil)                          // whole document
layout   := client.GetConfigValue("frontend", "dashboard_layout", "grid") // one entry's value
all      := client.ConfigsSnapshot()                                   // map[name]document
```

### Entry metadata

Beyond `value` and `apply_strategy`, an entry may carry optional metadata. The
server stores and delivers it untouched — none of it is a server-side gate — and
this SDK acts on it at read time, keyed off `Options.AppVersion`:

| Key | Type | What the SDK does with it |
|---|---|---|
| `default` | any | Served instead of `value` when the entry is gated out by app version |
| `min_app_version` | string | Lowest `app_version` the entry applies to (**inclusive**) |
| `max_app_version` | string | Highest `app_version` the entry applies to (**inclusive**) |
| `deprecated` | bool | Warns once, the first time the entry is read |
| `replaced_by` | string | Successor entry named in that deprecation warning |
| `sensitive` | bool | Keeps the value out of the on-disk `PersistentCache` |
| `required` | bool | Reports an error when the entry resolves to nothing |

Versions are lenient dotted strings — `"3"`, `"2.4"`, `"2.4.1"`, `"v2.4.1-rc.1"`
all parse, a leading `v` is dropped, and any pre-release/build suffix is ignored.
They must be **quoted**: unquoted `2.4` is a number, and both the editor and the
server reject it.

Everything fails open. A client built without an app version applies every
entry, an unparseable version or bound disables that gate, and a `default` is
only consulted for an entry that is actually gated out — so existing documents
and existing clients behave exactly as they did before.

```go
client, _ := goose.New(goose.Options{
    ClientID:      "<your-client-id>",
    ClientSecret:  "<your-client-secret>",
    ServerURL:     "https://goose.example.com",
    Flagsets:      []string{"checkout"},
    NamespaceName: "production",
    Configs:       []string{"frontend"},
    AppVersion:    "2.1.0", // optional; omit to apply every entry
})

// For an entry with min_app_version "2.4.0", this 2.1.0 build resolves to the
// entry's "default" rather than its "value".
layout := client.GetConfigValue("frontend", "dashboard_layout", "grid")
```

Gating applies to restarts too: a `requires_restart` change to an entry this
build is gated out of will not restart the process. `GetConfig` returns the raw
document, so reading `entry["value"]` out of it yourself bypasses all of the
above — `GetConfigValue` is where resolution happens.

### Apply strategy

`apply_strategy` is a property of each inner entry. A config change's overall
strategy is `requires_restart` when **any** changed inner entry requires a
restart, otherwise `immediate`. On a `requires_restart` change, per-name
listeners fire, then **all** drain callbacks registered with
`OnRestartRequired` run. Set `RestartOnRequiredChange: true` to additionally
have the SDK raise `SIGINT` (after drains) so an orchestrator restarts the
process. The initial snapshot is loaded on connect **without** firing listeners.

### Secrets in configs

Config documents can reference **secrets** managed in the dashboard's Secrets
tab. Inside any string value, `${secret_name}` is replaced with the secret's
value **server-side** before delivery, so the SDK needs no keys or setup —
values arrive already substituted. Changing a secret behaves like a config
change: the next poll delivers a different resolved document, firing listeners.

## Webhook mode

With `ConnectionTypes: []goose.ConnectionType{goose.Webhook}`:

- `WebhookTargetURL` (required) — the public URL the server posts deltas to.
- `WebhookSecret` (optional) — shared secret, auto-generated if empty.
- `WebhookListenerHost` (default `0.0.0.0`), `WebhookListenerPort` (default
  `8091`), `WebhookListenerPath` (default derived from the target URL, else
  `/webhook`).

The SDK starts an embedded listener and applies deltas internally — no handler
wiring required. Inspect it via `client.WebhookListenerURL()` and
`client.WebhookEventsReceived()`.

### At-least-once delivery / deduplication

The server delivers webhooks **at-least-once**: after a config-service replica
crashes mid-dispatch, another resends the same delta. Each delta carries a
unique `eventId`, and the SDK applies each `eventId` at most once. If you process
webhook payloads yourself with `client.ProcessWebhookEvent(...)` instead of the
embedded listener, the same dedup applies.

> Webhook mode is **not** for horizontally scaled SDK consumers — use polling or
> SSE for those.

## Resilience & self-healing

The SDK is built to keep serving flags through config-service outages, network
blips, and process restarts — and to recover on its own.

**Reads never fail.** `GetFlag` and the typed helpers always return a value from
a fallback ladder — the current value, else the last-known-good value, else your
default — and never panic or return an error to the caller. Malformed values
pushed from the server (e.g. a non-numeric value for a number flag) are rejected
and logged rather than overwriting a good value, so a bad delta can't corrupt
what you read.

**Warm start.** Provide a `Cache` to persist last-known-good state (flags,
rollouts, poll cursors, watched configs). It is read back on `Connect` **before**
any network call, so a process that restarts during an outage boots with real
values instead of an empty cache. `NewFileCache` is a built-in atomic
file-backed implementation:

```go
goose.Options{Cache: goose.NewFileCache("/var/lib/myapp/goose-cache.json")}
```

**Graceful degraded start.** With the default (`RequireInitialConnect: false`) a
failed initial fetch starts the client in `StateDegraded` and heals in the
background instead of erroring out of `New`/`Connect`.

**Disciplined reconnects.** SSE reconnects and post-error retries use
exponential backoff with jitter (capped), so a fleet of clients doesn't stampede
a recovering server, and a `429`'s `Retry-After` is always honored. A silently
half-open SSE stream is detected by an idle watchdog (`SSEReadTimeout`) and
reconnected. In SSE-only mode, a sustained stream outage automatically falls back
to polling until the stream recovers.

**Observability.** Inspect and react to health:

```go
client.State()      // StateConnecting | StateLive | StateDegraded | StateClosed
client.LastSync()   // time of the last successful sync
client.IsStale()    // true if no successful sync within the staleness window

client.OnStateChange(func(s goose.ConnectionState) {
    metrics.Gauge("goose.state", s.String())
})
client.OnError(func(err error) {   // background errors; never affect reads
    log.Printf("goose background error: %v", err)
})
```

## Notes

- Organization id is resolved best-effort from the client id via
  `/api/v1/sdk/resolve` (informational; exposed via `client.OrganizationID()`).
- Snapshots use `GET /api/v1/config`; flag polling uses `POST /api/v1/poll`.
  Poll responses may be served from a short-lived cache, so a brand-new change
  can take a few seconds to appear on the poll path.
- Config snapshots use `GET /api/v1/configs`; config polling uses
  `POST /api/v1/configs/poll`.
- The `Client` is safe for concurrent use. Call `client.Close()` on shutdown.

## Example

A consolidated runnable example lives in [`example/`](./example). See its README
to run it.

## Test

```bash
GOWORK=off go test ./...
```
