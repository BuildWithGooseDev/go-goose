# Goose Go SDK Example

A consolidated demo that runs a polling client, an SSE client, and (optionally) a
webhook client and a config watcher against a running config service.

## Setup

Copy the template and fill in your SDK credentials:

```bash
cp sdks/go/example/.env.example sdks/go/example/.env
$EDITOR sdks/go/example/.env
```

`main.go` auto-loads an `.env` file beside it (without overriding variables
already set in your environment). At minimum set `GOOSE_SDK_CLIENT_ID` and point
`GOOSE_SERVER_URL` at your config service.

The webhook client is set up only when both `GOOSE_SDK_CLIENT_SECRET` and
`GOOSE_WEBHOOK_TARGET_URL` are set; the config watcher only when
`GOOSE_SDK_CLIENT_SECRET` and `GOOSE_CONFIG_NAMESPACE` are set.

Set `GOOSE_APP_VERSION` (e.g. `2.4.0`) to switch on config **app-version
gating**: an entry whose `min_app_version`/`max_app_version` range excludes
that version resolves to its `default` instead of its `value`, and a change
to it never triggers a restart. Leave it unset and every entry applies.

## Run

From the repo root (the SDK module is outside the Go workspace, so disable it):

```bash
cd sdks/go/example
GOWORK=off go run .
```

## Behavior

- Connects a polling client (`GOOSE_FLAGSET_POLLTEST`) and an SSE client
  (`GOOSE_FLAGSET_SSETEST`); adds a webhook client (`GOOSE_FLAGSET_HOOKTEST`) and
  a config watcher when configured.
- Prints each client's initial snapshot and a canary read for `canary_demo`.
- Enters a live loop: every 10s it logs the polling snapshot, while SSE and
  webhook deltas print immediately as they arrive.
- Stop with `Ctrl+C`.
