# Ops Dashboard Production Runbook

This dashboard is a separate read-only service. It must not be embedded in
`copybot-app`, and it must not read `live_runtime.db` from web requests.

## Services

- web service: `copybot_ops_dashboard`
- snapshot exporter: `copybot_ops_dashboard_snapshot_export`
- source reports: existing operator binaries

## Dashboard Environment

For a public HTTPS reverse proxy such as `grindscout.com`:

```bash
COPYBOT_OPS_DASHBOARD_BIND=127.0.0.1:8787
COPYBOT_OPS_DASHBOARD_STATIC_DIR=/opt/solana-copy-bot/ops-dashboard/dist
COPYBOT_OPS_DASHBOARD_REPORT_DIR=/var/lib/solana-copy-bot/ops-dashboard-reports
COPYBOT_OPS_DASHBOARD_AUTH_DB=/var/lib/solana-copy-bot/ops-dashboard-auth.db
COPYBOT_OPS_DASHBOARD_PASSWORD_HASH='<argon2id hash>'
COPYBOT_OPS_DASHBOARD_COOKIE_SECURE=true
COPYBOT_OPS_DASHBOARD_TRUST_PROXY_HEADERS=true
```

For localhost/SSH-tunnel only, `COPYBOT_OPS_DASHBOARD_COOKIE_SECURE=false` is
acceptable. Do not use that setting for public HTTPS.

Generate a password hash:

```bash
COPYBOT_OPS_DASHBOARD_PASSWORD='replace-me' \
  copybot_ops_dashboard --hash-password
```

## Real Report Pipeline

Write source operator JSON into an input directory, then export dashboard
snapshots.

```bash
REPORT_IN=/var/lib/solana-copy-bot/operator-json
REPORT_OUT=/var/lib/solana-copy-bot/ops-dashboard-reports
CONFIG=/etc/solana-copy-bot/live.server.toml

copybot_execution_canary_quote_pnl \
  --config "$CONFIG" \
  --json \
  --wallet-reconciliation \
  --metis-diagnostics \
  > "$REPORT_IN/execution_canary_quote_pnl.json.tmp" && \
  mv "$REPORT_IN/execution_canary_quote_pnl.json.tmp" \
     "$REPORT_IN/execution_canary_quote_pnl.json"

discovery_v2_status \
  --config "$CONFIG" \
  > "$REPORT_IN/discovery_v2_status.json.tmp" && \
  mv "$REPORT_IN/discovery_v2_status.json.tmp" \
     "$REPORT_IN/discovery_v2_status.json"

discovery_v2_wallet_report \
  --config "$CONFIG" \
  --top 15 \
  > "$REPORT_IN/discovery_v2_wallet_report.json.tmp" && \
  mv "$REPORT_IN/discovery_v2_wallet_report.json.tmp" \
     "$REPORT_IN/discovery_v2_wallet_report.json"

copybot_runtime_sqlite_wal_pressure_report \
  --config "$CONFIG" \
  --json \
  > "$REPORT_IN/runtime_sqlite_wal_pressure.json.tmp" && \
  mv "$REPORT_IN/runtime_sqlite_wal_pressure.json.tmp" \
     "$REPORT_IN/runtime_sqlite_wal_pressure.json"

copybot_ops_dashboard_snapshot_export \
  --input-dir "$REPORT_IN" \
  --output-dir "$REPORT_OUT" \
  --candidate-floor 8 \
  --max-input-age-secs 900
```

Run this from a timer. Start with a 60-120 second cadence if the source reports
are cheap enough on production. If any source report becomes heavy, keep it on
its existing safe cadence; stale snapshots must show stale, not fake green.

## Reverse Proxy

For `grindscout.com`:

1. Create DNS `A`/`AAAA` records pointing to the server.
2. Terminate HTTPS at the reverse proxy.
3. Proxy only to `127.0.0.1:8787`.
4. Forward `X-Forwarded-For` and `X-Real-IP`.
5. Keep the dashboard port closed to the internet.

Example Caddy route:

```caddyfile
grindscout.com {
  encode zstd gzip
  reverse_proxy 127.0.0.1:8787
}
```

With Caddy/Nginx on the same host, enable
`COPYBOT_OPS_DASHBOARD_TRUST_PROXY_HEADERS=true`. Do not enable trusted proxy
headers if clients can reach the dashboard directly.

## Postflight

- `/api/session` without cookie returns `401`.
- login succeeds over HTTPS and cookie has `Secure; HttpOnly; SameSite=Strict`.
- `/api/overview` has fresh `source`, `generated_at`, `stale=false`.
- Missing source reports produce `stale=true`, never green.
- `copybot-app` is not restarted for dashboard-only changes.
