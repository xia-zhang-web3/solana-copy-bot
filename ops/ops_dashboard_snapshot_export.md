# Ops Dashboard Snapshot Export

The dashboard web service is a read-only viewer. It must not run heavy report
queries or keep long read transactions against `live_runtime.db`.

This export layer converts already-produced operator JSON files into the small
snapshot files served by `copybot_ops_dashboard`.

## Build Target

- binary: `copybot_ops_dashboard_snapshot_export`
- crate: `copybot-ops-dashboard`
- production daemon restart: not required

## Input Directory

Expected files:

- `execution_canary_quote_pnl.json`
- `discovery_v2_status.json`
- `discovery_v2_wallet_report.json`
- `runtime_sqlite_wal_pressure.json`
- optional: `strategy.json`

These files should be produced by existing operators, outside the dashboard web
request path.

`discovery_v2_wallet_report.json` is used only to display filter state such as
the rug-filter thresholds and quarantine readiness. The dashboard must show
`not reported` when it is missing or stale; it must not invent filter status.

## Output Directory

The exporter writes:

- `overview.json`
- `execution.json`
- `discovery.json`
- `storage.json`
- `strategy.json`
- `alerts.json`
- `reports.json`

Writes are atomic: `*.json.tmp` is written first, then renamed into place.

## Command

```bash
copybot_ops_dashboard_snapshot_export \
  --input-dir /var/lib/solana-copy-bot/operator-json \
  --output-dir /var/lib/solana-copy-bot/ops-dashboard-reports \
  --candidate-floor 8 \
  --max-input-age-secs 900
```

The dashboard service should use:

```bash
COPYBOT_OPS_DASHBOARD_REPORT_DIR=/var/lib/solana-copy-bot/ops-dashboard-reports
```

## Safety Rules

- The exporter reads files only.
- It does not open `live_runtime.db`.
- It does not run operator binaries.
- Missing or stale inputs produce `stale=true`, never fake green.
- `overview.status=green` requires fresh inputs and the basic floor/reconciliation/WAL checks.

## Suggested Timer

Run the heavy source operators on their existing safe cadence, then run this
exporter. Keep the dashboard refresh interval shorter than the export cadence,
but rely on `stale=true` instead of showing stale green.

Start with a 60-120 second export cadence after the source reports are written.
