# Discovery Runtime Restore Runbook

This runbook is the operator path for restoring discovery runtime state from the scheduled runtime artifact export and recent raw journal snapshot introduced in Batch 3.

## Scheduled backup surfaces

Live config defaults in `ops/server_templates/live.server.toml.example`:

1. Runtime artifact exports:
   - directory: `/var/www/solana-copy-bot/state/discovery_restore/artifacts`
   - latest artifact: `/var/www/solana-copy-bot/state/discovery_restore/artifacts/latest.json`
   - cadence: `runtime_restore_ops.artifact_cadence_minutes`
2. Recent raw journal snapshots:
   - directory: `/var/www/solana-copy-bot/state/discovery_restore/recent_raw`
   - latest snapshot: `/var/www/solana-copy-bot/state/discovery_restore/recent_raw/latest.sqlite`
   - latest metadata: `/var/www/solana-copy-bot/state/discovery_restore/recent_raw/latest.json`
   - cadence: `runtime_restore_ops.journal_snapshot_cadence_minutes`

Systemd wiring:

1. `copybot-discovery-runtime-export.service`
2. `copybot-discovery-runtime-export.timer`
3. `copybot-discovery-recent-raw-snapshot.service`
4. `copybot-discovery-recent-raw-snapshot.timer`

Check timers:

```bash
sudo systemctl list-timers 'copybot-discovery-*'
```

## Incident restore flow

### 1. Stop the app

```bash
sudo systemctl stop solana-copy-bot.service
```

### 2. Archive the broken runtime DB

```bash
ts="$(date -u +%Y%m%dT%H%M%SZ)"
cd /var/www/solana-copy-bot/state
mkdir -p broken
mv live_copybot.db "broken/live_copybot.db.${ts}" || true
mv live_copybot.db-wal "broken/live_copybot.db-wal.${ts}" || true
mv live_copybot.db-shm "broken/live_copybot.db-shm.${ts}" || true
```

### 3. Restore into a fresh runtime DB

```bash
/var/www/solana-copy-bot/target/release/discovery_runtime_restore \
  --config /etc/solana-copy-bot/live.server.toml \
  --artifact /var/www/solana-copy-bot/state/discovery_restore/artifacts/latest.json \
  --db-path /var/www/solana-copy-bot/state/live_copybot.db \
  --journal-db-path /var/www/solana-copy-bot/state/discovery_restore/recent_raw/latest.sqlite \
  --json | tee /tmp/discovery_runtime_restore.json
```

Inspect the restore verdict:

```bash
python3 - <<'PY'
import json
from pathlib import Path

payload = json.loads(Path("/tmp/discovery_runtime_restore.json").read_text())
print("verdict:", payload["verdict"]["verdict"])
print("runtime_mode:", payload["verdict"]["runtime_mode"])
print("runtime_state:", payload["verdict"]["runtime_state"])
print("journal_available:", payload["verdict"]["journal_available"])
print("journal_replayed:", payload["verdict"]["journal_replayed"])
print("journal_covers_artifact_cursor:", payload["verdict"]["journal_covers_artifact_cursor"])
print("raw_coverage_satisfied:", payload["verdict"]["raw_coverage_satisfied"])
print("journal_replayed_rows:", payload["verdict"]["journal_replayed_rows"])
PY
```

### 4. Cross-check operator status

```bash
/var/www/solana-copy-bot/target/release/discovery_status \
  --config /etc/solana-copy-bot/live.server.toml \
  --db-path /var/www/solana-copy-bot/state/live_copybot.db \
  --json | tee /tmp/discovery_status_after_restore.json
```

Minimum fields to inspect:

1. `runtime_state`
2. `runtime_mode`
3. `scoring_source`
4. `recent_raw_restore.journal_available`
5. `recent_raw_restore.journal_replayed`
6. `recent_raw_restore.journal_covers_artifact_cursor`
7. `recent_raw_restore.raw_coverage_satisfied`
8. `publication.bootstrap_degraded_active`

### 5. Decide service posture

1. If `verdict.verdict == "trading_ready"`, start the service:

```bash
sudo systemctl start solana-copy-bot.service
```

2. If `verdict.verdict == "bootstrap_degraded"`, keep `execution.enabled = false`. Starting the service is allowed only for explicit degraded recovery; it must remain non-trading-ready until fresh raw truth repopulates through the normal runtime path.
3. If `verdict.verdict == "fail_closed"`, do not re-enable the live service. Investigate backup freshness, journal coverage, or source corruption first.

## Restore drill

The drill uses the same export, journal snapshot, restore, and status surfaces that operators use during an incident.

```bash
cd /var/www/solana-copy-bot
./tools/discovery_restore_drill.sh \
  --config /etc/solana-copy-bot/live.server.toml \
  --workspace /var/www/solana-copy-bot/state/discovery_restore/drills/manual-$(date -u +%Y%m%dT%H%M%SZ)
```

The drill writes:

1. `artifact_export.json`
2. `journal_snapshot.json`
3. `restore_output.json`
4. `status_output.json`
5. `restore_drill_report.json`

The report contains:

1. `measured_rto_ms`
2. `artifact_cadence_minutes`
3. `journal_snapshot_cadence_minutes`
4. `guaranteed_rpo_minutes`
5. `final_verdict`
6. `final_runtime_mode`
7. `final_runtime_state`

## Measured Batch 3 outcome

Measured on: `2026-03-24`, local release drill using `target/release/*` binaries, `discovery_restore_demo_fixture`, and `tools/discovery_restore_drill.sh`.

1. RTO: `690 ms` from fresh target creation to restore verdict + status collection.
2. RPO: `10 minutes`, bounded by `runtime_restore_ops.artifact_cadence_minutes = 10` and `runtime_restore_ops.journal_snapshot_cadence_minutes = 10`.

## Remaining failure modes

1. RPO is bounded by the slower of scheduled artifact export cadence and scheduled journal snapshot cadence.
2. Restore remains fail-closed if the latest journal snapshot does not cover the artifact cursor lineage or the required raw window.
3. Bootstrap-degraded restore preserves publication truth but remains non-trading-ready until fresh raw truth is rebuilt by the normal runtime path.
