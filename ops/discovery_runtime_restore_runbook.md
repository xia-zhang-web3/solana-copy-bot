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
3. Bounded gap-fill source:
   - generic fallback block: `recent_raw_gap_fill`
   - Helius-specific block: `recent_raw_gap_fill_helius`
   - required fields: `recent_raw_gap_fill.helius_http_url` for the generic fallback, `recent_raw_gap_fill_helius.helius_http_url` for the Helius-exclusive `getTransactionsForAddress` path
   - live server contract: populate them in `/etc/solana-copy-bot/live.server.toml` or pass `--helius-http-url` explicitly during the incident run
   - default gap-fill output: `/var/www/solana-copy-bot/state/discovery_restore/gap_fill/latest.sqlite`
   - default Helius-specific output: `/var/www/solana-copy-bot/state/discovery_restore/gap_fill_helius/latest.sqlite`

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
CONFIG_PATH=/etc/solana-copy-bot/live.server.toml
APP_ROOT=/var/www/solana-copy-bot
cfg_value() {
  local section="$1"
  local key="$2"
  awk -F'=' -v section="[$section]" -v key="$key" '
    /^\s*\[/ {
      in_section = ($0 == section)
    }
    in_section {
      left = $1
      gsub(/[[:space:]]/, "", left)
      if (left == key) {
        value = substr($0, index($0, "=") + 1)
        sub(/[[:space:]]*#.*/, "", value)
        gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
        gsub(/^"|"$/, "", value)
        print value
        exit
      }
    }
  ' "${CONFIG_PATH}"
}
ts="$(date -u +%Y%m%dT%H%M%SZ)"
cd "${APP_ROOT}/state"
mkdir -p broken
ACTIVE_DB_RAW="$(cfg_value sqlite path)"
ACTIVE_DB="$(python3 - "$APP_ROOT" "$ACTIVE_DB_RAW" <<'PY'
import pathlib
import sys

app_root = pathlib.Path(sys.argv[1])
raw_path = pathlib.Path(sys.argv[2])
if not raw_path.is_absolute():
    raw_path = (app_root / raw_path).resolve()
print(raw_path)
PY
)"
echo "archiving configured sqlite.path=${ACTIVE_DB}"
mv "${ACTIVE_DB}" "broken/$(basename "${ACTIVE_DB}").${ts}" || true
mv "${ACTIVE_DB}-wal" "broken/$(basename "${ACTIVE_DB}").wal.${ts}" || true
mv "${ACTIVE_DB}-shm" "broken/$(basename "${ACTIVE_DB}").shm.${ts}" || true
TARGET_DB="${APP_ROOT}/state/live_runtime_${ts}.db"
```

### 3. Restore into a fresh runtime DB

```bash
CONFIG_PATH=/etc/solana-copy-bot/live.server.toml
APP_ROOT=/var/www/solana-copy-bot
/var/www/solana-copy-bot/target/release/discovery_runtime_restore \
  --config "${CONFIG_PATH}" \
  --artifact "${APP_ROOT}/state/discovery_restore/artifacts/latest.json" \
  --db-path "${TARGET_DB}" \
  --journal-db-path "${APP_ROOT}/state/discovery_restore/recent_raw/latest.sqlite" \
  --json | tee /tmp/discovery_runtime_restore.json
```

If `raw_coverage_satisfied=false` but `journal_covers_artifact_cursor=true`, choose one bounded gap-fill option.

Option A, generic fallback. This uses `getSignaturesForAddress + getTransaction` and stays the conservative baseline path:

```bash
GAP_FILL_SOURCE_URL="$(awk -F'=' '
  /^\s*\[/ {
    in_section = ($0 == "[recent_raw_gap_fill]")
  }
  in_section {
    left = $1
    gsub(/[[:space:]]/, "", left)
    if (left == "helius_http_url") {
      value = substr($0, index($0, "=") + 1)
      sub(/[[:space:]]*#.*/, "", value)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
      gsub(/^"|"$/, "", value)
      print value
      exit
    }
  }
' "${CONFIG_PATH}")"
if [ -z "${GAP_FILL_SOURCE_URL}" ]; then
  echo "recent_raw_gap_fill.helius_http_url is empty in ${CONFIG_PATH}; populate it or pass --helius-http-url explicitly" >&2
  exit 1
fi
/var/www/solana-copy-bot/target/release/discovery_raw_gap_fill \
  --config "${CONFIG_PATH}" \
  --db-path "${TARGET_DB}" \
  --helius-http-url "${GAP_FILL_SOURCE_URL}" \
  --json | tee /tmp/discovery_raw_gap_fill.json
```

Then rerun restore into a new fresh target with the produced generic gap-fill journal:

```bash
TARGET_DB_FILLED="${APP_ROOT}/state/live_runtime_${ts}_gapfill.db"
/var/www/solana-copy-bot/target/release/discovery_runtime_restore \
  --config "${CONFIG_PATH}" \
  --artifact "${APP_ROOT}/state/discovery_restore/artifacts/latest.json" \
  --db-path "${TARGET_DB_FILLED}" \
  --journal-db-path "${APP_ROOT}/state/discovery_restore/recent_raw/latest.sqlite" \
  --gap-fill-db-path "${APP_ROOT}/state/discovery_restore/gap_fill/latest.sqlite" \
  --json | tee /tmp/discovery_runtime_restore.json
TARGET_DB="${TARGET_DB_FILLED}"
```

Option B, Helius-specific historical path. Use this when the generic fallback stays sparse, especially when you need associated token account coverage on the bounded recent window. This path uses Helius `getTransactionsForAddress` directly:

```bash
HELIUS_GAP_FILL_SOURCE_URL="$(awk -F'=' '
  /^\s*\[/ {
    in_section = ($0 == "[recent_raw_gap_fill_helius]")
  }
  in_section {
    left = $1
    gsub(/[[:space:]]/, "", left)
    if (left == "helius_http_url") {
      value = substr($0, index($0, "=") + 1)
      sub(/[[:space:]]*#.*/, "", value)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
      gsub(/^"|"$/, "", value)
      print value
      exit
    }
  }
' "${CONFIG_PATH}")"
if [ -z "${HELIUS_GAP_FILL_SOURCE_URL}" ]; then
  echo "recent_raw_gap_fill_helius.helius_http_url is empty in ${CONFIG_PATH}; populate it or pass --helius-http-url explicitly" >&2
  exit 1
fi
/var/www/solana-copy-bot/target/release/discovery_raw_gap_fill_helius \
  --config "${CONFIG_PATH}" \
  --db-path "${TARGET_DB}" \
  --helius-http-url "${HELIUS_GAP_FILL_SOURCE_URL}" \
  --json | tee /tmp/discovery_raw_gap_fill_helius.json
```

Then rerun restore into a new fresh target with the produced Helius-specific gap-fill journal:

```bash
TARGET_DB_HELIUS_FILLED="${APP_ROOT}/state/live_runtime_${ts}_gapfill_helius.db"
/var/www/solana-copy-bot/target/release/discovery_runtime_restore \
  --config "${CONFIG_PATH}" \
  --artifact "${APP_ROOT}/state/discovery_restore/artifacts/latest.json" \
  --db-path "${TARGET_DB_HELIUS_FILLED}" \
  --journal-db-path "${APP_ROOT}/state/discovery_restore/recent_raw/latest.sqlite" \
  --gap-fill-db-path "${APP_ROOT}/state/discovery_restore/gap_fill_helius/latest.sqlite" \
  --json | tee /tmp/discovery_runtime_restore_helius.json
TARGET_DB="${TARGET_DB_HELIUS_FILLED}"
cp /tmp/discovery_runtime_restore_helius.json /tmp/discovery_runtime_restore.json
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
  --config "${CONFIG_PATH}" \
  --db-path "${TARGET_DB}" \
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
