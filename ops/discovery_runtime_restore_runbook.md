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
4. Program-scoped historical source validation:
   - config block: `program_history_validation`
   - required field: `program_history_validation.http_url`
   - candidate QuickNode-first source contract: `getSlot`, `getBlockTime`, `getBlocks`, `getBlock`
   - runnable tool: `discovery_program_history_source_validate`
   - explicit modes: `--phase phase_a` for cheap source-presence proof, `--phase phase_b` for expensive parseability/usefulness proof
   - built-in QuickNode pacing knobs: `program_history_validation.max_requests_per_second`, `program_history_validation.retry_429_max_attempts`, `program_history_validation.retry_429_backoff_ms`
   - live QuickNode contract currently throttles this path at `125 req/s`; keep the configured limiter below that ceiling
   - default Phase A budget knobs: `program_history_validation.phase_a_max_slots_to_scan`, `program_history_validation.phase_a_sampling_segments`, `program_history_validation.phase_a_max_blocks_per_window`
   - default Phase B parse budget: `program_history_validation.max_slots_to_scan` and `program_history_validation.sampling_segments`
   - Phase A positive means only `viable_enough_for_phase_b`; it is not final source proof and it is not restore-ready proof
   - this is validation-only; it does not restore coverage or enable trading by itself

Systemd wiring:

1. `copybot-discovery-runtime-export.service`
2. `copybot-discovery-runtime-export.timer`
3. `copybot-discovery-recent-raw-snapshot.service`
4. `copybot-discovery-recent-raw-snapshot.timer`
5. `copybot-discovery-recent-raw-snapshot.service` treats exit code `75` as an
   expected transient outcome for snapshot contention; inspect the emitted JSON
   `state`, not just the systemd success bit

Check timers:

```bash
sudo systemctl list-timers 'copybot-discovery-*'
```

Check the latest snapshot outcome:

```bash
journalctl -u copybot-discovery-recent-raw-snapshot.service -n 20 -o cat
```

Snapshot outcome contract:

1. `written`:
   - new archive created
   - latest snapshot surface updated
   - normal healthy timer outcome
2. `self_healed_latest_surface`:
   - latest snapshot or metadata was repaired from archive/latest sqlite
   - acceptable non-fatal outcome
3. `skipped_not_due`:
   - latest snapshot surface is healthy and cadence has not elapsed yet
   - normal healthy timer outcome
4. `deferred`:
   - snapshot hit bounded retryable SQLite contention
   - latest healthy snapshot surface was retained
   - timer may remain enabled and retry on the next run
   - after the practical-completion rollout, this should stay transient rather
     than becoming the steady-state result for every large-journal run
5. `retryable_busy`:
   - snapshot hit bounded retryable SQLite contention
   - there was no healthy latest surface to defer onto, or the run was forced
   - rerun soon or investigate contention before calling the snapshot surface healthy
6. `hard_failure`:
   - non-retryable failure or publish/update failure after the attempt
   - investigate before treating the snapshot timer as healthy

Healthy timer interpretation:

1. `written`, `self_healed_latest_surface`, and `skipped_not_due` are healthy.
2. `deferred` is acceptable as a transient state if it is occasional and the
   latest snapshot surface remains usable.
3. repeated `deferred` with very small `backup_copied_page_count` relative to
   `backup_total_page_count` means the snapshot path is not making meaningful
   forward progress and should be investigated.
4. repeated `retryable_busy` means the timer is not closing the steady-state
   snapshot contract and needs operator follow-up.
5. any `hard_failure` is abnormal.

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

### Optional: QuickNode program-history source validation (validation only)

Run this only if the address-scoped gap-fill paths stay sparse and you need an
explicit verdict on whether the next step should be a program-scoped historical
raw batch.

This command derives the bounded missing window from the current restore state
in `${TARGET_DB}` unless you pass `--window-start/--window-end` explicitly. It
does not replay data into the runtime DB and it does not change restore
readiness by itself.

Operator contract:

1. Run cheap `--phase phase_a` first.
2. Only if Phase A returns `viable_enough_for_phase_b`, escalate into
   `--phase phase_b`.
3. Do not treat a Phase A positive as final source proof.

Phase A is deliberately cheaper on live-sized windows:

1. it uses `phase_a_max_slots_to_scan`, `phase_a_sampling_segments`, and
   `phase_a_max_blocks_per_window`
2. it samples blocks inside each staged slot window instead of doing a full
   parse pass
3. it stops early once it proves target-program presence

If Phase A returns `not_proven_due_to_budget`, the current local validation
budget did not prove or disprove the provider. That is not the same as
`non_viable_source_contract`.

The QuickNode path now paces all `getSlot/getBlockTime/getBlocks/getBlock`
requests through one local limiter and retries transient `429 Too Many
Requests` responses. If retries still exhaust, the tool returns
`not_proven_due_to_provider_throttling`, not `non_viable_source_contract`.

```bash
PROGRAM_HISTORY_VALIDATION_URL="$(awk -F'=' '
  /^\s*\[/ {
    in_section = ($0 == "[program_history_validation]")
  }
  in_section {
    left = $1
    gsub(/[[:space:]]/, "", left)
    if (left == "http_url") {
      value = substr($0, index($0, "=") + 1)
      sub(/[[:space:]]*#.*/, "", value)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
      gsub(/^"|"$/, "", value)
      print value
      exit
    }
  }
' "${CONFIG_PATH}")"
if [ -z "${PROGRAM_HISTORY_VALIDATION_URL}" ]; then
  echo "program_history_validation.http_url is empty in ${CONFIG_PATH}; populate it or pass --http-url explicitly" >&2
  exit 1
fi
/var/www/solana-copy-bot/target/release/discovery_program_history_source_validate \
  --config "${CONFIG_PATH}" \
  --db-path "${TARGET_DB}" \
  --phase phase_a \
  --http-url "${PROGRAM_HISTORY_VALIDATION_URL}" \
  --json | tee /tmp/discovery_program_history_phase_a.json
```

Fields to inspect:

1. `verdict`
2. `reason`
3. `sufficient_for_next_step`
4. `coverage_method`
5. `next_step`
6. `phase_b_required_for_final_source_proof`
7. `final_source_proof_completed`
8. `scan_budget_slots`
9. `budget_exhausted`
10. `requested_window_start`
11. `requested_window_end`
12. `candidate_program_transactions`
13. `parsed_candidate_swap_rows`
14. `missing_segments`

Verdict semantics:

1. `viable_enough_for_phase_b`:
   - Phase A saw target-program historical data inside the bounded window
   - this only means there is reason to pay for Phase B
   - it does not prove parseability/usefulness yet
2. `viable`:
   - Phase B completed without budget exhaustion
   - program-scoped historical raw was observed and parsed into the current swap contract
   - this is the strongest source-proof outcome this tool can give today
3. `not_proven_due_to_budget`:
   - the current local scan budget was too small for the bounded slot span
   - in Phase A this means the cheap presence probe did not prove viability yet
   - in Phase B this means practical parseability/usefulness was not proven yet
4. `not_proven_due_to_sparse_program_history`:
   - the scanned window completed, but it did not yield enough target-program raw to prove the next step
5. `not_proven_due_to_provider_throttling`:
   - the tool respected its local limiter, retried 429s, and still exhausted provider throttling retries
   - this is not a hard provider rejection; lower `max_requests_per_second`, raise `retry_429_backoff_ms`, or rerun later
6. `non_viable_source_contract`:
   - the source contract itself failed to provide usable block-history coverage for the scan path

If Phase A returns `viable_enough_for_phase_b`, escalate explicitly into Phase B:

```bash
/var/www/solana-copy-bot/target/release/discovery_program_history_source_validate \
  --config "${CONFIG_PATH}" \
  --db-path "${TARGET_DB}" \
  --phase phase_b \
  --http-url "${PROGRAM_HISTORY_VALIDATION_URL}" \
  --json | tee /tmp/discovery_program_history_phase_b.json
```

If Phase A or Phase B returns `not_proven_due_to_budget`, you can rerun with a
larger explicit budget. For Phase A, raise `--max-slots-to-scan` and, if
needed, `--max-blocks-per-window`. For Phase B, raise `--max-slots-to-scan` to
at least the reported `slot_span`.

```bash
SLOT_SPAN="$(python3 - <<'PY'
import json
from pathlib import Path

payload = json.loads(Path("/tmp/discovery_program_history_phase_a.json").read_text())
print(payload.get("slot_span") or 0)
PY
)"
/var/www/solana-copy-bot/target/release/discovery_program_history_source_validate \
  --config "${CONFIG_PATH}" \
  --db-path "${TARGET_DB}" \
  --phase phase_a \
  --http-url "${PROGRAM_HISTORY_VALIDATION_URL}" \
  --max-slots-to-scan "${SLOT_SPAN}" \
  --max-blocks-per-window 24 \
  --json | tee /tmp/discovery_program_history_phase_a_expanded.json
```

If the verdict is `not_proven_due_to_provider_throttling`, tune the config in
`/etc/solana-copy-bot/live.server.toml` before rerunning:

1. lower `program_history_validation.max_requests_per_second`
2. raise `program_history_validation.retry_429_backoff_ms`
3. keep `program_history_validation.max_requests_per_second <= 125`

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
