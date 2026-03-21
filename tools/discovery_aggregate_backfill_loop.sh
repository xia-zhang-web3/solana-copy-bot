#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck source=tools/lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"
cd "$ROOT_DIR"

usage() {
  cat <<'EOF'
usage: discovery_aggregate_backfill_loop.sh \
  --config <path> \
  [--db-path <path>] \
  --start-ts <rfc3339> \
  --mode <resume|seeded-reset|reset> \
  [--resume-ts <rfc3339> --resume-slot <slot> --resume-signature <sig>] \
  [--max-runs <n>] \
  [--max-runtime-seconds <n>] \
  [--max-batches-per-run <n>] \
  [--batch-size <n>] \
  [--sleep-ms <n>] \
  [--report-dir <path>] \
  [--bin-dir <path>] \
  [--abort-on-runtime-pressure] \
  [--abort-on-runtime-infra-stop]

This operator wrapper repeatedly runs backfill_discovery_scoring in bounded slices,
captures aggregate_readiness_status after each slice, and chains the exact persisted
resume cursor into the next iteration.

Success condition:
  - covered_since != null
  - covered_through_cursor != null
  - materialization_gap_cursor = null
  - backfill_resume_required = false

Failure conditions:
  - backfill_discovery_scoring exits non-zero
  - materialization_gap_cursor becomes non-null
  - persisted backfill cursor stops advancing
  - max runs exhausted without coverage_marked
EOF
}

cfg_value() {
  local section="$1"
  local key="$2"
  awk -F'=' -v section="[$section]" -v key="$key" '
    /^\s*\[/ {
      in_section = ($0 == section)
    }
    in_section {
      line = $0
      sub(/#.*/, "", line)
      left = line
      sub(/=.*/, "", left)
      gsub(/[[:space:]]/, "", left)
      if (left == key) {
        value = line
        sub(/^[^=]*=/, "", value)
        gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
        gsub(/^"|"$/, "", value)
        print value
        exit
      }
    }
  ' "$CONFIG_PATH"
}

resolve_db_path_from_config() {
  local configured_path=""
  configured_path="$(trim_string "$(cfg_value "sqlite" "path")")"
  if [[ -z "$configured_path" ]]; then
    echo "sqlite.path missing in $CONFIG_PATH" >&2
    exit 1
  fi
  if [[ "$configured_path" = /* ]]; then
    printf '%s\n' "$configured_path"
    return
  fi
  local config_dir=""
  config_dir="$(cd "$(dirname "$CONFIG_PATH")" && pwd)"
  printf '%s\n' "$config_dir/$configured_path"
}

require_command() {
  local command_name="$1"
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "required command not found: $command_name" >&2
    exit 1
  fi
}

write_readiness_snapshot() {
  local phase="$1"
  local run_index="$2"
  local prefix
  prefix="$(printf '%03d_%s' "$run_index" "$phase")"
  local human_path="$REPORT_DIR/${prefix}_aggregate_readiness_status.txt"
  local json_path="$REPORT_DIR/${prefix}_aggregate_readiness_status.json"
  "$READINESS_BIN" --config "$CONFIG_PATH" --db-path "$DB_PATH" >"$human_path"
  "$READINESS_BIN" --config "$CONFIG_PATH" --db-path "$DB_PATH" --json >"$json_path"
  printf '%s\n' "$json_path"
}

write_resume_env() {
  local path="$1"
  local mode_value="$2"
  local start_ts_value="$3"
  local resume_ts_value="$4"
  local resume_slot_value="$5"
  local resume_signature_value="$6"
  cat >"$path" <<EOF
MODE=$mode_value
START_TS=$start_ts_value
RESUME_TS=$resume_ts_value
RESUME_SLOT=$resume_slot_value
RESUME_SIGNATURE=$resume_signature_value
EOF
}

CONFIG_PATH=""
DB_PATH=""
START_TS=""
MODE=""
RESUME_TS=""
RESUME_SLOT=""
RESUME_SIGNATURE=""
MAX_RUNS="${MAX_RUNS:-24}"
MAX_RUNTIME_SECONDS="${MAX_RUNTIME_SECONDS:-1800}"
MAX_BATCHES_PER_RUN=""
BATCH_SIZE="${BATCH_SIZE:-10000}"
SLEEP_MS="${SLEEP_MS:-0}"
REPORT_DIR="${REPORT_DIR:-$ROOT_DIR/state/aggregate-backfill-loop-$(date -u +"%Y%m%dT%H%M%SZ")}"
BIN_DIR="${BIN_DIR:-$ROOT_DIR/target/release}"
BACKFILL_BIN="${BACKFILL_BIN:-$BIN_DIR/backfill_discovery_scoring}"
READINESS_BIN="${READINESS_BIN:-$BIN_DIR/aggregate_readiness_status}"
ABORT_ON_RUNTIME_PRESSURE="false"
ABORT_ON_RUNTIME_INFRA_STOP="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)
      CONFIG_PATH="$2"
      shift 2
      ;;
    --db-path)
      DB_PATH="$2"
      shift 2
      ;;
    --start-ts)
      START_TS="$2"
      shift 2
      ;;
    --mode)
      MODE="$2"
      shift 2
      ;;
    --resume-ts)
      RESUME_TS="$2"
      shift 2
      ;;
    --resume-slot)
      RESUME_SLOT="$2"
      shift 2
      ;;
    --resume-signature)
      RESUME_SIGNATURE="$2"
      shift 2
      ;;
    --max-runs)
      MAX_RUNS="$2"
      shift 2
      ;;
    --max-runtime-seconds)
      MAX_RUNTIME_SECONDS="$2"
      shift 2
      ;;
    --max-batches-per-run)
      MAX_BATCHES_PER_RUN="$2"
      shift 2
      ;;
    --batch-size)
      BATCH_SIZE="$2"
      shift 2
      ;;
    --sleep-ms)
      SLEEP_MS="$2"
      shift 2
      ;;
    --report-dir)
      REPORT_DIR="$2"
      shift 2
      ;;
    --bin-dir)
      BIN_DIR="$2"
      BACKFILL_BIN="$BIN_DIR/backfill_discovery_scoring"
      READINESS_BIN="$BIN_DIR/aggregate_readiness_status"
      shift 2
      ;;
    --abort-on-runtime-pressure)
      ABORT_ON_RUNTIME_PRESSURE="true"
      shift
      ;;
    --abort-on-runtime-infra-stop)
      ABORT_ON_RUNTIME_INFRA_STOP="true"
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "$CONFIG_PATH" || -z "$START_TS" || -z "$MODE" ]]; then
  usage >&2
  exit 1
fi

case "$MODE" in
  resume|seeded-reset|reset)
    ;;
  *)
    echo "--mode must be one of: resume, seeded-reset, reset (got: $MODE)" >&2
    exit 1
    ;;
esac

if [[ "$MODE" == "reset" ]]; then
  if [[ -n "$RESUME_TS$RESUME_SLOT$RESUME_SIGNATURE" ]]; then
    echo "--mode reset does not accept --resume-*" >&2
    exit 1
  fi
else
  if [[ -z "$RESUME_TS" || -z "$RESUME_SLOT" || -z "$RESUME_SIGNATURE" ]]; then
    echo "--mode $MODE requires the full exact resume triple: --resume-ts --resume-slot --resume-signature" >&2
    exit 1
  fi
fi

if ! MAX_RUNS="$(parse_positive_u64_token_strict "$MAX_RUNS")"; then
  echo "--max-runs must be >= 1" >&2
  exit 1
fi
if ! MAX_RUNTIME_SECONDS="$(parse_positive_u64_token_strict "$MAX_RUNTIME_SECONDS")"; then
  echo "--max-runtime-seconds must be >= 1" >&2
  exit 1
fi
if ! BATCH_SIZE="$(parse_positive_u64_token_strict "$BATCH_SIZE")"; then
  echo "--batch-size must be >= 1" >&2
  exit 1
fi
if ! SLEEP_MS="$(parse_u64_token_strict "$SLEEP_MS")"; then
  echo "--sleep-ms must be >= 0" >&2
  exit 1
fi
if [[ -n "$MAX_BATCHES_PER_RUN" ]]; then
  if ! MAX_BATCHES_PER_RUN="$(parse_positive_u64_token_strict "$MAX_BATCHES_PER_RUN")"; then
    echo "--max-batches-per-run must be >= 1" >&2
    exit 1
  fi
fi

require_command jq

CONFIG_PATH="$(cd "$(dirname "$CONFIG_PATH")" && pwd)/$(basename "$CONFIG_PATH")"
if [[ -z "$DB_PATH" ]]; then
  DB_PATH="$(resolve_db_path_from_config)"
fi

if [[ ! -x "$BACKFILL_BIN" ]]; then
  echo "backfill binary is missing or not executable: $BACKFILL_BIN" >&2
  exit 1
fi
if [[ ! -x "$READINESS_BIN" ]]; then
  echo "readiness binary is missing or not executable: $READINESS_BIN" >&2
  exit 1
fi

mkdir -p "$REPORT_DIR"

current_mode="$MODE"
current_resume_ts="$RESUME_TS"
current_resume_slot="$RESUME_SLOT"
current_resume_signature="$RESUME_SIGNATURE"
previous_progress_cursor_key=""
final_verdict="max_runs_exhausted"

initial_readiness_json="$(write_readiness_snapshot "initial" 0)"
write_resume_env \
  "$REPORT_DIR/next_resume.env" \
  "$current_mode" \
  "$START_TS" \
  "${current_resume_ts:-}" \
  "${current_resume_slot:-}" \
  "${current_resume_signature:-}"

echo "[aggregate-backfill-loop] report_dir=$REPORT_DIR"
echo "[aggregate-backfill-loop] initial_readiness_json=$initial_readiness_json"

for ((run_index = 1; run_index <= MAX_RUNS; run_index++)); do
  run_prefix="$(printf '%03d' "$run_index")"
  run_log="$REPORT_DIR/${run_prefix}_backfill.log"
  run_cmd_file="$REPORT_DIR/${run_prefix}_backfill_command.txt"

  cmd=(
    "$BACKFILL_BIN"
    "$DB_PATH"
    --config "$CONFIG_PATH"
    --start-ts "$START_TS"
    --mark-covered
    --batch-size "$BATCH_SIZE"
    --sleep-ms "$SLEEP_MS"
    --max-runtime-seconds "$MAX_RUNTIME_SECONDS"
  )

  if [[ -n "$MAX_BATCHES_PER_RUN" ]]; then
    cmd+=(--max-batches-per-run "$MAX_BATCHES_PER_RUN")
  fi
  if [[ "$ABORT_ON_RUNTIME_PRESSURE" == "true" ]]; then
    cmd+=(--abort-on-runtime-pressure)
  fi
  if [[ "$ABORT_ON_RUNTIME_INFRA_STOP" == "true" ]]; then
    cmd+=(--abort-on-runtime-infra-stop)
  fi

  case "$current_mode" in
    reset)
      cmd+=(--reset)
      ;;
    resume)
      cmd+=(
        --resume-ts "$current_resume_ts"
        --resume-slot "$current_resume_slot"
        --resume-signature "$current_resume_signature"
      )
      ;;
    seeded-reset)
      cmd+=(
        --seeded-reset
        --resume-ts "$current_resume_ts"
        --resume-slot "$current_resume_slot"
        --resume-signature "$current_resume_signature"
      )
      ;;
  esac

  printf '%q ' "${cmd[@]}" >"$run_cmd_file"
  printf '\n' >>"$run_cmd_file"

  echo "[aggregate-backfill-loop] run=$run_index mode=$current_mode start_ts=$START_TS log=$run_log"
  set +e
  "${cmd[@]}" >"$run_log" 2>&1
  run_exit_code=$?
  set -e

  readiness_json="$(write_readiness_snapshot "post_run" "$run_index")"
  covered_since="$(jq -r '.covered_since // empty' "$readiness_json")"
  covered_through_ts="$(jq -r '.covered_through_ts // empty' "$readiness_json")"
  covered_through_cursor_ts="$(jq -r '.covered_through_cursor.ts_utc // empty' "$readiness_json")"
  covered_through_cursor_slot="$(jq -r '.covered_through_cursor.slot // empty' "$readiness_json")"
  covered_through_cursor_signature="$(jq -r '.covered_through_cursor.signature // empty' "$readiness_json")"
  gap_cursor_ts="$(jq -r '.materialization_gap_cursor.ts_utc // empty' "$readiness_json")"
  gap_cursor_slot="$(jq -r '.materialization_gap_cursor.slot // empty' "$readiness_json")"
  gap_cursor_signature="$(jq -r '.materialization_gap_cursor.signature // empty' "$readiness_json")"
  backfill_resume_required="$(jq -r '.backfill_resume_required' "$readiness_json")"
  backfill_progress_start_ts="$(jq -r '.backfill_progress.start_ts // empty' "$readiness_json")"
  backfill_progress_cursor_ts="$(jq -r '.backfill_progress.cursor.ts_utc // empty' "$readiness_json")"
  backfill_progress_cursor_slot="$(jq -r '.backfill_progress.cursor.slot // empty' "$readiness_json")"
  backfill_progress_cursor_signature="$(jq -r '.backfill_progress.cursor.signature // empty' "$readiness_json")"
  effective_writes_ready="$(jq -r '.effective_writes_ready' "$readiness_json")"
  effective_reads_ready="$(jq -r '.effective_reads_ready' "$readiness_json")"

  echo "[aggregate-backfill-loop] run=$run_index exit_code=$run_exit_code covered_since=${covered_since:-null} covered_through_ts=${covered_through_ts:-null} resume_required=$backfill_resume_required writes_ready=$effective_writes_ready reads_ready=$effective_reads_ready"

  if [[ -n "$gap_cursor_ts" ]]; then
    final_verdict="gap_latched"
    echo "[aggregate-backfill-loop] FAILURE gap cursor latched at ${gap_cursor_ts}/${gap_cursor_slot}/${gap_cursor_signature}" >&2
    break
  fi

  if [[ "$run_exit_code" != "0" ]]; then
    final_verdict="run_failed"
    echo "[aggregate-backfill-loop] FAILURE backfill exited non-zero; inspect $run_log" >&2
    break
  fi

  if [[ -n "$covered_since" && -n "$covered_through_cursor_ts" && -n "$covered_through_cursor_slot" && -n "$covered_through_cursor_signature" && "$backfill_resume_required" == "false" && -z "$backfill_progress_cursor_ts" ]]; then
    final_verdict="coverage_marked"
    write_resume_env \
      "$REPORT_DIR/next_resume.env" \
      "resume" \
      "$START_TS" \
      "$covered_through_cursor_ts" \
      "$covered_through_cursor_slot" \
      "$covered_through_cursor_signature"
    echo "[aggregate-backfill-loop] SUCCESS coverage marked"
    break
  fi

  if [[ -z "$backfill_progress_cursor_ts" || -z "$backfill_progress_cursor_slot" || -z "$backfill_progress_cursor_signature" ]]; then
    final_verdict="missing_progress_cursor"
    write_resume_env \
      "$REPORT_DIR/next_resume.env" \
      "$current_mode" \
      "$START_TS" \
      "${current_resume_ts:-}" \
      "${current_resume_slot:-}" \
      "${current_resume_signature:-}"
    echo "[aggregate-backfill-loop] FAILURE run finished without coverage markers and without persisted backfill progress" >&2
    break
  fi

  progress_cursor_key="${backfill_progress_start_ts}|${backfill_progress_cursor_ts}|${backfill_progress_cursor_slot}|${backfill_progress_cursor_signature}"
  if [[ -n "$previous_progress_cursor_key" && "$progress_cursor_key" == "$previous_progress_cursor_key" ]]; then
    final_verdict="progress_stalled"
    write_resume_env \
      "$REPORT_DIR/next_resume.env" \
      "resume" \
      "$START_TS" \
      "$backfill_progress_cursor_ts" \
      "$backfill_progress_cursor_slot" \
      "$backfill_progress_cursor_signature"
    echo "[aggregate-backfill-loop] FAILURE persisted backfill cursor did not advance" >&2
    break
  fi
  previous_progress_cursor_key="$progress_cursor_key"
  current_mode="resume"
  current_resume_ts="$backfill_progress_cursor_ts"
  current_resume_slot="$backfill_progress_cursor_slot"
  current_resume_signature="$backfill_progress_cursor_signature"
  write_resume_env \
    "$REPORT_DIR/next_resume.env" \
    "$current_mode" \
    "$START_TS" \
    "$current_resume_ts" \
    "$current_resume_slot" \
    "$current_resume_signature"
done

summary_path="$REPORT_DIR/summary.txt"
cat >"$summary_path" <<EOF
verdict=$final_verdict
config_path=$CONFIG_PATH
db_path=$DB_PATH
start_ts=$START_TS
mode=$MODE
max_runs=$MAX_RUNS
max_runtime_seconds=$MAX_RUNTIME_SECONDS
max_batches_per_run=${MAX_BATCHES_PER_RUN:-}
batch_size=$BATCH_SIZE
sleep_ms=$SLEEP_MS
backfill_bin=$BACKFILL_BIN
readiness_bin=$READINESS_BIN
next_resume_env=$REPORT_DIR/next_resume.env
EOF

echo "[aggregate-backfill-loop] summary=$summary_path verdict=$final_verdict"

if [[ "$final_verdict" != "coverage_marked" ]]; then
  exit 1
fi
