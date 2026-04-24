#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=tools/lib/common.sh
source "$ROOT_DIR/tools/lib/common.sh"

require_bin() {
  local bin="$1"
  if ! command -v "$bin" >/dev/null 2>&1; then
    echo "required binary not found: $bin" >&2
    exit 1
  fi
}

require_bin python3

usage() {
  cat <<'EOF'
usage: tools/discovery_restore_drill.sh --config <path> [--workspace <path>] [--target-db-path <path>] [--artifact-path <path>] [--journal-snapshot-path <path>] [--gap-fill-db-path <path> --gap-fill-progress-path <path> --gap-fill-window-start-utc <rfc3339> --gap-fill-window-end-utc <rfc3339>] [--use-existing-backups] [--report-path <path>] [--now <rfc3339>]
EOF
}

CONFIG_PATH=""
WORKSPACE=""
TARGET_DB_PATH=""
ARTIFACT_PATH=""
JOURNAL_SNAPSHOT_PATH=""
GAP_FILL_DB_PATH=""
GAP_FILL_PROGRESS_PATH=""
GAP_FILL_WINDOW_START_UTC=""
GAP_FILL_WINDOW_END_UTC=""
REPORT_PATH=""
NOW_ARG=""
REFRESH_BACKUPS="true"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)
      CONFIG_PATH="${2:-}"
      shift 2
      ;;
    --workspace)
      WORKSPACE="${2:-}"
      shift 2
      ;;
    --target-db-path)
      TARGET_DB_PATH="${2:-}"
      shift 2
      ;;
    --artifact-path)
      ARTIFACT_PATH="${2:-}"
      shift 2
      ;;
    --journal-snapshot-path)
      JOURNAL_SNAPSHOT_PATH="${2:-}"
      shift 2
      ;;
    --gap-fill-db-path)
      GAP_FILL_DB_PATH="${2:-}"
      shift 2
      ;;
    --gap-fill-progress-path)
      GAP_FILL_PROGRESS_PATH="${2:-}"
      shift 2
      ;;
    --gap-fill-window-start-utc)
      GAP_FILL_WINDOW_START_UTC="${2:-}"
      shift 2
      ;;
    --gap-fill-window-end-utc)
      GAP_FILL_WINDOW_END_UTC="${2:-}"
      shift 2
      ;;
    --report-path)
      REPORT_PATH="${2:-}"
      shift 2
      ;;
    --now)
      NOW_ARG="${2:-}"
      shift 2
      ;;
    --use-existing-backups)
      REFRESH_BACKUPS="false"
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

CONFIG_PATH="$(trim_string "$CONFIG_PATH")"
GAP_FILL_DB_PATH="$(trim_string "$GAP_FILL_DB_PATH")"
GAP_FILL_PROGRESS_PATH="$(trim_string "$GAP_FILL_PROGRESS_PATH")"
GAP_FILL_WINDOW_START_UTC="$(trim_string "$GAP_FILL_WINDOW_START_UTC")"
GAP_FILL_WINDOW_END_UTC="$(trim_string "$GAP_FILL_WINDOW_END_UTC")"
if [[ -z "$CONFIG_PATH" ]]; then
  echo "--config is required" >&2
  exit 1
fi

if [[ ! -f "$CONFIG_PATH" ]]; then
  echo "config file not found: $CONFIG_PATH" >&2
  exit 1
fi

validate_gap_fill_window() {
  python3 - "$1" "$2" <<'PY'
from datetime import datetime, timezone
import sys

def parse(raw):
    return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc)

try:
    start = parse(sys.argv[1].strip())
    end = parse(sys.argv[2].strip())
except Exception:
    sys.exit(1)
if end <= start:
    sys.exit(1)
PY
}

if [[ -n "$GAP_FILL_DB_PATH" ]]; then
  if [[ -z "$GAP_FILL_PROGRESS_PATH" ]]; then
    echo "discovery_restore_drill_gap_fill_gate_missing_progress_path: --gap-fill-progress-path is required when --gap-fill-db-path is supplied" >&2
    exit 1
  fi
  if [[ -z "$GAP_FILL_WINDOW_START_UTC" || -z "$GAP_FILL_WINDOW_END_UTC" ]]; then
    echo "discovery_restore_drill_gap_fill_gate_missing_window_args: --gap-fill-window-start-utc and --gap-fill-window-end-utc are required when --gap-fill-db-path is supplied" >&2
    exit 1
  fi
  if ! validate_gap_fill_window "$GAP_FILL_WINDOW_START_UTC" "$GAP_FILL_WINDOW_END_UTC"; then
    echo "discovery_restore_drill_gap_fill_gate_invalid_window: --gap-fill-window-end-utc must be after --gap-fill-window-start-utc" >&2
    exit 1
  fi
elif [[ -n "$GAP_FILL_PROGRESS_PATH" || -n "$GAP_FILL_WINDOW_START_UTC" || -n "$GAP_FILL_WINDOW_END_UTC" ]]; then
  echo "discovery_restore_drill_gap_fill_gate_missing_db_path: --gap-fill-db-path is required when gap-fill progress or window args are supplied" >&2
  exit 1
fi

resolve_relative_to_config() {
  local path="$1"
  python3 - "$CONFIG_PATH" "$path" <<'PY'
import pathlib
import sys

config_path = pathlib.Path(sys.argv[1]).resolve()
target = pathlib.Path(sys.argv[2])
if target.is_absolute():
    print(target)
else:
    print((config_path.parent / target).resolve())
PY
}

cfg_value_or_default() {
  local dotted_key="$1"
  local default_value="$2"
  local section="${dotted_key%%.*}"
  local key="${dotted_key#*.}"
  local value=""
  value="$(
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
          print value
          exit
        }
      }
    ' "$CONFIG_PATH"
  )"
  value="$(trim_string "$value")"
  if [[ -z "$value" ]]; then
    printf '%s\n' "$default_value"
    return
  fi
  if [[ "$value" =~ ^\".*\"$ ]]; then
    value="${value:1:${#value}-2}"
  fi
  printf '%s\n' "$value"
}

timestamp_slug() {
  local now_arg="$1"
  python3 - "$now_arg" <<'PY'
from datetime import datetime, timezone
import sys

raw = sys.argv[1].strip()
if raw:
    dt = datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc)
else:
    dt = datetime.now(timezone.utc)
print(dt.strftime("%Y%m%dT%H%M%SZ"))
PY
}

run_discovery_bin() {
  local bin="$1"
  shift
  if [[ -n "${COPYBOT_BIN_DIR:-}" && -x "${COPYBOT_BIN_DIR}/${bin}" ]]; then
    "${COPYBOT_BIN_DIR}/${bin}" "$@"
    return
  fi
  if [[ -x "${ROOT_DIR}/target/release/${bin}" ]]; then
    "${ROOT_DIR}/target/release/${bin}" "$@"
    return
  fi
  require_bin cargo
  (cd "$ROOT_DIR" && cargo run -q -p copybot-discovery --bin "$bin" -- "$@")
}

ARTIFACT_DIR="$(resolve_relative_to_config "$(cfg_value_or_default runtime_restore_ops.artifact_dir state/discovery_restore/artifacts)")"
ARTIFACT_CADENCE_MINUTES="$(cfg_value_or_default runtime_restore_ops.artifact_cadence_minutes 10)"
JOURNAL_SNAPSHOT_DIR="$(resolve_relative_to_config "$(cfg_value_or_default runtime_restore_ops.journal_snapshot_dir state/discovery_restore/recent_raw)")"
JOURNAL_SNAPSHOT_CADENCE_MINUTES="$(cfg_value_or_default runtime_restore_ops.journal_snapshot_cadence_minutes 10)"
DRILL_WORKSPACE_ROOT="$(resolve_relative_to_config "$(cfg_value_or_default runtime_restore_ops.drill_workspace_dir state/discovery_restore/drills)")"

if [[ -z "$WORKSPACE" ]]; then
  WORKSPACE="${DRILL_WORKSPACE_ROOT}/$(timestamp_slug "$NOW_ARG")"
fi
mkdir -p "$WORKSPACE"
WORKSPACE="$(cd "$WORKSPACE" && pwd)"

if [[ -z "$ARTIFACT_PATH" ]]; then
  ARTIFACT_PATH="${ARTIFACT_DIR}/latest.json"
fi
if [[ -z "$JOURNAL_SNAPSHOT_PATH" ]]; then
  JOURNAL_SNAPSHOT_PATH="${JOURNAL_SNAPSHOT_DIR}/latest.sqlite"
fi
if [[ -z "$TARGET_DB_PATH" ]]; then
  TARGET_DB_PATH="${WORKSPACE}/restored_runtime.db"
fi
if [[ -z "$REPORT_PATH" ]]; then
  REPORT_PATH="${WORKSPACE}/restore_drill_report.json"
fi

mkdir -p "$(dirname "$TARGET_DB_PATH")"
mkdir -p "$(dirname "$REPORT_PATH")"
rm -f "$TARGET_DB_PATH" "${TARGET_DB_PATH}-wal" "${TARGET_DB_PATH}-shm"

NOW_ARGS=()
if [[ -n "$NOW_ARG" ]]; then
  NOW_ARGS=(--now "$NOW_ARG")
fi

ARTIFACT_EXPORT_JSON_PATH="${WORKSPACE}/artifact_export.json"
JOURNAL_SNAPSHOT_JSON_PATH="${WORKSPACE}/journal_snapshot.json"
RESTORE_JSON_PATH="${WORKSPACE}/restore_output.json"
STATUS_JSON_PATH="${WORKSPACE}/status_output.json"

if [[ "$REFRESH_BACKUPS" == "true" ]]; then
  run_discovery_bin discovery_runtime_export \
    --config "$CONFIG_PATH" \
    --scheduled \
    --force \
    --json \
    "${NOW_ARGS[@]}" >"$ARTIFACT_EXPORT_JSON_PATH"

  run_discovery_bin discovery_recent_raw_snapshot \
    --config "$CONFIG_PATH" \
    --scheduled \
    --force \
    --json \
    "${NOW_ARGS[@]}" >"$JOURNAL_SNAPSHOT_JSON_PATH"
fi

if [[ ! -f "$ARTIFACT_PATH" ]]; then
  echo "artifact path not found: $ARTIFACT_PATH" >&2
  exit 1
fi
if [[ ! -f "$JOURNAL_SNAPSHOT_PATH" ]]; then
  echo "journal snapshot path not found: $JOURNAL_SNAPSHOT_PATH" >&2
  exit 1
fi

START_MS="$(python3 - <<'PY'
import time
print(time.time_ns() // 1_000_000)
PY
)"

if [[ -n "$GAP_FILL_DB_PATH" ]]; then
  run_discovery_bin discovery_runtime_restore \
    --config "$CONFIG_PATH" \
    --artifact "$ARTIFACT_PATH" \
    --db-path "$TARGET_DB_PATH" \
    --journal-db-path "$JOURNAL_SNAPSHOT_PATH" \
    --gap-fill-db-path "$GAP_FILL_DB_PATH" \
    --gap-fill-progress-path "$GAP_FILL_PROGRESS_PATH" \
    --gap-fill-window-start-utc "$GAP_FILL_WINDOW_START_UTC" \
    --gap-fill-window-end-utc "$GAP_FILL_WINDOW_END_UTC" \
    --json \
    "${NOW_ARGS[@]}" >"$RESTORE_JSON_PATH"
else
  run_discovery_bin discovery_runtime_restore \
    --config "$CONFIG_PATH" \
    --artifact "$ARTIFACT_PATH" \
    --db-path "$TARGET_DB_PATH" \
    --journal-db-path "$JOURNAL_SNAPSHOT_PATH" \
    --json \
    "${NOW_ARGS[@]}" >"$RESTORE_JSON_PATH"
fi

run_discovery_bin discovery_status \
  --config "$CONFIG_PATH" \
  --db-path "$TARGET_DB_PATH" \
  --json \
  "${NOW_ARGS[@]}" >"$STATUS_JSON_PATH"

END_MS="$(python3 - <<'PY'
import time
print(time.time_ns() // 1_000_000)
PY
)"
ELAPSED_MS="$((END_MS - START_MS))"

python3 - \
  "$REPORT_PATH" \
  "$CONFIG_PATH" \
  "$WORKSPACE" \
  "$TARGET_DB_PATH" \
  "$ARTIFACT_PATH" \
  "$JOURNAL_SNAPSHOT_PATH" \
  "$GAP_FILL_DB_PATH" \
  "$GAP_FILL_PROGRESS_PATH" \
  "$GAP_FILL_WINDOW_START_UTC" \
  "$GAP_FILL_WINDOW_END_UTC" \
  "$ELAPSED_MS" \
  "$ARTIFACT_CADENCE_MINUTES" \
  "$JOURNAL_SNAPSHOT_CADENCE_MINUTES" \
  "$REFRESH_BACKUPS" \
  "$ARTIFACT_EXPORT_JSON_PATH" \
  "$JOURNAL_SNAPSHOT_JSON_PATH" \
  "$RESTORE_JSON_PATH" \
  "$STATUS_JSON_PATH" <<'PY'
import json
import pathlib
import sys

(
    report_path,
    config_path,
    workspace,
    target_db_path,
    artifact_path,
    journal_snapshot_path,
    gap_fill_db_path,
    gap_fill_progress_path,
    gap_fill_window_start_utc,
    gap_fill_window_end_utc,
    elapsed_ms,
    artifact_cadence_minutes,
    journal_snapshot_cadence_minutes,
    refresh_backups,
    artifact_export_json_path,
    journal_snapshot_json_path,
    restore_json_path,
    status_json_path,
) = sys.argv[1:]

def load_json(path_str):
    path = pathlib.Path(path_str)
    if not path.exists():
        return None
    return json.loads(path.read_text())

restore = load_json(restore_json_path)
status = load_json(status_json_path)
artifact_export = load_json(artifact_export_json_path)
journal_snapshot = load_json(journal_snapshot_json_path)

artifact_cadence_minutes = int(artifact_cadence_minutes)
journal_snapshot_cadence_minutes = int(journal_snapshot_cadence_minutes)
guaranteed_rpo_minutes = max(artifact_cadence_minutes, journal_snapshot_cadence_minutes)

report = {
    "event": "discovery_restore_drill",
    "config_path": str(pathlib.Path(config_path).resolve()),
    "workspace": str(pathlib.Path(workspace).resolve()),
    "target_db_path": str(pathlib.Path(target_db_path).resolve()),
    "artifact_path": str(pathlib.Path(artifact_path).resolve()),
    "journal_snapshot_path": str(pathlib.Path(journal_snapshot_path).resolve()),
    "gap_fill_db_path": str(pathlib.Path(gap_fill_db_path).resolve()) if gap_fill_db_path else None,
    "gap_fill_progress_path": str(pathlib.Path(gap_fill_progress_path).resolve()) if gap_fill_progress_path else None,
    "gap_fill_window_start_utc": gap_fill_window_start_utc or None,
    "gap_fill_window_end_utc": gap_fill_window_end_utc or None,
    "gap_fill_gate_supplied": bool(gap_fill_db_path),
    "refresh_backups": refresh_backups == "true",
    "measured_rto_ms": int(elapsed_ms),
    "measured_rto_seconds": round(int(elapsed_ms) / 1000.0, 3),
    "artifact_cadence_minutes": artifact_cadence_minutes,
    "journal_snapshot_cadence_minutes": journal_snapshot_cadence_minutes,
    "guaranteed_rpo_minutes": guaranteed_rpo_minutes,
    "artifact_export": artifact_export,
    "journal_snapshot": journal_snapshot,
    "restore": restore,
    "status": status,
    "final_verdict": (restore or {}).get("verdict", {}).get("verdict"),
    "final_runtime_mode": (restore or {}).get("verdict", {}).get("runtime_mode"),
    "final_runtime_state": (restore or {}).get("verdict", {}).get("runtime_state"),
    "remaining_failure_modes": [
        "RPO is bounded by the slower of artifact export cadence and journal snapshot cadence.",
        "Restore stays fail-closed if the latest journal snapshot does not cover the artifact cursor or required raw window.",
        "Bootstrap-degraded remains non-trading-ready until fresh raw truth repopulates through the normal runtime path.",
    ],
}

pathlib.Path(report_path).write_text(json.dumps(report, indent=2) + "\n")
print(json.dumps(report, indent=2))
PY

FINAL_VERDICT="$(python3 - "$REPORT_PATH" <<'PY'
import json
import pathlib
import sys

report = json.loads(pathlib.Path(sys.argv[1]).read_text())
print(report.get("final_verdict") or "unknown")
PY
)"

if [[ "$FINAL_VERDICT" != "trading_ready" ]]; then
  echo "restore drill finished with non-trading verdict: $FINAL_VERDICT" >&2
  exit 2
fi
