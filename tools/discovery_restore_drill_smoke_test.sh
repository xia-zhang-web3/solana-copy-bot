#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

FIXTURE_DIR="$TMP_DIR/fixture"
REPORT_PATH="$TMP_DIR/report.json"
NOW="2026-03-24T12:00:00Z"

(cd "$ROOT_DIR" && cargo run -q -p copybot-discovery --bin discovery_restore_demo_fixture -- --output-dir "$FIXTURE_DIR" --now "$NOW" --json >/dev/null)

bash "$ROOT_DIR/tools/discovery_restore_drill.sh" \
  --config "$FIXTURE_DIR/drill.toml" \
  --workspace "$TMP_DIR/workspace" \
  --report-path "$REPORT_PATH" \
  --now "$NOW" >/dev/null

python3 - "$REPORT_PATH" <<'PY'
import json
import pathlib
import sys

report = json.loads(pathlib.Path(sys.argv[1]).read_text())
assert report["final_verdict"] == "trading_ready", report
assert report["guaranteed_rpo_minutes"] == 10, report
assert report["measured_rto_ms"] >= 0, report
assert report["gap_fill_gate_supplied"] is False, report
assert report["gap_fill_db_path"] is None, report
assert report["gap_fill_progress_path"] is None, report
assert report["gap_fill_window_start_utc"] is None, report
assert report["gap_fill_window_end_utc"] is None, report
PY

expect_drill_failure_without_target_mutation() {
  local expected_reason="$1"
  local target_db_path="$2"
  shift 2
  local stderr_path="$TMP_DIR/${expected_reason}.stderr"
  if bash "$ROOT_DIR/tools/discovery_restore_drill.sh" "$@" >"$TMP_DIR/${expected_reason}.stdout" 2>"$stderr_path"; then
    echo "expected restore drill failure for ${expected_reason}" >&2
    exit 1
  fi
  if ! grep -q "$expected_reason" "$stderr_path"; then
    echo "missing expected reason ${expected_reason}" >&2
    cat "$stderr_path" >&2
    exit 1
  fi
  if [[ -e "$target_db_path" ]]; then
    echo "invalid drill args created target DB path: $target_db_path" >&2
    exit 1
  fi
  local target_parent
  target_parent="$(dirname "$target_db_path")"
  if [[ -e "$target_parent" ]]; then
    echo "invalid drill args created target DB parent: $target_parent" >&2
    exit 1
  fi
}

TARGET_MISSING_PROGRESS="$TMP_DIR/invalid-target-missing-progress/restored.db"
expect_drill_failure_without_target_mutation \
  "discovery_restore_drill_gap_fill_gate_missing_progress_path" \
  "$TARGET_MISSING_PROGRESS" \
  --config "$FIXTURE_DIR/drill.toml" \
  --workspace "$TMP_DIR/workspace-missing-progress" \
  --target-db-path "$TARGET_MISSING_PROGRESS" \
  --gap-fill-db-path "$TMP_DIR/gap-fill.sqlite" \
  --gap-fill-window-start-utc "2026-04-18T16:56:04Z" \
  --gap-fill-window-end-utc "2026-04-23T15:59:39Z" \
  --now "$NOW"

TARGET_MISSING_DB="$TMP_DIR/invalid-target-missing-db/restored.db"
expect_drill_failure_without_target_mutation \
  "discovery_restore_drill_gap_fill_gate_missing_db_path" \
  "$TARGET_MISSING_DB" \
  --config "$FIXTURE_DIR/drill.toml" \
  --workspace "$TMP_DIR/workspace-missing-db" \
  --target-db-path "$TARGET_MISSING_DB" \
  --gap-fill-progress-path "$TMP_DIR/gap-fill.progress.json" \
  --gap-fill-window-start-utc "2026-04-18T16:56:04Z" \
  --gap-fill-window-end-utc "2026-04-23T15:59:39Z" \
  --now "$NOW"

TARGET_INVALID_WINDOW="$TMP_DIR/invalid-target-window/restored.db"
expect_drill_failure_without_target_mutation \
  "discovery_restore_drill_gap_fill_gate_invalid_window" \
  "$TARGET_INVALID_WINDOW" \
  --config "$FIXTURE_DIR/drill.toml" \
  --workspace "$TMP_DIR/workspace-invalid-window" \
  --target-db-path "$TARGET_INVALID_WINDOW" \
  --gap-fill-db-path "$TMP_DIR/gap-fill.sqlite" \
  --gap-fill-progress-path "$TMP_DIR/gap-fill.progress.json" \
  --gap-fill-window-start-utc "2026-04-23T15:59:39Z" \
  --gap-fill-window-end-utc "2026-04-18T16:56:04Z" \
  --now "$NOW"

python3 - "$REPORT_PATH" <<'PY'
import json
import pathlib
import sys

report = json.loads(pathlib.Path(sys.argv[1]).read_text())
assert report["final_verdict"] == "trading_ready", report
print("discovery_restore_drill_smoke_test: ok")
PY
