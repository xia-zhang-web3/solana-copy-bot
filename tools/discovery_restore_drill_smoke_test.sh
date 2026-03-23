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
print("discovery_restore_drill_smoke_test: ok")
PY
