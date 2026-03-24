#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEMPLATE_PATH="$ROOT_DIR/ops/server_templates/live.server.toml.example"
RUNBOOK_PATH="$ROOT_DIR/ops/discovery_runtime_restore_runbook.md"

cfg_value() {
  local file_path="$1"
  local section="$2"
  local key="$3"
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
  ' "$file_path"
}

[[ "$(cfg_value "$TEMPLATE_PATH" recent_raw_gap_fill source)" == "helius_rpc" ]] || {
  echo "template recent_raw_gap_fill.source must be helius_rpc" >&2
  exit 1
}
[[ -n "$(cfg_value "$TEMPLATE_PATH" recent_raw_gap_fill helius_http_url)" ]] || {
  echo "template recent_raw_gap_fill.helius_http_url must be explicit" >&2
  exit 1
}
[[ -n "$(cfg_value "$TEMPLATE_PATH" recent_raw_gap_fill output_dir)" ]] || {
  echo "template recent_raw_gap_fill.output_dir must be explicit" >&2
  exit 1
}
echo "template recent_raw_gap_fill contract: ok"

python3 - "$RUNBOOK_PATH" <<'PY'
import pathlib
import sys

runbook = pathlib.Path(sys.argv[1]).read_text()
assert "live_runtime_current.db" not in runbook, "runbook still references phantom symlink"
assert "sqlite.path" in runbook, "runbook must archive configured sqlite.path"
assert "recent_raw_gap_fill.helius_http_url" in runbook, "runbook must document gap-fill source contract"
assert "--helius-http-url" in runbook, "runbook must show explicit gap-fill source usage"
assert "TARGET_DB" in runbook, "runbook must document fresh target creation"
assert "--gap-fill-db-path" in runbook, "runbook must document bounded gap-fill replay"
print("runbook operator contract: ok")
PY

echo "discovery_gap_fill_operator_contract_smoke_test: ok"
