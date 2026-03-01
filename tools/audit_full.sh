#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck source=tools/lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"
cd "$ROOT_DIR"

SKIP_OPS_SMOKE_RAW="${AUDIT_SKIP_OPS_SMOKE:-false}"
if ! skip_ops_smoke="$(parse_bool_token_strict "$SKIP_OPS_SMOKE_RAW")"; then
  echo "AUDIT_SKIP_OPS_SMOKE must be boolean token (true/false/1/0/yes/no/on/off), got: $SKIP_OPS_SMOKE_RAW" >&2
  exit 1
fi

SKIP_CONTRACT_SMOKE_RAW="${AUDIT_SKIP_CONTRACT_SMOKE:-$skip_ops_smoke}"
if ! skip_contract_smoke="$(parse_bool_token_strict "$SKIP_CONTRACT_SMOKE_RAW")"; then
  echo "AUDIT_SKIP_CONTRACT_SMOKE must be boolean token (true/false/1/0/yes/no/on/off), got: $SKIP_CONTRACT_SMOKE_RAW" >&2
  exit 1
fi

run_ops_smoke() {
  if command -v timeout >/dev/null 2>&1; then
    timeout 300 bash tools/ops_scripts_smoke_test.sh
    return
  fi
  bash tools/ops_scripts_smoke_test.sh
}

echo "[audit:full] running quick baseline (AUDIT_SKIP_CONTRACT_SMOKE=$skip_contract_smoke)"
AUDIT_SKIP_CONTRACT_SMOKE="$skip_contract_smoke" bash tools/audit_quick.sh

echo "[audit:full] cargo test --workspace -q"
cargo test --workspace -q

if [[ "$skip_ops_smoke" == "false" ]]; then
  echo "[audit:full] tools/ops_scripts_smoke_test.sh"
  run_ops_smoke
else
  echo "[audit:full] AUDIT_SKIP_OPS_SMOKE=true -> skipped tools/ops_scripts_smoke_test.sh"
fi

echo "[audit:full] PASS"
