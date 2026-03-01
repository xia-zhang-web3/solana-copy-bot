#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck source=tools/lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"
cd "$ROOT_DIR"

SKIP_CONTRACT_SMOKE_RAW="${AUDIT_SKIP_CONTRACT_SMOKE:-false}"
if ! skip_contract_smoke="$(parse_bool_token_strict "$SKIP_CONTRACT_SMOKE_RAW")"; then
  echo "AUDIT_SKIP_CONTRACT_SMOKE must be boolean token (true/false/1/0/yes/no/on/off), got: $SKIP_CONTRACT_SMOKE_RAW" >&2
  exit 1
fi

echo "[audit:quick] cargo test -p copybot-executor -q"
cargo test -p copybot-executor -q

if [[ "$skip_contract_smoke" == "false" ]]; then
  echo "[audit:quick] bash tools/executor_contract_smoke_test.sh"
  bash tools/executor_contract_smoke_test.sh
else
  echo "[audit:quick] AUDIT_SKIP_CONTRACT_SMOKE=true -> skipped tools/executor_contract_smoke_test.sh"
fi

echo "[audit:quick] PASS"
