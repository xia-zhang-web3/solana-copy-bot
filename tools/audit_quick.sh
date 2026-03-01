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
CONTRACT_SMOKE_TIMEOUT_RAW="${AUDIT_CONTRACT_SMOKE_TIMEOUT_SEC:-300}"
if ! contract_smoke_timeout_sec="$(parse_u64_token_strict "$CONTRACT_SMOKE_TIMEOUT_RAW")"; then
  echo "AUDIT_CONTRACT_SMOKE_TIMEOUT_SEC must be integer seconds >= 1, got: $CONTRACT_SMOKE_TIMEOUT_RAW" >&2
  exit 1
fi
if [[ "$contract_smoke_timeout_sec" -eq 0 ]]; then
  echo "AUDIT_CONTRACT_SMOKE_TIMEOUT_SEC must be integer seconds >= 1, got: $CONTRACT_SMOKE_TIMEOUT_RAW" >&2
  exit 1
fi
EXECUTOR_TEST_TIMEOUT_RAW="${AUDIT_EXECUTOR_TEST_TIMEOUT_SEC:-600}"
if ! executor_test_timeout_sec="$(parse_u64_token_strict "$EXECUTOR_TEST_TIMEOUT_RAW")"; then
  echo "AUDIT_EXECUTOR_TEST_TIMEOUT_SEC must be integer seconds >= 1, got: $EXECUTOR_TEST_TIMEOUT_RAW" >&2
  exit 1
fi
if [[ "$executor_test_timeout_sec" -eq 0 ]]; then
  echo "AUDIT_EXECUTOR_TEST_TIMEOUT_SEC must be integer seconds >= 1, got: $EXECUTOR_TEST_TIMEOUT_RAW" >&2
  exit 1
fi

run_contract_smoke() {
  if command -v timeout >/dev/null 2>&1; then
    timeout "$contract_smoke_timeout_sec" bash tools/executor_contract_smoke_test.sh
    return
  fi
  bash tools/executor_contract_smoke_test.sh
}

run_executor_tests() {
  if command -v timeout >/dev/null 2>&1; then
    timeout "$executor_test_timeout_sec" cargo test -p copybot-executor -q
    return
  fi
  cargo test -p copybot-executor -q
}

echo "[audit:quick] cargo test -p copybot-executor -q (timeout=${executor_test_timeout_sec}s)"
run_executor_tests

if [[ "$skip_contract_smoke" == "false" ]]; then
  echo "[audit:quick] tools/executor_contract_smoke_test.sh (timeout=${contract_smoke_timeout_sec}s)"
  run_contract_smoke
else
  echo "[audit:quick] AUDIT_SKIP_CONTRACT_SMOKE=true -> skipped tools/executor_contract_smoke_test.sh"
fi

echo "[audit:quick] PASS"
