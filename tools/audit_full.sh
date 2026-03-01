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
SKIP_EXECUTOR_TESTS_RAW="${AUDIT_SKIP_EXECUTOR_TESTS:-false}"
if ! skip_executor_tests="$(parse_bool_token_strict "$SKIP_EXECUTOR_TESTS_RAW")"; then
  echo "AUDIT_SKIP_EXECUTOR_TESTS must be boolean token (true/false/1/0/yes/no/on/off), got: $SKIP_EXECUTOR_TESTS_RAW" >&2
  exit 1
fi
SKIP_WORKSPACE_TESTS_RAW="${AUDIT_SKIP_WORKSPACE_TESTS:-false}"
if ! skip_workspace_tests="$(parse_bool_token_strict "$SKIP_WORKSPACE_TESTS_RAW")"; then
  echo "AUDIT_SKIP_WORKSPACE_TESTS must be boolean token (true/false/1/0/yes/no/on/off), got: $SKIP_WORKSPACE_TESTS_RAW" >&2
  exit 1
fi
OPS_SMOKE_TIMEOUT_RAW="${AUDIT_OPS_SMOKE_TIMEOUT_SEC:-300}"
if ! ops_smoke_timeout_sec="$(parse_u64_token_strict "$OPS_SMOKE_TIMEOUT_RAW")"; then
  echo "AUDIT_OPS_SMOKE_TIMEOUT_SEC must be integer seconds >= 1, got: $OPS_SMOKE_TIMEOUT_RAW" >&2
  exit 1
fi
if [[ "$ops_smoke_timeout_sec" -eq 0 ]]; then
  echo "AUDIT_OPS_SMOKE_TIMEOUT_SEC must be integer seconds >= 1, got: $OPS_SMOKE_TIMEOUT_RAW" >&2
  exit 1
fi
CONTRACT_SMOKE_TIMEOUT_RAW="${AUDIT_CONTRACT_SMOKE_TIMEOUT_SEC:-$ops_smoke_timeout_sec}"
if ! contract_smoke_timeout_sec="$(parse_u64_token_strict "$CONTRACT_SMOKE_TIMEOUT_RAW")"; then
  echo "AUDIT_CONTRACT_SMOKE_TIMEOUT_SEC must be integer seconds >= 1, got: $CONTRACT_SMOKE_TIMEOUT_RAW" >&2
  exit 1
fi
if [[ "$contract_smoke_timeout_sec" -eq 0 ]]; then
  echo "AUDIT_CONTRACT_SMOKE_TIMEOUT_SEC must be integer seconds >= 1, got: $CONTRACT_SMOKE_TIMEOUT_RAW" >&2
  exit 1
fi
WORKSPACE_TEST_TIMEOUT_RAW="${AUDIT_WORKSPACE_TEST_TIMEOUT_SEC:-900}"
if ! workspace_test_timeout_sec="$(parse_u64_token_strict "$WORKSPACE_TEST_TIMEOUT_RAW")"; then
  echo "AUDIT_WORKSPACE_TEST_TIMEOUT_SEC must be integer seconds >= 1, got: $WORKSPACE_TEST_TIMEOUT_RAW" >&2
  exit 1
fi
if [[ "$workspace_test_timeout_sec" -eq 0 ]]; then
  echo "AUDIT_WORKSPACE_TEST_TIMEOUT_SEC must be integer seconds >= 1, got: $WORKSPACE_TEST_TIMEOUT_RAW" >&2
  exit 1
fi
EXECUTOR_TEST_TIMEOUT_RAW="${AUDIT_EXECUTOR_TEST_TIMEOUT_SEC:-$workspace_test_timeout_sec}"
if ! executor_test_timeout_sec="$(parse_u64_token_strict "$EXECUTOR_TEST_TIMEOUT_RAW")"; then
  echo "AUDIT_EXECUTOR_TEST_TIMEOUT_SEC must be integer seconds >= 1, got: $EXECUTOR_TEST_TIMEOUT_RAW" >&2
  exit 1
fi
if [[ "$executor_test_timeout_sec" -eq 0 ]]; then
  echo "AUDIT_EXECUTOR_TEST_TIMEOUT_SEC must be integer seconds >= 1, got: $EXECUTOR_TEST_TIMEOUT_RAW" >&2
  exit 1
fi

run_ops_smoke() {
  if command -v timeout >/dev/null 2>&1; then
    timeout "$ops_smoke_timeout_sec" bash tools/ops_scripts_smoke_test.sh
    return
  fi
  bash tools/ops_scripts_smoke_test.sh
}

run_workspace_tests() {
  if command -v timeout >/dev/null 2>&1; then
    timeout "$workspace_test_timeout_sec" cargo test --workspace -q
    return
  fi
  cargo test --workspace -q
}

echo "[audit:full] running quick baseline (AUDIT_SKIP_CONTRACT_SMOKE=$skip_contract_smoke)"
AUDIT_SKIP_CONTRACT_SMOKE="$skip_contract_smoke" \
AUDIT_CONTRACT_SMOKE_TIMEOUT_SEC="$contract_smoke_timeout_sec" \
AUDIT_SKIP_EXECUTOR_TESTS="$skip_executor_tests" \
AUDIT_EXECUTOR_TEST_TIMEOUT_SEC="$executor_test_timeout_sec" \
  bash tools/audit_quick.sh

if [[ "$skip_workspace_tests" == "false" ]]; then
  echo "[audit:full] cargo test --workspace -q (timeout=${workspace_test_timeout_sec}s)"
  run_workspace_tests
else
  echo "[audit:full] AUDIT_SKIP_WORKSPACE_TESTS=true -> skipped cargo test --workspace -q"
fi

if [[ "$skip_ops_smoke" == "false" ]]; then
  echo "[audit:full] tools/ops_scripts_smoke_test.sh"
  run_ops_smoke
else
  echo "[audit:full] AUDIT_SKIP_OPS_SMOKE=true -> skipped tools/ops_scripts_smoke_test.sh"
fi

echo "[audit:full] PASS"
