#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck source=tools/lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"
cd "$ROOT_DIR"

MAX_AUDIT_TIMEOUT_SEC=86400

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
EXECUTOR_TEST_MODE_RAW="${AUDIT_EXECUTOR_TEST_MODE:-full}"
case "$EXECUTOR_TEST_MODE_RAW" in
full | targeted)
  executor_test_mode="$EXECUTOR_TEST_MODE_RAW"
  ;;
*)
  echo "AUDIT_EXECUTOR_TEST_MODE must be one of: full,targeted (got: $EXECUTOR_TEST_MODE_RAW)" >&2
  exit 1
  ;;
esac
executor_test_targets="$(trim_string "${AUDIT_EXECUTOR_TEST_TARGETS:-}")"
if [[ "$executor_test_mode" == "targeted" && -z "$executor_test_targets" ]]; then
  echo "AUDIT_EXECUTOR_TEST_TARGETS must be non-empty when AUDIT_EXECUTOR_TEST_MODE=targeted" >&2
  exit 1
fi
SKIP_WORKSPACE_TESTS_RAW="${AUDIT_SKIP_WORKSPACE_TESTS:-false}"
if ! skip_workspace_tests="$(parse_bool_token_strict "$SKIP_WORKSPACE_TESTS_RAW")"; then
  echo "AUDIT_SKIP_WORKSPACE_TESTS must be boolean token (true/false/1/0/yes/no/on/off), got: $SKIP_WORKSPACE_TESTS_RAW" >&2
  exit 1
fi
CONTRACT_SMOKE_MODE_RAW="${AUDIT_CONTRACT_SMOKE_MODE:-full}"
case "$CONTRACT_SMOKE_MODE_RAW" in
full | targeted)
  contract_smoke_mode="$CONTRACT_SMOKE_MODE_RAW"
  ;;
*)
  echo "AUDIT_CONTRACT_SMOKE_MODE must be one of: full,targeted (got: $CONTRACT_SMOKE_MODE_RAW)" >&2
  exit 1
  ;;
esac
contract_smoke_target_tests="$(trim_string "${AUDIT_CONTRACT_SMOKE_TARGET_TESTS:-}")"
if [[ "$contract_smoke_mode" == "targeted" && -z "$contract_smoke_target_tests" ]]; then
  echo "AUDIT_CONTRACT_SMOKE_TARGET_TESTS must be non-empty when AUDIT_CONTRACT_SMOKE_MODE=targeted" >&2
  exit 1
fi
OPS_SMOKE_MODE_RAW="${AUDIT_OPS_SMOKE_MODE:-full}"
case "$OPS_SMOKE_MODE_RAW" in
full | targeted)
  ops_smoke_mode="$OPS_SMOKE_MODE_RAW"
  ;;
*)
  echo "AUDIT_OPS_SMOKE_MODE must be one of: full,targeted (got: $OPS_SMOKE_MODE_RAW)" >&2
  exit 1
  ;;
esac
ops_smoke_target_cases="$(trim_string "${AUDIT_OPS_SMOKE_TARGET_CASES:-}")"
if [[ "$ops_smoke_mode" == "targeted" && -z "$ops_smoke_target_cases" ]]; then
  echo "AUDIT_OPS_SMOKE_TARGET_CASES must be non-empty when AUDIT_OPS_SMOKE_MODE=targeted" >&2
  exit 1
fi
OPS_SMOKE_PROFILE_RAW="${AUDIT_OPS_SMOKE_PROFILE:-full}"
case "$OPS_SMOKE_PROFILE_RAW" in
full | fast)
  ops_smoke_profile="$OPS_SMOKE_PROFILE_RAW"
  ;;
*)
  echo "AUDIT_OPS_SMOKE_PROFILE must be one of: full,fast (got: $OPS_SMOKE_PROFILE_RAW)" >&2
  exit 1
  ;;
esac
OPS_SMOKE_TIMEOUT_RAW="${AUDIT_OPS_SMOKE_TIMEOUT_SEC:-1200}"
if ! ops_smoke_timeout_sec="$(parse_timeout_sec_strict "$OPS_SMOKE_TIMEOUT_RAW" 1 "$MAX_AUDIT_TIMEOUT_SEC")"; then
  echo "AUDIT_OPS_SMOKE_TIMEOUT_SEC must be integer seconds >= 1 and <= $MAX_AUDIT_TIMEOUT_SEC, got: $OPS_SMOKE_TIMEOUT_RAW" >&2
  exit 1
fi
CONTRACT_SMOKE_TIMEOUT_RAW="${AUDIT_CONTRACT_SMOKE_TIMEOUT_SEC:-$ops_smoke_timeout_sec}"
if ! contract_smoke_timeout_sec="$(parse_timeout_sec_strict "$CONTRACT_SMOKE_TIMEOUT_RAW" 1 "$MAX_AUDIT_TIMEOUT_SEC")"; then
  echo "AUDIT_CONTRACT_SMOKE_TIMEOUT_SEC must be integer seconds >= 1 and <= $MAX_AUDIT_TIMEOUT_SEC, got: $CONTRACT_SMOKE_TIMEOUT_RAW" >&2
  exit 1
fi
WORKSPACE_TEST_TIMEOUT_RAW="${AUDIT_WORKSPACE_TEST_TIMEOUT_SEC:-900}"
if ! workspace_test_timeout_sec="$(parse_timeout_sec_strict "$WORKSPACE_TEST_TIMEOUT_RAW" 1 "$MAX_AUDIT_TIMEOUT_SEC")"; then
  echo "AUDIT_WORKSPACE_TEST_TIMEOUT_SEC must be integer seconds >= 1 and <= $MAX_AUDIT_TIMEOUT_SEC, got: $WORKSPACE_TEST_TIMEOUT_RAW" >&2
  exit 1
fi
EXECUTOR_TEST_TIMEOUT_RAW="${AUDIT_EXECUTOR_TEST_TIMEOUT_SEC:-$workspace_test_timeout_sec}"
if ! executor_test_timeout_sec="$(parse_timeout_sec_strict "$EXECUTOR_TEST_TIMEOUT_RAW" 1 "$MAX_AUDIT_TIMEOUT_SEC")"; then
  echo "AUDIT_EXECUTOR_TEST_TIMEOUT_SEC must be integer seconds >= 1 and <= $MAX_AUDIT_TIMEOUT_SEC, got: $EXECUTOR_TEST_TIMEOUT_RAW" >&2
  exit 1
fi

run_ops_smoke() {
  if [[ "$ops_smoke_mode" == "targeted" ]]; then
    OPS_SMOKE_PROFILE="$ops_smoke_profile" \
    OPS_SMOKE_TARGET_CASES="$ops_smoke_target_cases" \
      run_with_timeout_if_available "$ops_smoke_timeout_sec" \
      bash tools/ops_scripts_smoke_test.sh
    return
  fi
  run_with_timeout_if_available "$ops_smoke_timeout_sec" \
    bash tools/ops_scripts_smoke_test.sh
}

run_workspace_tests() {
  run_with_timeout_if_available "$workspace_test_timeout_sec" \
    cargo test --workspace -q
}

echo "[audit:full] running quick baseline (AUDIT_SKIP_CONTRACT_SMOKE=$skip_contract_smoke)"
AUDIT_SKIP_CONTRACT_SMOKE="$skip_contract_smoke" \
AUDIT_CONTRACT_SMOKE_TIMEOUT_SEC="$contract_smoke_timeout_sec" \
AUDIT_SKIP_EXECUTOR_TESTS="$skip_executor_tests" \
AUDIT_EXECUTOR_TEST_TIMEOUT_SEC="$executor_test_timeout_sec" \
AUDIT_EXECUTOR_TEST_MODE="$executor_test_mode" \
AUDIT_EXECUTOR_TEST_TARGETS="$executor_test_targets" \
AUDIT_CONTRACT_SMOKE_MODE="$contract_smoke_mode" \
AUDIT_CONTRACT_SMOKE_TARGET_TESTS="$contract_smoke_target_tests" \
  bash tools/audit_quick.sh

if [[ "$skip_workspace_tests" == "false" ]]; then
  echo "[audit:full] cargo test --workspace -q (timeout=${workspace_test_timeout_sec}s)"
  run_workspace_tests
else
  echo "[audit:full] AUDIT_SKIP_WORKSPACE_TESTS=true -> skipped cargo test --workspace -q"
fi

if [[ "$skip_ops_smoke" == "false" ]]; then
  echo "[audit:full] tools/ops_scripts_smoke_test.sh (mode=${ops_smoke_mode}, profile=${ops_smoke_profile})"
  run_ops_smoke
else
  echo "[audit:full] AUDIT_SKIP_OPS_SMOKE=true -> skipped tools/ops_scripts_smoke_test.sh"
fi

echo "[audit:full] PASS"
