#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck source=tools/lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"
cd "$ROOT_DIR"

MAX_AUDIT_TIMEOUT_SEC=86400

SKIP_CONTRACT_SMOKE_RAW="${AUDIT_SKIP_CONTRACT_SMOKE:-false}"
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
CONTRACT_SMOKE_TIMEOUT_RAW="${AUDIT_CONTRACT_SMOKE_TIMEOUT_SEC:-300}"
if ! contract_smoke_timeout_sec="$(parse_timeout_sec_strict "$CONTRACT_SMOKE_TIMEOUT_RAW" 1 "$MAX_AUDIT_TIMEOUT_SEC")"; then
  echo "AUDIT_CONTRACT_SMOKE_TIMEOUT_SEC must be integer seconds >= 1 and <= $MAX_AUDIT_TIMEOUT_SEC, got: $CONTRACT_SMOKE_TIMEOUT_RAW" >&2
  exit 1
fi
EXECUTOR_TEST_TIMEOUT_RAW="${AUDIT_EXECUTOR_TEST_TIMEOUT_SEC:-600}"
if ! executor_test_timeout_sec="$(parse_timeout_sec_strict "$EXECUTOR_TEST_TIMEOUT_RAW" 1 "$MAX_AUDIT_TIMEOUT_SEC")"; then
  echo "AUDIT_EXECUTOR_TEST_TIMEOUT_SEC must be integer seconds >= 1 and <= $MAX_AUDIT_TIMEOUT_SEC, got: $EXECUTOR_TEST_TIMEOUT_RAW" >&2
  exit 1
fi

run_contract_smoke() {
  EXECUTOR_CONTRACT_SMOKE_MODE="$contract_smoke_mode" \
    EXECUTOR_CONTRACT_SMOKE_TARGET_TESTS="$contract_smoke_target_tests" \
    run_with_timeout_if_available "$contract_smoke_timeout_sec" \
    bash tools/executor_contract_smoke_test.sh
}

run_executor_tests() {
  if [[ "$executor_test_mode" == "targeted" ]]; then
    local -a target_entries=()
    local -a normalized_targets=()
    IFS=',' read -r -a target_entries <<<"$executor_test_targets"
    local target_raw=""
    local target_name=""
    for target_raw in "${target_entries[@]-}"; do
      target_name="$(trim_string "$target_raw")"
      if [[ -z "$target_name" ]]; then
        continue
      fi
      normalized_targets+=("$target_name")
    done
    if ((${#normalized_targets[@]} == 0)); then
      echo "AUDIT_EXECUTOR_TEST_TARGETS must contain at least one non-empty target when AUDIT_EXECUTOR_TEST_MODE=targeted" >&2
      exit 1
    fi

    local tests_list_path=""
    tests_list_path="$(mktemp)"
    if ! run_with_timeout_if_available "$executor_test_timeout_sec" \
      cargo test -p copybot-executor -q -- --list >"$tests_list_path"; then
      echo "failed to enumerate executor tests for AUDIT_EXECUTOR_TEST_MODE=targeted" >&2
      rm -f "$tests_list_path"
      exit 1
    fi

    local listed_tests=""
    listed_tests="$(awk '/: test$/ { print }' "$tests_list_path")"
    rm -f "$tests_list_path"
    if [[ -z "$listed_tests" ]]; then
      echo "failed to enumerate executor tests for AUDIT_EXECUTOR_TEST_MODE=targeted" >&2
      exit 1
    fi

    for target_name in "${normalized_targets[@]-}"; do
      if ! grep -Fq -- "$target_name" <<<"$listed_tests"; then
        echo "unknown executor test target in AUDIT_EXECUTOR_TEST_TARGETS: $target_name" >&2
        exit 1
      fi
    done

    for target_name in "${normalized_targets[@]-}"; do
      run_with_timeout_if_available "$executor_test_timeout_sec" \
        cargo test -p copybot-executor -q "$target_name"
    done
    return
  fi
  run_with_timeout_if_available "$executor_test_timeout_sec" \
    cargo test -p copybot-executor -q
}

if [[ "$skip_executor_tests" == "false" ]]; then
  echo "[audit:quick] cargo test -p copybot-executor -q (mode=${executor_test_mode}, timeout=${executor_test_timeout_sec}s)"
  run_executor_tests
else
  echo "[audit:quick] AUDIT_SKIP_EXECUTOR_TESTS=true -> skipped cargo test -p copybot-executor -q"
fi

if [[ "$skip_contract_smoke" == "false" ]]; then
  echo "[audit:quick] tools/executor_contract_smoke_test.sh (mode=${contract_smoke_mode}, timeout=${contract_smoke_timeout_sec}s)"
  run_contract_smoke
else
  echo "[audit:quick] AUDIT_SKIP_CONTRACT_SMOKE=true -> skipped tools/executor_contract_smoke_test.sh"
fi

echo "[audit:quick] PASS"
