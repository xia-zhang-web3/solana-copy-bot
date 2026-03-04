#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck source=tools/lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"
cd "$ROOT_DIR"

MAX_AUDIT_TIMEOUT_SEC=86400

AUDIT_PROFILE_RAW="${AUDIT_PROFILE:-default}"
case "$AUDIT_PROFILE_RAW" in
default | ops_fast)
  audit_profile="$AUDIT_PROFILE_RAW"
  ;;
*)
  echo "AUDIT_PROFILE must be one of: default,ops_fast (got: $AUDIT_PROFILE_RAW)" >&2
  exit 1
  ;;
esac

profile_skip_contract_smoke_default="false"
profile_skip_executor_tests_default="false"
if [[ "$audit_profile" == "ops_fast" ]]; then
  profile_skip_contract_smoke_default="true"
  profile_skip_executor_tests_default="true"
fi

SKIP_CONTRACT_SMOKE_RAW="${AUDIT_SKIP_CONTRACT_SMOKE:-$profile_skip_contract_smoke_default}"
if ! skip_contract_smoke="$(parse_bool_token_strict "$SKIP_CONTRACT_SMOKE_RAW")"; then
  echo "AUDIT_SKIP_CONTRACT_SMOKE must be boolean token (true/false/1/0/yes/no/on/off), got: $SKIP_CONTRACT_SMOKE_RAW" >&2
  exit 1
fi
SKIP_EXECUTOR_TESTS_RAW="${AUDIT_SKIP_EXECUTOR_TESTS:-$profile_skip_executor_tests_default}"
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
    local -a resolved_targets=()
    IFS=',' read -r -a target_entries <<<"$executor_test_targets"
    local target_raw=""
    local target_name=""
    local resolved_target=""
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
    listed_tests="$(awk -F': test$' '/: test$/ { print $1 }' "$tests_list_path")"
    rm -f "$tests_list_path"
    if [[ -z "$listed_tests" ]]; then
      echo "failed to enumerate executor tests for AUDIT_EXECUTOR_TEST_MODE=targeted" >&2
      exit 1
    fi

    for target_name in "${normalized_targets[@]-}"; do
      resolved_target=""
      if grep -Fxq -- "$target_name" <<<"$listed_tests"; then
        resolved_target="$target_name"
      else
        local matches=""
        matches="$(awk -v needle="$target_name" 'index($0, needle) > 0 { print }' <<<"$listed_tests")"
        local match_count=0
        if [[ -n "$matches" ]]; then
          match_count="$(printf '%s\n' "$matches" | sed '/^$/d' | wc -l | tr -d ' ')"
        fi
        if [[ "$match_count" == "0" ]]; then
          echo "unknown executor test target in AUDIT_EXECUTOR_TEST_TARGETS: $target_name" >&2
          exit 1
        fi
        if [[ "$match_count" != "1" ]]; then
          echo "ambiguous executor test target in AUDIT_EXECUTOR_TEST_TARGETS: $target_name" >&2
          echo "matched tests:" >&2
          printf '%s\n' "$matches" >&2
          exit 1
        fi
        resolved_target="$(printf '%s\n' "$matches" | head -n 1)"
      fi

      if grep -Fxq -- "$resolved_target" <<<"$(printf '%s\n' "${resolved_targets[@]-}")"; then
        echo "duplicate executor test target in AUDIT_EXECUTOR_TEST_TARGETS after resolution: $target_name -> $resolved_target" >&2
        exit 1
      fi
      resolved_targets+=("$resolved_target")
    done

    for target_name in "${resolved_targets[@]-}"; do
      run_with_timeout_if_available "$executor_test_timeout_sec" \
        cargo test -p copybot-executor -q "$target_name"
    done
    return
  fi
  run_with_timeout_if_available "$executor_test_timeout_sec" \
    cargo test -p copybot-executor -q
}

if [[ "$skip_executor_tests" == "false" ]]; then
  echo "[audit:quick] cargo test -p copybot-executor -q (profile=${audit_profile}, mode=${executor_test_mode}, timeout=${executor_test_timeout_sec}s)"
  run_executor_tests
else
  echo "[audit:quick] AUDIT_SKIP_EXECUTOR_TESTS=true -> skipped cargo test -p copybot-executor -q"
fi

if [[ "$skip_contract_smoke" == "false" ]]; then
  echo "[audit:quick] tools/executor_contract_smoke_test.sh (profile=${audit_profile}, mode=${contract_smoke_mode}, timeout=${contract_smoke_timeout_sec}s)"
  run_contract_smoke
else
  echo "[audit:quick] AUDIT_SKIP_CONTRACT_SMOKE=true -> skipped tools/executor_contract_smoke_test.sh"
fi

echo "[audit:quick] PASS"
