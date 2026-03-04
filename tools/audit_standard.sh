#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck source=tools/lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"
cd "$ROOT_DIR"

MAX_AUDIT_TIMEOUT_SEC=86400

DIFF_RANGE="${1:-${AUDIT_DIFF_RANGE:-}}"
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
SKIP_PACKAGE_TESTS_RAW="${AUDIT_SKIP_PACKAGE_TESTS:-false}"
if ! skip_package_tests="$(parse_bool_token_strict "$SKIP_PACKAGE_TESTS_RAW")"; then
  echo "AUDIT_SKIP_PACKAGE_TESTS must be boolean token (true/false/1/0/yes/no/on/off), got: $SKIP_PACKAGE_TESTS_RAW" >&2
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
full | targeted | targeted_fast | auto)
  ops_smoke_mode="$OPS_SMOKE_MODE_RAW"
  ;;
*)
  echo "AUDIT_OPS_SMOKE_MODE must be one of: full,targeted,targeted_fast,auto (got: $OPS_SMOKE_MODE_RAW)" >&2
  exit 1
  ;;
esac
ops_smoke_profile_default="full"
ops_smoke_target_cases_default=""
if [[ "$ops_smoke_mode" == "targeted_fast" ]]; then
  ops_smoke_mode="targeted"
  ops_smoke_profile_default="fast"
  ops_smoke_target_cases_default="heavy_runtime_chain"
fi
OPS_SMOKE_PRESET_RAW="$(trim_string "${AUDIT_OPS_SMOKE_PRESET:-}")"
case "$OPS_SMOKE_PRESET_RAW" in
"" | common_parsers | heavy_runtime_chain | audit_guardpack)
  ops_smoke_preset="$OPS_SMOKE_PRESET_RAW"
  ;;
*)
  echo "AUDIT_OPS_SMOKE_PRESET must be one of: common_parsers,heavy_runtime_chain,audit_guardpack (got: $OPS_SMOKE_PRESET_RAW)" >&2
  exit 1
  ;;
esac
if [[ "$ops_smoke_mode" == "auto" ]]; then
  if [[ -n "$ops_smoke_preset" || -n "$(trim_string "${AUDIT_OPS_SMOKE_TARGET_CASES:-}")" ]]; then
    ops_smoke_mode="targeted"
  else
    ops_smoke_mode="full"
  fi
fi
ops_smoke_target_cases="$(trim_string "${AUDIT_OPS_SMOKE_TARGET_CASES:-}")"
if [[ "$ops_smoke_mode" == "full" ]]; then
  if [[ -n "$ops_smoke_target_cases" ]]; then
    echo "AUDIT_OPS_SMOKE_TARGET_CASES can be used only when AUDIT_OPS_SMOKE_MODE=targeted|auto" >&2
    exit 1
  fi
  if [[ -n "$ops_smoke_preset" ]]; then
    echo "AUDIT_OPS_SMOKE_PRESET can be used only when AUDIT_OPS_SMOKE_MODE=targeted|auto" >&2
    exit 1
  fi
fi
if [[ "$ops_smoke_mode" == "targeted" ]]; then
  if [[ -n "$ops_smoke_target_cases" && -n "$ops_smoke_preset" ]]; then
    echo "AUDIT_OPS_SMOKE_TARGET_CASES and AUDIT_OPS_SMOKE_PRESET cannot both be set when AUDIT_OPS_SMOKE_MODE=targeted" >&2
    exit 1
  fi
  if [[ -z "$ops_smoke_target_cases" && -n "$ops_smoke_preset" ]]; then
    ops_smoke_target_cases="$ops_smoke_preset"
  fi
  if [[ -z "$ops_smoke_target_cases" && -z "$ops_smoke_preset" && -n "$ops_smoke_target_cases_default" ]]; then
    ops_smoke_target_cases="$ops_smoke_target_cases_default"
  fi
  if [[ -z "$ops_smoke_target_cases" ]]; then
    echo "AUDIT_OPS_SMOKE_TARGET_CASES must be non-empty when AUDIT_OPS_SMOKE_MODE=targeted" >&2
    exit 1
  fi
fi
OPS_SMOKE_PROFILE_RAW="${AUDIT_OPS_SMOKE_PROFILE:-$ops_smoke_profile_default}"
case "$OPS_SMOKE_PROFILE_RAW" in
full | fast | auto)
  ops_smoke_profile="$OPS_SMOKE_PROFILE_RAW"
  ;;
*)
  echo "AUDIT_OPS_SMOKE_PROFILE must be one of: full,fast,auto (got: $OPS_SMOKE_PROFILE_RAW)" >&2
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
PACKAGE_TEST_TIMEOUT_RAW="${AUDIT_PACKAGE_TEST_TIMEOUT_SEC:-600}"
if ! package_test_timeout_sec="$(parse_timeout_sec_strict "$PACKAGE_TEST_TIMEOUT_RAW" 1 "$MAX_AUDIT_TIMEOUT_SEC")"; then
  echo "AUDIT_PACKAGE_TEST_TIMEOUT_SEC must be integer seconds >= 1 and <= $MAX_AUDIT_TIMEOUT_SEC, got: $PACKAGE_TEST_TIMEOUT_RAW" >&2
  exit 1
fi
EXECUTOR_TEST_TIMEOUT_RAW="${AUDIT_EXECUTOR_TEST_TIMEOUT_SEC:-$package_test_timeout_sec}"
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

collect_changed_files() {
  if [[ -n "$DIFF_RANGE" ]]; then
    git diff --name-only "$DIFF_RANGE" | sed '/^$/d' | sort -u
    return
  fi

  {
    git diff --name-only
    git diff --name-only --cached
    git ls-files --others --exclude-standard
  } | sed '/^$/d' | sort -u
}

package_for_crate() {
  local crate_name="$1"
  local manifest_path="$ROOT_DIR/crates/$crate_name/Cargo.toml"

  if [[ ! -f "$manifest_path" ]]; then
    printf ''
    return
  fi

  awk '
    /^\[package\]/ { in_package = 1; next }
    /^\[/ { if (in_package) exit }
    in_package && /^[[:space:]]*name[[:space:]]*=/ {
      line = $0
      sub(/^[[:space:]]*name[[:space:]]*=[[:space:]]*"/, "", line)
      sub(/".*$/, "", line)
      print line
      exit
    }
  ' "$manifest_path"
}

add_unique_package() {
  local package="$1"
  local joined="${CHANGED_PACKAGES[*]-}"
  if [[ " $joined " == *" $package "* ]]; then
    return
  fi
  CHANGED_PACKAGES+=("$package")
}

run_package_tests() {
  local package="$1"
  run_with_timeout_if_available "$package_test_timeout_sec" \
    cargo test -p "$package" -q
}

if ! changed_files="$(collect_changed_files)"; then
  if [[ -n "$DIFF_RANGE" ]]; then
    echo "AUDIT_DIFF_RANGE is invalid: $DIFF_RANGE" >&2
  else
    echo "failed to collect changed files from working tree" >&2
  fi
  exit 1
fi

echo "[audit:standard] running quick baseline"
AUDIT_SKIP_CONTRACT_SMOKE="$skip_contract_smoke" \
AUDIT_CONTRACT_SMOKE_TIMEOUT_SEC="$contract_smoke_timeout_sec" \
AUDIT_SKIP_EXECUTOR_TESTS="$skip_executor_tests" \
AUDIT_EXECUTOR_TEST_TIMEOUT_SEC="$executor_test_timeout_sec" \
AUDIT_EXECUTOR_TEST_MODE="$executor_test_mode" \
AUDIT_EXECUTOR_TEST_TARGETS="$executor_test_targets" \
AUDIT_CONTRACT_SMOKE_MODE="$contract_smoke_mode" \
AUDIT_CONTRACT_SMOKE_TARGET_TESTS="$contract_smoke_target_tests" \
  bash tools/audit_quick.sh


if [[ -n "$DIFF_RANGE" ]]; then
  echo "[audit:standard] diff range: $DIFF_RANGE"
else
  echo "[audit:standard] diff range: working tree (unstaged + staged + untracked)"
fi

CHANGED_PACKAGES=()
ops_scope_touched="false"

while IFS= read -r path; do
  [[ -z "$path" ]] && continue
  case "$path" in
    crates/*)
      crate_name="${path#crates/}"
      crate_name="${crate_name%%/*}"
      package_name="$(package_for_crate "$crate_name")"
      if [[ -n "$package_name" ]]; then
        add_unique_package "$package_name"
      fi
      ;;
  esac

  case "$path" in
    tools/*|ops/*|README.md|ROAD_TO_PRODUCTION.md)
      ops_scope_touched="true"
      ;;
  esac
done <<<"$changed_files"

if [[ "$skip_package_tests" == "true" ]]; then
  echo "[audit:standard] AUDIT_SKIP_PACKAGE_TESTS=true -> skipped changed package tests"
elif [[ "${#CHANGED_PACKAGES[@]}" -eq 0 ]]; then
  echo "[audit:standard] no changed crates detected for targeted package tests"
else
  for package in "${CHANGED_PACKAGES[@]}"; do
    if [[ "$package" == "copybot-executor" ]]; then
      continue
    fi
    echo "[audit:standard] cargo test -p $package -q (timeout=${package_test_timeout_sec}s)"
    run_package_tests "$package"
  done
fi

if [[ "$skip_ops_smoke" == "true" ]]; then
  if [[ "$ops_smoke_mode" == "targeted" ]]; then
    echo "[audit:standard] targeted ops-smoke requested but AUDIT_SKIP_OPS_SMOKE=true -> skipped (mode=${ops_smoke_mode}, profile=${ops_smoke_profile}, preset=${ops_smoke_preset:-n/a}) cases=${ops_smoke_target_cases:-n/a}"
  elif [[ "$ops_scope_touched" == "true" ]]; then
    echo "[audit:standard] ops scope touched but AUDIT_SKIP_OPS_SMOKE=true -> skipped"
  else
    echo "[audit:standard] ops scope not touched -> skipped ops smoke"
  fi
elif [[ "$ops_smoke_mode" == "targeted" ]]; then
  echo "[audit:standard] targeted ops-smoke requested -> running tools/ops_scripts_smoke_test.sh (mode=${ops_smoke_mode}, profile=${ops_smoke_profile}, preset=${ops_smoke_preset:-n/a}) cases=${ops_smoke_target_cases:-n/a}"
  run_ops_smoke
elif [[ "$ops_scope_touched" == "true" ]]; then
  echo "[audit:standard] ops scope touched -> running tools/ops_scripts_smoke_test.sh (mode=${ops_smoke_mode}, profile=${ops_smoke_profile}, preset=${ops_smoke_preset:-n/a}) cases=${ops_smoke_target_cases:-n/a}"
  run_ops_smoke
else
  echo "[audit:standard] ops scope not touched -> skipped ops smoke"
fi

echo "[audit:standard] PASS"
