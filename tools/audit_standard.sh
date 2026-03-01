#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck source=tools/lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"
cd "$ROOT_DIR"

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

run_ops_smoke() {
  if command -v timeout >/dev/null 2>&1; then
    timeout 300 bash tools/ops_scripts_smoke_test.sh
    return
  fi
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

if ! changed_files="$(collect_changed_files)"; then
  if [[ -n "$DIFF_RANGE" ]]; then
    echo "AUDIT_DIFF_RANGE is invalid: $DIFF_RANGE" >&2
  else
    echo "failed to collect changed files from working tree" >&2
  fi
  exit 1
fi

echo "[audit:standard] running quick baseline"
AUDIT_SKIP_CONTRACT_SMOKE="$skip_contract_smoke" bash tools/audit_quick.sh


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

if [[ "${#CHANGED_PACKAGES[@]}" -eq 0 ]]; then
  echo "[audit:standard] no changed crates detected for targeted package tests"
else
  for package in "${CHANGED_PACKAGES[@]}"; do
    if [[ "$package" == "copybot-executor" ]]; then
      continue
    fi
    echo "[audit:standard] cargo test -p $package -q"
    cargo test -p "$package" -q
  done
fi

if [[ "$ops_scope_touched" == "true" && "$skip_ops_smoke" == "false" ]]; then
  echo "[audit:standard] ops scope touched -> running tools/ops_scripts_smoke_test.sh"
  run_ops_smoke
elif [[ "$ops_scope_touched" == "true" ]]; then
  echo "[audit:standard] ops scope touched but AUDIT_SKIP_OPS_SMOKE=true -> skipped"
else
  echo "[audit:standard] ops scope not touched -> skipped ops smoke"
fi

echo "[audit:standard] PASS"
