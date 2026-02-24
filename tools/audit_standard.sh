#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

DIFF_RANGE="${1:-${AUDIT_DIFF_RANGE:-}}"
SKIP_OPS_SMOKE_RAW="${AUDIT_SKIP_OPS_SMOKE:-false}"

normalize_bool() {
  local raw="${1:-}"
  raw="$(printf '%s' "$raw" | tr '[:upper:]' '[:lower:]')"
  case "$raw" in
    1|true|yes|on)
      printf 'true'
      ;;
    *)
      printf 'false'
      ;;
  esac
}

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
  case "$1" in
    adapter) printf 'copybot-adapter' ;;
    app) printf 'copybot-app' ;;
    config) printf 'copybot-config' ;;
    core-types) printf 'copybot-core-types' ;;
    discovery) printf 'copybot-discovery' ;;
    execution) printf 'copybot-execution' ;;
    executor) printf 'copybot-executor' ;;
    ingestion) printf 'copybot-ingestion' ;;
    shadow) printf 'copybot-shadow' ;;
    storage) printf 'copybot-storage' ;;
    *) printf '' ;;
  esac
}

add_unique_package() {
  local package="$1"
  local joined="${CHANGED_PACKAGES[*]-}"
  if [[ " $joined " == *" $package "* ]]; then
    return
  fi
  CHANGED_PACKAGES+=("$package")
}

echo "[audit:standard] running quick baseline"
bash tools/audit_quick.sh

changed_files="$(collect_changed_files || true)"

if [[ -n "$DIFF_RANGE" ]]; then
  echo "[audit:standard] diff range: $DIFF_RANGE"
else
  echo "[audit:standard] diff range: working tree (unstaged + staged + untracked)"
fi

CHANGED_PACKAGES=()
ops_scope_touched="false"
skip_ops_smoke="$(normalize_bool "$SKIP_OPS_SMOKE_RAW")"

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
