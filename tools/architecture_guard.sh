#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

# shellcheck source=tools/lib/architecture_guard/core.sh
source "$SCRIPT_DIR/lib/architecture_guard/core.sh"
# shellcheck source=tools/lib/architecture_guard/diff_cache.sh
source "$SCRIPT_DIR/lib/architecture_guard/diff_cache.sh"
# shellcheck source=tools/lib/architecture_guard/file_checks.sh
source "$SCRIPT_DIR/lib/architecture_guard/file_checks.sh"
# shellcheck source=tools/lib/architecture_guard/dependency_checks.sh
source "$SCRIPT_DIR/lib/architecture_guard/dependency_checks.sh"
# shellcheck source=tools/lib/architecture_guard/workspace_checks.sh
source "$SCRIPT_DIR/lib/architecture_guard/workspace_checks.sh"

mode="${1:---changed}"
case "$mode" in
  --changed | --all) ;;
  *)
    echo "usage: tools/architecture_guard.sh [--changed|--all]" >&2
    exit 2
    ;;
esac

failures=0
echo "[architecture:guard] mode=${mode#--}"

validate_diff_range
if ((failures > 0)); then
  echo "[architecture:guard] failed with $failures violation(s)" >&2
  exit 1
fi

if [[ "$mode" == "--changed" ]]; then
  selected_files="$(changed_files)"
else
  selected_files="$(all_files)"
fi
files=()
while IFS= read -r path; do
  [[ -n "$path" ]] && files+=("$path")
done <<< "$selected_files"
file_count=0
for path in ${files[@]+"${files[@]}"}; do
  file_count=$((file_count + 1))
done
if ((file_count == 0)); then
  files=("__architecture_guard_no_changed_files__")
fi

if [[ "$mode" == "--changed" ]]; then
  prepare_diff_cache
fi
check_file_batch
for path in "${files[@]}"; do
  check_app_dependency_growth "$path"
  check_workspace_dependency_identity_growth "$path"
  check_forbidden_operator_deps "$path"
  check_doc_build_commands "$path"
done

check_required_policy_files
prepare_workspace_metadata
check_duplicate_workspace_bins
check_forbidden_legacy_markers
check_forbidden_dependency_graph

if ((failures > 0)); then
  echo "[architecture:guard] failed with $failures violation(s)" >&2
  exit 1
fi

echo "[architecture:guard] changed files: $file_count"
echo "[architecture:guard] PASS"
