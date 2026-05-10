#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

# shellcheck source=tools/lib/architecture_guard/core.sh
source "$SCRIPT_DIR/lib/architecture_guard/core.sh"
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
  mapfile -t files < <(changed_files)
else
  mapfile -t files < <(all_files)
fi

for path in "${files[@]}"; do
  check_file_size "$path"
  check_inline_tests "$path"
  check_include_sharding "$path"
  check_forbidden_new_bin "$path"
  check_forbidden_cargo_bins "$path"
  check_app_dependency_growth "$path"
  check_workspace_dependency_identity_growth "$path"
  check_forbidden_operator_deps "$path"
  check_doc_build_commands "$path"
done

check_required_policy_files
check_duplicate_workspace_bins
check_forbidden_legacy_markers
check_forbidden_dependency_graph

if ((failures > 0)); then
  echo "[architecture:guard] failed with $failures violation(s)" >&2
  exit 1
fi

echo "[architecture:guard] changed files: ${#files[@]}"
echo "[architecture:guard] PASS"
