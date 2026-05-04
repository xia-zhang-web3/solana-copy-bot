#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

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

fail() {
  echo "[architecture:guard] FAIL $*" >&2
  failures=$((failures + 1))
}

is_rust_file() {
  [[ "$1" == *.rs ]]
}

is_test_rust_file_path() {
  case "$1" in
    tests/*.rs | */tests/*.rs | */src/tests.rs | */src/*tests*.rs | */src/*tests*/*.rs | */src/*/tests.rs | */src/*/tests/*.rs)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

is_production_rust_file() {
  local path="$1"
  is_rust_file "$path" || return 1
  [[ "$path" == crates/*/src/* ]] || return 1
  is_test_rust_file_path "$path" && return 1
  return 0
}

line_count() {
  wc -l <"$1" | tr -d ' '
}

limit_for_file() {
  local path="$1"
  if [[ "$path" == */src/bin/*.rs ]]; then
    printf '300'
  elif is_test_rust_file_path "$path"; then
    printf '1000'
  else
    printf '600'
  fi
}

known_oversized_baseline() {
  case "$1" in
    *) return 1 ;;
  esac
}

changed_files() {
  if [[ -n "${ARCH_GUARD_DIFF_RANGE:-}" ]]; then
    git diff --name-only --diff-filter=ACMRTUXB "$ARCH_GUARD_DIFF_RANGE" | sort -u
    return
  fi
  {
    git diff --name-only --diff-filter=ACMRTUXB
    git diff --name-only --cached --diff-filter=ACMRTUXB
    git ls-files --others --exclude-standard
  } | sort -u
}

doc_guard_file() {
  case "$1" in
    README.md)
      return 0
      ;;
    docs/*.md | docs/**/*.md)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

all_files() {
  {
    git ls-files
    git ls-files --others --exclude-standard
  } | sort -u
}

if [[ "$mode" == "--changed" ]]; then
  mapfile -t files < <(changed_files)
else
  mapfile -t files < <(all_files)
fi

check_file_size() {
  local path="$1"
  [[ -f "$path" ]] || return 0
  is_rust_file "$path" || return 0

  local current limit baseline
  current="$(line_count "$path")"
  limit="$(limit_for_file "$path")"

  if baseline="$(known_oversized_baseline "$path")"; then
    if ((current > baseline)); then
      fail "$path has grown above grandfathered baseline ($current > $baseline LOC)"
    fi
    return 0
  fi

  if ((current > limit)); then
    fail "$path exceeds hard size limit ($current > $limit LOC)"
  fi
}

check_inline_tests() {
  local path="$1"
  [[ -f "$path" ]] || return 0
  is_production_rust_file "$path" || return 0

  if ! git cat-file -e "HEAD:$path" 2>/dev/null; then
    if awk '
      /#\[cfg\(test\)\].*mod[[:space:]]+[[:alnum:]_]+[[:space:]]*\{/ { found=1 }
      /#\[cfg\(test\)\]/ { seen_cfg=1; next }
      seen_cfg && /^[[:space:]]*mod[[:space:]]+[[:alnum:]_]+[[:space:]]*\{/ { found=1 }
      seen_cfg && NF && $0 !~ /^[[:space:]]*#/ { seen_cfg=0 }
      END { exit found ? 0 : 1 }
    ' "$path"; then
      fail "$path adds inline #[cfg(test)] mod tests body"
    fi
    return 0
  fi

  if git diff --unified=0 -- "$path" | grep -E '^\+[^+].*mod[[:space:]]+[[:alnum:]_]+[[:space:]]*\{' >/dev/null; then
    fail "$path adds inline mod tests body"
  fi
}

check_new_app_bin() {
  local path="$1"
  [[ "$path" == crates/app/src/bin/*.rs ]] || return 0
  if ! git cat-file -e "HEAD:$path" 2>/dev/null; then
    fail "$path is a new app bin; operators must live outside copybot-app"
  fi
}

check_forbidden_operator_deps() {
  local path="$1"
  [[ "$path" == crates/discovery-v2/Cargo.toml || "$path" == crates/operators/Cargo.toml || "$path" == crates/live-proof/Cargo.toml || "$path" == crates/storage-core/Cargo.toml || "$path" == crates/*-ops/Cargo.toml ]] || return 0
  [[ -f "$path" ]] || return 0
  local forbidden=(
    'copybot-app'
    'copybot-ingestion'
    'copybot-shadow'
    'copybot-discovery'
    'copybot-storage'
  )
  if [[ "$path" != crates/operators/Cargo.toml ]]; then
    forbidden+=(
      'yellowstone-grpc-client'
      'yellowstone-grpc-proto'
      'tonic'
    )
  fi
  local dep
  for dep in "${forbidden[@]}"; do
    if grep -E "^[[:space:]]*$dep[[:space:]]*=" "$path" >/dev/null; then
      fail "$path declares forbidden operator dependency: $dep"
    fi
  done
}

check_doc_build_commands() {
  local path="$1"
  [[ "$path" == *.md ]] || return 0
  doc_guard_file "$path" || return 0
  [[ -f "$path" ]] || return 0

  if [[ "$mode" == "--changed" ]] && git cat-file -e "HEAD:$path" 2>/dev/null; then
    if git diff --unified=0 -- "$path" | grep -E '^\+[^+].*(cargo build --release|CARGO_BUILD_JOBS=.*cargo build|/var/www/.*cargo build)' | grep -Ev 'emergency|fallback|artifact|builder|CI|off production|off-server|Rejected normal rollout pattern' >/dev/null; then
      fail "$path adds production-local cargo build --release wording without emergency/artifact context"
    fi
  elif grep -E '(cargo build --release|CARGO_BUILD_JOBS=.*cargo build|/var/www/.*cargo build)' "$path" | grep -Ev 'emergency|fallback|artifact|builder|CI|off production|off-server|Rejected normal rollout pattern' >/dev/null; then
    fail "$path contains cargo build --release without emergency/artifact context"
  fi
}

check_required_policy_files() {
  local required=(
    BUILD_POLICY.md
    BUILD_REFACTOR_ROADMAP.md
    ARTIFACT_DEPLOY.md
    ARCHITECTURE_WAIVERS.md
  )
  local path
  for path in "${required[@]}"; do
    [[ -f "$path" ]] || fail "missing required architecture file: $path"
  done
  grep -E '^\[profile\.operator-release\]' Cargo.toml >/dev/null || fail "Cargo.toml missing [profile.operator-release]"
  grep -F 'tools/architecture_guard.sh --changed' BUILD_POLICY.md ARTIFACT_DEPLOY.md BUILD_REFACTOR_ROADMAP.md >/dev/null || fail "architecture docs do not mention tools/architecture_guard.sh --changed"
}

check_duplicate_workspace_bins() {
  local line name
  while IFS= read -r line; do
    [[ -n "$line" ]] || continue
    name="${line%% *}"
    fail "duplicate workspace bin name: $line"
  done < <(
    for crate_dir in crates/*; do
      [[ -d "$crate_dir" ]] || continue
      if [[ -f "$crate_dir/Cargo.toml" ]] && grep -q '^\[\[bin\]\]' "$crate_dir/Cargo.toml"; then
        awk -v cargo="$crate_dir/Cargo.toml" '
          /^\[\[bin\]\]/ { in_bin=1; next }
          /^\[/ { in_bin=0 }
          in_bin && /^[[:space:]]*name[[:space:]]*=/ {
            value=$0
            sub(/^[^=]*=[[:space:]]*/, "", value)
            gsub(/"/, "", value)
            gsub(/[[:space:]]/, "", value)
            if (value != "") {
              print value " " cargo
            }
          }
        ' "$crate_dir/Cargo.toml"
      elif [[ -d "$crate_dir/src/bin" ]]; then
        find "$crate_dir/src/bin" -maxdepth 1 -type f -name '*.rs' -print | awk -F/ '{ name=$NF; sub(/\.rs$/, "", name); print name " " $0 }'
      fi
    done | awk '
      {
        name=$1
        count[name]++
        paths[name]=paths[name] " " $2
      }
      END {
        for (name in count) {
          if (count[name] > 1) {
            print name " count=" count[name] paths[name]
          }
        }
      }
    '
  )
}

for path in "${files[@]}"; do
  check_file_size "$path"
  check_inline_tests "$path"
  check_new_app_bin "$path"
  check_forbidden_operator_deps "$path"
  check_doc_build_commands "$path"
done

check_required_policy_files
check_duplicate_workspace_bins

if ((failures > 0)); then
  echo "[architecture:guard] failed with $failures violation(s)" >&2
  exit 1
fi

echo "[architecture:guard] changed files: ${#files[@]}"
echo "[architecture:guard] PASS"
