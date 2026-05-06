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
    tests/*.rs | */tests/*.rs | */src/tests.rs | */src/*_tests.rs | */src/*_tests/*.rs | */src/*/tests.rs | */src/*/tests/*.rs | */src/*/tests_*.rs)
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
    printf '800'
  elif [[ "$path" == *.md ]]; then
    printf '800'
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

baseline_ref() {
  if [[ -n "${ARCH_GUARD_DIFF_RANGE:-}" ]]; then
    printf '%s' "${ARCH_GUARD_DIFF_RANGE%%..*}"
  else
    printf 'HEAD'
  fi
}

baseline_file_exists() {
  git cat-file -e "$(baseline_ref):$1" 2>/dev/null
}

baseline_file_content() {
  git show "$(baseline_ref):$1"
}

validate_diff_range() {
  [[ -n "${ARCH_GUARD_DIFF_RANGE:-}" ]] || return 0
  case "$ARCH_GUARD_DIFF_RANGE" in
    HEAD..HEAD | HEAD...HEAD)
      fail "ARCH_GUARD_DIFF_RANGE must not compare HEAD to itself"
      return 0
      ;;
  esac
  if ! git diff --name-only --diff-filter=ACMRTUXB "$ARCH_GUARD_DIFF_RANGE" >/dev/null 2>&1; then
    fail "ARCH_GUARD_DIFF_RANGE is invalid or cannot be diffed: $ARCH_GUARD_DIFF_RANGE"
    return 0
  fi
  local left right left_object right_object
  if [[ "$ARCH_GUARD_DIFF_RANGE" == *"..."* ]]; then
    left="${ARCH_GUARD_DIFF_RANGE%%...*}"
    right="${ARCH_GUARD_DIFF_RANGE#*...}"
  else
    left="${ARCH_GUARD_DIFF_RANGE%%..*}"
    right="${ARCH_GUARD_DIFF_RANGE#*..}"
  fi
  if [[ -z "$left" || -z "$right" ]]; then
    fail "ARCH_GUARD_DIFF_RANGE must include both diff endpoints"
    return 0
  fi
  if [[ -n "$left" && -n "$right" ]]; then
    left_object="$(git rev-parse --verify "$left^{object}" 2>/dev/null || true)"
    right_object="$(git rev-parse --verify "$right^{object}" 2>/dev/null || true)"
    if [[ -n "$left_object" && "$left_object" == "$right_object" ]]; then
      fail "ARCH_GUARD_DIFF_RANGE must not compare the same git object to itself"
    fi
  fi
}

doc_guard_file() {
  case "$1" in
    README.md | AGENTS.md | ARCHITECTURE_WAIVERS.md | ARTIFACT_DEPLOY.md | BUILD_POLICY.md | BUILD_REFACTOR_ROADMAP.md | BUILD_ARCHITECTURE_REDESIGN_NO_HOUR_BUILDS.md)
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

validate_diff_range
if [[ "$mode" == "--changed" ]]; then
  mapfile -t files < <(changed_files)
else
  mapfile -t files < <(all_files)
fi

check_file_size() {
  local path="$1"
  [[ -f "$path" ]] || return 0
  if ! is_rust_file "$path" && [[ "$path" != *.md ]]; then
    return 0
  fi

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

  local current baseline
  current="$(inline_test_body_count_file "$path")"
  ((current > 0)) || return 0
  baseline=0
  if baseline_file_exists "$path"; then
    baseline="$(inline_test_body_count_head "$path")"
  fi

  if ((current > baseline)); then
    fail "$path increases inline #[cfg(test)] module count ($current > $baseline)"
  elif [[ "$mode" == "--all" ]]; then
    echo "[architecture:guard] DEBT $path has $current grandfathered inline test module(s)"
  fi
}

inline_test_body_count_file() {
  awk '
      /#\[cfg[[:space:]]*\([[:space:]]*test[[:space:]]*\)\].*mod[[:space:]]+[[:alnum:]_]+.*\{/ { found=1 }
      /#\[cfg[[:space:]]*\([[:space:]]*test[[:space:]]*\)\]/ { seen_cfg=1; next }
      seen_cfg && /^[[:space:]]*(pub[[:space:]]+)?mod[[:space:]]+[[:alnum:]_]+.*\{/ { found=1 }
      seen_cfg && NF && $0 !~ /^[[:space:]]*#/ { seen_cfg=0 }
      found { count++; found=0; seen_cfg=0 }
      END { print count + 0 }
    ' "$1"
}

inline_test_body_count_head() {
  baseline_file_content "$1" | awk '
    /#\[cfg[[:space:]]*\([[:space:]]*test[[:space:]]*\)\].*mod[[:space:]]+[[:alnum:]_]+.*\{/ { found=1 }
    /#\[cfg[[:space:]]*\([[:space:]]*test[[:space:]]*\)\]/ { seen_cfg=1; next }
    seen_cfg && /^[[:space:]]*(pub[[:space:]]+)?mod[[:space:]]+[[:alnum:]_]+.*\{/ { found=1 }
    seen_cfg && NF && $0 !~ /^[[:space:]]*#/ { seen_cfg=0 }
    found { count++; found=0; seen_cfg=0 }
    END { print count + 0 }
  '
}

include_count_file() {
  (grep -E '(^|[^[:alnum:]_])include![[:space:]]*\(' "$1" || true) | wc -l | tr -d ' '
}

include_count_head() {
  (baseline_file_content "$1" | grep -E '(^|[^[:alnum:]_])include![[:space:]]*\(' || true) | wc -l | tr -d ' '
}

check_include_sharding() {
  local path="$1"
  [[ -f "$path" ]] || return 0
  is_production_rust_file "$path" || return 0

  local current baseline
  current="$(include_count_file "$path")"
  ((current > 0)) || return 0
  baseline=0
  if baseline_file_exists "$path"; then
    baseline="$(include_count_head "$path")"
  fi

  if ((current > baseline)); then
    fail "$path increases include! facade sharding ($current > $baseline)"
  elif [[ "$mode" == "--all" ]]; then
    echo "[architecture:guard] DEBT $path has $current grandfathered include! shards"
  fi
}

check_forbidden_new_bin() {
  local path="$1"
  case "$path" in
    crates/app/src/bin/* | crates/discovery/src/bin/* | crates/storage/src/bin/*)
      ;;
    *)
      return 0
      ;;
  esac
  if ! baseline_file_exists "$path"; then
    fail "$path is a new bin in a quarantined crate; operators must live outside app/discovery/storage monoliths"
  fi
}

check_forbidden_cargo_bins() {
  local path="$1"
  case "$path" in
    crates/app/Cargo.toml | crates/discovery/Cargo.toml | crates/storage/Cargo.toml)
      ;;
    *)
      return 0
      ;;
  esac
  [[ -f "$path" ]] || return 0
  if grep -q '^\[\[bin\]\]' "$path"; then
    fail "$path declares bin targets in a quarantined crate"
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
    if cargo_toml_mentions_dep "$path" "$dep"; then
      fail "$path declares forbidden operator dependency: $dep"
    fi
  done
}

cargo_toml_mentions_dep() {
  local path="$1"
  local dep="$2"
  awk -v dep="$dep" '
    function esc_re(value, out, i, c) {
      out = ""
      for (i = 1; i <= length(value); i++) {
        c = substr(value, i, 1)
        if (c ~ /[][(){}.^$*+?|\\-]/) {
          out = out "\\" c
        } else {
          out = out c
        }
      }
      return out
    }
    BEGIN {
      dep_re = esc_re(dep)
    }
    /^[[:space:]]*#/ { next }
    {
      line = $0
      sub(/[[:space:]]*#.*/, "", line)
      if (line ~ "^\\[[^]]*\\." dep_re "\\]") {
        found = 1
      }
      if (line ~ "^\\[[^]]*\\.\"" dep_re "\"\\]") {
        found = 1
      }
      if (line ~ "^[[:space:]]*" dep_re "[[:space:]]*=") {
        found = 1
      }
      if (line ~ "^[[:space:]]*\"" dep_re "\"[[:space:]]*=") {
        found = 1
      }
      if (line ~ "^[[:space:]]*" dep_re "\\.workspace[[:space:]]*=") {
        found = 1
      }
      if (line ~ "^[[:space:]]*\"" dep_re "\"\\.workspace[[:space:]]*=") {
        found = 1
      }
      if (line ~ "package[[:space:]]*=[[:space:]]*\"" dep_re "\"") {
        found = 1
      }
    }
    END { exit found ? 0 : 1 }
  ' "$path"
}

check_doc_build_commands() {
  local path="$1"
  [[ "$path" == *.md ]] || return 0
  doc_guard_file "$path" || return 0
  [[ -f "$path" ]] || return 0

  if [[ "$mode" == "--changed" ]] && baseline_file_exists "$path"; then
    local diff_args=(--unified=0)
    if [[ -n "${ARCH_GUARD_DIFF_RANGE:-}" ]]; then
      diff_args+=("$ARCH_GUARD_DIFF_RANGE")
    fi
    diff_args+=(-- "$path")
    if git diff "${diff_args[@]}" | grep -E '^\+[^+].*(cargo build --release|CARGO_BUILD_JOBS=.*cargo build|/var/www/.*cargo build)' | grep -Eiv 'emergency|fallback|artifact|builder|CI|forbidden|rejected|off production|off-server|--profile operator-release|Rejected normal rollout pattern' >/dev/null; then
      fail "$path adds production-local cargo build --release wording without emergency/artifact context"
    fi
  elif grep -E '(cargo build --release|CARGO_BUILD_JOBS=.*cargo build|/var/www/.*cargo build)' "$path" | grep -Eiv 'emergency|fallback|artifact|builder|CI|forbidden|rejected|off production|off-server|--profile operator-release|Rejected normal rollout pattern' >/dev/null; then
    fail "$path contains cargo build --release without emergency/artifact context"
  fi
}

legacy_stage3_marker_patterns() {
  printf '%s\n' \
    "5""-day" \
    "full""-window proof" \
    "accepted ""scoring"
}

check_forbidden_legacy_markers() {
  local pattern path
  if [[ "$mode" == "--changed" ]]; then
    local diff_args=(--unified=0)
    if [[ -n "${ARCH_GUARD_DIFF_RANGE:-}" ]]; then
      diff_args+=("$ARCH_GUARD_DIFF_RANGE")
    fi
    while IFS= read -r pattern; do
      if marker_diff_adds_pattern "$pattern" "${diff_args[@]}"; then
        fail "changed diff adds a forbidden legacy Stage 3 marker"
      fi
      if [[ -z "${ARCH_GUARD_DIFF_RANGE:-}" ]] && marker_diff_adds_pattern "$pattern" --cached --unified=0; then
        fail "changed diff adds a forbidden legacy Stage 3 marker"
      fi
      if [[ -z "${ARCH_GUARD_DIFF_RANGE:-}" ]]; then
        for path in "${files[@]}"; do
          [[ -f "$path" ]] || continue
          git ls-files --error-unmatch "$path" >/dev/null 2>&1 && continue
          if grep -IF "$pattern" "$path" >/dev/null; then
            fail "$path adds a forbidden legacy Stage 3 marker"
          fi
        done
      fi
    done < <(legacy_stage3_marker_patterns)
    local legacy_raw_source
    legacy_raw_source="raw_""window"
    if marker_diff_adds_regex "published_scoring_source.*${legacy_raw_source}" "${diff_args[@]}"; then
      fail "changed diff adds legacy publication-source readiness wiring"
    fi
    if [[ -z "${ARCH_GUARD_DIFF_RANGE:-}" ]] && marker_diff_adds_regex "published_scoring_source.*${legacy_raw_source}" --cached --unified=0; then
      fail "changed diff adds legacy publication-source readiness wiring"
    fi
    if [[ -z "${ARCH_GUARD_DIFF_RANGE:-}" ]]; then
      for path in "${files[@]}"; do
        [[ -f "$path" ]] || continue
        git ls-files --error-unmatch "$path" >/dev/null 2>&1 && continue
        if grep -E "published_scoring_source.*${legacy_raw_source}" "$path" >/dev/null; then
          fail "$path adds legacy publication-source readiness wiring"
        fi
      done
    fi
    return 0
  fi

  while IFS= read -r pattern; do
    for path in "${files[@]}"; do
      [[ -f "$path" ]] || continue
      if grep -IF "$pattern" "$path" >/dev/null; then
        fail "$path contains a forbidden legacy Stage 3 marker"
      fi
    done
  done < <(legacy_stage3_marker_patterns)
}

marker_diff_adds_pattern() {
  local pattern="$1"
  shift
  git diff "$@" -- "${files[@]}" 2>/dev/null \
    | grep -v -F -- '+++' \
    | grep -v -F -- '---' \
    | grep -F "$pattern" \
    | grep -E '^\+[^+]' >/dev/null
}

marker_diff_adds_regex() {
  local pattern="$1"
  shift
  git diff "$@" -- "${files[@]}" 2>/dev/null \
    | grep -v -F -- '+++' \
    | grep -v -F -- '---' \
    | grep -E "^\\+[^+].*${pattern}" >/dev/null
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
    cargo metadata --locked --format-version=1 --no-deps 2>/dev/null | python3 -c '
import json
import pathlib
import sys

metadata = json.load(sys.stdin)
entries = {}
for package in metadata.get("packages", []):
    manifest = pathlib.Path(package["manifest_path"])
    crate_dir = manifest.parent
    declared = set()
    for target in package.get("targets", []):
        if "bin" not in target.get("kind", []):
            continue
        name = target.get("name")
        if not name:
            continue
        declared.add(name)
        entries.setdefault(name, set()).add(str(target.get("src_path", manifest)))
    bin_dir = crate_dir / "src" / "bin"
    if not bin_dir.is_dir():
        continue
    paths = list(bin_dir.glob("*.rs")) + list(bin_dir.glob("*/main.rs"))
    for path in paths:
        name = path.parent.name if path.name == "main.rs" else path.stem
        if name in declared:
            continue
        entries.setdefault(name, set()).add(str(path))

for name, paths in sorted(entries.items()):
    if len(paths) > 1:
        print(f"{name} count={len(paths)} " + " ".join(sorted(paths)))
'
  )
}

check_forbidden_dependency_graph() {
  local package deps dep forbidden
  local packages=()
  mapfile -t packages < <(operator_guard_packages)
  if ((${#packages[@]} == 0)); then
    fail "failed to discover operator/storage-core packages for dependency guard"
    return 0
  fi

  for package in "${packages[@]}"; do
    if ! deps="$(cargo tree --locked -p "$package" --edges normal,build --prefix none 2>/dev/null | awk '{ print $1 }' | sort -u)"; then
      fail "failed to inspect dependency graph for $package"
      continue
    fi

    local forbidden_deps=(
      copybot-app
      copybot-ingestion
      copybot-shadow
      copybot-discovery
      copybot-storage
    )
    if [[ "$package" != "copybot-operators" ]]; then
      forbidden_deps+=(
        yellowstone-grpc-client
        yellowstone-grpc-proto
        tonic
      )
    fi

    for forbidden in "${forbidden_deps[@]}"; do
      if printf '%s\n' "$deps" | awk -v dep="$forbidden" '$0 == dep { found=1 } END { exit found ? 0 : 1 }'; then
        fail "$package pulls forbidden transitive dependency: $forbidden"
      fi
    done
  done
}

operator_guard_packages() {
  cargo metadata --locked --format-version=1 --no-deps 2>/dev/null | python3 -c '
import json
import sys

metadata = json.load(sys.stdin)
packages = []
quarantined = {"copybot-app", "copybot-discovery", "copybot-storage"}
for package in metadata.get("packages", []):
    name = package.get("name", "")
    has_bin = any("bin" in target.get("kind", []) for target in package.get("targets", []))
    if (
        name in {"copybot-discovery-v2", "copybot-storage-core", "copybot-operators"}
        or (name.startswith("copybot-") and name.endswith("-ops"))
        or name == "copybot-live-proof"
        or (has_bin and name.startswith("copybot-") and name not in quarantined)
    ):
        packages.append(name)
for name in sorted(set(packages)):
    print(name)
'
}

for path in "${files[@]}"; do
  check_file_size "$path"
  check_inline_tests "$path"
  check_include_sharding "$path"
  check_forbidden_new_bin "$path"
  check_forbidden_cargo_bins "$path"
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
