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

diff_left_endpoint() {
  if [[ "${ARCH_GUARD_DIFF_RANGE:-}" == *"..."* ]]; then
    printf '%s' "${ARCH_GUARD_DIFF_RANGE%%...*}"
  else
    printf '%s' "${ARCH_GUARD_DIFF_RANGE%%..*}"
  fi
}

diff_right_endpoint() {
  if [[ "${ARCH_GUARD_DIFF_RANGE:-}" == *"..."* ]]; then
    printf '%s' "${ARCH_GUARD_DIFF_RANGE#*...}"
  else
    printf '%s' "${ARCH_GUARD_DIFF_RANGE#*..}"
  fi
}

baseline_ref() {
  if [[ -n "${ARCH_GUARD_DIFF_RANGE:-}" ]]; then
    diff_left_endpoint
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

changed_files() {
  if [[ -n "${ARCH_GUARD_DIFF_RANGE:-}" ]]; then
    {
      git diff --name-only --diff-filter=ACMRTUXB "$ARCH_GUARD_DIFF_RANGE"
      if [[ "${GITHUB_ACTIONS:-}" != "true" ]]; then
        git diff --name-only --diff-filter=ACMRTUXB
        git diff --name-only --cached --diff-filter=ACMRTUXB
        git ls-files --others --exclude-standard
      fi
    } | sort -u
    return
  fi
  {
    git diff --name-only --diff-filter=ACMRTUXB
    git diff --name-only --cached --diff-filter=ACMRTUXB
    git ls-files --others --exclude-standard
  } | sort -u
}

all_files() {
  {
    git ls-files
    git ls-files --others --exclude-standard
  } | sort -u
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
  left="$(diff_left_endpoint)"
  right="$(diff_right_endpoint)"
  if [[ -z "$left" || -z "$right" ]]; then
    fail "ARCH_GUARD_DIFF_RANGE must include both diff endpoints"
    return 0
  fi

  left_object="$(git rev-parse --verify "$left^{object}" 2>/dev/null || true)"
  right_object="$(git rev-parse --verify "$right^{object}" 2>/dev/null || true)"
  if [[ -n "$left_object" && "$left_object" == "$right_object" ]]; then
    fail "ARCH_GUARD_DIFF_RANGE must not compare the same git object to itself"
  fi
}

doc_guard_file() {
  case "$1" in
    README.md | AGENTS.md | ARCHITECTURE_WAIVERS.md | ARTIFACT_DEPLOY.md | BUILD_POLICY.md | BUILD_REFACTOR_ROADMAP.md | BUILD_ARCHITECTURE_REDESIGN_NO_HOUR_BUILDS.md)
      return 0
      ;;
    docs/*.md | docs/**/*.md | ops/*.md | ops/**/*.md)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}
