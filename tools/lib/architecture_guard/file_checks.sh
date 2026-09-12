# Helpers return diagnostics only after all inputs have been read successfully.
# A helper/Git error is a guard failure, never an empty successful result.
batch_diagnostics() {
  local helper="$1" output diagnostic kind message
  shift
  if ! output="$(printf '%s\0' "${files[@]}" | python3 -B "$SCRIPT_DIR/lib/architecture_guard/$helper" "$@")"; then
    fail "failed to run architecture helper: $helper"
    return 0
  fi
  while IFS= read -r diagnostic; do
    [[ -n "$diagnostic" ]] || continue
    kind="${diagnostic%% *}"
    message="${diagnostic#* }"
    case "$kind" in
      FAIL) fail "$message" ;;
      DEBT) echo "[architecture:guard] DEBT $message" ;;
      *) fail "invalid diagnostic from architecture helper: $helper" ;;
    esac
  done <<< "$output"
}

check_file_batch() {
  batch_diagnostics file_batch.py "$mode" "$(baseline_ref)"
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
    local added_build_wording=1
    if git diff "${diff_args[@]}" | grep -E '^\+[^+].*(cargo build --release|CARGO_BUILD_JOBS=.*cargo build|/var/www/.*cargo build)' | grep -Eiv 'emergency|fallback|artifact|builder|CI|forbidden|rejected|off production|off-server|--profile operator-release|Rejected normal rollout pattern' >/dev/null; then
      added_build_wording=0
    fi
    if [[ "${GITHUB_ACTIONS:-}" != "true" && -n "${ARCH_GUARD_DIFF_RANGE:-}" ]] && git diff --unified=0 -- "$path" | grep -E '^\+[^+].*(cargo build --release|CARGO_BUILD_JOBS=.*cargo build|/var/www/.*cargo build)' | grep -Eiv 'emergency|fallback|artifact|builder|CI|forbidden|rejected|off production|off-server|--profile operator-release|Rejected normal rollout pattern' >/dev/null; then
      added_build_wording=0
    fi
    if [[ "${GITHUB_ACTIONS:-}" != "true" ]] && git diff --cached --unified=0 -- "$path" | grep -E '^\+[^+].*(cargo build --release|CARGO_BUILD_JOBS=.*cargo build|/var/www/.*cargo build)' | grep -Eiv 'emergency|fallback|artifact|builder|CI|forbidden|rejected|off production|off-server|--profile operator-release|Rejected normal rollout pattern' >/dev/null; then
      added_build_wording=0
    fi
    if [[ "$added_build_wording" == "0" ]]; then
      fail "$path adds production-local cargo build --release wording without emergency/artifact context"
    fi
  elif grep -E '(cargo build --release|CARGO_BUILD_JOBS=.*cargo build|/var/www/.*cargo build)' "$path" | grep -Eiv 'emergency|fallback|artifact|builder|CI|forbidden|rejected|off production|off-server|--profile operator-release|Rejected normal rollout pattern' >/dev/null; then
    fail "$path contains cargo build --release without emergency/artifact context"
  fi
}
