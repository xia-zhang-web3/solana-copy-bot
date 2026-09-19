# Each Git diff is read completely before any scanner can short-circuit.
# The directory exists only for this invocation and is removed on every exit.
prepare_diff_cache() {
  architecture_diff_dir="$(mktemp -d)"
  trap 'rm -rf "$architecture_diff_dir"' EXIT
  # Selected files include untracked evidence; passing their paths can exceed ARG_MAX.
  # Git diff already covers every tracked change in the requested worktree/index/range.
  local context source key
  for context in 0 3; do
    for source in main cached; do
      local args=(--unified="$context")
      if [[ "$source" == cached ]]; then
        [[ "${GITHUB_ACTIONS:-}" != true ]] || continue
        args+=(--cached)
      elif [[ -n "${ARCH_GUARD_DIFF_RANGE:-}" ]]; then
        args+=("$ARCH_GUARD_DIFF_RANGE")
      fi
      key="$source-$context"
      git diff "${args[@]}" -- > "$architecture_diff_dir/$key" || return
    done
    if [[ -n "${ARCH_GUARD_DIFF_RANGE:-}" && "${GITHUB_ACTIONS:-}" != true ]]; then
      git diff --unified="$context" -- > "$architecture_diff_dir/local-$context" || return
    fi
  done
}

marker_diff_scan() {
  local scanner="$1" pattern="$2" result status
  shift 2
  local context=3 source=main argument
  for argument in "$@"; do
    case "$argument" in
      --unified=*) context="${argument#*=}" ;;
      --cached) source=cached ;;
    esac
  done
  if [[ "$source" == main && -n "${ARCH_GUARD_DIFF_RANGE:-}" && "$*" != *"$ARCH_GUARD_DIFF_RANGE"* ]]; then
    source=local
  fi
  if result="$(python3 -B "$SCRIPT_DIR/lib/architecture_guard/diff_scan.py" "$architecture_diff_dir/$source-$context" "$scanner" "$pattern")"; then
    case "$result" in
      match) return 0 ;;
      no-match) return 1 ;;
      *) status=invalid-result ;;
    esac
  else
    status=$?
  fi
  # Called directly in the parent conditional: helper errors (including exit 1)
  # and malformed results must update its counter, never mean ordinary no-match.
  fail "failed to scan architecture diff cache: $source-$context ($scanner, exit $status)"
  return 1
}
