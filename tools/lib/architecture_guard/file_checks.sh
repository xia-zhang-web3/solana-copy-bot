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
