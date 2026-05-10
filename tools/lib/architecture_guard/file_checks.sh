limit_for_file() {
  local path="$1"
  if [[ "$path" == crates/*/src/main.rs || "$path" == */src/bin/*.rs || "$path" == */src/bin/*/main.rs ]]; then
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
  if ! is_rust_file "$path" && [[ "$path" != *.md && "$path" != *.sh && "$path" != *.py ]]; then
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

  local current baseline current_body_lines baseline_body_lines
  current="$(inline_test_body_count_file "$path")"
  current_body_lines="$(inline_test_body_line_count_file "$path")"
  local current_tests baseline_tests
  current_tests="$(inline_test_attr_count_file "$path")"
  ((current > 0 || current_body_lines > 0 || current_tests > 0)) || return 0
  baseline=0
  baseline_body_lines=0
  baseline_tests=0
  if baseline_file_exists "$path"; then
    baseline="$(inline_test_body_count_head "$path")"
    baseline_body_lines="$(inline_test_body_line_count_head "$path")"
    baseline_tests="$(inline_test_attr_count_head "$path")"
  fi

  if ((current > baseline)); then
    fail "$path increases inline #[cfg(test)] module count ($current > $baseline)"
  elif ((current_body_lines > baseline_body_lines)); then
    fail "$path increases inline #[cfg(test)] body size ($current_body_lines > $baseline_body_lines lines)"
  elif ((current_tests > baseline_tests)); then
    fail "$path increases inline test item count ($current_tests > $baseline_tests)"
  elif [[ "$mode" == "--all" ]]; then
    echo "[architecture:guard] DEBT $path has $current grandfathered inline test module(s), $current_body_lines inline test body line(s), $current_tests inline test item(s)"
  fi
}

inline_test_body_count_file() {
  python3 - "$1" <<'PY'
import re
import sys

text = open(sys.argv[1], encoding="utf-8").read()
count = 0
cfg_attr = re.compile(r"#\s*\[\s*cfg\s*\((?P<body>.*?)\)\s*\]", re.S)
item = re.compile(
    r"(?:#\s*\[[^\]]*\]\s*)*(?:(?:pub(?:\s*\([^)]*\))?\s+)?(?:async\s+)?(?:mod|fn)\s+[A-Za-z_][A-Za-z0-9_]*)",
    re.S,
)
def is_test_cfg(body):
    compact = re.sub(r"\s+", "", body)
    if "not(test)" in compact:
        return False
    return re.search(r"(^|[(),])test($|[(),])", compact) is not None
for match in cfg_attr.finditer(text):
    if not is_test_cfg(match.group("body")):
        continue
    tail = text[match.end() :]
    next_item = item.match(tail.lstrip())
    if next_item:
        count += 1
print(count)
PY
}

inline_test_body_count_head() {
  local tmp
  tmp="$(mktemp)"
  baseline_file_content "$1" > "$tmp"
  inline_test_body_count_file "$tmp"
  rm -f "$tmp"
}

inline_test_body_line_count_file() {
  python3 - "$1" <<'PY'
import re
import sys

text = open(sys.argv[1], encoding="utf-8").read()
cfg_attr = re.compile(r"#\s*\[\s*cfg\s*\((?P<body>.*?)\)\s*\]", re.S)
item = re.compile(
    r"(?:#\s*\[[^\]]*\]\s*)*(?:(?:pub(?:\s*\([^)]*\))?\s+)?(?:async\s+)?(?:mod|fn)\s+[A-Za-z_][A-Za-z0-9_]*)",
    re.S,
)

def is_test_cfg(body):
    compact = re.sub(r"\s+", "", body)
    if "not(test)" in compact:
        return False
    return re.search(r"(^|[(),])test($|[(),])", compact) is not None

def matching_brace_end(value, brace_index):
    depth = 0
    for index in range(brace_index, len(value)):
        char = value[index]
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return index
    return len(value)

total = 0
for match in cfg_attr.finditer(text):
    if not is_test_cfg(match.group("body")):
        continue
    tail_offset = match.end()
    stripped = text[tail_offset:].lstrip()
    item_match = item.match(stripped)
    if not item_match:
        continue
    item_start = tail_offset + (len(text[tail_offset:]) - len(stripped))
    semicolon = text.find(";", item_start)
    brace = text.find("{", item_start)
    if brace == -1 or (semicolon != -1 and semicolon < brace):
        continue
    end = matching_brace_end(text, brace)
    total += text[item_start : end + 1].count("\n") + 1
print(total)
PY
}

inline_test_body_line_count_head() {
  local tmp
  tmp="$(mktemp)"
  baseline_file_content "$1" > "$tmp"
  inline_test_body_line_count_file "$tmp"
  rm -f "$tmp"
}

inline_test_attr_count_file() {
  python3 - "$1" <<'PY'
import re
import sys

text = open(sys.argv[1], encoding="utf-8").read()
count = 0
for match in re.finditer(r"#\s*\[(.*?)\]", text, flags=re.S):
    attr = re.sub(r"\s+", "", match.group(1))
    if attr.startswith("test") or attr.startswith("tokio::test"):
        count += 1
print(count)
PY
}

inline_test_attr_count_head() {
  local tmp
  tmp="$(mktemp)"
  baseline_file_content "$1" > "$tmp"
  inline_test_attr_count_file "$tmp"
  rm -f "$tmp"
}

include_count_file() {
  python3 - "$1" <<'PY'
import re
import sys

text = open(sys.argv[1], encoding="utf-8").read()
text = re.sub(r"//.*?$", "", text, flags=re.M)
text = re.sub(r"/\*.*?\*/", "", text, flags=re.S)
text = re.sub(r'"(?:\\.|[^"\\])*"', '""', text)
text = re.sub(r"r#*\".*?\"#*", 'r""', text, flags=re.S)
print(len(re.findall(r"(?<![A-Za-z0-9_])include\s*!\s*\(", text)))
PY
}

include_count_head() {
  local tmp
  tmp="$(mktemp)"
  baseline_file_content "$1" > "$tmp"
  include_count_file "$tmp"
  rm -f "$tmp"
}

check_include_sharding() {
  local path="$1"
  [[ -f "$path" ]] || return 0
  is_rust_file "$path" || return 0

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
  if grep -Eq '^[[:space:]]*\[\[[[:space:]]*bin[[:space:]]*\]\]' "$path"; then
    fail "$path declares bin targets in a quarantined crate"
  fi
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
