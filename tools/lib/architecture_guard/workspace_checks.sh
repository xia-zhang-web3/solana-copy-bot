legacy_stage3_marker_patterns() {
  printf '%b%b\n' '\x35' '\x2dday'
  printf '%b%b%b\n' '\x66ull' '\x2dwindow' '\x20proof'
  printf '%b%b\n' '\x61ccepted' '\x20scoring'
}

check_forbidden_legacy_markers() {
  local pattern path
  if [[ "$mode" == "--changed" ]]; then
    local diff_args=(--unified=3)
    if [[ -n "${ARCH_GUARD_DIFF_RANGE:-}" ]]; then
      diff_args+=("$ARCH_GUARD_DIFF_RANGE")
    fi
    local include_local_worktree=0
    if [[ "${GITHUB_ACTIONS:-}" != "true" ]]; then
      include_local_worktree=1
    fi
    while IFS= read -r pattern; do
      if marker_diff_adds_pattern "$pattern" "${diff_args[@]}"; then
        fail "changed diff adds a forbidden legacy Stage 3 marker"
      fi
      if marker_diff_adds_normalized_pattern "$pattern" "${diff_args[@]}"; then
        fail "changed diff adds a split forbidden legacy Stage 3 marker"
      fi
      if [[ "$include_local_worktree" == "1" && -n "${ARCH_GUARD_DIFF_RANGE:-}" ]] && marker_diff_adds_pattern "$pattern" --unified=0; then
        fail "changed diff adds a forbidden legacy Stage 3 marker"
      fi
      if [[ "$include_local_worktree" == "1" && -n "${ARCH_GUARD_DIFF_RANGE:-}" ]] && marker_diff_adds_normalized_pattern "$pattern" --unified=3; then
        fail "changed diff adds a split forbidden legacy Stage 3 marker"
      fi
      if [[ "$include_local_worktree" == "1" ]] && marker_diff_adds_pattern "$pattern" --cached --unified=0; then
        fail "changed diff adds a forbidden legacy Stage 3 marker"
      fi
      if [[ "$include_local_worktree" == "1" ]] && marker_diff_adds_normalized_pattern "$pattern" --cached --unified=3; then
        fail "changed diff adds a split forbidden legacy Stage 3 marker"
      fi
      if [[ "$include_local_worktree" == "1" ]]; then
        for path in "${files[@]}"; do
          [[ -f "$path" ]] || continue
          git ls-files --error-unmatch "$path" >/dev/null 2>&1 && continue
          if grep -IF "$pattern" "$path" >/dev/null; then
            fail "$path adds a forbidden legacy Stage 3 marker"
          fi
          if file_contains_normalized_pattern "$pattern" "$path"; then
            fail "$path adds a split forbidden legacy Stage 3 marker"
          fi
        done
      fi
    done < <(legacy_stage3_marker_patterns)
    local legacy_raw_source legacy_raw_source_regex legacy_raw_source_normalized_regex
    legacy_raw_source="$(printf '%b' 'raw_\x77indow')"
    legacy_raw_source_regex="published_scoring_source.*${legacy_raw_source}([^[:alnum:]_]|$)"
    legacy_raw_source_normalized_regex="published_scoring_source.*${legacy_raw_source}[^a-z0-9_]"
    if marker_diff_adds_regex "$legacy_raw_source_regex" "${diff_args[@]}"; then
      fail "changed diff adds legacy publication-source readiness wiring"
    fi
    if marker_diff_adds_normalized_regex "$legacy_raw_source_normalized_regex" "${diff_args[@]}"; then
      fail "changed diff adds split legacy publication-source readiness wiring"
    fi
    if [[ "$include_local_worktree" == "1" && -n "${ARCH_GUARD_DIFF_RANGE:-}" ]] && marker_diff_adds_regex "$legacy_raw_source_regex" --unified=0; then
      fail "changed diff adds legacy publication-source readiness wiring"
    fi
    if [[ "$include_local_worktree" == "1" && -n "${ARCH_GUARD_DIFF_RANGE:-}" ]] && marker_diff_adds_normalized_regex "$legacy_raw_source_normalized_regex" --unified=3; then
      fail "changed diff adds split legacy publication-source readiness wiring"
    fi
    if [[ "$include_local_worktree" == "1" ]] && marker_diff_adds_regex "$legacy_raw_source_regex" --cached --unified=0; then
      fail "changed diff adds legacy publication-source readiness wiring"
    fi
    if [[ "$include_local_worktree" == "1" ]] && marker_diff_adds_normalized_regex "$legacy_raw_source_normalized_regex" --cached --unified=3; then
      fail "changed diff adds split legacy publication-source readiness wiring"
    fi
    if [[ "$include_local_worktree" == "1" ]]; then
      for path in "${files[@]}"; do
        [[ -f "$path" ]] || continue
        git ls-files --error-unmatch "$path" >/dev/null 2>&1 && continue
        if grep -E "$legacy_raw_source_regex" "$path" >/dev/null; then
          fail "$path adds legacy publication-source readiness wiring"
        fi
        if file_contains_normalized_regex_window "$legacy_raw_source_normalized_regex" "$path" 16; then
          fail "$path adds split legacy publication-source readiness wiring"
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
      if file_contains_normalized_pattern "$pattern" "$path"; then
        fail "$path contains a split forbidden legacy Stage 3 marker"
      fi
    done
  done < <(legacy_stage3_marker_patterns)

  local legacy_raw_source legacy_raw_source_regex legacy_raw_source_normalized_regex path
  legacy_raw_source="$(printf '%b' 'raw_\x77indow')"
  legacy_raw_source_regex="published_scoring_source.*${legacy_raw_source}([^[:alnum:]_]|$)"
  legacy_raw_source_normalized_regex="published_scoring_source.*${legacy_raw_source}[^a-z0-9_]"
  for path in "${files[@]}"; do
    [[ -f "$path" ]] || continue
    is_test_rust_file_path "$path" && continue
    if grep -E "$legacy_raw_source_regex" "$path" >/dev/null; then
      fail "$path contains legacy publication-source readiness wiring"
    fi
    if file_contains_normalized_regex_window "$legacy_raw_source_normalized_regex" "$path" 16; then
      fail "$path contains split legacy publication-source readiness wiring"
    fi
  done
}

file_contains_normalized_pattern() {
  local pattern="$1"
  local path="$2"
  python3 - "$pattern" "$path" <<'PY'
import re
import sys

def norm(value):
    return re.sub(r"[\s\"+,()]", "", value).replace(chr(39), "").lower()

pattern = norm(sys.argv[1])
path = sys.argv[2]
try:
    with open(path, "r", encoding="utf-8", errors="ignore") as handle:
        content = norm(handle.read()) + "!"
except OSError:
    raise SystemExit(1)
raise SystemExit(0 if pattern and pattern in content else 1)
PY
}

file_contains_normalized_regex() {
  local pattern="$1"
  local path="$2"
  python3 - "$pattern" "$path" <<'PY'
import re
import sys

def norm(value):
    return re.sub(r"[\s\"+,()]", "", value).replace(chr(39), "").lower()

pattern = re.compile(norm(sys.argv[1]))
path = sys.argv[2]
try:
    with open(path, "r", encoding="utf-8", errors="ignore") as handle:
        content = norm(handle.read()) + "!"
except OSError:
    raise SystemExit(1)
raise SystemExit(0 if pattern.search(content) else 1)
PY
}

file_contains_normalized_regex_window() {
  local pattern="$1"
  local path="$2"
  local window_lines="$3"
  python3 - "$pattern" "$path" "$window_lines" <<'PY'
import re
import sys

def norm(value):
    return re.sub(r"[\s\"+,()]", "", value).replace(chr(39), "").lower()

pattern = re.compile(norm(sys.argv[1]))
path = sys.argv[2]
window_lines = max(1, int(sys.argv[3]))
try:
    with open(path, "r", encoding="utf-8", errors="ignore") as handle:
        lines = [norm(line) for line in handle]
except OSError:
    raise SystemExit(1)

for index in range(len(lines)):
    chunk = "".join(lines[index:index + window_lines]) + "!"
    if pattern.search(chunk):
        raise SystemExit(0)
raise SystemExit(1)
PY
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

marker_diff_adds_normalized_pattern() {
  local pattern="$1"
  shift
  git diff "$@" -- "${files[@]}" 2>/dev/null \
    | python3 -c '
import re
import sys

def norm(value):
    return re.sub(r"[\s\"+,()]", "", value).replace(chr(39), "").lower()

pattern = norm(sys.argv[1])
hunk_old = []
hunk_new = []

def flush_hunk():
    old = norm("".join(hunk_old)) + "!"
    new = norm("".join(hunk_new)) + "!"
    return bool(pattern and pattern in new and pattern not in old)

for line in sys.stdin:
    if line.startswith(("diff ", "index ", "---", "+++")):
        continue
    if line.startswith("@@"):
        if flush_hunk():
            raise SystemExit(0)
        hunk_old = []
        hunk_new = []
        continue
    if line.startswith(" "):
        hunk_old.append(line[1:])
        hunk_new.append(line[1:])
    elif line.startswith("-"):
        hunk_old.append(line[1:])
    elif line.startswith("+"):
        hunk_new.append(line[1:])
    else:
        if flush_hunk():
            raise SystemExit(0)
        hunk_old = []
        hunk_new = []
if flush_hunk():
    raise SystemExit(0)
raise SystemExit(1)
' "$pattern"
}

marker_diff_adds_normalized_regex() {
  local pattern="$1"
  shift
  git diff "$@" -- "${files[@]}" 2>/dev/null \
    | python3 -c '
import re
import sys

def norm(value):
    return re.sub(r"[\s\"+,()]", "", value).replace(chr(39), "").lower()

pattern = norm(sys.argv[1])
regex = re.compile(pattern)
hunk_old = []
hunk_new = []

def flush_hunk():
    old = norm("".join(hunk_old)) + "!"
    new = norm("".join(hunk_new)) + "!"
    return bool(regex.search(new) and not regex.search(old))

for line in sys.stdin:
    if line.startswith(("diff ", "index ", "---", "+++")):
        continue
    if line.startswith("@@"):
        if flush_hunk():
            raise SystemExit(0)
        hunk_old = []
        hunk_new = []
        continue
    if line.startswith(" "):
        hunk_old.append(line[1:])
        hunk_new.append(line[1:])
    elif line.startswith("-"):
        hunk_old.append(line[1:])
    elif line.startswith("+"):
        hunk_new.append(line[1:])
    else:
        if flush_hunk():
            raise SystemExit(0)
        hunk_old = []
        hunk_new = []
if flush_hunk():
    raise SystemExit(0)
raise SystemExit(1)
' "$pattern"
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
  local package deps forbidden
  local packages=()
  while IFS= read -r package; do
    packages+=("$package")
  done < <(operator_guard_packages)
  if ((${#packages[@]} == 0)); then
    fail "failed to discover operator/storage-core packages for dependency guard"
    return 0
  fi

  for package in "${packages[@]}"; do
    if ! deps="$(cargo tree --locked -p "$package" --edges normal,build,dev --prefix none 2>/dev/null | awk '{ print $1 }' | sort -u)"; then
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
