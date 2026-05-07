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
  local package deps forbidden
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
