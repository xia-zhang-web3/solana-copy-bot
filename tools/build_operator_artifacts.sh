#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

usage() {
  cat >&2 <<'EOF'
usage: tools/build_operator_artifacts.sh

Environment:
  PROFILE=<derived from PACKAGE>
  TARGET=x86_64-unknown-linux-gnu
  ARTIFACT_ARCH=linux-x86_64
  ARTIFACT_ROOT=artifacts
  CARGO_TARGET_DIR=target
  PACKAGE=copybot-discovery-v2
  WANTED_BINS=<derived from PACKAGE>
  RUN_CHECKS=1
  ALLOW_DIRTY=0
  FORCE=0
EOF
}

if [ "${1:-}" = "--help" ] || [ "${1:-}" = "-h" ]; then
  usage
  exit 0
fi

TARGET="${TARGET:-x86_64-unknown-linux-gnu}"
ARTIFACT_ARCH="${ARTIFACT_ARCH:-linux-x86_64}"
ARTIFACT_ROOT="${ARTIFACT_ROOT:-artifacts}"
TARGET_ROOT="${CARGO_TARGET_DIR:-target}"
RUN_CHECKS="${RUN_CHECKS:-1}"
ALLOW_DIRTY="${ALLOW_DIRTY:-0}"
FORCE="${FORCE:-0}"
PACKAGE="${PACKAGE:-copybot-discovery-v2}"
lock_dir=""

cleanup() {
  if [ -n "$lock_dir" ]; then
    rmdir "$lock_dir" 2>/dev/null || true
  fi
}
trap cleanup EXIT

default_profile_for_package() {
  case "$1" in
    copybot-app) printf 'release' ;;
    *) printf 'operator-release' ;;
  esac
}

expected_profile="$(default_profile_for_package "$PACKAGE")"
PROFILE="${PROFILE:-$expected_profile}"
if [ "$PROFILE" != "$expected_profile" ]; then
  echo "$PACKAGE artifacts must use profile $expected_profile, got $PROFILE" >&2
  exit 1
fi

cargo_profile_dir() {
  case "$1" in
    dev) printf 'debug' ;;
    release) printf 'release' ;;
    *) printf '%s' "$1" ;;
  esac
}

if [ "$ALLOW_DIRTY" != "1" ]; then
  if ! git diff --quiet || ! git diff --cached --quiet || [ -n "$(git ls-files --others --exclude-standard)" ]; then
    echo "refusing to build production artifact from dirty tree; set ALLOW_DIRTY=1 for local smoke artifacts" >&2
    exit 1
  fi
fi

expected_bins="$(python3 tools/package_bins.py --package "$PACKAGE" | xargs)"
WANTED_BINS="${WANTED_BINS:-$expected_bins}"
canonical_words() {
  printf '%s\n' "$1" | tr ' ' '\n' | sed '/^$/d' | sort | xargs
}
if [ "$(canonical_words "$WANTED_BINS")" != "$(canonical_words "$expected_bins")" ]; then
  echo "$PACKAGE artifacts must include the complete package binary set: $expected_bins" >&2
  exit 1
fi

BINS=""
for bin in $WANTED_BINS; do
  BINS="${BINS:+$BINS }$bin"
done

if [ -z "$BINS" ]; then
  echo "no operator bins found for $PACKAGE" >&2
  exit 1
fi

package_has_lib() {
  python3 - "$1" <<'PY'
import json
import subprocess
import sys

package_name = sys.argv[1]
raw = subprocess.check_output(
    ["cargo", "metadata", "--locked", "--format-version=1", "--no-deps"],
    text=True,
)
metadata = json.loads(raw)
for package in metadata.get("packages", []):
    if package.get("name") != package_name:
        continue
    has_lib = any("lib" in target.get("kind", []) for target in package.get("targets", []))
    print("1" if has_lib else "0")
    raise SystemExit(0)
print(f"unknown package: {package_name}", file=sys.stderr)
raise SystemExit(1)
PY
}

test_check=""
if [ "$RUN_CHECKS" = "1" ]; then
  if [ -x tools/architecture_guard.sh ]; then
    tools/architecture_guard.sh --changed
    tools/architecture_guard.sh --all
  fi
  if [ "$(package_has_lib "$PACKAGE")" = "1" ]; then
    test_check="cargo test --locked -p $PACKAGE --lib -- --test-threads=1; cargo test --locked -p $PACKAGE --tests --no-run"
    cargo test --locked -p "$PACKAGE" --lib -- --test-threads=1
    cargo test --locked -p "$PACKAGE" --tests --no-run
  else
    test_check="cargo test --locked -p $PACKAGE --bins --no-run"
    cargo test --locked -p "$PACKAGE" --bins --no-run
  fi
fi

build_args=(build --locked --target-dir "$TARGET_ROOT" --profile "$PROFILE" --target "$TARGET" -p "$PACKAGE")
for bin in $BINS; do
  build_args+=(--bin "$bin")
done
cargo "${build_args[@]}"

sha="$(git rev-parse HEAD)"
dirty_suffix=""
if [ "$ALLOW_DIRTY" = "1" ]; then
  if ! git diff --quiet || ! git diff --cached --quiet || [ -n "$(git ls-files --others --exclude-standard)" ]; then
    dirty_suffix="-dirty"
  fi
fi
safe_package="${PACKAGE//[^A-Za-z0-9_.-]/-}"
artifact_id="$safe_package-$sha$dirty_suffix"
out="$ARTIFACT_ROOT/$ARTIFACT_ARCH/$artifact_id"
archive="$out.tar.gz"
mkdir -p "$ARTIFACT_ROOT/$ARTIFACT_ARCH"
lock_dir="$ARTIFACT_ROOT/$ARTIFACT_ARCH/.build-$safe_package.lock"
if ! mkdir "$lock_dir" 2>/dev/null; then
  echo "another artifact build appears to be active for $PACKAGE: $lock_dir" >&2
  exit 1
fi
if { [ -e "$out" ] || [ -e "$archive" ]; } && [ "$FORCE" != "1" ]; then
  echo "$out already exists; set FORCE=1 to replace it" >&2
  exit 1
fi
if [ -e "$out" ]; then
  rm -rf "$out"
fi
if [ -e "$archive" ]; then
  rm -f "$archive"
fi
mkdir -p "$out"

target_dir="$TARGET_ROOT/$TARGET/$(cargo_profile_dir "$PROFILE")"
for bin in $BINS; do
  cp "$target_dir/$bin" "$out/"
done

(
  cd "$out"
  : > SHA256SUMS
  for bin in $BINS; do
    if command -v sha256sum >/dev/null 2>&1; then
      sha256sum "$bin" >> SHA256SUMS
    else
      shasum -a 256 "$bin" >> SHA256SUMS
    fi
  done
)

manifest_args=(
  --artifact-dir "$out"
  --artifact-id "$artifact_id"
  --git-sha "$sha"
  --target "$TARGET"
  --profile "$PROFILE"
  --package "$PACKAGE"
)
if [ "$dirty_suffix" = "-dirty" ]; then
  manifest_args+=(--git-dirty)
fi
if [ "$RUN_CHECKS" = "1" ]; then
  if [ -x tools/architecture_guard.sh ]; then
    manifest_args+=(--check "tools/architecture_guard.sh --changed")
    manifest_args+=(--check "tools/architecture_guard.sh --all")
  fi
  manifest_args+=(--check "$test_check")
fi
for bin in $BINS; do
  manifest_args+=(--expected-binary "$bin")
  manifest_args+=(--binary "$bin:$PACKAGE")
done
python3 tools/build_manifest.py "${manifest_args[@]}"

tar -C "$ARTIFACT_ROOT/$ARTIFACT_ARCH" -czf "$archive" "$artifact_id"
archive_name="$(basename "$archive")"
(
  cd "$(dirname "$archive")"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$archive_name" > "$archive_name.sha256"
  else
    shasum -a 256 "$archive_name" > "$archive_name.sha256"
  fi
)

echo "$out"
echo "$archive"
echo "$archive.sha256"
