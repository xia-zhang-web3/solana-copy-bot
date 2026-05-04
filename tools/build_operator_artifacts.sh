#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

usage() {
  cat >&2 <<'EOF'
usage: tools/build_operator_artifacts.sh

Environment:
  PROFILE=operator-release
  TARGET=x86_64-unknown-linux-gnu
  ARTIFACT_ARCH=linux-x86_64
  ARTIFACT_ROOT=artifacts
  CARGO_TARGET_DIR=target
  PACKAGE=copybot-discovery-v2
  BIN_DIR=<derived from PACKAGE>
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

PROFILE="${PROFILE:-operator-release}"
TARGET="${TARGET:-x86_64-unknown-linux-gnu}"
ARTIFACT_ARCH="${ARTIFACT_ARCH:-linux-x86_64}"
ARTIFACT_ROOT="${ARTIFACT_ROOT:-artifacts}"
TARGET_ROOT="${CARGO_TARGET_DIR:-target}"
RUN_CHECKS="${RUN_CHECKS:-1}"
ALLOW_DIRTY="${ALLOW_DIRTY:-0}"
FORCE="${FORCE:-0}"
PACKAGE="${PACKAGE:-copybot-discovery-v2}"

default_bin_dir_for_package() {
  case "$1" in
    copybot-discovery-v2) printf 'crates/discovery-v2/src/bin' ;;
    copybot-discovery-ops) printf 'crates/discovery-ops/src/bin' ;;
    copybot-live-ops) printf 'crates/live-ops/src/bin' ;;
    copybot-operators) printf 'crates/operators/src/bin' ;;
    copybot-storage-ops) printf 'crates/storage-ops/src/bin' ;;
    *)
      echo "unknown operator package: $1; set BIN_DIR and WANTED_BINS explicitly" >&2
      return 1
      ;;
  esac
}

default_bins_for_package() {
  case "$1" in
    copybot-discovery-v2) printf 'discovery_v2_status discovery_v2_publish' ;;
    copybot-discovery-ops) printf 'discovery_runtime_export discovery_recent_raw_snapshot' ;;
    copybot-live-ops) printf 'copybot_operator_emergency_stop copybot_live_service_control_wrapper' ;;
    copybot-operators) printf 'copybot_yellowstone_source_probe' ;;
    copybot-storage-ops) printf 'copybot_runtime_sqlite_wal_maintenance copybot_runtime_sqlite_wal_pressure_report' ;;
    *)
      echo "unknown operator package: $1; set WANTED_BINS explicitly" >&2
      return 1
      ;;
  esac
}

cargo_profile_dir() {
  case "$1" in
    dev) printf 'debug' ;;
    release) printf 'release' ;;
    *) printf '%s' "$1" ;;
  esac
}

BIN_DIR="${BIN_DIR:-$(default_bin_dir_for_package "$PACKAGE")}"
if [ ! -d "$BIN_DIR" ]; then
  echo "missing $BIN_DIR for $PACKAGE; operator artifacts must not fall back to legacy app/discovery bins" >&2
  exit 1
fi

if [ "$ALLOW_DIRTY" != "1" ]; then
  if ! git diff --quiet || ! git diff --cached --quiet || [ -n "$(git ls-files --others --exclude-standard)" ]; then
    echo "refusing to build production artifact from dirty tree; set ALLOW_DIRTY=1 for local smoke artifacts" >&2
    exit 1
  fi
fi

WANTED_BINS="${WANTED_BINS:-$(default_bins_for_package "$PACKAGE")}"
BINS=""
for bin in $WANTED_BINS; do
  if [ -f "$BIN_DIR/$bin.rs" ]; then
    BINS="${BINS:+$BINS }$bin"
  else
    echo "requested operator bin not found for $PACKAGE: $BIN_DIR/$bin.rs" >&2
    exit 1
  fi
done

if [ -z "$BINS" ]; then
  echo "no operator bins found for $PACKAGE in $BIN_DIR" >&2
  exit 1
fi

if [ "$RUN_CHECKS" = "1" ]; then
  if [ -x tools/architecture_guard.sh ]; then
    tools/architecture_guard.sh --changed
  fi
  cargo test -p "$PACKAGE" --all-targets -- --test-threads=1
fi

build_args=(build --target-dir "$TARGET_ROOT" --profile "$PROFILE" --target "$TARGET" -p "$PACKAGE")
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
  fi
  manifest_args+=(--check "cargo test -p $PACKAGE --all-targets -- --test-threads=1")
fi
for bin in $BINS; do
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
