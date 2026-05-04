#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

usage() {
  cat >&2 <<'EOF'
usage: tools/install_operator_artifacts.sh [options] <artifact-dir-or-tar.gz>

Options:
  --install-dir <path>   default: /var/www/solana-copy-bot/bin
  --expect-package <pkg> default: copybot-discovery-v2
  --expect-profile <p>   default: operator-release
  --dry-run              verify and print install/rollback plan only
  --allow-dirty          allow artifacts marked git_dirty=true
  --rollback <id>        switch symlinks to an installed release id
  --help
EOF
}

install_dir="${INSTALL_DIR:-/var/www/solana-copy-bot/bin}"
expect_package="${EXPECT_PACKAGE:-copybot-discovery-v2}"
expect_profile="${EXPECT_PROFILE:-operator-release}"
dry_run=0
allow_dirty=0
artifact_input=""
rollback_id=""

while [ "$#" -gt 0 ]; do
  case "$1" in
    --install-dir)
      install_dir="${2:?missing --install-dir value}"
      shift 2
      ;;
    --expect-package)
      expect_package="${2:?missing --expect-package value}"
      shift 2
      ;;
    --expect-profile)
      expect_profile="${2:?missing --expect-profile value}"
      shift 2
      ;;
    --dry-run)
      dry_run=1
      shift
      ;;
    --allow-dirty)
      allow_dirty=1
      shift
      ;;
    --rollback)
      rollback_id="${2:?missing --rollback value}"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    -*)
      echo "unknown option: $1" >&2
      usage
      exit 2
      ;;
    *)
      if [ -n "$artifact_input" ]; then
        echo "multiple artifact inputs provided" >&2
        usage
        exit 2
      fi
      artifact_input="$1"
      shift
      ;;
  esac
done

if [ -n "$rollback_id" ] && [ -n "$artifact_input" ]; then
  echo "--rollback does not accept an artifact input" >&2
  exit 2
fi

if [ -z "$artifact_input" ] && [ -z "$rollback_id" ]; then
  artifact_input="${ARTIFACT_DIR:-}"
fi
if [ -z "$artifact_input" ] && [ -z "$rollback_id" ]; then
  echo "missing artifact input" >&2
  usage
  exit 2
fi

case "$expect_package" in
  */*|.*|*..*)
    echo "refusing unsafe package name: $expect_package" >&2
    exit 1
    ;;
esac

tmpdir=""
cleanup() {
  if [ -n "$tmpdir" ]; then
    rm -rf "$tmpdir"
  fi
}
trap cleanup EXIT

if [ -n "$rollback_id" ]; then
  case "$rollback_id" in
    */*|.*|*..*)
      echo "refusing unsafe rollback id: $rollback_id" >&2
      exit 1
      ;;
  esac
  artifact_dir="$install_dir/releases/$rollback_id"
  if [ ! -d "$artifact_dir" ]; then
    echo "missing installed release for rollback: $artifact_dir" >&2
    exit 1
  fi
else
  artifact_dir="$artifact_input"
  case "$artifact_input" in
    *.tar.gz|*.tgz)
      if [ -f "$artifact_input.sha256" ]; then
        archive_dir="$(cd "$(dirname "$artifact_input")" && pwd)"
        archive_name="$(basename "$artifact_input")"
        (
          cd "$archive_dir"
          if command -v sha256sum >/dev/null 2>&1; then
            sha256sum -c "$archive_name.sha256"
          else
            shasum -a 256 -c "$archive_name.sha256"
          fi
        )
      fi
      tmpdir="$(mktemp -d)"
      tar -xzf "$artifact_input" -C "$tmpdir"
      artifact_count="$(find "$tmpdir" -mindepth 1 -maxdepth 1 -type d | wc -l | tr -d ' ')"
      if [ "$artifact_count" != "1" ]; then
        echo "archive must contain exactly one artifact directory" >&2
        exit 1
      fi
      artifact_dir="$(find "$tmpdir" -mindepth 1 -maxdepth 1 -type d | head -n 1)"
      ;;
  esac
fi

verify_args=("$artifact_dir")
verify_args+=(--expect-package "$expect_package")
verify_args+=(--expect-profile "$expect_profile")
if [ "$allow_dirty" = "1" ]; then
  verify_args+=(--allow-dirty)
fi
python3 tools/verify_operator_artifact.py "${verify_args[@]}"

artifact_id="$(python3 - "$artifact_dir/build-manifest.json" <<'PY'
import json, sys
print(json.load(open(sys.argv[1]))["artifact_id"])
PY
)"
bins="$(python3 - "$artifact_dir/build-manifest.json" <<'PY'
import json, sys
for item in json.load(open(sys.argv[1]))["binaries"]:
    print(item["name"])
PY
)"

release_dir="$install_dir/releases/$artifact_id"
echo "artifact_id=$artifact_id"
echo "package=$expect_package"
echo "profile=$expect_profile"
echo "install_dir=$install_dir"
echo "release_dir=$release_dir"
echo "binaries=$(printf '%s' "$bins" | tr '\n' ' ')"

if [ "$dry_run" = "1" ]; then
  if [ -n "$rollback_id" ]; then
    echo "dry-run: rollback skipped"
  else
    echo "dry-run: install skipped"
  fi
  exit 0
fi

mkdir -p "$release_dir"
if [ -z "$rollback_id" ]; then
  cp "$artifact_dir/build-manifest.json" "$release_dir/"
  cp "$artifact_dir/SHA256SUMS" "$release_dir/"
fi

for bin in $bins; do
  case "$bin" in
    */*|.*|*..*)
      echo "refusing unsafe artifact name: $bin" >&2
      exit 1
      ;;
  esac
  if [ -z "$rollback_id" ]; then
    install -m 0755 "$artifact_dir/$bin" "$release_dir/$bin"
  fi
  ln -sfn "releases/$artifact_id/$bin" "$install_dir/$bin.new"
  mv -f "$install_dir/$bin.new" "$install_dir/$bin"
done

current_manifest="$install_dir/operator-artifact-current-$expect_package.json"
cp "$artifact_dir/build-manifest.json" "$current_manifest.new"
mv -f "$current_manifest.new" "$current_manifest"
if [ -n "$rollback_id" ]; then
  echo "rolled back to $artifact_id"
else
  echo "installed $artifact_id"
fi
