#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# shellcheck source=tools/lib/operator_artifact_install.sh
source "$ROOT/tools/lib/operator_artifact_install.sh"

install_dir="${INSTALL_DIR:-/var/www/solana-copy-bot/bin}"
expect_package="${EXPECT_PACKAGE:-copybot-discovery-v2}"
expect_profile="${EXPECT_PROFILE:-}"
expect_target="${EXPECT_TARGET:-x86_64-unknown-linux-gnu}"
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
    --expect-target)
      expect_target="${2:?missing --expect-target value}"
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

if [ "$allow_dirty" = "1" ] && [ "$dry_run" != "1" ]; then
  echo "--allow-dirty is restricted to local --dry-run artifact smoke checks" >&2
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
case "$expect_target" in
  x86_64-unknown-linux-gnu) ;;
  *)
    echo "refusing unsupported production artifact target: $expect_target" >&2
    exit 1
    ;;
esac

default_profile="$(default_profile_for_package "$expect_package")"
if [ -z "$expect_profile" ]; then
  expect_profile="$default_profile"
fi
if [ "$expect_profile" != "$default_profile" ]; then
  echo "$expect_package artifacts must use profile $default_profile, got $expect_profile" >&2
  exit 1
fi

if [ "$(host_target)" != "$expect_target" ]; then
  echo "host target $(host_target) does not match expected artifact target $expect_target" >&2
  exit 1
fi

tmpdir=""
lock_dir=""
staging_dir=""
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
  if [ ! -f "$artifact_dir/INSTALL_COMPLETE" ]; then
    echo "installed release is incomplete and cannot be rolled back to: $artifact_dir" >&2
    exit 1
  fi
  completed_id="$(cat "$artifact_dir/INSTALL_COMPLETE")"
  if [ "$completed_id" != "$rollback_id" ]; then
    echo "rollback id $rollback_id does not match INSTALL_COMPLETE marker $completed_id" >&2
    exit 1
  fi
else
  artifact_dir="$artifact_input"
  case "$artifact_input" in
    *.tar.gz|*.tgz)
      if [ ! -f "$artifact_input.sha256" ]; then
        echo "missing required archive checksum sidecar: $artifact_input.sha256" >&2
        exit 1
      fi
      archive_dir="$(cd "$(dirname "$artifact_input")" && pwd)"
      archive_name="$(basename "$artifact_input")"
      sidecar_lines="$(wc -l < "$artifact_input.sha256" | tr -d ' ')"
      if [ "$sidecar_lines" != "1" ]; then
        echo "archive checksum sidecar must contain exactly one entry" >&2
        exit 1
      fi
      sidecar_name="$(awk '{ print $2 }' "$artifact_input.sha256" | sed 's/^\*//')"
      if [ "$sidecar_name" != "$archive_name" ]; then
        echo "archive checksum sidecar entry $sidecar_name does not match $archive_name" >&2
        exit 1
      fi
      (
        cd "$archive_dir"
        if command -v sha256sum >/dev/null 2>&1; then
          sha256sum -c "$archive_name.sha256"
        else
          shasum -a 256 -c "$archive_name.sha256"
        fi
      )
      archive_members="$(tar -tzf "$artifact_input")"
      if printf '%s\n' "$archive_members" | awk '
        /^$/ { next }
        seen[$0]++ { duplicate=1 }
        END { exit duplicate ? 0 : 1 }
      '; then
        echo "archive contains duplicate member paths" >&2
        exit 1
      fi
      if printf '%s\n' "$archive_members" | awk '
        /^$/ { next }
        /^\// { bad=1 }
        /^\.\// { bad=1 }
        /\/\.\// { bad=1 }
        /\/\// { bad=1 }
        $0 == "." { bad=1 }
        /(^|\/)\.(\/|$)/ { bad=1 }
        /(^|\/)\.\.(\/|$)/ { bad=1 }
        END { exit bad ? 0 : 1 }
      '; then
        echo "archive contains unsafe member paths" >&2
        exit 1
      fi
      if tar -tzvf "$artifact_input" | awk '$1 ~ /^[lh]/ { bad=1 } END { exit bad ? 0 : 1 }'; then
        echo "archive contains symlink or hardlink members" >&2
        exit 1
      fi
      if tar -tzvf "$artifact_input" | awk '$1 !~ /^[-d]/ { bad=1 } END { exit bad ? 0 : 1 }'; then
        echo "archive contains unsupported special members" >&2
        exit 1
      fi
      archive_top_level_count="$(printf '%s\n' "$archive_members" | awk -F/ '
        NF && $1 != "" { top[$1]=1 }
        END {
          for (name in top) {
            count++
          }
          print count + 0
        }
      ')"
      if [ "$archive_top_level_count" != "1" ]; then
        echo "archive must contain exactly one top-level artifact directory" >&2
        exit 1
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
verify_args+=(--expect-target "$expect_target")
if [ "$allow_dirty" = "1" ]; then
  verify_args+=(--allow-dirty)
fi
if [ -n "$rollback_id" ]; then
  verify_args+=(--skip-workspace-bin-check)
  verify_args+=(--installed-release-root "$install_dir/releases")
fi
python3 tools/verify_operator_artifact.py "${verify_args[@]}"

artifact_id="$(python3 - "$artifact_dir/build-manifest.json" <<'PY'
import json, sys
print(json.load(open(sys.argv[1]))["artifact_id"])
PY
)"
if [ -n "$rollback_id" ] && [ "$artifact_id" != "$rollback_id" ]; then
  echo "rollback id $rollback_id does not match release manifest artifact_id $artifact_id" >&2
  exit 1
fi
bins="$(python3 - "$artifact_dir/build-manifest.json" <<'PY'
import json, sys
for item in json.load(open(sys.argv[1]))["binaries"]:
    print(item["name"])
PY
)"
migration_bundle="$(manifest_migration_bundle "$artifact_dir/build-manifest.json")"
if [ "$expect_package" = "copybot-app" ] && [ -z "$migration_bundle" ]; then
  echo "copybot-app artifact missing required migration bundle" >&2
  exit 1
fi

release_dir="$install_dir/releases/$artifact_id"
package_dir="$install_dir/packages/$expect_package"
package_current_link="$package_dir/current"
package_current_target="../../releases/$artifact_id"
current_manifest="$install_dir/operator-artifact-current-$expect_package.json"
echo "artifact_id=$artifact_id"
echo "package=$expect_package"
echo "profile=$expect_profile"
echo "target=$expect_target"
echo "install_dir=$install_dir"
echo "release_dir=$release_dir"
echo "package_current=$package_current_link -> $package_current_target"
echo "binaries=$(printf '%s' "$bins" | tr '\n' ' ')"
if [ -n "$migration_bundle" ]; then
  echo "migration_bundle=$migration_bundle"
fi

if [ -z "$rollback_id" ] && [ -e "$release_dir" ]; then
  echo "refusing to overwrite existing immutable release directory: $release_dir" >&2
  exit 1
fi

if [ "$dry_run" != "1" ]; then
  mkdir -p "$install_dir"
  lock_dir="$install_dir/.install-$expect_package.lock"
  if ! mkdir "$lock_dir" 2>/dev/null; then
    echo "another install appears to be active for $expect_package: $lock_dir" >&2
    exit 1
  fi

  mkdir -p "$install_dir/releases" "$package_dir"
  if [ -z "$rollback_id" ]; then
    staging_dir="$install_dir/releases/.staging-$artifact_id.$$"
    if [ -e "$staging_dir" ]; then
      echo "refusing to reuse existing staging directory: $staging_dir" >&2
      exit 1
    fi
    mkdir -p "$staging_dir"
    cp "$artifact_dir/build-manifest.json" "$staging_dir/"
    cp "$artifact_dir/SHA256SUMS" "$staging_dir/"
    if [ -n "$migration_bundle" ]; then
      cp "$artifact_dir/$migration_bundle" "$staging_dir/"
    fi
  fi
fi

for bin in $bins; do
  case "$bin" in
    */*|.*|*..*)
      echo "refusing unsafe artifact name: $bin" >&2
      exit 1
      ;;
  esac
  if [ -e "$install_dir/$bin" ] && [ ! -L "$install_dir/$bin" ]; then
    echo "refusing to replace non-symlink binary path: $install_dir/$bin" >&2
    exit 1
  fi
done

if [ -e "$package_current_link" ] && [ ! -L "$package_current_link" ]; then
  echo "refusing to replace non-symlink package current path: $package_current_link" >&2
  exit 1
fi
if [ -L "$package_current_link" ] && [ ! -e "$package_current_link" ]; then
  echo "refusing to activate over broken package current symlink: $package_current_link" >&2
  exit 1
fi
validate_package_current_symlink_target "$package_current_link"
if { [ -e "$current_manifest" ] || [ -L "$current_manifest" ]; } && [ ! -L "$current_manifest" ]; then
  echo "refusing to replace non-symlink current manifest path: $current_manifest" >&2
  exit 1
fi
if [ -L "$current_manifest" ] && [ ! -e "$current_manifest" ]; then
  echo "refusing to activate over broken current manifest symlink: $current_manifest" >&2
  exit 1
fi
validate_app_migrations_link "$expect_package" "$install_dir"

previous_bins=""
if [ -L "$package_current_link" ]; then
  if [ ! -f "$package_current_link/build-manifest.json" ]; then
    echo "refusing activation from unreadable previous package manifest: $package_current_link/build-manifest.json" >&2
    exit 1
  fi
  previous_bins="$(manifest_binaries "$package_current_link/build-manifest.json")"
fi

new_bins=()
for bin in $bins; do
  new_bins+=("$bin")
done

for previous_bin in $previous_bins; do
  if word_list_contains "$previous_bin" "${new_bins[@]}"; then
    continue
  fi
  previous_path="$install_dir/$previous_bin"
  if [ -L "$previous_path" ]; then
    previous_target="$(readlink "$previous_path")"
    if [ "$previous_target" != "packages/$expect_package/current/$previous_bin" ]; then
      echo "refusing stale symlink with unexpected target: $previous_path -> $previous_target" >&2
      exit 1
    fi
  elif [ -e "$previous_path" ]; then
    echo "refusing to remove stale non-symlink binary path before activation: $previous_path" >&2
    exit 1
  fi
done

for bin in $bins; do
  validate_scratch_symlink_path "$install_dir/$bin.new"
done
validate_scratch_symlink_path "$package_current_link.new"
validate_scratch_symlink_path "$current_manifest.new"
validate_app_migrations_scratch_link "$expect_package" "$install_dir"

if [ "$dry_run" = "1" ]; then
  if [ -n "$rollback_id" ]; then
    echo "dry-run: rollback skipped after install-dir safety validation"
  else
    echo "dry-run: install skipped after install-dir safety validation"
  fi
  exit 0
fi

if [ -z "$rollback_id" ]; then
  for bin in $bins; do
    install -m 0755 "$artifact_dir/$bin" "$staging_dir/$bin"
  done
  extract_release_migration_bundle_if_present "$expect_package" "$staging_dir"
  printf '%s\n' "$artifact_id" > "$staging_dir/INSTALL_COMPLETE"
  mv "$staging_dir" "$release_dir"
  staging_dir=""
else
  extract_release_migration_bundle_if_present "$expect_package" "$release_dir"
fi

for bin in $bins; do
  prepare_scratch_symlink "packages/$expect_package/current/$bin" "$install_dir/$bin.new"
done
prepare_scratch_symlink "$package_current_target" "$package_current_link.new"
prepare_scratch_symlink "packages/$expect_package/current/build-manifest.json" "$current_manifest.new"
prepare_app_migrations_scratch_link "$expect_package" "$install_dir"
replace_app_migrations_link "$expect_package" "$install_dir"
replace_symlink_no_deref "$current_manifest.new" "$current_manifest"
for bin in $bins; do
  replace_symlink_no_deref "$install_dir/$bin.new" "$install_dir/$bin"
done
replace_symlink_no_deref "$package_current_link.new" "$package_current_link"

for previous_bin in $previous_bins; do
  if word_list_contains "$previous_bin" "${new_bins[@]}"; then
    continue
  fi
  previous_path="$install_dir/$previous_bin"
  if [ -L "$previous_path" ]; then
    previous_target="$(readlink "$previous_path")"
    if [ "$previous_target" = "packages/$expect_package/current/$previous_bin" ]; then
      rm -f "$previous_path"
    fi
  elif [ -e "$previous_path" ]; then
    echo "refusing to remove stale non-symlink binary path: $previous_path" >&2
    exit 1
  fi
done

if [ -n "$rollback_id" ]; then
  echo "rolled back to $artifact_id"
else
  echo "installed $artifact_id"
fi
