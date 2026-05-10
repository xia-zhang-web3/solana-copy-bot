usage() {
  cat >&2 <<'EOF'
usage: tools/install_operator_artifacts.sh [options] <artifact-dir-or-tar.gz>

Options:
  --install-dir <path>   default: /var/www/solana-copy-bot/bin
  --expect-package <pkg> default: copybot-discovery-v2
  --expect-profile <p>   default: derived from package
  --expect-target <t>    default: x86_64-unknown-linux-gnu
  --dry-run              verify and print install/rollback plan only
  --allow-dirty          allow git_dirty=true artifacts only with --dry-run
  --rollback <id>        switch symlinks to an installed release id
  --help
EOF
}

default_profile_for_package() {
  case "$1" in
    copybot-app) printf 'release' ;;
    *) printf 'operator-release' ;;
  esac
}

host_target() {
  local os arch
  os="$(uname -s)"
  arch="$(uname -m)"
  if [ "$os" = "Linux" ] && [ "$arch" = "x86_64" ]; then
    printf 'x86_64-unknown-linux-gnu'
  else
    printf '%s-%s' "$arch" "$os"
  fi
}

replace_symlink_no_deref() {
  local new_path="$1"
  local dest_path="$2"
  if [ ! -L "$new_path" ]; then
    echo "refusing to activate non-symlink scratch path: $new_path" >&2
    exit 1
  fi
  if mv --help 2>/dev/null | grep -q -- '--no-target-directory'; then
    mv -Tf "$new_path" "$dest_path"
    return
  fi
  if [ -d "$dest_path" ] && [ ! -L "$dest_path" ]; then
    echo "refusing to replace directory path: $dest_path" >&2
    exit 1
  fi
  rm -f "$dest_path"
  mv -f "$new_path" "$dest_path"
}

prepare_scratch_symlink() {
  local target="$1"
  local new_path="$2"
  if [ -L "$new_path" ]; then
    rm -f "$new_path"
  elif [ -e "$new_path" ]; then
    echo "refusing stale non-symlink scratch path: $new_path" >&2
    exit 1
  fi
  ln -s "$target" "$new_path"
}

validate_scratch_symlink_path() {
  local new_path="$1"
  if [ -L "$new_path" ] || [ ! -e "$new_path" ]; then
    return
  fi
  echo "refusing stale non-symlink scratch path before release creation: $new_path" >&2
  exit 1
}

validate_package_current_symlink_target() {
  local link_path="$1"
  local target release_id
  if [ ! -L "$link_path" ]; then
    return
  fi
  target="$(readlink "$link_path")"
  case "$target" in
    ../../releases/*) ;;
    *)
      echo "refusing package current symlink outside releases: $link_path -> $target" >&2
      exit 1
      ;;
  esac
  release_id="${target#../../releases/}"
  case "$release_id" in
    ""|*/*|.*|*..*)
      echo "refusing unsafe package current release target: $link_path -> $target" >&2
      exit 1
      ;;
  esac
}

manifest_binaries() {
  python3 - "$1" <<'PY'
import json, sys
for item in json.load(open(sys.argv[1]))["binaries"]:
    print(item["name"])
PY
}

manifest_migration_bundle() {
  python3 - "$1" <<'PY'
import json, sys
bundle = json.load(open(sys.argv[1])).get("migration_bundle")
if bundle:
    print(bundle.get("name", ""))
PY
}

validate_migration_bundle_members() {
  local bundle="$1"
  if tar -tzf "$bundle" | awk '
    /^$/ { next }
    /^\// { bad=1 }
    /^\.\// { bad=1 }
    /(^|\/)\.\.(\/|$)/ { bad=1 }
    !/^migrations\/[A-Za-z0-9_.-]+\.sql$/ && $0 != "migrations/" { bad=1 }
    /^migrations\/[A-Za-z0-9_.-]+\.sql$/ {
      seen[$0]++
      if (seen[$0] > 1) { bad=1 }
    }
    END { exit bad ? 0 : 1 }
  '; then
    echo "migration bundle contains unsafe member paths: $bundle" >&2
    exit 1
  fi
  if tar -tzvf "$bundle" | awk '$1 ~ /^[lh]/ { bad=1 } END { exit bad ? 0 : 1 }'; then
    echo "migration bundle contains symlink or hardlink members: $bundle" >&2
    exit 1
  fi
  if tar -tzvf "$bundle" | awk '$1 !~ /^[-d]/ { bad=1 } END { exit bad ? 0 : 1 }'; then
    echo "migration bundle contains unsupported special members: $bundle" >&2
    exit 1
  fi
}

extract_release_migration_bundle_if_present() {
  local package="$1"
  local release_dir="$2"
  local bundle_name bundle staging_dir
  bundle_name="$(manifest_migration_bundle "$release_dir/build-manifest.json")"
  if [ "$package" = "copybot-app" ] && [ -z "$bundle_name" ]; then
    echo "copybot-app release missing required migration bundle" >&2
    exit 1
  fi
  if [ "$package" != "copybot-app" ] && [ -z "$bundle_name" ]; then
    return
  fi
  if [ "$package" != "copybot-app" ]; then
    echo "only copybot-app releases may install migration bundles" >&2
    exit 1
  fi
  if [ "$bundle_name" != "migrations.tar.gz" ]; then
    echo "unsupported migration bundle name: $bundle_name" >&2
    exit 1
  fi
  bundle="$release_dir/$bundle_name"
  if [ ! -f "$bundle" ]; then
    echo "release migration bundle missing: $bundle" >&2
    exit 1
  fi
  validate_migration_bundle_members "$bundle"
  if [ -e "$release_dir/migrations" ] || [ -L "$release_dir/migrations" ]; then
    if [ -L "$release_dir/migrations" ] || [ ! -d "$release_dir/migrations" ]; then
      echo "installed release migrations path is not a plain directory: $release_dir/migrations" >&2
      exit 1
    fi
    if find "$release_dir/migrations" -type l -print -quit | grep -q .; then
      echo "installed release migrations directory contains symlinks: $release_dir/migrations" >&2
      exit 1
    fi
    return
  fi
  staging_dir="$release_dir/.migrations-extract-$$"
  rm -rf "$staging_dir"
  mkdir -p "$staging_dir"
  tar -xzf "$bundle" -C "$staging_dir"
  if [ ! -d "$staging_dir/migrations" ]; then
    echo "migration bundle did not extract a migrations directory" >&2
    rm -rf "$staging_dir"
    exit 1
  fi
  if find "$staging_dir/migrations" -type l -print -quit | grep -q .; then
    echo "migration bundle extracted symlink members: $bundle" >&2
    rm -rf "$staging_dir"
    exit 1
  fi
  mv "$staging_dir/migrations" "$release_dir/migrations"
  rm -rf "$staging_dir"
}

app_migrations_link_path() {
  local install_dir="$1"
  local app_root
  app_root="$(cd "$install_dir/.." && pwd)"
  printf '%s/migrations' "$app_root"
}

app_migrations_link_target() {
  local install_dir="$1"
  local install_name normalized
  normalized="${install_dir%/}"
  while [ "$(basename "$normalized")" = "." ]; do
    normalized="$(dirname "$normalized")"
    normalized="${normalized%/}"
  done
  install_name="$(basename "$normalized")"
  printf '%s/packages/copybot-app/current/migrations' "$install_name"
}

validate_app_migrations_link() {
  local package="$1"
  local install_dir="$2"
  local expected_target link_path target
  if [ "$package" != "copybot-app" ]; then
    return
  fi
  link_path="$(app_migrations_link_path "$install_dir")"
  expected_target="$(app_migrations_link_target "$install_dir")"
  if [ ! -e "$link_path" ] && [ ! -L "$link_path" ]; then
    return
  fi
  if [ ! -L "$link_path" ]; then
    echo "refusing to replace non-symlink app migrations path: $link_path" >&2
    exit 1
  fi
  target="$(readlink "$link_path")"
  if [ "$target" != "$expected_target" ]; then
    echo "refusing app migrations symlink with unexpected target: $link_path -> $target" >&2
    exit 1
  fi
}

validate_app_migrations_scratch_link() {
  local package="$1"
  local install_dir="$2"
  local link_path
  if [ "$package" != "copybot-app" ]; then
    return
  fi
  link_path="$(app_migrations_link_path "$install_dir").new"
  validate_scratch_symlink_path "$link_path"
}

prepare_app_migrations_scratch_link() {
  local package="$1"
  local install_dir="$2"
  local link_path
  if [ "$package" != "copybot-app" ]; then
    return
  fi
  link_path="$(app_migrations_link_path "$install_dir").new"
  prepare_scratch_symlink "$(app_migrations_link_target "$install_dir")" "$link_path"
}

replace_app_migrations_link() {
  local package="$1"
  local install_dir="$2"
  local link_path
  if [ "$package" != "copybot-app" ]; then
    return
  fi
  link_path="$(app_migrations_link_path "$install_dir")"
  replace_symlink_no_deref "$link_path.new" "$link_path"
}

word_list_contains() {
  local needle="$1"
  local item
  shift
  for item in "$@"; do
    if [ "$item" = "$needle" ]; then
      return 0
    fi
  done
  return 1
}

cleanup() {
  if [ -n "$tmpdir" ]; then
    rm -rf "$tmpdir"
  fi
  if [ -n "$staging_dir" ]; then
    rm -rf "$staging_dir"
  fi
  if [ -n "$lock_dir" ]; then
    rmdir "$lock_dir" 2>/dev/null || true
  fi
}
