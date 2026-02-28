#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=tools/lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"

EVIDENCE_DIR="${1:-}"
if [[ -z "$EVIDENCE_DIR" ]]; then
  echo "usage: $0 <evidence_dir>" >&2
  exit 1
fi
if [[ ! -d "$EVIDENCE_DIR" ]]; then
  echo "evidence directory not found: $EVIDENCE_DIR" >&2
  exit 1
fi
EVIDENCE_DIR_ABS="$(cd "$EVIDENCE_DIR" && pwd -P)"

OUTPUT_DIR="${OUTPUT_DIR:-$EVIDENCE_DIR}"
BUNDLE_LABEL="${BUNDLE_LABEL:-evidence_bundle}"
BUNDLE_TIMESTAMP_UTC="${BUNDLE_TIMESTAMP_UTC:-}"

if [[ ! "$BUNDLE_LABEL" =~ ^[A-Za-z0-9._-]+$ ]]; then
  echo "BUNDLE_LABEL must match ^[A-Za-z0-9._-]+$ (got: $BUNDLE_LABEL)" >&2
  exit 1
fi

mkdir -p "$OUTPUT_DIR"
OUTPUT_DIR_ABS="$(cd "$OUTPUT_DIR" && pwd -P)"

if [[ -n "$BUNDLE_TIMESTAMP_UTC" ]]; then
  timestamp_compact="$BUNDLE_TIMESTAMP_UTC"
else
  timestamp_compact="$(date -u +"%Y%m%dT%H%M%SZ")"
fi
if [[ ! "$timestamp_compact" =~ ^[0-9]{8}T[0-9]{6}Z$ ]]; then
  echo "BUNDLE_TIMESTAMP_UTC must match ^[0-9]{8}T[0-9]{6}Z$ (got: $timestamp_compact)" >&2
  exit 1
fi

bundle_base="${BUNDLE_LABEL}_${timestamp_compact}"
bundle_candidate="$bundle_base"
bundle_collision_index=0
while [[ -e "$OUTPUT_DIR/${bundle_candidate}.tar.gz" || -e "$OUTPUT_DIR/${bundle_candidate}.sha256" || -e "$OUTPUT_DIR/${bundle_candidate}.contents.sha256" ]]; do
  bundle_collision_index=$((bundle_collision_index + 1))
  bundle_candidate="${bundle_base}_${bundle_collision_index}"
done
bundle_base="$bundle_candidate"
bundle_path="$OUTPUT_DIR_ABS/${bundle_base}.tar.gz"
bundle_sha_path="$OUTPUT_DIR_ABS/${bundle_base}.sha256"
contents_manifest_path="$OUTPUT_DIR_ABS/${bundle_base}.contents.sha256"

tmp_list="$(mktemp)"
tmp_exclude="$(mktemp)"
trap 'rm -f "$tmp_list" "$tmp_exclude"' EXIT

relative_to_evidence_dir() {
  local absolute_path="$1"
  if [[ "$absolute_path" == "$EVIDENCE_DIR_ABS/"* ]]; then
    printf '%s' "${absolute_path#"$EVIDENCE_DIR_ABS"/}"
    return
  fi
  printf ''
}

bundle_index_path="$OUTPUT_DIR_ABS/.copybot_evidence_bundle_outputs.txt"
bundle_index_relative="$(relative_to_evidence_dir "$bundle_index_path")"
if [[ -n "$bundle_index_relative" ]]; then
  printf '%s\n' "$bundle_index_relative" >>"$tmp_exclude"
fi
if [[ -f "$bundle_index_path" ]]; then
  cat "$bundle_index_path" >>"$tmp_exclude"
fi

relative_files=()
while IFS= read -r relative_file; do
  if grep -Fqx -- "$relative_file" "$tmp_exclude"; then
    continue
  fi
  relative_files+=("$relative_file")
done < <(
  cd "$EVIDENCE_DIR"
  find . -type f -print | sed 's#^\./##' | LC_ALL=C sort
)

if ((${#relative_files[@]} == 0)); then
  echo "no evidence files found under: $EVIDENCE_DIR" >&2
  exit 1
fi

printf '%s\n' "${relative_files[@]}" >"$tmp_list"

: >"$contents_manifest_path"
for relative_file in "${relative_files[@]}"; do
  file_sha="$(sha256_file_value "$EVIDENCE_DIR/$relative_file")"
  printf '%s  %s\n' "$file_sha" "$relative_file" >>"$contents_manifest_path"
done

COPYFILE_DISABLE=1 tar -C "$EVIDENCE_DIR" -czf "$bundle_path" -T "$tmp_list"
bundle_sha="$(sha256_file_value "$bundle_path")"
printf '%s  %s\n' "$bundle_sha" "$(basename "$bundle_path")" >"$bundle_sha_path"

bundle_path_relative="$(relative_to_evidence_dir "$bundle_path")"
bundle_sha_path_relative="$(relative_to_evidence_dir "$bundle_sha_path")"
contents_manifest_path_relative="$(relative_to_evidence_dir "$contents_manifest_path")"
if [[ -n "$bundle_index_relative" ]]; then
  {
    if [[ -f "$bundle_index_path" ]]; then
      cat "$bundle_index_path"
    fi
    if [[ -n "$bundle_path_relative" ]]; then
      printf '%s\n' "$bundle_path_relative"
    fi
    if [[ -n "$bundle_sha_path_relative" ]]; then
      printf '%s\n' "$bundle_sha_path_relative"
    fi
    if [[ -n "$contents_manifest_path_relative" ]]; then
      printf '%s\n' "$contents_manifest_path_relative"
    fi
  } | awk 'NF && !seen[$0]++' >"${bundle_index_path}.tmp"
  mv "${bundle_index_path}.tmp" "$bundle_index_path"
fi

echo "artifacts_written: true"
echo "evidence_dir: $EVIDENCE_DIR_ABS"
echo "output_dir: $OUTPUT_DIR_ABS"
echo "bundle_path: $bundle_path"
echo "bundle_sha256: $bundle_sha"
echo "bundle_sha256_path: $bundle_sha_path"
echo "contents_manifest: $contents_manifest_path"
echo "file_count: ${#relative_files[@]}"
