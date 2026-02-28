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

OUTPUT_DIR="${OUTPUT_DIR:-$EVIDENCE_DIR}"
BUNDLE_LABEL="${BUNDLE_LABEL:-evidence_bundle}"
BUNDLE_TIMESTAMP_UTC="${BUNDLE_TIMESTAMP_UTC:-}"

if [[ ! "$BUNDLE_LABEL" =~ ^[A-Za-z0-9._-]+$ ]]; then
  echo "BUNDLE_LABEL must match ^[A-Za-z0-9._-]+$ (got: $BUNDLE_LABEL)" >&2
  exit 1
fi

mkdir -p "$OUTPUT_DIR"

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
bundle_path="$OUTPUT_DIR/${bundle_base}.tar.gz"
bundle_sha_path="$OUTPUT_DIR/${bundle_base}.sha256"
contents_manifest_path="$OUTPUT_DIR/${bundle_base}.contents.sha256"

tmp_list="$(mktemp)"
trap 'rm -f "$tmp_list"' EXIT

relative_files=()
while IFS= read -r relative_file; do
  case "$relative_file" in
    "${BUNDLE_LABEL}"_*.tar.gz|*/"${BUNDLE_LABEL}"_*.tar.gz|\
    "${BUNDLE_LABEL}"_*.sha256|*/"${BUNDLE_LABEL}"_*.sha256|\
    "${BUNDLE_LABEL}"_*.contents.sha256|*/"${BUNDLE_LABEL}"_*.contents.sha256)
      continue
      ;;
  esac
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

echo "artifacts_written: true"
echo "evidence_dir: $EVIDENCE_DIR"
echo "output_dir: $OUTPUT_DIR"
echo "bundle_path: $bundle_path"
echo "bundle_sha256: $bundle_sha"
echo "bundle_sha256_path: $bundle_sha_path"
echo "contents_manifest: $contents_manifest_path"
echo "file_count: ${#relative_files[@]}"
