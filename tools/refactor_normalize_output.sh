#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <input> <output>" >&2
  exit 1
fi

input="$1"
output="$2"

if [[ ! -f "$input" ]]; then
  echo "input file not found: $input" >&2
  exit 1
fi

mkdir -p "$(dirname "$output")"

sed -E \
  -e 's#^utc_now: .*#utc_now: <normalized>#' \
  -e 's#^timestamp_utc: .*#timestamp_utc: <normalized>#' \
  -e 's#^(artifact_[^:]+): .*#\1: <normalized>#' \
  -e 's#^go_nogo_nested_capture_path: .*#go_nogo_nested_capture_path: <normalized>#' \
  -e 's#tmp/refactor-baseline/[^/[:space:]]+#tmp/refactor-baseline/<run>#g' \
  -e 's#/tmp/refactor-baseline/[^/[:space:]]+#/tmp/refactor-baseline/<run>#g' \
  "$input" >"$output"
