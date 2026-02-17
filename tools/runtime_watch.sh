#!/usr/bin/env bash
set -euo pipefail

INTERVAL_SECONDS="${1:-15}"
WINDOW_HOURS="${2:-24}"

if ! [[ "$INTERVAL_SECONDS" =~ ^[0-9]+$ ]] || (( INTERVAL_SECONDS < 1 )); then
  echo "interval must be integer >= 1 (got: $INTERVAL_SECONDS)" >&2
  exit 1
fi

if ! [[ "$WINDOW_HOURS" =~ ^[0-9]+$ ]] || (( WINDOW_HOURS < 1 )); then
  echo "window hours must be integer >= 1 (got: $WINDOW_HOURS)" >&2
  exit 1
fi

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

while true; do
  clear
  echo "refresh every ${INTERVAL_SECONDS}s | window=${WINDOW_HOURS}h"
  echo
  "$SCRIPT_DIR/runtime_snapshot.sh" "$WINDOW_HOURS"
  sleep "$INTERVAL_SECONDS"
done
