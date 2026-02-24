#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "[audit:quick] cargo test -p copybot-executor -q"
cargo test -p copybot-executor -q

echo "[audit:quick] bash tools/executor_contract_smoke_test.sh"
bash tools/executor_contract_smoke_test.sh

echo "[audit:quick] PASS"
