#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

run_ops_smoke() {
  if command -v timeout >/dev/null 2>&1; then
    timeout 300 bash tools/ops_scripts_smoke_test.sh
    return
  fi
  bash tools/ops_scripts_smoke_test.sh
}

echo "[audit:full] running quick baseline"
bash tools/audit_quick.sh

echo "[audit:full] cargo test --workspace -q"
cargo test --workspace -q

echo "[audit:full] tools/ops_scripts_smoke_test.sh"
run_ops_smoke

echo "[audit:full] PASS"
