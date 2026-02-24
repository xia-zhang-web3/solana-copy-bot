#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONTRACT_FILE="$ROOT_DIR/ops/executor_contract_v1.md"
PLAN_FILE="$ROOT_DIR/ops/executor_backend_master_plan_2026-02-24.md"

fail() {
  echo "[fail] $1" >&2
  exit 1
}

pass() {
  echo "[ok] $1"
}

[[ -f "$CONTRACT_FILE" ]] || fail "missing canonical contract: $CONTRACT_FILE"
[[ -f "$PLAN_FILE" ]] || fail "missing executor master plan: $PLAN_FILE"

required_contract_patterns=(
  "# Executor Contract v1 \(Canonical\)"
  "## 2\.1 .*healthz.* response schema"
  "## 3\.3 Compute budget policy \(mandatory\)"
  "## 4\.4 Fee consistency rule"
  "## 5\.3 Runtime guarded fallback"
  "## 6\.4 HTTP status code convention"
  "## 7\) Durable Idempotency Requirements"
)

for pattern in "${required_contract_patterns[@]}"; do
  rg -q "$pattern" "$CONTRACT_FILE" || fail "contract missing required section: $pattern"
done
pass "canonical contract sections present"

required_plan_patterns=(
  "# Executor Backend Master Plan \(Rev-4\)"
  "## 8\.3 Error taxonomy and responsibility boundaries"
  "Cross-route guarded fallback policy"
  "## 8\.5 HTTP status convention"
  "## 8\.6 .*healthz.* summary schema"
  "Compute-budget passthrough policy"
  "## 10\) Durable Idempotency Model"
  "tools/executor_final_evidence_report\.sh"
)

for pattern in "${required_plan_patterns[@]}"; do
  rg -q "$pattern" "$PLAN_FILE" || fail "plan missing required section: $pattern"
done
pass "master plan sections present"

cargo test -p copybot-executor -q >/dev/null
pass "copybot-executor tests pass"

contract_guard_tests=(
  "simulate_reject_status_is_http_200_for_retryable_and_terminal"
  "require_authenticated_mode_fails_closed_by_default"
  "resolve_signer_source_config_rejects_keypair_pubkey_mismatch"
)

for test_name in "${contract_guard_tests[@]}"; do
  cargo test -p copybot-executor -q "$test_name" >/dev/null
done
pass "contract guard tests pass"

echo "executor contract smoke: PASS"
