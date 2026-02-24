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
[[ -f "$ROOT_DIR/tools/executor_rollout_evidence_report.sh" ]] || fail "missing helper: tools/executor_rollout_evidence_report.sh"
[[ -f "$ROOT_DIR/tools/executor_final_evidence_report.sh" ]] || fail "missing helper: tools/executor_final_evidence_report.sh"
pass "executor evidence helpers present"

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
  "handle_simulate_rejects_empty_request_id"
  "handle_simulate_rejects_empty_signal_id"
  "handle_simulate_rejects_upstream_route_mismatch"
  "validate_fastlane_route_policy_enforces_feature_gate"
  "validate_common_contract_rejects_fastlane_when_feature_disabled"
  "common_contract_validation_rejects_invalid_side"
  "contract_version_token_validation"
  "parse_route_allowlist_rejects_unknown_route"
  "route_allowlist_parse_rejects_unknown_route"
  "key_validation_accepts_pubkey_and_signature_shapes"
  "constant_time_eq_checks_content"
  "to_hex_lower_matches_expected"
  "parse_bool_token_accepts_true_forms"
  "parse_bool_token_rejects_false_forms"
  "normalize_route_trims_and_lowercases"
  "handle_submit_forces_rpc_tip_to_zero_and_emits_trace"
  "handle_submit_allows_rpc_tip_when_nonzero_tip_disabled"
  "handle_submit_rejects_compute_budget_limit_out_of_range"
  "handle_submit_rejects_compute_budget_price_out_of_range"
  "handle_submit_rejects_slippage_bps_out_of_range"
  "handle_submit_rejects_slippage_exceeding_route_cap"
  "handle_submit_rejects_when_upstream_missing_transport_artifacts"
  "handle_submit_rejects_invalid_submitted_at_in_upstream_response"
  "handle_submit_rejects_invalid_fee_hint_field_type_from_upstream_response"
  "handle_submit_returns_cached_response_for_duplicate_client_order_id"
  "handle_submit_rejects_parallel_duplicate_client_order_id_in_flight"
  "handle_submit_returns_canonical_cached_response_when_store_conflicts"
  "handle_submit_rejects_when_submit_deadline_budget_exhausted"
  "store_persists_across_store_reopen"
  "store_does_not_overwrite_existing_response"
  "claim_flow_returns_claimed_inflight_then_cached"
  "release_claim_requires_request_id_owner_match"
  "min_claim_ttl_sec_for_submit_path_applies_500ms_runtime_floor"
  "route_backend_send_rpc_endpoint_chain_checked_rejects_fallback_without_primary"
  "send_signed_transaction_via_rpc_treats_blockhash_expired_payload_as_terminal"
  "send_signed_transaction_via_rpc_rejects_fallback_without_primary_url"
  "resolve_fee_hints_rejects_derived_priority_fee_overflow"
  "parse_response_fee_hint_fields_rejects_invalid_field_type"
  "submit_transport_extract_rejects_missing_artifacts"
  "parse_rfc3339_utc_parses_valid_timestamp"
  "submit_response_resolve_submitted_at_rejects_invalid_rfc3339"
  "submit_payload_includes_tip_policy_when_present"
  "simulate_response_validation_rejects_route_mismatch"
  "submit_deadline_remaining_timeout_rejects_when_budget_exhausted"
  "upstream_outcome_rejects_unknown_status"
)

for test_name in "${contract_guard_tests[@]}"; do
  cargo test -p copybot-executor -q "$test_name" >/dev/null
done
pass "contract guard tests pass"

echo "executor contract smoke: PASS"
