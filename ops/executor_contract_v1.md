# Executor Contract v1 (Canonical)

Date: 2026-02-24  
Version: v1  
Owner: execution-dev  
Status: frozen for Phase 0 implementation start

## 1) Scope

Этот документ — единственный canonical source-of-truth для `copybot-executor` HTTP-контракта.

## 2) Endpoints

1. `GET /healthz`
2. `POST /simulate`
3. `POST /submit`

## 2.1 `/healthz` response schema

`GET /healthz` should return HTTP 200 JSON with minimum fields:

1. `status` (`ok|degraded`)
2. `contract_version`
3. `enabled_routes`
4. `signer_source` (`kms|file`)
5. optional: `idempotency_store_status`

## 3) Request Contract

## 3.1 `/simulate` request

Required fields:

1. `action = "simulate"`
2. `dry_run = true`
3. `contract_version`
4. `request_id`
5. `signal_id`
6. `side`
7. `token`
8. `notional_sol`
9. `signal_ts` (RFC3339)
10. `route`

## 3.2 `/submit` request

Required fields:

1. `contract_version`
2. `request_id`
3. `signal_id`
4. `client_order_id`
5. `side`
6. `token`
7. `notional_sol`
8. `signal_ts` (RFC3339)
9. `route`
10. `slippage_bps`
11. `route_slippage_cap_bps`
12. `tip_lamports`
13. `compute_budget.cu_limit`
14. `compute_budget.cu_price_micro_lamports`

## 3.3 Compute budget policy (mandatory)

1. Executor MUST treat request compute budget as source-of-truth.
2. Executor MUST NOT silently override `cu_limit`/`cu_price_micro_lamports`.
3. If backend/route cannot satisfy requested compute budget, return terminal reject with `executor_*` code.

## 4) Response Contract

## 4.1 Success envelope

Success MUST include:

1. `status = "ok"`
2. `ok = true`
3. `accepted = true`

## 4.2 `/simulate` success payload

1. Success envelope fields
2. optional: `route`
3. optional: `contract_version`
4. `detail` string (recommended always; actionable simulation reason)

## 4.3 `/submit` success payload

1. Success envelope fields
2. MUST include one of:
   1. `tx_signature`
   2. `signed_tx_base64`
3. optional policy echo:
   1. `route`
   2. `contract_version`
   3. `client_order_id`
   4. `request_id`
4. optional time:
   1. `submitted_at` RFC3339
5. optional fee hints:
   1. `network_fee_lamports`
   2. `base_fee_lamports`
   3. `priority_fee_lamports`
   4. `ata_create_rent_lamports`

## 4.4 Fee consistency rule (mandatory if hints provided)

If `network_fee_lamports` is provided:

1. `network_fee_lamports MUST equal base_fee_lamports + priority_fee_lamports`
2. mismatch is treated by adapter as terminal contract violation

## 4.5 Reject payload

Reject MUST include:

1. `status` in `reject|error|failed` (or equivalent `ok=false`/`accepted=false`)
2. `retryable` boolean
3. `code`
4. `detail`

## 5) Error Taxonomy Boundaries

## 5.1 Executor-emitted domain codes

1. Executor SHOULD use its own domain codes (`executor_*` prefix) for pure executor business logic.
2. Executor retryability is expressed via `retryable`.
3. Compatibility transport/forwarding codes are allowed for adapter/runtime interoperability:
   1. `upstream_*`
   2. `send_rpc_*`
   3. `submit_adapter_*`

## 5.2 Adapter-generated transport codes

Codes below are adapter-layer in runtime semantics and also used as compatibility classes by executor forwarding path:

1. `upstream_*`
2. `send_rpc_*`
3. `submit_adapter_*`

## 5.3 Runtime guarded fallback

Runtime guarded fallback (`jito|fastlane -> rpc`) relies on adapter-layer classification and codes, not raw executor naming.

Intentional policy:

1. `executor_*` retryable codes are business-level.
2. They do not by themselves grant guarded cross-route fallback.
3. Guarded fallback is triggered by adapter-layer transport/service classification (`upstream_*`/`send_rpc_*` compatibility classes).

## 6) Solana Execution Semantics

## 6.1 Transaction pipeline

On submit:

1. route-specific quote/build input resolve
2. build instructions:
   1. swap
   2. compute budget
   3. optional tip
3. optional ATA create instruction
4. recent blockhash fetch
5. sign
6. send via selected route
7. normalize response to contract

Route-specific slippage/tip semantics:

1. `slippage_bps` MUST be propagated into route quote/build path as slippage bound.
2. `tip_lamports`:
   1. `jito|fastlane` routes apply tip according to route instruction model,
   2. `rpc` route treats tip as non-applicable (ignored or coerced to zero by policy with explicit trace).
3. `fastlane` route MUST be explicitly enabled by executor feature flag (`COPYBOT_EXECUTOR_SUBMIT_FASTLANE_ENABLED=true`); otherwise requests to `fastlane` are terminal rejects.

## 6.2 Blockhash lifecycle

1. fetch blockhash per attempt
2. refresh blockhash on retryable resend attempts
3. blockhash-expired classified with explicit `executor_*` code and bounded retry policy

## 6.3 Fee hint computation

If executor emits fee hints, it should compute:

1. `base_fee_lamports` (current Solana base fee baseline)
2. `priority_fee_lamports = cu_limit * cu_price_micro_lamports / 1_000_000`
3. `network_fee_lamports = base_fee_lamports + priority_fee_lamports`
4. `ata_create_rent_lamports` only when ATA create path is actually used

## 6.4 HTTP status code convention

1. Business-level responses (success + reject) MUST use HTTP 200 with JSON body.
2. HTTP 429/5xx are reserved for service-level failures only.
3. Executor MUST NOT encode business reject states as HTTP 5xx.

## 7) Durable Idempotency Requirements

1. Persistent idempotency storage is mandatory.
2. Required keys:
   1. `client_order_id` primary
   2. `request_id` correlation
3. State machine:
   1. `received -> built -> signed -> submitted -> confirmed|failed`

Recovery from partial `signed` state:

1. check signature on-chain (`getSignatureStatuses`)
2. if found -> recover to `submitted|confirmed`
3. if not found and blockhash valid -> controlled re-submit allowed
4. if not found and blockhash expired -> mark failed with explicit `executor_*` reason

## 8) Auth Boundary (adapter -> executor)

Current mandatory mode:

1. Bearer token authentication

Current non-mandatory mode:

1. HMAC for adapter->executor hop is optional until adapter implements upstream HMAC forwarding

## 9) Change Control

Any change to this contract requires:

1. version bump (`v1 -> v2`)
2. compatibility matrix
3. migration note for adapter/runtime
4. auditor sign-off update
