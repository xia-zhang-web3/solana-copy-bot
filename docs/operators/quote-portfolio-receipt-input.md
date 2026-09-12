# Batch 125 frozen input contract (before source edits)

Version stays 1; all existing scalar103 values/outcomes are preserved. The optional
receipt_ref has exactly order_id, tx_signature, wallet, payer, operation_at,
recorded_at and source_provenance (existing Origin); absent fields serialize absent.

Scalar action example:
```json
{"kind":"failed_attempt_expense","amount":{"value":{"known":"5000"},"provenance":{"synthetic":"caller scenario fee"}}}
```
Receipt-bound example (synthetic identities, not chain evidence):
```json
{"kind":"failed_attempt_expense","amount":{"value":{"known":"5000"},"provenance":{"synthetic":"equality assertion"}},"receipt_ref":{"order_id":"exec-canary:synthetic-a","tx_signature":"1111111111111111111111111111111111111111111111111111111111111111","wallet":"11111111111111111111111111111111","payer":"11111111111111111111111111111111","operation_at":"2026-09-12T00:00:00Z","recorded_at":"2026-09-12T00:00:01Z","source_provenance":{"synthetic":"hermetic canonical-writer fixture"}}}
```
The amount is a canonical-u64 equality assertion against validated wallet fee,
never an override. Fee already includes priority; native delta is disclosure only.
Opening cash is before the modeled expense; post-fee wallet balance is not the
oracle opening. Event/position/cash/cohort associations remain caller assertions;
receipt wallet is unrelated to source-leader QuoteRef.wallet_id. No inference from
order token to virtual position. No full cash history or real trade causation proof.

## Errors fixed before implementation

Structural input errors => unavailable report, explicit Unknown dataset coverage,
no partial book: malformed JSON/unknown fields, >256 events, invalid canonical-u64
Known operands, empty provenance/reference identity, mixed scalar/bound expenses,
multiple wallet/payer pairs, wallet!=payer (unsupported scope), one signature
relabelled under different id/position/order/amount/time. Duplicate policy compares
the entire caller event before lookups; exact duplicate retains historical103
outcome. A duplicate cannot silently skip or debit twice. No mode chosen by lookup.

Evidence resolution errors => source_binding unavailable plus Unknown failed expense
passed to unchanged kernel103: absent/Unknown assertion, missing DB/order/task/facts/
ledger/schema, corrupt row, failed shared validator, identity/amount mismatch,
invalid or mismatched claimed timestamps, invalid chronology. Cash remains subtotal,
expense is not debited and sticky subsequent BUY Unknown remains. If DB itself
cannot open, the existing report unavailable path applies. No scalar/zero fallback.

Chronology uses exact UTC instants (RFC3339, no millisecond truncation):
window.start <= original order submit operation_at <= ledger recorded_at <=
event.unix_ms <= window.end. Equal instants with different offsets accepted;
+1ns beyond event refused. This is evidence availability, not TTL or external clock
accuracy. Pending with fee known/native unknown may debit validated fee; native
and residual remain separately unknown and no complete reconciliation is asserted.

## Resolution and output

One bounded read-only order_id method reuses report_row::validated_row; row and
ledger recorded_at share one read transaction. It also checks unique ledger payment.
Existing report/cohort/risk readers and all kernel103 source stay unchanged.
source_binding labels scalar caller assertion or receipt_bound, separates caller
assertions from validated payment facts (identities, exact fee/times), preserves
source_provenance including synthetic basis and discloses native/residual coverage.
No DB table presence upgrades provenance or proves current-network receipt truth.
Dataset/full coverage remains Unknown; production_green=false and independent quote
plus rent book value scope remain. No TTL, policy, app, migration, config, dependency,
new binary, network/sign/send/trading/commit/rollout changes.

Build: one offline locked private dev Cargo operators lib + exactly
copybot_execution_canary_quote_pnl and copybot_execution_tiny_economics; external
selected test harnesses via the same Cargo JSON rlibs/rustc (avoid auto-building all
operator bins). At most one corrective rebuild, only after recorded RED and reason.
Consumer check only affected storage-core/operators lib and these two bins.
