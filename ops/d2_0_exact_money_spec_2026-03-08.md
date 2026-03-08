# D2-0 Exact Money Specification

Status: draft for auditor review  
Date: 2026-03-08  
Scope owner: codex batch  
Audit source: `TEMP_CONSOLIDATED_AUDIT_2026-03-05.md`, section `D2-0`

## 1. Purpose

This document is the field-by-field mapping required before opening the larger `D-2` exact-money refactor.

It does not reopen `PRED2-3`.
It is safe to review in parallel with the remaining server evidence work.
Implementation batches after this spec should stay small and preserve reversibility.

## 2. Locked Decisions

1. Exact source-of-truth money state is:
   - SOL amounts as integer lamports
   - token quantities as raw integer units plus explicit decimals
2. `REAL` / `f64` columns may remain temporarily as compatibility or display mirrors, but not as post-cutover source-of-truth.
3. Post-cutover rows count as exact only when all required exact sidecars for that surface are populated.
4. Pre-cutover legacy `REAL` rows must not be dishonestly backfilled into fake exact token quantities or fake exact historical cost basis.
5. Synthetic or priced-intent live fills must not create post-cutover rows that pretend to have exact token quantity if no observed exact quantity exists.
6. Post-cutover live and shadow source-of-truth rows fail closed on missing exact quantity:
   - either exact ingress data exists and the row is exact
   - or the row does not enter exact accounting state yet
7. Exact and legacy-open inventory must not be merged into the same source-of-truth bucket across cutover.
8. Human-readable config may stay in SOL-denominated `f64` TOML/env form, but runtime monetary comparisons must use normalized exact values.
9. Ratios and analytics may remain `f64`:
   - win rate
   - score
   - tradable ratio
   - rug ratio
   - slippage basis points
   - drawdown and loss percentages
   - derived display prices

## 3. Current Exact Surfaces Already Landed

The repo already has meaningful exact-money groundwork:

1. Runtime primitives:
   - `Lamports`
   - `SignedLamports`
   - `TokenQuantity`
2. Ingress exact quantity sidecars:
   - `observed_swaps.qty_in_raw`
   - `observed_swaps.qty_in_decimals`
   - `observed_swaps.qty_out_raw`
   - `observed_swaps.qty_out_decimals`
3. Execution exact sidecars:
   - `fills.notional_lamports`
   - `fills.fee_lamports`
   - `fills.qty_raw`
   - `fills.qty_decimals`
   - `positions.cost_lamports`
   - `positions.pnl_lamports`
   - `positions.qty_raw`
   - `positions.qty_decimals`
4. Shadow exact sidecars:
   - `shadow_lots.cost_lamports`
   - `shadow_lots.qty_raw`
   - `shadow_lots.qty_decimals`
   - `shadow_closed_trades.entry_cost_lamports`
   - `shadow_closed_trades.exit_value_lamports`
   - `shadow_closed_trades.pnl_lamports`
   - `shadow_closed_trades.qty_raw`
   - `shadow_closed_trades.qty_decimals`
5. Cutover and audit tooling:
   - `exact_money_cutover_state`
   - `tools/mark_exact_money_cutover.py`
   - `tools/exact_money_coverage_report.py`

## 4. Field Mapping

### 4.1 Ingress and Signal Boundary

| Field / surface | Current type | Target type | Post-D2 source-of-truth | Migration / cutover requirement | Boundary / rounding policy |
| --- | --- | --- | --- | --- | --- |
| `SwapEvent.amount_in`, `SwapEvent.amount_out` | `f64` | keep as derived display mirror | not source-of-truth after exact parse succeeds | no schema migration; runtime contract only | derive from raw units and decimals only; never reconstruct raw units from these floats |
| `SwapEvent.exact_amounts` | `Option<ExactSwapAmounts>` | `Option<ExactSwapAmounts>` initially, later mandatory wherever parser can prove both sides | exact ingress quantity source | no schema migration; parser coverage tightening | raw token units copied verbatim from upstream payloads; post-cutover supported live paths must not finalize exact accounting rows when exact ingress data is absent |
| `observed_swaps.qty_in`, `observed_swaps.qty_out` | SQLite `REAL` | keep as legacy/display mirror through cutover | not source-of-truth post-cutover | existing rows remain legacy; no lossy backfill into exact | post-cutover mirror must be derived from raw+decimals |
| `observed_swaps.qty_in_raw`, `qty_in_decimals`, `qty_out_raw`, `qty_out_decimals` | `TEXT` + `INTEGER` | unchanged | exact ingress quantity source in storage | already landed; coverage report must treat all-or-nothing presence as exact | raw values stored verbatim; decimals must match parser output |
| `ShadowCandidate.leader_notional_sol` | `f64` | derived mirror | not source-of-truth when exact SOL leg exists | runtime-only | derive from `exact_leader_notional_lamports` for display/logging only |
| `ShadowCandidate.exact_leader_notional_lamports` | `Option<Lamports>` | unchanged | leader SOL notional source in shadow sizing | runtime-only | SOL leg must use 9-decimal lamport representation |
| `copy_signals.notional_sol` | SQLite `REAL` / runtime `f64` | add `notional_lamports INTEGER`, keep `notional_sol` mirror | `copy_signals.notional_lamports` only when exact-provenance says the leader SOL leg was exact | schema migration required; post-cutover writes must populate both for exact signals | if exact leader SOL leg exists, clamp once at lamport precision: `copy_notional_lamports = min(config_copy_lamports, leader_notional_lamports)`; only then derive `notional_sol` mirror and any mirrored qty scaling |
| `copy_signals.notional_origin` | not present | add `TEXT NOT NULL` | durable exactness provenance for signal notional | schema migration required before `copy_signals` can participate in exact coverage | required values are `leader_exact_lamports` and `leader_approximate`; rows with `leader_approximate` may keep a compatibility `notional_sol` / optional lamport mirror for workflow, but they are explicit approximate rows, excluded from exact coverage, and must not seed exact shadow qty |
| planned token qty in `copy_signals` | not stored | do not add in first exact pass | no signal-level qty source-of-truth required in v1 | no migration in initial batch | execution truth comes from confirmed fill qty; shadow truth is stored in shadow lots/trades |

### 4.2 Config and Runtime Boundary

| Field / surface | Current type | Target type | Post-D2 source-of-truth | Migration / cutover requirement | Boundary / rounding policy |
| --- | --- | --- | --- | --- | --- |
| `ObservedExecutionFill.signer_balance_delta_lamports` | runtime `i64` | runtime `SignedLamports` wrapper | observed SOL delta source in confirm path | runtime refactor only | keep signed delta exact end-to-end; never rederive from `notional_sol` display value |
| `ObservedExecutionFill.token_delta_qty` + `token_delta_exact` | `f64` + `Option<TokenQuantity>` | keep display `f64`, exact `TokenQuantity` becomes preferred runtime source | `token_delta_exact` when present; otherwise explicitly approximate | runtime policy tightening only | confirm path may use float qty for approximate handling, but post-cutover exact rows must come only from exact token quantity |
| `ShadowConfig.copy_notional_sol` | config `f64` | runtime normalized `Lamports` plus optional display mirror | normalized lamports at runtime | no DB migration; config/runtime refactor | floor to lamports when sizing copied notional to avoid oversizing beyond configured SOL budget |
| `ShadowConfig.min_leader_notional_sol` | config `f64` | runtime normalized `Lamports` | normalized lamports at runtime | no DB migration | compare leader notional using exact lamports when exact SOL leg exists; fallback float path remains explicitly approximate |
| `RiskConfig.shadow_soft_exposure_cap_sol` | config `f64` | runtime normalized `Lamports` | normalized lamports at runtime | no DB migration | floor to lamports for cap enforcement in shadow risk state |
| `RiskConfig.shadow_hard_exposure_cap_sol` | config `f64` | runtime normalized `Lamports` | normalized lamports at runtime | no DB migration | floor to lamports for hard-stop enforcement |
| `RiskConfig.shadow_drawdown_1h_stop_sol`, `shadow_drawdown_6h_stop_sol`, `shadow_drawdown_24h_stop_sol` | config `f64` | runtime normalized signed lamport thresholds | normalized signed lamport thresholds at runtime | no DB migration | negative SOL stop values convert once into signed lamport deltas; comparisons must not be repeated on float boundaries |
| `ExecutionConfig.pretrade_min_sol_reserve` | config `f64` | runtime normalized `Lamports` | normalized lamports at runtime | no DB migration | reserve checks use exact lamports only |
| `RiskConfig.max_position_sol` | config `f64` | runtime normalized `Lamports` | normalized lamports at runtime | no DB migration | floor to lamports for caps so runtime never exceeds configured hard limit |
| `RiskConfig.max_total_exposure_sol` | config `f64` | runtime normalized `Lamports` | normalized lamports at runtime | no DB migration | floor to lamports for caps |
| `RiskConfig.max_exposure_per_token_sol` | config `f64` | runtime normalized `Lamports` | normalized lamports at runtime | no DB migration | floor to lamports for caps |
| `ExecutionConfirmStateSnapshot.total_exposure_sol`, `token_exposure_sol` | runtime `f64` | add `total_exposure_lamports` / `token_exposure_lamports`, keep SOL mirrors | exact exposure lamports inside confirm risk recheck | runtime refactor only | snapshot is built from `live_open_exposure_lamports*`; threshold comparisons must use exact lamports, while `*_sol` mirrors remain for logs and JSON details only |
| `SqliteStore.shadow_open_notional_sol()` | runtime `f64` return value | add `shadow_open_notional_lamports()` or widen return shape, keep SOL mirror | summed `shadow_lots.cost_lamports` | runtime/storage refactor only | shadow exposure checks must compare exact lamports; `*_sol` is derived for telemetry only |
| `SqliteStore.shadow_realized_pnl_since()` | runtime `(u64, f64)` | add exact signed-lamport return, keep SOL mirror | summed `shadow_closed_trades.pnl_lamports` | runtime/storage refactor only | drawdown / stop checks must use signed lamports; SOL mirror is display-only |
| `SqliteStore.live_realized_pnl_since()` | runtime `(u64, f64)` | add exact signed-lamport return, keep SOL mirror | summed `positions.pnl_lamports` for closed rows | runtime/storage refactor only | realized-PnL windows must not be re-expanded through float-only state before comparison |
| reliable live mark quote for unrealized PnL (`reliable_token_sol_price_for_live_unrealized`) | runtime `Option<f64>` median | replace float-only output with canonical exact quote boundary, keep float mirror | exact quote rational derived from reliable observed swap samples | runtime/pricing refactor required before unrealized PnL can be treated as exact | target exact quote is a canonical rational `(quote_sol_lamports, quote_token_raw, quote_token_decimals)` chosen from the reliable sample set; exact path must not average floats; for even sample counts use a deterministic lower-median exact sample or equivalent deterministic exact selection |
| `SqliteStore.live_unrealized_pnl_sol()` | runtime `(f64, u64)` | add exact signed-lamport return plus missing-price count, keep SOL mirror | exact unrealized PnL computed from exact qty, exact cost, and price boundary | runtime/storage refactor only | risk gates compare exact signed lamports whenever a reliable price exists; float return remains logging/display-only |
| `SqliteStore.live_unrealized_pnl_sol()` exact path boundary | not explicit today | exact mark-value math over rational quote boundary | exact unrealized PnL source when quote boundary exists | runtime/storage refactor only | mark value for long inventory must be `floor(qty_raw * quote_sol_lamports / quote_token_raw)` in lamports; if only a float quote exists, unrealized PnL remains explicit approximate state and cannot participate in post-cutover exact drawdown gating |
| `SqliteStore.live_max_drawdown_since()` / `live_max_drawdown_with_unrealized_since()` | runtime `f64` | add exact lamport return, keep SOL mirror | cumulative exact realized/unrealized lamport deltas | runtime/storage refactor only | drawdown gates compare lamports to normalized lamport stops; unrealized contribution is exact only when the canonical exact quote boundary above exists; otherwise the row/window stays explicit approximate |
| tip / fee policy fields (`submit_route_tip_lamports`, `pretrade_max_priority_fee_lamports`, `orders.*_lamports_hint`) | config/runtime/storage integer fields | unchanged exact integers | existing integer fields already exact | no migration for D2 core path | keep all fee/tip policy comparisons in integer units; do not convert through SOL floats |
| `ExecutionIntent.notional_sol` | runtime `f64` | add `notional_lamports: Lamports`, keep `notional_sol` mirror | `ExecutionIntent.notional_lamports` | runtime refactor; no DB migration directly | construct once from `copy_signals.notional_lamports`; no repeated float-born recomputation |
| runtime `ExecutionFill.qty_exact` | `Option<TokenQuantity>` | unchanged | runtime fill qty source whenever exact observation exists | no DB migration directly | storage exact rows must come from this exact qty, not from recomputed float qty |
| `ExecutionIntent` synthetic qty from priced intent | implicit `qty = notional_sol / avg_price_sol` | remain approximate only until observed exact qty arrives | not allowed to create post-cutover exact rows in live mode | runtime policy change required | no fake exact qty; if observed exact qty is unavailable, row must stay explicitly approximate/reconcile-pending rather than silently exactified |

### 4.3 Execution Storage

| Field / surface | Current type | Target type | Post-D2 source-of-truth | Migration / cutover requirement | Boundary / rounding policy |
| --- | --- | --- | --- | --- | --- |
| `fills.notional_lamports` | SQLite `INTEGER` nullable | unchanged | execution fee-exclusive traded principal source-of-truth | already landed; post-cutover must be non-null | `notional_lamports` excludes fees and represents the trade value only: for buys it is spend excluding `fee_lamports`; for sells it is proceeds excluding fee deduction; `positions.cost_lamports` adds fee on buys, realized PnL subtracts `fee_lamports` on sells, and `fills.avg_price` derives from fee-exclusive notional over exact qty |
| `fills.fee_lamports` | SQLite `INTEGER` nullable | unchanged | total execution fee source-of-truth | already landed; post-cutover must be non-null | must represent the full charged fee burden recorded by the live path: network fee plus route tip plus ATA rent when applicable; never rederived from display float alone |
| `fills.qty_raw`, `fills.qty_decimals` | `TEXT` + `INTEGER` nullable | unchanged | execution token quantity source-of-truth | already landed; post-cutover must be non-null for observed exact fills | raw quantity copied from confirmed observation; no recovery from rounded `qty REAL` |
| `fills.qty` | SQLite `REAL` | keep as derived mirror until cleanup | not source-of-truth post-cutover | no lossy backfill; cleanup later | derive from raw+decimals when exact qty exists |
| `fills.fee` | SQLite `REAL` | keep as derived mirror until cleanup | not source-of-truth post-cutover | no lossy backfill | derive from lamports / 1e9 |
| `fills.avg_price` | SQLite `REAL` | keep as derived field | derived analytics / display only | no migration now | compute from exact notional and exact qty when both exist |
| `orders.status` / `copy_signals.status` for confirmed-but-not-accounted execution | runtime/storage `TEXT` lifecycle | keep string lifecycle but add durable `execution_confirmed_reconcile_pending` contract | durable exact-accounting exclusion state until manual reconcile or exact observation promotion | runtime/storage contract change required; optional schema sidecar if a separate reason column is needed | confirmed tx with missing or unusable exact qty must persist `confirm_ts`, `tx_signature`, and reconcile reason, must not enter `execution_confirmed`, must not create fills/positions exact rows, and must not be auto-retried as a fresh submit while pending manual reconcile |
| `positions.cost_lamports` | SQLite `INTEGER` nullable | unchanged | live cost basis source-of-truth | already landed; post-cutover must be non-null | buy cost basis increments by `fills.notional_lamports + fills.fee_lamports`; sell path realizes cost from prior open position basis and must not reinterpret `fills.notional_lamports` as fee-inclusive |
| `positions.pnl_lamports` | SQLite `INTEGER` nullable | unchanged | realized cumulative PnL source-of-truth | already landed; post-cutover must be non-null where position accounting is exact | signed integer math only |
| `positions.qty_raw`, `positions.qty_decimals` | `TEXT` + `INTEGER` nullable | unchanged | live position qty source-of-truth | already landed; post-cutover must be non-null | sell path must subtract raw units exactly; residual dust handling remains explicit policy |
| `positions.accounting_bucket` | not present | add `TEXT NOT NULL` | live inventory bucket discriminator across cutover | schema migration required before exact cutover; open-row uniqueness must move from `token` to `(token, accounting_bucket)` | required values are `legacy_pre_cutover` and `exact_post_cutover`; post-cutover exact buys may only mutate the exact bucket, and exact sells must not collapse legacy and exact inventory into the same open row |
| `positions.qty`, `cost_sol`, `pnl_sol` | SQLite `REAL` | keep as derived / legacy mirrors through cutover | not source-of-truth post-cutover | preserve for compatibility until D2-7 | write mirrors from exact fields for new rows; legacy rows remain approximate |

### 4.4 Shadow Storage

| Field / surface | Current type | Target type | Post-D2 source-of-truth | Migration / cutover requirement | Boundary / rounding policy |
| --- | --- | --- | --- | --- | --- |
| `shadow_lots.cost_lamports` | SQLite `INTEGER` nullable | unchanged | shadow open-lot cost basis source-of-truth | already landed; post-cutover must be non-null | write from exact copy notional lamports |
| `shadow_lots.qty_raw`, `shadow_lots.qty_decimals` | `TEXT` + `INTEGER` nullable | unchanged | shadow open-lot qty source-of-truth | landed in `0030`; post-cutover must be non-null when leader exact qty exists | scale leader raw qty by exact copied notional; truncate toward zero on proportional split; if exact scaling yields `qty_raw = 0` while copied `cost_lamports > 0`, reject the candidate from exact accounting and write no fake-exact lot/trade row |
| `shadow_lots.accounting_bucket` | not present | add `TEXT NOT NULL` | shadow open-lot bucket discriminator across cutover | schema migration required before exact cutover | required values are `legacy_pre_cutover` and `exact_post_cutover`; post-cutover exact lots must be tagged exact and FIFO close logic must preserve bucket provenance |
| `shadow_lots.qty`, `cost_sol` | SQLite `REAL` | keep as derived / legacy mirrors through cutover | not source-of-truth post-cutover | preserve until D2-7 | derive mirrors from exact fields for new rows |
| `shadow_closed_trades.entry_cost_lamports`, `exit_value_lamports`, `pnl_lamports` | SQLite `INTEGER` nullable | unchanged | shadow closed-trade money source-of-truth | already landed; post-cutover must be non-null | integer entry/exit/PnL math only |
| `shadow_closed_trades.qty_raw`, `shadow_closed_trades.qty_decimals` | `TEXT` + `INTEGER` nullable | unchanged | shadow closed-trade qty source-of-truth | landed in `0030`; post-cutover must be non-null when exact qty exists | raw closed qty copied from exact FIFO close path |
| `shadow_closed_trades.accounting_bucket` | not present | add `TEXT NOT NULL` | shadow close provenance across cutover | schema migration required before exact cutover | a closed-trade row may only represent a single bucket; exact and legacy lot closes must not be collapsed into one source-of-truth row |
| `shadow_closed_trades.qty`, `entry_cost_sol`, `exit_value_sol`, `pnl_sol` | SQLite `REAL` | keep as derived / legacy mirrors through cutover | not source-of-truth post-cutover | preserve until D2-7 | derive mirrors from exact lamports/raw qty for new rows |

### 4.5 Cutover and Legacy Policy

| Field / surface | Current type | Target type | Post-D2 source-of-truth | Migration / cutover requirement | Boundary / rounding policy |
| --- | --- | --- | --- | --- | --- |
| `exact_money_cutover_state.cutover_ts` | SQLite `TEXT` | unchanged | authoritative cutover epoch | already landed; operator-set once rollout is ready | must be explicit RFC3339 timestamp with timezone |
| pre-cutover legacy rows in `observed_swaps`, `copy_signals`, `fills`, `positions`, `shadow_lots`, `shadow_closed_trades` | mixed exact + float | explicit legacy approximate state | legacy rows remain approximate unless exact sidecars and provenance fields already exist | no fake backfill from `REAL` only rows | only honest derivations are allowed; otherwise leave row marked implicitly legacy |
| live positions / shadow lots that remain open across cutover | currently single mixed inventory path | segregated legacy-open inventory policy enforced by persisted bucket fields | pre-cutover open inventory remains legacy until drained; new exact inventory must not merge into the same accounting bucket | blocked on the bucket migrations above before final cutover | exact and legacy-open inventory may coexist, but they must stay in separate accounting buckets with durable provenance; mutating a `legacy_pre_cutover` row/lot from an exact post-cutover buy is forbidden |
| `tools/exact_money_coverage_report.py` output | CLI report | expanded audit gate | coverage evidence for post-cutover exactness | tool update required for currently landed `shadow_lots.qty_*` / `shadow_closed_trades.qty_*` sidecars, `copy_signals.notional_lamports`, signal provenance, bucketed inventory, and invalid exact rejects | surfaces count as exact only on all-or-nothing required sidecar presence plus required provenance fields; `copy_signals` counts exact only when `notional_origin = 'leader_exact_lamports'`; `qty_raw = 0 AND cost_lamports > 0` must count as invalid exact, not exact; report must also expose forbidden exact/legacy bucket merges |
| archive/export of remaining legacy approximate evidence | ad hoc operator procedure today | required operator artifact before later pruning | durable evidence that approximate legacy rows were not silently discarded | operator checklist and path convention required before `D2-7` cleanup | capture row counts plus export/snapshot of remaining legacy approximate surfaces before any pruning that would destroy audit evidence |

## 5. Explicit Non-Goals for D2-0

These fields can remain `f64` after the exact-money cutover because they are derived analytics or display data, not source-of-truth accounting:

1. wallet metrics:
   - `pnl`
   - `win_rate`
   - `score`
   - `tradable_ratio`
   - `rug_ratio`
2. discovery and shadow quality gates based on ratios or proxies:
   - `min_score`
   - `min_tradable_ratio`
   - `max_rug_ratio`
   - `min_liquidity_sol`
   - `min_volume_5m_sol`
3. execution policy values that are not money state:
   - `slippage_bps`
   - `daily_loss_limit_pct`
   - `max_drawdown_pct`
4. derived prices:
   - `avg_price`
   - `price_sol_per_token`

## 6. Recommended Implementation Batches After This Spec

### Batch 1: exact signal and config boundary

1. Add `copy_signals.notional_lamports` and durable signal provenance for exact-vs-approximate leader SOL leg.
2. Add runtime-normalized lamport fields for:
   - `copy_notional_sol`
   - `min_leader_notional_sol`
   - `risk.shadow_soft_exposure_cap_sol`
   - `risk.shadow_hard_exposure_cap_sol`
   - `risk.shadow_drawdown_1h_stop_sol`
   - `risk.shadow_drawdown_6h_stop_sol`
   - `risk.shadow_drawdown_24h_stop_sol`
   - `pretrade_min_sol_reserve`
   - `risk.max_position_sol`
   - `risk.max_total_exposure_sol`
   - `risk.max_exposure_per_token_sol`
3. Carry exact notional through `ExecutionIntent` without repeated float-origin conversions.
4. Make the clamp order explicit in code and tests: if exact leader SOL leg exists, clamp at lamport precision before deriving any float mirror.
5. Define the exact quote boundary used by unrealized PnL instead of relying on float-only median prices.

### Batch 2: live fill exactness policy

1. Tighten live fill building so post-cutover exact rows never come from synthetic token qty.
2. Do not write post-cutover live source-of-truth rows that are still approximate; move confirmed-but-not-accounted tx into a durable reconcile-pending lifecycle that retains `confirm_ts`, `tx_signature`, and reconcile reason until manual reconcile.
3. Add persisted `accounting_bucket` handling for positions and shadow inventory so cutover does not merge exact and approximate buckets.
4. Reject zero-raw exact shadow sizing results instead of persisting fake-exact lots/trades.
5. Add exact runtime carriers for exposure and realized/unrealized PnL so both sides of risk comparisons are exact.

### Batch 3: cutover and coverage gate

1. Expand `exact_money_coverage_report.py` to include:
   - `shadow_lots.qty_raw` / `qty_decimals`
   - `shadow_closed_trades.qty_raw` / `qty_decimals`
   - `copy_signals.notional_lamports`
   - bucket-specific inventory coverage
   - invalid zero-raw exact rejects
2. Define operator checklist for setting `exact_money_cutover_state`.
3. Gate post-cutover rollout on:
   - zero partial exact rows
   - expected exact ratios for post-cutover surfaces
   - zero forbidden exact/legacy bucket merges
   - explicit count of remaining legacy approximate rows
   - exported/archive snapshot of remaining legacy approximate evidence before any later pruning

## 7. Acceptance for D2-0

`D2-0` should be considered complete when auditors agree that:

1. every in-scope monetary field has a declared target exact representation or an explicit reason to remain derived `f64`,
2. no core source-of-truth field is left in "decide later" state,
3. the spec explicitly distinguishes:
   - exact post-cutover rows,
   - legacy approximate rows,
   - and fake-exact states that must be rejected,
4. the spec names the durable cutover segregation mechanism for open inventory and the durable lifecycle for confirmed-but-not-accounted executions,
5. archive/export requirements for legacy approximate evidence are stated before any pruning phase,
6. the next implementation batch is mechanically obvious from the mapping above.
