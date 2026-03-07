# TEMP Consolidated Audit Report - Solana Copy Bot

Original snapshot date: 2026-03-05
Last refreshed: 2026-03-07
Current main baseline: `2bd62f8`
Status: working file, updated to reflect what is already closed in `main`

## Purpose

This file is no longer a literal snapshot of the 2026-03-05 repo state.
It is now the live working summary of:

- what the original consolidated audit reported,
- what has already been fixed and merged into `main`,
- what is still open,
- and what remains deferred or operational rather than code-only.

Important scope note:

- every status below is relative to `origin/main` at baseline `2bd62f8`
- this file is not a description of arbitrary local dirty worktrees or older batch branches
- when a reviewer compares against a stale local checkout, it can legitimately disagree with this file without invalidating the file itself

## 1. Original Priority List - Current Status

### Critical

- `C-1` Credentials committed to git:
  - Status: `PARTIALLY CLOSED`
  - Done: tracked repo surfaces were sanitized and placeholders replaced live values.
  - Revalidated on current `main`: tracked `live.toml`, server template, and playbook are placeholder-only; the remaining work is git history cleanup, rotation decision, server-side secret hygiene, and any non-tracked/server copies.

### High

- `H-1` Impossible config / universe-stop contradiction:
  - Status: `CLOSED IN MAIN`
- `H-2` Synthetic confirm reconciliation instead of observed on-chain fills:
  - Status: `PARTIALLY CLOSED IN MAIN`
  - Done: observed on-chain fill path exists and is preferred when usable transaction observation is present.
  - Still open: confirmed tx without usable observed fill can still finalize via synthetic fill from intent/fallback price, which keeps a residual drift risk in fills / positions / exposure / PnL.
- `H-3` Confirm-timeout can strand real positions:
  - Status: `PARTIALLY CLOSED IN MAIN`
  - Done: timeout path with manual reconcile now transitions to reconcile-pending instead of hard-failing.
  - Still open: generic confirm error after deadline still marks the order `execution_failed` even when manual reconcile is enabled, so the "stranded live position" class is reduced but not fully closed.
- `H-4` Executor idempotency break after successful live submit:
  - Status: `CLOSED IN MAIN`
- `H-5` Upstream fallback can duplicate live submit:
  - Status: `CLOSED IN MAIN`
- `H-6` Exposure TOCTOU on parallel confirm path:
  - Status: `CLOSED IN MAIN`
- `H-7` NaN poisoning through ingestion / Yellowstone amount parsing:
  - Status: `CLOSED IN MAIN`
- `H-10` Secret leakage surface through `Debug`:
  - Status: `CLOSED IN MAIN`
- `H-11` Roadmap/runtime ATA mismatch:
  - Status: `CLOSED IN MAIN`

### Medium

- `M-1` Adapter HMAC replay cache poisoning / no cap:
  - Status: `CLOSED IN MAIN`
- `M-2` Secret-file permission checks not fail-closed:
  - Status: `CLOSED IN MAIN`
- `M-4` Shadow holdback queue bypasses main cap:
  - Status: `CLOSED IN MAIN`
- `M-5` Rug gate treats "not yet evaluated" as safe:
  - Status: `CLOSED IN MAIN`

## 2. Additional Inventory Closed Since The Original Audit

These were originally in the imported action inventory or later follow-up work and are now closed in `main`.

- `activate_follow_wallet` TOCTOU:
  - closed with schema-level guard and idempotent activation path
- `insert_shadow_lot` reading `last_insert_rowid()` outside retry closure:
  - closed
- `persist_discovery_cycle` using deferred `unchecked_transaction()` without retry:
  - closed
- missing foreign keys in execution chain:
  - closed for `orders.signal_id -> copy_signals.signal_id`
  - closed for `fills.order_id -> orders.order_id`
- multiple open `positions` rows per token:
  - closed with schema-level guard and migration
- key order status regressions in storage:
  - hardened across simulated/submitted/failed/dropped/reconcile-pending/confirmed transitions
- dust `shadow_lots` causing phantom inventory / phantom exits:
  - closed
- discovery quality gates bypass when RPC/cache quality data is missing:
  - closed
- executor replay protection after restart:
  - closed for persistent HMAC nonce claims
- external plaintext `http://` endpoints:
  - closed for adapter/executor endpoint validation
- adapter `/submit` always returning `200`:
  - closed
- unbounded upstream response body reads:
  - closed
- ingestion URL logging with secrets in query/userinfo:
  - closed
- hand-rolled JSON escaping in app:
  - closed
- unused `reconcile_followlist()` helper:
  - closed / removed from main
- discovery cap-eviction false-demotion path:
  - closed
- discovery ordering hardening around snapshot rebuild / `partition_point()` assumptions:
  - closed
- `SignalSide` deserialization rigidity:
  - closed with canonicalized serde / parsing hardening
- discovery env override parsing now fail-closed:
  - closed
- ingestion queue overflow policy validation:
  - closed
- ingestion source validation / normalization:
  - closed
- malformed bool env overrides now fail-closed:
  - closed
- malformed execution numeric env overrides now fail-closed:
  - closed
- malformed ingestion / yellowstone numeric env overrides now fail-closed:
  - closed
- malformed shadow / risk numeric env overrides now fail-closed:
  - closed
- adapter inbound request body size limit:
  - closed
- residual CSV / route-list / route-map config env parsing:
  - closed
- `RUST_LOG` ambient override able to suppress app warning surfaces:
  - closed; app now uses explicit `COPYBOT_APP_LOG_FILTER`
- alert delivery gap:
  - closed with webhook delivery path and persisted cursor over `risk_events`
- dead `core-types` wrappers:
  - closed / removed from main (`EventEnvelope`, `CopyIntent`, obsolete `SignalSide`)

## 3. Discovery / SQLite Hotfix Report Status

The separate file `ops/server_reports/2026-03-06_hotfix_discovery_sqlite_io_assessment.md` is now effectively implemented in code.

Closed in `main`:

- `wallet_metrics(window_start)` index
- tuple-comparison cursor query for `observed_swaps`
- simpler indexed wallet-metrics retention path
- dedicated observed-swap writer
- bounded `observed_swaps` retention
- tighter discovery defaults / config validation for live pressure reduction
- metric snapshot throttling
- exact-bucket metric dedupe
- cached discovery summary reuse within a snapshot bucket to avoid full recompute every tick

What still remains from that report is operational, not code-only:

- deploy the new config/runtime knobs to the real server config
- observe post-deploy runtime behavior
- decide whether further redesign is still needed after real production-like measurements
- decide whether the current "stable under cap" discovery design tradeoff is acceptable long-term, because scoring/followlist now operate on a capped in-memory tail rather than the full theoretical time-window

## 4. Still Open In This Audit File

### Deferred / architectural

- `D-2` `f64` financial-state architecture drift:
  - Status: `OPEN / LARGE ARCHITECTURE ITEM`
  - Concrete pre-D-2 risk already visible today: partial sell / qty mismatch can leave tiny live residual positions ("ghost positions"), so there is value in deciding whether to ship a narrow dust/EPS fix before opening the full refactor.

### Reopened / newly validated code work

- residual confirm/reconcile correctness:
  - Status: `OPEN`
  - Scope:
    1. deadline-passed generic confirm errors should follow reconcile-pending semantics when manual reconcile is enabled, not hard-fail into `execution_failed`;
    2. live confirmed tx without usable observed fill should not silently finalize through synthetic pricing without a stronger policy boundary.
- history retention outside `wallet_metrics` / `observed_swaps`:
  - Status: `OPEN`
  - Scope:
    1. `risk_events`,
    2. `copy_signals`,
    3. `orders`,
    4. `fills`,
    5. `shadow_closed_trades`.
  - Rationale: the main DB is already large in production-like runtime, and these tables still have no bounded lifecycle policy.

### Operational / repo tasks

- `C-1` remaining history cleanup / rotation decision:
  - Status: `OPEN`

## 5. Practical Next Order

If coding continues off this audit file, the clean next order is:

1. finish the residual confirm/reconcile correctness gaps (`H-2` / `H-3` partial reopen)
2. add retention policy for `risk_events` / `copy_signals` / `orders` / `fills` / `shadow_closed_trades`
3. decide whether capped-tail discovery semantics are acceptable or need a deeper redesign after more runtime evidence
4. handle `C-1` as an ops / git-history / rotation task, not as routine code hardening
5. then decide whether `D-2` (`f64` financial-state architecture) is worth opening as a dedicated refactor track

## 5.1 Detailed Plan - `C-1`

`C-1` is not a normal code-hardening item. It is a combined repo / history / server-secret hygiene track and should be treated as an ops/security project with explicit evidence.

### `C-1` objective

Close the credentials issue fully, not just at current-tree level.

For `C-1` to be considered closed, the following must all be true:

1. current tracked repo tree is placeholder-only,
2. rotated live credentials are active on the server,
3. old credentials are revoked,
4. server-local copies / backups are cleaned,
5. authoritative upstream refs and artifacts under operator control no longer contain the old credential material,
6. operators know that old clones/caches must not be trusted and must be reset or recloned after the rewrite.

### `C-1` work breakdown

#### `C1-1` Current-surface inventory

Build the exact inventory of all places where the credential material may still exist:

1. current tracked repo tree,
2. git history,
3. `/etc/solana-copy-bot/*.toml`,
4. `/etc/solana-copy-bot/*.env`,
5. `*.bak` server backups,
6. secrets files,
7. copied reports / ad-hoc snapshots / runbooks,
8. local developer copies if they were ever synced from server values,
9. server shell history if credentials were ever typed into the command line.

Acceptance:

1. one written inventory exists,
2. every surface is marked `clean`, `needs rotation`, `needs deletion`, or `needs history rewrite`.

#### `C1-2` Rotation

Rotate all live provider credentials that were ever present in tracked repo files or server-local configs.

Acceptance:

1. new credential set is deployed and confirmed working,
2. old credential set is revoked,
3. server runtime is confirmed healthy on the rotated credentials.

#### `C1-3` Server-local cleanup

Clean server-local copies after rotation:

1. `/etc/solana-copy-bot/live.server.toml.bak*`,
2. old secret files,
3. stale env files,
4. copied config snapshots with credential material,
5. any systemd drop-ins or operator helper files that still embed old values.

Acceptance:

1. server-side scan no longer finds the old secret material,
2. current runtime uses only the rotated credentials.

#### `C1-4` Git history rewrite

Do the history cleanup only after rotation.

Recommended flow:

1. mirror clone,
2. temporary push freeze while the rewrite is prepared and validated,
3. `git filter-repo` or BFG using exact token and URL patterns,
4. rewrite tags/refs,
5. force-push rewritten history,
6. invalidate old clones and communicate reset / re-clone requirement before normal pushes resume.

Acceptance:

1. exact old token no longer appears in rewritten history,
2. rewritten history is pushed successfully,
3. push freeze / rewrite window is documented,
4. old clones are explicitly declared untrusted.

#### `C1-5` Verification and closure

Final closure evidence must show:

1. tracked tree clean,
2. history clean,
3. server copies clean,
4. rotation complete,
5. operators informed.

`C-1` is not closed by current-tree sanitization alone.

## 5.2 Detailed Plan - pre-`D-2` cleanup

Before opening the large `D-2` architecture refactor, there are smaller correctness/storage items that should be closed first.

### `PRED2-1` Residual confirm/reconcile semantics

Close the remaining `H-2` / `H-3` tails:

1. deadline-passed generic confirm error must follow reconcile-pending semantics when manual reconcile is enabled,
2. confirmed live tx without usable observed fill must not silently finalize through synthetic pricing without an explicit stronger policy.

Acceptance:

1. no live confirmed position can disappear into `execution_failed` solely because confirm errored after deadline with manual reconcile enabled,
2. synthetic fill fallback in live mode is either prohibited or moved behind an explicit reconcile/manual boundary,
3. reporting / query parity is explicit for the new reconcile-pending paths, including any `confirm_price_unavailable_manual_reconcile_required`-style status / reason surfaces used by runtime snapshots or operator tooling,
4. regression tests cover both cases.

### `PRED2-2` History retention

Add lifecycle policy for:

1. `risk_events`,
2. `copy_signals`,
3. `orders`,
4. `fills`,
5. `shadow_closed_trades`.

Acceptance:

1. retention horizon is explicit per table,
2. cleanup path is implemented and tested,
3. no active runtime query or alert cursor is broken by cleanup,
4. child-first delete order or equivalent FK-safe cleanup is explicit where table relationships matter,
5. a protected history window, archive export, or snapshot rule exists before pruning evidence that may be needed for later validation or migration work.

### `PRED2-3` Discovery capped-tail decision

Current runtime is intentionally stable under cap, but scoring/followlist now operate on a capped tail rather than the full theoretical time-window.

This must be explicitly resolved as one of:

1. accepted design tradeoff for current rollout,
2. follow-up redesign item after more production-like measurements.

Acceptance:

1. the decision is written down,
2. operational guardrails are explicit:
   `swaps_fetch_limit_reached_ratio`,
   head-lag / backlog thresholds,
   and the condition that makes capped-tail behavior acceptable vs unacceptable,
3. the evidence window and re-review trigger are explicit, so it is no longer left as an implicit runtime compromise.

### `PRED2-4` Narrow ghost-position fix

There is value in closing the most operationally relevant `f64`-adjacent bug class before the full `D-2` refactor:

1. partial sell / qty mismatch leaving tiny residual live positions,
2. tiny residual positions causing ghost-position behavior.

Acceptance:

1. if fixed before `D-2`, the runtime invariant is explicit:
   "dust / residual qty below the agreed threshold must not count as an open live position",
2. if fixed before `D-2`, regression tests cover the residual-qty / ghost-position class,
3. otherwise it is explicitly deferred into `D-2` with that risk called out.

## 5.3 Detailed Plan - `D-2`

`D-2` is not a small hardening task. It is a dedicated financial-data architecture refactor.

### `D-2` goal

Move source-of-truth monetary state off ad-hoc `f64` arithmetic for live trading state.

The key distinction:

1. money/state-of-record should become exact,
2. ratios/scores/analytics may still remain `f64` where appropriate.

### `D-2` scope rules

#### Must move to exact representation

1. live execution fills,
2. positions,
3. shadow lots,
4. shadow closed trades,
5. exposure state,
6. realized/unrealized PnL state,
7. fee / tip / reserve comparisons used in risk or pretrade logic.

#### Can remain `f64` initially

1. `win_rate`,
2. `score`,
3. `tradable_ratio`,
4. `rug_ratio`,
5. telemetry ratios,
6. other derived analytics that are not source-of-truth money state.

### `D-2` target model

1. SOL-denominated state -> lamports / exact integer representation,
2. token quantity -> exact raw units plus decimals-aware conversion boundary,
3. prices -> derived values, not primary accounting truth.

### `D-2` phases

#### `D2-0` Specification

Create a field-by-field map:

1. current field,
2. current type,
3. target type,
4. source of truth,
5. migration requirement,
6. rounding policy at boundaries.

Acceptance:

1. one written mapping/spec exists,
2. no "decide later" on core monetary fields,
3. the spec explicitly distinguishes:
   exact cutover-era rows,
   legacy approximate rows that cannot be losslessly reconstructed from `REAL`,
   and any archive/export required before pruning legacy evidence.

#### `D2-1` Exact ingress / quantity boundary

Introduce the exact quantity boundary at the first point where token amounts and SOL amounts enter the live pipeline:

1. observed fills / parsers,
2. confirm path observation,
3. adapter / execution request payload boundaries,
4. decimals-aware conversion rules from raw units to display values.

Acceptance:

1. token quantity no longer first appears as source-of-truth `f64` in the live path,
2. raw token units plus decimals-aware conversion rules are explicitly defined,
3. later exact storage phases are not forced to recover exact quantities from already-rounded float inputs.

#### `D2-2` Core exact primitives

Introduce and validate exact runtime primitives for:

1. lamports,
2. signed lamport deltas / PnL,
3. exact token quantity representation.

Acceptance:

1. business-critical SOL comparisons no longer depend on plain `f64`.

#### `D2-3` Config and sizing boundary normalization

Keep configs human-readable if needed, but convert monetary thresholds once at load time into exact runtime representation.

Also move the live sizing path off float-shaped source values as early as possible:

1. intent / notional boundary,
2. pretrade fee / reserve comparisons,
3. execution risk-gate comparisons,
4. any place where runtime sizing is still born from float notional and only exactified later.

Acceptance:

1. runtime risk/pretrade gates compare exact amounts, not raw float config values,
2. live monetary truth is no longer float-shaped until the accounting layer.

#### `D2-4` Execution storage path

Convert:

1. fills,
2. positions,
3. exposure snapshots,
4. confirm/reconcile accounting.

Acceptance:

1. no ghost-position behavior from float residue in live execution state,
2. cost-basis accounting no longer depends on float accumulation for source-of-truth state.

#### `D2-5` Shadow storage path

Convert:

1. `shadow_lots`,
2. `shadow_closed_trades`,
3. shadow risk-state accounting.

Acceptance:

1. shadow FIFO/update paths use exact state, not float residue as truth.

#### `D2-6` Schema migration and cutover strategy

Do not big-bang rewrite the whole DB in one shot.

Preferred strategy:

1. define a cutover epoch after which newly written rows are exact,
2. add new exact columns,
3. backfill only the values that can be derived honestly from legacy rows,
4. explicitly mark pre-cutover legacy rows as approximate where exact raw units / exact historical cost basis cannot be reconstructed,
5. preserve a protected history window, snapshot, or archive before destructive pruning,
6. dual-read / dual-write cutover,
7. only then remove or demote legacy float columns.

Acceptance:

1. migration is testable,
2. rollout is reversible during intermediate phases,
3. mixed-state reads are covered by tests,
4. the plan does not pretend that legacy `REAL` rows can always be losslessly converted into exact raw token units / exact historical cost basis.

#### `D2-7` Cleanup

Remove legacy float source-of-truth monetary paths once the exact path is stable.

Acceptance:

1. production money state no longer depends on float columns,
2. remaining `f64` usage is intentional analytics / presentation only.

### `D-2` done criteria

`D-2` should be considered done only when:

1. source-of-truth monetary state is exact in runtime and storage,
2. ingress and sizing boundaries are exact before accounting/storage,
3. schema cutover is complete with an explicit legacy-row policy,
4. ghost-position / float-residue class is closed,
5. remaining `f64` usage is explicitly limited to non-source-of-truth analytics.

## 6. Notes

- This file intentionally tracks current closure state relative to `main`, not just the original 2026-03-05 snapshot.
- Items marked `CLOSED IN MAIN` have already been implemented, audited, and merged.
- `PARTIALLY CLOSED` means repo-facing code/config surfaces are fixed, but operational or history cleanup still remains.
