# DISCOVERY_SELECTOR_RESCUE_PLAN

## Current Status (`2026-04-23`)

This document is historical context for the selector-rescue branch, not the
current live blocker.

Current live truth:

- selector, writer, ingress, follow-universe, and replay `sol_leg` have already
  been ruled out as the current primary blocker
- the live service is fail-closed on the recent-raw/runtime-window branch:
  - `repair_state = skipped_recent_raw_journal_head_gap_no_repairable_rows`
  - `repair_reason = recent_raw_journal_has_no_rows_in_required_window_before_runtime_cursor`
  - `publication_reason = raw_window_unusable_no_recent_published_universe`
- the current seam is recent-raw interval/frontier derivation:
  - metadata says `journal_covers_runtime_cursor = true`
  - `runtime_window_first_cursor_* = None`
  - `required_window_start` is later than `replay_until_cursor_ts`
  - the derived replay interval is empty

Do not resume selector-rescue work unless the user explicitly reopens it. The
next production-moving batch should target the recent-raw
`required_window_start` / `replay_until_cursor` / head-gap replayability
contract.

## 1. Diagnosis

- `AGENTS.md` still governs project hierarchy: Stage 3 production discovery truth remains the hard gate. This document does not replace that hierarchy. It defines a bounded offline selector-proof branch to determine whether the selector core is still salvageable from already persisted data.
- The project is not blocked because wallet selection logic is missing. It is blocked because the runtime entrypoint is dominated by `publication truth` / `recent_raw` / recovery work before selection is allowed to matter.
- The actual selector core still exists in `crates/discovery/src/lib.rs` and `crates/discovery/src/followlist.rs`, but there is no simple, read-only, proof-oriented path of the form `persisted DB -> snapshots -> eligibility/scoring -> ranked wallets`.
- The existing closest path, `crates/discovery/src/bin/materialize_wallet_metrics_bootstrap.rs`, reuses the right discovery core but is not suitable as the proof path because it opens the DB read-write, runs migrations, and persists snapshot metadata.
- The selection contract is wider than `observed_swaps`. It also depends on `token_quality_cache`, `wallet_activity_days`, and `ShadowConfig` quality thresholds. Any rescue plan has to prove the selector against the existing persisted DB contract as it actually exists, not against a simplified mental model.

## 2. Current Wallet-Selection Path

- Service wiring:
  - `crates/app/src/main.rs`
  - `DiscoveryService::new_with_helius(config.discovery.clone(), config.shadow.clone(), ...)`
  - Important consequence: discovery scoring and eligibility are coupled to `shadow` quality-gate config, not just discovery config.

- Runtime entrypoint and coupling:
  - `crates/app/src/task_spawns.rs`
  - `spawn_discovery_task(...)`
  - Current order is:
    - open runtime DB
    - try `repair_runtime_store_publication_truth_from_recent_raw_journal_if_needed(...)`
    - only then call `run_cycle(...)`
    - only after that read `list_active_follow_wallets()`
  - This is the core architectural problem. Runtime repair became the front door to wallet selection.

- Snapshot build from persisted data:
  - `crates/discovery/src/lib.rs`
  - `build_wallet_snapshots_from_persisted_stream_one_shot(...)`
  - This is the salvageable offline core. It:
    - scans `observed_swaps` in a bounded window
    - resolves `token_quality_cache`
    - builds wallet accumulators
    - finalizes rug metrics
    - materializes `WalletSnapshot` rows

- Eligibility and scoring:
  - `crates/discovery/src/lib.rs`
  - `snapshot_from_components(...)`
  - `snapshot_from_accumulator_with_persisted_active_days_internal(...)`
  - `crates/discovery/src/quality_cache.rs`
  - `evaluate_buy_tradability(...)`
  - Effective eligibility depends on:
    - `min_trades`
    - `min_active_days`
    - `min_buy_count`
    - `min_tradable_ratio`
    - `min_leader_notional_sol`
    - `decay_window_days`
    - `max_rug_ratio`
    - `require_open_positions_for_publication`
    - quality cache freshness / completeness

- Ranking and desired wallet set:
  - `crates/discovery/src/followlist.rs`
  - `rank_follow_candidates(...)`
  - `desired_wallets(...)`
  - This is the real `top wallets` step.

- Persistence and publication state:
  - `crates/storage/src/discovery.rs`
  - `persist_discovery_cycle_with_snapshot_metadata(...)`
  - `set_discovery_publication_state(...)`
  - This layer writes:
    - `wallets`
    - `wallet_metrics`
    - `followlist`
    - publication truth / runtime state

- Existing bootstrap helper that is close but not the final proof path:
  - `crates/discovery/src/lib.rs`
  - `materialize_trusted_bootstrap_wallet_metrics(...)`
  - `crates/discovery/src/bin/materialize_wallet_metrics_bootstrap.rs`
  - Reuses the right snapshot/ranking core, but persists metadata and is therefore not the clean read-only selector path.

- Downstream consumer:
  - `crates/shadow/src/lib.rs`
  - `process_swap(...)`
  - Shadow is downstream of the selector. It is not the place to prove selector correctness.

## 3. What Is Salvageable

- `build_wallet_snapshots_from_persisted_stream_one_shot(...)` as the primary persisted-data computation path.
- `snapshot_from_components(...)` and `snapshot_from_accumulator_with_persisted_active_days_internal(...)` as the real scoring and eligibility contract.
- `evaluate_buy_tradability(...)` as the actual token-quality gate already enforced by current policy.
- `rank_follow_candidates(...)` and `desired_wallets(...)` as the ranking layer.
- Existing storage readers for:
  - `observed_swaps`
  - `wallet_activity_days`
  - `token_quality_cache`
- Existing CLI/config loading patterns from `materialize_wallet_metrics_bootstrap.rs`, but only as implementation reference for a thin wrapper over one new public read-only selector-report API in `copybot_discovery`.

## 4. What Is Not The Primary Path For Proving Selector Correctness

- `recent_raw` restore / replay / staged snapshot promotion as the proof surface for selector correctness. These still matter for Stage 3 and production truth, but they are not the primary path for proving the selector itself.
- `publication truth export` and export-blocker surfaces as the primary planning surface for selector correctness. They are downstream control-plane surfaces, not the first selector-proof surface.
- `attached-source` / `xread-*` / header-prefix / page-kind / source-vfs micro-probes as the next default branch for this question. They are low-yield for determining whether the persisted selector core is salvageable.
- Any attempt to continue debugging runtime entrypoint behavior before proving whether the persisted selector can already produce a non-empty ranked universe.
- Legacy bootstrap-bridge behavior that bypasses real persisted recomputation. The ignored tests around `legacy trusted-bootstrap runtime contract replaced by publication truth` are a warning, not a rescue direction.
- `tiny_live_activation` and adjacent activation-package work. It is unrelated to recovering selector truth from persisted data.

## 5. Primary Bounded Proof Path

- Stage 3 production truth remains the hard gate. This plan does not replace that gate.
- The next bounded branch for selector rescue is: build a dedicated read-only offline selector proof on a copy of the persisted DB.
- The tool should not touch runtime scheduling, `recent_raw`, `publication truth`, recovery checkpoints, export gates, or shadow execution.
- The implementation must not assume a separate CLI/bin can call current selector internals directly. Today the critical selector helpers are private or `pub(super)`.
- The fix path is:
  - add one public read-only selector-report API in `copybot_discovery`
  - keep snapshot build / eligibility / scoring / ranking private inside `copybot_discovery`
  - make the separate CLI/bin call only that public report API
- API placement should stay pragmatic:
  - if a method on `DiscoveryService` keeps the surface narrow and avoids extra coupling, use it
  - if a narrower standalone report builder is cleaner, prefer that
- The public report API should internally reuse the existing selector core, not replace it:
  - `build_wallet_snapshots_from_persisted_stream_one_shot(...)`
  - current eligibility/scoring code
  - current ranking logic
- The CLI/bin should be a thin wrapper, not a second selector implementation.
- The CLI/bin should open the DB with `SqliteStore::open_read_only(...)` and must not run migrations or persist `wallet_metrics` / `followlist` / publication state.

Why this is the right bounded branch before further selector work:

- It isolates the real question: can the current persisted contract already yield deterministic top wallets.
- It avoids the entire recovery/publication/recent_raw branch that currently obscures the answer.
- It has a bounded blast radius.
- It produces a decision-level result quickly:
  - either the persisted selector works and runtime is the blocker
  - or the persisted selector itself collapses and the data contract is the blocker

## 6. Bounded Implementation Plan

1. Define the offline selector contract.
   - Input:
     - DB copy path
     - config path
     - fixed `--now`
   - Output:
     - config identity / fingerprint
       - computed either from:
         - exact config bytes, or
         - a resolved effective selector/shadow subset
       - the chosen fingerprint method must be explicit in output
     - DB identity:
       - input path
       - file size
       - file mtime
       - SQLite page count from the read-only-opened DB
     - explicit read-only-open confirmation
     - RPC enabled / disabled flag
     - metrics window start
     - observed swaps loaded
     - wallets seen
     - eligible wallet count
     - ranked top wallets
     - reject/gate breakdown
     - token-quality coverage summary
   - Check:
     - contract is explicit and does not mention runtime/publication/recent_raw state

2. Batch 1: add a minimal public read-only identity-report API in `copybot_discovery`.
   - Place the surface wherever it keeps runtime coupling lowest:
     - on `DiscoveryService`, or
     - in a narrow standalone report builder
   - Keep selector internals private.
   - Return a structured proof report that includes at minimum:
     - proof identity fields from step 1
     - fixed `--now`
     - explicit read-only-open confirmation
     - empty selector skeleton output shape
   - Check:
     - the selector proof path is callable through one public API without exposing low-level internals individually
     - the first batch proves identity and read-only discipline before selector semantics are added

3. Batch 1: add a dedicated read-only CLI/bin for selector proof.
   - Make the bin a thin wrapper over the new public read-only identity-report API.
   - Open the DB with `SqliteStore::open_read_only(...)`.
   - Disable RPC by default.
   - Do not run migrations.
   - Do not persist any tables or state.
   - Check:
     - running the tool does not mutate the DB copy

4. Batch 2: extend the same public read-only API to reuse the existing selector core.
   - Reuse existing eligibility/scoring logic inside the report API.
   - Add:
     - `metrics_window_start`
     - `observed_swaps_loaded`
     - `wallets_seen`
     - ranked output
     - token-quality coverage telemetry
   - Expose grouped reject reasons for the existing gates instead of only `eligible=true/false`.
   - Reject/gate breakdown must be derived from the current selector decisions, not from a second independent classification tree.
   - Include at minimum:
     - insufficient active days
     - insufficient buy count
     - low tradable ratio
     - low notional
     - decayed / stale last seen
     - rug gate
     - open-position gate
     - missing or stale token-quality coverage
   - Check:
     - output explains why the ranked universe is empty or non-empty

5. Prove determinism on the same DB copy and fixed timestamp.
   - Run the selector multiple times against the same DB copy and same config.
   - Check:
     - top-wallet ordering is identical across runs
     - counts and gate breakdown are identical across runs

6. Use the offline result to decide the next branch.
   - If the tool produces a sane non-empty ranked universe, the next batch is to wire runtime consumption to the already-proven selector path with minimal coupling.
   - If the tool produces an empty or obviously broken universe, the next batch is not runtime repair; it is a bounded fix to the persisted selector contract.
   - Check:
     - the project has a hard branch decision for selector work based on selector proof, not another diagnostics loop
     - this branch decision does not by itself satisfy or replace the Stage 3 production gate

## 7. Acceptance Criteria

- There is a dedicated offline selector command that runs against a copy of the persisted DB in read-only mode.
- The command calls one public read-only selector-report API in `copybot_discovery`.
- That public API internally reuses the current selector core rather than reimplementing scoring in a parallel path.
- The report records proof identity explicitly:
  - fixed `--now`
  - config identity / fingerprint
    - fingerprint method is explicit:
      - exact config bytes, or
      - resolved effective selector/shadow subset
  - DB identity:
    - input path
    - file size
    - file mtime
    - page count
  - read-only-open confirmed
  - RPC enabled / disabled flag
- At fixed `--now`, fixed config identity, and fixed DB identity, repeated runs produce the same ranked wallet output.
- The output includes enough information to answer:
  - did the selector produce a non-empty candidate universe
  - which wallets ranked at the top
  - which gates removed the rest
  - whether missing/stale token-quality coverage is a blocker
- Reject/gate breakdown is derived from the existing selector decisions, not from a second classification tree.
- The proof path does not depend on:
  - `recent_raw`
  - `publication truth repair`
  - runtime scheduler state
  - export blockers
  - shadow execution
- The proof path does not mutate:
  - `wallet_metrics`
  - `followlist`
  - publication state tables
  - runtime DB metadata
- Completing this proof path does not by itself satisfy the Stage 3 production discovery truth gate.
- Batch 1 acceptance:
  - read-only open succeeds
  - identity report is emitted
  - config fingerprint method is explicit
  - page count is present
  - selector output can remain skeletal / empty
- Batch 2 acceptance:
  - the same proof path now emits ranked selector output and gate breakdown derived from existing selector decisions

## 8. Risks And Unknowns

- The repo currently does not contain a normal local runtime DB snapshot, so persisted-data sufficiency is still unproven from local artifacts alone.
- Without a real copy of the persisted DB, this remains an engineering direction rather than an executed proof path.
- `token_quality_cache` coverage on the real persisted DB is unknown.
- `wallet_activity_days` coverage on the real persisted DB is unknown.
- Current config truth is drifted across files:
  - `configs/live.toml` contains stricter gates
  - `configs/prod.toml` still contains weaker gates
- The current selector code collapses many failures into `eligible=false`; reject-reason surfacing will likely require a structured report layer inside `copybot_discovery`, not just a print-only shell.
- Discovery currently depends on `ShadowConfig` quality thresholds. The offline tool must make that dependency explicit instead of hiding it.

## 9. Explicitly Do Not Do

- Do not continue `xread-*` / attached-source / source-vfs / header-prefix diagnostics as the next default branch.
- Do not collect new data.
- Do not treat `recent_raw` repair or publication-truth export as the primary milestone for selector recovery.
- Do not mutate the live/runtime DB to prove selector correctness.
- Do not use bootstrap-clone / bridged-followlist shortcuts as a substitute for proving selection from persisted data.
- Do not rewrite discovery from scratch before the offline selector path is proven or falsified.
- Do not present this selector-proof branch as a replacement for the Stage 3 production truth hierarchy in `AGENTS.md`.
