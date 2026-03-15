# Selection / Bootstrap Redesign Plan

Date: 2026-03-15

## Purpose

This document defines the redesign plan for the selection/bootstrap architecture.

The goal is not to rewrite the whole bot. The goal is to remove the operationally fragile part of the system that decides:

- which wallets are currently trusted for follow/copy
- how the bot recovers that trusted set after restart, incident, or fail-close
- how the bot rotates from one top-N universe to another over time

## Current Verdict

Current live state is no longer unsafe in the old way:

- runtime/data-plane is healthy
- invalid historical `976-wallet` universe is no longer used
- `followlist.active` is back to `15`
- `trusted_selection_bootstrap_required = 0`

But the recovery path that got us here is still architecturally wrong:

- the system required multiple code-only bootstrap attempts before a simple clone-latest bridge restored top-15
- expensive raw-history materialization paths were explored before the simplest persisted-source bridge
- the current architecture still makes restart/bootstrap behavior too complex, too expensive, and too easy to mis-operate

So the system is no longer in the emergency state, but the selection/bootstrap architecture is still not production-grade.

Important nuance:

This redesign does not start from zero.

The following safety pieces already exist in code/live behavior:

- startup can hold recovered historical `followlist.active` rows out of runtime truth
- invalid selection can fail-close shadow/copy progression
- durable `trusted_selection_bootstrap_required` state already exists
- persisted `wallet_metrics` bootstrap is already part of the recovery logic

So this plan is a hardening/completion plan for an already-corrected safety contract, not a full greenfield redesign.

## What Went Wrong

### 1. Selection truth was ambiguous

Different pieces of the system tried to act as the source of truth:

- historical `followlist.active`
- current raw window
- cached raw summary
- persisted `wallet_metrics`
- operator recovery writes

That ambiguity is what allowed the system to oscillate between:

- invalid historical follow universe
- fail-close
- expensive bootstrap recompute attempts

### 2. Bootstrap was treated like recompute

The architecture blurred two different jobs:

- bootstrap: "get me back to a safe trusted top-N quickly"
- scoring refresh: "recompute rankings from live data over time"

Bootstrap must be cheap and operational.
Scoring refresh can be heavier.

The system treated bootstrap like a giant recompute problem.

### 3. Raw-window restart semantics were too fragile

After restart, raw-window discovery could not safely be trusted for top-N selection under short history / truncated history / warm restore conditions.

This is correct as a safety principle.

The problem was that there was no equally cheap, first-class persisted bootstrap alternative.

### 4. Operator recovery depended on undocumented data formatting details

The successful `clone-latest` bridge worked only after using canonical UTC RFC3339 with `+00:00` rather than `Z`.

That means recovery depended on a string-formatting implementation detail instead of a formalized admin path.

## Design Principle

Selection/control-plane must obey one hard rule:

> The bot must either follow a trusted persisted top-N universe or fail-close.

Not:

- trust raw window because it is available
- trust historical followlist because it exists
- trust a giant recompute because it eventually finishes

## Target Architecture

The architecture should be split into two clean layers.

### Layer A: Runtime / Data Plane

Responsibilities:

- ingest swaps
- persist observed swaps
- run shadow/copy loop
- stay healthy under load

Properties:

- bounded queues
- bounded maintenance work
- restart-safe
- independent from expensive selection bootstrap work

This layer is already much healthier than before and should not be the main redesign target right now.

### Layer B: Selection / Control Plane

Responsibilities:

- produce trusted top-N universe
- publish active followlist
- fail-close if trusted selection is unavailable
- rotate top-N over time as rankings change

Properties:

- one source of truth at a time
- cheap bootstrap
- explicit invalid-selection state
- no dependence on huge full-history rescans for restart recovery

This is the layer that must be redesigned.

## Source of Truth Contract

The system must define exactly one trusted selection source for each mode.

### Normal steady-state

Trusted source:

- latest trusted persisted `wallet_metrics` snapshot
- produced by normal discovery scoring refresh

Clarification:

- "latest" must mean latest snapshot that satisfies explicit trust metadata, freshness policy, and config/version compatibility
- it must not mean raw `MAX(window_start)` by itself

Selection output:

- `desired_wallets = top N ranked eligible wallets`
- `followlist.active == desired_wallets`

### Restart / bootstrap recovery

Trusted source:

- latest trusted persisted `wallet_metrics` snapshot that is still within the allowed bootstrap age budget

Bootstrap output:

- immediate recovery of `followlist.active` to trusted top-N
- recovery must work even when startup begins with an empty/suppressed runtime follow snapshot
- no dependence on raw in-memory window to make the first safe selection

### Invalid-selection state

If no trusted persisted selection source exists:

- runtime must fail-close shadow/copy progression
- `followlist.active` must not silently continue from historical garbage
- raw-window/cached summary must not become bootstrap truth
- stale or corrupted persisted snapshots must also fail-close by default unless an explicit operator override path is used

## The Core Redesign

### 1. Make persisted wallet-metrics bootstrap the primary recovery mechanism

The system already proved that a clone-latest bridge can restore top-15 quickly.

That should become the default architectural path, not an emergency improvisation.

Required result:

- restart recovery is based on trusted persisted snapshot reuse
- cheap routine recovery is a clone of the latest trusted snapshot into the current effective bootstrap bucket
- source snapshot must satisfy a maximum bootstrap staleness budget by default
- not on expensive raw rescan

### 2. Introduce an explicit admin bootstrap path

Need a first-class admin/operator tool for:

- clone latest trusted `wallet_metrics` snapshot into the current bootstrap window
- canonicalize `window_start`
- verify target window absence before write
- verify source snapshot existence and row count
- verify source snapshot age
- avoid direct followlist mutation
- support dry-run mode
- support explicit stale override with audit trail (for example `--force-stale`)
- emit structured pre-check and post-check output for operator review

This must replace ad hoc SQL surgery.

### 3. Canonicalize timestamp identity rules

The `wallet_metrics.window_start` matching bug (`Z` vs `+00:00`) is unacceptable operationally.

The redesign must remove exact-string timestamp fragility by doing one of:

- canonicalize all persisted timestamps to a single format at write time
- stop relying on exact string equality where logical UTC equality is intended

Chosen immediate direction:

- canonicalize at write time on every storage/admin boundary using one shared helper
- backfill existing `wallet_metrics.window_start` values to canonical UTC RFC3339 with `+00:00`

Minimum requirement:

- all bootstrap/admin paths must always write canonical UTC RFC3339 with `+00:00`

Preferred requirement:

- storage and lookup semantics should not depend on textual UTC spelling variants

### 4. Formalize trusted snapshot metadata and lineage

The system must stop treating snapshot identity as "whatever row group currently wins `MAX(window_start)`".

Required result:

- each trusted snapshot has an immutable identity
- bridged snapshots retain lineage back to their source snapshot
- "fresh", "trusted", and "current effective bootstrap target" are tracked separately

### 5. Make trusted snapshots self-contained

Bootstrap should not depend on mutable side tables or on re-deriving critical truth from current-time state if that can be persisted once.

Required result:

- the snapshot used for restart/bootstrap carries enough metadata to explain and validate why it is trusted
- bootstrap from a trusted snapshot can converge `followlist.active` even from an empty startup state
- if extra eligibility/decay inputs are required, they must be persisted or explicitly versioned as part of the trusted snapshot contract

### 6. Make normal scoring refresh incremental, not bootstrap-critical

Normal discovery scoring should continue to evolve rankings over time.

But bootstrap must not depend on having to recompute the whole scoring window from raw history.

So:

- incremental refresh can remain sophisticated
- bootstrap must remain cheap

Bridge removal rule:

- short-term official recovery path is clone-latest from a trusted persisted snapshot
- heavy rebuild and bridge removal can only become normal policy after aggregate/scoring parity is actually proven in production conditions

### 7. Decouple live strategy truth from historical followlist rows

Historical `followlist.active` must never again become implicit strategy truth on restart.

At startup:

- trusted persisted selection source decides whether the bot can run
- historical active rows are not enough by themselves

## Required Components

### A. Trusted Selection State Table

Persisted state should explicitly track:

- whether bootstrap is required
- whether trusted selection is available
- current selection state
- active trusted snapshot id
- last trusted snapshot window
- last trusted bootstrap source kind
- last trusted bootstrap timestamp
- last bootstrap reason

This partly exists today and should remain the durable coordination point, but the current implementation is still only the minimal subset of this contract.

### B. Trusted Snapshot Metadata

Need explicit metadata for each trusted snapshot:

- `snapshot_id`
- `source_snapshot_id` when a snapshot is bridged/cloned from another snapshot
- `source_window_start`
- `effective_window_start`
- `window_start`
- creation timestamp
- source kind:
  - normal discovery refresh
  - clone-latest bridge
  - admin materialization
- row count
- snapshot age at bootstrap time
- policy/config hash or version
- code version / schema version
- trust status

This is needed so that "latest trusted snapshot" is a first-class concept, not an inferred one.

Practical scoping note:

- `code_version` / `schema_version` are Phase 3+ metadata fields
- they are desirable for long-term provenance, but they are not required to unblock the Phase 1 clone-latest tool

Legacy migration requirement:

- existing historical `wallet_metrics` rows need an explicit migration story
- at minimum, legacy rows must be backfilled as `source_kind=legacy` with explicit trust-state handling instead of silently becoming trusted by default

### C. Admin Tooling

Need two official operator tools plus one decision runbook:

1. `clone-latest` bridge tool

Responsibilities:

- clone latest trusted snapshot into current bootstrap window
- canonical UTC formatting
- strict pre-checks and post-checks
- staleness guard with explicit override path
- source/target row-count verification
- duplicate target hard-fail
- structured output for audit and runbooks
- no direct followlist mutation

2. optional heavy rebuild tool

Responsibilities:

- rebuild `wallet_metrics` from raw history when persisted snapshot is genuinely unavailable or invalid
- be resumable/chunked
- expose progress cursor and abort/restart semantics
- obey operator budgets for downtime / WAL / memory

Starting operator budgets for heavy rebuild:

- max downtime budget: `10 minutes`
- max RSS budget: `2 GiB`
- max WAL growth budget: `500 MiB`

If those budgets are exceeded, the default action should be operator abort and return to fail-closed runtime rather than continuing an unbounded maintenance window.

This tool should exist only as a recovery fallback, not the primary restart path.

3. bootstrap decision matrix / runbook

Responsibilities:

- define condition -> action -> expected time -> risk
- cover stale snapshot, corrupted snapshot, missing snapshot, heavy rebuild timeout, and operator abort conditions

### D. Selection State Model

Selection/control-plane should expose explicit operator-visible states:

- `trusted_current`
- `trusted_bridged`
- `trusted_bridged_stale`
- `invalid`

The policy for shadow/copy/live execution must be defined against these states rather than inferred from scattered flags.

Default policy mapping:

- `trusted_current` -> shadow/copy allowed; execution remains gated by its own independent safety policy
- `trusted_bridged` -> shadow/copy allowed; execution still gated separately and should treat this as bridged, not fully current
- `trusted_bridged_stale` -> fail-close by default unless there is an explicit operator override
- `invalid` -> fail-close always

State transition rule:

- `trusted_bridged -> trusted_bridged_stale` when bridged snapshot age exceeds `max_bootstrap_snapshot_age`
- unless a future redesign introduces separate bridge-age and source-age thresholds, the same threshold should be used for both the clone-latest staleness guard and the stale-state transition

### E. Selection Go/No-Go Gate

Selection/bootstrap must become a top-level readiness gate, not just an implementation detail.

Before progressing to more permissive execution modes, the system should be able to prove:

- trusted snapshot availability
- acceptable snapshot age / bridge age
- successful post-bootstrap discovery publish
- expected `followlist.active` convergence
- short post-bootstrap soak without WAL/contention/queue regression
- fresh behavioral recovery evidence (`copy_signals` / equivalent downstream output) or an explicit documented `N/A` reason if no fresh signal opportunity occurred during the validation window

## What Must Happen After Bootstrap

Once trusted bootstrap succeeds:

1. discovery clears bootstrap-required flag
2. trusted persisted snapshot becomes current selection truth
3. `followlist.active` converges to `follow_top_n` even if startup began from an empty/suppressed runtime follow snapshot
4. runtime follows that active set
5. normal discovery cycles continue ranking and rotating top-N over time
6. operator can identify the active snapshot id, source kind, state, and age immediately

That means bootstrap is only the starting lineup.
Normal discovery remains the manager that changes the lineup over time.

## Phased Migration Plan

### Phase 0: Freeze and document

Status:

- done in practice
- formal closure artifact:
  - `ops/server_reports/2026-03-15_late_evening_clone_latest_bridge_recovery_report.md`

Actions:

- stop large unbounded bootstrap experiments
- keep live on safe runtime
- document successful `clone-latest` recovery bridge

Exit criteria:

- operator can explain current recovery mechanism in one page

### Phase 1: Operationalize clone-latest bridge

Goal:

- make the successful bridge a first-class supported admin path

Work:

- add official admin command/tool for clone-latest bootstrap
- encode canonical `+00:00` behavior
- add dry-run and verify output
- add source/target row-count checks
- add duplicate target guard
- add `max_bootstrap_snapshot_age` guard
- add explicit `--force-stale` override path with audit logging
- add structured pre-check/post-check output including source age and snapshot identity
- add decision matrix for stale/corrupt/missing snapshot cases

Exit criteria:

- operator can restore top-N bootstrap in minutes without raw SQL
- routine clone-latest recovery completes in `< 2 minutes` wall-clock on the production DB
- stale source snapshots fail-close by default
- no formatting footguns

### Phase 2: Canonical timestamp hardening

Goal:

- remove text-format dependence from timestamp identity

Work:

- canonicalize at write boundaries first
- audit all `window_start` reads/writes
- normalize canonical serialization at write boundaries through one shared helper
- backfill existing `wallet_metrics.window_start` rows to canonical UTC `+00:00`
- clean mixed-format duplicates if they exist
- reduce exact-string dependence where logical timestamp equality is intended

Exit criteria:

- no operational difference between `Z` and `+00:00`
- snapshot lookup no longer depends on textual UTC spelling as its only identity mechanism

### Phase 3: Clean source-of-truth model

Goal:

- make trusted selection source explicit and singular

Work:

- formalize latest trusted snapshot metadata
- formalize snapshot lineage and immutable snapshot identity
- formalize source-kind and trust state
- define legacy migration for existing `wallet_metrics` rows
- make trusted snapshot contract self-contained enough for bootstrap reasoning
- ensure startup/bootstrap consumes only trusted persisted selection source
- ensure empty-followlist startup still converges to trusted top-N on the first safe recovery cycle
- keep historical `followlist.active` out of startup truth
- add selection/bootstrap as an explicit top-level go/no-go gate in production readiness checklists

Exit criteria:

- restart behavior is deterministic and explainable
- "latest" and "trusted" are no longer conflated
- selection state is explicit and operator-visible

### Phase 4: Make heavy rebuild a fallback only

Goal:

- raw-history reconstruction remains available, but not on the critical restart path

Work:

- if kept, make rebuild path resumable/chunked
- add explicit operator budgets and abort thresholds for downtime / memory / WAL
- use it only when no acceptable trusted snapshot exists, or when an explicit stale override is rejected as unsafe
- document it as exceptional maintenance, not standard recovery
- keep bridge-removal as a later step only after aggregate/parity work proves an equivalent trusted persisted source

Starting default budgets if heavy rebuild is attempted:

- abort if downtime exceeds `10 minutes`
- abort if RSS exceeds `2 GiB`
- abort if WAL growth exceeds `500 MiB`

Exit criteria:

- operator recovery does not rely on full-history rebuild under normal incidents
- heavy rebuild has explicit decision-tree and abort/restart semantics

## Acceptance Criteria for the Redesign

The redesign is not complete until all of the following are true.

### Functional

- after restart, bot restores trusted top-N in `< 2 minutes` for routine clone-latest recovery on the production DB
- `followlist.active == follow_top_n` or `<= follow_top_n` when insufficient eligible wallets exist
- recovery works both from recovered historical followlist state and from empty followlist startup
- new higher-ranked wallets are admitted over time
- lower-ranked wallets are removed over time

### Operational

- no full-window raw scan is required for routine bootstrap
- no giant memory spike is required
- no hour-long maintenance stop is required
- operator recovery path is documented and repeatable
- routine bridge recovery does not require manual raw SQL or manual offline checkpoint cleanup
- post-bootstrap validation includes a short soak window (`15-30 minutes`) with stable WAL/contention/queue telemetry
- post-bootstrap validation includes a fresh discovery cycle plus fresh downstream-output evidence, or an explicit documented `N/A` when no qualifying signal opportunity occurred
- heavy rebuild fallback has explicit progress, cursor, and abort semantics

### Safety

- invalid historical followlist cannot become truth again
- raw partial window cannot become bootstrap truth again
- stale snapshots older than the allowed bootstrap age budget fail-close by default unless explicitly overridden
- if trusted selection source is unavailable, system fail-closes

### Observability

- operator can answer within minutes:
  - why bot is active
  - why bot is fail-closed
  - what trusted snapshot id / window / source kind it is using
  - how old the active bridged snapshot is
  - how many active wallets it is following
  - whether selection truth is `trusted_current`, `trusted_bridged`, `trusted_bridged_stale`, or `invalid`

## Non-Goals

This redesign is not:

- a full rewrite of ingestion/runtime
- a full rewrite of discovery scoring math
- a scoring-policy rewrite (`follow_top_n`, eligibility thresholds, etc.)
- an aggregate-scoring project
- a new live execution rollout

## Immediate Next Steps

1. Convert the successful clone-latest recovery into an official admin tool/runbook with age guard, dry-run, and post-checks.
2. Add timestamp canonicalization hardening at write boundaries and backfill existing mixed-format bucket values.
3. Formalize trusted snapshot metadata, lineage semantics, and legacy migration rules.
4. Add selection/bootstrap as a top-level go/no-go gate in production readiness documents.
5. Leave heavy raw rebuild logic as fallback only, and do not retire the bridge path until aggregate/parity work proves an equivalent trusted source.

## Final Architectural Rule

The system must obey this rule:

> Restart recovery must be cheap, trusted, and operationally boring.

If recovering a valid top-N universe requires giant rescans, long downtime, or hand-crafted SQL guesswork, the architecture is still wrong.
