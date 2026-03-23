# DISCOVERY RUNTIME RESTORE PLAN

Date: 2026-03-23
Status: Active
Scope: narrow implementation plan that supplements [ROAD_TO_PRODUCTION_v2.md](/Users/blacktower/Documents/solana-copy-bot/ROAD_TO_PRODUCTION_v2.md); it does not replace that roadmap

## 0. Why this document exists

`ROAD_TO_PRODUCTION_v2.md` already removed aggregate/backfill recovery from the
runtime critical path.

What is still missing is a concrete restore contract for discovery/runtime.

Right now the code already treats exact recent publication truth as the only
restart-safe control plane:

- startup reads recent publication truth directly in [`crates/app/src/main.rs`](/Users/blacktower/Documents/solana-copy-bot/crates/app/src/main.rs)
- restart/fallback truth is validated in [`crates/discovery/src/lib.rs`](/Users/blacktower/Documents/solana-copy-bot/crates/discovery/src/lib.rs)
- exact published wallet membership is persisted in [`crates/storage/src/discovery.rs`](/Users/blacktower/Documents/solana-copy-bot/crates/storage/src/discovery.rs)

But that truth still lives inside the same SQLite runtime database. That is not
yet a production restore contract.

## 1. Current problem

As of 2026-03-23:

- discovery on live is still `fail_closed`
- `active_follow_wallets = 0`
- the bot is intentionally stopped
- aggregate/backfill is not allowed back into the production boot path

This creates one hard requirement:

- we must stop treating one heavy SQLite database as runtime state, historical archive, and disaster recovery source at the same time

## 2. Decision

The project should not recover from an empty database.

The project should recover from a fresh runtime database that is seeded from a
small runtime artifact.

That artifact must be the restart-safe control plane for discovery.

## 3. What this plan is and is not

This plan is:

- a 1-2 day implementation target for a real runtime restore path
- a path to launch from a fresh runtime DB without waiting for giant history replay
- a way to keep archive/backfill out of the runtime boot path

This plan is not:

- another aggregate recovery program
- a reason to re-enable aggregate reads/writes on prod
- a reason to start from a blank DB with no seed artifact
- a replacement for `ROAD_TO_PRODUCTION_v2.md`

## 4. Restore contract target

The discovery runtime must support this contract:

1. export a runtime artifact from a good runtime state
2. restore a fresh runtime DB from that artifact
3. start the bot with `execution.enabled = false`
4. come up either as:
   - `healthy_runtime_truth`, or
   - `degraded_recent_publication_truth`
5. continue ingesting fresh swaps without waiting for archive replay

Failure target:

- loss of archive/history must not block runtime start

Success target:

- cold restore to bounded degraded runtime: minutes
- cold restore to current healthy runtime truth: bounded and predictable

## 5. Important constraint for the current incident

This plan does not magically create a valid recent published universe if none
exists today.

Current code correctly refuses to invent runtime truth from stale followlist or
ranking residue. Therefore:

- if there is no valid recent publication truth, an empty DB will still fail-close
- the first runtime artifact may be seeded only from a source that can prove
  valid exact recent publication truth

Acceptable one-time bootstrap sources for this incident:

- a last known good runtime DB that still satisfies `recent_runtime_publication_truth`
- an exported runtime artifact cut earlier from such a DB
- the existing offline clone only if it can prove:
  - `recent_publication_truth_available = true`
  - non-empty exact published wallet ids
  - publication freshness still passes the current runtime gate

Not acceptable:

- an offline clone merely because it exists
- any source with stale or missing publication truth
- reconstructing runtime truth from ranking residue or followlist residue

That one-time seed is a bootstrap step for this incident.
It must not become the normal restore model.

## 6. Runtime artifact v1

The first production artifact should contain at minimum:

- exact published wallet ids
- publication metadata:
  - runtime mode
  - publish timestamp
  - published window start
  - scoring source
  - reason
- discovery runtime cursor
- active follow universe snapshot as a derived cache only
- last published wallet metrics snapshot for the published window

The preferred artifact should also contain:

- bounded recent `observed_swaps` slice needed for fast runtime catch-up

The artifact must be external to the runtime SQLite file.
JSON plus optional newline-delimited swap payloads is acceptable for v1.

Truth rule for restore:

- exact recent publication truth is authoritative
- active follow universe snapshot is only a convenience cache
- if follow-universe snapshot disagrees with exact publication truth, restore
  must discard or rebuild the follow snapshot from publication truth

## 7. 48-hour delivery plan

### Slice A. Ship the runtime artifact export/import path

This slice is mandatory.

Add:

- `copybot-discovery` binary to export runtime artifact
- `copybot-discovery` binary to restore a fresh runtime DB from that artifact
- typed storage helpers for export/import of publication truth, runtime cursor,
  active follow universe, and published wallet-metrics snapshot

Likely files:

- [`crates/storage/src/discovery.rs`](/Users/blacktower/Documents/solana-copy-bot/crates/storage/src/discovery.rs)
- [`crates/discovery/src/bin/discovery_runtime_export.rs`](/Users/blacktower/Documents/solana-copy-bot/crates/discovery/src/bin/discovery_runtime_export.rs)
- [`crates/discovery/src/bin/discovery_runtime_restore.rs`](/Users/blacktower/Documents/solana-copy-bot/crates/discovery/src/bin/discovery_runtime_restore.rs)
- [`docs/discovery_runtime_restore.md`](/Users/blacktower/Documents/solana-copy-bot/docs/discovery_runtime_restore.md)

Minimum tests:

- export reads exact publication truth, not inferred ranking
- restore writes exact publication truth into fresh DB
- restored DB keeps stale followlist residue out of runtime truth
- restored DB produces correct `discovery_status`

Exit criteria:

- a fresh DB restored from artifact reports `recent_publication_truth_available = true`
- startup uses the restored exact published universe
- aggregate state is not consulted for runtime truth

### Slice B. Add bounded fast-catch-up payload

This slice is strongly recommended before live cutover.

Goal:

- avoid “publication truth only” restore that always comes up degraded and then
  slowly reconstructs runtime from scratch

Add one of these, in order of preference:

1. bounded recent `observed_swaps` export/import
2. if the bounded swaps slice is too heavy for the first implementation, keep
   the last published `wallet_metrics` snapshot in the artifact and explicitly
   accept a degraded-first restart for the first live cutover

Rule:

- do not pull archive replay into the boot path

Exit criteria:

- restore can reach either immediate degraded runtime with correct active
  wallets, or fast healthy runtime if bounded recent swaps are present

### Slice C. Restore drill and post-restore gates

This slice is mandatory.

Run a full drill on a fresh runtime DB:

1. export runtime artifact from a trusted source
2. create a new runtime DB
3. restore artifact into the new DB
4. run `discovery_status`
5. start `copybot-app` with execution disabled
6. verify startup runtime mode and active wallet count

For the restore drill itself, `discovery_cutover_readiness` is not a gating
verdict.

Reason:

- current `discovery_cutover_readiness` intentionally includes offline aggregate
  blockers in its verdict
- initial runtime restore is allowed to run with aggregate reads/writes disabled
- therefore `discovery_cutover_readiness` belongs to a later post-restore
  cutover stage, not to the initial seeded-runtime restore acceptance gate

Required restore-drill validation command:

```bash
cargo run -p copybot-discovery --quiet --bin discovery_status -- --config configs/live.toml --db-path /path/to/restored-runtime.db --json
```

Later post-restore command, not a gate for initial seeded-runtime recovery:

```bash
cargo run -p copybot-discovery --quiet --bin discovery_cutover_readiness -- --config configs/live.toml --db-path /path/to/restored-runtime.db --json
```

## 8. Launch sequence for the current incident

1. Keep production service stopped until the restored runtime DB is ready.
2. Use only a bootstrap source that proves valid exact recent publication truth
   to cut the first runtime artifact.
3. Restore that artifact into a fresh runtime DB.
4. Verify `discovery_status` on the restored DB.
5. Start `copybot-app` against the restored DB with:
   - `execution.enabled = false`
   - aggregate reads/writes still disabled
6. Observe for:
   - runtime mode
   - `active_follow_wallets > 0`
   - no false healthy state
   - no dependency on aggregate readiness
7. Only after stable runtime truth returns, continue with later discovery
   freshness validation.

## 9. Done means

This plan is done only when all of the following are true:

- a fresh runtime DB can be created from an external artifact
- runtime startup no longer depends on the old heavy DB
- restart uses exact published wallet membership from the artifact
- `followlist` is treated as a derived cache, not runtime truth
- aggregate recovery remains outside the runtime boot path
- the restore drill is repeatable and documented

## 10. Explicitly not allowed

- no blank-DB start in production without a seed artifact
- no aggregate/backfill validation on the production host
- no “just wait for replay to finish” as the runtime restore model
- no using aggregate readiness as a proxy for runtime wallet truth
- no reopening the old monolithic recovery path as the normal operating model

## 11. Realistic promise for the next 1-2 days

The realistic production target for the next 1-2 days is:

- ship a restoreable seeded runtime DB contract
- prove a fresh runtime DB can come up in bounded degraded mode from exact
  publication truth
- optionally add bounded recent swaps payload so healthy catch-up becomes faster

What is not realistic for the same window:

- redesign the entire historical archive stack
- make archive/history loss irrelevant without first shipping the runtime
  artifact itself

That means the immediate priority is narrow and clear:

- build the runtime artifact
- restore from it
- drill it
- launch from it
