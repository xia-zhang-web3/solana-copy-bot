# Build Refactor Roadmap

Status: mandatory migration plan
Date: 2026-05-03

This roadmap turns the build policy into cutting tasks.

## 1. Goal

Remove hour-long production builds by changing the build graph:

1. production gets artifacts,
2. operators are separate from daemon code,
3. Discovery V2 is independent,
4. storage-core replaces the storage monolith for operators,
5. `copybot-app` stops accumulating bins and dependencies,
6. large files are split until normal changes stay inside small modules.

## 2. Phase 0: Architecture Guard

This is the first required batch.

Files to add:

```text
tools/architecture_guard.sh
ARCHITECTURE_WAIVERS.md
```

Guard checks:

1. file size,
2. grandfathered oversized file growth,
3. inline test bodies,
4. new `crates/app/src/bin` files,
5. forbidden operator dependencies,
6. production-local build commands in rollout docs.

Acceptance:

1. `tools/architecture_guard.sh --changed` runs.
2. known oversized files are listed as baseline debt.
3. new violations fail.
4. current repository can pass via explicit baseline/waivers only.

## 3. Phase 1: Artifact Deploy First

Status: implemented for Discovery V2 operators in Batches 1-3.

Build artifacts off production before any deep Rust refactor.

Files to add:

```text
tools/build_operator_artifacts.sh
tools/install_operator_artifacts.sh
tools/build_manifest.py
tools/verify_operator_artifact.py
.github/workflows/operator-artifacts.yml
```

Acceptance:

1. V2 operator binaries can be built off production.
2. artifact includes `SHA256SUMS`.
3. production install verifies checksums.
4. V2 status runs on server without Cargo.
5. no daemon restart is required for read-only V2 status.

## 4. Phase 2: Storage-Core Facade

Status: implemented in Batch 2 as the first thin facade.

This phase comes before Discovery V2 extraction because V2 cannot be ultra-fast
if it depends on the current storage monolith.

Implemented:

```text
crates/storage-core/Cargo.toml
crates/storage-core/src/lib.rs
crates/storage-core/src/db.rs
crates/storage-core/src/observed.rs
crates/storage-core/src/quality.rs
crates/storage-core/src/publication.rs
crates/storage-core/src/schema.rs
crates/storage-core/src/types.rs
```

Move only APIs needed by Discovery V2:

1. open/open_read_only,
2. migrations needed by runtime DB,
3. observed swaps bounded read,
4. observed swaps tail cursor,
5. token quality cache get/upsert if publish path needs it,
6. publication state read/write,
7. follow wallet write.

Do not move:

1. backfill,
2. aggregate repair,
3. restore operators,
4. RPC fetchers,
5. shadow/execution helpers.

Acceptance:

1. `cargo check -p copybot-storage-core` passes.
2. `copybot-storage-core` does not depend on `reqwest`.
3. `copybot-storage-core` does not depend on app/shadow/execution/ingestion.
4. storage-core files obey size policy or have waivers.

## 5. Phase 3: Discovery V2 Extraction

Status: implemented in Batch 2 as a separate operator crate with status and
publish binaries.

Implemented:

```text
crates/discovery-v2/Cargo.toml
crates/discovery-v2/src/lib.rs
crates/discovery-v2/src/accumulator.rs
crates/discovery-v2/src/status.rs
crates/discovery-v2/src/metric.rs
crates/discovery-v2/src/filters.rs
crates/discovery-v2/src/rug.rs
crates/discovery-v2/src/tradability.rs
crates/discovery-v2/src/token_market.rs
crates/discovery-v2/src/policy.rs
crates/discovery-v2/src/publish.rs
crates/discovery-v2/src/bin/discovery_v2_status.rs
crates/discovery-v2/src/bin/discovery_v2_publish.rs
crates/discovery-v2/tests/status_publish.rs
```

Rules:

1. bins are thin argument parsers,
2. business logic lives in library modules,
3. tests are outside production files,
4. no dependency on legacy `copybot-discovery`,
5. no dependency on monolithic `copybot-storage`.

Acceptance:

1. `cargo test --locked -p copybot-discovery-v2 --lib -- --test-threads=1` passes.
2. `cargo test --locked -p copybot-discovery-v2 --tests --no-run` passes.
3. `cargo check --locked -p copybot-discovery-v2 --bins` passes.
4. build graph excludes app, ingestion, Yellowstone, shadow, execution, legacy
   discovery, and monolithic storage.
5. V2 status/publish artifacts can be installed and run without rebuilding
   `copybot-app`.

## 6. Phase 4: `copybot-app` Quarantine

Status: app-bin evacuation is complete.

No new app bins. `copybot-app` owns the daemon target only. Operational
commands must live in dedicated operator crates, ship as artifacts, and avoid
pulling the live daemon dependency graph.

Allowed operator homes:

```text
crates/operators/
crates/live-ops/
crates/live-proof/
crates/storage-ops/
```

Acceptance:

1. no new app bins are added,
2. moved operators compile outside `copybot-app`,
3. app Cargo dependencies do not grow from operator work,
4. architecture guard blocks new app bins,
5. moved operator names are removed from `crates/app/src/bin` to avoid
   duplicate workspace bin targets,
6. artifact packaging supports each operator package explicitly.

Batch 4 start:

- `copybot_yellowstone_source_probe` moves to `copybot-operators`.
- `copybot-app` no longer owns that binary name after the move.
- Discovery V2 duplicate legacy names were removed from legacy
  `copybot-discovery`; `discovery_v2_status` and `discovery_v2_publish` now
  belong only to `copybot-discovery-v2`.

Batch 7 start:

- `copybot_runtime_sqlite_wal_maintenance` moves from `copybot-app` to
  `copybot-storage-ops`.
- `copybot_runtime_sqlite_wal_pressure_report` moves from `copybot-app` to
  `copybot-storage-ops`.
- shared WAL file metadata/path helpers live in
  `crates/storage-ops/src/runtime_sqlite_wal/common.rs`.
- SQLite checkpoint execution lives in
  `crates/storage-ops/src/runtime_sqlite_wal/checkpoint.rs`.
- moved tests live outside production files in
  `maintenance_tests.rs` and `pressure_tests.rs`.
- artifact packaging now supports `copybot-storage-ops` and package-prefixed
  artifact IDs to avoid same-SHA package collisions.

Batch 8 start:

- `copybot_operator_emergency_stop` moves from `copybot-app` to
  `copybot-live-ops`.
- `copybot_live_service_control_wrapper` moves from `copybot-app` to
  `copybot-live-ops`.
- service-control wrapper tests no longer import the large execution
  app bin just to deserialize status JSON; they use a local minimal schema.
- live-ops artifact packaging is first-class and does not build `copybot-app`.

Completed cuts:

- Yellowstone source probe moved to `copybot-operators`.
- WAL maintenance and WAL pressure operators moved to `copybot-storage-ops`.
- emergency stop and service-control wrapper moved to `copybot-live-ops`.
- obsolete activation, devnet rehearsal, execution-readiness, guardrail audit,
  raw-history, restore, backfill, aggregate repair, and one-off proof/probe
  lanes were deleted instead of moved.
- `crates/app/src/bin` was emptied; future diagnostics must be built in
  dedicated operator crates or deleted.
- deleted activation/devnet/restore/backfill lanes are not compatibility
  surfaces. Reintroducing one requires a new explicit operator-crate design,
  fail-closed tests, artifact packaging, and a current production reason.

Legacy discovery scheduled exports retained for now:

- `discovery_runtime_export`
- `discovery_recent_raw_snapshot`

These now live in `copybot-discovery-ops`. `discovery_recent_raw_snapshot` and
runtime artifact export use `storage-core`, and this crate has no direct
dependency on `copybot-discovery` or `copybot-storage`. They must not become a
place to restore old probe or repair modes.

Batch 12 cut:

- all remaining `crates/app/src/bin` operator/diagnostic targets were removed.
- removed app-owned runtime/Stage 3 diagnostic bins:
  - `copybot_discovery_aggregate_repair`
  - `copybot_discovery_scoring_fact_writer_blocker_report`
  - `copybot_recent_raw_journal_catch_up`
  - `copybot_runtime_writer_commit_path_audit`
  - `copybot_observed_swap_extraction_handoff_audit`
  - `copybot_observed_swap_ingress_enqueue_audit`
- app-bin source is now zero; `copybot-app` owns only the daemon target.
- aggregate repair is no longer an app-owned escape hatch.
- future diagnostics must live in dedicated operator crates or be deleted.

Batch 13 cut:

- removed the old aggregate backfill binary and shell wrapper
- removed the historical Stage 1 aggregate recovery program document that
  pointed operators back to the deleted backfill lane.
- removed the remaining one-off legacy discovery proof/probe/audit bin zoo
  from `copybot-discovery/src/bin`.
- kept only the currently server-managed scheduled discovery binaries:
  - `discovery_runtime_export`
  - `discovery_recent_raw_snapshot`
- legacy discovery/storage operator/docs source removed in this cut: 61,746 LOC.
- old publication/scoring/replay/freshness probes are not the Discovery V2
  contract; future proof work must live in Discovery V2 or a dedicated operator
  crate.

Batch 14 cut:

- rewrote `discovery_runtime_export` from a 176,364 LOC probe dump into a 596
  LOC scheduled/manual runtime artifact export binary.
- kept:
  - `--config ... --scheduled --json`
  - `--config ... --output <path>`
- removed from this binary:
  - retained recent_raw explain helper modes,
  - checkpoint-row-fetch probe modes,
  - publication-truth export blocker explain mode,
  - replay-sol-leg blocker/deep/source-compare trace modes,
  - SQLite FFI/VFS/xRead instrumentation and probe-only inline tests.
- this cut removed 176,288 lines from the live scheduled export target.
- future probes must be dedicated operator crates or V2 commands; they must not
  be restored into `discovery_runtime_export`.

Batch 15 cut:

- moved `discovery_runtime_export` and `discovery_recent_raw_snapshot` from
  `copybot-discovery/src/bin` into `copybot-discovery-ops`.
- extracted runtime artifact/journal helpers from legacy discovery into
  `copybot-runtime-artifacts`.
- artifact packaging and CI now include `copybot-discovery-ops`, so scheduled
  discovery operators can be shipped without rebuilding `copybot-app` or the
  legacy discovery crate bins.
- binary names stayed unchanged for existing systemd timer templates.

## 7. Phase 5: `app/main.rs` Split

Current problem:

```text
crates/app/src/main.rs: about 21.0k LOC
```

Target modules:

```text
crates/app/src/main.rs
crates/app/src/startup.rs
crates/app/src/runtime_config.rs
crates/app/src/service_loop.rs
crates/app/src/discovery_cycle.rs
crates/app/src/runtime_pressure.rs
crates/app/src/heartbeat.rs
crates/app/src/shutdown.rs
crates/app/src/risk_guard/mod.rs
crates/app/src/risk_guard/tests.rs
```

First cutting task:

1. move pure config/runtime setup helpers out of `main.rs`,
2. move tests with them,
3. keep behavior identical,
4. reduce `main.rs` LOC,
5. no new dependencies.

Acceptance:

1. `main.rs` line count decreases,
2. new modules obey file-size policy,
3. tests are not inline,
4. `cargo check -p copybot-app --bin copybot-app` passes.

Batch 5 start:

- startup progress/WAL helpers moved to `crates/app/src/startup.rs`.
- `main.rs` dropped from about `21.0k` LOC to about `20.8k` LOC.
- Startup behavior is unchanged; startup tests and `copybot-app` check pass.

## 8. Phase 6: `observed_swap_writer.rs` Split

Current problem:

```text
crates/app/src/observed_swap_writer.rs: about 11.0k LOC
```

Target modules:

```text
crates/app/src/observed_swap_writer/mod.rs
crates/app/src/observed_swap_writer/queue.rs
crates/app/src/observed_swap_writer/recent_raw_journal.rs
crates/app/src/observed_swap_writer/aggregate_replay.rs
crates/app/src/observed_swap_writer/retention.rs
crates/app/src/observed_swap_writer/retry.rs
crates/app/src/observed_swap_writer/capacity.rs
crates/app/src/observed_swap_writer/sqlite_retry.rs
crates/app/src/observed_swap_writer/telemetry.rs
crates/app/src/observed_swap_writer/tests.rs
```

First cutting task:

1. move retryable SQLite lock classification helpers,
2. move tests with them,
3. then move recent_raw journal phase helpers,
4. preserve all reason strings.

Acceptance:

1. original file line count decreases,
2. no reason string changes unless explicitly required,
3. observed-writer tests pass,
4. new modules obey file-size policy.

Batch 5 start:

- capacity/backpressure math moved to `observed_swap_writer/capacity.rs`.
- SQLite fatal/retryable lock classification and bounded retry helper moved to
  `observed_swap_writer/sqlite_retry.rs`.
- `observed_swap_writer.rs` dropped from about `11.0k` LOC to about `10.8k`
  LOC.
- Observed-writer tests and `copybot-app` check pass.

## 9. Phase 7: Legacy Discovery Quarantine

Current problem:

```text
crates/discovery/src/lib.rs: about 57.5k LOC
```

Rules:

1. no new V2 work in this file,
2. no new operators through this file,
3. no restore/backfill lane revival inside legacy discovery,
4. prefer extraction over edits.

Target split:

```text
crates/discovery-v2/
crates/storage-core/
crates/operators/
```

First cutting task:

1. move V2 completely out,
2. remove V2 export from legacy discovery,
3. keep only temporary scheduled legacy exports with active server users,
4. delete obsolete probes instead of moving them by default.

Batch 6 start:

- legacy `crates/discovery/src/bin/discovery_v2_status.rs` removed,
- legacy `crates/discovery/src/bin/discovery_v2_publish.rs` removed,
- architecture guard now rejects all duplicate workspace bin names without a
  V2 waiver.
- legacy `copybot-discovery` is no longer a default workspace member; it remains
  source-quarantined for explicit compatibility work via
  `cargo test --locked --manifest-path crates/discovery/Cargo.toml --lib --no-run`.
- operator artifact CI no longer rebuilds app/operators for edits under
  `crates/discovery/**`; architecture guard still runs for all `crates/**`
  changes.

## 10. Phase 8: Storage Monolith Split

Current problem:

```text
crates/storage/src/lib.rs: about 14.4k LOC
crates/storage/src/market_data.rs: about 9.5k LOC
crates/storage/src/discovery_scoring.rs: about 4.1k LOC
```

Target split:

```text
crates/storage-core/
crates/storage-market/
crates/storage-scoring/
crates/storage-ops/
crates/storage/
```

First cutting task:

1. create storage-core facade for V2,
2. move only observed_swaps and publication APIs,
3. keep compatibility wrapper for old callers,
4. remove V2 dependency on monolithic storage.

Current storage-ops extraction:

- WAL pressure and WAL maintenance operators no longer live under
  `crates/app/src/bin`.
- `copybot-storage-ops` depends on config, rusqlite, chrono, serde/json, and
  anyhow only.
- It must not depend on `copybot-app`, monolithic `copybot-storage`, legacy
  discovery, ingestion, shadow, execution, Yellowstone, or tonic.

## 11. Required Metrics

Every refactor batch must report:

1. files changed,
2. line count before/after for touched oversized files,
3. build target,
4. build time if measured,
5. tests run,
6. architecture guard result.

## 12. Stop Conditions

Stop and reject the batch if:

1. `copybot-app` grows from operator work,
2. new app bin is added,
3. new production file exceeds hard LOC limit,
4. inline tests are added,
5. V2 pulls legacy discovery or monolithic storage after extraction,
6. production rollout requires normal server-local build.
