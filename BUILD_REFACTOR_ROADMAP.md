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
2. no active oversized-file waiver is required.
3. new violations fail.
4. current repository can pass with only explicit grandfathered inline/include
   debt reports.

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
2. `cargo test --locked -p copybot-discovery-v2 --tests -- --test-threads=1` passes.
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

Completed cuts:

- app-owned operator bins were moved to `copybot-operators`,
  `copybot-storage-ops`, or `copybot-live-ops`, or deleted when obsolete.
- `crates/app/src/bin` is empty; `copybot-app` owns only the daemon target.
- legacy discovery V2 bins and one-off proof/probe/audit lanes were deleted.
- only server-managed scheduled discovery binaries remain, now in
  `copybot-discovery-ops`: `discovery_runtime_export` and
  `discovery_recent_raw_snapshot`.
- runtime artifact and recent_raw helpers moved to `storage-core`/
  `copybot-runtime-artifacts`; scheduled discovery operators no longer depend on
  `copybot-app`, legacy `copybot-discovery`, or monolithic `copybot-storage`.
- future diagnostics must live in dedicated operator crates or be deleted.

## 7. Phase 5: `app/main.rs` Split

Current problem:

```text
crates/app/src/main.rs: under 200 LOC after split
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
- `main.rs` is now under 200 LOC.
- Startup behavior is now explicitly V2-publication-gated; startup tests and
  `copybot-app` check pass.

## 8. Phase 6: `observed_swap_writer.rs` Split

Current problem:

```text
crates/app/src/observed_swap_writer.rs: about 111 LOC after split
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
- `observed_swap_writer.rs` is now a small facade around extracted modules.
- Observed-writer tests and `copybot-app` check pass.

## 9. Phase 7: Legacy Discovery Quarantine

Current problem:

```text
crates/discovery/src/lib.rs: about 154 LOC after quarantine split
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
  `cargo test --locked --manifest-path crates/discovery/Cargo.toml --lib --no-run`
  and targeted compatibility integration tests such as
  `cargo test --locked --manifest-path crates/discovery/Cargo.toml --test restore_verdict`.
- operator artifact CI no longer rebuilds app/operators for edits under
  `crates/discovery/**`; architecture guard still runs for all `crates/**`
  changes.

## 10. Phase 8: Storage Monolith Split

Current problem:

```text
crates/storage/src/lib.rs: about 105 LOC after split
crates/storage/src/market_data.rs: about 82 LOC after split
crates/storage/src/discovery_scoring.rs: about 65 LOC after split
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
