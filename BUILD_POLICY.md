# Build Policy

Status: mandatory architecture contract
Date: 2026-05-03

This policy exists to stop hour-long builds, large unreviewable files, and
operator code being added to the live daemon crate.

## 1. Production Build Ban

Production servers are not default build machines.

Rejected normal rollout pattern:

```bash
cargo build --release # rejected normal rollout pattern
cargo build --release -p copybot-app # rejected normal rollout pattern
```

Accepted normal rollout pattern:

1. build on local builder or CI,
2. package binary artifacts,
3. generate checksums and manifest,
4. upload artifacts to production,
5. verify manifest and checksums on production,
6. install only affected binaries,
7. restart only affected services.

Production-local Cargo builds are allowed only as emergency fallback. The
rollout note must state:

1. why artifact deploy was impossible,
2. exact package/bin built,
3. build profile,
4. start/end time,
5. memory/disk pressure check,
6. reason this does not become the default path.

## 2. Build Budgets

Build time is an architecture gate.

| Target | Cold builder budget | Warm builder budget |
| --- | ---: | ---: |
| Discovery V2 operators | 8 min | 90 sec |
| Storage-core operators | 12 min | 2 min |
| `copybot-app` | 20 min | 5 min |

A batch that makes these worse is rejected unless it includes a written,
time-boxed waiver and a follow-up removal task.

## 3. App Quarantine

`copybot-app` is the live daemon only.

Do not add:

1. new operator binaries under `crates/app/src/bin`,
2. read-only diagnostics under `crates/app/src/bin`,
3. discovery proof tools under `crates/app/src/bin`,
4. artifact/report generators under `crates/app/src/bin`,
5. new heavy dependencies unless live daemon behavior directly requires them.

New operators must go into dedicated operator crates, for example:

```text
crates/discovery-v2/
crates/operators/
crates/live-ops/
crates/live-proof/
crates/storage-ops/
```

`crates/operators/` is for standalone operator binaries that may need
source-specific clients such as Yellowstone. These crates still must not depend
on `copybot-app`, `copybot-ingestion`, monolithic storage, legacy discovery,
shadow, or execution.

`crates/storage-ops/` is for SQLite/storage maintenance operators. It must not
pull daemon, ingestion, shadow, execution, legacy discovery, monolithic storage,
Yellowstone, or tonic dependencies.

`crates/live-ops/` is for operational safety/control binaries such as emergency
stop and bounded service-control wrapper tooling. It must stay independent from
daemon, storage, discovery, execution, shadow, ingestion, Yellowstone, and
tonic dependencies.

Any new dependency added to `copybot-app` must answer:

1. Which live daemon path needs this?
2. Why can this not live in an operator crate?
3. Which binary rebuild budget is affected?

Legacy app-bin deletion rule:

1. removed operator/report/generator bins are not dormant features,
2. they are deleted build-graph debt,
3. restoring a deleted bin requires a dedicated operator crate,
4. restoring it under `crates/app/src/bin` is rejected by default,
5. package/archive/activation generators must never be reintroduced into the
   daemon crate,
6. Stage 4/devnet/execution-rehearsal operators are not allowed back
   into `copybot-app`; a future execution lane must live in dedicated crates,
7. aggregate repair, recent_raw catch-up, and source-inspection audits are not
   allowed back into `copybot-app`.

Legacy discovery/storage operator deletion rule:

1. deleted legacy `copybot-discovery` proof/probe/audit bins are not dormant
   production surfaces,
2. deleted aggregate backfill binaries and wrappers are old repair-lane debt,
3. restoring any deleted legacy discovery/storage operator requires a dedicated
   operator crate and a current production reason,
4. restoring them into `copybot-discovery/src/bin` or `copybot-storage/src/bin`
   is rejected by default,
5. active scheduled discovery binaries live in `copybot-discovery-ops`, not
   `copybot-discovery/src/bin`.
6. `discovery_runtime_export` must stay focused on scheduled/manual runtime
   artifact export; recent_raw explain helpers, checkpoint-row-fetch probes,
   publication-truth blocker explainers, and replay-sol-leg traces are rejected
   in that binary.

## 4. File-Size Policy

New production files must be small.

| File class | Soft limit | Hard limit |
| --- | ---: | ---: |
| library module | 400 LOC | 600 LOC |
| runtime module | 400 LOC | 600 LOC |
| operator bin | 200 LOC | 300 LOC |
| CLI arg parser | 150 LOC | 250 LOC |
| test file | 500 LOC | 800 LOC |
| generated file | waiver required | waiver required |

Definitions:

- LOC means physical lines.
- Comments count.
- Blank lines count.
- Test fixtures count in test files.
- Large JSON/golden fixtures must be external fixture files, not embedded in
  Rust source.

Hard limit exceptions require an architecture waiver.

## 5. Existing Oversized Files

No active oversized-file waivers remain.

The previous oversized waivers for `copybot-app` main loop, observed-swap
writer facade, storage facade, market data facade, discovery-scoring facade,
`copybot-discovery` lib facade, and the discovery run-cycle/rebuild files are
retired. Those files are now below the hard limit and must stay under the
normal guard.

Rules:

1. no new feature work may expand these files,
2. bug fixes may touch them only when the live blocker is inside them,
3. any touch must prefer extraction over adding code,
4. every batch touching them must state whether line count went up or down.

## 6. Test Placement Policy

No inline test bodies in production files.

Rejected pattern:

```rust
#[cfg(test)]
mod tests {
    // test bodies here
}
```

Accepted patterns:

```rust
#[cfg(test)]
mod tests;
```

with test bodies in:

```text
src/tests.rs
src/*_tests.rs
src/*_tests/*.rs
src/<module>/tests.rs
tests/<integration_test>.rs
```

New production files may contain only a test module declaration, not test
bodies. Existing inline tests are grandfathered only as debt and must not grow.

## 7. Dependency Policy

Operator crates must not depend on live runtime crates unless absolutely
required.

Discovery V2 operators must not depend on:

1. `copybot-app`,
2. `copybot-ingestion`,
3. Yellowstone crates,
4. `tonic`,
5. `copybot-shadow`,
6. current monolithic `copybot-discovery`,
7. current monolithic `copybot-storage` after `storage-core` extraction.

Discovery V2 target dependencies:

```text
copybot-config
copybot-core-types
copybot-storage-core
anyhow
chrono
serde
serde_json
rusqlite only if direct SQLite is required
```

## 8. Storage Policy

`copybot-storage` is currently too broad. Operator builds must move to
`storage-core`.

`copybot-discovery-ops` must not depend directly on `copybot-storage` or
`copybot-discovery`.

`storage-core` owns:

1. SQLite open/open_read_only,
2. observed swap bounded reads,
3. runtime cursor reads,
4. token quality cache reads,
5. follow wallet writes,
6. publication state reads/writes.

`storage-core` must not own:

1. aggregate repair,
2. historical backfill,
3. restore lanes,
4. HTTP/RPC quality fetching,
5. shadow/execution helpers,
6. app telemetry-only storage.

## 9. Automated Enforcement

Architecture rules must be enforced by a guard.

Required tool:

```text
tools/architecture_guard.sh
```

The guard must check:

1. oversized files,
2. growth of grandfathered oversized files,
3. inline `#[cfg(test)] mod tests` bodies in production files,
4. new files under `crates/app/src/bin`,
5. forbidden dependencies in operator crates,
6. duplicate workspace bin names, including explicit `[[bin]]` names,
7. production-local build commands in rollout docs,
8. build profile presence,
9. artifact deploy docs presence.

The guard must support:

```bash
tools/architecture_guard.sh --changed
tools/architecture_guard.sh --all
```

`--changed` is required for normal batches. `--all` is required before
architecture refactor acceptance.

## 10. Waivers

Waivers are explicit and temporary.

Required waiver file:

```text
ARCHITECTURE_WAIVERS.md
```

Each waiver must include:

1. file/path,
2. violated rule,
3. reason,
4. owner,
5. expiry date or removal batch,
6. proof that no smaller cut was feasible.

No silent exceptions.

## 11. Acceptance Standard

A build/refactor batch is accepted only if:

1. it does not add new app operators,
2. it does not add inline test bodies,
3. it does not grow known oversized files unless waiver exists,
4. it declares exact binary build targets,
5. it passes architecture guard,
6. it does not require production-local build in the normal path.
