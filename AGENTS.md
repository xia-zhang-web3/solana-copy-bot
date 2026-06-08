# AGENTS.md

This is the active engineering contract for AI-assisted work in this
repository.

The old patch-loop era is over. The project must be made fast to build, fast to
test, fast to deploy, and easy to change. Do not add more monoliths. Do not
hide slow builds behind patience. Do not turn production incidents into
multi-day compile/repair loops.

## Current Direction

The project is moving to a small-module, artifact-first architecture.

Primary goals:

- isolate production runtime from operator tooling
- remove hour-long production builds from the rollout path
- split monolithic files and crates into bounded modules
- keep production discovery fail-closed until live proof exists
- make every change cheap to review, test, build, deploy, and roll back

The current architecture redesign document is:

- `BUILD_ARCHITECTURE_REDESIGN_NO_HOUR_BUILDS.md`

Treat that document as the roadmap for build and deployment architecture.

## Non-Negotiable Rules

- Do not weaken fail-closed behavior.
- Do not mark production green from local tests, devnet, stale evidence, or
  operator observability.
- Do not use legacy aggregate/materialization repair as the primary production
  readiness path without a new explicit user decision.
- Do not build release binaries on production as the normal rollout path.
- Do not add new operator binaries, proof tools, read-only reports, or heavy
  diagnostics to `copybot-app`.
- Do not add new heavy dependencies to `copybot-app` unless live daemon behavior
  directly requires them.
- Do not add new large files.
- Do not add new inline tests inside production source files.
- Do not add broad refactors hidden inside feature or diagnostic batches.

## Hard File-Size Policy

New and touched code must move toward small files.

Default limits:

- production source file: target <= 400 lines, hard limit <= 600 lines
- binary entrypoint file: target <= 200 lines, hard limit <= 300 lines
- test file: target <= 500 lines, hard limit <= 800 lines
- docs/policy file: target <= 400 lines, hard limit <= 800 lines

Exceptions require an explicit architecture waiver in the batch summary. A
waiver must explain why the file cannot be split now and when it will be split.

No active oversized-file waivers remain. Previously quarantined files include:

- `crates/discovery/src/lib.rs`
- `crates/storage/src/lib.rs`
- `crates/storage/src/market_data.rs`
- `crates/storage/src/discovery_scoring.rs`
- `crates/app/src/main.rs`
- `crates/app/src/observed_swap_writer.rs`

Rules for previously quarantined files:

- keep them below the normal guard limits
- do not add new operator/report logic to them
- only touch them for extraction, deletion, narrow emergency fixes, or wiring
  required to move code out
- every touch must reduce coupling or be justified as a temporary emergency fix

## Test Placement Rules

Production files are not test containers.

Rules:

- no new `#[cfg(test)] mod tests` inside production source files
- new tests go into `tests/`, `*_tests.rs`, or a dedicated test crate/module
- if a touched production file already has inline tests, do not add more inline
  tests unless the batch is explicitly a test-extraction batch
- prefer black-box or module-boundary tests over giant internal fixture blocks
- test fixtures must be small and local to test files

A batch that adds feature code and hundreds of lines of inline tests to the same
file should be rejected.

## Logging And Telemetry Rules

Logging must not become business logic.

Rules:

- no ad hoc logging blocks scattered through core logic
- no `println!` in production runtime code
- CLI binaries may print final operator output
- repeated event fields must use helper structs/functions
- telemetry-heavy code belongs in a telemetry module or operator crate
- logs must be bounded and actionable

If a log statement needs many fields, create a small typed event builder or move
the diagnostic into an operator.

## Build Rules

Production is not a build machine.

Normal rollout path:

1. commit the accepted change
2. push the commit so GitHub Actions builds from a clean tree
3. download the matching CI artifact for that exact git SHA
4. verify SHA256 checksums and manifest locally
5. upload artifacts to production
6. verify checksums on production
7. install only the affected binary
8. restart only the affected service, if required
9. collect live proof

Do not use Docker or a local release build as the normal production rollout
path. Docker/local builds are only for development smoke checks unless an
emergency fallback is explicitly declared and recorded.

Forbidden by default:

- `cargo build --release` on production is forbidden by default
- rebuilding `copybot-app` for read-only operators
- rebuilding the workspace during incident response
- running heavy builds while production runtime is under memory, disk, WAL, or
  SQLite pressure

Emergency production-local builds are allowed only if explicitly marked as
fallback and the reason artifact deploy is unavailable is recorded.

## Build Target Contract

Every batch must declare the exact build target.

Examples:

- Discovery V2 status/report: build `copybot-discovery-v2` operator artifact
- publication dry-run operator: build only that operator artifact
- docs-only change: no build
- config-only proof: no build unless daemon consumes the config at startup
- observed-swap writer runtime change: build `copybot-app`
- ingestion runtime change: build `copybot-app`

If the batch cannot answer "which binary changed?", reject it.

## Build Budgets

Build time is an architecture budget.

Hard budgets on the builder:

- Discovery/operator artifact cold build: <= 8 minutes
- Discovery/operator artifact warm build: <= 90 seconds
- storage-only operator cold build: <= 12 minutes
- storage-only operator warm build: <= 2 minutes
- `copybot-app` cold release build: <= 20 minutes
- `copybot-app` warm release build: <= 5 minutes

If a batch exceeds the relevant budget, treat it as architecture regression.
Do not paper over it with longer timeouts.

## Dependency Rules

Dependencies are architecture.

Rules:

- operators must not depend on `copybot-app`
- Discovery V2 must not depend on legacy discovery or aggregate repair
- Discovery V2 should depend on `storage-core` or a tiny temporary SQLite facade,
  not the current storage monolith
- operator crates must not pull Yellowstone, tonic, execution, shadow, or app
  runtime unless that operator genuinely needs them
- new dependencies in `copybot-app` require explicit justification tied to live
  daemon behavior

If a read-only operator pulls the runtime daemon graph, reject it.

## Required Architecture Direction

Target split:

- `copybot-app`: thin live daemon only
- `copybot-discovery-v2`: current-window discovery status/publish operators
- `copybot-storage-core`: minimal SQLite APIs used by operators/runtime
- `copybot-storage-ops`: backfill, repair, restore, and maintenance tooling
- `copybot-operators`: standalone operational reports and maintenance commands
- `copybot-live-ops`: bundled live proof and service-control commands

Legacy monoliths remain only as compatibility shells while code is extracted.

## Worker Contract

The coding worker implements one bounded batch.

Every worker response must include:

- goal
- files changed
- build target
- tests/checks run
- line-count impact for touched files
- dependencies added or removed
- what was intentionally not touched
- whether production rollout is required

The worker must not self-approve. Passing tests is not acceptance.

## Reviewer Contract

The reviewer must not trust summaries.

Before accepting a batch:

1. read the diff
2. inspect critical semantic paths
3. rerun relevant checks
4. verify file-size policy
5. verify tests are not added into production files
6. verify build target is narrow
7. verify no forbidden dependency creep
8. verify production rollout need

Reject for:

- false green
- stale evidence treated as current
- hidden runtime behavior change
- hidden config semantics change
- new monoliths
- oversized touched files without waiver
- new inline tests in production files
- new operator/report logic in `copybot-app`
- production-local release build as normal path
- broad refactor hidden inside a small batch

## Commit Rules

- Commit only accepted work.
- Do not commit scratch files.
- Do not commit target/build artifacts.
- Do not commit unrelated changes unless the user explicitly requests all
  current changes.
- Use short commit messages.
- Never force-push `main` unless the user explicitly orders it.
- If local and remote diverge, integrate without discarding remote work.

## Server Rules

Before touching production:

1. check service state
2. check current commit
3. check disk and memory
4. check whether a build is already running
5. state exactly what will be installed or restarted

Production install must prefer artifacts over source builds.

After rollout:

1. verify service active
2. verify restart count
3. check recent logs
4. run bounded live proof
5. record whether production remains fail-closed or green

Do not restart the daemon for read-only operator changes.

## Documentation Rules

Docs must be operational, not endless incident dumps.

Keep `AGENTS.md` short and current. Long history belongs elsewhere.

Update docs when:

- architecture rules change
- build/deploy flow changes
- a production incident changes operating procedure
- an operator is deprecated or becomes primary

Do not use docs to hide missing code enforcement.

## Current Immediate Priority

Stop adding to monoliths.

Immediate engineering priority:

1. add/enforce architecture guards for file size, inline tests, forbidden app
   operators, and dependency creep
2. make operator artifact deployment the default
3. extract `storage-core` or a tiny V2 SQLite facade
4. extract Discovery V2 into its own crate
5. quarantine `copybot-app` and legacy discovery/storage monoliths

No more hour-long production builds as a normal workflow.
