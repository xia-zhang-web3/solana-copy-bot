# solana-copy-bot

Rust workspace for live Solana copy-trading research and Discovery V2
publication safety.

Current priority is Discovery V2 and build-architecture cleanup. The project is
operated fail-closed: discovery must prove fresh raw truth and publication
quality before any production activation can be trusted.

## Current Contract

1. `execution.enabled` stays `false` unless the user explicitly authorizes a
   separate execution rollout.
2. Production hosts are not normal build machines. Build artifacts locally or
   in CI and deploy the verified binaries.
3. `copybot-app` owns live runtime only; do not add operator bins under
   `crates/app/src/bin`.
4. Discovery V2 proof and publish surfaces live in `crates/discovery-v2`.
5. Legacy raw-history/program-history restore, aggregate backfill gates,
   standalone adapter/executor services, and activation-package lanes are not
   active procedure.

## Workspace

Active crates:

1. `crates/app` — live runtime
2. `crates/config` — config loading and validation
3. `crates/core-types` — shared DTOs
4. `crates/discovery` — retained legacy runtime export / snapshot support
5. `crates/discovery-v2` — current discovery proof and publication operators
6. `crates/ingestion` — swap ingestion sources
7. `crates/live-ops` — bounded live-service operators
8. `crates/operators` — dedicated operator crate
9. `crates/shadow` — shadow accounting / risk helpers
10. `crates/storage` — legacy runtime storage
11. `crates/storage-core` — lightweight storage facade for operators
13. `crates/storage-ops` — storage operators

## Quick Start

```bash
cargo run -p copybot-app -- --config configs/dev.toml
```

Alternative config path:

```bash
SOLANA_COPY_BOT_CONFIG=configs/paper.toml cargo run -p copybot-app
```

Run Discovery V2 status:

```bash
cargo run -p copybot-discovery-v2 --bin discovery_v2_status -- --config configs/prod.toml
```

Run Discovery V2 publish dry-run:

```bash
cargo run -p copybot-discovery-v2 --bin discovery_v2_publish -- --config configs/prod.toml
```

## Config Profiles

1. `configs/dev.toml` — local development defaults
2. `configs/paper.toml` — paper/runtime-ops profile
3. `configs/prod.toml` — production scaffold with execution disabled
4. `configs/live.toml` — live template baseline, execution disabled

## Production Templates

Active systemd/config templates are in `ops/server_templates`.

The active template set is intentionally small:

1. app service
2. app env
3. live config example
4. runtime export service/timer
5. recent_raw snapshot service/timer

## Build Policy

Read these before adding operators, crates, or release process changes:

1. `BUILD_POLICY.md`
2. `BUILD_REFACTOR_ROADMAP.md`
3. `ARTIFACT_DEPLOY.md`
4. `BUILD_ARCHITECTURE_REDESIGN_NO_HOUR_BUILDS.md`
5. `ARCHITECTURE_WAIVERS.md`

Run the architecture guard after large cleanup or boundary changes:

```bash
./tools/architecture_guard.sh
```

## Roadmap

The active roadmap summary is `ROAD_TO_PRODUCTION_v2.md`.

Historical incident docs and deleted operator lanes should be recovered from
git history only when needed. They are not active operating instructions.
