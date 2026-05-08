# Artifact Deploy

Status: mandatory deployment policy
Date: 2026-05-03

This document defines the deployment path that replaces production-local
release builds.

## 1. Rule

Production receives artifacts. Production does not compile by default.

Normal deploy path:

1. build elsewhere,
2. test elsewhere,
3. package artifacts,
4. upload artifacts,
5. verify checksums on production,
6. install exact binaries,
7. restart only affected services.

## 2. Artifact Layout

```text
artifacts/
  linux-x86_64/
    <package>-<git_sha>/
      build-manifest.json
      SHA256SUMS
      discovery_v2_status
      discovery_v2_publish
      discovery_runtime_export
      discovery_recent_raw_snapshot
      copybot_operator_emergency_stop
      copybot_live_service_control_wrapper
      copybot_yellowstone_source_probe
      copybot_runtime_sqlite_wal_maintenance
      copybot_runtime_sqlite_wal_pressure_report
    <package>-<git_sha>.tar.gz
    <package>-<git_sha>.tar.gz.sha256
```

Only include binaries built for the batch.

## 3. Build Manifest

Required `build-manifest.json`:

```json
{
  "git_sha": "...",
  "built_at": "...",
  "builder_host": "...",
  "rustc": "...",
  "cargo": "...",
  "target": "x86_64-unknown-linux-gnu",
  "profile": "operator-release",
  "checks": [
    "cargo test --locked -p copybot-discovery-v2 --lib -- --test-threads=1; cargo test --locked -p copybot-discovery-v2 --tests --no-run",
    "tools/architecture_guard.sh --changed",
    "tools/architecture_guard.sh --all"
  ],
  "expected_binaries": ["discovery_v2_publish", "discovery_v2_status"],
  "binaries": [
    {
      "name": "discovery_v2_status",
      "source_package": "copybot-discovery-v2",
      "sha256": "..."
    }
  ]
}
```

## 4. Builder Commands

Discovery V2 operators:

```bash
tools/build_operator_artifacts.sh
```

Discovery scheduled operators:

```bash
PACKAGE=copybot-discovery-ops \
WANTED_BINS="discovery_runtime_export discovery_recent_raw_snapshot" \
tools/build_operator_artifacts.sh
```

Source operators:

```bash
PACKAGE=copybot-operators \
WANTED_BINS=copybot_yellowstone_source_probe \
tools/build_operator_artifacts.sh
```

Live operators:

```bash
PACKAGE=copybot-live-ops \
WANTED_BINS="copybot_operator_emergency_stop copybot_live_service_control_wrapper" \
tools/build_operator_artifacts.sh
```

Storage operators:

```bash
PACKAGE=copybot-storage-ops \
WANTED_BINS="copybot_runtime_sqlite_wal_maintenance copybot_runtime_sqlite_wal_pressure_report" \
tools/build_operator_artifacts.sh
```

The script runs the architecture guard, tests, operator build, checksum
generation, manifest generation, and tarball packaging.

CI builder:

```text
.github/workflows/operator-artifacts.yml
```

The workflow builds operator packages and the live daemon artifact.
Manual `workflow_dispatch` is the full artifact proof path and must build every
matrix package, including `copybot-app`. Push and pull-request runs may skip the
daemon artifact when the changed paths cannot affect the daemon runtime graph.

Runtime daemon artifact:

```bash
PACKAGE=copybot-app \
WANTED_BINS=copybot-app \
tools/build_operator_artifacts.sh
```

Build the runtime daemon only on a builder or CI host. Do not build
`copybot-app` for read-only Discovery V2 proof.

## 5. Packaging

```text
artifacts/linux-x86_64/<package>-<git_sha>/
artifacts/linux-x86_64/<package>-<git_sha>.tar.gz
artifacts/linux-x86_64/<package>-<git_sha>.tar.gz.sha256
```

Use `ALLOW_DIRTY=1` only for local smoke artifacts. Production artifacts must
come from a clean tree.

The archive checksum is external. The internal `SHA256SUMS` covers the binaries.
The manifest binds binary names, source package, target, profile, git SHA,
checks, and binary checksums.

## 6. Config Rollout Review

Artifact install does not approve config semantics by itself.

The current refactor branch intentionally changes the live/prod config surface:

1. `configs/live.toml` uses `state/live_runtime.db`,
2. live/prod/template Discovery V2 `min_active_days` is `1`,
3. live/template metric snapshot cadence is `1800` seconds,
4. live/template observed-swap retention is `7` days.

Before daemon rollout, the rollout note must explicitly choose one of:

1. accept these config values as part of the daemon rollout,
2. split them into a config-only rollout with separate proof,
3. revert the config deltas before packaging the daemon artifact.

Read-only operator rollout does not imply daemon config acceptance.

## 7. Production Install

Install Discovery V2 status/publish operators:

```bash
tools/install_operator_artifacts.sh \
  --expect-package copybot-discovery-v2 \
  --expect-target x86_64-unknown-linux-gnu \
  --install-dir /var/www/solana-copy-bot/bin \
  /tmp/copybot-discovery-v2-<git_sha>.tar.gz
```

Install scheduled Discovery operators used by the runtime export and recent raw
snapshot services:

```bash
tools/install_operator_artifacts.sh \
  --expect-package copybot-discovery-ops \
  --expect-target x86_64-unknown-linux-gnu \
  --install-dir /var/www/solana-copy-bot/bin \
  /tmp/copybot-discovery-ops-<git_sha>.tar.gz
```

The installer:

1. extracts tarballs when needed,
2. requires and verifies the external archive checksum sidecar,
3. verifies `build-manifest.json`,
4. verifies expected package/profile/target,
5. verifies `SHA256SUMS`,
6. rejects dirty artifacts unless `--allow-dirty` is explicitly passed,
7. verifies the complete expected binary set for the package,
8. stages into `bin/releases/.staging-<artifact_id>.<pid>/`,
9. writes `INSTALL_COMPLETE`,
10. atomically renames the staged release into `bin/releases/<artifact_id>/`,
11. keeps stable top-level binary symlinks pointed through
   `bin/packages/<package>/current/<binary>`,
12. activates the package by atomically replacing
   `bin/packages/<package>/current`,
13. keeps `bin/operator-artifact-current-<package>.json` as a stable link to
    the active package manifest.

The package-current symlink is the activation point. Top-level binary symlinks
are stable service entrypoints, not per-release activation state.

Read-only operators do not require daemon restart.

Install daemon artifact:

```bash
tools/install_operator_artifacts.sh \
  --expect-package copybot-app \
  --expect-target x86_64-unknown-linux-gnu \
  --install-dir /var/www/solana-copy-bot/bin \
  /tmp/copybot-app-<git_sha>.tar.gz
sudo systemctl restart solana-copy-bot.service
```

Systemd should point to:

```text
/var/www/solana-copy-bot/bin/copybot-app
```

## 8. Rollback

Discovery scheduled operator rollback:

```bash
tools/install_operator_artifacts.sh \
  --expect-package copybot-discovery-ops \
  --expect-target x86_64-unknown-linux-gnu \
  --install-dir /var/www/solana-copy-bot/bin \
  --rollback <previous_artifact_id>
```

Then rerun read-only status. No daemon restart is required for operator
rollback.

Discovery V2 status/publish operator rollback uses the same command with
`--expect-package copybot-discovery-v2`.

Daemon rollback:

```bash
tools/install_operator_artifacts.sh \
  --expect-package copybot-app \
  --expect-target x86_64-unknown-linux-gnu \
  --install-dir /var/www/solana-copy-bot/bin \
  --rollback <previous_artifact_id>
sudo systemctl restart solana-copy-bot.service
```

Then verify:

```bash
systemctl is-active solana-copy-bot.service
systemctl status --no-pager solana-copy-bot.service
```

## 9. Emergency Fallback

Production-local build is emergency fallback only.

Operator emergency build:

```bash
CARGO_BUILD_JOBS=1 cargo build --profile operator-release \
  -p copybot-discovery-v2 \
  --bin discovery_v2_status
```

Source operator emergency build:

```bash
CARGO_BUILD_JOBS=1 cargo build --profile operator-release \
  -p copybot-operators \
  --bin copybot_yellowstone_source_probe
```

Live operator emergency build:

```bash
CARGO_BUILD_JOBS=1 cargo build --profile operator-release \
  -p copybot-live-ops \
  --bin copybot_operator_emergency_stop
```

Storage operator emergency build:

```bash
CARGO_BUILD_JOBS=1 cargo build --profile operator-release \
  -p copybot-storage-ops \
  --bin copybot_runtime_sqlite_wal_pressure_report
```

Discovery scheduled operator emergency build:

```bash
CARGO_BUILD_JOBS=1 cargo build --profile operator-release \
  -p copybot-discovery-ops \
  --bin discovery_runtime_export
```

Daemon emergency build:

```bash
CARGO_BUILD_JOBS=1 cargo build --release -p copybot-app --bin copybot-app # emergency fallback
```

Before emergency build:

1. check disk,
2. check memory,
3. check service pressure,
4. explain why artifact deploy cannot be used.

After emergency build:

1. record build duration,
2. record memory/disk state,
3. record exact command,
4. open a follow-up to restore artifact deploy.

## 10. Rejection Rules

Reject rollout if:

1. it builds on production without emergency reason,
2. it builds `copybot-app` for operator-only changes,
3. it lacks checksums,
4. it lacks manifest,
5. it cannot identify exact binary targets,
6. it restarts daemon for read-only operator install,
7. it skips architecture guard for build/refactor changes.
