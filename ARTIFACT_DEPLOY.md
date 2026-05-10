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

Artifacts live under `artifacts/linux-x86_64/<package>-<git_sha>/` plus the
matching `.tar.gz` and `.tar.gz.sha256`. Each artifact directory contains
`build-manifest.json`, `SHA256SUMS`, and only the binaries built for that
package.

## 3. Build Manifest

`build-manifest.json` must bind git SHA, target, profile, executed checks,
expected binaries, source package, and binary checksums. Production manifests
must include locked package tests plus both architecture guard modes.

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
Changes under `migrations/**` are daemon-affecting because `copybot-app`
applies migrations on startup; CI must build the daemon artifact for migration
changes unless the batch explicitly declares a separate migration-only rollout
proof.

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
come from a clean tree. `tools/install_operator_artifacts.sh --allow-dirty`
is valid only with `--dry-run`; a dirty artifact must never be installed.

The archive checksum is external. The internal `SHA256SUMS` covers the binaries
and, for `copybot-app`, `migrations.tar.gz`. The manifest binds binary names,
source package, target, profile, git SHA, dirty state, checks, binary checksums,
and the daemon migration bundle checksum.

## 6. Config Rollout Review

Artifact install does not approve config semantics by itself. A daemon rollout
must carry an explicit config acceptance note before restart.

The current refactor branch intentionally changes the live/prod config surface:

1. `configs/live.toml` uses `state/live_runtime.db`,
2. live/prod/template Discovery V2 `min_active_days` is `1`,
3. live/prod/template metric snapshot cadence is `1800` seconds,
4. live/prod/template observed-swap retention is `7` days,
5. live/prod/template define `[recent_raw_journal]` and `[runtime_restore_ops]`
   as active operational surfaces,
6. live/prod/template set runtime restore bootstrap snapshot age and cadence gates,
7. live config carries lower shadow/risk caps than prod and must be accepted
   explicitly before daemon restart,
8. live/prod/template execution remains disabled and must not be relaxed,
9. live/prod/template aggregate/materialized discovery readiness remains
   rejected as a production readiness path,
10. live/prod/template runtime artifact/publication freshness gates must stay
   bound to Discovery V2 source, policy fingerprint, and runtime cursor.

Before daemon rollout, review every delta in:

1. `configs/live.toml`,
2. `configs/prod.toml`,
3. `ops/server_templates/live.server.toml.example`,
4. `ops/server_templates/app.env.example`,
5. `ops/server_templates/*.service`,
6. `ops/server_templates/*.timer`,
7. `migrations/**` when the daemon artifact includes migration changes.

Before daemon rollout, the rollout note must explicitly choose one of:

1. accept these config values as part of the daemon rollout,
2. split them into a config-only rollout with separate proof,
3. revert the config deltas before packaging the daemon artifact.

The note must record date, operator, commit, artifact id, exact config files,
chosen option, and whether production remains fail-closed. A green production
state can only be recorded by the postflight live proof section, not by config
review, code review, local tests, devnet, or operator observability.

Read-only operator rollout does not imply daemon config acceptance.

## 7. Production Install

Production preflight for any install or rollback:

1. verify service state for every service that may use the package,
2. record current server commit and target artifact `git_sha`,
3. verify disk and memory headroom,
4. verify no production-local cargo/rustc build is running,
5. state the exact artifact path or rollback id, package, binary, and service
   to touch,
6. verify `execution.enabled` remains disabled when the daemon package is in
   scope,
7. verify config deltas were accepted, split, or reverted when the daemon
   package is in scope.

Install Discovery V2 status/publish operators:

```bash
tools/install_operator_artifacts.sh \
  --expect-package copybot-discovery-v2 \
  --expect-target x86_64-unknown-linux-gnu \
  --install-dir /var/www/solana-copy-bot/bin \
/tmp/copybot-discovery-v2-<git_sha>.tar.gz
```

`discovery_v2_publish --commit` requires
`--acknowledge-daemon-restart-required`. The live daemon samples publication
truth at startup, so a committed publish must be followed by a daemon restart or
an explicitly implemented reload before the live follow surface can use it.

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

It verifies archive and binary checksums, manifest identity, package/profile/
target, dirty state, expected binaries, migration bundle checksums, symlink
targets, and immutable release collisions. Normal installs stage into
`.staging-<artifact_id>.<pid>/`, write `INSTALL_COMPLETE`, atomically rename
into `bin/releases/<artifact_id>/`, install `copybot-app` migrations when
present, and then atomically switch `bin/packages/<package>/current`.

The package-current symlink is the activation point. Top-level binary symlinks
are stable service entrypoints, not per-release activation state.

Read-only operators do not require daemon restart, but they still require the
production preflight above before touching `/var/www/solana-copy-bot/bin`.

Daemon artifact rollout order:

1. complete daemon restart preflight,
2. install the daemon artifact,
3. restart the daemon,
4. complete daemon restart postflight.

Install daemon artifact after preflight:

```bash
tools/install_operator_artifacts.sh \
  --expect-package copybot-app \
  --expect-target x86_64-unknown-linux-gnu \
  --install-dir /var/www/solana-copy-bot/bin \
  /tmp/copybot-app-<git_sha>.tar.gz
```

Systemd should point to:

```text
/var/www/solana-copy-bot/bin/copybot-app
```

Daemon restart preflight:

1. record the server commit and artifact `git_sha`,
2. verify `systemctl status solana-copy-bot.service --no-pager`,
3. verify disk and memory headroom,
4. verify no production-local cargo build is running,
5. state the exact artifact path, package, binary, and service to touch,
6. verify `execution.enabled` remains disabled in the active config,
7. verify config deltas were accepted, split, or reverted.

Restart after preflight and install:

```bash
sudo systemctl restart solana-copy-bot.service
```

Daemon restart postflight:

1. verify `systemctl is-active solana-copy-bot.service`,
2. record `NRestarts` / restart counter,
3. inspect recent logs for startup publication truth and fatal errors,
4. run bounded Discovery V2/runtime status proof,
5. record whether runtime remains fail-closed or has live V2 green proof.

## 8. Rollback

Discovery scheduled operator rollback:

Run the production preflight before operator rollback, including the exact
rollback artifact id.

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

Run the same daemon restart preflight before rollback, including exact rollback
artifact id and config acceptance state.

```bash
tools/install_operator_artifacts.sh \
  --expect-package copybot-app \
  --expect-target x86_64-unknown-linux-gnu \
  --install-dir /var/www/solana-copy-bot/bin \
  --rollback <previous_artifact_id>
```

Restart only after rollback preflight and successful artifact activation:

```bash
sudo systemctl restart solana-copy-bot.service
```

Then verify:

```bash
systemctl is-active solana-copy-bot.service
systemctl status --no-pager solana-copy-bot.service
journalctl -u solana-copy-bot.service -n 200 --no-pager
```

## 9. Emergency Fallback

Production-local build is emergency fallback only.

Before any emergency build command:

1. record current server commit and dirty state,
2. state the exact package, binary, and service scope,
3. check disk,
4. check memory,
5. verify no other `cargo` or `rustc` build is running,
6. check service pressure,
7. verify `execution.enabled = false` when the daemon is in scope,
8. explain why artifact deploy cannot be used.

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
