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
    "cargo test -p copybot-discovery-v2 --all-targets -- --test-threads=1",
    "tools/architecture_guard.sh --changed"
  ],
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

The workflow builds operator packages only. It does not build `copybot-app`.

Runtime daemon:

```bash
tools/architecture_guard.sh --changed
cargo test -p copybot-app --bin copybot-app
cargo build --release -p copybot-app --bin copybot-app
```

Do not build `copybot-app` for read-only Discovery V2 proof.

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

## 6. Production Install

Install operators:

```bash
tools/install_operator_artifacts.sh \
  --expect-package copybot-discovery-v2 \
  --install-dir /var/www/solana-copy-bot/bin \
  /tmp/discovery-v2-operators-<git_sha>-linux-x86_64.tar.gz
```

The installer:

1. extracts tarballs when needed,
2. verifies `build-manifest.json`,
3. verifies `SHA256SUMS`,
4. rejects dirty artifacts unless `--allow-dirty` is explicitly passed,
5. installs into `bin/releases/<artifact_id>/`,
6. atomically updates symlinks for binaries in the artifact,
7. writes `bin/operator-artifact-current-<package>.json`.

Read-only operators do not require daemon restart.

Install daemon:

```bash
sha="<git_sha>"
install_dir=/var/www/solana-copy-bot/bin

sudo install -m 0755 copybot-app "$install_dir/copybot-app-$sha"
sudo ln -sfn "$install_dir/copybot-app-$sha" "$install_dir/copybot-app-current"
sudo systemctl restart solana-copy-bot.service
```

Systemd should point to:

```text
/var/www/solana-copy-bot/bin/copybot-app-current
```

## 7. Rollback

Operator rollback:

```bash
tools/install_operator_artifacts.sh \
  --expect-package copybot-discovery-v2 \
  --install-dir /var/www/solana-copy-bot/bin \
  --rollback <previous_artifact_id>
```

Then rerun read-only status. No daemon restart is required for operator
rollback.

Daemon rollback:

```bash
previous="<previous_sha>"
install_dir=/var/www/solana-copy-bot/bin

sudo ln -sfn "$install_dir/copybot-app-$previous" "$install_dir/copybot-app-current"
sudo systemctl restart solana-copy-bot.service
```

Then verify:

```bash
systemctl is-active solana-copy-bot.service
systemctl status --no-pager solana-copy-bot.service
```

## 8. Emergency Fallback

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
CARGO_BUILD_JOBS=1 cargo build --release -p copybot-app --bin copybot-app
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

## 9. Rejection Rules

Reject rollout if:

1. it builds on production without emergency reason,
2. it builds `copybot-app` for operator-only changes,
3. it lacks checksums,
4. it lacks manifest,
5. it cannot identify exact binary targets,
6. it restarts daemon for read-only operator install,
7. it skips architecture guard for build/refactor changes.
