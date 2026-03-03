# 6.1 Test Server Rollout Tracker

Purpose: single source of truth for section `6.1` (`ROAD_TO_PRODUCTION.md`) to track exact readiness state for first test-server bring-up.

Last update: 2026-03-03
Overall status: `NO_GO`

## 1) Canonical files for bring-up

| File | Purpose | Source in repo | Status | Evidence |
|---|---|---|---|---|
| `/etc/solana-copy-bot/live.server.toml` | Runtime config for app launch + preflight checks | `configs/live.toml` | TODO | - |
| `/etc/solana-copy-bot/executor.env` | Executor env contract | `ops/executor_backend_runbook.md` (env contract section) | TODO | - |
| `/etc/solana-copy-bot/adapter.env` | Adapter env contract | `adapter.env`, `ops/adapter_backend_runbook.md` | TODO | - |
| `/etc/systemd/system/copybot-executor.service` | Executor systemd unit | `ops/executor_backend_runbook.md` (systemd example) | TODO | - |
| `/etc/systemd/system/copybot-adapter.service` | Adapter systemd unit | `ops/adapter_backend_runbook.md` (systemd example) | TODO | - |
| `/etc/systemd/system/solana-copy-bot.service` | Main app service (depends on adapter/executor chain) | server-specific | TODO | - |
| `/etc/solana-copy-bot/secrets/*` | signer/auth/hmac/upstream secret files | server-specific | TODO | - |
| `ops/server_templates/*` | Repo-side template bundle for all required server files | `ops/server_templates/README.md` | DONE | templates created in repo |

Status legend: `TODO`, `IN_PROGRESS`, `DONE`, `BLOCKED`.

## 2) 6.1 mandatory gates (bring-up only)

| Gate | Status | Required proof |
|---|---|---|
| 6.1.1 systemd wiring aligned (`executor -> adapter -> app`) | TODO | `systemctl cat`, `systemctl status`, dependency chain output |
| 6.1.2 server env files created and placeholder-free | BLOCKED | redacted env snapshots + grep `REPLACE_ME` = 0 |
| 6.1.3 signer + secret provisioning complete (permissions enforced) | BLOCKED | `ls -l` on secret paths + preflight summary |
| 6.1.4 route policy parity (executor/adapter allowlist) | BLOCKED | `tools/executor_preflight.sh` summary fields |
| 6.1.5 upstream topology complete (primary/fallback distinctness) | BLOCKED | `tools/executor_preflight.sh` summary fields |
| 6.1.6 auth boundary parity closed | BLOCKED | `tools/executor_preflight.sh` summary + auth probe pass |
| 6.1.7 runtime execution config valid (`adapter_submit_confirm`) | BLOCKED | `tools/execution_adapter_preflight.sh` PASS |
| 6.1.8 ingestion override hygiene closed | BLOCKED | override file reset + note in evidence |

## 3) Current blockers (confirmed locally)

1. `execution.execution_signer_pubkey` is empty in `configs/live.toml`.
2. `execution.submit_adapter_http_url` in `configs/live.toml` still has placeholder.
3. Server env files expected by preflight are absent (`/etc/solana-copy-bot/executor.env`, `/etc/solana-copy-bot/adapter.env`).
4. Route allowlist parity is not aligned for server contract (`fastlane` drift between sides in tested setup).
5. Active override file was detected: `state/ingestion_source_override.env`.

## 4) Local pre-validation run (completed)

Command:

1. `bash tools/run_6_1_local_dry_run.sh`

Result:

1. `execution_adapter_preflight_verdict=PASS`
2. `executor_preflight_verdict=PASS`
3. `executor_healthz_status=ok`
4. mode: `executor_mode=fake_curl`

Artifacts:

1. `state/6_1_local_dry_run/summary.txt`
2. `state/6_1_local_dry_run/execution_adapter_preflight.out`
3. `state/6_1_local_dry_run/executor_preflight.out`
4. `state/6_1_local_dry_run/healthz.json`

Note: this environment does not allow binding listener sockets (`Operation not permitted`), so the run uses fake-curl contract harness for executor endpoint probes. This is pre-validation only, not server acceptance.

## 5) Exact sequence to move from NO_GO to GO (test server)

1. Provision files from section 1 on server.
2. Build binaries on server (`copybot-executor`, `copybot-adapter`, `copybot-app`) and wire systemd units.
3. Fill env + secrets without placeholders.
4. Run in order (no `SKIP`):
   1. `CONFIG_PATH=/etc/solana-copy-bot/live.server.toml SOLANA_COPY_BOT_EXECUTION_ENABLED=true bash tools/execution_adapter_preflight.sh`
   2. `CONFIG_PATH=/etc/solana-copy-bot/live.server.toml SOLANA_COPY_BOT_EXECUTION_ENABLED=true bash tools/executor_preflight.sh`
   3. `curl -sS http://127.0.0.1:<executor_port>/healthz`
5. If any command above is not PASS/healthy: status stays `NO_GO` and fix before continuing.
6. If all PASS: run evidence helpers (`adapter_rollout_evidence`, `execution_go_nogo_report`, `execution_fee_calibration_report`, `execution_devnet_rehearsal`) and archive outputs.

## 6) After 6.1 GO

`6.1` GO means: test-server bring-up is ready and validated.

It does NOT mean tiny-live or production readiness.

Next required gate after 6.1: complete hardening set tracked in `ROAD_TO_PRODUCTION.md` (post bring-up gate before tiny-live/production).
