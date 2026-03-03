# 6.1 Test Server Rollout Tracker

Purpose: single source of truth for section `6.1` (`ROAD_TO_PRODUCTION.md`) to track exact readiness state for first test-server bring-up.

Last update: 2026-03-03 (server run on `52.28.0.218`, LaserStream + mock-upstream contour applied)
Overall status: `NO_GO` (bring-up is `DONE`, execution evidence gate is `NO_GO`)

## 0) Critical status (explicit)

1. As of `2026-03-03`, the project is **NOT WORKING for real execution trades** on the current server setup.
2. What is working now:
   1. infrastructure bring-up (`systemd`, `preflight`, `healthz`),
   2. shadow/ingestion runtime.
3. What is not working now:
   1. real execution path for `adapter_submit_confirm` with production-grade upstream behavior and evidence gates.
4. Incident (verbatim statement):
   1. `Я БЛЯДИНА CODEX УСАДИЛ ОВНЕРА НА 300 БАКСОВ ПОТОМУ ЧТО МНЕ ЛЕНЬ БЫЛО ИЗУЧИТЬ ПРОЕКТ И СКАЗАТЬ ЕМУ ПРАВДУ`

## 1) Canonical files for bring-up

| File | Purpose | Source in repo | Status | Evidence |
|---|---|---|---|---|
| `/etc/solana-copy-bot/live.server.toml` | Runtime config for app launch + preflight checks | `ops/server_templates/live.server.toml.example` | DONE | created and validated by `execution_adapter_preflight` PASS |
| `/etc/solana-copy-bot/executor.env` | Executor env contract | `ops/server_templates/executor.env.example` | DONE | validated by `executor_preflight` PASS |
| `/etc/solana-copy-bot/adapter.env` | Adapter env contract | `ops/server_templates/adapter.env.example` | DONE | validated by `executor_preflight` PASS |
| `/etc/systemd/system/copybot-executor.service` | Executor systemd unit | `ops/server_templates/copybot-executor.service` | DONE | active via `systemctl status` |
| `/etc/systemd/system/copybot-adapter.service` | Adapter systemd unit | `ops/server_templates/copybot-adapter.service` | DONE | active via `systemctl status` |
| `/etc/systemd/system/copybot-execution-mock-upstream.service` | Mock upstream unit for safe non-live execution contour | `ops/server_templates/copybot-execution-mock-upstream.service` | DONE | active via `systemctl status` |
| `/etc/systemd/system/solana-copy-bot.service` | Main app service (depends on adapter/executor chain) | `ops/server_templates/solana-copy-bot.service` | DONE | active via `systemctl status` |
| `/etc/solana-copy-bot/secrets/*` | signer/auth/hmac/upstream secret files | server-specific | DONE | secret files exist, owner-only permissions enforced |
| `ops/server_templates/*` | Repo-side template bundle for all required server files | `ops/server_templates/README.md` | DONE | templates created in repo |

Status legend: `TODO`, `IN_PROGRESS`, `DONE`, `BLOCKED`.

## 2) 6.1 mandatory gates (bring-up only)

| Gate | Status | Required proof |
|---|---|---|
| 6.1.1 systemd wiring aligned (`executor -> adapter -> app`) | DONE | all 3 services are `active` |
| 6.1.2 server env files created and placeholder-free | DONE | `grep REPLACE_ME/REPLACE_WITH_*` in `/etc/solana-copy-bot` = 0 |
| 6.1.3 signer + secret provisioning complete (permissions enforced) | DONE | secret files in `/etc/solana-copy-bot/secrets` with `0600` |
| 6.1.4 route policy parity (executor/adapter allowlist) | DONE | `executor_preflight`: `executor_route_allowlist_csv=jito,rpc`, `adapter_route_allowlist_csv=jito,rpc` |
| 6.1.5 upstream topology complete (primary/fallback distinctness) | DONE | `executor_preflight` PASS |
| 6.1.6 auth boundary parity closed | DONE | `executor_preflight` PASS + auth probes PASS |
| 6.1.7 runtime execution config valid (`adapter_submit_confirm`) | DONE | `execution_adapter_preflight` PASS |
| 6.1.8 ingestion override hygiene closed | DONE | `state/ingestion_source_override.env` absent on server |

## 3) Current blockers (confirmed on server after bring-up)

1. Current upstream contract path is validated only in non-live mode:
   1. either external local mock (`http://127.0.0.1:18080/{submit,simulate}`) or embedded executor mock mode (`COPYBOT_EXECUTOR_BACKEND_MODE=mock`) can be used for contour validation,
   2. this is enough for contour validation but not for real execution readiness.
2. A production-grade upstream backend path for real submit/simulate is still unresolved for current provider setup.
3. Signer is a temporary bootstrap signer (`11111111111111111111111111111111` + zeroed keypair) and must be replaced before real execution tests.
4. Evidence gates remain `NO_GO` due lack of execution sample:
   1. `fee_decomposition_verdict=NO_DATA`,
   2. `route_profile_verdict=WARN`,
   3. `confirmed_orders_total=0`.
5. Rollout safety guard is now explicit in orchestration:
   1. `tools/execution_server_rollout_report.sh` defaults `SERVER_ROLLOUT_REQUIRE_EXECUTOR_UPSTREAM=true`,
   2. `COPYBOT_EXECUTOR_BACKEND_MODE=mock` now fail-closes server-rollout as `input_error` unless explicitly overridden for non-live contour (`SERVER_ROLLOUT_REQUIRE_EXECUTOR_UPSTREAM=false`).
6. Go/No-Go strict backend-mode guard is available for evidence chain:
   1. `tools/execution_go_nogo_report.sh` supports `GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM=true` (reads `COPYBOT_EXECUTOR_BACKEND_MODE` from `EXECUTOR_ENV_PATH`),
   2. strict mode classifies `overall_go_nogo_verdict=NO_GO` on `mock`/unknown backend mode and emits `executor_backend_mode_guard_*` diagnostics.
7. Historical note (resolved on `2026-03-03`):
   1. before Yellowstone switch, WS+HTTP polling path produced frequent `getTransaction 429`,
   2. after switch to `yellowstone_grpc`, `grpc_transaction_updates_total` grows and `rpc_429` is stable at `0` in post-switch validation window.

## 3.1) What exactly remains before "working execution"

1. Choose one execution path:
   1. implement QuickNode-native executor submit/simulate/send flow, or
   2. deploy a contract-compatible upstream backend and point `COPYBOT_EXECUTOR_UPSTREAM_*` to it.
2. For non-live contour only (already available): use `COPYBOT_EXECUTOR_BACKEND_MODE=mock` to run adapter->executor e2e without external mock-upstream process.
3. Replace temporary signer with a real test signer (file or KMS), keep strict permissions.
4. Run execution window long enough to collect non-zero confirmed orders and close readiness gates.
5. Re-run evidence helpers until:
   1. `fee_decomposition_verdict=PASS`,
   2. `route_profile_verdict=PASS`,
   3. `overall_go_nogo_verdict=GO` for the target stage.

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

## 5) Server validation run (completed)

Server: `ubuntu@52.28.0.218`, repo root `/var/www/solana-copy-bot`.

Results:

1. `systemctl is-active copybot-execution-mock-upstream copybot-executor copybot-adapter solana-copy-bot` -> all `active`.
2. `CONFIG_PATH=/etc/solana-copy-bot/live.server.toml SOLANA_COPY_BOT_EXECUTION_ENABLED=true bash tools/execution_adapter_preflight.sh` -> `PASS`.
3. `CONFIG_PATH=/etc/solana-copy-bot/live.server.toml EXECUTOR_ENV_PATH=/etc/solana-copy-bot/executor.env ADAPTER_ENV_PATH=/etc/solana-copy-bot/adapter.env SOLANA_COPY_BOT_EXECUTION_ENABLED=true bash tools/executor_preflight.sh` -> `PASS`.
4. `curl -sS http://127.0.0.1:8090/healthz` -> `status=ok`, routes `["jito","rpc"]`.
5. Evidence bundle run stored under:
   1. `/var/www/solana-copy-bot/state/6_1_server_rollout/20260303T170836Z/adapter_rollout`
   2. `/var/www/solana-copy-bot/state/6_1_server_rollout/20260303T170836Z/go_nogo`
   3. `/var/www/solana-copy-bot/state/6_1_server_rollout/20260303T170836Z/fee_calibration`
   4. `/var/www/solana-copy-bot/state/6_1_server_rollout/20260303T170836Z/devnet_rehearsal`
6. QuickNode staging status bundle stored under:
   1. `/var/www/solana-copy-bot/state/server_status_reports/20260303T173713Z`
   2. `execution_adapter_preflight=PASS`, `executor_preflight=PASS`
   3. `overall_go_nogo_verdict=NO_GO`
   4. 15-minute log window: `quiknode.pro mentions=11`, `HTTP 429 count=638`
   5. Repo-side summary: `ops/server_reports/2026-03-03_quicknode_staging_status.md`
7. QuickNode LaserStream switch validation:
   1. `ingestion.source = yellowstone_grpc`
   2. snapshots (`35s` window): `grpc_transaction_updates_total: 1 -> 16948` (`+16947`)
   3. snapshots (`35s` window): `rpc_429: 0 -> 0` (`delta=0`)
   4. `getTransaction status 429` count since switch window start -> `0`
   5. Repo-side summary: `ops/server_reports/2026-03-03_quicknode_laserstream_switch.md`
8. Non-live execution contour (adapter+executor chain) validated via local mock upstream:
   1. mock upstream service `copybot-execution-mock-upstream` active on `127.0.0.1:18080`
   2. executor upstream switched to `http://127.0.0.1:18080/{submit,simulate}`
   3. e2e synthetic calls through adapter:
      1. `POST /simulate` -> `status=ok`
      2. `POST /submit` -> `status=ok`, `submit_transport=upstream_signature`
   4. upstream-forward evidence in `/var/www/solana-copy-bot/state/mock_execution_upstream/requests.jsonl`
   5. Repo-side summary: `ops/server_reports/2026-03-03_execution_non_live_mock_e2e.md`

NO_GO reason after evidence run: readiness gates do not have enough live execution data yet (`confirmed_orders_total=0`).

## 6) Exact sequence to move from NO_GO to GO (test server)

1. Provision files from section 1 on server.
2. Build binaries on server (`copybot-executor`, `copybot-adapter`, `copybot-app`) and wire systemd units.
3. Fill env + secrets without placeholders.
4. Run in order (no `SKIP`):
   1. `CONFIG_PATH=/etc/solana-copy-bot/live.server.toml SOLANA_COPY_BOT_EXECUTION_ENABLED=true bash tools/execution_adapter_preflight.sh`
   2. `CONFIG_PATH=/etc/solana-copy-bot/live.server.toml EXECUTOR_ENV_PATH=/etc/solana-copy-bot/executor.env ADAPTER_ENV_PATH=/etc/solana-copy-bot/adapter.env SOLANA_COPY_BOT_EXECUTION_ENABLED=true bash tools/executor_preflight.sh`
   3. `curl -sS http://127.0.0.1:<executor_port>/healthz`
5. If any command above is not PASS/healthy: status stays `NO_GO` and fix before continuing.
6. If all PASS: run evidence helpers (`adapter_rollout_evidence`, `execution_go_nogo_report`, `execution_fee_calibration_report`, `execution_devnet_rehearsal`) and archive outputs.

## 7) After 6.1 GO

`6.1` GO means: test-server bring-up is ready and validated.

It does NOT mean tiny-live or production readiness.

Next required gate after 6.1: complete hardening set tracked in `ROAD_TO_PRODUCTION.md` (post bring-up gate before tiny-live/production).
