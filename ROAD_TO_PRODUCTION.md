# ROAD TO PRODUCTION

Date: 2026-02-19  
Owner: copybot runtime team

## 1) Цель документа

Этот план описывает порядок доведения проекта до реального трейдинга с минимизацией операционных и финансовых рисков.

Связанный документ по ingestion-масштабированию:

1. `YELLOWSTONE_GRPC_MIGRATION_PLAN.md`  
   Этот документ не дублирует migration-plan, а опирается на него на этапах `Observation` и `Go/No-Go`.

## 2) Где мы сейчас (факт)

1. Runtime покрывает ingestion/discovery/shadow/risk gating.
2. Execution core в `paper`-режиме уже реализован: status-machine, idempotency, simulation -> submit -> confirm -> reconcile, recovery для `execution_submitted`/`execution_simulated`.
3. BUY-only pause gates активны: operator emergency stop, risk hard-stop и outage-блокировка применяются только к pre-submit BUY.
4. SELL и confirm-path не блокируются pause-гейтами, что сохраняет возможность закрытия риска.
5. Execution risk gates в рантайме enforce: `max_position_sol`, `max_total_exposure_sol`, `max_exposure_per_token_sol`, `max_concurrent_positions`, staleness и `sell_requires_open_position`.
6. Submit route policy в runtime уже enforce: route allowlist, explicit ordered fallback list (`submit_route_order`), per-route slippage/tip/CU caps, adapter-response correlation guards, contract-version pin (`submit_adapter_contract_version`) and optional strict response policy echo, плюс attempt-based route fallback.
7. Adapter auth hardening baseline готов: optional Bearer + optional HMAC request signing (`key_id/secret/ttl`) с fail-closed валидацией на старте; HMAC считается по точным bytes исходящего JSON-body; token/secret могут подниматься из file-based secret paths.
8. Оставшиеся code-gaps до real-money submit: production adapter integration (реальный signed-tx backend + ops rollout по уже готовому runtime контракту).
9. Для закрытия upstream execution backend добавлен отдельный мастер-план: `ops/executor_backend_master_plan_2026-02-24.md`.

Текущий статус этапов:

| Направление | Статус | Owner | Due |
| --- | --- | --- | --- |
| Yellowstone primary runtime | Done | runtime-ops | 2026-02-19 |
| Watchdog script/policy in repo | Done | runtime-ops | 2026-02-18 |
| Watchdog systemd deploy on server | Done | runtime-ops | 2026-02-24 |
| Post-cutover 1h/6h/24h evidence | Done | runtime-ops | 2026-02-24 |
| 7-day observation closure | Done | runtime-ops | 2026-02-24 |
| Execution runtime (paper lifecycle) | Done | execution-dev | 2026-02-19 |
| Execution runtime (live submit path) | In progress | execution-dev | 2026-03-09 |
| Execution safety hardening (audit batch #1) | Done | execution-dev | 2026-02-19 |
| Emergency stop (no-restart) | Done | execution-dev | 2026-02-19 |
| `pause_new_trades_on_outage` wiring/removal | Done | execution-dev | 2026-02-19 |

### 2.1 Сквозной phase tracker (A→Live)

| Фаза | Цель | Статус на 2026-02-24 | Главный блокер выхода |
| --- | --- | --- | --- |
| A | Закрыть Yellowstone migration observation | Done | закрыт; evidence архивирован (`ops/yellowstone_observation_closure_2026-02-24.md`) |
| B | Закрыть security/ops baseline до первого submit | In progress | key policy + alert delivery + rollback drill |
| C | Поднять execution core MVP | In progress | закрыть live submit-path + real tx policy (CU-limit/CU-price + route slippage bounds) |
| C.5 | Пройти devnet dress rehearsal | In progress | собрать Stage C.5 evidence через `tools/execution_devnet_rehearsal.sh` + устранить P0/P1 по отчету |
| D | Подключить Jito как primary route | Pending | route policy + tip strategy + fallback policy |
| E | Заэнфорсить live risk limits в execution | In progress | добрать fee reserve/cooldown policy + live-runtime проверку |
| F | Пройти staged rollout (dry/tiny/limited) | Pending | KPI-gates по success/timeout/duplicates |
| G | Стабилизировать controlled live (first 7-14 days) | Pending | нулевые P0 и подтвержденная reconcile-дисциплина |
| H | Перейти в standard live / steady-state ops | Pending | signed go-live + runbook completeness + ownership handoff |

Фактический прогресс на 2026-02-24:

1. Закрыты safety-gates `R2P-06` и `R2P-16` (runtime BUY-gate).
2. Execution baseline поднят: `R2P-08` и `R2P-09` закрыты; `R2P-10`/`R2P-11` в прогрессе (paper lifecycle + recovery + risk gates готовы).
3. До real-money submit остаются code-only блокеры: live signed-tx backend за адаптерным контрактом + калибровка route-профилей под реальные market regimes.
4. Stage A закрыт: watchdog server deploy + post-cutover evidence + observation closure архивированы (`ops/yellowstone_observation_closure_2026-02-24.md`).

## 3) Критичная правда по сроку "завтра торговать"

1. Полностью "законченный проект" к завтрашнему дню нереалистичен без резкого роста риска.
2. Реалистичный вариант: ограниченный `controlled live` с очень малыми лимитами, только после закрытия обязательных safety-гейтов ниже.
3. Если safety-гейты не закрыты, live запуск откладывается.

## 4) Definition of Done (финальная точка)

Проект считается production-ready только когда одновременно выполнены все условия:

1. `YELLOWSTONE_GRPC_MIGRATION_PLAN.md` закрыт по разделу Success Criteria + завершено observation окно.
2. Watchdog на сервере развернут как `systemd service + timer`, протестирован forced-failover сценарием.
3. Execution RPC endpoint(s) и policy failover зафиксированы и проверены.
4. Реализован execution pipeline с:
   1. идемпотентностью (`client_order_id`),
   2. pre-trade balance checks,
   3. simulation перед отправкой,
   4. submit + confirmation polling,
   5. on-chain reconciliation.
5. Emergency stop реализован и срабатывает без перезапуска.
6. Jito path (`Lil' JIT`) используется как primary submit route, RPC fallback задокументирован и проверен.
7. Live risk limits зафиксированы и реально enforced в execution-контуре (включая per-token cap).
8. Пройден staged rollout: dry-run -> tiny live -> limited live -> standard live.
9. После включения submit пройден минимум 7 дней controlled live без нерешенных P0 инцидентов.

## 5) Порядок внедрения (строго по очереди)

## Stage A — Yellowstone Observation Closure
Связь с `YELLOWSTONE_GRPC_MIGRATION_PLAN.md`:

1. `Phase E.5` (watchdog) должен перейти из partial в done.
2. `Phase G` закрывается после 1h/6h/24h отчетов и завершения 7-дневного окна.
3. Replay gate зафиксирован как waiver (с reason и approval).

Работы:

1. Развернуть server `systemd` units по `ops/ingestion_failover_watchdog.md`.
2. Провести forced-failover drill:
   1. эмулировать trigger,
   2. проверить запись override-файла,
   3. проверить перезапуск сервиса,
   4. проверить переключение source.
3. Проверить инвариант override-пути:
   1. runtime effective path = `SOLANA_COPY_BOT_INGESTION_OVERRIDE_FILE` (или default `state/ingestion_source_override.env`),
   2. watchdog `OVERRIDE_FILE` должен указывать в тот же путь.
4. Собрать и сохранить артефакты:
   1. 1h/6h/24h post-cutover reports,
   2. статус watchdog timer/service,
   3. сводка observation окна.

Exit criteria Stage A:

1. Все blocking items из `YELLOWSTONE_GRPC_MIGRATION_PLAN.md` закрыты.
2. Документ переведен из `Observation mode` в `Migration completed`.

## Stage B — Security and Ops Baseline (до первой real tx)

Работы:

1. Key management:
   1. отдельный hot-wallet под бота,
   2. хранение ключа только в server secrets/env (не в repo),
   3. лимитированный баланс на кошельке,
   4. политика ротации (manual runbook + частота + кто делает).
2. Wallet funding strategy:
   1. минимальный SOL reserve для fees/tips (неиспользуемый торговый буфер),
   2. политика пополнения и лимит максимального баланса на hot-wallet.
3. Pre-trade balance policy:
   1. hard stop при недостатке SOL для fee/tip,
   2. отдельный порог reserve SOL (не тратить "в ноль"),
   3. алерт при достижении warning-порога.
4. Alert delivery wiring:
   1. канал доставки (Telegram bot / webhook),
   2. список алертов P0/P1,
   3. проверка доставки тестовым событием.
5. Emergency stop:
   1. немедленно прекращает submit новых транзакций,
   2. не пытается "отменить" уже отправленные транзакции,
   3. переключается file-flag или env без полного redeploy.
6. Operational alerts:
   1. ingestion lag/replaced ratio/reconnect/decode errors,
   2. sqlite contention growth,
   3. execution failure rate,
   4. wallet balance low.

Exit criteria Stage B:

1. Ключевой материал не хранится локально в репо/файлах окружения разработчика.
2. Есть рабочие алерты и проверка их доставки.
3. Emergency stop проверен на стенде/сухом прогоне.
4. Есть проверенный runbook ручного rollback.

## Stage C — Execution Core MVP (обязательный минимум)

Цель: начать исполнять реальные ордера безопасным, отслеживаемым и идемпотентным способом.

Статус на 2026-02-19:

1. ✅ Уже сделано:
   1. `crates/execution` с модульной архитектурой (`intent/idempotency/simulator/submitter/confirm/reconcile`),
   2. status-machine flow с recovery (`execution_submitted`/`execution_simulated` ре-обрабатываются при каждом тике),
   3. BUY-only pause model (operator/hard-stop/outage) без блокировки SELL/confirm,
   4. risk gates в execution path (`max_position_sol`, `max_total_exposure_sol`, `max_exposure_per_token_sol`, `max_concurrent_positions`, staleness, sell-open-position validation).
2. 🟡 В работе:
   1. live submit/confirm implementations (paper path + adapter submit mode реализованы; production adapter backend pending),
   2. production adapter contract handoff (real tx builder/signer + rollback-safe rollout).
3. ✅ Уже добавлено после audit hardening:
   1. bounded submit retry policy (`max_submit_attempts`) в execution runtime,
   2. typed submit error taxonomy (`Retryable`/`Terminal`) вместо message-based heuristic,
   3. pre-trade checker contract в execution pipeline (retryable/terminal outcomes + lifecycle wiring),
   4. RPC pre-trade второго уровня: ATA account-existence policy (`getTokenAccountsByOwner`) + priority fee cap check (`getRecentPrioritizationFees`) через `pretrade_require_token_account` и `pretrade_max_priority_fee_lamports` (unit: micro-lamports/CU).

Prerequisites Stage C:

1. Execution RPC provisioning:
   1. primary RPC endpoint (`execution.rpc_http_url`) для blockhash/simulation/account reads/confirm,
   2. fallback RPC endpoint (`execution.rpc_fallback_http_url`) для деградационных сценариев,
   3. отдельный devnet RPC endpoint (`execution.rpc_devnet_http_url`) для Stage C.5 rehearsal.
2. Явный runtime toggle:
   1. `execution.enabled=false` по умолчанию,
   2. включение только после прохождения Stage B exit criteria.
3. Статусный flow исполнения:
   1. `shadow_recorded -> execution_pending -> execution_simulated -> execution_submitted -> execution_confirmed|execution_failed|execution_dropped`.

Кодовые изменения:

1. Добавить `crates/execution`:
   1. `intent.rs` — модель execution intent,
   2. `idempotency.rs` — генерация/проверка `client_order_id`,
   3. `simulator.rs` — pre-trade simulation,
   4. `submitter.rs` — интерфейс отправки,
   5. `confirm.rs` — confirmation polling,
   6. `reconcile.rs` — on-chain reconciliation в `orders`/`fills`.
2. Добавить интеграцию в `crates/app/src/main.rs`:
   1. источник intents: `copy_signals` со статусом `execution_pending`,
   2. worker loop execution,
   3. обновление статусов ордеров по flow выше.
3. Добавить SQL schema update для execution-аудита:
   1. `orders.client_order_id` (unique),
   2. `orders.tx_signature`,
   3. `orders.simulation_status/simulation_error`,
   4. retry/attempt fields,
   5. `copy_signals.status` переходы для execution-state.

Обязательные проверки на каждый intent:

1. Проверка дубликата по `client_order_id`.
2. Проверка баланса (SOL + токен по стороне сделки).
3. Проверка/создание ATA (Associated Token Account) для токена сделки.
4. Simulation.
5. Submit.
6. Confirmation polling в пределах timeout budget.
7. Reconcile в БД.

Solana-specific требования Stage C:

1. Recent blockhash lifecycle:
   1. blockhash refresh policy (TTL-safe),
   2. retry с новым blockhash при expiry.
2. Compute budget:
   1. явная настройка CU limit/CU price,
   2. отказ от submit при simulation compute failure.
3. Slippage:
   1. обязательный параметр `slippage_bps` в execution config,
   2. per-route upper bound.
4. Transaction format:
   1. поддержка `legacy` и `v0` (versioned),
   2. фиксированная policy выбора формата.
5. Latency budget:
   1. end-to-end budget (signal->submit) должен укладываться в `max_signal_lag_seconds`,
   2. целевой execution budget: 5-10 секунд.

Exit criteria Stage C:

1. Нулевые двойные отправки в тестовом прогоне.
2. Любой ордер имеет прозрачный lifecycle в `orders`.
3. Ошибки симуляции не приводят к отправке транзакций.
4. All Stage C flows проходят на devnet без критичных ошибок.

## Stage C.5 — Devnet Dress Rehearsal (между C и D)

Цель: проверить полный execution pipeline до mainnet submit.

Работы:

1. Прогон всех execution-state переходов на devnet.
2. Проверка ATA create path и повторных ордеров в уже существующий ATA.
3. Проверка blockhash refresh/retry сценариев.
4. Проверка emergency stop и recovery.

Exit criteria Stage C.5:

1. Devnet smoke report сохранен.
2. Критичные defects Stage C устранены до перехода в Stage D.

## Stage D — Jito Primary Route

Цель: перейти на latency-aware и MEV-aware submit path.

Работы:

1. Подключить QuickNode `Lil' JIT` API как primary route.
   1. V1 решение: использовать QuickNode `Lil' JIT` adapter (не direct Jito API).
2. Реализовать policy:
   1. `bundleOnly=true` для чувствительных сделок,
   2. RPC fallback для допустимых классов ошибок,
   3. accepted risk: при RPC fallback MEV protection ниже.
3. Подключить `Priority Fee API` для динамического CU price.
4. Реализовать tip strategy:
   1. базовый режим (min tip floor),
   2. динамический режим (в привязке к priority fee / congestion),
   3. верхний лимит tip на ордер.
5. Учитывать bundle slot deadline + retry policy.
6. `Fastlane` оставить под feature-flag, не включать в default route.

Exit criteria Stage D:

1. Primary отправка идет через Jito.
2. Fallback логируется и ограничен policy.
3. На дашборде видно распределение маршрутов и latency/failure по route.
4. Tip spend не выходит за установленный бюджет.

## Stage E — Live Risk Enforcement

Цель: убрать разрыв между paper limits и реальным исполнением.

Работы:

1. В execution-контуре enforce:
   1. `max_position_sol`,
   2. `max_total_exposure_sol`,
   3. `max_concurrent_positions`,
   4. `max_exposure_per_token_sol`,
   5. daily loss guard,
   6. drawdown guard.
2. Ввести `live.toml` с отдельными лимитами.
3. Добавить breakeven-анализ размера позиции:
   1. учитывать base fee + priority fee + Jito tip + ATA cost (если требуется создание),
   2. минимальный размер позиции должен выдерживать fee/tip overhead.
4. Начальные лимиты для tiny-live:
   1. `max_position_sol = 0.10`,
   2. `max_total_exposure_sol = 0.30`,
   3. `max_exposure_per_token_sol = 0.10`,
   4. `max_concurrent_positions = 3`,
   5. hard stop при fee reserve < `0.05 SOL`.

Exit criteria Stage E:

1. Лимиты реально блокируют ордера при breach.
2. Любая блокировка фиксируется в `risk_events` с причиной.
3. Breakeven policy зафиксирована и применена в sizing.

## Stage F — Staged Rollout to Real Trading

Последовательность:

1. Dry-run (live market, no submit) — 12-24h.
2. Tiny-live — 24h:
   1. `max_position_sol = 0.10`,
   2. `max_total_exposure_sol = 0.30`,
   3. `max_exposure_per_token_sol = 0.10`,
   4. `max_concurrent_positions = 3`.
3. Limited-live — 48h:
   1. `max_position_sol = 0.25`,
   2. `max_total_exposure_sol = 1.00`,
   3. `max_exposure_per_token_sol = 0.25`,
   4. `max_concurrent_positions = 5`.
4. Standard-live (initial) — после green KPI:
   1. `max_position_sol = 0.50`,
   2. `max_total_exposure_sol = 3.00`,
   3. `max_exposure_per_token_sol = 0.75`,
   4. `max_concurrent_positions = 8`.
   Note: `max_exposure_per_token_sol / max_total_exposure_sol = 25%` зафиксировано осознанно как более строгий concentration cap, чем в tiny-live.

KPI-гейты перехода между стадиями:

1. execution success rate >= 95%.
2. confirm timeout ratio <= 2%.
3. duplicate submit = 0.
4. watchdog health = green.
5. отсутствие критичных data-integrity инцидентов.

## Stage G — Controlled Live Stabilization (first 7-14 days)

Цель: закрепить устойчивость после первого live submit и не перепрыгнуть в standard-live до подтверждения операционной зрелости.

Работы:

1. Ежедневный reconcile-контур:
   1. `orders`/`fills`/on-chain совпадают,
   2. все `execution_failed`/`execution_dropped` классифицированы и имеют owner.
2. Incident discipline:
   1. P0/P1 инциденты с timestamps, root cause, corrective action,
   2. postmortem обязателен для любого P0.
3. SLO monitoring:
   1. execution success rate, confirm timeout ratio, duplicate submit, route split (Jito/RPC),
   2. failover watch (ingestion + execution RPC health).
4. Daily go/no-go review:
   1. решение о повышении лимитов принимается только после KPI green + без открытых P0.

Exit criteria Stage G:

1. Минимум 7 дней controlled live завершены.
2. Ноль открытых P0.
3. Reconcile drift = 0 по обязательным полям ордеров/филлов.
4. Документированы и выполнены корректирующие действия по всем P1, возникшим в окне.

## Stage H — Standard Live Handover

Цель: перевести runtime в регулярный production-режим со стабильными процессами изменений и дежурств.

Работы:

1. Production ownership:
   1. зафиксировать primary on-call и backup on-call,
   2. определить SLA/response targets для P0/P1.
2. Config freeze + release discipline:
   1. `live.toml` и env baseline версионируются,
   2. любое изменение лимитов/route-policy — только через change record.
3. Runbook completeness:
   1. emergency stop,
   2. rollback/restore,
   3. key rotation,
   4. watchdog failover drill.
4. Governance:
   1. подписанный go-live note,
   2. список residual risks с owner и review date.

Exit criteria Stage H:

1. Подписан production handoff.
2. Standard-live лимиты утверждены.
3. Все обязательные runbook-процедуры проверены drill-сценариями.

## 6) Детальный backlog "что за чем внедряем"

`R2P-01` — Watchdog deployment on server  
Status: ✅ Done (2026-02-24)  
Depends on: none  
Artifacts: `systemctl status`, timer logs, failover drill logs (`ops/yellowstone_observation_closure_2026-02-24.md`)

`R2P-02` — Migration evidence pack  
Status: ✅ Done (2026-02-24)  
Depends on: R2P-01  
Artifacts: 1h/6h/24h reports + observation summary + replay waiver note (`ops/yellowstone_observation_closure_2026-02-24.md`)

`R2P-03` — Key management baseline  
Depends on: none  
Artifacts: key policy/runbook, rotation checklist

`R2P-04` — Execution RPC endpoint provisioning  
Depends on: none  
Artifacts: approved endpoint list (mainnet primary/fallback + devnet endpoint), rate/limits policy

`R2P-05` — Alert delivery wiring (Telegram/webhook)  
Depends on: none  
Artifacts: test alert delivery logs, on-call routing

`R2P-06` — Emergency stop mechanism  
Status: ✅ Done (2026-02-19)  
Depends on: none  
Artifacts: emergency-stop runbook + test evidence (file-flag/env path without restart)  
Evidence (code/tests): `crates/app/src/main.rs` (`OperatorEmergencyStop`, risk events `operator_emergency_stop_activated`/`operator_emergency_stop_cleared`, BUY-drop reason `operator_emergency_stop`, tests green in `cargo test --workspace`)
Integration note: standalone dependency is `none`, but this gate is mandatory before enabling `execution.enabled=true`; submit path in `R2P-10`/`R2P-11` must enforce the same stop.

`R2P-07` — Wallet funding + SOL reserve policy  
Depends on: R2P-03  
Artifacts: reserve thresholds, funding playbook

`R2P-08` — Execution crate skeleton  
Status: ✅ Done (2026-02-19)  
Depends on: none  
Files: `crates/execution/*`, workspace `Cargo.toml`
Evidence (code/tests): execution crate wired into app/workspace, `cargo test --workspace` green.

`R2P-09` — DB schema updates for order lifecycle fields + signal status flow  
Status: ✅ Done (2026-02-19)  
Depends on: R2P-08  
Files: `migrations/*`, `crates/storage/src/lib.rs`
Evidence (code/tests): lifecycle fields/indexes + store methods for orders/fills/positions, lifecycle integration tests green.

`R2P-10` — Idempotency + balance checks + simulation + ATA/blockhash/CU/slippage  
Status: 🟡 In progress (paper baseline done)  
Depends on: R2P-04, R2P-08, R2P-09  
Files: `crates/execution/*`, `crates/app/src/main.rs`
Done now:
1. idempotency (`client_order_id`) + order recovery reprocessing,
2. simulation gate + staleness gate + risk gates (incl. per-token cap),
3. BUY-only pause integration (operator/hard-stop/outage),
4. bounded retry policy (`max_submit_attempts`) for submit/pre-trade retryable failures,
5. pre-trade checker contract wired in lifecycle (`Allow` / `RetryableReject` / `TerminalReject`),
6. RPC pre-trade checker added (`paper_rpc_pretrade_confirm`): `getLatestBlockhash` + signer balance check with `pretrade_min_sol_reserve` gate,
7. pre-trade account/fee gates: optional ATA existence policy (`pretrade_require_token_account`) + optional priority fee cap (`pretrade_max_priority_fee_lamports`, unit: micro-lamports/CU).
Remaining:
1. CU-budget/slippage-route policy for real submit.

`R2P-11` — Submit + confirmation polling + reconciliation  
Status: 🟡 In progress (paper path done)  
Depends on: R2P-10  
Files: `crates/execution/*`, `crates/storage/src/lib.rs`
Done now:
1. submit -> confirm -> reconcile flow в paper path,
2. timeout handling + repeated confirm attempts до deadline,
3. recovery of stuck `execution_submitted`/`execution_simulated`,
4. insert-outcome disambiguation for idempotency path: `Inserted` vs `Duplicate` (+ anomaly error on ignored-without-duplicate),
5. RPC confirmer path added (`paper_rpc_confirm` / `paper_rpc_pretrade_confirm`) with fallback endpoint support and explicit `confirm_failed` branch,
6. adapter submit mode added (`adapter_submit_confirm`): HTTP adapter submitter contract + route allowlist policy (`submit_allowed_routes`) + explicit route fallback order policy (`submit_route_order`) + route slippage caps (`submit_route_max_slippage_bps`) + route tip policy (`submit_route_tip_lamports`) + route-level compute budget policy (`submit_route_compute_unit_limit`, `submit_route_compute_unit_price_micro_lamports`) + fail-closed wiring for submitter/confirmer initialization.
7. adapter auth policy hardened: optional HMAC signing headers (`submit_adapter_hmac_key_id`, `submit_adapter_hmac_secret`, `submit_adapter_hmac_ttl_sec`) with startup fail-fast on partial/invalid config; adapter auth token/HMAC secret support file-based sources (`submit_adapter_auth_token_file`, `submit_adapter_hmac_secret_file`) for secret-management rollout.
8. adapter contract hardening: request includes `contract_version`; runtime validates adapter response against expected contract version and (optionally, when `submit_adapter_require_policy_echo=true`) strict echo of route-policy fields (`slippage_bps`, `tip_lamports`, `compute_budget.cu_limit`, `compute_budget.cu_price_micro_lamports`).
Remaining:
1. production adapter backend (real signed tx build/send + operational rollout),
2. route-level policy evolution для Jito-primary/RPC-fallback in real-money path.

`R2P-12` — Devnet dress rehearsal  
Status: 🟡 In progress (rehearsal tooling + runbook ready)  
Depends on: R2P-04, R2P-10, R2P-11  
Files: `tools/execution_devnet_rehearsal.sh`, `ops/execution_devnet_rehearsal_runbook.md`, `tools/ops_scripts_smoke_test.sh`  
Artifacts: devnet smoke report + preflight/go-no-go/test logs bundle

`R2P-13` — Jito primary + RPC fallback + tip strategy  
Depends on: R2P-11, R2P-12  
Files: `crates/execution/*`, config/env docs

`R2P-14` — Live risk enforcement + `configs/live.toml`  
Status: 🟡 In progress (`configs/live.toml` scaffold added; rollout evidence pending)  
Depends on: R2P-11  
Files: `crates/app/src/main.rs`, `crates/execution/*`, `configs/live.toml`

`R2P-15` — Dry-run and tiny-live rollout  
Depends on: R2P-13, R2P-14, R2P-05, R2P-06, R2P-07  
Artifacts: rollout reports, KPI dashboard snapshots

`R2P-16` — Config truthfulness cleanup (`pause_new_trades_on_outage`)  
Status: ✅ Done (2026-02-19)  
Depends on: R2P-06  
Files: `crates/app/src/main.rs`, `crates/config/src/lib.rs`, `configs/*.toml`  
Evidence (code/tests): `crates/app/src/main.rs` (`run_app_loop(..., pause_new_trades_on_outage)`, `can_open_buy(..., pause_new_trades_on_outage)`), unit test `risk_guard_infra_block_respects_pause_new_trades_on_outage_flag`

`R2P-17` — Close migration + production readiness sign-off  
Depends on: R2P-02, R2P-15, R2P-16  
Artifacts: signed go-live decision note

`R2P-18` — Controlled-live reconcile and incident discipline  
Depends on: R2P-15, R2P-17  
Artifacts: daily reconcile reports, incident log, postmortems for P0

`R2P-19` — Operational SLO dashboard + alert tuning for submit path  
Depends on: R2P-11, R2P-13, R2P-18  
Artifacts: dashboard snapshots, alert thresholds, on-call ack evidence

`R2P-20` — Standard-live config freeze and release policy  
Depends on: R2P-18, R2P-19  
Artifacts: approved config baseline (`live.toml` + env contract), change-management checklist

`R2P-21` — Runbook drill pack (emergency stop, rollback, key rotation, watchdog failover)  
Depends on: R2P-20  
Artifacts: dated drill reports with participants and pass/fail outcomes

`R2P-22` — Final standard-live handoff and ownership sign-off  
Depends on: R2P-20, R2P-21  
Artifacts: signed handoff note, ownership matrix, residual risk register

### 6.1 Audit Notes Consolidation (2026-02-19)

Закрыто в коде:

1. execution hard-stop gap: BUY pause now respects operator/hard-stop/outage, SELL/confirm unaffected.
2. per-token exposure cap: enforced in runtime risk checks + config validation.
3. lifecycle stuck-risk: `execution_submitted` and `execution_simulated` are reprocessed each tick.
4. idempotency ordering: order creation precedes signal-status update; recovery uses `client_order_id`.
5. storage safety: transactional positions apply, strict side/sell checks, affected-rows checks, `fills(order_id)` index, non-empty `client_order_id` guard.
6. dead API cleanup: removed redundant `execution_order_status_by_client_order_id`.
7. order insert telemetry contract tightened: `insert_execution_order_pending` now distinguishes `Inserted` vs `Duplicate` and fails on unknown ignore.
8. pre-trade pipeline wired before simulation/submit with bounded retries for retryable pre-trade failures.
9. submit classification hardened: runtime now uses typed submit errors (`SubmitErrorKind`) for deterministic retry/terminal branching.
10. RPC pre-trade/confirm hardening: new mode `paper_rpc_pretrade_confirm`, signer-balance reserve gate, optional ATA existence policy (`pretrade_require_token_account`), optional priority fee cap (`pretrade_max_priority_fee_lamports`, unit: micro-lamports/CU), and RPC confirmer support for confirmed/failed/pending states.
11. execution scheduling decoupled from main async loop: execution batch runs in dedicated blocking task to avoid ingestion stalls under RPC latency.
12. confirm->reconcile path hardened to atomic finalize transaction (`fills + positions + order/signal status`) with idempotent `AlreadyConfirmed` outcome.
13. execution price policy hardened for confirmed-path consistency: when market price is unavailable at confirm time, runtime uses deterministic reconcile fallback pricing (open-position avg-cost if present, else bounded `1.0`) and emits `execution_price_unavailable_fallback_used` risk event (`manual_reconcile_recommended=true`) instead of marking confirmed tx as failed; ops handling is fixed in `ops/execution_manual_reconcile_runbook.md` with `tools/execution_price_fallback_report.sh`.
14. ingestion telemetry now tracks parse rejects by reason (in addition to `parse_rejected_total`) and parser fallback reasons (for example `missing_program_ids_fallback`) as separate reason breakdown; `tools/runtime_snapshot.sh` now prints both breakdown maps explicitly from latest ingestion telemetry sample, and `tools/ops_scripts_smoke_test.sh` validates this breakdown path via journal fixture output.
15. RPC pre-trade balance gate is side-aware: BUY requires `notional + reserve`, SELL requires reserve only (exit path no longer blocked by BUY notional budget).
16. submit path hardening advanced: added `adapter_submit_confirm` mode with HTTP adapter submitter contract, route allowlist policy, route-level slippage caps, and fail-closed init behavior for non-paper submit mode.
17. adapter response policy tightened: response `route` must match requested route; mismatch is terminal fail-closed (`submit_adapter_route_mismatch`) before order status write.
18. route-level compute budget policy added to submit path (`cu_limit`, `cu_price_micro_lamports`) with strict runtime validation for allowed/default routes.
19. adapter response correlation tightened: optional `client_order_id`/`request_id` echoes must match requested `client_order_id` or submit is terminal-failed (`submit_adapter_client_order_id_mismatch` / `submit_adapter_request_id_mismatch`).
20. adapter confirm-failure semantics hardened: deadline-passed confirm errors/timeouts are marked with `*_manual_reconcile_required` err-codes + risk events to enforce explicit on-chain reconcile workflow.
21. submit route fallback hardened: per-attempt route selection now follows ordered policy (`default_route` -> allowed fallbacks), and both pre-trade + submit use the same selected route for deterministic retries.
22. adapter auth hardening baseline: runtime now supports optional HMAC request signing for submit adapter calls (`x-copybot-key-id`, `x-copybot-timestamp`, `x-copybot-auth-ttl-sec`, `x-copybot-nonce`, `x-copybot-signature`) with strict startup validation; signature verifier must use raw request body bytes.
23. adapter secret-sourcing hardened: runtime supports file-based sources for adapter token/HMAC secret (`submit_adapter_auth_token_file`, `submit_adapter_hmac_secret_file`) with fail-closed checks (non-empty file, no inline+file duplication), and relative paths resolve against loaded config directory (not process cwd).
24. route policy now has explicit operator-controlled order knob: `submit_route_order` (validated against `submit_allowed_routes` + must include `default_route`) and consumed by attempt-based fallback selection.
25. execution telemetry now emits per-route metrics per batch (`submit_attempted/retry/failed` + `pretrade_retry/rejected/failed` + `confirm_confirmed/retry/failed` + runtime-observed `submit->confirm` latency sum/samples) to support route-profile calibration.
26. adapter response contract pinning added: runtime enforces `contract_version` match and optional strict policy-echo checks via `submit_adapter_require_policy_echo` (slippage/tip/CU + full fee-breakdown echo `network_fee_lamports`/`base_fee_lamports`/`priority_fee_lamports`).
27. route-level tip policy added to submit path (`submit_route_tip_lamports`) with strict runtime validation for allowed/default routes, guardrail `0..=100_000_000 lamports` per route, and optional strict adapter policy-echo verification (`tip_lamports`).
28. reconcile fee accounting wired into confirmed-path finalize: `fee_sol` now uses on-chain network fee (`getTransaction.meta.fee` from RPC confirmer) + applied tip from submit lifecycle (persisted per-order) + optional ATA-create rent from adapter response, and positions/PnL now account for fees instead of fixed `0.0`.
29. fee diagnostics improved: if confirmed order is processed without resolved network fee in `adapter_submit_confirm`, runtime now emits reasoned telemetry (`rpc_lookup_error` vs `meta_fee_unavailable`) plus sanitized typed lookup error class (`timeout`/`connect`/`invalid_json`/`rpc_error_payload`/`other`) for incident triage.
30. fee-breakdown persistence safety hardened: adapter response rejects `ata_create_rent_lamports > i64::MAX`, storage write uses checked u64→i64 conversion (no wrap), read path fails on negative lamport fields, and DB triggers enforce non-negative lamports for fee-breakdown columns.
31. fee-breakdown hints extended and persisted end-to-end: submit adapter response now supports optional `network_fee_lamports` / `base_fee_lamports` / `priority_fee_lamports` hints, persists them on `orders` (`0015`), enforces non-negative safety via trigger refresh (`0016`), and confirmed-path finalize uses persisted `network_fee_lamports_hint` fallback (with explicit risk-event source tagging) when RPC `meta.fee` is unavailable.
32. fee-breakdown hint consistency hardened: adapter response is now fail-closed if `network_fee_lamports` disagrees with `base_fee_lamports + priority_fee_lamports`, and runtime emits `execution_network_fee_hint_mismatch` telemetry when RPC `meta.fee` disagrees with persisted submit hint while still preferring RPC fee for accounting.
33. per-route fee-source telemetry added to execution batch report/logs for adapter mode: confirmed orders now emit route counters for `network_fee` source (`rpc_meta` / `submit_hint` / `missing`) to support route-profile fee calibration and incident triage.
34. per-route fee lamports aggregation added for confirmed path: execution batch telemetry now includes route-level sums for `network_fee`, `tip`, `ata_rent`, total fee, and optional `base/priority` hint sums for direct calibration of adapter fee policy; ops helper `tools/execution_fee_calibration_report.sh` added for DB-based route breakdown snapshots.
35. prod runtime contract tightened for adapter mode: `execution.submit_adapter_require_policy_echo` is now mandatory (`true`) when `execution.mode=adapter_submit_confirm` in production-like env profiles (`prod`, `production`, `prod-*`, `production-*`, underscore variants), preventing non-strict adapter responses in production deployment profiles.
36. ops fee diagnostics helpers (`tools/execution_fee_calibration_report.sh`, `tools/runtime_snapshot.sh`) are schema-tolerant for legacy DB snapshots: fee-breakdown report SQL now auto-detects optional `orders` fee columns and falls back to `0` when columns are absent, avoiding hard failure on older local snapshots; calibration report includes per-route fee-hint coverage counts (`network/base/priority/tip/ata`), per-route outcome KPI/confirm-latency sections (attempted/confirmed/failed/timeout/inflight and latency min/avg/p95/max), a route calibration scorecard (recommended rank by success/timeout/latency with explicit `NULL`-latency de-prioritization), and an explicit suggested `SOLANA_COPY_BOT_EXECUTION_SUBMIT_ROUTE_ORDER` env override filtered by current `execution.submit_allowed_routes`, while smoke integration helper `tools/ops_scripts_smoke_test.sh` validates both scripts against synthetic legacy+modern SQLite schemas (including malformed/no-ingestion journal branches).
37. `tools/execution_fee_calibration_report.sh` strict policy reject section now merges structured `risk_events` with legacy `orders.simulation_error` pattern; deduplication is by `order_id` when present (fallback to source-row identity when `order_id` is absent) so historical windows remain comparable across telemetry format migration.
38. adapter endpoint runtime validation hardened: `execution.submit_adapter_http_url` / `execution.submit_adapter_fallback_http_url` are now parsed via strict URL parser (`url::Url`) and must be explicit valid `http(s)` URLs (no bare host / malformed authority), and production-like env profiles fail-closed for non-loopback `http://` endpoints (`https://` required except loopback hosts).
39. adapter endpoint URL security hardening: runtime now rejects URL-embedded credentials (`user:pass@`), query parameters, and fragments for adapter endpoints to prevent secret-in-URL patterns and enforce header-based auth contract.
40. adapter endpoint placeholder hardening: runtime now fail-closes when adapter URL contains `REPLACE_ME` in case-insensitive form (`REPLACE_ME` / `replace_me`), preventing accidental startup with template placeholder endpoints.
41. adapter `submit_adapter_contract_version` token hardening: runtime/submitter now reject non-token contract versions and allow only `[A-Za-z0-9._-]` characters (no spaces/slashes), preventing malformed contract-version values from entering adapter handshake.
42. adapter endpoint redundancy guard: when both `submit_adapter_http_url` and `submit_adapter_fallback_http_url` are set, runtime now normalizes endpoint identity (`scheme+host+port+path`) and fail-closes if they resolve to the same endpoint.
43. route policy normalization guard: config ENV parsing now fail-closes on case-insensitive duplicate route entries/keys (`rpc` vs `RPC`) for both route lists (`submit_allowed_routes`, `submit_route_order`) and route-policy maps (`submit_route_max_slippage_bps`, `submit_route_tip_lamports`, `submit_route_compute_unit_limit`, `submit_route_compute_unit_price_micro_lamports`) before config load, and runtime keeps the same fail-close checks as a second layer.
44. execution BUY-risk enforcement extended to live loss controls: runtime now applies `risk.daily_loss_limit_pct` (24h realized + current unrealized PnL guard) and `risk.max_drawdown_pct` (rolling 24h drawdown with terminal unrealized open-position mark-to-market) as `risk_blocked` BUY gates, backed by storage queries (`live_realized_pnl_since`, `live_unrealized_pnl_sol`, `live_max_drawdown_with_unrealized_since`) and runtime risk contract validation for both percent fields (`[0, 100]` bounds); missing open-position price quotes now fail-closed (`unrealized_price_unavailable`) and unrealized mark-to-market is computed once per BUY risk check.
45. fee decomposition calibration helpers now include explicit readiness evidence for live-path rollout: `tools/execution_fee_calibration_report.sh` prints per-route decomposition completeness (`network/base/priority/tip/ata` coverage + hint consistency checks), adds per-order fee accounting consistency check (`fills.fee` vs `network+tip+ata` with lamport-level tolerance), and adapter-mode verdict now requires non-missing `ata` coverage plus full consistency-check coverage (`fee_consistency_full_hint_rows == confirmed_orders_total`) to avoid false `PASS`; smoke coverage (`tools/ops_scripts_smoke_test.sh`) validates the new output path.
46. route-profile operational calibration helper added: `tools/execution_fee_calibration_report.sh` now emits adapter-mode route-profile readiness verdict (`route_profile_verdict`) for recommended primary/fallback order using submit-window KPI thresholds (attempted/success/timeout), and prints explicit policy knobs to tune (`submit_route_order`, route slippage/tip/CU maps); smoke suite validates both `SKIP` and adapter-mode `WARN` branches.
47. go/no-go evidence helper added: `tools/execution_go_nogo_report.sh` now composes calibration + runtime snapshot into a single summary (`GO/NO_GO/HOLD`) with gate reasons (`fee_decomposition_verdict`, `route_profile_verdict`), ingestion context, and optional artifact export (`OUTPUT_DIR`); gate classification is fail-closed for `UNKNOWN` verdicts (takes precedence over `SKIP/NO_DATA`), and smoke suite validates paper-mode `HOLD`, adapter-mode `NO_GO`, artifact export, and `UNKNOWN+SKIP => NO_GO` precedence.
48. adapter production preflight helper added: `tools/execution_adapter_preflight.sh` validates adapter-submit contract readiness before process startup (mode/env gates, strict URL policy, signer/contract token checks, route/default invariants, secret source conflicts, secret-file resolution relative to config, HMAC pair+TTL bounds, strict-policy-echo requirement in production-like profiles) and returns fail-closed `preflight_verdict: FAIL` on contract drift; smoke suite covers both PASS and FAIL branches.
49. go/no-go summary now includes explicit adapter preflight gate: `tools/execution_go_nogo_report.sh` runs `tools/execution_adapter_preflight.sh` in-band, surfaces `preflight_verdict/preflight_reason/error_count` in summary, forces `NO_GO` on `preflight_verdict=FAIL|UNKNOWN`, and writes preflight raw artifact alongside calibration/snapshot exports.
50. adapter simulation path is now wired for `adapter_submit_confirm`: execution no longer uses paper simulator in adapter mode; it calls adapter HTTP simulation endpoint contract (same endpoint set, `action=simulate`, optional strict policy-echo for `route`/`contract_version`, HMAC/Bearer parity with submit path), and fail-closes to dropped intent via `FailClosedIntentSimulator` when simulator init fails.
51. Stage C.5 devnet rehearsal helper added: `tools/execution_devnet_rehearsal.sh` now orchestrates adapter preflight + go/no-go evidence + targeted regression tests into a single rehearsal verdict (`GO/HOLD/NO_GO`) with artifact export; operator workflow is documented in `ops/execution_devnet_rehearsal_runbook.md`, and smoke suite covers a test-mode execution path for the helper.
52. production adapter backend service scaffold added as standalone binary `copybot-adapter` (`crates/adapter`): contract-validated `/simulate` + `/submit` + `/healthz`, fail-closed request validation (contract version, route allowlist, token/notional/compute-budget bounds), optional inbound Bearer + HMAC verification (nonce replay window, TTL checks, raw-body signature), route-aware upstream forwarding with per-route/default backend URLs and auth tokens, strict upstream outcome normalization (unknown status fail-closed, `429/5xx` retryable), and normalized submit response echo (`route`/`contract_version`/policy fields + fee hint safety checks) compatible with runtime strict policy-echo expectations.
53. adapter backend auth hardened to fail-closed defaults: startup now requires inbound auth (`COPYBOT_ADAPTER_BEARER_TOKEN` or HMAC key/secret pair) unless explicitly overridden with `COPYBOT_ADAPTER_ALLOW_UNAUTHENTICATED=true` for controlled non-production testing; `/simulate` now returns HTTP `503` for retryable rejects so runtime simulator fallback can activate, and upstream `429/5xx` non-JSON bodies are classified as retryable transport errors (not terminal invalid-json).
54. adapter backend secret-sourcing hardened for production rotation workflow: inbound secrets and upstream auth tokens now support file-based sources (`COPYBOT_ADAPTER_BEARER_TOKEN_FILE`, `COPYBOT_ADAPTER_HMAC_SECRET_FILE`, `COPYBOT_ADAPTER_UPSTREAM_AUTH_TOKEN_FILE`, `COPYBOT_ADAPTER_ROUTE_<ROUTE>_AUTH_TOKEN_FILE`) with fail-closed semantics (inline+file conflict rejected, secret file trimmed, empty/missing file rejected) and unit coverage for conflict/missing/empty-file scenarios.
55. adapter secret-rotation evidence helper added: `tools/adapter_secret_rotation_report.sh` validates adapter secret file sources from `adapter.env` (exist/readable/non-empty, auth contract completeness, permission hardening warnings), emits `PASS/WARN/FAIL` readiness verdict with optional artifact export, and is covered in `tools/ops_scripts_smoke_test.sh` pass/warn/fail branches.
56. adapter upstream failover wiring added for production backend resilience: per-route and global submit/simulate fallback endpoints are now supported (`COPYBOT_ADAPTER_UPSTREAM_*_FALLBACK_URL`, `COPYBOT_ADAPTER_ROUTE_<ROUTE>_*_FALLBACK_URL`), startup fail-closes when fallback resolves to same endpoint identity as primary, and forwarding policy now retries only retryable upstream failures (`send`/`429`/`5xx`) while preserving terminal short-circuit on contract/`4xx` rejects.
57. adapter submit visibility hardening added: adapter now validates upstream `tx_signature` as base58-encoded 64-byte signature and supports optional post-submit on-chain visibility check via RPC (`COPYBOT_ADAPTER_SUBMIT_VERIFY_RPC_URL` with optional fallback/attempts/interval/strict mode); in strict mode unseen signatures fail-closed as retryable rejects (`upstream_submit_signature_unseen`), while non-strict mode records warning diagnostics in response/logs.
58. adapter failover auth policy expanded: fallback upstream can use dedicated auth secret (`COPYBOT_ADAPTER_UPSTREAM_FALLBACK_AUTH_TOKEN[_FILE]` and `COPYBOT_ADAPTER_ROUTE_<ROUTE>_FALLBACK_AUTH_TOKEN[_FILE]`) while preserving backward-compatible inheritance from primary auth when fallback secret is not configured.
59. adapter signed-transaction send wiring added: adapter submit path now accepts upstream `signed_tx_base64` (when `tx_signature` is absent), broadcasts via route-configured send RPC endpoint set (`COPYBOT_ADAPTER_SEND_RPC_URL` / per-route `..._SEND_RPC_URL` with optional fallback/auth), and returns validated on-chain signature; legacy upstream `tx_signature` path remains backward-compatible.
60. adapter rollout evidence orchestrator added: `tools/adapter_rollout_evidence_report.sh` now composes adapter secret-rotation readiness (`tools/adapter_secret_rotation_report.sh`) and Stage C.5 devnet rehearsal (`tools/execution_devnet_rehearsal.sh`) into one fail-closed summary (`adapter_rollout_verdict=GO|HOLD|NO_GO`) with captured raw artifacts and smoke coverage for all verdict branches.
61. live runtime baseline config added: `configs/live.toml` now provides a dedicated Stage D/E profile scaffold (`adapter_submit_confirm` + `jito/rpc` route policy + Stage E tiny-live risk limits), kept fail-safe with `execution.enabled=false` by default until rollout gates are closed.
62. Stage D fallback policy tightened in runtime: `jito -> rpc` submit-route fallback is now allowed only for explicit availability-class retryable submit errors (adapter/upstream/send-rpc transport and HTTP-unavailable classes); non-allowlisted retryable codes fail-closed as `submit_fallback_blocked` with risk-event evidence instead of silently switching route.
63. optional dynamic submit CU-price policy added for adapter mode: runtime can now raise route `compute_budget.cu_price_micro_lamports` using RPC `getRecentPrioritizationFees` percentile hints (`execution.submit_dynamic_cu_price_enabled`, `execution.submit_dynamic_cu_price_percentile`), bounded fail-closed by `execution.pretrade_max_priority_fee_lamports`; default remains disabled and falls back to static per-route CU policy when hints are unavailable.
64. dynamic CU-price hinting timeout isolation added: fee-hint RPC polling now uses a dedicated short-timeout client (capped independently from submit timeout) so degraded priority-fee endpoints cannot consume the full submit request budget before adapter submit.
65. dynamic CU-price hint phase now has a strict total timeout budget across primary+fallback RPC endpoints (not per-endpoint additive), preventing sequential endpoint probing from extending submit latency beyond the dedicated hint window.
66. dynamic CU-price timeout behavior is now covered by integration test (`slow primary + optional fast fallback`) that verifies wall-clock budget enforcement and static-policy fallback when hint budget is exhausted before secondary endpoint polling.
67. optional dynamic tip strategy added for adapter mode: runtime can now raise route `tip_lamports` from resolved compute budget (`cu_limit` × `cu_price_micro_lamports`) using configurable multiplier (`execution.submit_dynamic_tip_lamports_multiplier_bps`) with static tip as floor and global tip guardrail cap; policy is fail-closed and default-off.
68. execution batch telemetry extended for dynamic submit policies: runtime now emits per-route counters for dynamic CU policy enablement/hint usage/applied-vs-static-fallback and dynamic tip policy enablement/applied-vs-static-floor, allowing rollout evidence to distinguish true dynamic-path usage from static fallback behavior.
69. dynamic submit-policy telemetry now covers failed/retryable submit attempts too: adapter submit errors preserve dynamic policy flags (`enabled/hint_used/applied`) and execution pipeline records the same per-route dynamic counters on both success and error paths, eliminating observability blind spots during degraded submit windows.
70. ops runtime snapshot helper now surfaces latest execution batch counters from journal logs (including dynamic submit-policy route maps), and smoke coverage validates presence/absence branches to keep go/no-go evidence observable during adapter-mode rollout.
71. go/no-go summary now explicitly carries execution submit dynamic-policy evidence from runtime snapshot (`execution_batch_sample_available` and per-route submit/dynamic maps), so rollout packets can be reviewed from a single `tools/execution_go_nogo_report.sh` artifact without cross-reading raw snapshot output.
72. go/no-go helper now emits explicit dynamic-policy readiness verdicts (`dynamic_cu_policy_verdict`, `dynamic_tip_policy_verdict`) with per-policy reasons and aggregate counters from runtime submit telemetry; this keeps dynamic-policy rollout evidence actionable without changing top-level fail-closed `overall_go_nogo_verdict` precedence.
73. Stage C.5/rollout orchestrators now propagate dynamic-policy readiness fields from go/no-go output (`dynamic_cu_policy_verdict/reason`, `dynamic_tip_policy_verdict/reason`) into `tools/execution_devnet_rehearsal.sh` and `tools/adapter_rollout_evidence_report.sh` summaries, so auditors can review dynamic submit readiness directly from top-level rehearsal/rollout artifacts.
74. go/no-go summary now includes route-profile and fee-decomposition calibration context fields (`primary_route/fallback_route`, primary/fallback KPI sample metrics, and fee-consistency/fallback counters), so Stage D tightening evidence for route order and fee decomposition can be audited from one consolidated artifact.
75. devnet rehearsal summary now mirrors go/no-go route-profile/fee-decomposition context fields (primary/fallback route and KPI/counter excerpts), reducing operator dependency on nested artifact lookup during Stage C.5 evidence review.
76. adapter rollout evidence summary now also mirrors rehearsal route-profile/fee-decomposition context fields (primary/fallback route, KPI snippets, fee-consistency/fallback counters), so final rollout packets remain self-contained at the top-level orchestrator artifact.
77. go/no-go artifact export now emits deterministic SHA-256 checksums (`calibration/snapshot/preflight/summary`) and writes a manifest file (`execution_go_nogo_manifest_*.txt`) alongside raw captures, improving evidence-chain verification during Stage C.5 and rollout audits.
78. devnet rehearsal and rollout orchestrators now emit checksum manifests too (`execution_devnet_rehearsal_manifest_*.txt`, `adapter_rollout_evidence_manifest_*.txt`) with SHA-256 hashes for their captured artifacts, aligning all Stage C.5 rollout evidence tools to the same integrity contract.
79. adapter secret rotation helper now emits a checksum manifest (`adapter_secret_rotation_manifest_*.txt`) with report SHA-256 when `OUTPUT_DIR` is set, completing checksum-chain parity across rotation/rehearsal/rollout evidence helpers.
80. rehearsal/rollout summaries now propagate nested go/no-go manifest/hash metadata (`go_nogo_artifact_manifest`, `go_nogo_*_sha256`) so checksum-chain tracing can be audited directly from top-level orchestrator outputs.
81. rollout summary now also exposes nested rotation and rehearsal checksum metadata (`rotation_artifact_manifest/report_sha256`, `rehearsal_artifact_manifest/*_sha256`) so both sub-gates are integrity-traceable from the top-level rollout artifact.
82. smoke coverage now validates checksum fields as strict lowercase SHA-256 hex (64 chars) across rotation/go-no-go/rehearsal/rollout helpers, preventing silent fallback to non-hash tokens in evidence outputs.
83. dynamic CU-price policy now supports an optional external Priority Fee API source (`execution.submit_dynamic_cu_price_api_primary_url` with optional fallback/auth token and `_file` secret source), queried before RPC hints with shared timeout budget and fail-closed config guards; RPC `getRecentPrioritizationFees` remains automatic fallback when API hints are unavailable.
84. dynamic CU hint source split (`api` vs `rpc`) is now propagated end-to-end in evidence orchestration: go/no-go emits source totals, devnet rehearsal mirrors them (`dynamic_cu_hint_api_total`/`dynamic_cu_hint_rpc_total`), and rollout summary carries the same fields; smoke validates snapshot/go-no-go/rehearsal/rollout branches to keep source-level telemetry regressions visible.
85. go/no-go now emits explicit dynamic CU hint-source readiness gate (`dynamic_cu_hint_source_verdict/reason`) with config awareness (`dynamic_cu_hint_api_configured`) and source totals, and rehearsal/rollout summaries propagate the same fields so API-vs-RPC hint adoption can be audited without opening nested artifacts.
86. multi-window adapter readiness helper added: `tools/execution_windowed_signoff_report.sh` now aggregates `execution_go_nogo_report.sh` over configurable windows (default `1,6,24`) into one `signoff_verdict=GO|HOLD|NO_GO` with per-window fee/route gates, route stability checks, and captured-artifact checksum manifest; smoke covers both `HOLD` (paper/skip) and `GO` (adapter test-mode PASS) branches.
87. windowed signoff helper hardened fail-closed: per-window PASS now requires nested `overall_go_nogo_verdict=GO` and exit code `0` (not only fee/route PASS), preventing false `signoff_verdict=GO` when nested go/no-go is `NO_GO/HOLD`; smoke includes explicit regression case (`fee/route PASS` with nested overall `NO_GO` => signoff `NO_GO`).
88. devnet rehearsal/rollout orchestration now carries windowed signoff evidence end-to-end: `tools/execution_devnet_rehearsal.sh` always captures nested `execution_windowed_signoff_report.sh` output (`windowed_signoff_*` fields + artifact hashes) and can enforce it as a required gate via `WINDOWED_SIGNOFF_REQUIRED=true` (with configurable `WINDOWED_SIGNOFF_WINDOWS_CSV`), while `tools/adapter_rollout_evidence_report.sh` propagates the same fields to top-level rollout summary; smoke covers pass-path propagation and required-gate `NO_GO` branches for both helpers.
89. windowed signoff helper now supports optional dynamic-hint-source strictness (`WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS=true`): when dynamic CU policy is enabled in nested go/no-go output, each window requires `dynamic_cu_hint_source_verdict=PASS` (UNKNOWN remains fail-closed), and the strictness flag is propagated through rehearsal/rollout summaries with smoke coverage for the `HOLD` branch.
90. windowed signoff helper now also supports optional dynamic-tip strictness (`WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS=true`): when dynamic tip policy is enabled in nested go/no-go output, each window requires `dynamic_tip_policy_verdict=PASS` (UNKNOWN remains fail-closed), and the strictness flag is propagated through rehearsal/rollout summaries with smoke coverage for the `HOLD` branch.
91. windowed signoff summary now includes nested go/no-go artifact-chain metadata per window (`window_*_go_nogo_artifact_manifest` and `window_*_go_nogo_{calibration,snapshot,preflight,summary}_sha256`), so multi-window signoff packets can be audited without opening each captured go/no-go output manually.
92. evidence helpers now emit explicit `artifacts_written` flags even when `OUTPUT_DIR` is unset (`false`), and upstream orchestration summaries propagate nested artifact-write flags (`go_nogo_artifacts_written`, `windowed_signoff_artifacts_written`, `rehearsal_artifacts_written`) so `n/a` hash fields remain auditable as intentional no-export states.
93. submit fallback hardening now also applies fail-closed allowlist policy to `fastlane -> rpc` transitions (same retryable availability/transport code set as `jito -> rpc`), preventing silent fallback on unclassified retryable submit errors when Fastlane is introduced under feature-flag routing.
94. go/no-go helper now supports optional strict route-policy gate (`GO_NOGO_REQUIRE_JITO_RPC_POLICY=true`) that requires adapter-mode route-profile evidence to resolve as `primary=jito` and `fallback=rpc` (`jito_rpc_policy_verdict=PASS`), and this strictness is propagated through windowed signoff/devnet rehearsal/rollout summaries.
95. adapter route contract now enforces explicit Fastlane feature-flag gating: `execution.submit_fastlane_enabled` defaults to `false`, and runtime config validation + adapter preflight fail-closed if `fastlane` appears in default/allowed/order/CU-tip-slippage maps without enabling this flag.
96. go/no-go/windowed/rehearsal/rollout orchestration now supports optional strict Fastlane-disabled gate (`GO_NOGO_REQUIRE_FASTLANE_DISABLED=true`): nested go/no-go requires `execution.submit_fastlane_enabled=false` (`fastlane_feature_flag_verdict=PASS`), and this strictness metadata is propagated through higher-level evidence summaries.
97. route-profile + fee-decomposition signoff helper added: `tools/execution_route_fee_signoff_report.sh` runs `execution_go_nogo_report.sh` and `execution_fee_calibration_report.sh` across multi-window sets (default `1,6,24`), enforces per-window verdict parity (`route_profile_verdict` and `fee_decomposition_verdict`), checks primary/fallback route stability, and emits a single `signoff_verdict=GO|HOLD|NO_GO` with artifact manifest for next-code-queue evidence packaging.
98. top-level rollout orchestrator now captures route/fee signoff evidence in-band (`execution_route_fee_signoff_report.sh`) and can enforce it as required gate via `ROUTE_FEE_SIGNOFF_REQUIRED=true` (windows configurable with `ROUTE_FEE_SIGNOFF_WINDOWS_CSV`), while propagating route/fee signoff verdict, stability fields, and capture hashes in rollout summary artifacts.
99. Stage C.5 rehearsal helper now also captures nested route/fee signoff evidence (`execution_route_fee_signoff_report.sh`) and can enforce it as required gate via `ROUTE_FEE_SIGNOFF_REQUIRED=true` (`ROUTE_FEE_SIGNOFF_WINDOWS_CSV` configurable), while propagating verdict/stability/hash fields in `tools/execution_devnet_rehearsal.sh` summary and artifacts.
100. rollout summary now exposes nested rehearsal route/fee signoff fields under explicit `rehearsal_route_fee_*` keys, so operators can distinguish rehearsal-gate evidence from top-level rollout route/fee gate evidence in a single artifact.
101. route/fee signoff helper now propagates per-window nested go/no-go reason fields (`window_*_overall_go_nogo_reason`) and uses that reason in hard-block summaries (`signoff_reason` for nested `NO_GO`/`HOLD`), with smoke coverage for strict-policy hard-block path (`GO_NOGO_REQUIRE_JITO_RPC_POLICY=true`) to keep fail-closed diagnostics explicit in multi-window evidence output.
102. strict policy diagnostics now emit stable reason codes (`jito_rpc_policy_reason_code`, `fastlane_feature_flag_reason_code`, `overall_go_nogo_reason_code`, `signoff_reason_code`, `devnet_rehearsal_reason_code`, `adapter_rollout_reason_code`) across go-no-go/windowed/rehearsal/rollout summaries, and smoke assertions use these codes to reduce false failures from copy-edits in human-readable reason text.
103. diagnostics hardening finalized for rollout evidence chain: dynamic hint-source and route/fee signoff now also emit stable reason codes end-to-end (`dynamic_cu_hint_source_reason_code`, `route_fee_signoff_reason_code`, `rehearsal_route_fee_signoff_reason_code`), and smoke assertions were upgraded from free-text substring matching to field-scoped exact-code checks (`assert_field_equals`/`assert_field_in`) to remove false positives from reason phrasing edits.
104. final rollout evidence package helper added: `tools/adapter_rollout_final_evidence_report.sh` runs strict-gated rollout evidence (`adapter_rollout_evidence_report.sh`) into a single package artifact root (`OUTPUT_ROOT`) with consolidated package verdict fields, captured rollout output, and top-level checksum manifest; smoke covers the pass-path package export and nested artifact presence.
105. final route/fee signoff evidence package helper added: `tools/execution_route_fee_final_evidence_report.sh` runs `execution_route_fee_signoff_report.sh` into a single package artifact root (`OUTPUT_ROOT`) with strict policy defaults (`GO_NOGO_REQUIRE_JITO_RPC_POLICY=true`, `GO_NOGO_REQUIRE_FASTLANE_DISABLED=true`), consolidated package verdict fields (`final_route_fee_package_verdict`), and checksum manifest chaining top-level + nested signoff artifacts; smoke now covers wrapper exit mapping (`GO/HOLD/NO_GO`).
106. executor rollout/final evidence helpers added: `tools/executor_rollout_evidence_report.sh` now composes signer-rotation readiness + executor preflight + Stage C.5 devnet rehearsal into one fail-closed summary (`executor_rollout_verdict=GO|HOLD|NO_GO`) with capture+manifest artifacts, and `tools/executor_final_evidence_report.sh` wraps it into a strict final package (`final_executor_package_verdict`) with top-level checksum manifest; smoke covers GO and fail-closed branches.
107. executor evidence-chain hardening follow-up: smoke coverage now includes explicit `HOLD` paths for both `executor_rollout_evidence_report.sh` and `executor_final_evidence_report.sh` (rehearsal-hold propagation), and runbooks (`ops/adapter_backend_runbook.md`, `ops/execution_devnet_rehearsal_runbook.md`) now document executor rollout/final evidence commands for self-hosted adapter-upstream topology.
108. executor phase-2 tx-build extraction continued: submit compute-budget validation now lives in `crates/executor/src/tx_build.rs` (`validate_submit_compute_budget` with bound-typed errors), `handle_submit` consumes the shared validator while preserving `invalid_compute_budget` reject contract, and new guard tests (`handle_submit_rejects_compute_budget_limit_out_of_range`, `handle_submit_rejects_compute_budget_price_out_of_range`) are included in `tools/executor_contract_smoke_test.sh`.
109. executor phase-2 tx-build extraction continued: submit slippage policy checks now use shared `tx_build::validate_submit_slippage_policy` (finite/positive checks + cap/epsilon enforcement), `handle_submit` preserves existing reject code semantics (`invalid_slippage_bps`, `invalid_route_slippage_cap_bps`, `slippage_exceeds_route_cap`), and contract smoke now covers the new slippage guard tests.
110. executor phase-2 submit-path extraction continued: submit transport artifact parsing/validation moved into `crates/executor/src/submit_transport.rs` (`extract_submit_transport_artifact`), `handle_submit` now uses typed transport outcomes (`upstream_signature` vs `adapter_send_rpc`) while preserving fail-closed reject semantics (`submit_adapter_invalid_response`), and guard coverage now includes missing transport artifact rejection.
111. executor phase-2 submit-path extraction continued: upstream submit-response echo/metadata validation (`route`, `contract_version`, `client_order_id`, `request_id`, `submitted_at`) moved into `crates/executor/src/submit_response.rs` with typed errors and preserved reject mappings (`submit_adapter_*` codes), and contract smoke now includes invalid `submitted_at` guard coverage.
112. executor phase-2 submit-path extraction continued: upstream fee-hint field parsing (`network/base/priority/ata`) moved into `crates/executor/src/fee_hints.rs` (`parse_response_fee_hint_fields`) with explicit typed parse errors and preserved fail-closed mapping to `submit_adapter_invalid_response`, and contract smoke now includes invalid fee-hint field-type guard coverage.
113. executor phase-2 submit-path extraction continued: upstream business-outcome classification (`status/ok/accepted/retryable/code/detail`) moved into `crates/executor/src/upstream_outcome.rs` (`parse_upstream_outcome`) with parsed reject model and unchanged caller-level mapping to terminal/retryable rejects, and contract smoke now includes direct module guard coverage for unknown upstream status.
114. executor phase-2 submit-path extraction continued: successful submit response assembly moved into `crates/executor/src/submit_payload.rs` (`build_submit_success_payload`) with unchanged payload schema (`submit_transport`, `fee hints`, `compute_budget`, optional `tip_policy`, `submit_signature_verify`) and contract smoke now includes direct payload guard coverage for tip-policy inclusion semantics.
115. executor phase-2 simulate-path extraction continued: upstream simulate response handling moved into `crates/executor/src/simulate_response.rs` (route/contract echo validation, detail normalization, success payload builder) with unchanged fail-closed reject mappings (`simulation_route_mismatch`, `simulation_contract_version_mismatch`) and added guard coverage for upstream route-mismatch branch.
116. executor route normalization de-dup completed: shared helper `crates/executor/src/route_normalization.rs` now owns `normalize_route`, and all previous duplicate implementations in `main.rs`, `submit_response.rs`, and `simulate_response.rs` were replaced with this single source of truth; contract smoke now includes direct helper coverage (`normalize_route_trims_and_lowercases`).
117. executor submit deadline extraction completed: `SubmitDeadline` moved from `main.rs` into dedicated module `crates/executor/src/submit_deadline.rs` with focused unit tests, and all call sites (`main.rs`, `send_rpc.rs`, `submit_verify.rs`) now consume the shared deadline type while preserving existing submit timeout budget reject behavior (`executor_submit_timeout_budget_exceeded`).
118. executor key-shape validation extraction completed: `validate_pubkey_like` and `validate_signature_like` moved from `main.rs` into shared module `crates/executor/src/key_validation.rs`, all call sites (`main.rs`, `send_rpc.rs`, `submit_transport.rs`) now consume the unified helper, and contract smoke now includes direct module guard coverage for valid key/signature shapes.
119. executor common contract validation extraction completed: `validate_common_contract` logic moved into shared module `crates/executor/src/common_contract.rs` (`validate_common_contract_inputs` + typed errors), while `main.rs` now maps module errors back to unchanged reject-code semantics (`contract_version_*`, `route_not_allowed`, `fastlane_not_enabled`, `invalid_side`, `invalid_token`, `invalid_notional_sol`, `notional_too_high`); contract smoke now includes direct module guard coverage for invalid-side branch.
120. executor startup route-allowlist extraction completed: `parse_route_allowlist` and `validate_fastlane_route_policy` moved from `main.rs` into dedicated module `crates/executor/src/route_allowlist.rs` (shared normalization + known-route contract + fastlane feature-gate), while startup behavior and error semantics remain unchanged; contract smoke now includes direct module guard coverage for unknown-route rejection.
121. executor RFC3339 parsing de-dup completed: shared helper `crates/executor/src/rfc3339_time.rs` now owns `parse_rfc3339_utc`, and duplicate parser implementations were removed from `main.rs` and `submit_response.rs`; contract smoke now includes direct helper coverage for valid timestamp parsing to guard against parser drift.
122. executor send-RPC topology guard de-dup completed: send-RPC primary/fallback chain validation (`fallback without primary` fail-closed invariant) moved into `crates/executor/src/route_backend.rs` via `send_rpc_endpoint_chain_checked`, while `send_rpc.rs` now consumes the shared checked-chain path and preserves existing reject semantics; contract smoke now includes direct module guard coverage for fallback-without-primary rejection.
123. executor auth-crypto helper extraction completed: HMAC signature hex builder and constant-time compare primitives (`compute_hmac_signature_hex`, `constant_time_eq`, `to_hex_lower`) moved from `main.rs` into shared module `crates/executor/src/auth_crypto.rs`, while auth-verifier behavior remains unchanged; contract smoke now includes direct module guard coverage for these primitives.
124. executor contract-version token validation extraction completed: `is_valid_contract_version_token` moved from `main.rs` into dedicated module `crates/executor/src/contract_version.rs` with unchanged token grammar (`[A-Za-z0-9._-]`), and contract smoke now includes direct module guard coverage for valid/invalid token shapes.
125. executor env parsing helper extraction completed: env readers/parsers (`non_empty_env`, `optional_non_empty_env`, `parse_u64_env`, `parse_f64_env`, `parse_bool_env`) moved from `main.rs` into `crates/executor/src/env_parsing.rs` with unchanged fail-closed semantics for missing/invalid env values; contract smoke now includes direct module guard coverage for boolean token parsing behavior.
126. executor secret-source helper extraction completed: secret resolution and file-permission checks (`resolve_secret_source`, `secret_file_has_restrictive_permissions`) moved from `main.rs` into `crates/executor/src/secret_source.rs` with unchanged conflict/file-read/empty-secret semantics and warning behavior for permissive file modes; contract smoke now includes guard coverage for inline+file conflict and trimmed file-secret resolution.
127. executor upstream-forwarding extraction completed: route backend forwarding logic (`forward_to_upstream`) moved from `main.rs` into dedicated module `crates/executor/src/upstream_forward.rs` with preserved retry/fallback semantics (`upstream_unavailable`, `upstream_request_failed`, `upstream_http_unavailable`, `upstream_http_rejected`, `upstream_invalid_json`) and unchanged submit-deadline per-hop timeout behavior; contract smoke now includes guard coverage for retryable fallback and terminal no-fallback branches.
128. executor signer-source extraction completed: signer source model and validation (`SignerSource`, `resolve_signer_source_config`, keypair file format/pubkey checks) moved from `main.rs` into shared module `crates/executor/src/signer_source.rs`, preserving fail-closed startup semantics for file/kms source constraints and keypair mismatch handling; existing contract smoke guard coverage for signer-source mismatch remains intact.
129. executor auth-mode guard extraction completed: startup auth requirement check (`require_authenticated_mode`) moved from `main.rs` into dedicated module `crates/executor/src/auth_mode.rs` with unchanged fail-closed behavior for missing bearer token when `COPYBOT_EXECUTOR_ALLOW_UNAUTHENTICATED=false`; existing contract smoke guard coverage remains anchored on `require_authenticated_mode_fails_closed_by_default`.
130. executor submit-budget extraction completed: submit-path budget helpers (`default_submit_total_budget_ms`, `min_claim_ttl_sec_for_submit_path`) and associated constants moved from `main.rs` into dedicated module `crates/executor/src/submit_budget.rs`, preserving request-timeout floor semantics and topology-aware claim-TTL math; existing guard tests remain stable for submit-budget and claim-ttl branches.
131. executor reject-mapping extraction completed: request/response error mapping logic (`map_*_to_reject`, `map_parsed_upstream_reject`) and reject payload helpers (`reject_to_json`, `simulate_http_status_for_reject`) moved from `main.rs` into shared module `crates/executor/src/reject_mapping.rs` with unchanged reject-code/detail semantics across contract, fee-hints, transport, and submit/simulate validation branches.
132. executor auth-verifier extraction completed: ingress auth verification logic (`AuthVerifier`, optional HMAC envelope checks, nonce replay cache, header requirements) moved from `main.rs` into dedicated module `crates/executor/src/auth_verifier.rs`, with unchanged bearer/hmac reject semantics and timestamp/nonce replay guards; contract smoke now includes direct bearer verify guard coverage (`auth_verifier_*` tests).
133. executor submit-claim RAII extraction completed: idempotency claim release guard (`SubmitClaimGuard`) moved from `main.rs` into dedicated module `crates/executor/src/submit_claim_guard.rs`, preserving owner-bound release logging semantics (`owner-match miss` warning vs `release error` warning) and unchanged submit-path in-flight lifecycle behavior.
134. executor request-validation extraction completed: shared module `crates/executor/src/request_validation.rs` now owns simulate/submit field guards (`action`, `dry_run`, `signal_ts`, `request_id`, `signal_id`, `client_order_id`), `main.rs` now uses the shared validators for both handlers, and reject mappings remain contract-equivalent via `map_request_validation_error_to_reject` (`invalid_action`, `invalid_dry_run`, `invalid_signal_ts`, `invalid_request_id`, `invalid_signal_id`, `invalid_client_order_id`).
135. executor request-type extraction completed: serde request DTOs (`SimulateRequest`, `SubmitRequest`, `ComputeBudgetRequest`) moved from `main.rs` into shared module `crates/executor/src/request_types.rs`, preserving endpoint deserialization behavior while reducing handler-module coupling; contract smoke now includes module guard coverage for required-field deserialization (`request_id`, `compute_budget`).
136. executor common-contract call-site de-dup completed: inline wrapper `validate_common_contract` removed from `main.rs`; `/simulate`, `/submit`, and fastlane gate test now call `validate_common_contract_inputs(CommonContractInputs { ... })` directly with existing reject mapping (`map_common_contract_validation_error_to_reject`), keeping reject-code semantics unchanged while reducing main-module indirection.
137. executor ingress helper extraction completed: duplicated ingress handling for `/simulate` and `/submit` (`auth verify -> reject_to_json`, `invalid_json` parse rejection) moved from `main.rs` into shared module `crates/executor/src/request_ingress.rs` (`verify_auth_or_reject`, `parse_json_or_reject`) with unchanged HTTP/JSON reject contract and added direct module guard coverage in contract smoke.
138. executor response-envelope extraction completed: duplicated endpoint-level result wrapping (`Result<Value, Reject> -> HTTP 200 JSON`) moved from `main.rs` into shared module `crates/executor/src/response_envelope.rs` (`success_or_reject_to_http`), preserving reject payload semantics including optional `client_order_id` propagation for `/submit`.
139. executor healthz-payload extraction completed: `/healthz` JSON assembly moved from `main.rs` into shared module `crates/executor/src/healthz_payload.rs` (`build_healthz_payload`, `top_level_healthz_status`) with unchanged field set/status semantics (`ok` vs `degraded`) and added direct module guard coverage in contract smoke.
140. executor healthz alias semantics documented in code: `routes` field is explicitly retained as backward-compat alias of `enabled_routes` for existing preflight/report consumers, and module test coverage now asserts alias equality (`healthz_payload_routes_alias_matches_enabled_routes`) to prevent accidental drift.
141. executor bind-address parsing extraction completed: `COPYBOT_EXECUTOR_BIND_ADDR` parsing moved from local `main.rs` helper into shared env-parsing module (`parse_socket_addr_str` in `crates/executor/src/env_parsing.rs`), with retained fail-closed error context and added direct guard coverage in contract smoke (`parse_socket_addr_str_rejects_invalid_socket_addr`).
142. executor import hygiene cleanup completed: `simulate_http_status_for_reject` import in `main.rs` is now test-scoped (`#[cfg(test)]`) since production call sites were already removed, reducing non-test surface while preserving existing guard test coverage.
143. executor contract-version parsing extraction completed: `COPYBOT_EXECUTOR_CONTRACT_VERSION` normalization/validation moved from `main.rs` into `crates/executor/src/contract_version.rs` (`parse_contract_version`), keeping the same fail-closed token constraints (`[A-Za-z0-9._-]`, len<=64, non-empty) while adding direct module guard coverage for invalid contract-version values.
144. executor auth-mode extraction continued: HMAC pair/TTL validation moved from `ExecutorConfig::from_env` into `crates/executor/src/auth_mode.rs` (`validate_hmac_auth_config`) so auth policy guards are centralized with `require_authenticated_mode`; fail-closed error messages/ranges remain unchanged and contract smoke now includes direct guard coverage for partial HMAC config.
145. executor request-validation extraction continued: per-field basic checks in handlers were composed into shared chain helpers (`validate_simulate_request_basics`, `validate_submit_request_identity`) inside `crates/executor/src/request_validation.rs`, preserving validation order and reject mapping while reducing duplicate call sequences in `/simulate` and `/submit` paths.
146. executor submit-verify payload extraction completed: serialization of `SubmitSignatureVerification` into JSON moved from `crates/executor/src/submit_verify.rs` into dedicated module `crates/executor/src/submit_verify_payload.rs` (`submit_signature_verification_to_json`) with unchanged payload shape for `skipped/seen/unseen` states and added direct guard coverage in contract smoke.
147. executor send-rpc defense-in-depth hardening added: `send_signed_transaction_via_rpc` now enforces `fallback send RPC URL requires primary send RPC URL` invariant locally before endpoint-chain resolution, preserving existing fail-closed reject contract (`adapter_send_rpc_not_configured`) while reducing reliance on upstream config-only guarantees.
148. executor send-rpc reject-message de-dup completed: duplicated `adapter_send_rpc_not_configured` construction for `fallback without primary` branches in `crates/executor/src/send_rpc.rs` was consolidated via shared local helper (`reject_send_rpc_fallback_without_primary`), preserving identical code/detail semantics while preventing message drift between invariant guards.
149. executor preflight hardening added for python dependency: `tools/executor_preflight.sh` now detects missing `python3` upfront and reports explicit fail-closed error (`python3 is required for executor_preflight URL/JSON helpers`) while helper functions degrade safely without abrupt command-not-found exits, preserving summary/diagnostic emission path.
150. executor submit-verify config extraction completed: submit-signature verification config model/builders (`SubmitSignatureVerifyConfig`, `parse_submit_signature_verify_config`, `build_submit_signature_verify_config`) moved from `crates/executor/src/submit_verify.rs` into dedicated module `crates/executor/src/submit_verify_config.rs`, while runtime visibility-check logic remains in `submit_verify.rs`; `main.rs` and `submit_budget.rs` now consume the shared config module with unchanged fail-closed behavior and existing guard tests.
151. executor reject-model extraction completed: shared reject envelope type (`Reject` with `terminal/retryable` constructors) moved from `crates/executor/src/main.rs` into dedicated module `crates/executor/src/reject.rs`, preserving existing reject semantics across handlers/modules and adding direct guard coverage (`reject_builders_set_retryable_flag`) in contract smoke.
152. executor simulate-handler extraction completed: `/simulate` business handler logic (`handle_simulate`) moved from `crates/executor/src/main.rs` into dedicated module `crates/executor/src/simulate_handler.rs`, preserving contract validation, upstream outcome classification, route normalization, and simulate success payload semantics while keeping ingress/auth/envelope wiring unchanged.
153. executor submit-handler extraction completed: `/submit` business handler logic (`handle_submit`) moved from `crates/executor/src/main.rs` into dedicated module `crates/executor/src/submit_handler.rs` with unchanged idempotency-claim flow, upstream forwarding/send-rpc path, submit-verify visibility checks, fee-hint resolution, and first-write-wins canonical response semantics.
154. executor request-endpoint extraction completed: HTTP ingress handlers for `/simulate` and `/submit` moved from `crates/executor/src/main.rs` into dedicated module `crates/executor/src/request_endpoints.rs` (auth verification, JSON decode, and response-envelope wiring), while router contract and handler semantics remain unchanged.
155. executor startup-config extraction completed: `ExecutorConfig::from_env()` environment parsing/validation logic moved from `crates/executor/src/main.rs` into dedicated module `crates/executor/src/executor_config_env.rs`, preserving fail-closed startup contract for routes/auth/secrets/timeouts/idempotency/submit-verify settings.
156. executor health endpoint extraction completed: `/healthz` handler moved from `crates/executor/src/main.rs` into dedicated module `crates/executor/src/healthz_endpoint.rs`, preserving runtime idempotency probe behavior and health payload schema while keeping router wiring unchanged.
157. executor phase-2A extraction baseline completed: `crates/executor/src/main.rs` runtime path now contains only bootstrap/router wiring (`main`) while HTTP endpoints, business handlers, startup config parsing, reject/envelope/validation/auth helpers, and transport/policy logic are modularized in dedicated files for phase-2B implementation slices.
158. executor phase-2A post-audit build fix applied: non-test binary build regression after extraction was resolved by restoring runtime imports for `/simulate` endpoint wiring and de-coupling submit handler from root-scope send-RPC import (`submit_handler` now imports `crate::send_rpc::send_signed_transaction_via_rpc` directly), with `cargo check -p copybot-executor` restored to PASS.
159. executor audit hardening fixes applied (new external review): claim-TTL derivation now enforces submit-budget floor (`submit_total_budget_ms`) in addition to hop topology estimates, idempotency store/lookup now normalizes `client_order_id` consistently via trimmed key path, HMAC verification now signs exact raw-body bytes (no UTF-8 lossy conversion), nonce is recorded only after successful signature validation with expiry aligned to accepted timestamp-skew window, and submit response identity (`client_order_id`/`request_id`) is validated before send-rpc/visibility steps to avoid post-submit idempotency gaps.
160. executor phase-2B route-adapter abstraction started: shared dispatcher module `crates/executor/src/route_executor.rs` now resolves deterministic route executor kind (`paper|rpc|jito|fastlane`) and routes `/simulate` + `/submit` upstream actions through a single execution boundary (`execute_route_action`), preserving current forwarding behavior while creating explicit per-route execution extension points for upcoming tx-build route backends; contract smoke guard pack now includes direct route-executor resolution coverage.
161. executor phase-2B route-executor lookup hardening: `execute_route_action` now normalizes route input internally before dispatch/backend lookup and uses normalized route keys end-to-end, eliminating false `route_not_allowed` on case/whitespace-variant route tokens when route-executor is reused outside current handlers; guard coverage now includes `route_executor_resolve_normalized_route_for_backend_lookup`.
162. executor phase-2B route-policy foundation hardening: shared normalized classifier `classify_normalized_route` added in `crates/executor/src/route_policy.rs` and consumed by route-executor dispatch so route-kind mapping is sourced from one normalized route-policy path; `execute_route_action` now performs a single normalization cycle before kind resolution, and contract smoke includes direct guard coverage for normalized-route classification.
163. executor phase-2B route-adapter primitives introduced: dedicated module `crates/executor/src/route_adapters.rs` now defines explicit adapter types (`PaperRouteExecutor`, `RpcRouteExecutor`, `JitoRouteExecutor`, `FastlaneRouteExecutor`) and typed selector `RouteAdapter::from_kind`, while `execute_route_action` dispatches through this adapter layer instead of per-route inline functions; behavior remains forwarding-equivalent today, but route-specific execution specialization can now be added per adapter without touching endpoint handlers.
164. executor phase-2B route-adapter action hooks enabled: each adapter now has explicit `simulate`/`submit` entrypoints (instead of one shared pass-through), and `RpcRouteExecutor` adds defense-in-depth submit guard `validate_rpc_submit_tip_payload` that fail-closes on non-zero `tip_lamports` at adapter boundary (`tip_not_supported`) to prevent drift if upstream callers bypass earlier submit-policy normalization; contract smoke includes guard coverage for this route-adapter check.
165. executor phase-2B route-adapter payload contract hardening: submit adapters now validate payload route echo at adapter boundary (`validate_submit_payload_for_route`) before forwarding, and RPC tip guard coverage is expanded to invalid-JSON and non-object payload branches; this closes the previous low test-gap and adds fail-closed protection against submit payload route drift (`invalid_request_body`) inside the adapter layer.
166. executor phase-2B route-adapter guard coverage expanded: added missing route-validator tests for `route missing` and `route non-string` branches, plus integration guard test `handle_submit_rejects_route_payload_mismatch_before_forward` to assert adapter-boundary route mismatch is rejected before any upstream forwarding attempt.
167. executor phase-2B simulate adapter-boundary guard hardening completed: all route adapters now validate simulate payload route echo before forwarding (`validate_simulate_payload_for_route`), and guard coverage now includes unit tests for mismatch/missing/non-string route branches plus integration test `handle_simulate_rejects_route_payload_mismatch_before_forward` to prove fail-closed behavior before upstream network I/O.
168. executor phase-2B route-adapter guard de-dup completed: submit/simulate payload route validation now uses a shared action-aware helper (`validate_payload_route_for_action` + `parse_payload_object_for_action`) to remove duplicated parsing/route-check logic and prevent future drift in behavior/error-text between actions; simulate guard coverage also includes explicit case-insensitive happy-path assertion.
169. executor phase-2B adapter-boundary action guard hardening completed: route adapters now enforce action semantics at payload boundary (`simulate` requires `action=simulate`; `submit` enforces `action=submit` when field is present) with fail-closed `invalid_request_body` on mismatch, and coverage now includes unit + integration guards proving action-mismatch rejection before upstream forwarding.
170. executor phase-2B action-guard branch coverage expanded: submit adapter boundary now has explicit tests for non-string `action` reject and case-insensitive `action=submit` accept paths, plus integration guard `handle_submit_rejects_non_string_action_payload_before_forward` to assert fail-closed rejection before any upstream forwarding.
171. executor phase-2B adapter-boundary contract-version guard hardening completed: submit/simulate route-adapter validators now require payload `contract_version` to be present, string, non-empty, and equal to executor contract (`v1`) before forwarding; guard coverage includes unit tests for missing/mismatch paths on both actions and integration tests proving contract-version mismatch is rejected pre-forward.
172. executor phase-2B contract-version guard coverage expanded: adapter-boundary validators now have explicit branch tests for `contract_version` non-string and non-empty reject paths (submit + simulate), and integration guards confirm both classes reject pre-forward (`handle_simulate_rejects_non_string_contract_version_payload_before_forward`, `handle_submit_rejects_empty_contract_version_payload_before_forward`).
173. executor phase-2B adapter-boundary identity guard hardening added: submit/simulate validators now enforce optional raw-payload identity fields (`request_id`, `signal_id`, `client_order_id`) as non-empty strings when present, with branch tests for non-string/empty cases and integration guards proving malformed identity fields are rejected pre-forward (`handle_simulate_rejects_non_string_request_id_payload_before_forward`, `handle_submit_rejects_empty_client_order_id_payload_before_forward`).
174. executor phase-2B identity guard branch coverage expanded: added explicit coverage for the previously uncovered helper paths (`submit.signal_id` empty and `simulate.request_id` empty) at both unit and integration levels, proving these malformed raw payloads fail-closed before upstream forwarding (`handle_submit_rejects_empty_signal_id_payload_before_forward`, `handle_simulate_rejects_empty_request_id_payload_before_forward`).
175. executor phase-2B adapter-boundary identity consistency hardening added: when optional identity fields are present in raw payload (`request_id`, `signal_id`, `client_order_id`), validators now enforce both shape (string/non-empty) and equality against request-level expected values, with unit/integration coverage for `request_id`/`signal_id` mismatch branches proving pre-forward fail-closed behavior.
176. executor phase-2B identity mismatch coverage completed: explicit tests now cover all expected identity match branches across submit/simulate (`submit.signal_id`, `submit.client_order_id`, `simulate.request_id` mismatch), including integration pre-forward guards to prove malformed/mismatched raw identity payloads are rejected before upstream I/O.
177. executor phase-2B side/token identity guard coverage expanded: adapter-boundary expected-value checks for `side` and `token` now have dedicated submit+simulate unit branches and integration pre-forward guards (`handle_{simulate,submit}_rejects_{side,token}_payload_mismatch_before_forward`) to prove route-adapter fail-closed behavior before any upstream I/O.
178. executor phase-2B adapter-boundary identity strictness hardened: when request-level expected values for `side`/`token` are present, missing raw payload fields now fail-closed (`invalid_request_body`, `missing <field> at route-adapter boundary`) instead of silently passing; unit and integration pre-forward coverage now includes missing-`side` and missing-`token` branches for both `/simulate` and `/submit`.
179. executor phase-2B idempotency claim cleanup optimization: global stale-claim DELETE is now cadence-throttled (TTL-derived interval bounds) instead of running on every submit claim attempt, while preserving stale recovery via same-key stale-claim reclaim+single retry on claim insert conflict; guard coverage includes interval logic and reclaimed-stale-key behavior under throttled cleanup.
180. executor phase-2B test-structure cleanup continued: `submit_budget` guard tests (`min_claim_ttl_sec_for_submit_path_*`) were moved from `crates/executor/src/main.rs` into module-local `crates/executor/src/submit_budget.rs` to reduce main-test sprawl while preserving test names and contract smoke coverage.
181. executor phase-2B test-structure cleanup continued: duplicated route-allowlist tests were removed from `crates/executor/src/main.rs` and consolidated in `crates/executor/src/route_allowlist.rs` with compatibility test names (`parse_route_allowlist_rejects_unknown_route`, `validate_fastlane_route_policy_enforces_feature_gate`) preserved to keep contract smoke guard pack unchanged.
182. executor phase-2B adapter-boundary identity coverage expanded for strict-missing branches: when expected request identity exists, missing raw payload fields now have explicit submit/simulate unit and integration pre-forward guards for `request_id`, `signal_id`, and `client_order_id`, making the broader strictness (beyond side/token) explicit and regression-proof.
183. executor phase-2B idempotency cleanup cadence hardened for multi-process topology: claim-cleanup throttle now consults shared SQLite metadata (`executor_runtime_meta.claim_cleanup_last_unix`) before running global stale-claim DELETE, reducing redundant cross-process cleanup churn while preserving same-key stale reclaim fallback when insert conflicts on stale claims.
184. executor phase-2B test-structure cleanup continued: `require_authenticated_mode_fails_closed_by_default` was moved from `crates/executor/src/main.rs` into module-local `crates/executor/src/auth_mode.rs` with the same guard test name, reducing main-test surface while preserving contract smoke compatibility.
185. executor phase-2B route-adapter dispatch de-dup completed: duplicated per-route `simulate`/`submit` execution paths were collapsed into a single adapter execution flow in `crates/executor/src/route_adapters.rs` with route-specific hooking preserved (`Rpc` keeps non-zero tip defense guard via `validate_rpc_submit_tip_payload`), reducing drift risk while keeping adapter-boundary contract rejects unchanged; contract smoke now includes direct guard coverage for RPC-only tip-guard selection (`route_adapter_rpc_tip_guard_applies_only_to_rpc_submit`).
186. executor phase-2B auth hardening continued: HMAC nonce replay cache now enforces configurable capacity (`COPYBOT_EXECUTOR_HMAC_NONCE_CACHE_MAX_ENTRIES`, default `100000`, fail-closed on zero/misconfig), and verification returns retryable overflow reject (`hmac_replay_cache_overflow`) when nonce cache is saturated; ingress/runbook/contract smoke were updated with dedicated guard coverage (`auth_verifier_hmac_rejects_when_nonce_cache_capacity_reached`).
187. executor phase-2B auth replay-cache coverage expanded: added explicit guard test proving expired HMAC nonce entries are evicted before capacity checks (`auth_verifier_hmac_evicts_expired_nonce_before_capacity_check`), preventing false-positive overflow after TTL expiry and hardening regression visibility in contract smoke.
188. executor runtime shutdown hardening: server now uses graceful shutdown on process signals (`SIGINT`/`SIGTERM`) via `axum::serve(...).with_graceful_shutdown(shutdown_signal())`, reducing abrupt in-flight termination risk during systemd restarts/rollouts while preserving fail-closed startup behavior.
189. executor test-runtime optimization: HMAC skew-window replay guard test duration reduced (`ttl=1`, shorter wait), and nonce-eviction capacity guard switched to deterministic pre-seeded expired-entry setup (no sleep), keeping semantics unchanged while speeding up quick/standard audit loops.
190. executor shutdown/nonce guard coverage hardened: signal wait logic was extracted into testable helpers (`await_shutdown_signal_ctrl_c_only`, unix `await_shutdown_signal_unix`) with dedicated async guard tests, and forward-skew HMAC replay test was made deterministic (no sleep) by asserting nonce expiry horizon directly before replay check.
191. executor auth-verifier testclock hardening: HMAC verify path now has internal deterministic test clock hook (`verify_with_now_epoch`) used by guard tests, restoring explicit “time progression within forward-skew window” coverage in `auth_verifier_hmac_keeps_nonce_through_forward_skew_window` without reintroducing sleep-based flakiness.
192. executor idempotency retention hardening: submit-response cache now has configurable retention (`COPYBOT_EXECUTOR_IDEMPOTENCY_RESPONSE_RETENTION_SEC`, default `604800`) with cadence-throttled cleanup coordinated via shared SQLite runtime metadata (`response_cleanup_last_unix`), reducing unbounded growth of `executor_submit_idempotency` while preserving claim-path semantics; guard coverage includes cleanup interval bounds and stale-response eviction.
193. executor idempotency hot-path contention hardening: stale cached-response retention cleanup was moved out of submit claim/load path into a dedicated background worker (`idempotency_cleanup_worker`), so `/submit` hot path no longer runs response DELETE operations; cleanup remains cadence-throttled via shared SQLite runtime metadata and is now triggered asynchronously with graceful shutdown cancellation, with new guard coverage for worker tick bounds.
194. executor idempotency worker burst-hardening: response retention cleanup now uses bounded batched DELETE per run (`rowid` subquery with indexed `updated_at_utc`) to cap lock duration under large stale backlogs, and worker startup now delays first tick by one interval (`interval_at`) to avoid immediate cold-start cleanup contention.
195. executor idempotency retention-SLA hardening: response cleanup metadata marker (`response_cleanup_last_unix`) is now advanced only when a cleanup run fully drains stale rows below batch cap; when per-run batch limit is hit, marker is intentionally not advanced so subsequent worker ticks continue draining backlog immediately instead of deferring for full cleanup interval.
196. executor idempotency exact-cap completion hardening: batched response cleanup now performs post-limit stale probe (`SELECT 1 ... LIMIT 1`) when deletes hit the per-run batch cap exactly, so fully drained exact-cap runs still mark cleanup complete and advance marker in the same tick; guard coverage includes exact-limit marker advancement test.
197. executor idempotency cleanup tuning surfaced to ops config: response cleanup batch controls are now configurable via env (`COPYBOT_EXECUTOR_IDEMPOTENCY_RESPONSE_CLEANUP_BATCH_SIZE`, `COPYBOT_EXECUTOR_IDEMPOTENCY_RESPONSE_CLEANUP_MAX_BATCHES_PER_RUN`) with fail-closed range validation at startup (including i64/usize conversion bounds), enabling runtime capacity tuning without code changes.
198. executor idempotency cleanup ops-safety hardening: startup validation now fail-closes configurations where `COPYBOT_EXECUTOR_IDEMPOTENCY_RESPONSE_CLEANUP_BATCH_SIZE * COPYBOT_EXECUTOR_IDEMPOTENCY_RESPONSE_CLEANUP_MAX_BATCHES_PER_RUN` exceeds bounded per-run rows budget (`<=200000`), preventing accidental long-lock cleanup runs under misconfigured extreme throughput settings.
199. executor idempotency cleanup cadence tuning surfaced: response cleanup worker tick is now explicitly configurable via env (`COPYBOT_EXECUTOR_IDEMPOTENCY_RESPONSE_CLEANUP_WORKER_TICK_SEC`) with fail-closed bounds validation (`15..=300` seconds) while retaining retention-derived default calculation, enabling ops to tune cleanup responsiveness independently from retention horizon.
200. executor cleanup tick wiring coverage added: `ExecutorConfig::from_env` now has explicit env-to-config guard test for `COPYBOT_EXECUTOR_IDEMPOTENCY_RESPONSE_CLEANUP_WORKER_TICK_SEC` (`executor_config_from_env_wires_response_cleanup_worker_tick_override`), including precedence check that explicit tick override wins over retention-derived default; test is included in `tools/executor_contract_smoke_test.sh`.
201. executor env-isolation test harness hardened: `with_clean_executor_env` now clears all current `COPYBOT_EXECUTOR_*` keys after scoped test execution before restoring snapshot, preventing leakage of newly created env vars across tests; regression guard `with_clean_executor_env_removes_newly_added_keys_after_scope` added and wired into contract smoke.
202. executor cleanup worker contention hardening: background response-retention cleanup now uses non-blocking idempotency lock acquisition (`run_response_cleanup_if_due_nonblocking`) and skips busy ticks instead of blocking submit-path lock acquisition, reducing latency-spike risk under load; regression guard `response_cleanup_nonblocking_skips_when_mutex_is_busy` added and wired into contract smoke.
203. executor cleanup cadence SLA hardening: startup now fail-closes configs where `COPYBOT_EXECUTOR_IDEMPOTENCY_RESPONSE_CLEANUP_WORKER_TICK_SEC` is slower than retention-derived cleanup interval (`<= retention_interval_sec`), preventing stale-response retention drift from misconfigured worker tick; guard `validate_response_cleanup_worker_cadence_rejects_tick_slower_than_cleanup_interval` added and wired into contract smoke.
204. executor cleanup worker anti-herd hardening: first background cleanup tick now uses deterministic per-process jitter (`1..=tick_sec`) derived from time+pid seed instead of fixed full-tick delay, reducing synchronized multi-instance cleanup bursts and SQLite lock collisions; guard `response_cleanup_worker_initial_delay_sec_from_seed_is_within_tick_bounds` added and wired into contract smoke.
205. executor cleanup worker async-runtime hardening: background cleanup tick now executes SQLite cleanup call through `tokio::task::spawn_blocking` and handles join failures explicitly, preventing blocking DB work from occupying core async runtime worker threads during cleanup ticks.
206. executor cleanup metadata skew hardening: global claim/response cleanup markers loaded from shared SQLite metadata are now clamped to local `now_unix` before cadence gating, preventing future-dated markers (clock skew across instances) from suppressing cleanup for extended periods; guards `clamp_cleanup_marker_to_now_limits_future_marker`, `claim_cleanup_clamps_future_global_marker_to_now`, and `response_cleanup_clamps_future_global_marker_to_now` added and wired into contract smoke.
207. executor contract smoke runtime optimized: guard-pack verification now defaults to single-suite execution (`cargo test -p copybot-executor -q`) plus exact test-name presence checks from `cargo test -- --list`, eliminating hundreds of redundant per-test invocations while preserving coverage guarantees; strict legacy per-test execution remains available via `EXECUTOR_CONTRACT_SMOKE_RUN_EACH_GUARD=true`.
208. executor cleanup observability hardening: response-retention cleanup now emits explicit debug signal when a run hits per-run delete cap and intentionally does not advance cleanup marker, improving operator visibility into sustained stale-backlog pressure without changing cleanup semantics.
209. executor phase-2B ATA-fee-hint semantics hardened: `ata_create_rent_lamports` is now normalized as absent when upstream returns `0`, and `/submit` success payload omits the field entirely unless ATA rent hint is actually present (`>0`), aligning contract behavior with “ATA hint only when ATA-create path is used” and adding dedicated `ata_*` guard coverage.
210. executor phase-2B fee-hint parser consistency hardened: `parse_response_fee_hint_fields` now applies the same ATA normalization (`ata_create_rent_lamports=0 -> absent`) as resolver path, eliminating potential future semantic drift between parser-only and resolved fee-hint consumers; guard `ata_parse_response_fee_hint_fields_normalizes_zero_to_absent` added to contract smoke.
211. executor phase-2B tx-build core consolidation started: submit-path build primitives are now composed via shared `tx_build::build_submit_plan` (slippage policy, tip policy, compute-budget bounds, forward payload rewrite) and consumed directly by `submit_handler`, reducing handler-level build drift and establishing a single route-aware tx-build planner API for upcoming instruction/ATA composition work.
212. executor phase-2B instruction-composition primitives added: `tx_build` now emits explicit `SubmitInstructionPlan` (compute budget + optional tip instruction) as part of submit build planning, and `submit_handler` logs this normalized instruction plan before route execution; guard coverage includes `tx_build_instruction_plan_omits_tip_when_zero`.
213. executor phase-2B planner observability and positive-tip coverage hardened: submit handler now logs `tip_instruction_lamports` as true `Option` (plus explicit presence flag) to avoid ambiguous `0` logging, and tx-build coverage now includes integrated positive-tip planner scenario (`jito`) proving `build_submit_plan` preserves tip in both payload and instruction plan (`Some(...)`) without RPC-style coercion.
214. executor phase-2B route-adapter context wiring added: `execute_route_action` now accepts explicit `RouteSubmitExecutionContext` carrying optional `SubmitInstructionPlan`, and submit-path wiring passes this plan through `route_executor -> route_adapters` boundary (with adapter-side debug visibility) without changing upstream forwarding semantics; this establishes a typed handoff point for upcoming route-specific instruction/ATA execution logic.
215. executor phase-2B typed submit-context wiring guard completed: route-adapter test hook now captures submit-context instruction-plan presence per `client_order_id`, and integration guard `handle_submit_wires_instruction_plan_presence_into_route_adapter_context` proves submit-path passes `RouteSubmitExecutionContext { instruction_plan: Some(...) }` through the route layer (preparing future adapter-side instruction execution without silent default-context regressions).
216. executor phase-2B route-adapter context consistency hardening added: submit adapter now validates typed `SubmitInstructionPlan` against outbound submit payload (`tip_lamports`, `compute_budget.cu_limit`, `compute_budget.cu_price_micro_lamports`) before forwarding, fail-closing on mismatch (`invalid_request_body`) to prevent silent drift between tx-build planner and raw forward body; coverage includes direct unit mismatch guards plus integration pre-forward rejection (`handle_submit_rejects_instruction_plan_payload_mismatch_before_forward`).
217. executor phase-2B submit-context guard strictness completed: submit route-adapter now fail-closes when `instruction_plan` is missing (`invalid_request_body`) to prevent silent fallback to unguarded forwarding, and context-consistency coverage now explicitly includes `compute_budget.cu_price_micro_lamports` mismatch at unit+integration levels (`validate_submit_instruction_plan_payload_consistency_rejects_cu_price_mismatch`, `handle_submit_rejects_instruction_plan_cu_price_payload_mismatch_before_forward`).
218. executor phase-2B route-layer fastlane gate hardening added: `execute_route_action` now enforces fastlane feature-flag policy (`fastlane_not_enabled`) as defense-in-depth before adapter forwarding, so direct route-layer calls cannot bypass `submit_fastlane_enabled=false`; coverage includes unit gate guard (`route_executor_feature_gate_rejects_fastlane_when_disabled`) and integration pre-forward rejection (`execute_route_action_rejects_fastlane_when_feature_disabled_before_forward`).
219. executor phase-2B fastlane gate coverage completed for submit-variant route execution: added explicit integration pre-forward guard (`execute_route_action_rejects_fastlane_submit_when_feature_disabled_before_forward`) so both simulate and submit actions are covered for route-layer fastlane feature gating.
220. executor phase-2B route-executor context contract hardened: route layer now enforces action/context invariants before adapter dispatch (`submit` requires `instruction_plan`; `simulate` must not carry it), preventing cross-action context misuse from reaching adapters; coverage includes unit guards (`route_executor_action_context_rejects_*`) and integration pre-forward reject for simulate-with-plan (`execute_route_action_rejects_simulate_with_submit_instruction_plan_before_forward`).
221. executor phase-2B route-layer allowlist hardening added: `execute_route_action` now enforces executor route allowlist before adapter dispatch, preventing direct route-layer invocation from bypassing allowlist policy when backend wiring exists for non-allowlisted routes; coverage includes unit guard (`route_executor_allowlist_rejects_route_not_in_allowlist`) and integration pre-forward reject (`execute_route_action_rejects_route_not_in_allowlist_before_forward`).
222. executor phase-2B allowlist coverage expanded to submit-variant route execution: added explicit integration pre-forward guard (`execute_route_action_rejects_submit_route_not_in_allowlist_before_forward`) to lock in `route_not_allowed` priority for submit path when route is known/configured but absent from allowlist.
223. executor phase-2B route-layer backend-presence hardening added: `execute_route_action` now rejects allowlisted routes that have no configured backend (`route_not_allowed`, `route=<...> not configured`) before adapter/context checks, preventing direct route-layer invocation from reaching action-specific guards when backend topology is invalid; coverage includes unit guard (`route_executor_backend_configured_rejects_missing_backend`) plus simulate/submit integration pre-forward rejects.
224. executor phase-2B route-layer payload-route binding hardening added: `execute_route_action` now requires and validates route payload hint (`RouteActionPayloadExpectations.route_hint`) against normalized route argument before allowlist/backend/action checks, fail-closing malformed direct route-layer calls with `invalid_request_body`; coverage includes unit guard (`route_executor_payload_route_expectation_rejects_mismatch`) and submit integration pre-forward reject (`execute_route_action_rejects_submit_route_payload_hint_mismatch_before_forward`).
225. executor phase-2B route-layer payload-route coverage expanded: added explicit simulate integration pre-forward guards for both route-hint mismatch and missing-hint branches (`execute_route_action_rejects_simulate_route_payload_hint_mismatch_before_forward`, `execute_route_action_rejects_simulate_route_payload_hint_missing_before_forward`), closing the route-executor integration gap beyond submit-only coverage.
226. executor phase-2B route-layer payload-route coverage matrix completed: added submit integration pre-forward missing-hint guard (`execute_route_action_rejects_submit_route_payload_hint_missing_before_forward`), making mismatch+missing branches explicit for both `simulate` and `submit` at route-executor boundary.
227. executor phase-2B route-layer guard-ordering coverage hardened: added integration pre-forward priority tests proving payload-route guard fires before downstream policy gates (`execute_route_action_rejects_route_hint_mismatch_before_allowlist_check`, `execute_route_action_rejects_route_hint_missing_before_backend_check`), preventing future reorder regressions from masking malformed route-hint payloads behind `route_not_allowed`.
228. executor phase-2B route-layer payload-expectation shape hardening added: `execute_route_action` now fail-closes malformed action expectations before adapter dispatch (`submit` requires `client_order_id` expectation; `simulate` must not include it, and shared expectations for `request_id/signal_id/side/token` are required), with unit and integration pre-forward guards (`route_executor_payload_expectations_shape_*`, `execute_route_action_rejects_{submit_missing_client_order_expectation,simulate_with_client_order_expectation}_before_forward`).
229. executor phase-2B payload-expectation shape branch coverage expanded: added table-driven unit guard `route_executor_payload_expectations_shape_rejects_missing_shared_fields` to explicitly lock reject branches/messages for missing shared route-layer expectations (`request_id`, `signal_id`, `side`, `token`) and registered it in contract smoke.
230. executor phase-2B payload-expectation non-empty hardening added: route-layer shape guard now rejects empty/whitespace expectation values (shared fields plus `submit.client_order_id`) and coverage now includes table-driven unit reject branches for empty expectations plus integration pre-forward rejects for `submit.token` empty and `simulate.request_id` empty expectations.
231. executor phase-2B route-layer guard-ordering hardened for expectation shape: malformed payload expectations are now validated before policy gates (`allowlist/backend/feature`) in `execute_route_action`, with integration guards proving `invalid_request_body` (shape) has priority over `route_not_allowed`/`fastlane_not_enabled` (`execute_route_action_rejects_payload_shape_before_allowlist_check`, `execute_route_action_rejects_payload_shape_before_fastlane_feature_gate`).
232. executor phase-2B submit-budget boundary hardening at route layer: `execute_route_action` now enforces deadline context invariants (`submit` with instruction plan requires `submit_deadline`; `simulate` must not carry submit deadline) before adapter dispatch, preventing direct route-layer invocations from bypassing deadline budget enforcement; unit+integration pre-forward guards cover both reject branches.
233. executor phase-2B deadline-context guard-ordering hardened: route-layer deadline invariants are now validated before policy gates (`allowlist/backend/feature`) so malformed internal submit-context wiring fails deterministically as `invalid_request_body` instead of being masked by route policy errors; integration guards prove priority over allowlist and fastlane feature-gate rejects (`execute_route_action_rejects_deadline_context_before_allowlist_check`, `execute_route_action_rejects_deadline_context_before_fastlane_feature_gate`).
234. executor phase-2B upstream-forward deadline boundary hardened: `forward_to_upstream` now enforces deadline-context invariants directly (`submit` requires deadline, `simulate` forbids deadline) as defense-in-depth below route layer, preventing budget-control bypass if `forward_to_upstream` is called outside expected route-executor path; unit and integration pre-request guards lock both reject branches, and existing submit-forward fallback tests now pass explicit deadlines.
235. executor phase-2B send-rpc deadline boundary hardened: `send_signed_transaction_via_rpc` now fails closed when invoked without submit deadline (`invalid_request_body` at send-rpc boundary) before topology/backend/request checks, preserving submit latency-budget invariants even for direct helper usage; unit guard coverage added in `send_rpc.rs`, integration pre-request priority coverage added in `main.rs`, and existing send-rpc integration tests now pass explicit deadlines.
236. executor phase-2B submit-verify deadline boundary hardened: `verify_submitted_signature_visibility` now fails closed without submit deadline (`invalid_request_body` at submit-verify boundary) before verify-config/request logic, keeping submit latency-budget invariants consistent across all submit transport stages (`upstream_forward` -> `send_rpc` -> `submit_verify`); added unit guard coverage in `submit_verify.rs`, integration pre-request/config-priority coverage in `main.rs`, and updated verification integration tests to pass explicit deadlines.
237. executor phase-2B submit-verify signature boundary hardened: `verify_submitted_signature_visibility` now validates `tx_signature` format at submit-verify boundary and fails closed with `invalid_request_body` for malformed signatures before config/request logic, adding defense-in-depth for direct helper usage outside the normal submit transport parser; added unit guard coverage in `submit_verify.rs`, integration pre-request/config-priority coverage in `main.rs`, and registered new guards in contract smoke.
238. executor phase-2B submit-transport artifact consistency hardened: submit success payload now enforces exactly-one transport artifact (`tx_signature` XOR `signed_tx_base64`) at extraction boundary, fail-closing conflicting dual-artifact responses as `submit_adapter_invalid_response` before downstream transport path selection; added unit guard coverage in `submit_transport.rs`, integration reject coverage in `main.rs`, and registered new guards in contract smoke.
239. executor phase-2B submit-transport type boundary hardened: submit transport extraction now rejects non-string `tx_signature` / `signed_tx_base64` values (when present) as explicit `submit_adapter_invalid_response` instead of collapsing them into generic missing-artifact path, improving fail-closed precision for malformed adapter payloads; added unit coverage in `submit_transport.rs`, integration rejects in `main.rs`, and smoke guard registrations.
240. executor phase-2B submit-response echo type boundary hardened: optional upstream echo fields in submit response (`route`, `contract_version`, `client_order_id`, `request_id`) now fail closed when present as non-string/empty values (instead of being silently ignored), mapping to terminal `submit_adapter_invalid_response` with precise field-level details; added unit coverage in `submit_response.rs`, integration rejects in `main.rs`, and smoke guard registrations.
241. executor phase-2B submit-response null-boundary follow-up: optional submit-response echo fields now treat explicit `null` as invalid-present (non-empty string required when field key exists), closing fail-open gap where `null` was previously accepted as absent; added unit guards for `route/contract_version/client_order_id/request_id = null`, integration reject for `request_id = null`, and smoke registrations.
242. executor phase-2B simulate-response echo type/null boundary hardened: optional upstream echo fields in simulate response (`route`, `contract_version`) now fail closed whenever present as non-string/empty/null values (non-empty string required when key exists), mapping to terminal `simulation_invalid_response`; added strict parsing helper coverage in `simulate_response.rs`, integration rejects in `main.rs` (`contract_version` non-string and `route` null), and smoke guard registrations.
243. executor phase-2B simulate-response detail type boundary hardened: simulate success `detail` now follows the same optional-non-empty-string contract (missing uses default detail, but present `non-string|empty|null` is terminal `simulation_invalid_response`), preventing malformed upstream detail from silently collapsing into default text; added unit guards for detail branches, integration pre-downstream reject (`detail` non-string), and smoke registrations.
244. executor phase-2B upstream-reject echo type boundary hardened: `parse_upstream_outcome` now enforces reject `code`/`detail` as non-empty strings when keys are present (instead of silently falling back), fail-closing malformed reject payloads as terminal `upstream_invalid_response`; added unit coverage for invalid `code/detail` types and integration reject propagation via `handle_simulate`.
245. executor phase-2B upstream-reject retryable type boundary hardened: `parse_upstream_outcome` now treats `retryable` as strict optional-boolean (`present => bool`), fail-closing malformed reject payloads that previously degraded to `retryable=false` silently; invalid `retryable` now maps to terminal `upstream_invalid_response`, with new unit coverage and simulate-path integration reject coverage.
246. executor phase-2B upstream-reject retryable-null coverage hardened: added explicit null-branch guards proving `retryable: null` is fail-closed (unit + simulate integration) and cannot silently degrade to default retryability; contract smoke updated to pin this branch.
247. executor phase-2B upstream-status type boundary hardened: `parse_upstream_outcome` now treats `status` as strict optional non-empty string (`present => non-empty string`) and fail-closes malformed status values (`non-string|empty`) as terminal `upstream_invalid_response` before status-domain classification; added unit coverage for status type/empty branches and simulate-path integration reject coverage.
248. executor phase-2B upstream success-flag type boundary hardened: `parse_upstream_outcome` now validates `ok` and `accepted` as strict optional booleans (`present => bool`) and fail-closes malformed success flags (`non-bool|null`) as terminal `upstream_invalid_response` before outcome classification, preventing malformed truthy/falsey payloads from being silently coerced; added unit coverage for `ok/accepted` invalid branches and simulate-path integration reject coverage.
249. executor phase-2B accepted-flag integration coverage closed: added explicit simulate-path integration guard for malformed upstream `accepted` (`handle_simulate_rejects_upstream_accepted_type_invalid`) to complement existing unit coverage and prevent regression where only `ok` invalid branch was covered at integration level.
250. executor phase-2B upstream outcome-conflict boundary hardened: `parse_upstream_outcome` now fail-closes contradictory status/flag combinations (`status=ok` with reject flags or `status=reject` with success flags) as terminal `upstream_invalid_response` before reject/success classification, preventing ambiguous upstream envelopes from being interpreted as valid business rejects; added unit coverage for both conflict branches plus simulate-path integration reject coverage.
251. executor phase-2B upstream conflict-matrix completion and missing-status hardening: closed remaining integration gap for `status=reject + success flag` conflict, and added new fail-closed guard for contradictory `ok/accepted` flags when `status` is absent (`ok != accepted`), with unit + simulate integration coverage and smoke registration for all new branches.
252. executor phase-2B fee-hint null boundary hardened: submit response fee-hint parser now treats `null` as invalid-present (instead of silently absent) for all optional fee fields, fail-closing malformed upstream fee-hint payloads as terminal `submit_adapter_invalid_response`; added unit coverage for null branches (`network_fee_lamports`, `ata_create_rent_lamports`), integration reject coverage in submit path, and smoke registrations.
253. executor phase-2B upstream-status priority hardening: unknown non-empty upstream `status` is now classified as terminal `upstream_invalid_status` before reject/success envelope parsing, even when `ok/accepted` reject flags are present, eliminating prior path where unknown status could be masked as a generic business reject; added unit and simulate integration coverage for `status=unknown + reject flags`, plus smoke registration.
254. executor phase-2B submit-transport non-empty boundary hardened: upstream submit transport artifacts (`tx_signature`, `signed_tx_base64`) now require non-empty string when present (`null`/empty rejected as invalid-present) instead of being silently treated as missing; reject mapping updated to explicit non-empty wording, with new unit + submit integration coverage and smoke registrations for null/empty transport cases.
255. executor phase-2B unknown-status precedence matrix completed: `parse_upstream_outcome` now explicitly proves that unknown non-empty `status` remains terminal `upstream_invalid_status` even when `ok/accepted` are malformed non-bool values, preventing type-boundary rejects from masking status-domain classification; added unit coverage for `status=unknown + invalid ok|accepted`, simulate integration rejects for both variants, and smoke registrations.
256. executor phase-2B submit-path unknown-status precedence coverage completed: added submit integration guards proving unknown non-empty upstream `status` still returns terminal `upstream_invalid_status` when `ok` or `accepted` are malformed non-bool values, pinning parity with simulate path and preventing future regressions where submit could drift to `upstream_invalid_response` on mixed malformed envelopes.
257. executor phase-2B unknown-status precedence over reject-envelope type validation completed: added unit + simulate + submit guards proving unknown non-empty upstream `status` still resolves to terminal `upstream_invalid_status` even when reject-envelope `retryable` is malformed, preventing deeper reject-field type errors from masking status-domain classification.
258. executor phase-2B unknown-status precedence over reject `code/detail` type validation completed: added unit + simulate + submit guards proving malformed reject metadata (`code` non-string, `detail` null) cannot mask unknown non-empty upstream `status`; classification now stays terminal `upstream_invalid_status` across reject-envelope metadata branches.
259. executor phase-2B simulate route-adapter dry_run boundary hardened: simulate payload validation at route-adapter boundary now requires `dry_run=true` (missing/non-bool/false reject as `invalid_request_body`) to enforce parity with typed request validation and prevent raw-body drift from bypassing simulate intent; added unit coverage for all dry_run branches, integration pre-forward mismatch coverage, and smoke registrations.
260. executor phase-2B submit slippage payload-consistency guard added: route-adapter submit boundary now enforces `slippage_bps` and `route_slippage_cap_bps` consistency between typed request context and forwarded raw payload (missing/non-numeric/mismatch => terminal `invalid_request_body`), preventing raw-body drift from bypassing submit risk checks; added unit coverage for slippage consistency branches and integration pre-forward mismatch rejection.
261. executor phase-2B submit slippage-cap coverage matrix completed: added missing/non-numeric `route_slippage_cap_bps` unit guards and a submit integration pre-forward mismatch reject for route slippage cap, closing the remaining low-gap so both slippage fields now have symmetric boundary coverage across type, presence, and mismatch branches.
262. executor phase-2B route-executor action-context slippage guard added: route layer now fail-closes submit actions missing slippage expectations (`slippage_bps`, `route_slippage_cap_bps`) and fail-closes simulate actions that incorrectly include them, guaranteeing context-shape parity before adapter forwarding; added route-executor unit coverage and execute_route_action integration pre-forward coverage for all new branches.
263. executor phase-2B route-executor numeric expectation boundary hardened: submit action-context now fail-closes non-finite slippage expectations (`slippage_bps`, `route_slippage_cap_bps`) at route-executor boundary before adapter dispatch, preventing `NaN/inf` internal-context drift from bypassing payload consistency checks; added unit guards plus submit integration pre-forward rejects and smoke registrations.
264. executor phase-2B route-layer guard-ordering hardened for action context: `execute_route_action` now validates `action_context` before allowlist/backend/feature gates, ensuring malformed submit context (`missing instruction plan` and related context-shape errors) deterministically returns `invalid_request_body` instead of policy-gate rejects; added integration priority guards for allowlist and fastlane-gate precedence plus smoke registration.
265. executor phase-2B action-context ordering matrix expanded: added explicit submit integration guard proving `action_context` rejection has priority over backend topology checks (`execute_route_action_rejects_action_context_before_backend_check`), closing the remaining ordering branch so action-context precedence is now pinned against allowlist, backend, and feature-gate paths.
266. executor phase-2B simulate action-context ordering matrix completed: added simulate integration priority guards proving `action_context` reject (`must not include submit instruction plan`) wins over allowlist, backend, and fastlane feature-gate checks, ensuring guard-order determinism is now symmetric across submit and simulate paths.
267. executor phase-2B deadline-vs-action ordering matrix expanded: added integration guards proving `deadline_context` remains higher priority than `action_context` when both are invalid simultaneously (submit and simulate variants), keeping pre-policy reject order deterministic (`missing deadline` / `must not include submit deadline`) and regression-proof.
268. executor phase-2B payload-shape priority matrix completed: added submit/simulate integration guards proving `payload_shape` rejects (empty expectation fields) always win over both `deadline_context` and `action_context`, closing the remaining pre-policy ordering matrix and pinning deterministic `invalid_request_body` detail classification across overlapping malformed inputs.
269. executor phase-2B route-hint priority matrix completed: added submit/simulate integration guards proving `payload_route` (`route_hint`) rejects always fire before `payload_shape`, `deadline_context`, and `action_context` when multiple malformed conditions overlap, finalizing deterministic ordering for the top-of-chain route-executor guards.
270. executor phase-2B route-hint priority symmetry + feature-gate coverage completed: closed remaining route-hint ordering gaps (`route_hint > action_context` on submit, `route_hint > deadline_context` on simulate) and added explicit route-hint-vs-fastlane-feature-gate priority guards for submit/simulate, ensuring top-of-chain `payload_route` determinism across policy-gate overlaps.
271. executor phase-2B route-hint allowlist/backend symmetry completed: added submit integration priority guards proving `route_hint` rejects fire before `allowlist` and `backend` policy gates (parity with existing simulate coverage), closing the remaining low-gap in route-hint ordering matrix.
272. executor phase-2B ordering governance policy codified: persistent policy document `ops/executor_ordering_coverage_policy.md` added with finite matrix scope, explicit residual count (`N=2`), closure criteria, post-closure freeze rules, and KPI limiting consecutive coverage-only slices; this is now the mandatory coordination source for cross-session executor continuation.
273. executor phase-2B ordering matrix v1 closure completed: added missing integration priority guards for `payload_shape > allowlist` (submit) and `payload_shape > feature_gate` (simulate), updated smoke registry, and closed governance residual count from `N=2` to `N=0` in `ops/executor_ordering_coverage_policy.md`.
274. executor runtime hardening: upstream/send-rpc HTTP error details now truncate oversized response bodies via shared helper (`MAX_HTTP_ERROR_BODY_DETAIL_CHARS`) to prevent reject-detail/log bloat from large upstream payloads while preserving fail-closed classification; added helper unit coverage and integration guard `forward_to_upstream_truncates_large_http_error_body_detail`.
275. executor runtime hardening coverage completed for send-rpc truncation path: added integration guards proving oversized send-rpc HTTP body and oversized JSON-RPC `error` payload details are truncated with marker (`...[truncated]`) and do not leak tail markers, then registered both guards in executor contract smoke.
276. executor runtime hardening extended into signature-verify path: `upstream_submit_failed_onchain` detail now truncates oversized on-chain `err` payloads via shared `truncate_detail_chars` limit, with integration guard `verify_submit_signature_truncates_large_onchain_error_detail` ensuring truncation marker is present and tail marker does not leak.
277. executor transport-memory hardening: upstream/send-rpc non-2xx paths now read response bodies via bounded reader (`read_response_body_limited`, `MAX_HTTP_ERROR_BODY_READ_BYTES=4096`) before detail truncation, preventing unbounded error-body reads; updated oversized-body integration guards to exceed byte-cap and added helper coverage (`read_response_body_limited_truncates_large_http_body`) to contract smoke.
278. executor smoke stability fix: `read_response_body_limited_truncates_large_http_body` now follows fail-soft network-test pattern (early-return skip when local `TcpListener::bind`/`local_addr` is unavailable), preventing hard panic in restricted environments (`cargo test -q >/dev/null`) while keeping functional coverage where bind is permitted.
279. executor 200-JSON memory hardening for send-rpc: replaced unbounded `response.json()` parse with bounded body read (`MAX_HTTP_JSON_BODY_READ_BYTES=64KiB`) + explicit JSON parse in send-rpc success path, preventing oversized 200-OK payload allocation spikes; added integration guard `send_signed_transaction_via_rpc_rejects_oversized_json_response_body` and smoke registration.
280. executor send-rpc oversized-JSON diagnostics hardening: oversized bounded-response parse failures now map to explicit terminal code `send_rpc_response_too_large` (instead of generic `send_rpc_invalid_json`) when truncation marker is present, improving operator classification without changing fail-closed behavior; added helper guard `body_text_was_truncated_detects_truncation_marker`.
281. executor diagnostics false-positive fix: `read_response_body_limited` now returns structured metadata (`ReadResponseBody { text, was_truncated }`) and send-rpc oversized classification uses `was_truncated` directly instead of string-suffix heuristics, eliminating misclassification for malformed-but-small JSON ending with `...[truncated]`; regression guard `send_signed_transaction_via_rpc_keeps_invalid_json_classification_with_marker_suffix` added and smoke updated.
282. executor upstream-forward 200-JSON memory/diagnostics hardening: success-path JSON parse switched from unbounded `response.json()` to bounded read (`MAX_HTTP_JSON_BODY_READ_BYTES`) + `serde_json::from_str`, with structured oversized classification (`upstream_response_too_large` when `was_truncated=true`, otherwise `upstream_invalid_json`); integration guards added for both oversized and marker-suffix invalid-JSON cases.
283. executor submit-verify 200-JSON memory/diagnostics hardening: signature-visibility RPC response parse switched from unbounded `response.json()` to bounded read (`MAX_HTTP_JSON_BODY_READ_BYTES`) + `serde_json::from_str`, with structured oversized classification stored in unseen reason (`rpc response_too_large` when `was_truncated=true`, otherwise `rpc invalid_json`) so strict verify mode fail-closes as retryable `upstream_submit_signature_unseen` without unbounded allocations; integration guards added for oversized response and marker-suffix invalid-JSON classification.
284. ops gate hardening: `tools/execution_go_nogo_report.sh` now fail-closes on invalid strict-policy bool env tokens (`GO_NOGO_REQUIRE_JITO_RPC_POLICY`, `GO_NOGO_REQUIRE_FASTLANE_DISABLED`) instead of silently normalizing unknown values to `false`, preventing accidental gate disablement due typos; smoke coverage now asserts both invalid-token branches return explicit input errors.
285. Stage C.5 orchestration gate hardening: `tools/execution_devnet_rehearsal.sh` now fail-closes on invalid boolean gate tokens (`RUN_TESTS`, `DEVNET_REHEARSAL_TEST_MODE`, `WINDOWED_SIGNOFF_REQUIRED`, `WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS`, `WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS`, `GO_NOGO_REQUIRE_JITO_RPC_POLICY`, `GO_NOGO_REQUIRE_FASTLANE_DISABLED`, `ROUTE_FEE_SIGNOFF_REQUIRED`, `ROUTE_FEE_SIGNOFF_GO_NOGO_TEST_MODE`) instead of silently normalizing unknown values to `false`; smoke coverage now asserts invalid-token branches for required windowed-signoff and route-fee test-mode toggles.
286. rollout orchestration gate hardening: `tools/executor_rollout_evidence_report.sh` and `tools/adapter_rollout_evidence_report.sh` now fail-close on invalid boolean env gate tokens (`RUN_TESTS`, `DEVNET_REHEARSAL_TEST_MODE`, `GO_NOGO_TEST_MODE`, `WINDOWED_SIGNOFF_*`, `GO_NOGO_REQUIRE_*`, `ROUTE_FEE_SIGNOFF_REQUIRED`, plus rehearsal/top-level route-fee go/no-go mode toggles), preventing silent gate disablement in top-level rollout wrappers; smoke coverage now includes invalid-token branches for executor/adapter rollout helpers.
287. rollout orchestration input short-circuit hardening: `tools/executor_rollout_evidence_report.sh` and `tools/adapter_rollout_evidence_report.sh` now skip downstream heavy helpers (`rotation/preflight/rehearsal/route-fee-signoff`) whenever top-level input validation already has errors (including invalid strict bool tokens), preserving fail-closed `NO_GO` while avoiding unnecessary external calls/noise; smoke invalid-token guards now assert helper sections remain unexecuted (`rotation_readiness_verdict=UNKNOWN`, etc.).
288. final-evidence wrapper hardening: `tools/executor_final_evidence_report.sh` and `tools/adapter_rollout_final_evidence_report.sh` now enforce strict bool-token validation and input short-circuit before invoking rollout helpers, preventing nested rollout execution on already-invalid top-level input; smoke coverage now includes invalid-token branches for both final wrappers and asserts fail-closed `input_error` classification (`rollout_reason_code=input_error`, `rollout_artifacts_written=false`).
289. route-fee signoff strict-bool hardening + final wrapper short-circuit: `tools/execution_route_fee_signoff_report.sh` now fail-closes on invalid gate booleans (`GO_NOGO_REQUIRE_JITO_RPC_POLICY`, `GO_NOGO_REQUIRE_FASTLANE_DISABLED`, `GO_NOGO_TEST_MODE`) instead of silent normalization, and `tools/execution_route_fee_final_evidence_report.sh` now applies strict bool parsing with top-level input short-circuit before nested signoff execution; smoke coverage adds invalid-token guards for both scripts and asserts fail-closed `input_error` classification with `signoff_artifacts_written=false` in final wrapper.
290. windowed-signoff + go/no-go test-mode strict bool hardening: `tools/execution_windowed_signoff_report.sh` now fail-closes on invalid bool tokens for `GO_NOGO_REQUIRE_JITO_RPC_POLICY`, `GO_NOGO_REQUIRE_FASTLANE_DISABLED`, `WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_HINT_SOURCE_PASS`, `WINDOWED_SIGNOFF_REQUIRE_DYNAMIC_TIP_POLICY_PASS`, and forwarded `GO_NOGO_TEST_MODE`, while `tools/execution_go_nogo_report.sh` now fail-closes on invalid `GO_NOGO_TEST_MODE`; smoke coverage adds invalid-token guards for both scripts and confirms deterministic `input_error`/exit semantics.
291. preflight + secret-rotation strict bool hardening: `tools/execution_adapter_preflight.sh` now fail-closes invalid boolean env overrides in `cfg_or_env_bool` (instead of silently falling back to file value), and `tools/adapter_secret_rotation_report.sh` now fail-closes invalid `COPYBOT_ADAPTER_ALLOW_UNAUTHENTICATED` tokens; smoke coverage adds invalid-token guards for both scripts to pin deterministic `FAIL` classification and explicit bool-token error messages.
292. adapter-preflight config-bool fail-closed completion: `tools/execution_adapter_preflight.sh` now strict-parses config fallback booleans in `cfg_or_env_bool_into` (no silent `normalize_bool_token` fallback), and fails early before SKIP branches when bool parsing already produced errors (e.g., invalid `SOLANA_COPY_BOT_EXECUTION_ENABLED` override or malformed `[execution].enabled` config value); smoke coverage adds explicit invalid `SOLANA_COPY_BOT_EXECUTION_ENABLED` token guard.
293. executor-preflight bool parsing fail-closed completion: `tools/executor_preflight.sh` now uses non-subshell bool parsing (`cfg_or_env_bool_into`) for `execution.enabled` and fails early with `preflight_verdict=FAIL`/`preflight_reason_code=config_error` when bool parsing already has errors, preventing invalid `SOLANA_COPY_BOT_EXECUTION_ENABLED` tokens from being masked as `SKIP`; smoke coverage adds explicit invalid-token guard for this branch.
294. runtime fail-closed bool/JSON boundary hardening: Rust env bool parsing is now strict at startup (`parse_bool_env` rejects invalid tokens instead of silently coercing to `false`) for executor feature/auth/tip/log gates, and bounded HTTP JSON paths (`upstream_forward`, `send_rpc`, `submit_verify`) now parse from raw bytes (`serde_json::from_slice`) rather than lossy UTF-8 text, preventing malformed non-UTF8 bodies from being accepted as valid JSON; added integration guards for invalid UTF-8 JSON classification and rehearsal-script strict guards for `GO_NOGO_TEST_MODE` / `SOLANA_COPY_BOT_EXECUTION_ENABLED`.
295. auth/runtime strictness follow-up: HMAC key-id comparison now uses constant-time equality (`constant_time_eq`) at auth boundary (eliminating variable-time mismatch check), and bool strict-parser test matrix expanded with explicit invalid-token rejects for `COPYBOT_EXECUTOR_ALLOW_UNAUTHENTICATED` and `COPYBOT_EXECUTOR_SUBMIT_VERIFY_STRICT`; contract smoke registry updated with new guard tests.
296. crypto hygiene follow-up: `constant_time_eq` no longer has early-return on length mismatch and now accumulates differences across max input length before returning, removing the length fast-fail branch in auth comparisons; added guard test `constant_time_eq_rejects_length_mismatch` and registered it in contract smoke.
297. ops bool-normalization fail-closed hardening: shared `tools/lib/common.sh::normalize_bool_token` now accepts explicit true/false forms and rejects unknown non-empty tokens instead of silently coercing them to `false`, closing downstream helper fail-open behavior for nested rollout/rehearsal/signoff parsing; synchronized local bool normalizers in `tools/execution_fee_calibration_report.sh` and `tools/execution_adapter_preflight.sh`, added strict reject guard in fee calibration (`execution.submit_adapter_require_policy_echo` invalid token), and added smoke coverage for shared common bool normalization (`true`/empty/invalid branches).
298. go/no-go bool-guard drift cleanup: `tools/execution_go_nogo_report.sh` now validates `execution_batch_sample_available` once via fail-closed normalization before policy verdict branching (instead of command-substitution checks inside `[[ ... ]]` that could swallow invalid-token failures), and emits normalized value in summary output; removed dead local `normalize_bool_token` duplicate from `tools/execution_adapter_preflight.sh` to eliminate drift risk and keep bool parsing centralized on strict `parse_env_bool_token`/`cfg_or_env_bool_into` path.
299. runtime secret-memory hygiene hardening: introduced `SecretValue` (`Zeroizing<String>`) for in-memory secrets across executor runtime (`bearer_token`, `hmac_secret`, route backend auth/send-rpc auth tokens), migrated `resolve_secret_source` to return `SecretValue`, and updated auth/backend paths to consume secret references without plaintext `String` storage in config-facing secret fields; `main` now transfers auth secrets into `AuthVerifier` via `take()` (avoiding duplicate secret copies in `AppState.config`), signer keypair validation wraps file contents and parsed keypair bytes in `Zeroizing` to clear sensitive material on drop, and guard test `secret_value_debug_redacts_plaintext` is registered in contract smoke.
300. runtime request-state secret-copy elimination: axum router state is now shared as `Arc<AppState>` (handlers extract `State<Arc<AppState>>`) so request handling no longer deep-clones `ExecutorConfig`/`route_backends` per request, idempotency cleanup worker now accepts shared `Arc<AppState>`, and `SecretValue` now stores `Arc<Zeroizing<String>>` so fallback/default token clones share one zeroized backing allocation instead of duplicating plaintext secret bytes in memory.
301. HTTP body read-failure and route guard hardening: bounded-body metadata now includes structured `read_error_class`, and success-JSON parsers (`upstream_forward`, `send_rpc`, `submit_verify`) classify incomplete/transport-corrupted response bodies as response-read failures (retryable transport path) instead of mislabeling them as `invalid_json`; submit-verify non-success HTTP responses now capture bounded body detail for diagnostics, and route-executor submit action-context removed `.expect("checked above")` panic path by converting expectation extraction to explicit fail-closed `Result` handling.
302. transport read-failure fail-closed ordering fix: success-JSON paths now short-circuit on `read_error_class` before attempting JSON parse (preventing acceptance of transport-incomplete responses that happen to contain syntactically valid partial JSON), and retryable read-failure paths in `upstream_forward` / `send_rpc` now participate in fallback endpoint traversal exactly like other retryable transport failures; added integration guards for partial-valid-json rejection and fallback-after-read-failure on both forward and send-rpc, plus submit-verify guard proving `response_read_failed` precedence for partial-valid-json bodies.
303. request ingress bound + send-rpc blockhash classifier tightening: router now enforces explicit request body size cap via `DefaultBodyLimit::max(256KiB)` (fail-fast with HTTP 413 before handler extraction), closing implicit-limit ambiguity for `/simulate` and `/submit`; send-rpc blockhash-expired classifier removed broad `recent blockhash` substring heuristic and now relies on specific expiry phrases, preventing false positive `executor_blockhash_expired` mapping for generic “recent blockhash” diagnostics.
304. route-backend contract hardening + startup consistency guard: backend-missing failures now emit dedicated terminal code `route_backend_not_configured` (no longer conflated with allowlist rejection `route_not_allowed`) across route-executor/upstream-forward/send-rpc boundaries, route backends are now explicitly cross-validated against allowlist at startup (reject on missing allowlisted backend or extra backend outside allowlist), and signer file validation now compares decoded pubkey bytes directly (with zeroized buffers) instead of deriving intermediate base58 string from keypair bytes.
305. route-scoped env/allowlist fail-closed coverage completion: startup config now rejects non-empty route-scoped env keys (`COPYBOT_EXECUTOR_ROUTE_<ROUTE>_*`) that target routes outside the parsed allowlist, so stale/typo route env (for non-allowlisted routes) cannot silently coexist and confuse runtime assumptions; parser handles longest suffix match to avoid misclassifying keys like `*_SEND_RPC_FALLBACK_AUTH_TOKEN_FILE`, and integration/contract smoke coverage now includes explicit outside-allowlist rejection path.
306. route-scoped env UTF-8 boundary hardening: route-scoped env scan switched from `env::vars()` to `env::vars_os()` to eliminate panic risk on non-UTF8 environment entries, with explicit fail-closed errors for non-UTF8 route-scoped key/value cases; added unix guard tests for non-UTF8 key/value rejection and registered them in contract smoke to preserve deterministic startup-failure behavior instead of process panic.
307. route-scoped env unknown-suffix fail-closed hardening: startup route-scoped validator now explicitly rejects non-empty `COPYBOT_EXECUTOR_ROUTE_<...>` keys that do not match supported scoped suffixes (instead of silently ignoring typos), while still exempting non-scoped `COPYBOT_EXECUTOR_ROUTE_ALLOWLIST`; added unit guards for unknown-suffix reject + allowlist-key passthrough and integration reject coverage in `ExecutorConfig::from_env`, with smoke registrations.
308. route-allowlist UTF-8 fail-closed hardening: startup config no longer treats non-UTF8 `COPYBOT_EXECUTOR_ROUTE_ALLOWLIST` as missing/default; environment read now distinguishes `NotPresent` (default `paper,rpc,jito`) from `NotUnicode` (explicit startup error), and unix integration guard `executor_config_from_env_rejects_non_utf8_route_allowlist_value` is added to contract smoke.

Остается в next-code-queue:

Execution governance gate for all next executor slices:
1. Read `ops/executor_ordering_coverage_policy.md` first.
2. Report current ordering residual count `N`.
3. Keep no more than one coverage-only slice in a row.
4. Prefer runtime/e2e progress mapped to queue items below.

1. execute production adapter rollout evidence run on server (`tools/adapter_rollout_evidence_report.sh`) after systemd secret mounts + rotation drill, and archive emitted artifacts as final runtime evidence package.
2. run route-profile calibration windows in adapter mode and attach evidence package (`route_profile_verdict=PASS`, stable primary/fallback KPI) before tightening Jito-primary/RPC-fallback policy for go/no-go (`tools/execution_go_nogo_report.sh` summary + raw artifacts).
3. finish live fee decomposition sign-off package: run adapter-mode calibration windows and attach evidence that `base/priority/tip/rent` telemetry is complete, mismatch/fallback counters are green, and totals reconcile against chain truth for go/no-go (`tools/execution_go_nogo_report.sh` + `tools/execution_fee_calibration_report.sh` raw output).
4. execute Stage C.5 devnet rehearsal with `tools/execution_devnet_rehearsal.sh`, attach artifact bundle, and close residual P0/P1 before moving to Stage D.
5. implement and close executor upstream backend per phased plan `ops/executor_backend_master_plan_2026-02-24.md` (contract freeze -> route adapters -> adapter integration -> rehearsal evidence).

## 7) Форсированный запуск на "завтра" (только controlled live)

Это не full production и не "законченный проект"; это аварийный режим минимального live.

1. До запуска:
   1. watchdog systemd реально развернут и проверен,
   2. ключ и wallet policy зафиксированы,
   3. execution MVP с idempotency+simulation+confirm готов,
   4. execution RPC endpoint + alert delivery + emergency stop готовы.
2. Запуск:
   1. tiny-live limits из Stage E,
   2. только ограниченный список token/wallet сценариев,
   3. постоянный мониторинг первых часов.
3. Критерий немедленного отката:
   1. рост fail ratio,
   2. confirm timeout spike,
   3. несоответствие on-chain reconcile,
   4. деградация ingestion pipeline.
4. Если любой пункт из блока "До запуска" не выполнен — live запуск не производится.

## 8) Запрещенные shortcuts

1. Запуск live execution без watchdog на сервере.
2. Запуск live execution без simulation и confirmation polling.
3. Использование одного и того же ключа для dev/paper/live.
4. Повышение лимитов до прохождения staged rollout KPI.

## 9) Что обновлять в документации по мере внедрения

1. `YELLOWSTONE_GRPC_MIGRATION_PLAN.md`:
   1. статус observation/completed,
   2. evidence ledger,
   3. replay waiver запись.
2. `README.md`:
   1. новый execution runtime flow,
   2. Jito/Fallback env настройки,
   3. live rollout runbook.
3. `ops/*`:
   1. watchdog deployment steps (actual),
   2. incident rollback playbook + fallback-price manual reconcile SOP (`ops/execution_manual_reconcile_runbook.md`),
   3. key rotation and emergency stop procedures.

## 10) Master Go/No-Go Checklist (до включения `execution.enabled=true`)

Все пункты обязательны одновременно:

1. Stage A закрыт (`YELLOWSTONE_GRPC_MIGRATION_PLAN.md` переведен в migration-completed).
2. Stage B закрыт (keys/alerts/emergency stop/rollback drill).
3. `R2P-08`/`R2P-09` закрыты, `R2P-10`/`R2P-11` доведены до live-path без paper stubs.
4. `R2P-12` закрыт (devnet rehearsal без P0).
5. `R2P-13` и `R2P-14` закрыты (Jito primary + live risk enforcement).
6. Подписан go/no-go note с датой, owner и rollback owner.

## 11) Live Advancement Policy (после первого submit)

1. Переход tiny-live -> limited-live только после минимум 24h green KPI.
2. Переход limited-live -> standard-live только после минимум 48h green KPI + закрытых инцидентов.
3. Любой P0 автоматически возвращает режим на предыдущую ступень лимитов до разбора причины.
