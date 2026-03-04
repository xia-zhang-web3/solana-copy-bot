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
309. startup env UTF-8 fail-closed sweep for defaults/numeric parsers: executor config now rejects non-UTF8 values for `COPYBOT_EXECUTOR_BIND_ADDR`, `COPYBOT_EXECUTOR_CONTRACT_VERSION`, and `COPYBOT_EXECUTOR_IDEMPOTENCY_DB_PATH` instead of silently falling back to defaults, while numeric parsers (`parse_u64_env`, `parse_f64_env`) now distinguish `NotPresent` vs `NotUnicode` and fail-close on non-UTF8 numeric tokens; unix integration guards added for bind/contract-version/idempotency path plus representative numeric vars (`COPYBOT_EXECUTOR_REQUEST_TIMEOUT_MS`, `COPYBOT_EXECUTOR_MAX_NOTIONAL_SOL`) and registered in contract smoke.
310. optional env parser fail-closed hardening: `optional_non_empty_env` now returns `Result<Option<String>>` and rejects non-UTF8 values when keys are present instead of silently mapping them to `None`; all executor call-sites (including auth secret inputs and submit-verify endpoint inputs) were migrated to propagated error handling, with unix integration guards proving fail-closed rejection for `COPYBOT_EXECUTOR_BEARER_TOKEN` and `COPYBOT_EXECUTOR_SUBMIT_VERIFY_RPC_URL` non-UTF8 values.
311. ordering governance decision record: deferred `M-1` reorder remains `NO-GO` under current canonical policy because ordering matrix v1 is closed (`N=0`) and handler-level route/allowlist checks already cover main paths; policy now explicitly defines reopen condition (new product/security requirement for policy-first route-executor rejects) and required closure steps if reopened.
312. env parser UTF-8 boundary completion for required/optional strings: `non_empty_env` now explicitly distinguishes `NotPresent` (`is required`) from `NotUnicode` (`must be valid UTF-8 string`) instead of masking non-UTF8 under generic required-context messaging, and unit guards in `env_parsing.rs` now pin non-UTF8 rejection for both `non_empty_env` and `optional_non_empty_env`; contract smoke includes these parser-level guards.
313. startup log-filter boundary hardening: executor startup now parses `COPYBOT_EXECUTOR_LOG_FILTER` with explicit `NotPresent`/`NotUnicode` handling and fail-closes invalid tracing filter syntax instead of silently falling back to `info`, while preserving the same default filter when key is absent; added parser guards in `main.rs` for missing/invalid/non-UTF8 branches and registered them in contract smoke.
314. startup unknown-env fail-closed hardening: executor config now rejects unknown non-route-scoped `COPYBOT_EXECUTOR_*` keys when they are non-empty (typo protection) while preserving existing route-scoped validation and allowing `COPYBOT_EXECUTOR_TEST_*` test namespace; this closes silent fail-open behavior where malformed top-level config keys were ignored. Added integration guards for reject (`COPYBOT_EXECUTOR_SUBMIT_FASTLANE_ENABLD`) and empty-value ignore behavior, and registered the reject guard in contract smoke.
315. fallback-auth inheritance hardening: executor config now inherits primary auth token into fallback auth token only when corresponding fallback endpoint is configured (upstream: submit/simulate fallback URL; send-rpc: send-rpc fallback URL). This removes unnecessary fallback secret materialization when no fallback path exists, keeps fallback semantics unchanged when fallback endpoint is present, and adds integration guards for both non-inheritance and inheritance branches (upstream + send-rpc) with smoke registration.
316. fallback-auth topology strictness hardening: executor config now fail-closes when fallback auth is explicitly configured but corresponding fallback endpoint topology is absent (`route-specific` and `global` branches for both upstream and send-rpc). This removes unused-secret drift and prevents misleading config states where fallback credentials exist without any retry endpoint path. Added 4 integration guards for these reject branches and registered them in contract smoke.
317. fallback-auth file-branch coverage completion: strict fallback-topology rejects from item 316 are now symmetrically covered for `*_FALLBACK_AUTH_TOKEN_FILE` branches (route-specific and global, upstream and send-rpc), closing residual low-gap where only token-value branches were tested; added 4 integration guards and registered them in contract smoke.
318. submit full-chain verification coverage hardening: added integration coverage for `handle_submit` path where upstream returns `signed_tx_base64` and flow continues through `send_rpc` and `submit_verify` in one chain; one guard proves success shape includes `submit_signature_verify={enabled:true,seen:true,confirmation_status}`, and one guard proves strict-mode unseen visibility after successful send-rpc returns retryable `upstream_submit_signature_unseen`. Both tests are registered in contract smoke.
319. HTTP success-JSON `Content-Length` pre-read guard hardening: `upstream_forward`, `send_rpc`, and `submit_verify` now fail-close on declared `Content-Length` exceeding `MAX_HTTP_JSON_BODY_READ_BYTES` before body read/parse, preventing unnecessary oversized success-body reads and preserving deterministic `*_response_too_large` classification. Added integration guards for declared-oversized path in all three flows and registered them in contract smoke.
320. signer keypair file parsing boundary hardening: signer source validation now reads keypair file as `raw bytes` (`fs::read`) and parses via `serde_json::from_slice` (instead of `read_to_string`/`from_str`), removing intermediate UTF-8 string materialization for sensitive keypair payload and keeping fail-closed JSON-shape checks on byte input. Added guard test `resolve_signer_source_config_rejects_non_json_keypair_payload` and registered it in contract smoke.
321. auth header ASCII-boundary hardening: ingress auth verification now distinguishes `missing` vs `invalid` required headers by rejecting non-ASCII header values explicitly (`auth_invalid` / `hmac_invalid`) instead of collapsing them into missing-header paths. `Authorization` now fails closed on invalid ASCII before Bearer parsing, and HMAC required headers now use strict `missing_code`+`invalid_code` split in `get_required_header`. Added guard tests for non-ASCII `Authorization` and non-ASCII `x-copybot-key-id`, and registered both in contract smoke.
322. declared-oversized success-body fallback hardening: `upstream_forward` and `send_rpc` now classify oversized declared `Content-Length` on success JSON as retryable (`*_response_too_large`) so endpoint-chain fallback remains available (instead of terminal stop on primary) while preserving fail-closed classification. Added integration guards proving fallback after primary declared-oversized response for both upstream and send-rpc, and updated existing declared-oversized reject tests to assert retryable semantics.
323. truncated-success-body fallback symmetry hardening: non-declared oversize (`was_truncated` after bounded read) on success JSON is now treated as retryable `*_response_too_large` with endpoint-chain fallback in `upstream_forward` and `send_rpc` (instead of terminal stop), aligning behavior with declared `Content-Length` oversized branch. Added chunked-response integration guards proving fallback after primary truncated oversized success body for both upstream and send-rpc paths, with smoke registration.
324. pre-parse truncated-body fail-closed hardening: success JSON paths now short-circuit on `body_read.was_truncated` before JSON parse in `upstream_forward`, `send_rpc`, and `submit_verify`, preventing acceptance of truncated-but-syntactically-valid JSON prefixes. Added integration guards using chunked oversized responses with valid JSON prefix + whitespace tail to prove strict `*_response_too_large` / verify `response_too_large` classification and fallback behavior.
325. submit-verify fallback observability + boundary coverage hardening: verify loop now records and emits explicit fallback-transition warnings (endpoint/attempt/reason) whenever a primary verify endpoint fails and another endpoint remains in the same attempt, and fallback coverage is extended for both oversized success-body branches (`declared Content-Length` and `was_truncated`) by proving `verify_submitted_signature_visibility` reaches fallback endpoint and returns `Seen` confirmation status.
326. send-rpc error classifier precision hardening: `classify_send_rpc_error_payload` now derives classifier text only from structured error fields (`message`, `data.message`, `data.details`, `data.err`) instead of full serialized JSON payload, preventing false retryable classification from unrelated marker substrings in opaque payload blobs while preserving retryable detection for structured provider diagnostics. Added unit guards for unstructured timeout-marker ignore + structured data-message retryable path and integration guards proving terminal classification for unstructured marker payload and fallback success for structured retryable payload.
327. submit full-chain send-rpc fallback coverage expansion: added integration guards proving `handle_submit` (upstream returns `signed_tx_base64`) succeeds through `send_rpc` fallback when primary send-rpc endpoint fails on both oversized success-body classes (`declared Content-Length` and `was_truncated`), locking full submit pipeline fallback behavior beyond helper-level `send_signed_transaction_via_rpc` tests.
328. submit full-chain verify fallback coverage expansion: added integration guards proving `handle_submit` (with submit-signature-verify enabled) succeeds via verify endpoint fallback when primary verify endpoint fails on oversized success-body classes (`declared Content-Length` and `was_truncated`), locking end-to-end submit chain behavior (`upstream -> send_rpc -> submit_verify`) at the handler level.
329. submit verify oversized-cause assertion hardening: added full-chain strict-mode guards proving `handle_submit` without verify fallback rejects with explicit `response_too_large` cause for both primary verify oversized classes (`declared Content-Length` and `was_truncated`), preventing false-green fallback tests from masking drift to unrelated retryable failure reasons.
330. send-rpc classifier string-data parity hardening: `classify_send_rpc_error_payload` now also consumes string-valued `error.data` in classifier text extraction, preserving retryable classification when providers encode transport hints in `data: "..."` (without nested `data.message`). Added unit guards for string-data extraction and retryable classification plus integration fallback guard proving `send_signed_transaction_via_rpc` succeeds via fallback on retryable signal from string `error.data`.
331. submit send-rpc oversized-cause assertion hardening: added full-chain guards proving `handle_submit` without send-rpc fallback rejects with explicit `send_rpc_response_too_large` cause for both primary send-rpc oversized classes (`declared Content-Length` and `was_truncated`), preventing fallback-success tests from masking drift to unrelated retryable/terminal send-rpc reasons.
332. submit upstream oversized fallback/cause full-chain hardening: added full-chain `handle_submit` guards for upstream oversized success-body classes so upstream retry fallback behavior is pinned at handler level (`declared Content-Length` and `was_truncated`), plus no-fallback negative guards that assert explicit `upstream_response_too_large` cause markers (`declared content-length` / `max_bytes=65536`) to prevent false-green fallback coverage from hiding upstream-cause drift.
333. signer pubkey compare constant-time hardening: signer file validation now compares `COPYBOT_EXECUTOR_SIGNER_PUBKEY` bytes against keypair pubkey bytes via `constant_time_eq` (removing direct slice `!=` comparison), and coverage adds explicit positive file-source guard (`resolve_signer_source_config_accepts_file_source_with_matching_pubkey`) to pin successful file signer path in contract smoke.
334. simulate full-chain upstream oversized fallback/cause hardening: added full-chain `handle_simulate` guards for upstream oversized success-body classes (`declared Content-Length` and `was_truncated`) covering both fallback-success and no-fallback reject branches, with explicit `upstream_response_too_large` cause markers (`declared content-length` / `max_bytes=65536`) pinned at simulate handler boundary.
335. healthz route ordering determinism hardening: `build_healthz_payload` now emits `enabled_routes` and backward-compat `routes` alias as sorted arrays (instead of HashSet iteration order), making health snapshots deterministic across runs/nodes for evidence diffing and ops consumers while preserving existing field schema.
336. route-list ordering determinism unification: introduced shared `route_allowlist::sorted_routes(...)` and reused it for both healthz payload route arrays and startup configuration logging, removing duplicate sort logic and making startup `routes` log output deterministic across process restarts/evidence captures.
337. route allowlist duplicate-entry fail-closed hardening: `parse_route_allowlist` now rejects duplicate routes after normalization (e.g., `rpc,RPC`) instead of silently deduplicating via `HashSet`, and startup config coverage now pins `ExecutorConfig::from_env` reject behavior for duplicate allowlist entries to avoid masking operator config mistakes.
338. route allowlist empty-entry fail-closed hardening: `parse_route_allowlist` now rejects empty CSV entries (e.g., trailing comma in `COPYBOT_EXECUTOR_ROUTE_ALLOWLIST=rpc,`) instead of silently skipping them, and startup config coverage pins explicit from-env rejection for empty entry cases to avoid masking malformed route policy input.
339. startup fastlane allowlist gate integration hardening: fail-closed startup guard for `fastlane` in `COPYBOT_EXECUTOR_ROUTE_ALLOWLIST` with `COPYBOT_EXECUTOR_SUBMIT_FASTLANE_ENABLED=false` is now pinned at `ExecutorConfig::from_env` level (not only parser unit scope), and error detail now includes deterministic normalized allowlist snapshot (`allowlist=...`) for faster operator diagnosis.
340. startup env-prefix casing hardening: executor config now fail-closes on non-empty mixed-case `copybot_executor_*` keys (canonical uppercase `COPYBOT_EXECUTOR_*` required), preventing silent misconfiguration from wrong key casing while preserving empty-value ignore semantics; `with_clean_executor_env` cleanup in tests is now case-insensitive for executor-prefixed keys to avoid cross-test leakage, with integration guards covering reject and empty-ignore branches and smoke registration.
341. startup env typo-diagnostics hardening: unknown executor env keys now include deterministic `did you mean ...` suggestions for close matches (both non-route `COPYBOT_EXECUTOR_*` and route-scoped `COPYBOT_EXECUTOR_ROUTE_<ROUTE>_<SUFFIX>` keys), and mixed-case prefix rejects now suggest canonical uppercase key names when derivable; this preserves fail-closed behavior while reducing operator triage time on env typos.
342. route allowlist typo-diagnostics hardening: `parse_route_allowlist` now emits deterministic `did you mean route=<known>?` hints for close unknown route tokens (for example `faslane` -> `fastlane`) while remaining fail-closed; parser and startup config guards now pin both branches (suggestion present for close typo, suggestion absent for distant unknown token).
343. contract smoke registry sync for allowlist-typo startup guard: `executor_contract_smoke_test.sh` now includes `executor_config_from_env_rejects_unknown_route_allowlist_entry_with_suggestion`, closing coverage drift where parser guard was registered but from-env integration guard was not.
344. typo-distance engine de-dup hardening: shared module `text_distance` now owns Levenshtein + closest-match logic used by both startup env typo diagnostics (`executor_config_env`) and route-allowlist typo suggestions (`route_allowlist`), removing duplicated implementations and reducing drift risk while preserving all existing fail-closed semantics and guard behavior.
345. route-scoped allowlist typo-diagnostics hardening: route-scoped env target validation now emits deterministic `did you mean route=<allowlisted>?` hints when key route token is a close typo of an allowlisted route (for example `COPYBOT_EXECUTOR_ROUTE_RCP_SUBMIT_URL` with allowlist `rpc`), while preserving fail-closed rejection and existing outside-allowlist policy; unit + from-env integration guards pin the suggestion branch and smoke registry includes the integration guard.
346. go/no-go execution-bool short-circuit hardening: `tools/execution_go_nogo_report.sh` now parses `execution.*` boolean settings (`submit_dynamic_cu_price_enabled`, `submit_dynamic_tip_lamports_enabled`, `submit_fastlane_enabled`) before invoking heavy helper scripts and emits source-aware fail-closed diagnostics (`env <NAME>` or `config [section].key`) on invalid tokens, preventing expensive calibration/snapshot runs on already-invalid input; smoke coverage now includes invalid `SOLANA_COPY_BOT_EXECUTION_SUBMIT_FASTLANE_ENABLED` token path.
347. calibration strict env-bool short-circuit hardening: `tools/execution_fee_calibration_report.sh` now parses `execution.submit_adapter_require_policy_echo` via source-aware strict bool helper (`env SOLANA_COPY_BOT_EXECUTION_SUBMIT_ADAPTER_REQUIRE_POLICY_ECHO` or `config [execution].submit_adapter_require_policy_echo`) before DB introspection/queries, fail-closing invalid tokens early with explicit diagnostics; ops smoke coverage now includes invalid env token branch for this gate.
348. ops strict-bool parser de-dup hardening: shared `tools/lib/common.sh` now owns canonical `parse_bool_token_strict()` and rollout/signoff/rehearsal/go-no-go helpers consume the common implementation instead of carrying local duplicates, eliminating parser drift risk while preserving strict fail-closed semantics and existing invalid-token behavior across scripts.
349. calibration parser drift closure: `tools/execution_fee_calibration_report.sh` now sources shared `tools/lib/common.sh` and consumes canonical `parse_bool_token_strict()` (local duplicate removed), aligning strict-bool semantics and eliminating the remaining same-name parser drift between calibration and rollout/go-no-go helpers.
350. evidence bundle packaging helper: added `tools/evidence_bundle_pack.sh` to package arbitrary evidence directory trees into a tarball with generated file-level contents checksum manifest (`*.contents.sha256`) plus bundle checksum (`*.sha256`), enabling deterministic archive handoff for rollout/rehearsal evidence artifacts; ops smoke now includes a guard validating bundle creation, checksum consistency, and packaged file inventory.
351. evidence bundle idempotence/collision hardening: `tools/evidence_bundle_pack.sh` now avoids bundle name collisions by adding deterministic numeric suffixes when target artifacts already exist, supports optional fixed `BUNDLE_TIMESTAMP_UTC` for reproducible runs/tests, and excludes previous helper-generated bundle/checksum artifacts (`<label>_*.tar.gz|*.sha256|*.contents.sha256`) from packed file lists so repeated runs with default `OUTPUT_DIR=$EVIDENCE_DIR` remain stable and non-recursive.
352. evidence bundle cross-label recursion hardening: helper artifact exclusion now matches canonical bundle artifact signature regardless of label (`<any_label>_<timestamp>[_N].{tar.gz,sha256,contents.sha256}`), preventing cross-label self-output recursion when repeated runs target the same evidence directory; smoke coverage now pins both same-label and cross-label repeated-run scenarios with stable `file_count` and archive inventory checks.
353. evidence bundle false-positive exclusion closure: helper now tracks generated bundle/checksum artifact paths via dedicated index file (`.copybot_evidence_bundle_outputs.txt`) and excludes only indexed helper outputs (plus the index itself) from future pack runs, replacing broad filename-pattern suppression; this keeps same-label/cross-label recursion protection while preserving legitimate evidence files that happen to match timestamped archive naming conventions.
354. evidence bundle index trust hardening: helper no longer blindly trusts `.copybot_evidence_bundle_outputs.txt`; index entries are now validated (safe relative paths scoped to output dir), promoted to exclusion list only when they form an existing complete helper artifact triplet (`.tar.gz` + `.sha256` + `.contents.sha256`), and invalid/poisoned entries are ignored during both exclusion and index rewrite, preventing accidental or malicious suppression of legitimate evidence files.
355. evidence bundle external-output index isolation: when `OUTPUT_DIR` is outside `EVIDENCE_DIR`, helper now ignores external index content entirely (no read/merge from `.copybot_evidence_bundle_outputs.txt`) so poisoned/stale index files outside evidence tree cannot suppress in-tree evidence triplet files; smoke coverage now includes explicit outside-output poisoned-index scenario and pins full file inclusion.
356. adapter final evidence auto-bundle option: `tools/adapter_rollout_final_evidence_report.sh` now supports optional post-run archive packaging (`PACKAGE_BUNDLE_ENABLED=true`) through `tools/evidence_bundle_pack.sh`, with explicit bundle outputs (`package_bundle_*` fields), checksum recording in final manifest, and fail-closed exit when bundling is requested but not written; ops smoke now includes a bundle-enabled final-package guard.
357. executor final evidence auto-bundle parity: `tools/executor_final_evidence_report.sh` now supports optional post-run archive packaging (`PACKAGE_BUNDLE_ENABLED=true`) through `tools/evidence_bundle_pack.sh`, emits deterministic `package_bundle_*` machine-readable fields, appends bundle checksum metadata into final manifest, and fail-closes with exit `3` when bundling is requested but not written; ops smoke now includes executor final-package bundle-enabled guard coverage.
358. final evidence checksum invariant fix: both final package helpers now compute `summary_sha256` only after all summary content is finalized (including `package_bundle_*` append), then generate manifest from that finalized digest; smoke now asserts `summary_sha256 == sha256(artifact_summary)` and `manifest_sha256 == sha256(artifact_manifest)` for bundle-disabled and bundle-enabled paths (adapter and executor), preventing stale hash publication regressions.
359. rollout evidence auto-bundle + checksum parity: both rollout helpers (`tools/executor_rollout_evidence_report.sh`, `tools/adapter_rollout_evidence_report.sh`) now support optional archive packaging (`PACKAGE_BUNDLE_ENABLED=true`) via `tools/evidence_bundle_pack.sh`, emit deterministic `package_bundle_*` fields, include bundle checksum metadata in rollout manifests, and fail-close when bundling is requested but not written; smoke coverage now validates summary/manifest sha-file invariants and bundle-enabled success paths for both rollout helpers.
360. go/no-go + devnet rehearsal auto-bundle completion: `tools/execution_go_nogo_report.sh` and `tools/execution_devnet_rehearsal.sh` now support optional archive packaging (`PACKAGE_BUNDLE_ENABLED=true`) with strict `OUTPUT_DIR` requirement, deterministic `package_bundle_*` output fields, bundle checksum metadata in their manifests, and fail-closed behavior on requested-but-unwritten bundles (`go_nogo: exit 1`, `rehearsal: exit 3`); smoke adds bundle-enabled guard paths and summary/manifest sha-file invariants for both scripts.
361. windowed + route-fee evidence auto-bundle completion: `tools/execution_windowed_signoff_report.sh`, `tools/execution_route_fee_signoff_report.sh`, and `tools/execution_route_fee_final_evidence_report.sh` now support optional archive packaging (`PACKAGE_BUNDLE_ENABLED=true`) through `tools/evidence_bundle_pack.sh`, emit deterministic `package_bundle_*` fields, append bundle checksum metadata into manifests, enforce strict `OUTPUT_DIR` requirement where applicable (windowed/route-fee signoff), and fail-close when bundling is requested but not written; smoke adds bundle-enabled guards and summary/manifest sha-file invariants for all three helpers.
362. helper manifest-sha parity hardening: artifact-exporting helper scripts `tools/executor_preflight.sh`, `tools/executor_signer_rotation_report.sh`, and `tools/adapter_secret_rotation_report.sh` now emit `manifest_sha256` alongside existing summary/report digests, and ops smoke now asserts hash-to-file parity for both artifact and manifest outputs to prevent silent drift in machine-readable checksum fields.
363. adapter secret rotation `set -u` empty-array hardening: `tools/adapter_secret_rotation_report.sh` now iterates secret-file keys with nounset-safe array expansion (`"${secret_keys[@]-}"`), preventing unbound-variable crashes when no `COPYBOT_ADAPTER_*_FILE` keys exist in env; ops smoke now includes a guard case for a no-file-keys env and asserts deterministic PASS with zero secret-file counters.
364. ops nounset-safe CSV/array iteration hardening: `tools/execution_adapter_preflight.sh`, `tools/execution_fee_calibration_report.sh`, and `tools/executor_preflight.sh` now use nounset-safe array expansions in CSV/error iteration loops (`"${arr[@]-}"`) to prevent `set -u` unbound-variable crashes on empty arrays; ops smoke adds an adapter-preflight guard that enforces deterministic FAIL (not crash) for empty `submit_allowed_routes` under adapter mode.
365. evidence bundle smoke canonical path parity: `tools/ops_scripts_smoke_test.sh` now canonicalizes expected `evidence_dir`/`output_dir` via `pwd -P` before comparing with `tools/evidence_bundle_pack.sh` machine-readable fields, eliminating `/var` vs `/private/var` symlink drift flakes while preserving strict path equality checks.
366. window-csv nounset hardening for signoff helpers: `tools/execution_windowed_signoff_report.sh` and `tools/execution_route_fee_signoff_report.sh` now parse windows with nounset-safe array expansion (`"${raw_windows[@]-}"`), preventing `set -u` crashes on empty `WINDOWS_CSV`; ops smoke adds explicit empty-window guards for both scripts and asserts deterministic `NO_GO` with `no valid windows parsed` input errors.
367. `audit_standard.sh` strict bool gate for smoke runner switch: `AUDIT_SKIP_OPS_SMOKE` now uses shared fail-closed parser (`parse_bool_token_strict` from `tools/lib/common.sh`) and exits `1` on invalid tokens instead of silently normalizing unknown values to `false`; ops smoke now includes a targeted guard asserting explicit rejection for `AUDIT_SKIP_OPS_SMOKE=maybe`.
368. shared bool helper cleanup hardening: removed dead `normalize_bool_token()` from `tools/lib/common.sh` (no in-repo call sites), and updated ops smoke common-bool guard to validate canonical `parse_bool_token_strict()` semantics (`yes -> true`, `off -> false`, empty/invalid -> reject), preventing future semantic drift from reintroducing parallel bool normalizers.
369. shared bool helper compatibility rollback: restored `normalize_bool_token()` in `tools/lib/common.sh` as a compatibility wrapper (`"" -> false`, otherwise strict bool token parsing with fail-closed invalid-token error) after downstream audit identified active call-sites across ops helpers; this removes `command not found` runtime breakage while preserving strict parser usage for newly hardened gates.
370. bool compatibility guard pinning for ops runtime helpers: `tools/ops_scripts_smoke_test.sh` now includes explicit `normalize_bool_token()` compat-wrapper coverage (`" yes " -> true`, `"" -> false`, invalid token -> `exit 1` with expected error), preventing silent regressions where shared wrapper removal or behavior drift would break rollout/final helper scripts at runtime.
371. `audit_full.sh` strict smoke-skip gate hardening: `AUDIT_SKIP_OPS_SMOKE` is now parsed via shared fail-closed `parse_bool_token_strict` before any heavy audit steps in `tools/audit_full.sh`; invalid token exits `1` with explicit error, and ops smoke is only skipped on canonical `true`. Ops smoke now includes targeted invalid-token guard coverage for `audit_full.sh`.
372. `audit_quick.sh` strict contract-smoke skip gate hardening: `AUDIT_SKIP_CONTRACT_SMOKE` is now parsed via shared fail-closed `parse_bool_token_strict` before baseline tests in `tools/audit_quick.sh`; invalid token exits `1` with explicit error, and contract smoke runs unless canonical `true` skip is requested. Ops smoke now includes targeted invalid-token guard coverage for `audit_quick.sh`.
373. `audit_full.sh` contract-smoke gate propagation hardening: `AUDIT_SKIP_CONTRACT_SMOKE` is now parsed fail-closed directly in `tools/audit_full.sh` (defaulting to canonical `AUDIT_SKIP_OPS_SMOKE` when unset) and propagated explicitly into `audit_quick.sh`; invalid contract-smoke token now fails fast before baseline execution, and ops smoke includes targeted invalid-token guard for this path.
374. `audit_standard.sh` diff-range fail-closed hardening: invalid `AUDIT_DIFF_RANGE` is now rejected explicitly (`exit 1`) instead of being swallowed by `collect_changed_files || true`, and validation now happens before quick baseline so heavy tests are not launched on malformed range input; ops smoke adds targeted guard that pins invalid-range rejection and fail-fast ordering.
375. `audit_standard.sh` contract-smoke gate propagation hardening: `AUDIT_SKIP_CONTRACT_SMOKE` is now parsed fail-closed in `tools/audit_standard.sh` (defaulting to canonical `AUDIT_SKIP_OPS_SMOKE` when unset) and explicitly propagated to `audit_quick.sh`; invalid contract-smoke token now fails before quick baseline, avoiding accidental long contract-smoke runs under ops-skip scenarios. Ops smoke adds targeted invalid-token guard for this path.
376. audit runner ops-smoke timeout hardening: `tools/audit_standard.sh` and `tools/audit_full.sh` now consume strict `AUDIT_OPS_SMOKE_TIMEOUT_SEC` (`parse_u64_token_strict`, `>=1`) instead of fixed `timeout 300`, fail-closing invalid/zero values before heavy work and allowing explicit bounded timeout control for ops smoke runs; ops smoke adds targeted invalid-timeout guards for both scripts.
377. audit runner contract-smoke timeout hardening: `tools/audit_quick.sh` now consumes strict `AUDIT_CONTRACT_SMOKE_TIMEOUT_SEC` (`parse_u64_token_strict`, `>=1`) and runs contract smoke under bounded timeout when enabled; `audit_standard.sh` and `audit_full.sh` now parse/propagate canonical contract-smoke timeout (defaulting to parsed ops timeout when unset), fail-closing invalid/zero values before baseline starts. Ops smoke adds targeted invalid-timeout guards for quick/standard paths.
378. audit full workspace-test timeout hardening: `tools/audit_full.sh` now consumes strict `AUDIT_WORKSPACE_TEST_TIMEOUT_SEC` (`parse_u64_token_strict`, `>=1`) and executes `cargo test --workspace -q` under bounded timeout when available; invalid/zero timeout values fail-close before baseline, and ops smoke adds targeted invalid-timeout guard to pin fail-fast behavior.
379. audit quick executor-test timeout hardening: `tools/audit_quick.sh` now consumes strict `AUDIT_EXECUTOR_TEST_TIMEOUT_SEC` (`parse_u64_token_strict`, `>=1`) and executes `cargo test -p copybot-executor -q` under bounded timeout when available; invalid/zero timeout values fail-close before baseline starts. Ops smoke adds targeted invalid-timeout guard that pins fail-fast behavior for this path.
380. audit standard package-test timeout hardening: `tools/audit_standard.sh` now consumes strict `AUDIT_PACKAGE_TEST_TIMEOUT_SEC` (`parse_u64_token_strict`, `>=1`) and runs changed-package tests (`cargo test -p <package> -q`) under bounded timeout when available; invalid/zero timeout values fail-close before baseline starts. Ops smoke adds targeted invalid-timeout guard that pins fail-fast behavior for this path.
381. audit quick executor-test skip gate hardening: `tools/audit_quick.sh` now supports strict/fail-closed `AUDIT_SKIP_EXECUTOR_TESTS` (default `false`), allowing explicit baseline executor-test skip while rejecting invalid bool tokens before any heavy steps.
382. audit standard baseline control propagation hardening: `tools/audit_standard.sh` now parses strict `AUDIT_SKIP_EXECUTOR_TESTS`, `AUDIT_SKIP_PACKAGE_TESTS`, and `AUDIT_EXECUTOR_TEST_TIMEOUT_SEC` (defaulting to parsed package timeout), and propagates canonical quick-baseline controls into `audit_quick.sh`; invalid tokens/timeouts fail-close before baseline.
383. audit full baseline/workspace control hardening: `tools/audit_full.sh` now parses strict `AUDIT_SKIP_EXECUTOR_TESTS`, `AUDIT_SKIP_WORKSPACE_TESTS`, and `AUDIT_EXECUTOR_TEST_TIMEOUT_SEC` (defaulting to parsed workspace timeout), propagates canonical quick-baseline controls into `audit_quick.sh`, and allows explicit workspace-test skip with deterministic logging.
384. audit guard-batch coverage expansion: `tools/ops_scripts_smoke_test.sh` now includes combined strict-bool guard coverage for newly added skip gates (`AUDIT_SKIP_EXECUTOR_TESTS`, `AUDIT_SKIP_PACKAGE_TESTS`, `AUDIT_SKIP_WORKSPACE_TESTS`) and timeout guard coverage for `AUDIT_EXECUTOR_TEST_TIMEOUT_SEC` validation in `audit_standard.sh`/`audit_full.sh`.
385. shared timeout helper hardening for ops scripts: `tools/lib/common.sh` now exposes `parse_timeout_sec_strict(raw, min, max)`, `resolve_timeout_command()`, and `run_with_timeout_if_available(timeout, cmd...)`, centralizing timeout bounds parsing and cross-platform timeout command discovery (`timeout`/`gtimeout`) to reduce duplicated shell logic and drift risk.
386. audit runner timeout execution de-dup: `tools/audit_quick.sh`, `tools/audit_standard.sh`, and `tools/audit_full.sh` now use shared `run_with_timeout_if_available` for all bounded test/smoke invocations, removing duplicated `command -v timeout` branches and ensuring consistent timeout command behavior across quick/standard/full runners.
387. audit timeout upper-bound fail-closed hardening: all audit timeout env gates (`AUDIT_OPS_SMOKE_TIMEOUT_SEC`, `AUDIT_CONTRACT_SMOKE_TIMEOUT_SEC`, `AUDIT_EXECUTOR_TEST_TIMEOUT_SEC`, `AUDIT_PACKAGE_TEST_TIMEOUT_SEC`, `AUDIT_WORKSPACE_TEST_TIMEOUT_SEC`) now enforce `1..=86400` via `parse_timeout_sec_strict`, rejecting over-limit values before baseline execution.
388. timeout parser and upper-bound guard coverage expansion: `tools/ops_scripts_smoke_test.sh` now includes explicit shared-helper coverage for `parse_timeout_sec_strict` (valid/zero/over-limit) and batch guard coverage proving quick/standard/full runners fail-fast on over-limit timeout env values before baseline starts.
389. server rollout orchestration helper: added `tools/execution_server_rollout_report.sh` to execute a single end-to-end server rollout chain (`execution_adapter_preflight` -> `execution_fee_calibration_report` -> `execution_go_nogo_report` -> `execution_devnet_rehearsal` -> `executor_final_evidence_report` -> `adapter_rollout_final_evidence_report`) with captured stage outputs, machine-readable per-stage verdict fields, and consolidated top-level `server_rollout_verdict`.
390. server rollout consolidated artifact model: orchestration helper now writes deterministic stage capture artifacts plus top-level summary/manifest with checksum fields (`*_capture_sha256`, stage `summary_sha256` passthrough, top-level `summary_sha256` / `manifest_sha256`) so rollout handoff can be validated from one package root.
391. server rollout fail-closed input governance: orchestration helper now validates core file/arg prerequisites and strict bool gates before stage execution (`RUN_TESTS`, `DEVNET_REHEARSAL_TEST_MODE`, `GO_NOGO_TEST_MODE`, window/route fee requirement gates, package bundle gate), emitting `server_rollout_reason_code=input_error` and `exit 3` on malformed startup input.
392. server rollout smoke coverage expansion: `tools/ops_scripts_smoke_test.sh` now includes `run_execution_server_rollout_report_case` with deterministic HOLD-path assertions on smoke fixtures (`calibration_*_verdict=WARN` while downstream stages remain GO), bundle-enabled path checks, and invalid-bool fail-closed guard assertions, wiring the helper into ops smoke registry for regression detection.
393. server rollout bundle finalization order fix: `tools/execution_server_rollout_report.sh` now builds top-level `summary` + `manifest` before invoking `tools/evidence_bundle_pack.sh`, eliminating pre-finalization bundle snapshots (missing manifest / incomplete summary block) and keeping packaged evidence self-consistent at creation time.
394. server rollout bundle-content smoke guard: `tools/ops_scripts_smoke_test.sh` bundle branch now verifies tar payload contains both `execution_server_rollout_summary_*` and `execution_server_rollout_manifest_*` and asserts `package_bundle_*` lines exist inside bundled summary content, so future regressions in packaging order are detected.
395. server rollout package-summary value consistency: `tools/execution_server_rollout_report.sh` now resolves package status via first-pass bundle run, appends resolved `package_bundle_artifacts_written` / `package_bundle_exit_code` / `package_bundle_error` to artifact summary, then performs a second bundle pass so packaged summary/manifest reflect finalized package-status values instead of default placeholders.
396. server rollout bundle smoke value pinning: `tools/ops_scripts_smoke_test.sh` now asserts bundled summary `package_bundle_artifacts_written` and `package_bundle_exit_code` exactly match top-level stdout fields, preventing regressions where bundle payload drifts from reported package status.
397. final-wrapper package status consistency hardening: `tools/execution_route_fee_final_evidence_report.sh`, `tools/executor_final_evidence_report.sh`, and `tools/adapter_rollout_final_evidence_report.sh` now use two-pass package-bundle flow (status pass -> summary/manifest finalize -> payload pass) and persist only non-recursive package status fields (`artifacts_written`, `exit_code`, `error`) in summary artifacts.
398. final-wrapper manifest recursion guard: removed self-referential bundle-artifact hash fields from final-wrapper manifests (`package_bundle_*_sha256` over bundle artifacts) to avoid recursive/unstable manifest values when manifest itself is packaged into the bundle payload.
399. final-wrapper bundle smoke parity expansion: `tools/ops_scripts_smoke_test.sh` now validates bundle payload parity for route-fee/executor-final/adapter-final wrappers by asserting bundled summary `package_bundle_artifacts_written` and `package_bundle_exit_code` match stdout, and that bundled manifest exists alongside summary in archive.
400. adapter rollout smoke determinism guard: `run_adapter_rollout_evidence_case` now bootstraps fake `journalctl` internally and pins `dynamic_cu_hint_api_total`/`dynamic_cu_hint_rpc_total` to canonical fixture values (`1/1`), removing caller-order dependence that previously caused targeted-run flaps.
401. non-final helper bundle finalization parity hardening: `tools/execution_go_nogo_report.sh`, `tools/execution_devnet_rehearsal.sh`, `tools/execution_windowed_signoff_report.sh`, `tools/execution_route_fee_signoff_report.sh`, `tools/executor_rollout_evidence_report.sh`, and `tools/adapter_rollout_evidence_report.sh` now use two-pass package bundling (status pass -> summary/manifest finalize -> payload pass), so bundle payload includes finalized summary/manifest artifacts.
402. non-final helper manifest recursion guard: the six non-final helpers now persist only non-recursive package status fields (`package_bundle_artifacts_written`, `package_bundle_exit_code`, `package_bundle_error`) in summary artifacts and drop recursive bundle-artifact hash fields from manifests; ops smoke bundle-enabled branches now assert bundled summary/manifest parity for all six helpers.
403. runtime readiness orchestration helper: added `tools/execution_runtime_readiness_report.sh` to compose `adapter_rollout_final_evidence_report.sh` + `execution_route_fee_final_evidence_report.sh` into one machine-readable runtime handoff package, with normalized aggregate verdict (`GO`/`HOLD`/`NO_GO`), captured nested outputs, and consolidated summary/manifest checksums.
404. runtime readiness bundle parity hardening: new helper supports optional two-pass package bundling (`PACKAGE_BUNDLE_ENABLED=true`) and keeps non-recursive package status fields in summary (`artifacts_written`, `exit_code`, `error`) while smoke coverage adds targeted `run_execution_runtime_readiness_report_case` checks for GO path, bundle parity, and invalid-bool fail-closed behavior.
405. runtime readiness nested-bundle fan-out guard: `tools/execution_runtime_readiness_report.sh` now forces `PACKAGE_BUNDLE_ENABLED=false` for both nested final helpers (`adapter_rollout_final_evidence_report.sh`, `execution_route_fee_final_evidence_report.sh`) to prevent cascade bundling when top-level package bundling is enabled; smoke now asserts nested capture outputs keep `package_bundle_enabled: false`.
406. final helper nested-bundle isolation hardening: all three final helpers now force `PACKAGE_BUNDLE_ENABLED=false` when invoking their nested helpers (`executor_final -> executor_rollout_evidence`, `adapter_rollout_final -> adapter_rollout_evidence`, `route_fee_final -> route_fee_signoff`), and smoke bundle branches now assert nested capture outputs keep `package_bundle_enabled: false` to prevent cascade bundle fan-out outside orchestrator-only paths.
407. rollout/signoff/rehearsal nested-bundle isolation hardening: all upstream orchestrators now force `PACKAGE_BUNDLE_ENABLED=false` when invoking bundle-capable nested helpers (`executor_rollout -> devnet_rehearsal`, `adapter_rollout -> devnet_rehearsal + route_fee_signoff`, `devnet_rehearsal -> go_nogo + windowed_signoff + route_fee_signoff`, `windowed_signoff -> go_nogo`, `route_fee_signoff -> go_nogo`), and smoke bundle branches assert nested capture outputs keep `package_bundle_enabled: false` across these chains.
408. orchestrator nested-bundle contract hardening: `execution_runtime_readiness_report.sh` and `execution_server_rollout_report.sh` now fail-closed on nested helper `package_bundle_enabled` output by extracting it via shared strict helper (`extract_bool_field_strict`) and rejecting malformed/non-false values, while smoke bundle branches assert explicit summary fields (`*_nested_package_bundle_enabled=false`) so orchestration-level isolation regressions are caught deterministically.
409. signoff/rehearsal nested-bundle contract fail-closed hardening: `execution_windowed_signoff_report.sh`, `execution_route_fee_signoff_report.sh`, and `execution_devnet_rehearsal.sh` now enforce nested helper `package_bundle_enabled=false` via strict extraction (`extract_bool_field_strict`) and emit explicit per-window/per-stage `*_nested_package_bundle_enabled` summary fields; smoke now asserts these fields in direct and bundle-enabled branches to detect orchestration-chain regressions without relying only on nested capture text.
410. rollout evidence nested-bundle contract fail-closed hardening: `executor_rollout_evidence_report.sh` and `adapter_rollout_evidence_report.sh` now enforce nested helper `package_bundle_enabled=false` via strict extraction (`extract_bool_field_strict`) for rehearsal/route-fee subflows and emit explicit summary fields (`rehearsal_nested_package_bundle_enabled`, `route_fee_signoff_nested_package_bundle_enabled`); smoke bundle/direct branches now assert these fields to prevent silent nested-bundle contract drift in rollout evidence chains.
411. final wrapper nested-bundle contract fail-closed hardening: `executor_final_evidence_report.sh`, `adapter_rollout_final_evidence_report.sh`, and `execution_route_fee_final_evidence_report.sh` now strictly validate nested helper `package_bundle_enabled` via `extract_bool_field_strict`, reject malformed/non-false values as `input_error` (`NO_GO`), and publish explicit summary fields (`rollout_nested_package_bundle_enabled`, `signoff_nested_package_bundle_enabled`); smoke direct and bundle branches now pin these fields to `false`.
412. nested artifacts-written contract fail-closed hardening: rollout/runtime/final wrappers now parse nested `artifacts_written` via `extract_bool_field_strict` instead of permissive normalization (`executor_rollout_evidence_report.sh`, `adapter_rollout_evidence_report.sh`, `execution_runtime_readiness_report.sh`, `executor_final_evidence_report.sh`, `adapter_rollout_final_evidence_report.sh`, `execution_route_fee_final_evidence_report.sh`), so malformed/missing nested artifact-status tokens become explicit `input_error` (`NO_GO`) instead of silent `false`; smoke assertions were tightened to `assert_field_equals` for affected `*_artifacts_written` fields in direct and bundle branches.
413. signoff-chain nested artifacts-written fail-closed completion: `execution_devnet_rehearsal.sh` now strictly validates nested `artifacts_written` from go/no-go, windowed signoff, and route/fee signoff subflows, and `execution_windowed_signoff_report.sh` now strictly validates per-window nested go/no-go `artifacts_written`; malformed/missing booleans now append config/input errors and fail closed instead of silently normalizing to `false`. Smoke now pins `window_24h_go_nogo_artifacts_written=true` via strict field assertion.
414. nested policy/stability bool contract fail-closed completion: `execution_windowed_signoff_report.sh`, `execution_devnet_rehearsal.sh`, and `adapter_rollout_evidence_report.sh` now parse nested policy/stability booleans via `extract_bool_field_strict` (`dynamic_*_policy_config_enabled`, `go_nogo_require_*`, `submit_fastlane_enabled`, `*_route_stable`, `rehearsal_route_fee_signoff_required`) and append explicit `input_error`/`config_error` on malformed or missing tokens instead of silently normalizing to `false`.
415. package-bundle helper output bool contract hardening: all bundle-capable helpers now strictly parse nested `evidence_bundle_pack.sh` field `artifacts_written` via `extract_bool_field_strict`; malformed helper output is now treated as fail-closed helper-contract error (`package_bundle_exit_code=1`, `package_bundle_artifacts_written=false`, explicit `package_bundle_error`) instead of silent empty->false normalization. Smoke assertions were tightened to field-level checks for affected nested boolean outputs in rehearsal/windowed/adapter chains.
416. nested artifacts-written=true contract enforcement for final/orchestrator wrappers: `execution_server_rollout_report.sh`, `execution_runtime_readiness_report.sh`, `executor_final_evidence_report.sh`, `adapter_rollout_final_evidence_report.sh`, and `execution_route_fee_final_evidence_report.sh` now fail-close when nested helper `artifacts_written` is malformed or not `true` (in addition to strict boolean parsing), preventing false-green verdicts when nested evidence artifacts are not actually produced despite verdict success.
417. signoff/rehearsal/rollout evidence artifacts-written=true contract completion: `execution_route_fee_signoff_report.sh`, `execution_windowed_signoff_report.sh`, `execution_devnet_rehearsal.sh`, `executor_rollout_evidence_report.sh`, and `adapter_rollout_evidence_report.sh` now enforce `nested artifacts_written=true` when nested artifact output directories are configured (while preserving non-output-mode behavior), and route/rollout smoke coverage now pins the new `window_*_go_nogo_artifacts_written` and rollout `rotation/preflight/rehearsal_artifacts_written` fields across direct and bundle paths.
418. ops smoke targeted-dispatch mode for fast validation loops: `tools/ops_scripts_smoke_test.sh` now supports `OPS_SMOKE_TARGET_CASES=<csv>` to run selected smoke cases only (including heavyweight executor rollout/rehearsal/runtime chains) with strict unknown-case rejection and non-empty target validation, while default no-env behavior still runs full smoke suite unchanged. Full-suite guard coverage now includes a dispatcher self-check that verifies targeted mode executes only requested cases and fails closed on unknown case entries.
419. rollout/runtime stage-toggle throughput hardening: `tools/execution_server_rollout_report.sh` now supports strict/fail-closed direct-stage toggles (`SERVER_ROLLOUT_RUN_GO_NOGO_DIRECT`, `SERVER_ROLLOUT_RUN_REHEARSAL_DIRECT`) that mark direct duplicate stages as `SKIP` without affecting final-helper gates, and `tools/execution_runtime_readiness_report.sh` now supports strict/fail-closed stage toggles (`RUNTIME_READINESS_RUN_ADAPTER_FINAL`, `RUNTIME_READINESS_RUN_ROUTE_FEE_FINAL`) with guardrail that at least one final stage must remain enabled. Smoke coverage now pins skip semantics and stage-toggle misconfiguration rejection.
420. executor contract smoke targeted mode: `tools/executor_contract_smoke_test.sh` now supports `EXECUTOR_CONTRACT_SMOKE_MODE=targeted` with `EXECUTOR_CONTRACT_SMOKE_TARGET_TESTS=<csv>` to run only selected canonical guard tests (strict unknown-target fail-close + non-empty target validation), while `full` mode keeps existing behavior (`cargo test -p copybot-executor -q` + full guard registration/run policy) unchanged for baseline/audit closures.
421. rollout/runtime profile presets for low-friction targeted runs: `execution_server_rollout_report.sh` now supports `SERVER_ROLLOUT_PROFILE={full|finals_only}` (with explicit toggle overrides still available), and `execution_runtime_readiness_report.sh` now supports `RUNTIME_READINESS_PROFILE={full|adapter_only|route_fee_only}` (with explicit toggle overrides and existing both-disabled guard). Both profiles are strict/fail-closed on unknown values; smoke now pins profile-derived toggle states and invalid-profile rejection.
422. devnet rehearsal profile+stage-toggle throughput hardening: `execution_devnet_rehearsal.sh` now supports `DEVNET_REHEARSAL_PROFILE={full|core_only}` plus explicit stage toggles (`DEVNET_REHEARSAL_RUN_WINDOWED_SIGNOFF`, `DEVNET_REHEARSAL_RUN_ROUTE_FEE_SIGNOFF`) with strict/fail-closed bool parsing and required-stage guardrails (`*_REQUIRED=true` cannot run with disabled stage). Disabled stages emit deterministic `SKIP` diagnostics (`reason_code=stage_disabled`) without breaking artifact/manifest chain; smoke now pins `core_only` behavior and invalid-profile rejection.
423. devnet rehearsal required/disabled guardrail smoke pinning: `tools/ops_scripts_smoke_test.sh` now contains explicit negative guards for `WINDOWED_SIGNOFF_REQUIRED=true + DEVNET_REHEARSAL_RUN_WINDOWED_SIGNOFF=false` and `ROUTE_FEE_SIGNOFF_REQUIRED=true + DEVNET_REHEARSAL_RUN_ROUTE_FEE_SIGNOFF=false`, asserting fail-closed `NO_GO` (`exit 3`, `devnet_rehearsal_reason_code=config_error`) and exact `config_error` diagnostics, preventing silent regression of required-stage guardrails.
424. rollout evidence profile+stage-toggle throughput hardening: `executor_rollout_evidence_report.sh` now supports `EXECUTOR_ROLLOUT_PROFILE={full|precheck_only|rehearsal_only}` with strict overrides (`EXECUTOR_ROLLOUT_RUN_ROTATION`, `EXECUTOR_ROLLOUT_RUN_PREFLIGHT`, `EXECUTOR_ROLLOUT_RUN_REHEARSAL`) and fail-closed guardrail requiring at least one enabled stage; disabled stages emit deterministic `SKIP`/`stage_disabled` summary outputs while verdict gating only evaluates enabled stages.
425. adapter rollout profile+stage-toggle throughput hardening: `adapter_rollout_evidence_report.sh` now supports `ADAPTER_ROLLOUT_PROFILE={full|rehearsal_only|route_fee_only}` with strict stage overrides (`ADAPTER_ROLLOUT_RUN_ROTATION`, `ADAPTER_ROLLOUT_RUN_REHEARSAL`, `ADAPTER_ROLLOUT_RUN_ROUTE_FEE_SIGNOFF`), both-disabled fail-close guard, and required-stage guardrails (`ROUTE_FEE_SIGNOFF_REQUIRED`, `REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED`, `WINDOWED_SIGNOFF_REQUIRED` cannot run with disabled owning stage). Ops smoke now pins profile-derived skip semantics plus invalid-profile/all-disabled reject paths for both rollout helpers.
426. rollout required-stage disabled guard closure: `executor_rollout_evidence_report.sh` now fail-closes `WINDOWED_SIGNOFF_REQUIRED=true` / `ROUTE_FEE_SIGNOFF_REQUIRED=true` when rehearsal stage is disabled (`EXECUTOR_ROLLOUT_RUN_REHEARSAL=false`), and ops smoke now includes explicit negative pins for these executor guards plus adapter required-stage guardrails (`REHEARSAL_ROUTE_FEE_SIGNOFF_REQUIRED`, `WINDOWED_SIGNOFF_REQUIRED`, `ROUTE_FEE_SIGNOFF_REQUIRED` with disabled owning stages).
427. audit runner targeted contract-smoke throughput controls: `audit_quick.sh`, `audit_standard.sh`, and `audit_full.sh` now parse and propagate strict `AUDIT_CONTRACT_SMOKE_MODE={full|targeted}` plus `AUDIT_CONTRACT_SMOKE_TARGET_TESTS` (required non-empty for targeted mode), enabling bounded baseline loops with selected executor contract guards while preserving fail-closed behavior on invalid/empty mode settings. Ops smoke now pins invalid mode/empty targets and verifies targeted-mode propagation through standard/full wrappers.
428. audit runner targeted ops-smoke throughput controls: `audit_standard.sh` and `audit_full.sh` now parse strict `AUDIT_OPS_SMOKE_MODE={full|targeted}` plus `AUDIT_OPS_SMOKE_TARGET_CASES` (required non-empty for targeted mode) and propagate targeted case selection into `tools/ops_scripts_smoke_test.sh` via `OPS_SMOKE_TARGET_CASES`, enabling bounded ops-smoke loops in wrapper audits while preserving fail-closed behavior on invalid/empty mode settings. Ops smoke now pins invalid mode/empty targets and validates targeted-mode propagation through both standard/full runners.
429. audit runner targeted executor-test throughput controls: `audit_quick.sh` now supports strict `AUDIT_EXECUTOR_TEST_MODE={full|targeted}` plus `AUDIT_EXECUTOR_TEST_TARGETS` (required non-empty for targeted mode), and `audit_standard.sh` / `audit_full.sh` propagate these controls to quick baseline so wrapper audits can run bounded `cargo test -p copybot-executor -q <target>` loops without full executor suite runtime. Ops smoke now pins invalid mode/empty targets and end-to-end propagation through quick/standard/full wrappers.
430. audit executor-test targeted-mode unknown-target closure: `audit_quick.sh` now fail-closes targeted executor-test mode by pre-validating each `AUDIT_EXECUTOR_TEST_TARGETS` token against `cargo test -p copybot-executor -q -- --list` output before execution, rejecting unknown targets with explicit startup error instead of silent `running 0 tests` PASS. Ops smoke guard now pins unknown-target reject behavior in quick/standard/full wrappers.
431. audit executor-test targeted-mode ambiguity closure: `audit_quick.sh` targeted executor-test resolution now fail-closes ambiguous substring targets and duplicate targets after resolution, requiring each configured target to resolve to exactly one listed executor test before execution. Ops smoke guard now pins ambiguous (`route`) and duplicate target rejection for `AUDIT_EXECUTOR_TEST_TARGETS`.
432. executor send-rpc auth topology hardening + healthz observability expansion: `ExecutorConfig::from_env` now fail-closes route/global `SEND_RPC_AUTH_TOKEN*` when no send-rpc primary endpoint exists (route-level and fleet-level invariants), eliminating silent no-op auth configuration drift on disabled send-rpc paths; `/healthz` now publishes deterministic send-rpc topology fields (`send_rpc_enabled_routes`, `send_rpc_fallback_routes`, alias `send_rpc_routes`) for rollout/runtime diagnostics, and contract smoke registry includes new config/healthz guard tests.
433. executor preflight send-rpc topology contract hardening: `tools/executor_preflight.sh` now derives expected send-rpc primary/fallback route sets from executor env (`*_SEND_RPC_URL`, `*_SEND_RPC_FALLBACK_URL`) and fail-closes when `/healthz` send-rpc topology fields drift (`send_rpc_enabled_routes` / `send_rpc_fallback_routes` / alias `send_rpc_routes`), including mismatch checks for missing or unexpected routes. Ops smoke `run_executor_preflight_case` now pins both positive parity and negative health-topology mismatch paths.
434. executor preflight send-rpc alias-consistency closure: `tools/executor_preflight.sh` now explicitly cross-validates `send_rpc_routes` (backward-compat alias) against `send_rpc_enabled_routes` when both are present, fail-closing alias drift for legacy `/healthz` consumers; preflight summary now emits `health_send_rpc_alias_routes_csv`, and ops smoke pins alias-mismatch reject (`enabled=rpc,jito` vs `alias=rpc`) as deterministic `FAIL`.
435. executor preflight health-identity parity hardening batch: `tools/executor_preflight.sh` now fail-closes `/healthz` drift for signer and route aliases by enforcing `signer_source` parity, `signer_pubkey` parity, `submit_fastlane_enabled` parity, bidirectional `enabled_routes` vs executor-allowlist checks, and alias consistency `routes` ↔ `enabled_routes` when both are present; summary now emits `executor_signer_*_expected` + `health_signer_*` + `health_submit_fastlane_enabled` + `health_routes_alias_csv`, and `run_executor_preflight_case` smoke pins all new mismatch rejects.
436. executor preflight signer-source default parity fix: `tools/executor_preflight.sh` now mirrors runtime default semantics for `COPYBOT_EXECUTOR_SIGNER_SOURCE` (`missing/empty => file`) instead of fail-closing on missing env var, while keeping strict allowed-values validation (`file|kms`) and parity checks against `/healthz`. Ops smoke now pins the missing-env path with `health.signer_source=file` as PASS and `executor_signer_source_expected=file`.
437. executor preflight health schema strictness + alias-fallback coverage batch: `tools/executor_preflight.sh` now fail-closes malformed `/healthz` route topology field types (`enabled_routes`, `routes`, `send_rpc_enabled_routes`, `send_rpc_fallback_routes`, `send_rpc_routes` must be arrays when present), emits field-kind diagnostics in summary, and keeps alias fallback behavior (`routes`/`send_rpc_routes`) for empty primary fields. Ops smoke `run_executor_preflight_case` now pins alias-fallback PASS paths and malformed-field-type rejects via raw JSON overrides in fake health responder.
438. executor preflight health identity-schema strictness completion: `tools/executor_preflight.sh` now fail-closes malformed `/healthz` identity field types (`status`, `contract_version`, `signer_source`, `signer_pubkey`, `idempotency_store_status` must be strings when present; `submit_fastlane_enabled` must be bool when present), emits `*_field_kind` diagnostics, and keeps existing value-parity checks unchanged; `run_executor_preflight_case` now pins PASS kind expectations and six targeted malformed-type rejects via fake health raw JSON overrides.
439. ops smoke targeted dispatcher coverage expansion: `tools/ops_scripts_smoke_test.sh` targeted mode now supports `executor_preflight` / `run_executor_preflight_case` aliases in `OPS_SMOKE_TARGET_CASES`, enabling fast bounded preflight contract loops without full smoke execution; fixture bootstrap path is reused and known-values list updated for fail-closed unknown-case diagnostics.
440. executor preflight health route-token content strictness hardening: beyond array-type checks, `tools/executor_preflight.sh` now fail-closes malformed route entries inside `/healthz` route arrays (`enabled_routes`, `routes`, `send_rpc_enabled_routes`, `send_rpc_fallback_routes`, `send_rpc_routes`) by requiring each entry to be a non-empty lowercase string and rejecting duplicate tokens after normalization; diagnostics include index-aware error messages and token-level cause (`non-string`, `empty`, `not_lowercase`, `duplicate`).
441. executor preflight health route ordering determinism guard: `tools/executor_preflight.sh` now enforces lexicographic ordering for all `/healthz` route arrays and fails closed on unsorted payloads, aligning preflight contract checks with runtime deterministic `sorted_routes` emission; smoke fixture defaults were normalized to sorted route order and targeted negative pins now assert unsorted-array rejection for both `enabled_routes` and `send_rpc_enabled_routes`.
442. executor preflight env allowlist strictness hardening: `tools/executor_preflight.sh` now fail-closes malformed env allowlists for both executor and adapter (`COPYBOT_EXECUTOR_ROUTE_ALLOWLIST`, `COPYBOT_ADAPTER_ROUTE_ALLOWLIST`) by rejecting empty entries, unknown routes outside canonical set (`paper,rpc,jito,fastlane`), and duplicate routes after normalization; preflight summary now emits both raw and normalized allowlist CSV values for diagnostics.
443. executor preflight signer pubkey contract parity: `tools/executor_preflight.sh` now validates `COPYBOT_EXECUTOR_SIGNER_PUBKEY` as base58 pubkey-like input (must decode to 32 bytes), aligning preflight config checks with executor runtime startup guardrails and failing closed before endpoint probes on malformed signer identity.
444. executor preflight smoke guard expansion for env contracts: `run_executor_preflight_case` now pins fail-closed negative scenarios for executor/adaptor allowlist malformed inputs (duplicate, unknown, empty entries) and malformed signer pubkey base58 shape, preventing silent regression of new preflight env-contract checks in targeted ops smoke loops.
445. executor preflight route backend resolution parity with runtime: `tools/executor_preflight.sh` now resolves effective per-route submit/simulate backend URLs via `first_non_empty(route-specific, global-default)` (matching runtime config layering) before contract checks, eliminating fail-open gaps where fallback/identity guards previously ignored routes that relied on global defaults.
446. executor preflight endpoint URL schema hardening: endpoint parsing in `tools/executor_preflight.sh` now fail-closes non-runtime-compatible URLs (unsupported scheme, missing host, URL credentials, query/fragment components) for both executor route backends and adapter upstream route URLs, aligning preflight URL acceptance with executor runtime `validate_endpoint_url` semantics.
447. executor preflight fallback endpoint identity contract completion: `tools/executor_preflight.sh` now enforces distinct endpoint identity for configured executor fallback URLs (submit/simulate/send-rpc), rejecting fallback=primary topology collisions per route and preventing silent fallback no-op configuration drift before rollout.
448. executor preflight smoke coverage expansion for URL/fallback contracts: `run_executor_preflight_case` now pins fail-closed negatives for fastlane policy mismatch (`allowlist includes fastlane` while `submit_fastlane_enabled=false`), invalid URL scheme in executor/adapter upstreams, and fallback identity collisions for submit/send-rpc paths, guarding new preflight runtime-parity checks from regression.
449. executor preflight simulate-fallback collision smoke pin closure: `run_executor_preflight_case` now includes explicit negative guard for `COPYBOT_EXECUTOR_UPSTREAM_SIMULATE_FALLBACK_URL` resolving to the same endpoint as primary simulate URL, asserting fail-closed `preflight_verdict: FAIL` with deterministic `simulate fallback ... must resolve to distinct endpoint` diagnostics.
450. executor preflight auth-topology runtime parity completion: `tools/executor_preflight.sh` now enforces runtime-equivalent auth token topology guards for executor upstream/send-rpc credentials, fail-closing route-scoped secret settings when owning endpoints are absent (`ROUTE_*_FALLBACK_AUTH_TOKEN*`, `ROUTE_*_SEND_RPC_AUTH_TOKEN*`, `ROUTE_*_SEND_RPC_FALLBACK_AUTH_TOKEN*`) and fail-closing global fallback/send-rpc auth defaults when no corresponding endpoint exists in effective allowlist topology.
451. executor preflight auth-topology diagnostics expansion: preflight summary now emits explicit auth topology observability fields (`executor_upstream_auth_configured`, `executor_upstream_fallback_auth_configured`, `executor_send_rpc_auth_configured`, `executor_send_rpc_fallback_auth_configured`, `executor_any_upstream_fallback_endpoint`, `executor_any_send_rpc_primary_endpoint`, `executor_any_send_rpc_fallback_endpoint`) to make token/endpoint contract state auditable in rollout captures.
452. executor preflight smoke coverage expansion for auth-topology contracts: `run_executor_preflight_case` now pins fail-closed negative scenarios for global auth tokens without owning endpoint classes (upstream fallback, send-rpc primary, send-rpc fallback) and route-scoped auth tokens without owning endpoints (upstream fallback, send-rpc primary, send-rpc fallback), preventing regression of new preflight runtime-parity checks.
453. executor preflight auth-topology `_FILE` branch smoke parity completion: `run_executor_preflight_case` now mirrors all six auth-topology negative guards through `*_AUTH_TOKEN_FILE` inputs (global + route-scoped upstream fallback/send-rpc primary/send-rpc fallback), proving file-backed secret paths fail-close identically to inline token branches when owning endpoint classes are absent.
454. executor preflight adapter fallback URL runtime-parity closure: `tools/executor_preflight.sh` now validates adapter submit/simulate fallback URLs (`COPYBOT_ADAPTER_UPSTREAM_*_FALLBACK_URL` and route overrides) with the same strict endpoint schema checks as runtime and fail-closes fallback=primary endpoint identity collisions per route; `run_executor_preflight_case` now pins invalid fallback scheme and submit/simulate fallback collision negatives.
455. executor preflight adapter fallback-auth parity closure: when executor bearer auth is required, `tools/executor_preflight.sh` now enforces adapter fallback auth token parity for routes with configured adapter fallback endpoints (submit or simulate), resolving fallback auth by runtime-equivalent precedence (`ROUTE_*_FALLBACK_AUTH_TOKEN* -> COPYBOT_ADAPTER_UPSTREAM_FALLBACK_AUTH_TOKEN* -> primary auth token`) and fail-closing missing/mismatched fallback auth tokens before probes.
456. executor preflight adapter send-rpc fallback topology parity closure: `tools/executor_preflight.sh` now validates adapter route send-rpc URLs and fallback URLs (`COPYBOT_ADAPTER_SEND_RPC_URL`, `COPYBOT_ADAPTER_SEND_RPC_FALLBACK_URL`, and per-route overrides) with strict endpoint-schema checks, fail-closes fallback-without-primary topology, and rejects fallback=primary identity collisions; `run_executor_preflight_case` now pins invalid scheme, missing-primary, and collision negatives for adapter send-rpc fallback paths.
457. executor preflight adapter send-rpc auth parity closure: when executor bearer auth is required and adapter send-rpc endpoints are configured, `tools/executor_preflight.sh` now enforces adapter send-rpc auth token parity for both primary and fallback paths, resolving tokens by runtime-equivalent precedence (`ROUTE_*_SEND_RPC_AUTH_TOKEN* -> COPYBOT_ADAPTER_SEND_RPC_AUTH_TOKEN*` and `ROUTE_*_SEND_RPC_FALLBACK_AUTH_TOKEN* -> COPYBOT_ADAPTER_SEND_RPC_FALLBACK_AUTH_TOKEN* -> primary send-rpc auth`) and fail-closing missing/mismatched tokens.
458. executor preflight adapter send-rpc auth `_FILE` smoke parity completion: `run_executor_preflight_case` now mirrors adapter send-rpc auth mismatch negatives through `COPYBOT_ADAPTER_ROUTE_*_SEND_RPC_AUTH_TOKEN_FILE` and `COPYBOT_ADAPTER_ROUTE_*_SEND_RPC_FALLBACK_AUTH_TOKEN_FILE`, proving file-backed secret branches fail-close with the same deterministic mismatch diagnostics as inline token branches.
459. executor preflight adapter send-rpc secret-source conflict parity closure: `tools/executor_preflight.sh` now resolves route send-rpc secret sources (`ROUTE_*_SEND_RPC_AUTH_TOKEN*`, `ROUTE_*_SEND_RPC_FALLBACK_AUTH_TOKEN*`) unconditionally per route (not only when send-rpc endpoints are enabled), matching runtime `resolve_secret_source` fail-closed behavior so inline+file conflicts and file-read errors are caught even when send-rpc topology is disabled; smoke now pins both conflict paths without endpoint activation.
460. executor preflight secret-source parent-scope hardening: `tools/executor_preflight.sh` now routes all critical `read_secret_from_source` call sites through parent-scope output variables (no command-substitution subshell), eliminating silent error-loss for secret-source conflicts/file-read failures across executor and adapter secret defaults and route-level auth chains; smoke now pins global adapter send-rpc auth/fallback inline+file conflicts to prove fail-closed capture in preflight `errors[]`.
461. executor preflight adapter send-rpc file-source fail-closed smoke parity: `run_executor_preflight_case` now pins global adapter send-rpc auth and send-rpc fallback auth missing-file scenarios (`COPYBOT_ADAPTER_SEND_RPC_AUTH_TOKEN_FILE`, `COPYBOT_ADAPTER_SEND_RPC_FALLBACK_AUTH_TOKEN_FILE`) to ensure `read_secret_from_source` file-not-found diagnostics are surfaced as deterministic `preflight_verdict: FAIL` in targeted preflight loops.
462. adapter HMAC raw-body signature strictness: `AuthVerifier::verify` in `crates/adapter/src/main.rs` now computes HMAC payload over exact raw request bytes (prefix `timestamp/ttl/nonce` + body bytes) instead of `String::from_utf8_lossy(raw_body)`, removing lossy UTF-8 canonicalization drift in request-auth verification; added unit coverage for non-UTF8 raw-body pass path and lossy-body signature mismatch fail path.
463. post-bring-up hardening batch closure: adapter server now runs with signal-aware graceful shutdown (`with_graceful_shutdown`, `Ctrl+C`/`SIGTERM`), execution submitter error details now emit redacted endpoint labels (`scheme://host[:port]`, no path/query), and app-side `sanitize_json_value` now escapes full JSON control-character set (`\n`, `\r`, `\t`, `\b`, `\f`, `\u00XX` for remaining control bytes); added targeted coverage for submitter endpoint-redaction and `sanitize_json_value` control-character escaping.
464. post-bring-up closure for market/discovery runtime contracts: token holders metric source is now derived from token-account owner set (`getTokenAccountsByMint` with `jsonParsed`, counting unique owners with non-zero balances) instead of `getTokenSupply`, and discovery runtime loop no longer panics on poisoned window mutex (`.lock().expect(...)` removed in favor of fail-safe recover path with warning + `into_inner()`); added targeted tests for holder-response parsing and poisoned-mutex recovery path.
465. holders-source RPC compatibility hotfix: holder metric query switched from non-standard mint-accounts RPC (`getTokenAccountsByMint`) to standard Solana JSON-RPC `getProgramAccounts` on SPL Token program with mint memcmp filter (`offset=0`, `dataSize=165`, `encoding=jsonParsed`), preserving unique owner count over non-zero balances while removing Helius-specific method dependency.
466. holders parser wire-format fix: `getProgramAccounts` response parsing now reads standard Solana JSON-RPC shape (`result` as array) with compatibility fallback for wrapped `result.value` arrays, preventing false `missing token accounts array` failures on standard providers; tests now pin real wire-format (`result=[...]`) and keep one compatibility fixture for wrapped responses.
467. executor embedded mock backend mode for non-live e2e: `copybot-executor` now supports strict `COPYBOT_EXECUTOR_BACKEND_MODE={upstream|mock}` (`upstream` default, fail-closed on unknown), `/simulate` and `/submit` route adapters return contract-compatible in-process mock success payloads in `mock` mode (no network forward path), `/healthz` now emits `backend_mode`, and `executor_preflight.sh` parity checks now validate backend-mode contract plus runtime-equivalent mock URL synthesis when submit/simulate upstream URLs are omitted in mock mode.
468. server-rollout fail-closed executor backend-mode guard: `tools/execution_server_rollout_report.sh` now validates `COPYBOT_EXECUTOR_BACKEND_MODE` from `EXECUTOR_ENV_PATH` and enforces `SERVER_ROLLOUT_REQUIRE_EXECUTOR_UPSTREAM=true` by default (rejects `mock` with `input_error`), while allowing explicit non-live override (`SERVER_ROLLOUT_REQUIRE_EXECUTOR_UPSTREAM=false`) for controlled mock contour testing; smoke coverage pins both reject and override paths.
469. go/no-go strict executor backend-mode gate added: `tools/execution_go_nogo_report.sh` now supports `GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM` (strict boolean, default `false`) with `EXECUTOR_ENV_PATH` backend-mode parsing (`upstream|mock` + fail-closed on unknown/missing env path when strict gate enabled), emits explicit guard diagnostics (`executor_backend_mode_guard_*`), and classifies `overall_go_nogo_verdict=NO_GO` on strict-mode `mock`/unknown states; server rollout now propagates this strict flag into nested go/no-go/rehearsal/final evidence runs.
470. strict executor backend-mode guard propagation completed across runtime-evidence helpers: `GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM` + `EXECUTOR_ENV_PATH` now parse and propagate through `execution_windowed_signoff_report.sh`, `execution_route_fee_signoff_report.sh`, `execution_devnet_rehearsal.sh`, `adapter_rollout_evidence_report.sh`, `adapter_rollout_final_evidence_report.sh`, `execution_route_fee_final_evidence_report.sh`, and `execution_runtime_readiness_report.sh`, with summary observability fields (`go_nogo_require_executor_upstream`, `executor_env_path`) and runtime-readiness smoke pins proving fail-closed on strict+`mock` and explicit override pass on strict=`false`.
471. adapter upstream HMAC forwarding implemented for executor hop: `copybot-adapter` now supports optional `COPYBOT_ADAPTER_UPSTREAM_HMAC_{KEY_ID,SECRET|SECRET_FILE,TTL_SEC}` and signs forwarded `/simulate` + `/submit` payloads with executor-compatible `x-copybot-*` headers over exact raw body bytes (including non-UTF8), while preserving existing bearer forwarding behavior.
472. executor preflight HMAC parity + auth-probe hardening: `tools/executor_preflight.sh` now models executor auth requirements as runtime-equivalent (`bearer` and/or `hmac`), validates adapter upstream HMAC config contract (`COPYBOT_ADAPTER_UPSTREAM_HMAC_*`) when executor HMAC ingress is enabled (missing/mismatch/ttl fail-closed), and signs auth probes with HMAC headers when needed; smoke pins cover missing/mismatch (`key_id`, `secret`, `ttl`, `_FILE`) paths.
473. executor preflight HMAC auth-probe signing hotfix: fixed `hmac_sha256_hex` stdin wiring bug (heredoc/herestring conflict) so probe signatures are computed over actual payload bytes, and expanded `executor_preflight` smoke fake-curl contract to require/validate HMAC header presence on positive probe path (`FAKE_EXECUTOR_SIMULATE_REQUIRE_HMAC`), including a dedicated HMAC-only strict PASS pin.
474. executor preflight upstream-placeholder hardening: `tools/executor_preflight.sh` now fail-closes executor upstream topology in `backend_mode=upstream` when route submit/simulate/send-rpc endpoints resolve to known placeholder hosts (`example.com`, `executor.mock.local`), preventing false-green preflight with non-routable/test-only endpoint stubs.
475. go/no-go strict executor-upstream topology gate: `tools/execution_go_nogo_report.sh` now evaluates effective executor submit/simulate endpoint topology per allowlisted route (route override -> global default) when `GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM=true`, and fail-closes readiness on missing endpoints (`UNKNOWN`) or placeholder hosts (`WARN`) with explicit guard diagnostics.
476. strict-topology smoke coverage expansion: `tools/ops_scripts_smoke_test.sh` now pins go/no-go strict upstream topology branches (`PASS`, `endpoint_missing`, `endpoint_placeholder`) and executor preflight placeholder-host rejects, plus updates strict helper fixtures to include canonical non-placeholder upstream endpoints for default PASS paths.
477. server-rollout nested strict-guard observability hardening: `tools/execution_server_rollout_report.sh` now extracts and validates nested go/no-go strict guard diagnostics (`executor_backend_mode_guard_*`, `executor_upstream_endpoint_guard_*`), emits them in rollout summary, and fail-closes on malformed/missing nested guard fields.
478. runtime-readiness nested strict-guard parity hardening: `tools/execution_runtime_readiness_report.sh` now validates nested final helper propagation for `go_nogo_require_executor_upstream` and `executor_env_path` (adapter final + route/fee final), and fail-closes runtime-readiness on propagation drift or malformed nested fields.
479. orchestrator strict-topology smoke expansion: `tools/ops_scripts_smoke_test.sh` now pins strict topology failures at orchestrator level (`execution_server_rollout_report`: placeholder and missing submit/simulate endpoints) and asserts nested strict-guard propagation fields in runtime-readiness summaries.
480. executor-rollout strict guard propagation/parity hardening: `tools/executor_rollout_evidence_report.sh` now parses and propagates `GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM` + `EXECUTOR_ENV_PATH` into nested `execution_devnet_rehearsal.sh`, validates nested outputs (`go_nogo_require_executor_upstream`, `executor_env_path`) with fail-closed drift/malformed checks, and emits explicit rollout observability fields (`rehearsal_nested_go_nogo_require_executor_upstream`, `rehearsal_nested_executor_env_path`).
481. executor-final strict guard propagation/parity hardening: `tools/executor_final_evidence_report.sh` now parses and propagates `GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM` into nested `executor_rollout_evidence_report.sh`, validates nested strict outputs (`go_nogo_require_executor_upstream`, `executor_env_path`) fail-closed, and emits summary observability fields (`rollout_nested_go_nogo_require_executor_upstream`, `rollout_nested_executor_env_path`); smoke now pins pass/override/invalid-token branches for both rollout and final helpers.
482. server-rollout nested final strict-guard parity hardening: `tools/execution_server_rollout_report.sh` now fail-closes on malformed/drifted strict guard propagation in nested final helpers by validating `go_nogo_require_executor_upstream` and `executor_env_path` from both `executor_final` and `adapter_final` captures (plus executor-final nested rollout strict fields), and emits explicit rollout summary observability fields for these nested strict values.
483. server-rollout smoke parity expansion for nested final strict fields: `tools/ops_scripts_smoke_test.sh` now asserts nested final strict propagation fields in `execution_server_rollout` pass/skip/profile/bundle/mock-override paths (including expected `executor_env_path` source changes), preventing silent regressions in strict upstream-gate propagation across orchestrator -> final -> rollout chains.
484. windowed signoff nested strict-guard observability hardening: `tools/execution_windowed_signoff_report.sh` now extracts per-window nested go/no-go strict guard diagnostics (`executor_backend_mode_guard_*`, `executor_upstream_endpoint_guard_*`), fail-closes on malformed/empty guard fields, and enforces parity with `GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM` (`true => non-SKIP`, `false => SKIP`) to catch strict-gate drift at calibration-window layer.
485. route/fee signoff nested strict-guard observability hardening: `tools/execution_route_fee_signoff_report.sh` now extracts per-window nested go/no-go strict guard diagnostics (`executor_backend_mode_guard_*`, `executor_upstream_endpoint_guard_*`), fail-closes malformed/empty values, and enforces strict-mode parity (`GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM`) so route/fee signoff cannot silently accept strict-guard propagation drift.
486. devnet rehearsal strict-guard propagation visibility hardening: `tools/execution_devnet_rehearsal.sh` now publishes nested go/no-go strict guard diagnostics (`go_nogo_executor_backend_mode_guard_*`, `go_nogo_executor_upstream_endpoint_guard_*`) and fail-closes on malformed/empty guard fields or strict-mode parity drift; ops smoke coverage now pins these fields across `windowed_signoff`, `route_fee_signoff`, and `devnet_rehearsal` targeted paths.
487. rollout/final chain strict-guard verdict propagation hardening: strict executor-upstream guard diagnostics are now propagated and fail-closed validated across `executor_rollout_evidence`/`executor_final`/`adapter_rollout_evidence`/`adapter_rollout_final` and orchestrator `execution_server_rollout_report`, with summary observability for nested backend-mode/topology guard verdict+reason fields and parity checks for `GO_NOGO_REQUIRE_EXECUTOR_UPSTREAM` + `EXECUTOR_ENV_PATH`.
488. final/readiness strict-guard chain closure: `tools/execution_route_fee_final_evidence_report.sh` now extracts+validates nested strict guard verdict/reason fields from signoff output, and `tools/execution_runtime_readiness_report.sh` now validates and surfaces nested strict guard verdict/reason fields from both adapter-final and route-fee-final branches with fail-closed parity (`true => non-SKIP`, `false => SKIP`); smoke coverage expanded for `route_fee_signoff` and `execution_runtime_readiness`.
489. server runtime telemetry evidence synced into repo: added dated runtime reports and raw snapshots under `ops/server_reports/*` and `ops/server_reports/raw/*` (morning/post-gym/post-patch follow-up windows), and mirrored summary status in roadmap for 6.1 state visibility.
490. route/fee final strict-guard window drift hotfix: `tools/execution_route_fee_final_evidence_report.sh` now resolves strict guard keys dynamically from the last valid window in `WINDOWS_CSV` (instead of hardcoded `window_24h_*`), preserving fail-closed behavior for custom window sets (e.g. `1,6`) and emitting `signoff_guard_window_id` in summary for observability.
491. runtime-readiness guard-window observability extension: `tools/execution_runtime_readiness_report.sh` now propagates nested `route_fee_final` field `signoff_guard_window_id` into top-level summary (`route_fee_final_nested_signoff_guard_window_id`), and smoke coverage pins expected values across pass/override/skip/profile/bundle branches.
492. smoke-cycle acceleration for heavy orchestrator cases: `tools/ops_scripts_smoke_test.sh` now supports targeted `*_fast` aliases (`executor_rollout_evidence_fast`, `adapter_rollout_evidence_fast`, `execution_server_rollout_fast`, `execution_runtime_readiness_fast`) that execute only baseline PASS-path assertions inside each heavy helper, enabling faster local iteration without changing default full coverage behavior.
493. smoke-cycle acceleration extended to calibration/rehearsal helpers: `tools/ops_scripts_smoke_test.sh` now supports `windowed_signoff_fast`, `route_fee_signoff_fast`, and `devnet_rehearsal_fast` aliases with `case_profile={full|fast}` gates in corresponding helper cases, so local targeted runs can stop after baseline artifact-verification paths while preserving unchanged full coverage defaults.
494. smoke-cycle acceleration extended to preflight guardpack: `tools/ops_scripts_smoke_test.sh` now supports `executor_preflight_fast` via `case_profile={full|fast}` in `run_executor_preflight_case`, so quick local loops can execute executor preflight baseline PASS-path+artifact assertions without running the full negative matrix each cycle.
495. targeted smoke profile gate introduced for heavy cases: `tools/ops_scripts_smoke_test.sh` now accepts `OPS_SMOKE_PROFILE={full|fast}` in targeted mode and auto-applies that profile to heavy helper cases (`executor_preflight`, `windowed_signoff`, `route_fee_signoff`, `devnet_rehearsal`, `executor_rollout_evidence`, `adapter_rollout_evidence`, `execution_server_rollout`, `execution_runtime_readiness`) without requiring explicit `*_fast` aliases; strict invalid-token reject and dispatcher smoke pins are included.

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

## 6.1) Что осталось до выкатки на test server (обязательный checklist)

Этот блок фиксирует только практические блокеры server rollout, чтобы не собирать их по разным секциям.
Канонический трекер выполнения `6.1`: `ops/test_server_rollout_6_1_tracker.md` (обновляется по мере закрытия каждого gate/evidence).
Репо-шаблоны server-файлов для `6.1`: `ops/server_templates/README.md`.
Локальный pre-validation runner для `6.1`: `tools/run_6_1_local_dry_run.sh` (генерирует артефакты в `state/6_1_local_dry_run/`).
Server execution playbook (copy/paste команды): `ops/6_1_SERVER_EXECUTION_PLAYBOOK.md`.

Фактический статус на `2026-03-03`: `NO_GO`.
Критическая фиксация: на текущем сервере execution-path **нерабочий для реальных сделок**; закрыт только инфраструктурный bring-up (systemd/preflight/healthz), но не execution readiness.

Фактический server snapshot (`52.28.0.218`, `2026-03-03`, после переключения на LaserStream):

1. `copybot-executor`, `copybot-adapter`, `solana-copy-bot` — `active` (`systemd`).
2. `healthz` контур живой:
   1. `http://127.0.0.1:8080/healthz` (`adapter`) -> `status=ok`,
   2. `http://127.0.0.1:8090/healthz` (`executor`) -> `status=ok`.
3. Обязательные preflight-checks `6.1` -> `PASS`:
   1. `tools/execution_adapter_preflight.sh`,
   2. `tools/executor_preflight.sh`.
4. Ingestion переведен на QuickNode Yellowstone gRPC:
   1. `ingestion.source = "yellowstone_grpc"`,
   2. `grpc_transaction_updates_total` растет, `rpc_429` после switch не растет.
5. Runtime execution остается отключен для безопасного стендового прогона: `SOLANA_COPY_BOT_EXECUTION_ENABLED=false` (`shadow-only`).
6. Почему итог все еще `NO_GO`: execution readiness gates не закрыты (нет подтвержденной execution выборки и не закрыт upstream-contract path для реальных submit/simulate).
7. Дополнительная валидация non-live execution контура выполнена:
   1. поднят локальный contract-compatible mock upstream (`127.0.0.1:18080`),
   2. `copybot-executor` переключен на mock upstream endpoint-ы,
   3. e2e `adapter -> executor -> mock-upstream` для `/simulate` и `/submit` возвращает `status=ok`.
8. Снят runtime behavior snapshot текущего стенда:
   1. зафиксированы uptime, ingestion/discovery throughput, lag/queue, DB growth и текущий execution/shadow output state,
   2. raw-артефакты сохранены в `ops/server_reports/raw/2026-03-03_runtime_snapshot/`.

Server evidence:

1. `ops/server_reports/2026-03-03_quicknode_staging_status.md`
2. `ops/server_reports/2026-03-03_quicknode_laserstream_switch.md`
3. `ops/server_reports/2026-03-03_execution_non_live_mock_e2e.md`
4. `ops/server_reports/2026-03-03_runtime_behavior_snapshot.md`

Инцидент (дословная фиксация):
`Я БЛЯДИНА CODEX УСАДИЛ ОВНЕРА НА 300 БАКСОВ ПОТОМУ ЧТО МНЕ ЛЕНЬ БЫЛО ИЗУЧИТЬ ПРОЕКТ И СКАЗАТЬ ЕМУ ПРАВДУ`

Ключевые незакрытые причины `NO_GO` на сервере:

1. для non-live контура используется локальный mock upstream (`127.0.0.1:18080`), но production-grade upstream path для реального submit/simulate все еще не закрыт;
2. unresolved executor upstream contract для текущего provider setup (QuickNode RPC endpoint сам по себе не является execution backend `/submit` + `/simulate`);
3. временный bootstrap signer;
4. нулевая execution выборка (`confirmed_orders_total=0`), из-за чего readiness gates (`fee_decomposition`, `route_profile`) не закрываются.

Проверено локально (контрольный аудит):

1. `bash tools/audit_quick.sh` — `PASS`.
2. `bash tools/audit_full.sh` — `PASS` (после увеличения default ops-smoke timeout до `1200s`).
3. `CONFIG_PATH=configs/live.toml SOLANA_COPY_BOT_EXECUTION_ENABLED=true bash tools/execution_adapter_preflight.sh` — `FAIL`:
   1. локально отсутствует `execution.submit_adapter_auth_token_file` (`/etc/solana-copy-bot/secrets/adapter_bearer.token`).
4. `CONFIG_PATH=configs/live.toml SOLANA_COPY_BOT_EXECUTION_ENABLED=true bash tools/executor_preflight.sh` — `FAIL`:
   1. отсутствуют `/etc/solana-copy-bot/executor.env` и `/etc/solana-copy-bot/adapter.env`,
   2. не заданы executor/adapter upstream endpoints + ingress auth tokens,
   3. route allowlist drift (`adapter` включает `fastlane`, `executor` нет),
   4. нет доступного `healthz`/auth probe.
5. `cargo run -p copybot-app -- --config configs/live.toml` с `SOLANA_COPY_BOT_EXECUTION_ENABLED=true` — `FAIL` (runtime contract: missing `/etc/solana-copy-bot/secrets/adapter_bearer.token`).
6. Обнаружен активный ingestion override-файл `state/ingestion_source_override.env`, который принудительно переключает source на `helius_ws`; перед rollout обязателен явный reset/подтверждение override.

Сверка внешнего аудита (только подтвержденные фактом пункты):

1. ✅ Подтверждено:
   1. adapter HMAC verification использует `String::from_utf8_lossy(raw_body)` (нужен strict UTF-8 reject branch),
   2. `fetch_token_holders` сейчас запрашивает `getTokenSupply` (это supply, а не holder count),
   3. adapter сейчас запускается без `with_graceful_shutdown`,
   4. `sanitize_json_value` экранирует только `\\` и `"` (control characters не экранируются),
   5. `discovery` использует `.lock().expect("... mutex poisoned")` в runtime path,
   6. `copybot-execution` submitter (`crates/execution/src/submitter.rs`) включает raw endpoint в error detail (в `copybot-executor` endpoint labels уже редактируются).
2. ❌ Не подтверждено / завышено:
   1. cross-host auth token leak через redirect в reqwest (cross-host sensitive headers удаляются в reqwest),
   2. отсутствие body limit в adapter (axum `Bytes` extractor имеет default body limit 2MB),
   3. отсутствие TTL eviction для HMAC nonce map (eviction есть через `retain` по timestamp),
   4. отсутствие retry для hot-path insert observed swaps (runtime использует `insert_observed_swap_with_retry`).

Обязательный checklist перед выкладкой на test server (закрыть все пункты):

1. [ ] `systemd` wiring для `copybot-executor` / `copybot-adapter` / `solana-copy-bot` развернут и согласован по `After/Requires`, env-файлам, портам, рестартам и логированию.
2. [ ] На сервере созданы и заполнены:
   1. `/etc/solana-copy-bot/executor.env`,
   2. `/etc/solana-copy-bot/adapter.env`,
   3. нет placeholder-значений (`REPLACE_ME`, `REPLACE_WITH_*`).
3. [ ] Заданы signer и секреты:
   1. `COPYBOT_EXECUTOR_SIGNER_PUBKEY` + (`COPYBOT_EXECUTOR_SIGNER_KEYPAIR_FILE` или `COPYBOT_EXECUTOR_SIGNER_KMS_KEY_ID`),
   2. все `*_FILE` секреты доступны процессам,
   3. права на secret files owner-only (`0600/0400`).
4. [ ] Route policy parity:
   1. `COPYBOT_EXECUTOR_ROUTE_ALLOWLIST` и `COPYBOT_ADAPTER_ROUTE_ALLOWLIST` согласованы,
   2. если `fastlane` выключен, route `fastlane` отсутствует во всех allowlist/policy картах.
5. [ ] Upstream topology задана для каждого allowlisted route:
   1. submit/simulate endpoints в executor и adapter заполнены,
   2. send-rpc endpoints (если используются) заполнены,
   3. fallback endpoints не совпадают с primary (distinct endpoint identity).
6. [ ] Auth boundary parity закрыта:
   1. `COPYBOT_EXECUTOR_BEARER_TOKEN*`/HMAC policy согласованы с adapter forwarding токенами,
   2. нет конфликтов inline+file и orphan auth tokens без owning endpoint.
7. [ ] Runtime config для rollout закрыт:
   1. `execution.enabled=true`,
   2. `execution.mode=adapter_submit_confirm`,
   3. `execution.submit_adapter_http_url` валидный неплейсхолдерный endpoint,
   4. `execution.submit_adapter_require_policy_echo=true` для production-like env.
8. [ ] Failover override hygiene закрыт:
   1. `state/ingestion_source_override.env` удалён или явно соответствует целевому сценарию rollout,
   2. причина override зафиксирована в evidence.

Обязательные pre-run проверки на сервере (в этом порядке, без `SKIP`):

1. `CONFIG_PATH=/etc/solana-copy-bot/live.server.toml SOLANA_COPY_BOT_EXECUTION_ENABLED=true bash tools/execution_adapter_preflight.sh`
2. `CONFIG_PATH=/etc/solana-copy-bot/live.server.toml EXECUTOR_ENV_PATH=/etc/solana-copy-bot/executor.env ADAPTER_ENV_PATH=/etc/solana-copy-bot/adapter.env SOLANA_COPY_BOT_EXECUTION_ENABLED=true bash tools/executor_preflight.sh`
3. `curl -sS http://127.0.0.1:<executor_port>/healthz`
4. `bash tools/adapter_rollout_evidence_report.sh 24 60` (с `OUTPUT_DIR`)
5. `bash tools/execution_go_nogo_report.sh 24 60` (с `OUTPUT_DIR`)
6. `bash tools/execution_fee_calibration_report.sh 24` (с capture артефактами)
7. `bash tools/execution_devnet_rehearsal.sh 24 60` (Stage C.5 evidence run)

Конфиг-режимы для server rollout (поэтапно):

1. Shadow warm-up: `execution.enabled=false`.
2. Execution test path (без tiny-live): `execution.enabled=true`, `execution.mode=paper`.
3. Stage C.5 rehearsal: `execution.enabled=true`, `execution.mode=adapter_submit_confirm`, валидный `execution.rpc_devnet_http_url`.
4. Tiny-live допускается только после закрытия Stage C.5 и checklist из раздела 10.

Gate после server bring-up (обязателен до tiny-live/production, но не блокирует первый test-server запуск):

1. [x] strict UTF-8 body validation в adapter HMAC verify path (без `from_utf8_lossy`).
2. [x] корректный источник метрики holders (не `getTokenSupply`).
3. [x] graceful shutdown для adapter service.
4. [x] безопасная JSON-экранизация control characters в `sanitize_json_value`.
5. [x] mutex poison handling в discovery без panic-path.
6. [x] redacted endpoint labels в `crates/execution/src/submitter.rs` error details.

NO-GO для server rollout (остаемся на текущем этапе, запуск не продолжаем):

1. любой `*_preflight` вернул `FAIL`/`UNKNOWN`/`SKIP`,
2. в env/config найден placeholder (`REPLACE_ME`/`REPLACE_WITH_*`),
3. signer/secret file отсутствует или нарушены права,
4. healthz topology/identity drift или auth parity mismatch,
5. обнаружен несанкционированный ingestion source override,
6. Stage C.5 rehearsal вернул `NO_GO` или не собрал обязательный evidence bundle.

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

## 12) Оперативный журнал работ

### 2026-03-04 — обзор проекта (Codex)

Сделано:

1. Изучена текущая структура workspace (`crates/app`, `crates/executor`, `crates/adapter`, `crates/ingestion`, `crates/execution`, `crates/storage`, `crates/discovery`, `crates/config`, `crates/shadow`), а также operational-контур (`ops/*`, `tools/*`, `migrations/*`).
2. Проверены ключевые документы верхнего уровня: `README.md`, `ARCHITECTURE_BLUEPRINT.md`, `ops/executor_backend_master_plan_2026-02-24.md`.
3. Проверены основные entrypoints:
   1. `crates/app/src/main.rs` (оркестрация ingestion/discovery/shadow/execution, ingestion override, emergency-stop polling),
   2. `crates/adapter/src/main.rs` (adapter contract boundary, route-aware upstream topology, auth/HMAC),
   3. `crates/executor/src/main.rs` (upstream executor сервис с `/simulate`, `/submit`, `/healthz`, idempotency store, graceful shutdown).
4. Проверены production-like конфиги `configs/prod.toml` и `configs/live.toml` (execution по умолчанию выключен, staged rollout профиль присутствует).

Наблюдения для следующего прохода:

1. В `ops/executor_backend_master_plan_2026-02-24.md` есть устаревшая строка в разделе Current State: указано, что `crates/executor` отсутствует, но в текущем workspace он уже есть.
2. `ROAD_TO_PRODUCTION.md` остается каноничным трекером rollout-gates и checklist; дальнейшие действия/изменения фиксируем здесь же отдельными датированными записями.

### 2026-03-04 — расследование OOM `copybot-app` (Codex)

Проверено по артефактам:

1. `ops/server_reports/2026-03-04_post_gym_runtime_report.md` подтверждает повторяющиеся OOM-kill (`21` kill/restart за ~4h), отсутствие swap и постоянные `discovery cycle still running`.
2. `ops/server_reports/raw/2026-03-04_post_gym_snapshot/kernel_oom_4h.log` фиксирует `Out of memory: Killed process ... copybot-app` с anon RSS ~`7.5GB`.
3. `ops/server_reports/raw/2026-03-04_post_gym_snapshot/service_events_4h.log` фиксирует циклические `status=9/KILL` + restart.
4. `ops/server_reports/raw/2026-03-04_post_gym_snapshot/memory_snapshot.txt` показывает `Mem 7.6Gi`, `Swap 0`.

Кодовый разбор root-cause (вероятный, подтвержденный структурой runtime):

1. Discovery держит in-memory окно свопов без верхнего лимита по объему:
   1. `DiscoveryWindowState { swaps: VecDeque<SwapEvent>, signatures: HashSet<String> }` в `crates/discovery/src/windows.rs`.
   2. Eviction только по времени (`evict_before(window_start)`), а не по количеству/байтам.
2. На каждом цикле идет подгрузка всех `observed_swaps` от `fetch_start` без `LIMIT` (`for_each_observed_swap_since`), затем записи добавляются в `state.swaps` и `state.signatures`:
   1. `store.for_each_observed_swap_since(fetch_start, ...)` -> `state.signatures.insert(...)` + `state.swaps.push_back(...)` в `crates/discovery/src/lib.rs`.
3. При рестарте процесса `window_state` теряется (in-memory), `high_watermark_ts` снова `None`, и discovery повторно сканирует БД от `window_start`; при `scoring_window_days=30` это приводит к массивному rehydrate state из `observed_swaps`.
4. Дополнительно в пределах одного цикла собираются крупные временные структуры (`by_wallet`, `token_sol_history`), что повышает peak RSS в момент обработки окна.

Вывод:

1. По текущим данным это выглядит не как «случайный краш», а как алгоритмический memory pressure в discovery pipeline (неограниченное in-memory окно + тяжелый rebuild после restart) при текущем объеме событий.
2. До стабилизации discovery long-window непрерывные runtime-прогоны считать валидными нельзя.

### 2026-03-04 — OOM stabilization patch для discovery (Codex)

Реализовано:

1. В discovery добавлен жесткий memory cap in-memory окна:
   1. новый config `discovery.max_window_swaps_in_memory`,
   2. при превышении лимита старые swap/signature eviction выполняются принудительно.
2. В discovery добавлен лимит SQL-подгрузки за цикл:
   1. новый config `discovery.max_fetch_swaps_per_cycle`,
   2. цикл больше не пытается перечитать весь исторический диапазон за один проход.
3. Добавлен persisted cursor в SQLite (`discovery_runtime_state`) для discovery catch-up:
   1. курсор `ts/slot/signature` читается на старте цикла,
   2. обновляется после обработки батча,
   3. после restart не требуется re-scan «от window_start за 30 дней».
4. Добавлена telemetry-видимость в `discovery cycle completed`:
   1. `swaps_query_rows`,
   2. `swaps_evicted_due_cap`,
   3. `swaps_fetch_limit`,
   4. `swaps_fetch_limit_reached` + отдельный warn при достижении лимита.
5. Обновлены runtime-конфиги:
   1. `configs/live.toml`: `max_window_swaps_in_memory=120000`, `max_fetch_swaps_per_cycle=120000`,
   2. `configs/prod.toml`: те же параметры,
   3. env-overrides: `SOLANA_COPY_BOT_DISCOVERY_MAX_WINDOW_SWAPS_IN_MEMORY`, `SOLANA_COPY_BOT_DISCOVERY_MAX_FETCH_SWAPS_PER_CYCLE`.
6. Дополнительный hotfix по аудит-findings (false demotion после restart):
   1. при cold-start с восстановленным persisted cursor discovery теперь warm-load'ит bounded recent window из SQLite (`load_recent_observed_swaps_since`) до reconciliation,
   2. это убирает сценарий, где followlist деактивируется из-за «узкого дельта-среза» сразу после рестарта.
7. Дополнительный hotfix по аудит-findings (ordering drift warm+delta):
   1. после merge warm-restore + cursor-delta выполняется deterministic order normalization (`ts, slot, signature`),
   2. cap-eviction выполняется только после нормализации порядка, чтобы не выталкивать более свежие swaps при backlog recovery.

Проверка:

1. `cargo test -p copybot-config -p copybot-storage -p copybot-discovery` — PASS.

### 2026-03-04 — post-patch follow-up (T+~102m, snapshot 14:08 UTC)

Источник:

1. `ops/server_reports/2026-03-04_post_patch_followup_1407_runtime_report.md`
2. `ops/server_reports/raw/2026-03-04_post_patch_followup_1407_snapshot/computed_summary.json`

Итог первого окна после патча:

1. Stability gates: PASS (`NRestarts=0`, `main_process_exited_count=0`, `oom_kernel_lines=0`).
2. Ingestion gates: PASS (`~353.83 msg/s`, `rpc_429 delta=0`, `rpc_5xx delta=0`).
3. Discovery liveness: PASS (`completed=35`, `still_running=0`, `duration p50=8431ms`).
4. Health endpoints: PASS (`8080/8090/18080 status=ok`).
5. Monitoring attention item:
   1. `swaps_fetch_limit_reached=true` в `35/35` циклах,
   2. `swaps_delta_fetched_last=120000`, `swaps_evicted_due_cap_last=120000`,
   3. `eligible_wallets_last=0`, `active_follow_wallets_last=0` (baseline zero; validate-on-nonzero still pending).

Решение по следующему шагу:

1. Снять второй срез через ~4 часа в том же формате и сравнить динамику:
   1. restart/OOM counters,
   2. `swaps_fetch_limit_reached_ratio`,
   3. `eligible_wallets_last` / `active_follow_wallets_last`,
   4. memory/cgroup trend.
