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
6. Submit route policy в runtime уже enforce: route allowlist, explicit ordered fallback list (`submit_route_order`), per-route slippage/CU caps, adapter-response correlation guards и attempt-based route fallback.
7. Adapter auth hardening baseline готов: optional Bearer + optional HMAC request signing (`key_id/secret/ttl`) с fail-closed валидацией на старте; HMAC считается по точным bytes исходящего JSON-body; token/secret могут подниматься из file-based secret paths.
8. Оставшиеся code-gaps до real-money submit: production adapter integration (реальный signed-tx backend + ops rollout по уже готовому runtime контракту).

Текущий статус этапов:

| Направление | Статус | Owner | Due |
| --- | --- | --- | --- |
| Yellowstone primary runtime | Done | runtime-ops | 2026-02-19 |
| Watchdog script/policy in repo | Done | runtime-ops | 2026-02-18 |
| Watchdog systemd deploy on server | In progress | runtime-ops | 2026-02-20 |
| Post-cutover 1h/6h/24h evidence | In progress | runtime-ops | 2026-02-20 |
| 7-day observation closure | Pending | runtime-ops | 2026-02-26 |
| Execution runtime (paper lifecycle) | Done | execution-dev | 2026-02-19 |
| Execution runtime (live submit path) | In progress | execution-dev | 2026-03-09 |
| Execution safety hardening (audit batch #1) | Done | execution-dev | 2026-02-19 |
| Emergency stop (no-restart) | Done | execution-dev | 2026-02-19 |
| `pause_new_trades_on_outage` wiring/removal | Done | execution-dev | 2026-02-19 |

### 2.1 Сквозной phase tracker (A→Live)

| Фаза | Цель | Статус на 2026-02-19 | Главный блокер выхода |
| --- | --- | --- | --- |
| A | Закрыть Yellowstone migration observation | In progress | systemd watchdog deploy + 1h/6h/24h evidence + 7-day window |
| B | Закрыть security/ops baseline до первого submit | In progress | key policy + alert delivery + rollback drill |
| C | Поднять execution core MVP | In progress | закрыть live submit-path + real tx policy (CU-limit/CU-price + route slippage bounds) |
| C.5 | Пройти devnet dress rehearsal | Pending | end-to-end smoke без критичных дефектов |
| D | Подключить Jito как primary route | Pending | route policy + tip strategy + fallback policy |
| E | Заэнфорсить live risk limits в execution | In progress | добрать fee reserve/cooldown policy + live-runtime проверку |
| F | Пройти staged rollout (dry/tiny/limited) | Pending | KPI-gates по success/timeout/duplicates |
| G | Стабилизировать controlled live (first 7-14 days) | Pending | нулевые P0 и подтвержденная reconcile-дисциплина |
| H | Перейти в standard live / steady-state ops | Pending | signed go-live + runbook completeness + ownership handoff |

Фактический прогресс на 2026-02-19:

1. Закрыты safety-gates `R2P-06` и `R2P-16` (runtime BUY-gate).
2. Execution baseline поднят: `R2P-08` и `R2P-09` закрыты; `R2P-10`/`R2P-11` в прогрессе (paper lifecycle + recovery + risk gates готовы).
3. До real-money submit остаются code-only блокеры: live signed-tx backend за адаптерным контрактом + калибровка route-профилей под реальные market regimes.

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
Depends on: none  
Artifacts: `systemctl status`, timer logs, failover drill logs

`R2P-02` — Migration evidence pack  
Depends on: R2P-01  
Artifacts: 1h/6h/24h reports, 7-day summary, replay waiver note

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
6. adapter submit mode added (`adapter_submit_confirm`): HTTP adapter submitter contract + route allowlist policy (`submit_allowed_routes`) + explicit route fallback order policy (`submit_route_order`) + route slippage caps (`submit_route_max_slippage_bps`) + route-level compute budget policy (`submit_route_compute_unit_limit`, `submit_route_compute_unit_price_micro_lamports`) + fail-closed wiring for submitter/confirmer initialization.
7. adapter auth policy hardened: optional HMAC signing headers (`submit_adapter_hmac_key_id`, `submit_adapter_hmac_secret`, `submit_adapter_hmac_ttl_sec`) with startup fail-fast on partial/invalid config; adapter auth token/HMAC secret support file-based sources (`submit_adapter_auth_token_file`, `submit_adapter_hmac_secret_file`) for secret-management rollout.
Remaining:
1. production adapter backend (real signed tx build/send + operational rollout),
2. route-level policy evolution для Jito-primary/RPC-fallback in real-money path.

`R2P-12` — Devnet dress rehearsal  
Depends on: R2P-04, R2P-10, R2P-11  
Artifacts: devnet smoke report

`R2P-13` — Jito primary + RPC fallback + tip strategy  
Depends on: R2P-11, R2P-12  
Files: `crates/execution/*`, config/env docs

`R2P-14` — Live risk enforcement + `configs/live.toml`  
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
13. execution price policy switched to fail-closed (`price_unavailable`) instead of unsafe fallback `avg_price_sol=1.0`.
14. ingestion telemetry now tracks parse rejects by reason (in addition to `parse_rejected_total`).
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

Остается в next-code-queue:

1. wire production adapter backend for real signed tx send path (using `adapter_submit_confirm` contract) and complete production secret distribution/rotation rollout for auth headers.
2. complete operational calibration for route profiles (Jito-primary/RPC-fallback) using existing slippage/CU policy knobs and explicit `submit_route_order` policy.

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
   2. incident rollback playbook,
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

## 12) Target Calendar (рабочий план, UTC)

1. 2026-02-20: закрыть server watchdog systemd deploy + первые 1h/6h/24h артефакты.
2. 2026-02-26: закрыть 7-day observation и Stage A.
3. 2026-02-27 — 2026-03-02: Stage B (security/ops baseline) и фиксация go/no-go для начала Stage C.
4. 2026-03-03 — 2026-03-09: Stage C + C.5 (execution MVP + devnet rehearsal).
5. 2026-03-10 — 2026-03-14: Stage D + E (Jito primary + live risk enforcement).
6. 2026-03-15 — 2026-03-18: Stage F (dry-run -> tiny-live -> limited-live при green KPI).
7. 2026-03-19 — 2026-03-26: Stage G (controlled-live stabilization).
8. 2026-03-27+: Stage H (standard-live handoff / steady-state ops).
