# Real-Time UI Plan For `copybot-app`

Date: 2026-02-17
Status: Proposed implementation plan

## 1. Goal

Сделать встроенный real-time dashboard в `copybot-app`, чтобы без ручных запросов видеть:

- все трейды (recorded и dropped),
- текущие открытые/закрытые позиции,
- PnL (realized и unrealized),
- состояние risk guards,
- состояние ingestion/discovery/shadow scheduler,
- проблемные состояния ("зависло", backlog, stale).

## 2. Why In-Process Axum

Встроенный `Axum` в `copybot-app` выбран как базовая архитектура, потому что только так можно без задержек читать in-memory runtime state из `run_app_loop`:

- risk guard state,
- ingestion runtime snapshot,
- shadow queue/backpressure state,
- follow snapshot и temporal follow changes.

Отдельный сервис видит только SQLite и теряет live-контекст runtime.

## 3. Required Additions Before UI

## 3.1 Event Audit Layer (must-have)

Сейчас в БД полноценно сохраняются только recorded signals. Для полного "ВСЕ" нужен унифицированный event log.

Добавить таблицу `ui_events` (новая миграция), куда писать:

- `event_type` (signal_recorded, signal_dropped, risk_event, queue_event, ingestion_event),
- `stage` (follow, risk, quality, scheduler, ingestion, discovery),
- `reason`,
- `wallet`,
- `token`,
- `signature`,
- `payload_json`,
- `ts`.

Это сделает `/api/signals`, `/api/risk`, `/api/ingestion`, `/api/discovery` полными и объяснимыми.

## 3.2 Unrealized PnL Source (must-have)

Unrealized PnL нельзя корректно считать только из локальной БД. Нужен live mark-price источник:

- базовый вариант: Jupiter quote API,
- fallback: last SOL-leg trade proxy из `observed_swaps`.

При отсутствии цены показывать `unrealized_pnl = null` и статус `price_unavailable`.

## 3.3 Health/Stuck Model (must-have)

Явно определить и вычислять "зависло":

- ingestion stale (`now - last_swap_ts > threshold`),
- discovery stale (`now - last_discovery_cycle_ts > threshold`),
- shadow queue saturated (`pending >= capacity`),
- ws reconnect loop (frequent reconnects),
- persistent lag breach (p95 > threshold длительное время),
- repeated worker failures/panics.

Все это выводить в UI и API как `health_alerts`.

## 3.4 API/WS Authentication (must-have)

Нужна обязательная авторизация для всех `GET /api/*` и `WS /ws/live`:

- `Authorization: Bearer <token>`,
- token читается из env/конфига (`web.auth_token`),
- при пустом токене web server либо не стартует, либо стартует только на localhost (явно через флаг).

Минимум: reject `401 Unauthorized` без валидного bearer.

## 3.5 WS Broadcast Lag Handling (must-have)

`tokio::sync::broadcast` допускает lagging receivers. Это норм для UI, но handler обязан корректно обрабатывать:

- `RecvError::Lagged(n)` -> лог + отправка клиенту `resync_required`/`lagged`,
- соединение не должно падать из-за единичного lag,
- на клиенте триггерить soft-resync (перечитать `/api/dashboard` + `/api/signals`).

## 3.6 Event Retention Policy (must-have)

`ui_events` может расти очень быстро. Нужен конкретный retention:

- базово: хранить 48h,
- периодическая очистка: `DELETE FROM ui_events WHERE ts < <cutoff>`,
- запуск очистки: минимум на каждом discovery cycle,
- индексы: `(ts)`, `(event_type, ts)`,
- maintenance: периодический `VACUUM`/`PRAGMA auto_vacuum` по операционной политике.

## 4. Runtime State Sharing Design

Не один общий lock на все. Использовать шардированный shared state:

- `RuntimeRiskSnapshot`,
- `RuntimeIngestionSnapshot`,
- `RuntimeDiscoverySnapshot`,
- `RuntimeSchedulerSnapshot`,
- `RuntimeHealthSnapshot`.

Общий контейнер:

```rust
pub struct WebRuntimeState {
    pub risk: Arc<RwLock<RuntimeRiskSnapshot>>,
    pub ingestion: Arc<RwLock<RuntimeIngestionSnapshot>>,
    pub discovery: Arc<RwLock<RuntimeDiscoverySnapshot>>,
    pub scheduler: Arc<RwLock<RuntimeSchedulerSnapshot>>,
    pub health: Arc<RwLock<RuntimeHealthSnapshot>>,
    pub live_tx: tokio::sync::broadcast::Sender<LiveEvent>,
}
```

Main loop обновляет snapshots в точках, где уже появляются события.

## 5. API Contract

Реализовать endpoints:

- `GET /api/dashboard` — KPI summary (PnL, winrate, open exposure, lots, closed trades, alerts),
- `GET /api/lots` — open lots + entry cost + mark + unrealized pnl + links,
- `GET /api/trades` — closed trades, pagination, hold time, pnl,
- `GET /api/signals` — recent recorded + dropped + reasons/stages,
- `GET /api/risk` — guards state, stop/pause reasons, active blocks,
- `GET /api/ingestion` — lag p50/p95/p99, queue depth, rps, rpc errors,
- `GET /api/discovery` — active follow list, top wallets, last cycle stats,
- `GET /api/config` — redacted runtime config,
- `WS /ws/live` — push событий в UI.

Auth middleware обязателен для всех перечисленных API и WS endpoint.

## 6. WebSocket Event Model

События:

- `signal_recorded`,
- `signal_dropped`,
- `lot_opened`,
- `lot_closed`,
- `risk_state_changed`,
- `ingestion_snapshot`,
- `discovery_cycle_done`,
- `health_alert`.

Формат:

```json
{
  "type": "signal_dropped",
  "ts": "2026-02-17T13:40:00Z",
  "payload": { "reason": "lag_exceeded", "wallet": "...", "token": "...", "signature": "..." }
}
```

## 7. Backend Implementation Steps

## Phase 1: Foundation

- Добавить зависимости в `crates/app/Cargo.toml`: `axum`, `tower-http`, `serde_json`.
- Создать `crates/app/src/web.rs` с router, DTO, handlers, websocket.
- Добавить `WebRuntimeState` + `LiveEvent` структуры.
- Добавить auth middleware (bearer token check) для `/api/*` и `/ws/live`.
- Добавить web-конфиг (`[web]`) в `copybot-config` + env override.

## Phase 2: Runtime Integration

- В `run_app_loop` и смежных функциях обновлять runtime snapshots.
- В местах dropped/recorded/risk queue events отправлять `live_tx.send(...)`.
- Параллельно писать `ui_events` в SQLite для историчности.

## Phase 3: Storage Queries For UI

В `copybot-storage` добавить read API:

- dashboard aggregates,
- paginated closed trades,
- open lots with cost basis,
- recent ui_events,
- recent risk events/alerts.

Все тяжелые операции выполнять через `spawn_blocking`.
Для web handlers использовать отдельный read-only SQLite connection (WAL read mode), не shared write connection runtime loop.

## Phase 4: Frontend Page

- Один `index.html` (served by Axum).
- Tailwind + vanilla JS.
- Таблицы: сортировка, фильтрация, пагинация.
- Карточки KPI + alerts panel + sticky status bar.
- Solscan links для `tx`, `token`, `wallet`.
- Цветовая схема для PnL/guard status/backpressure.

## Phase 5: Hardening

- Redaction в `/api/config` для ключей/URL с токенами.
- Rate limit для API и WS connections cap.
- Retention policy для `ui_events` (конкретно 48h + scheduled cleanup + index coverage).
- Regression tests на core handlers и event ingestion path.
- Тесты на auth (401/403), и на WS lagged receiver behavior.

## 8. UI Layout Recommendation

Порядок экранных блоков:

1. Global status strip: env, uptime, ws status, last update, alert count.
2. KPI cards: realized pnl 1h/6h/24h, open exposure, winrate, open lots.
3. Risk panel: все guards + причины блоков.
4. Ingestion panel: lag/queue/rpc/fetch metrics.
5. Discovery panel: top leaders + follow changes.
6. Lots table: open positions.
7. Trades table: closed trades.
8. Signals/event feed: recorded/dropped/risk/health stream в реальном времени.

## 9. Security/Operational Rules

- `/api/config` не должен отдавать секреты.
- SQLite write path не блокировать UI read path.
- При отказе price source не ломать API, только деградировать unrealized.
- Все runtime snapshots должны иметь `ts` и `staleness` индикатор.

## 10. Acceptance Criteria

- UI показывает live updates без refresh через WS.
- Видны both recorded и dropped signals с reason/stage.
- Видно, какие лоты открыты/закрыты и какой PnL.
- Видно активные risk блокировки и причины.
- Видно ingestion/discovery health и "stuck" alerts.
- Dashboard выдерживает sustained поток без заметного торможения ingestion.

## 11. Suggested File Changes

- `crates/app/Cargo.toml`
- `crates/app/src/main.rs`
- `crates/app/src/web.rs` (new)
- `crates/storage/src/lib.rs`
- `migrations/0009_ui_events.sql` (new)
- `crates/config/src/lib.rs`
- `configs/dev.toml`
- `configs/paper.toml`
- `configs/prod.toml`
- `README.md` (добавить раздел про web dashboard)

## 11.1 Web Config Contract

Добавить в конфиг:

```toml
[web]
enabled = true
host = "0.0.0.0"
port = 8080
auth_token = "REPLACE_ME"
```

Env overrides:

- `SOLANA_COPY_BOT_WEB_ENABLED`
- `SOLANA_COPY_BOT_WEB_HOST`
- `SOLANA_COPY_BOT_WEB_PORT`
- `SOLANA_COPY_BOT_WEB_AUTH_TOKEN`

## 12. Execution Order

1. `Phase 1 + Phase 2` (поднять сервер и live state без фронта),
2. `Phase 3` (закрыть data contract),
3. `Phase 4` (UI),
4. `Phase 5` (hardening + тесты).
