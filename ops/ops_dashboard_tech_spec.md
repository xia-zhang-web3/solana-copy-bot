# Ops Dashboard Tech Spec

Дата: 2026-06-17  
Статус: черновик ТЗ для отдельного read-only UI  
Контекст: tiny entries выключены, daemon продолжает писать shadow/discovery данные, storage compact rebuild завершен, нужен удобный интерфейс наблюдения.

## 1. Цель

Сделать отдельный web-интерфейс для операционного мониторинга Solana Copy Bot.

Dashboard должен быстро отвечать:

- можно ли спокойно отходить/спать;
- жив ли daemon;
- не вернулась ли discovery/starvation петля;
- нет ли hard stop / risk freeze, который остановил BUY;
- что происходит с tiny/shadow/canary;
- сколько места осталось на volume;
- растут ли DB/WAL/recent raw;
- какие последние ошибки, blockers, verdict-ы и freshness-сигналы;
- накопилось ли достаточно свежей статистики для следующих стратегических решений.

Главный формат — mobile-first. С телефона состояние системы должно быть понятно за 10-20 секунд.

## 2. Не цели V1

В V1 не делаем:

- кнопки включения торговли;
- изменение production config;
- ручную отправку сделок;
- sweep/write-off/rebuild из UI;
- публичный API без авторизации;
- тяжелые SQL-сканы из HTTP request path;
- новую логику в `copybot-app`;
- замену существующих CLI/operator reports.

V1 — read-only панель поверх существующих операторских данных и bounded read-only запросов.

V1.5 может добавить alerting, но не destructive ops.

## 3. Архитектура

Dashboard должен быть отдельным сервисом, не частью daemon.

Правило: UI не должен влиять на fail-closed поведение runtime. Если UI упал, daemon продолжает работать. Если daemon fail-closed, UI показывает это явно, а не маскирует статус как green.

Overview не имеет права быть green, если любой источник stale сверх порога. Stale-green запрещен.

Запрещено:

- добавлять web/report код в `copybot-app`;
- тянуть runtime graph в read-only dashboard;
- запускать production release build на сервере как нормальный путь;
- делать unbounded SQLite scans в web requests.

## 4. Стек

### Backend

Рекомендуемый стек:

- Rust;
- `axum`;
- `tokio`;
- `tower-http`;
- `serde` / `serde_json`;
- `rusqlite` read-only connections;
- `storage-core` или минимальный read-only SQLite facade;
- server-side session cookies.

Новый build target: `copybot-ops-dashboard` или отдельный binary в ops/live-ops зоне.

Backend не должен зависеть от `copybot-app`.

### Frontend

Рекомендуемый стек:

- React;
- TypeScript;
- Vite;
- TanStack Query;
- Tailwind CSS или CSS Modules с design tokens;
- `lucide-react`;
- `recharts` или `visx`;
- static bundle, раздаваемый dashboard backend.

UI должен быть application-first, не landing page.

### Auth

Нужна username/password система. Bearer-token для браузера не используем.

V1 auth:

- login form: username + password;
- password hash: Argon2id;
- session cookie: `HttpOnly`, `Secure`, `SameSite=Strict`;
- server-side revocable session store;
- отдельный auth SQLite файл, не `live_runtime.db`;
- session expiry: например 12 часов;
- idle timeout: например 2 часа;
- logout endpoint;
- login rate limit;
- audit log для login/logout/failed login;
- CSRF protection для POST endpoints.

Production exposure:

- bind по умолчанию на `127.0.0.1`;
- доступ через SSH tunnel, Cloudflare Access, Caddy/Nginx с TLS или VPN;
- password не хранить plain text;
- session signing key через env/secret file.

## 5. Data Flow

Dashboard читает:

- `live_runtime.db`;
- latest operator report JSON/CSV, если они уже пишутся;
- filesystem stats по DB volume;
- systemd/service state через bounded ops helper;
- git/artifact metadata, если доступно локально.

Тяжелые отчеты должны считаться заранее или через cache.

HTTP endpoint должен:

- читать готовый snapshot;
- или выполнять маленький indexed query;
- или вернуть stale/cached status с freshness timestamp.

Dashboard должен в основном показывать operator snapshot/report данные. Прямые чтения из hot WAL-mode `live_runtime.db` допустимы только для коротких indexed/bounded запросов. Нельзя держать long-lived read transaction: это может pin-ить WAL и мешать checkpoint/TRUNCATE.

## 6. API V1

Минимальные endpoints:

- `POST /login`, `POST /logout`, `GET /api/session`;
- `GET /api/overview`: главный "можно отходить?" статус;
- `GET /api/runtime`: service, SHA, logs, blockers, ingestion freshness;
- `GET /api/storage`: volume, DB/WAL/recent raw, retention, runway;
- `GET /api/execution`: tiny gate, open positions, fills, wallet reconciliation, failures;
- `GET /api/strategy`: limit-matched shadow-vs-executable delta, multi-window PnL, follower gap, rug/stale tail, green-criterion;
- `GET /api/discovery`: candidates, eligible, floor, filters, quarantine, starvation-loop;
- `GET /api/reports/latest`: ссылки/метаданные последних отчетов.

Каждый ответ должен содержать `source`, `generated_at`, `freshness_age_ms` и явный `stale` flag.

Overview green вычисляется как строгий AND:

- daemon active/running;
- нет hard stop / permanent risk freeze;
- нет active timed risk pause, скрытой от gate;
- candidate count >= floor;
- publication свежая;
- shadow/discovery данные текут;
- disk runway выше порога;
- WAL не растет неконтролируемо;
- wallet reconciliation clean;
- все источники fresh.

Если любой пункт неизвестен, Overview должен стать `unknown` или `warning`, не `green`.

Overview thresholds must be explicit config values, not interpretation in UI code. Starting defaults can be:

- publication stale after 90 minutes;
- disk runway warning below 14 days, critical below 7 days;
- candidate floor from discovery config, currently expected around 8;
- WAL warning/critical thresholds from storage ops config;
- source freshness thresholds per endpoint.

## 7. UI Screens

- Login: username/password, error/loading/session-expired states, password-manager friendly.
- Overview: system status, entries, sells, open positions, wallet reconciliation, disk, discovery, data freshness, latest blocker.
- Execution: buy/sell funnel, fills, open positions, wallet reconciliation, recent errors.
- Strategy: limit-matched shadow vs executable PnL, follower gap, rug/stale tail, regime notes, multi-window green-criterion checklist.
- Discovery: candidates vs floor, eligible/published wallets, starvation guard, executable/rug filters, quarantine.
- Storage: disk trend, DB/WAL/recent raw sizes, retention, last rebuild, next maintenance recommendation.
- Alerts: critical transition history and planned push-channel state.

Mobile: one-column status stack with sticky top status. Desktop: compact grid with charts below.

## 8. Mobile UX Requirements

Primary viewport: about 390px width.

Rules:

- no horizontal scroll for core status;
- tables collapse into cards or summary rows;
- key status visible in first screen;
- bottom navigation or compact tab bar;
- touch targets at least 44px;
- simplified mobile charts;
- no dense desktop table squeezed into mobile;
- every metric has source/freshness timestamp.

Killer metrics need trend/projection, not only point value:

- disk runway: `free / growth_rate`;
- ingestion rate and recent delta;
- candidate-to-floor trend;
- publication freshness;
- DB/WAL growth;
- shadow closed trade freshness.

## 9. Visual Direction

Style: strict Apple / Stripe / VICE inspired ops UI.

Interpretation:

- Apple: clarity, spacing, typographic discipline;
- Stripe: polished data-product components, clean charts, hierarchy;
- VICE: contrast, sharp labels, editorial confidence.

Do not copy brand assets. Use the mood, not branding.

Preferred feel:

- precise;
- premium;
- sober;
- fast to scan;
- mobile-native;
- not crypto-cartoon;
- not terminal-only;
- not generic admin template.

## 10. Security Requirements

V1 must satisfy:

- all non-login routes require session;
- no bearer token in browser localStorage;
- session cookie is HttpOnly;
- password hashes use Argon2id;
- login is rate-limited;
- auth failures are logged;
- service binds locally by default;
- reverse proxy must terminate TLS if exposed;
- no secret values returned to frontend.

## 11. Performance Requirements

Targets:

- initial page load under 2 seconds;
- overview refresh every 10-30 seconds;
- heavy strategy/storage refresh every 1-5 minutes;
- no endpoint performs unbounded scans;
- endpoint timeout is enforced;
- stale data shows freshness.

Maintenance mode must be explicit. During DB rebuild/swap, stale DB handle, or operator maintenance window, UI should show `maintenance`, not false `critical` or false `green`.

## 12. Acceptance Criteria V1

V1 is acceptable when:

- login/logout works with username/password;
- unauthenticated requests are blocked;
- overview shows daemon, disk, execution gate, discovery, strategy freshness;
- mobile layout is usable at 390px;
- desktop layout is usable at 1440px;
- no code is added to `copybot-app`;
- dashboard can run while daemon is active;
- dashboard can stop without affecting daemon;
- every displayed metric has timestamp/source;
- heavy reports are cached or precomputed.
- active hard stop / risk freeze appears on Overview within one refresh;
- candidate count below floor is critical;
- stale Overview source prevents green;
- disk runway projection is visible;
- strategy uses limit-matched delta across multiple windows, not one headline canary number.

## 13. Implementation Phases

1. Design: login, overview, execution, strategy, discovery, storage.
2. Static UI with mock JSON fixtures.
3. Read-only backend API with bounded queries and cached snapshots.
4. Production access as separate artifact/service on localhost behind tunnel/proxy.
5. Alerting V1.5: Telegram, ntfy, or email for critical transitions.
6. Optional controlled ops later, only after explicit decision.

Critical alert candidates:

- hard stop / permanent risk freeze activated;
- candidate count below floor;
- daemon down;
- disk runway below threshold;
- WAL growth above threshold;
- wallet reconciliation unmatched persists;
- publication freshness expired;
- shadow/discovery data stopped flowing.

Controlled write-ops through UI require the same audit bar as CLI money/storage tools. No one-click destructive actions.

## 14. Open Decisions

- Exact crate/package name.
- Whether frontend lives inside repo or separate package.
- Auth DB schema and session TTL values.
- Reverse proxy: Caddy, Nginx, Cloudflare Tunnel, or SSH tunnel.
- Light mode only, dark mode only, or both.
- Report snapshots as files or DB rows.

## 15. Prompt For Design Coder

```text
Сделай дизайн read-only ops dashboard для Solana Copy Bot.

Контекст:
- Это не landing page и не marketing site.
- Это рабочий mobile-first интерфейс для владельца торгового бота.
- Владелец с телефона хочет за 10-20 секунд понять: система жива, деньги не текут, disk не заканчивается, discovery работает, tiny entries paused/running, open positions, последние ошибки, хватает ли свежих данных.
- Нужна login/password auth, не bearer-token UX.

Стиль:
- Строгий Apple / Stripe / VICE inspired.
- Apple: чистота, воздух, типографика, дисциплина.
- Stripe: polished data-product UI, аккуратные графики, статусные панели.
- VICE: высокий контраст, sharp labels, editorial confidence.
- Не копировать бренды и ассеты. Только направление.
- Не делать crypto-cartoon, neon Web3, gradient blobs, декоративные орбы, generic admin template.

Форматы:
- Mobile-first: основной viewport 390px.
- Desktop variant: 1440px.
- Дизайн должен быть гибким: карточки/таблицы не ломаются от длинных статусов, ошибок и чисел.
- На mobile нельзя просто ужать desktop table; нужны summary rows/cards.

Экраны:
- Login: username/password, error/loading/session-expired states.
- Overview: green/warning/critical, daemon, entries, sells, positions, wallet reconciliation, disk/runway, discovery floor, freshness, latest blocker.
- Execution: buy/sell confirmed/failed, fills symmetry, open positions, failure classes.
- Strategy: Shadow/Canary/Tiny PnL, limit-matched executable delta, follower gap, rug/stale tail, multi-window green-criterion.
- Discovery: candidates, eligible/published wallets, floor, starvation-loop, executable/rug filters, quarantine.
- Storage: volume, live_runtime.db, WAL, recent_raw, retention, last rebuild, next maintenance.
- Alerts: critical transition history and push-channel concept.

Interaction:
- Bottom nav or compact tab nav on mobile.
- Desktop can use left sidebar or top segmented nav.
- Use clear status colors, but do not make the UI one-note.
- Use icons only where they improve scanning.
- Every metric should have source/freshness timestamp.
- Critical state must be visually obvious without reading a paragraph.

Deliverables:
- High-fidelity mobile screens first.
- Desktop adaptation.
- Component states: loading, stale data, empty, warning, critical, auth error.
- Short design note describing spacing, typography, color tokens, and responsive behavior.
```
