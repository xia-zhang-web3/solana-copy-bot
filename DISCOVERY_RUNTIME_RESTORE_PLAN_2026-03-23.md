# DISCOVERY RUNTIME RESTORE PLAN

Date: 2026-03-23
Status: Canonical, Batch 1-3 implemented; bounded gap-fill compare-runs completed; validation/snapshot hardening follow-ups implemented; live server is currently in bootstrap-degraded bridge mode

## 0. Суть

Цель этого плана одна:

- больше никогда не дропать runtime DB и не собирать все заново из нуля

Главный принцип:

- runtime DB должна стать disposable
- source of truth для восстановления должен быть вне нее

Контекст текущего инцидента на 2026-03-23:

- путь, где bounded replay / aggregate recovery уже крутится часами и показывает
  остаток порядка многих дней, считать мертвым
- если recovery отработал примерно `9.5` часа и оценка остатка порядка `14`
  дней, это не restore, а giant replay
- такой путь не продолжать как runtime recovery
- его state, cursor и логи можно сохранить только для postmortem
- этот план фиксирует новый контракт именно для runtime restore, а не для
  доведения старого giant replay до конца

## 0.1 Live server status on 2026-03-24

Что уже правда на реальном сервере:

- код restore stack уже раскатан на live server в коммите `4148969`
- старый giant replay path больше не является активным runtime path
- `solana-copy-bot.service` снова запущен и live ingestion снова идет
- live сейчас идет уже не на старой runtime DB, а на fresh restored runtime DB:
  - active DB path:
    `/var/www/solana-copy-bot/state/live_runtime_20260324T134339Z.db`
  - legacy `/var/www/solana-copy-bot/state/live_copybot.db` удалена с диска
- текущий live runtime state:
  - `runtime_state = bootstrap_degraded_publication_truth`
  - `runtime_mode = bootstrap_degraded`
  - `scoring_source = bootstrap_degraded_publication_truth_raw_window_degraded`
  - `active_follow_wallets = 15`
  - `published_wallet_count = 15`
- текущий bootstrap-degraded universe был явно поднят как временный мост из
  top-15 `wallet_metrics` snapshot с
  `last_published_window_start = 2026-03-19T12:00:00Z`
- live execution остается выключенным:
  - `execution.enabled = false`
  - copy trading / shadow trading не должны открывать новые позиции
- live restore surfaces уже существуют:
  - artifact: `/var/www/solana-copy-bot/state/discovery_restore/artifacts/latest.json`
  - recent raw journal: `/var/www/solana-copy-bot/state/discovery_recent_raw.db`
  - recent raw snapshot: `/var/www/solana-copy-bot/state/discovery_restore/recent_raw/latest.sqlite`
  - latest successful recent raw archive snapshot:
    `/var/www/solana-copy-bot/state/discovery_restore/recent_raw/discovery_recent_raw_20260324T200613Z.sqlite`
- текущий provider contract на live server:
  - QuickNode остается активным path в `/etc/solana-copy-bot/live.server.toml`
  - `[recent_raw_gap_fill]` по-прежнему смотрит в текущий QuickNode HTTP endpoint
  - `[program_history_validation]` теперь тоже смотрит в тот же QuickNode path
    и использует local pacing:
    - `max_requests_per_second = 60`
    - `retry_429_max_attempts = 6`
    - `retry_429_backoff_ms = 500`
  - Helius не оставлен в live config как active source
  - Helius использовался только как explicit compare-run path
- server-side fresh-DB restore drill уже прошел на этих поверхностях:
  - `journal_available = true`
  - `journal_replayed = true`
  - `journal_covers_artifact_cursor = true`
  - live cutover replayed `534244` rows into the fresh runtime DB
  - `raw_coverage_satisfied = false`
  - final verdict = `bootstrap_degraded`
- bounded gap-fill compare-run уже тоже проведен на реальном сервере:
  - missing window:
    `2026-03-19T13:43:40.230377748Z -> 2026-03-24T12:07:11.775090344Z`
  - generic QuickNode path (`discovery_raw_gap_fill`) дал:
    - `scanned_signatures = 63000`
    - `fetched_rows = 95`
    - `inserted_rows = 95`
    - `gap_fill_covered_since = 2026-03-19T13:43:51Z`
    - `gap_fill_covered_through_cursor = 2026-03-21T09:01:20Z`
    - `sufficient_for_healthy_restore = false`
  - новый Helius-specific path (`discovery_raw_gap_fill_helius`) дал:
    - `scanned_items = 46071`
    - `scanned_pages = 485`
    - `fetched_rows = 95`
    - `inserted_rows = 95`
    - ту же `gap_fill_covered_since`
    - тот же `gap_fill_covered_through_cursor`
    - `sufficient_for_healthy_restore = false`

Что это значит честно:

- сервер больше не мертв и не сидит на старом giant replay path
- сервер также больше не зависит от старой month-old runtime DB вообще
- restore chain уже существует и реально исполним на проде
- provider swap и Helius-specific address-history path уже проверены на том же
  bounded окне и не дали лучшего покрытия
- QuickNode program-history validation path после throttling adaptation больше
  не умирает мгновенно в `429`, но на live окне все еще не дал terminal
  verdict в пределах bounded `timeout 900`
- live recent-raw snapshot path после practical-completion rollout уже
  завершает fresh large-journal snapshot и timer снова возвращен в
  `enabled/active`
- но инцидент еще не закрыт:
  - runtime все еще не `healthy`
  - trading-ready restore еще не достигнут
  - текущий state это safe bridge, а не полноценное восстановление

## 1. Что считать истиной

Не истина:

- старая `followlist`
- старые top wallets
- aggregate readiness
- offline clone сам по себе

Истина для restore:

- `runtime artifact`
- `recent raw journal`

## 2. Что такое runtime artifact

Обязательный состав:

- exact publication truth
- publication metadata
- discovery runtime cursor
- latest published wallet metrics snapshot

Важно:

- `followlist` не истина
- `followlist` только derived cache
- если `followlist` расходится с publication truth, побеждает publication truth

Правило свежести артефакта:

- normal restore не должен “легализовать” stale artifact простым переписыванием
  `last_published_at = now`
- если артефакт stale по текущему runtime gate, normal restore должен либо
  отказать, либо входить только в явный bootstrap-degraded режим
- bootstrap-degraded режим не должен притворяться recent publication truth
- bootstrap-degraded режим должен запускаться только с `execution.enabled = false`

## 3. Что такое recent raw journal

Это отдельное durable хранилище свежих `observed_swaps`, независимое от
runtime DB.

Нужно хранить:

- bounded recent raw swaps для текущего runtime horizon
- достаточно данных, чтобы после restore не ждать многодневный replay

Без этого журнала:

- restore почти всегда придет только в `degraded`
- если fresh rebuild снова не сойдется, инцидент не закрыт

V1 storage contract:

- не внешний сервис
- не “какой-нибудь файл”
- отдельная SQLite journal DB, например `state/discovery_recent_raw.db`
- хранит bounded recent `observed_swaps` window
- retention: `scoring_window_days` + небольшой safety buffer
- runtime DB и raw journal DB живут отдельно и могут восстанавливаться независимо

## 4. Жесткие правила

1. Старые кошельки нельзя использовать как runtime truth.
2. Старую DB нельзя использовать как торговый source of truth после потери freshness.
3. Aggregate/backfill нельзя пускать в boot path.
4. Если restore требует giant replay истории, это не restore, а провал архитектуры.
5. Если runtime DB сломалась, ее убирают в архив и поднимают новую.

## 5. P0: что делать сейчас

Считать, что текущей runtime truth у тебя нет.

Считать также, что текущий long-running replay/backfill path для runtime уже
дисквалифицирован.

Нужно честно признать:

- если prebuilt `recent raw journal` не существовал до инцидента,
  one-button healthy restore для этого первого инцидента еще невозможен
- тогда первый подъем до healthy неизбежно имеет one-time bootstrap cost:
  либо recent raw backfill, либо live ingestion до достаточного fresh raw window
- это допустимо только как разовый переходный шаг
- после внедрения `recent raw journal` такой сценарий повторяться уже не должен

Действия:

1. Если старый long-running replay/backfill еще идет, остановить его.
2. Его state и логи сохранить только для postmortem.
3. Старую DB убрать из active runtime path.
4. Не брать из нее кошельки.
5. Поднять fresh runtime DB.
6. `execution.enabled = false`.
7. Наполнить новую DB только свежими `observed_swaps`.
8. Ждать только fresh runtime truth.

Источник для пункта 7 только такой:

1. либо recent raw backfill за актуальный lookback
2. либо live ingestion с накоплением свежего raw window

Чего делать нельзя:

1. не bootstrap старых кошельков для торговли
2. не считать clone от 2026-03-09 свежим truth source
3. не пытаться “долечить” старую DB как production runtime
4. не продолжать giant replay с multi-day ETA как будто это restore

## 6. P1: что кодить в ближайшие 2 дня

### Task 1. Runtime artifact export/import

Добавить:

- [`crates/discovery/src/bin/discovery_runtime_export.rs`](/Users/blacktower/Documents/solana-copy-bot/crates/discovery/src/bin/discovery_runtime_export.rs)
- [`crates/discovery/src/bin/discovery_runtime_restore.rs`](/Users/blacktower/Documents/solana-copy-bot/crates/discovery/src/bin/discovery_runtime_restore.rs)

Поддержать через:

- [`crates/storage/src/discovery.rs`](/Users/blacktower/Documents/solana-copy-bot/crates/storage/src/discovery.rs)

Обязательный контракт Task 1:

1. export должен писать `exported_at`
2. export должен брать один консистентный snapshot, а не собирать artifact из
   разных publish-state в разное время
3. artifact должен нести gate metadata, по которой потом валидируется freshness:
   минимум `exported_at`, `last_published_at`, `last_published_window_start`,
   текущие `scoring_window_days`, `metric_snapshot_interval_seconds`
4. restore должен валидировать freshness артефакта против текущего runtime gate
   и импортированных gate metadata
5. import должен быть all-or-nothing:
   - publication truth
   - publication metadata
   - runtime cursor
   - latest published wallet metrics snapshot
   должны относиться к одному согласованному snapshot
6. normal restore не должен переписывать `last_published_at` на `now`
7. normal restore не должен переписывать `last_published_window_start` на
   текущий bucket
8. если артефакт stale:
   - normal restore должен отказать как trading-ready path
   - допускается только явный `--bootstrap-degraded` режим
9. `--bootstrap-degraded`:
   - не маркирует stale artifact как recent truth
   - не должен открывать торговлю
   - должен существовать только как incident tool, а не нормальный steady-state path

Критический implementation risk для `--bootstrap-degraded`:

- в текущем коде stale publication truth не переживает runtime gate сама по себе
- если restore просто импортирует старый artifact и запустит `copybot-app`, рантайм
  на старте и в discovery cycle увидит, что publication truth stale, и перестанет
  считать ее usable recent truth
- значит `--bootstrap-degraded` нельзя оставлять только как CLI-флаг restore tool
- нужен отдельный runtime contract, который переживает запуск `copybot-app`

Минимально допустимый путь:

1. добавить отдельный runtime state, например `BootstrapDegraded`, в
   `DiscoveryRuntimeMode` или эквивалентный ему явный persisted marker
2. научить startup/runtime отличать:
   - stale recent truth, которую нужно отвергать
   - явный incident bootstrap-degraded state, который разрешен временно
3. пока runtime находится в этом состоянии:
   - не открывать торговлю
   - не считать state trading-ready
   - не стирать artifact только потому, что его `last_published_at` старый
4. выход из этого состояния должен происходить только после того, как runtime
   реально догонит свежее raw window и сможет перейти в `healthy`

Предпочтительный путь для future steady-state restore:

- артефакт должен быть свежим сам по себе, потому что он регулярно экспортируется
- при наличии свежего `recent raw journal` никакой rewrite timestamps не нужен

### Task 2. Recent raw journal

Добавить отдельный durable путь для recent `observed_swaps`.

Минимально допустимый вариант:

- export/import bounded recent `observed_swaps`

Лучший вариант:

- отдельный sidecar/secondary store для recent raw journal

Фиксируем V1:

- V1 recent raw journal = отдельная SQLite DB
- не reuse основной runtime DB
- не зависеть от aggregate tables
- хранить только bounded recent raw data, нужные для fast restore
- не считать V1 “просто экспортом в файл”; нужен явный runtime contract

Если позже появится лучший storage, его можно заменить.
Но V1 контракт должен быть конкретным уже сейчас.

Обязательные implementation notes для V1:

1. отдельно определить writer path в journal на ingest path
2. отдельно определить restore reader/replay path
3. отдельно определить retention / rotation
4. явно выбрать одно из двух:
   - либо restore сначала импортирует journal в fresh runtime DB, и discovery
     дальше работает как сейчас
   - либо discovery учится читать recent raw journal напрямую
5. не оставлять это implicit, потому что текущий runtime warm-load, raw coverage
   checks и cursor-based catch-up сейчас читают `observed_swaps` из основной
   runtime SQLite

Критический performance / lock risk для V1:

- в текущей архитектуре ingestion уже идет через `ObservedSwapWriter` с bounded
  channel и отдельным raw writer thread
- поэтому проблема не в том, что gRPC callback начнет делать два синхронных
  `INSERT` напрямую
- проблема в другом: если journal write добавить синхронно внутрь существующего
  raw writer critical section, это увеличит latency writer-а, backlog по
  `pending_requests`, риск sqlite lock pressure и общий runtime backpressure

Следствие:

1. двойную запись нельзя просто “вкрутить рядом” в тот же write step без budget
   и telemetry
2. предпочтительный V1 путь:
   - либо отдельный async sidecar writer / queue для journal
   - либо другой явно bounded fan-out path после primary raw commit
3. не считать `ATTACH DATABASE` автоматическим решением:
   - он усиливает coupling между write path двух БД
   - он не отменяет lock / latency risk сам по себе
4. для V1 обязательно мерить:
   - writer queue backlog
   - raw batch latency
   - journal batch latency
   - sqlite busy / retry pressure

### Task 3. Restore command

Нужна одна команда:

1. создать fresh runtime DB
2. импортировать runtime artifact
3. доиграть recent raw journal от cursor
4. выдать restore verdict

### Task 4. Restore verification

Не использовать `discovery_cutover_readiness` как gate initial restore.

Для initial restore gate нужен отдельный restore verdict:

- recent publication truth loaded
- runtime cursor restored
- raw coverage подтверждена для runtime horizon или restore честно остается
  только в bounded degraded / fail_closed
- active follow wallets не пусты или runtime честно в bounded degraded
- aggregate readiness не участвует в verdict

Важно:

- `runtime cursor restored` сам по себе недостаточен
- cursor без raw coverage не считается usable runtime truth
- restore verdict должен переиспользовать ту же freshness-логику publication
  truth, что и normal runtime path, чтобы не плодить drift между restore и runtime

Для normal trading-ready restore verdict должен дополнительно требовать:

- свежий artifact
- доступный recent raw journal
- отсутствие полного reread истории
- путь к `healthy`, а не permanent degraded

## 7. P2: что должно появиться после этого

1. Регулярный export runtime artifact по расписанию.
2. Регулярный export recent raw journal по расписанию.
3. Restore drill на чистую DB.
4. Документированный RTO/RPO.

Минимальный target:

- RTO: минуты, не дни
- RPO: ограничен интервалом artifact export

Scheduling contract:

- без регулярного export этот план не работает
- V1 production scheduling: `systemd` service + `systemd` timer
- artifact export cadence должен быть заметно меньше freshness gate
- для live-конфига сейчас freshness gate около 2 часов, значит export cadence
  должен быть порядка 5-15 минут, не часов
- если `systemd` timer недоступен, временно допустим cron, но это fallback
- scheduling должен входить в deploy scope, а не оставаться “на потом”

## 8. Как должен выглядеть следующий инцидент

Если runtime DB снова умерла:

1. сервис остановили
2. старую DB убрали в архив
3. создали новую DB
4. импортировали последний runtime artifact
5. доиграли recent raw journal от cursor
6. стартовали сервис

Что не должно происходить:

- не должно быть полного дропа состояния
- не должно быть giant history replay для простого запуска
- не должно быть возврата к старым мертвым кошелькам

## 9. Trading readiness after restore

После `restore` торговать можно не всегда.

Разрешение такое:

1. `healthy` после restore:
   - можно переходить к торговле после короткой operator-проверки
2. `degraded` после restore:
   - это не финальный trading state
   - торговлю не включать, кроме отдельной аварийной политики
3. `fail_closed` после restore:
   - торговлю не включать вообще

Для нормального trading-ready restore должно быть выполнено все:

1. `discovery` вышел в `healthy`
2. `scoring_source` идет из fresh raw path
3. `active_follow_wallets > 0`
4. top wallets свежие и меняются по текущему raw flow
5. restore не потребовал полного reread всей истории

Ключевое правило:

- если после restore нужно ждать 5 дней накопления, значит restore-контракт
  сделан неправильно

## 10. One-button restore contract

One-button restore означает ровно следующее:

1. одной командой создается fresh runtime DB
2. импортируется последний `runtime artifact`
3. доигрывается `recent raw journal` от сохраненного cursor
4. сервис стартует
5. `discovery` возвращается в рабочее состояние без полного reread истории

One-button restore обещает immediate healthy только если:

1. до инцидента уже существовали свежий `runtime artifact`
2. до инцидента уже существовал свежий `recent raw journal`

Для текущего первого инцидента без prebuilt raw journal это обещание еще не
выполнено. Там возможен только one-time bootstrap cost.

One-button restore не означает:

1. взять старую сломанную DB и “полечить”
2. ждать многодневный rebuild
3. использовать старых мертвых кошельков
4. запускать aggregate/backfill ради самого старта

## 11. Done criteria

План считается выполненным только если:

1. runtime DB можно уничтожить и поднять заново без старой DB
2. restore не зависит от aggregate/backfill readiness
3. restore не требует старых кошельков
4. restore использует fresh raw data
5. restore может вернуть `healthy discovery` без ожидания 5 дней накопления
6. one-button restore поднимает новый runtime без полного reread истории
7. следующий инцидент не приводит к “дропаем базу и собираем заново”

## 12. Один итог

Чтобы больше не дропать базу, тебе нужен не “еще один recovery flow”.

Тебе нужен вот такой контракт:

- broken runtime DB is disposable
- runtime artifact is external
- recent raw journal is external
- restore = artifact + recent raw replay
- trading allowed only after healthy restore

На языке текущего инцидента это означает еще и следующее:

- giant replay с multi-day ETA не считается restore
- старый recovery path можно остановить без сожалений
- future restore contract должен делать минуты, а не дни

## 13. Auditor execution batches

### Batch 1. Runtime artifact + bootstrap-degraded runtime contract

Цель батча:

- довести до завершенного состояния первый production-usable кусок restore
  контракта
- не делать косметический CLI без runtime semantics
- закрыть gap, где stale artifact импортируется, но `copybot-app` сам же убивает
  его на старте

Что должно быть завершено в этом батче:

1. Runtime artifact export/import CLI.
2. Полный storage contract для artifact.
3. Явный runtime contract для `bootstrap-degraded`, который переживает startup
   `copybot-app`.
4. Restore verdict, который не плодит отдельную ложную логику freshness.
5. Regression coverage на весь новый контракт.

Что не считается завершением батча:

- только новые bin-файлы без изменения runtime semantics
- только JSON export/import без интеграции в startup/runtime contract
- только happy-path тесты без stale / bootstrap-degraded сценариев
- любые “TODO later” в критических местах bootstrap-degraded logic

Границы батча:

- recent raw journal sidecar storage сюда не входит
- giant replay / aggregate recovery сюда не возвращаем
- этот батч должен оставить кодовую базу в состоянии, где artifact restore уже
  реален как bounded degraded path even before journal lands

Готовый промт для кодера:

> Реализуй **завершенный Batch 1** из `DISCOVERY_RUNTIME_RESTORE_PLAN_2026-03-23.md`: `runtime artifact export/import + bootstrap-degraded runtime contract`. Это **не косметический фикс** и **не частичный CLI prototype**. Работа должна быть доведена **до завершенного состояния в коде, storage contract и тестах**, чтобы этот батч можно было принять как production-meaningful piece of the restore architecture.
>
> Что нужно сделать:
>
> 1. Добавить полноценные утилиты:
>    - `crates/discovery/src/bin/discovery_runtime_export.rs`
>    - `crates/discovery/src/bin/discovery_runtime_restore.rs`
>
> 2. Добавить storage/model contract для runtime artifact.
>    Artifact обязан включать один **консистентный snapshot**:
>    - exact publication truth
>    - publication metadata
>    - discovery runtime cursor
>    - latest published wallet metrics snapshot
>    - gate metadata, нужные для freshness validation
>    Нельзя делать artifact, собранный из несогласованных кусков state.
>
> 3. Реализовать restore semantics:
>    - normal restore валидирует freshness artifact против runtime gate
>    - normal restore не переписывает `last_published_at` на `now`
>    - normal restore не переписывает `last_published_window_start` на текущий bucket
>    - stale artifact в normal restore не должен становиться trading-ready path
>
> 4. Реализовать **полный runtime contract** для `bootstrap-degraded`.
>    Это ключевая часть батча.
>    В текущем коде простой импорт stale artifact не выживает, потому что startup/runtime считает stale publication truth unusable.
>    Значит нужен **не только CLI flag**, а persisted runtime semantics, которые переживают запуск `copybot-app`.
>    Минимально допустимо:
>    - добавить отдельный runtime state (`BootstrapDegraded`) в `DiscoveryRuntimeMode` или эквивалентный явный persisted marker
>    - научить startup/runtime отличать explicit bootstrap-degraded state от просто stale publication truth
>    - пока runtime в bootstrap-degraded:
>      - trading remains disabled
>      - state не считается healthy/trading-ready
>      - imported artifact не стирается мгновенно только из-за stale age
>    - выход из bootstrap-degraded происходит только после реального восстановления fresh raw truth по нормальному runtime path
>
> 5. Restore verdict:
>    - не делать отдельную “параллельную правду”
>    - переиспользовать ту же freshness/runtime logic, что использует основной discovery runtime
>    - `runtime cursor restored` сам по себе недостаточен
>    - verdict должен отличать:
>      - normal trading-ready restore
>      - explicit bootstrap-degraded restore
>      - fail-closed restore
>
> 6. Добавить regression coverage.
>    Нужны тесты минимум на:
>    - export/import roundtrip консистентного artifact
>    - stale artifact rejects normal restore
>    - stale artifact in `--bootstrap-degraded` restores explicit bootstrap-degraded state
>    - startup `copybot-app` не убивает bootstrap-degraded artifact мгновенно
>    - normal stale publication truth по-прежнему отвергается как recent truth
>    - runtime can later leave bootstrap-degraded only through fresh raw recovery semantics
>
> 7. Обновить существующие operator/status/readiness surfaces, если нужно, чтобы новый runtime state был наблюдаем и недвусмысленен.
>
> Жесткие требования:
>
> - Не оставляй критический behavior на “later”.
> - Не делай половинчатую реализацию, где CLI есть, а runtime contract отсутствует.
> - Не возвращай giant replay / aggregate path в boot path.
> - Не ломай действующий Stage 1 contract для `healthy / degraded / fail_closed`.
> - Если потребуется изменить enum, storage parsing/serialization, startup logic, discovery runtime logic, status commands и тесты, делай это в этом же батче.
>
> Ожидаемый результат батча:
>
> - artifact export/import существует и работает
> - bootstrap-degraded restore существует как реальный persisted runtime state, а не фиктивный CLI режим
> - `copybot-app` может стартовать после такого restore, не уничтожая imported state в первую же секунду
> - код и тесты доказывают, что это завершенный кусок restore architecture, а не черновик

Аудит статуса на `2026-03-23`:

- `partial pass`, но **Batch 1 пока не принят**
- storage artifact contract, export/import CLI, persisted `bootstrap-degraded`
  semantics, startup survival и целевые тесты реализованы
- найден один **blocker**, который не дает считать батч завершенным

Blocker:

- restore path заявляет `fresh runtime db`, но фактически проверяет только
  discovery-таблицы (`wallets`, `wallet_metrics`, `followlist`,
  `observed_swaps`, `discovery_strategy_state`, `discovery_runtime_state`,
  `trusted_wallet_metrics_snapshots`, `discovery_persisted_rebuild_state`)
- при этом startup `copybot-app` после restore читает и другой durable runtime
  state, в частности:
  - `shadow_lots` через `list_shadow_open_pairs()`
  - `risk_events` через `restore_pause_from_store()`
- значит restore сейчас можно применить к частично грязной runtime DB и
  получить phantom shadow positions / stale risk pause state, хотя код уже
  сообщил оператору, что DB “fresh”

Что должно быть исправлено до acceptance:

1. Restore обязан либо работать **только** в brand-new DB, либо явно и
   исчерпывающе валидировать пустоту всех runtime-bearing таблиц, которые
   могут повлиять на startup/runtime behavior.
2. Проверка fresh DB не должна ограничиваться discovery-only subset.
3. Нужен regression test, который доказывает, что restore отвергает dirty DB
   хотя бы при наличии:
   - `shadow_lots`
   - `risk_events`
   - и любого другого runtime-bearing state, который вы решите включить в
     strict preflight contract
4. Только после этого Batch 1 можно считать закрытым.

Follow-up промт для кодера:

> Закрой blocker из Batch 1 в `DISCOVERY_RUNTIME_RESTORE_PLAN_2026-03-23.md`.
> Batch 1 **не принят**, потому что `discovery_runtime_restore` и
> `restore_discovery_runtime_artifact()` пока не гарантируют настоящий
> `fresh runtime db` contract.
>
> Проблема:
>
> - restore сейчас проверяет только discovery-таблицы
> - но startup/runtime после restore читает и другой durable state, минимум:
>   - `shadow_lots` через `list_shadow_open_pairs()`
>   - `risk_events` через `restore_pause_from_store()`
> - в результате restore можно влить в частично грязную DB и получить stale
>   runtime side effects, хотя tool пишет, что это “fresh runtime db”
>
> Что нужно сделать:
>
> 1. Довести `fresh runtime db` contract до завершенного состояния.
>    Выбери и реализуй один из двух production-grade вариантов:
>    - либо restore разрешен только в brand-new / empty DB и это строго
>      проверяется
>    - либо restore разрешен в migrated DB, но preflight исчерпывающе
>      проверяет пустоту всех runtime-bearing tables, которые могут повлиять на
>      startup/runtime behavior
>
> 2. Если выбираешь второй вариант, проверь не только discovery tables, но и
>    весь relevant durable runtime state. Минимум включить:
>    - `shadow_lots`
>    - `risk_events`
>    - другие runtime-bearing таблицы/sidecars, которые могут изменить startup,
>      execution gating, risk gating или shadow accounting
>
> 3. Обнови error contract так, чтобы оператору было понятно, какая именно
>    таблица/категория state делает DB грязной.
>
> 4. Добавь regression coverage:
>    - restore rejects DB with existing `shadow_lots`
>    - restore rejects DB with existing `risk_events`
>    - restore still succeeds on genuinely fresh DB
>    - если есть grouped preflight helper, тесты должны покрывать его contract
>
> 5. Не оставляй это как cosmetic guard. Работа должна быть завершена так,
>    чтобы после acceptance действительно существовал честный `fresh runtime db`
>    restore contract.
>
> Жесткие требования:
>
> - не ослабляй уже реализованный Batch 1 contract
> - не убирай bootstrap-degraded semantics
> - не переводи проблему в docs-only warning
> - не считай задачу завершенной без тестов на dirty DB rejection

Resolution update on `2026-03-23`:

- blocker закрыт в коде через **strict empty runtime DB contract**
- выбран production path: restore разрешен только в runtime DB, где после
  migrations нет durable rows ни в одной user table, кроме
  `schema_migrations`
- preflight теперь не ограничивается discovery-only subset; он инвентаризирует
  все runtime-bearing user tables и отвергает restore при любом dirty state,
  включая минимум:
  - `shadow_lots`
  - `risk_events`
  - а также любые другие непустые durable runtime tables
- operator-facing error contract теперь явно показывает `table + category`
  dirty state
- regression coverage добавлена на:
  - dirty-table inventory helper contract
  - reject restore with existing `shadow_lots`
  - reject restore with existing `risk_events`
  - success path on genuinely fresh DB

Final Batch 1 acceptance update on `2026-03-23`:

- Batch 1 **принят**
- artifact export/import, persisted `bootstrap-degraded` runtime contract,
  restore verdict и strict empty runtime DB contract считаются закрытыми
- дополнительный cleanup после acceptance тоже завершен:
  - удалены мертвые helper-методы trusted-selection legacy path
  - удален мертвый test helper
  - `copybot-discovery` больше не оставляет `dead_code` warning по этому batch
- повторно подтверждены целевые тесты:
  - `cargo test -p copybot-discovery --bin discovery_runtime_restore`
  - `cargo test -p copybot-discovery --lib -- --skip quality_cache::tests::resolve_token_quality_for_mints_returns_error_on_fatal_cache_write_failure`

### Batch 2. Recent raw journal sidecar + replay path

Цель батча:

- довести restore architecture от artifact-only bootstrap до реального path,
  который может вернуть runtime к fresh raw truth без giant replay
- внедрить production V1 recent raw journal как отдельный durable sidecar
- закрыть gap между `runtime cursor restored` и фактической raw coverage

Что должно быть завершено в этом батче:

1. Отдельный durable `recent raw journal` store.
2. Отдельный bounded writer path в journal на ingest path.
3. Retention / rotation contract для journal.
4. Replay/import path от saved cursor в fresh runtime DB.
5. Restore verdict, который учитывает raw coverage после journal replay.
6. Regression coverage и telemetry для sidecar path.

Что не считается завершением батча:

- “просто экспорт observed_swaps в файл”
- синхронная двойная запись в ту же write critical section без budget и
  telemetry
- journal без replay path
- replay path без raw coverage validation
- docs-only описание без работающего end-to-end restore flow

Готовый промт для кодера:

> Реализуй **завершенный Batch 2** из `DISCOVERY_RUNTIME_RESTORE_PLAN_2026-03-23.md`: `recent raw journal sidecar + replay path`. Это **не косметический фикс** и **не partial prototype**. Работа должна быть доведена **до завершенного состояния в коде, storage contract, replay path, observability и тестах**, чтобы это был production-usable V1 restore path, а не заготовка.
>
> Что нужно сделать:
>
> 1. Добавить отдельный durable V1 `recent raw journal`.
>    Минимально допустимый production contract:
>    - отдельная SQLite DB, например `state/discovery_recent_raw.db`
>    - не reuse основной runtime DB
>    - bounded recent `observed_swaps` horizon
>    - retention = `scoring_window_days` + safety buffer
>    - journal независим от aggregate/backfill tables
>
> 2. Реализовать отдельный writer path на ingest path.
>    Это ключевая часть батча.
>    Нельзя просто делать синхронный второй insert рядом с primary raw write в
>    ту же critical section.
>    Нужен production-grade путь, например:
>    - отдельный async sidecar writer / bounded queue
>    - либо другой явно bounded fan-out path после primary raw commit
>    Обязательно сохранить устойчивость ingest path под нагрузкой.
>
> 3. Реализовать journal retention / pruning contract.
>    Нужно гарантировать:
>    - bounded growth
>    - сохранение достаточного raw horizon для restore
>    - отсутствие giant unbounded history accumulation
>
> 4. Реализовать replay/import path из journal в fresh runtime DB от
>    сохраненного discovery runtime cursor.
>    Явно выбери и доведи до конца один путь:
>    - либо journal replay импортирует recent raw data в `observed_swaps`
>      основной runtime DB, и discovery дальше работает как сейчас
>    - либо discovery учится читать journal напрямую
>    Полумеры не подходят. В конце батча должен существовать работающий
>    end-to-end restore path.
>
> 5. Обновить restore command / tooling.
>    После Batch 2 оператор должен иметь реальный flow:
>    - fresh runtime DB
>    - runtime artifact restore
>    - recent raw journal replay from cursor
>    - restore verdict
>    Если для этого нужен новый bin/tooling или расширение существующего
>    `discovery_runtime_restore`, сделай это в этом же батче.
>
> 6. Расширить restore verdict и observability.
>    Verdict больше не должен опираться только на artifact freshness и cursor.
>    Нужны явные сигналы:
>    - journal available / replayed
>    - raw coverage satisfied for runtime horizon или нет
>    - restore result distinguishes:
>      - trading-ready after journal replay
>      - bootstrap-degraded artifact-only restore
>      - fail-closed restore
>    Не плодить отдельную ложную правду; использовать existing runtime
>    readiness/freshness semantics там, где это возможно.
>
> 7. Добавить telemetry.
>    Минимум измерять:
>    - journal writer queue backlog
>    - journal batch latency
>    - primary raw writer latency impact
>    - sqlite busy / retry pressure для journal path
>    - replay progress / replay rows
>
> 8. Добавить regression coverage.
>    Нужны тесты минимум на:
>    - journal writer persists bounded recent observed_swaps
>    - retention prunes old rows but keeps required restore horizon
>    - replay from runtime cursor restores raw window into fresh runtime DB
>    - replay path не требует giant full-history reread
>    - restore verdict stays non-trading-ready без raw coverage
>    - restore verdict can become trading-ready after valid artifact + journal replay
>    - journal path не ломает existing bootstrap-degraded semantics
>
> Жесткие требования:
>
> - не ломай принятый Batch 1 contract
> - не возвращай giant replay / aggregate path в boot path
> - не делай journal как implicit side effect без explicit restore contract
> - не оставляй double-write risk без bounded queue / telemetry
> - не считай задачу завершенной без end-to-end restore path и тестов
>
> Ожидаемый результат батча:
>
> - в системе существует отдельный recent raw journal sidecar
> - ingest path умеет наполнять его без деградации primary runtime contract
> - fresh runtime DB можно поднять через artifact + journal replay
> - restore verdict различает artifact-only bootstrap и реальный raw-backed restore
> - код и тесты доказывают, что это production-meaningful V1 restore path

Final Batch 2 acceptance update on `2026-03-24`:

- Batch 2 **принят**
- recent raw journal sidecar, bounded async writer, retention/pruning,
  replay/import path в fresh runtime DB, persisted recent-raw restore state и
  restore verdict считаются закрытыми
- bootstrap-degraded semantics из Batch 1 сохранены
- повторно подтверждены целевые тесты:
  - `cargo test -p copybot-storage --lib recent_raw_journal`
  - `cargo test -p copybot-app recent_raw_journal`
  - `cargo test -p copybot-discovery --bin discovery_status`
  - `cargo test -p copybot-discovery --bin discovery_runtime_restore`
  - `cargo test -p copybot-discovery --lib -- --skip quality_cache::tests::resolve_token_quality_for_mints_returns_error_on_fatal_cache_write_failure`

Residual note after audit:

- текущий `raw_coverage_satisfied` для journal replay опирается на bounded window
  extents и cursor coverage, а не на отдельный gap-lineage proof
- это не блокирует acceptance текущего Batch 2, потому что соответствует
  текущей runtime semantics, но если позже понадобится stronger continuity
  guarantee, это уже отдельный tightening batch, а не незавершенность Batch 2

### Batch 3. Scheduled exports + restore drill

Цель батча:

- перевести restore architecture из “код существует” в operational contract
- обеспечить, что свежий runtime artifact и recent raw journal реально доступны
  во время следующего инцидента
- провести one-button restore drill и зафиксировать реальный RTO/RPO

Что должно быть завершено в этом батче:

1. Scheduled runtime artifact export.
2. Scheduled recent raw journal snapshot/export strategy.
3. Operator runbook для restore.
4. Restore drill на чистую DB.
5. Документированный RTO/RPO и failure modes.

Что не считается завершением батча:

- ручной запуск export-команд “когда-нибудь”
- наличие bin-файлов без deploy wiring
- отсутствие проверенного restore drill
- слова про RTO/RPO без реального измерения

Готовый промт для кодера:

> Реализуй **завершенный Batch 3** из `DISCOVERY_RUNTIME_RESTORE_PLAN_2026-03-23.md`: `scheduled exports + restore drill`. Это **не docs-only задача** и **не просто timer-файлы**. Работа должна быть доведена **до завершенного operational состояния в коде, deploy wiring, runbook и проверке restore drill**, чтобы следующий инцидент не зависел от ручной импровизации.
>
> Что нужно сделать:
>
> 1. Реализовать scheduled export runtime artifact.
>    Production requirement:
>    - регулярный export cadence заметно меньше freshness gate
>    - для live-конфига ориентир порядка 5-15 минут, не часы
>    - артефакт должен писаться в predictable operator-visible location
>    - должна быть rotation/retention политика, чтобы не копить мусор бесконечно
>
> 2. Реализовать scheduled strategy для recent raw journal.
>    Явно выбери и доведи до конца production path:
>    - либо регулярные snapshot/export копии journal sidecar
>    - либо другой эквивалентный backup/rotation contract
>    Нельзя оставлять journal только как локальный runtime sidecar без
>    operational story для следующего инцидента.
>
> 3. Добавить deploy wiring.
>    Минимально допустимо:
>    - systemd service + systemd timer для artifact export
>    - systemd service/timer или эквивалентный production-safe path для journal snapshot/export
>    - конфигурируемые пути, retention и cadence
>    Если в репозитории уже есть deploy conventions, следуй им.
>
> 4. Подготовить restore runbook.
>    Нужен operator-facing документ с четким flow:
>    - stop service
>    - archive broken runtime DB
>    - create fresh runtime DB
>    - restore artifact
>    - replay/import recent raw journal
>    - inspect restore verdict
>    - enable service / keep fail-closed depending on verdict
>    Runbook должен ссылаться на реальные команды и реальные output fields.
>
> 5. Реализовать restore drill.
>    Это ключевая часть батча.
>    Нужен воспроизводимый scripted path, который:
>    - создает fresh target
>    - поднимает runtime через artifact + journal replay
>    - собирает итоговый verdict/status
>    - фиксирует elapsed time и итоговый state
>    Если для этого нужен отдельный script/tool, добавь его.
>
> 6. Зафиксировать measured RTO/RPO.
>    Нужны не абстрактные слова, а operator-visible значения:
>    - какой RTO получился на drill
>    - какой RPO гарантируется текущим export/snapshot cadence
>    - какие failure modes остаются
>
> 7. Добавить regression/integration coverage там, где это уместно.
>    Минимум:
>    - config/loader tests для новых export/snapshot settings
>    - tests на path resolution / retention wiring
>    - если добавлен script/tool, smoke path должен быть проверяемым
>
> Жесткие требования:
>
> - не ломай принятые Batch 1 и Batch 2 contracts
> - не подменяй drill ручным checklist без исполняемого path
> - не оставляй scheduling “на потом”
> - не считай задачу завершенной без measured restore drill outcome
>
> Ожидаемый результат батча:
>
> - artifact export и journal backup/snapshot запускаются по расписанию
> - у оператора есть реальный runbook, а не теория
> - restore drill воспроизводим и измерен
> - RTO/RPO зафиксированы в репозитории на основе фактического прогона

Batch 3 completion note (`2026-03-24`):

- добавлен явный config contract `runtime_restore_ops` с cadence/retention/path
  для artifact export, journal snapshot и drill workspace
- `discovery_runtime_export` получил scheduled mode с `latest.json`,
  archive rotation и cadence-aware skip
- добавлен `discovery_recent_raw_snapshot` с отдельным snapshot contract для
  `recent_raw_journal`, metadata manifest, rotation и cadence-aware skip
- добавлены systemd templates:
  - `ops/server_templates/copybot-discovery-runtime-export.service`
  - `ops/server_templates/copybot-discovery-runtime-export.timer`
  - `ops/server_templates/copybot-discovery-recent-raw-snapshot.service`
  - `ops/server_templates/copybot-discovery-recent-raw-snapshot.timer`
- добавлен operator runbook:
  - `ops/discovery_runtime_restore_runbook.md`
- добавлен scripted drill path:
  - `tools/discovery_restore_drill.sh`
  - `tools/discovery_restore_drill_smoke_test.sh`
  - `crates/discovery/src/bin/discovery_restore_demo_fixture.rs`
- measured local release drill outcome зафиксирован:
  - `measured_rto_ms = 690`
  - `guaranteed_rpo_minutes = 10`
  - final verdict = `trading_ready`
- residual failure modes остаются прозрачными и documentированы:
  - RPO ограничен более медленным из artifact export cadence и journal snapshot cadence
  - restore остается fail-closed без cursor/raw coverage
  - bootstrap-degraded остается non-trading-ready до нормального fresh raw recovery path

Final Batch 3 acceptance update on `2026-03-24`:

- Batch 3 **принят**
- scheduled export/snapshot contract, deploy wiring, operator runbook,
  scripted restore drill и measured `RTO/RPO` считаются закрытыми
- закрыт operational blocker по `discovery_recent_raw_snapshot --scheduled`:
  healthy skip теперь допустим только при целой latest surface
  (`latest.json` + `latest.sqlite`)
- scheduled journal snapshot path теперь self-heal'ит broken latest surface:
  - missing `latest.sqlite` -> восстановление из archive snapshot
  - missing `latest.json` -> перепись metadata из archive или current latest
    snapshot
  - если latest surface broken и archive self-heal невозможен, path уходит в
    fresh snapshot write, а не в ложный green skip
- output contract расширен явными operator-visible полями:
  - `latest_surface_status`
  - `latest_surface_action`
- повторно подтверждены целевые прогоны:
  - `cargo test -p copybot-discovery --bin discovery_recent_raw_snapshot`
  - `cargo test -p copybot-discovery --bin discovery_runtime_export`
  - `cargo test -p copybot-discovery --bin discovery_runtime_restore`
  - `tools/discovery_restore_drill_smoke_test.sh`
- дополнительно вручную воспроизведен бывший blocker path:
  1. scheduled snapshot создает `latest.sqlite` и `latest.json`
  2. `latest.sqlite` удаляется
  3. следующий scheduled run в пределах cadence возвращает
     `self_healed_latest_surface`
  4. `latest.sqlite` восстанавливается из archive snapshot

## 14. Live server verdict on 2026-03-24

Batch 1-3 закрыли repo-level restore architecture, но live incident закрыт не
полностью.

На реальном сервере уже достигнуто:

- giant replay path убран из active runtime path
- новый restore stack реально раскатан и работает
- live уже переведен на fresh restored runtime DB
- старая `live_copybot.db` удалена с диска и больше не участвует ни в каком path
- discovery больше не сидит в `active_follow_wallets = 0`
- live runtime держит `15` кошельков через explicit bootstrap-degraded bridge
- artifact export baseline создан
- recent raw journal sidecar живет и наполняется
- recent raw snapshot baseline создан
- fresh-DB restore drill на реальных live surfaces проходит и дает
  воспроизводимый `bootstrap_degraded` verdict

Но business closure пока не достигнут:

- live runtime все еще не `healthy`
- trading-ready restore на сервере еще не доказан
- `execution.enabled = false` должен оставаться false
- copy trading / shadow trading не должны открывать позиции
- текущий server state нельзя называть “recovered” в healthy-смысле

Отдельно важно, что удаление старой базы уже ответило на один спорный вопрос:

- проблема была не в том, что runtime все еще “сидит на старой базе”
- старая база уже удалена
- а `healthy` все равно не получен
- значит remaining blocker теперь чисто в missing recent raw coverage, а не в
  старом runtime DB state

Отдельно важно зафиксировать текущий live sharp edge:

- `copybot-discovery-runtime-export.timer` можно держать включенным
- `copybot-discovery-recent-raw-snapshot.timer` на этом сервере снова
  включен
- artifact cadence и recent raw snapshot cadence теперь обе автоматизированы
- это означает, что steady-state restore surface на проде теперь симметричен:
  - runtime artifact cadence автоматизирован
  - recent raw snapshot cadence тоже автоматизирован

Отдельно важно зафиксировать, что новая ветка экспериментов тоже уже narrowed:

- current blocker больше не выглядит как “не тот provider”
- QuickNode generic path и Helius-specific path сошлись к одному и тому же
  coverage outcome
- значит следующий шаг не должен быть еще одной ротацией адресного
  gap-fill provider

## 15. Что код должен сделать дальше, чтобы реально закрыть инцидент

Ниже не новый большой “план восстановления всего мира”, а узкий список того,
что действительно нужно, чтобы закрыть оставшийся gap.

### 15.0 TODO list (`2026-03-24`)

Это текущий рабочий TODO, а не wishlist.

1. Не идти по `wait-only` модели.
   - ожидание допустимо только как фоновый шанс накопить raw window
   - ожидание не считается primary plan и не считается closure criterion

2. За `1-2` дня сделать короткий source-validation для bounded
   program-scoped historical raw по `Raydium` / `PumpSwap`.
   - цель не “сразу написать новый gap-fill bin вслепую”
   - цель: доказать или опровергнуть, что у нас вообще есть source contract,
     который может отдать bounded raw window на интервале
     `required_window_start -> journal_covered_since`
   - acceptance этого шага только один:
     - source-validation показывает, что provider действительно отдает
       достаточный program-scoped historical raw
     - либо честно доказывает, что этот путь не подтверждается текущими
       источниками

3. Snapshot ветка на текущий момент закрыта.
   - `copybot-discovery-recent-raw-snapshot.timer` уже возвращен в `enabled`
     без остановки `copybot-app`
   - live rerun уже дал `written`
   - practical completion на large journal доказан на реальном сервере
   - этот пункт больше не является активным blocker для incident closure

4. Каждый день смотреть, движется ли покрытие к `required_window_start`, а не
   просто ждать.
   - мониторить `covered_since`, `covered_through`, `raw_coverage_satisfied`,
     состояние timer'ов и свежесть artifact/recent-raw surfaces
   - если покрытие не движется, не продолжать пассивное ожидание как будто
     “еще чуть-чуть и само закроется”

### 15.1 Не путать текущий bridge с recovery closure

Сейчас сервер уже не мертв, но это все еще bridge-state.

Значит нельзя:

- форсить `healthy`
- ослаблять freshness / raw coverage gate
- включать `execution` в `bootstrap_degraded`
- считать top-15 bridge нормальной trading truth

Иначе мы просто сделаем красивую ложную зелень и снова въедем в еще более
грязный инцидент.

### 15.2 Первое узкое место по коду: live-safe recent raw snapshot

Следующий кодовый приоритет номер один:

- довести `discovery_recent_raw_snapshot` до состояния, где он стабильно
  работает под реальными live writes на сервере, а не только в локальном
  smoke path

Почему это важно:

- без этого у нас нет надежного steady-state capture для `recent raw journal`
- значит следующий инцидент снова может застать нас без свежего raw snapshot
- текущий disabled timer на проде делает Batch 3 operationally неполным именно
  для реального сервера

Что я бы требовал от следующего кода:

1. snapshot path должен быть writer-safe на живом `discovery_recent_raw.db`
2. решение не должно требовать остановки `copybot-app`
3. операторский результат должен быть однозначным:
   - snapshot written
   - self-healed
   - retryable busy/deferred
   - hard failure
4. после фикса timer должен быть безопасно возвращаем в `enabled`

### 15.3 Второе узкое место по коду: deterministic path из bootstrap-degraded в healthy

Сейчас главный бизнес-gap не в том, что “restore chain отсутствует”.

Он уже есть.

Главный gap в том, что live runtime и live drill пока доходят только до
`bootstrap_degraded`.

Причина простая:

- текущий live recent raw snapshot покрывает только короткий свежий хвост
- scoring horizon в live-конфиге = `5` дней
- значит текущего raw window недостаточно, чтобы честно получить
  trading-ready `healthy`

Поэтому “просто ждать” я не считаю основной стратегией.

Да, ожидание теоретически может помочь, но это слишком похоже на прошлую
ловушку “еще один день, еще один blocker”.

Что код должен дать вместо этого:

1. targeted bounded gap-fill для missing recent raw horizon
2. текущий address-scoped gap-fill уже проверен на QuickNode и Helius и не
   закрыл окно
3. значит следующий реальный кандидат уже не provider swap, а другой fetch
   contract
4. этот следующий gap-fill должен наполнять `recent raw journal` или fresh restore target,
   а не возвращать giant replay в boot path
5. gap-fill должен быть ограничен runtime horizon, а не всей историей
6. после gap-fill должен существовать явный operator flow:
   - fresh DB
   - artifact restore
   - recent raw journal replay
   - gap-fill apply if needed
   - final verdict

И еще одно жесткое правило после сегодняшнего cutover:

- не проектировать следующий шаг вокруг старой runtime DB
- старой DB больше нет
- gap-fill должен работать без нее

Иными словами, следующий код должен не “улучшать bootstrap-degraded”, а
довести path до честного выхода в `healthy` без многодневной пассивной надежды.

При этом нужно честно зафиксировать текущее знание:

- у нас пока нет доказанного production-ready `program history` plan
- это не code-complete next step, а инженерная гипотеза
- теоретически более правильный источник выглядел бы как bounded historical
  raw stream по swap-программам / program history, а не по wallet-address
  history
- но конкретный provider contract для такого path пока не подтвержден
- значит next step по `program history` должен считаться новой исследовательской
  веткой, а не уже доказанным решением

### 15.4 Нужен реальный server-side proof of healthy exit

После фикса snapshot path и после targeted gap-fill нужен не абстрактный вывод,
а реальный серверный proof:

1. на fresh DB выполняется restore из live artifact + live recent raw inputs
2. verdict становится `trading_ready`
3. `runtime_mode` становится `healthy`
4. только после этого можно обсуждать включение `execution`

Пока такого доказательства нет, инцидент надо считать открытым.

### 15.5 Optional hardening, но уже после closure

После закрытия основного инцидента имеет смысл отдельно рассмотреть:

- stronger continuity proof для raw coverage, чтобы уменьшить риск false
  trading-ready verdict на sparse/gappy raw data
- alerting на слишком долгий `bootstrap_degraded`
- alerting на disabled snapshot timer или stale journal snapshot

Но это следующий слой.

Сначала нужно закрыть две практические вещи:

1. live-safe snapshot under write pressure
2. deterministic healthy exit path вместо пассивного ожидания

## 16. Auditor review of bounded gap-fill batch (`2026-03-24`)

Статус аудита:

- batch **принят**
- code path и operator contract теперь согласованы end-to-end
- unit/integration/smoke coverage по bounded window derivation, replay merge,
  trading-ready verdict и operator surface проходит

Что подтверждено:

- `discovery_raw_gap_fill` существует как реальный bin
- `discovery_runtime_restore` умеет принимать `--gap-fill-db-path`
- restore state теперь хранит effective coverage и gap-fill replay facts
- локальные целевые тесты по `discovery_raw_gap_fill`,
  `discovery_runtime_restore`, `discovery_status` и `copybot-config` проходят
- `ops/server_templates/live.server.toml.example` теперь явно содержит
  `[recent_raw_gap_fill]`
- runbook явно показывает source URL contract и передает
  `--helius-http-url` в `discovery_raw_gap_fill`
- archive step больше не опирается на фантомный `live_runtime_current.db`,
  а архивирует фактический `sqlite.path` из live config
- operator smoke test теперь валит build, если template потеряет
  `[recent_raw_gap_fill]` или runbook снова вернет скрытый source URL / phantom
  DB path

Что именно было закрыто:

1. Config/template blocker:
   - documented operator path больше не скрывает source URL contract
   - template loader test подтверждает наличие bounded
     `recent_raw_gap_fill` config surface
   - runbook теперь runnable как self-contained flow

2. Runtime DB archive blocker:
   - runbook больше не зависит от несуществующего `live_runtime_current.db`
   - archive step приведен к actual deploy contract
   - incident flow теперь совпадает с тем, как live реально запущен

Повторно подтверждены проверки:

- `cargo test -p copybot-config --lib`
- `cargo test -p copybot-discovery --bin discovery_raw_gap_fill`
- `cargo test -p copybot-discovery --bin discovery_runtime_restore`
- `cargo test -p copybot-discovery --bin discovery_status`
- `bash tools/discovery_gap_fill_operator_contract_smoke_test.sh`
- `bash tools/discovery_restore_drill_smoke_test.sh`

Вывод аудита:

- deterministic healthy-exit batch теперь можно считать operationally complete
  на уровне репозитория
- old runtime DB не возвращен ни в код, ни в runbook, ни в operator reasoning
- remaining risk уже не в gap-fill operator contract, а в live server closure из
  разделов `14-15`: нужно доказать реальный `healthy` verdict на сервере, а не
  только в repo-level tests и runbook flow

## 17. Address-history gap-fill compare-run findings (`2026-03-24`)

Этот раздел фиксирует уже не теорию, а фактический compare-run на реальном
сервере.

Что проверено:

- generic address-scoped gap-fill на текущем QuickNode path
- отдельный Helius-specific bin
  `discovery_raw_gap_fill_helius`
- Helius запускался как explicit CLI override / separate output surface, а не
  как активный live config contract

Что получилось:

- оба пути дали одинаковый practical outcome:
  - `fetched_rows = 95`
  - `inserted_rows = 95`
  - `gap_fill_covered_since = 2026-03-19T13:43:51Z`
  - `gap_fill_covered_through_cursor = 2026-03-21T09:01:20Z`
  - `sufficient_for_healthy_restore = false`
- Helius-specific path оказался быстрее и чище operationally, но не дал
  лучшего coverage outcome

Что это означает:

- проблема больше не выглядит как “QuickNode плохой, Helius хороший”
- проблема также уже не выглядит как “нужно просто другой address-history API
  method”
- текущая address-history ветка на этом инциденте уперлась в свой потолок

Operational conclusion:

- QuickNode остается приоритетным активным path на сервере
- Helius не должен silently подменять live source contract
- Helius остается только как отдельный last-resort experimental path
- следующая инженерная ставка уже не на provider swap, а на другой type of
  historical raw source / fetch contract, если мы решим продолжать

## 18. Program-scoped historical source validation (`2026-03-24`)

Следующий шаг после address-history compare-run не должен быть blind coding
нового program-gap-fill bin.

Сначала нужно проверить, существует ли вообще рабочий source contract для
bounded program-scoped historical raw по missing window
`[required_window_start, journal_covered_since)`.

Validation contract для этого шага:

- отдельный bin:
  `crates/discovery/src/bin/discovery_program_history_source_validate.rs`
- QuickNode-first source path:
  - `getSlot`
  - `getBlockTime`
  - `getBlocks`
  - `getBlock`
- target programs:
  - Raydium
  - PumpSwap
- bounded window может быть передан явно или derived из текущего restore state
- output должен давать явный verdict:
  - `viable`
  - `not_proven_due_to_budget`
  - `not_proven_due_to_provider_throttling`
  - `not_proven_due_to_sparse_program_history`
  - `non_viable_source_contract`

Важно:

- это validation-only step, не restore path
- он не должен делать вид, что healthy restore уже решен
- old runtime DB не возвращается ни как source, ни как fallback
- только если этот validation step даст честный `viable`, есть смысл делать
  следующий batch на program-scoped gap-fill
- exhaustion local scan budget не должен маркироваться как provider failure;
  это отдельный `not_proven_due_to_budget` outcome
- QuickNode `429 Too Many Requests` path тоже не должен маркироваться как
  provider failure; сначала local pacing + retry/backoff, потом только честный
  `not_proven_due_to_provider_throttling`, если throttling не ушел

### 18.1 Auditor review of the validation batch (`2026-03-24`)

Статус аудита:

- batch **принят**
- large-window operator contract теперь разведен честно:
  - `not_proven_due_to_budget`
  - `not_proven_due_to_sparse_program_history`
  - `non_viable_source_contract`
  - `viable`
- local budget exhaustion больше не маркируется как доказанный provider failure
- staged slot sampling добавлен как explicit validation method для live-sized
  bounded window
- runbook/template/operator surface синхронизированы с этим contract

Что именно было исправлено:

- default live-sized path больше не сводится к ложному hard negative
- documented command теперь дает либо содержательный source verdict, либо
  честный `not_proven_due_to_budget`
- runbook явно показывает escalation path через `--max-slots-to-scan`
- template/config surface теперь включает `sampling_segments`

Повторно подтверждены проверки:

- `cargo test -p copybot-config --lib`
- `cargo test -p copybot-discovery --bin discovery_program_history_source_validate`
- `cargo test -p copybot-discovery --bin discovery_runtime_restore`
- `bash tools/discovery_gap_fill_operator_contract_smoke_test.sh`

Вывод аудита:

- validation batch теперь можно считать accepted как honest source-proof stage
- это все еще validation-only step, а не готовый program-gap-fill restore path
- следующий decision point должен опираться уже на реальный server-side run
  этого validation tool, а не на еще одно blind coding усилие

### 18.2 Auditor follow-up on QuickNode throttling adaptation (`2026-03-24`)

Статус аудита:

- follow-up batch **принят**
- QuickNode-first validation path больше не должен сам загонять себя в `429`
  storm на provider limit `125 req/s`
- throttling outcome теперь разведен честно:
  - `not_proven_due_to_provider_throttling`
  - `non_viable_source_contract`
- hard-negative `non_viable_source_contract` больше не должен появляться
  только из-за того, что local tool превысил provider ceiling

Что именно было исправлено:

- добавлен per-process pacing contract для `getSlot` / `getBlockTime` /
  `getBlocks` / `getBlock`
- добавлены bounded `429` retry/backoff semantics
- добавлены explicit config knobs:
  - `program_history_validation.max_requests_per_second`
  - `program_history_validation.retry_429_max_attempts`
  - `program_history_validation.retry_429_backoff_ms`
- runbook/template/operator contract синхронизированы с QuickNode-first
  throttling semantics

Повторно подтверждены проверки:

- `cargo test -p copybot-config --lib`
- `cargo test -p copybot-discovery --bin discovery_program_history_source_validate`
- `cargo test -p copybot-discovery --bin discovery_runtime_restore`
- `bash tools/discovery_gap_fill_operator_contract_smoke_test.sh`

Вывод аудита:

- validation path теперь честнее ведет себя на live QuickNode contract
- следующий meaningful шаг уже server-side rerun с новым pacing/retry behavior
- если throttling останется и после этого, это будет уже честный provider /
  quota blocker, а не self-inflicted tool behavior

### 18.3 Live server rerun after QuickNode throttling adaptation (`2026-03-24`)

Фактический live rerun:

- live config был обновлен до:
  - `program_history_validation.max_requests_per_second = 60`
  - `program_history_validation.retry_429_max_attempts = 6`
  - `program_history_validation.retry_429_backoff_ms = 500`
- `discovery_program_history_source_validate` был реально прогнан на live
  missing window против current QuickNode endpoint
- outcome changed materially:
  - tool больше не умер мгновенно в `429 Too Many Requests`
  - process stayed alive and kept scanning under pacing
  - but it still did not emit a terminal JSON verdict before bounded
    `timeout 900`

Что это значит:

- self-inflicted QuickNode throttling storm действительно снят
- но practical closure для этой ветки пока не достигнут:
  - current QuickNode block-history validation path все еще operationally
    expensive on the real incident window
  - next decision point теперь уже не про correctness verdict semantics, а про
    actual source/path cost on live
- честный текущий вывод:
  - эта ветка больше не падает сразу по нашей вине
  - но и viable/non-viable live source proof пока еще не дала

### 18.4 Current engineering reading of the QuickNode path (`2026-03-24`)

Это важно зафиксировать явно для обсуждения:

- текущий server-side result не выглядит как “мы просто слишком рано
  остановили run”
- bounded `timeout 900` уже является meaningful signal для validation-only
  шага

Почему:

- до throttling adaptation path умирал почти сразу в `429`
- после pacing/retry adaptation path уже не падает мгновенно, а реально живет
  и считает под live QuickNode contract
- при этом terminal live verdict все равно не получен даже за bounded `900s`
- для validation-only шага это уже существенная operational cost, а не просто
  “надо было еще чуть-чуть подождать”

Текущая инженерная гипотеза:

- проблема теперь выглядит не как correctness bug в verdict semantics
- проблема выглядит как cost profile текущего QuickNode block-history path на
  live incident window
- то есть ветка уперлась уже не в self-inflicted throttling, а в practical
  cost / throughput самого validation contract

Из этого следуют два рациональных направления, если мы вернемся к
program-history ветке:

1. не “ждать дольше”, а удешевить сам validation contract
   - split phase A / phase B
   - phase A: быстрый proof of source viability по bounded sampling и
     target-program presence
   - phase B: дорогой parse до swap-shaped rows только после positive signal
   - цель: не платить full parse / full heavy block scan слишком рано

2. если QuickNode path даже после contract split остается слишком дорогим,
   менять source/path, а не крутить тот же validation run дольше
   - не еще один wallet-history provider
   - а другой program-scoped historical source contract

Чего этот документ пока не утверждает:

- что QuickNode path definitively dead

### 18.5 Audit sync: Phase A / Phase B validation split (`2026-03-24`)

В репозитории этот split теперь реализован как explicit operator contract в
`discovery_program_history_source_validate`:

- `--phase phase_a`:
  - cheap bounded viability proof
  - smaller `phase_a_*` budget
  - block sampling inside staged windows
  - early stop after target-program presence proof
  - positive verdict = `viable_enough_for_phase_b`
- `--phase phase_b`:
  - expensive parseability / usefulness proof
  - uses the larger parse budget
  - only здесь `viable` означает, что source дал parseable/useful enough raw
    signal for the next program-gap-fill batch

Явно зафиксировано и в output contract:

- `phase_b_required_for_final_source_proof = true` только в Phase A
- `final_source_proof_completed = true` только при positive Phase B

Это не закрывает deterministic healthy exit само по себе, но дает более
дешевый и более честный decision point по QuickNode path, чем прежний
однофазный validation run.

Статус аудита:

- принят
- заявленный split Phase A / Phase B подтвержден кодом, config/template
  contract и целевыми тестами
- новый operator contract больше не смешивает cheap source-presence proof с
  final parseability/usefulness proof

### 18.6 Audit sync: bounded terminal contract for expensive Phase B (`2026-03-24`)

Следующий узкий шаг после split тоже зафиксирован в коде:

- `phase_b` теперь не должен зависеть от внешнего `timeout 900`, чтобы
  завершиться
- у него появился отдельный internal cost budget:
  - `phase_b_max_blocks_to_fetch`
  - `phase_b_max_candidate_transactions_to_parse`
  - `phase_b_parseable_rows_target`
- добавлен terminal outcome:
  - `not_proven_due_to_phase_b_cost_budget`

Что это значит practically:

- если expensive QuickNode Phase B быстро находит достаточный parseable signal,
  он завершает run рано и дает terminal verdict
- если signal не найден до internal cost budget, run тоже завершается рано,
  но уже честным bounded `not_proven` outcome
- `non_viable_source_contract` больше не должен случайно использоваться для
  этой operational cost problem

Это по-прежнему не делает QuickNode expensive path automatically viable для
инцидента. Но теперь этот path дает уже meaningful bounded decision, а не
просто hanging run, который убивает внешний timeout.

Статус аудита:

- принят
- expensive `phase_b` теперь имеет собственный bounded terminal contract
- `not_proven_due_to_phase_b_cost_budget` честно отделяет operational cost
  problem от hard source-contract failure

- что другой source уже доказан

Честный текущий вывод:

- “просто подождать дольше” сейчас не выглядит сильной основной стратегией для
  этой ветки
- если возвращаться к `program_history_source_validate`, то уже не с идеей
  “дать ему больше времени”, а с идеей:
  - либо делать cheaper two-phase validation contract
  - либо менять source/path

## 19. Auditor review of live-safe recent raw snapshot batch (`2026-03-24`)

Статус аудита:

- batch **принят**
- writer-safe backup path, bounded retry/backoff, outcome states
  (`written` / `self_healed_latest_surface` / `retryable_busy` / `deferred` /
  `hard_failure`) и systemd/runbook contract теперь можно считать
  operationally complete
- correctness blocker в publish/manifest contract закрыт

Что именно блокирует acceptance:

- после успешного online backup и `rename` snapshot manifest сейчас строится не
  по только что записанному snapshot sqlite, а по live source journal state
- под реальными live writes это может завысить:
  - `row_count`
  - `covered_since`
  - `covered_through_cursor`
  - `last_batch_completed_at`
  относительно того, что реально попало в snapshot file

Почему это критично:

- тогда latest/archive metadata может описывать более свежий raw horizon, чем
  реально лежит в `latest.sqlite`
- restore / operator verdict потом будут смотреть на metadata и принимать
  решения на завышенном snapshot coverage
- это уже не transient contention detail, а correctness bug в steady-state
  restore surface

Что нужно исправить:

1. manifest после successful snapshot write должен строиться по snapshot file,
   а не по live source DB
2. metadata contract должен описывать ровно то, что реально лежит в snapshot
3. нужен regression test на live-write drift scenario:
   - snapshot contents отстают от source
   - manifest все равно совпадает именно со snapshot contents

Closure update (`2026-03-24`):

- blocker закрыт
- fresh snapshot manifest теперь строится read-only по уже опубликованному
  snapshot sqlite, а не по live source DB
- добавлен regression test, который продвигает source после snapshot publish и
  проверяет, что `latest.json` и archive metadata по-прежнему совпадают именно
  с содержимым `latest.sqlite` / archive sqlite
- повторно подтверждены проверки:
  - `cargo test -p copybot-discovery --bin discovery_recent_raw_snapshot -- --nocapture`
  - `cargo test -p copybot-storage --lib snapshot_into_path_with_policy_returns_retryable_busy_after_bounded_destination_lock -- --nocapture`
  - `bash tools/discovery_restore_drill_smoke_test.sh`

Throughput hardening update (`2026-03-24`):

- follow-up batch **принят**
- snapshot path получил adaptive size-aware pacing:
  - larger `pages_per_step` для large journals
  - reduced pause between backup steps
  - bounded attempt-duration budget вместо many-minute ambiguous run
- large journal attempt теперь должен либо:
  - завершиться `written`
  - либо выйти в явный transient terminal outcome с причиной
    `attempt_duration_budget_exhausted` / retryable contention
- systemd template получил `TimeoutStartSec=3min` как внешний guard поверх
  process-level bounded attempt contract
- повторно подтверждены проверки:
  - `cargo test -p copybot-discovery --bin discovery_recent_raw_snapshot -- --nocapture`
  - `cargo test -p copybot-storage --lib snapshot_into_path_with_policy_returns_retryable_busy_after_bounded_destination_lock -- --nocapture`
  - `cargo test -p copybot-storage --lib snapshot_into_path_with_policy_returns_deferred_after_bounded_attempt_budget -- --nocapture`
  - `bash tools/discovery_restore_drill_smoke_test.sh`

Practical completion update (`2026-03-24`):

- follow-up batch **принят**
- source snapshot теперь pin'ится read transaction на время online backup, так
  что live writes больше не заставляют snapshot chase'ить moving target
- latest surface publication теперь старается использовать hard link от fresh
  archive snapshot к `latest.sqlite`, чтобы не делать еще одну full-size copy
- large-journal path снова должен иметь реальный one-shot completion chance под
  live writes, а не только bounded perpetual `deferred`
- timer path теперь можно осмысленно возвращать в `enabled` для live проверки
- добавлены regression tests:
  - `cargo test -p copybot-storage --lib snapshot_into_path_with_policy_completes_under_concurrent_source_writes_when_snapshot_is_pinned -- --nocapture`
  - `cargo test -p copybot-discovery --bin discovery_recent_raw_snapshot -- --nocapture`
- следующий meaningful шаг уже server-side rerun на live journal после rollout,
  чтобы проверить не correctness, а фактический throughput / bounded completion

### 19.1 Live server rerun after throughput hardening (`2026-03-24`)

Фактический live rerun:

- `discovery_recent_raw_snapshot --scheduled --json` был реально прогнан на
  live journal после rollout
- server-local source size at rerun time:
  - `source_db_bytes = 1308995584`
  - `source_wal_bytes = 480540352`
  - `source_total_bytes = 1789535936`
- terminal outcome:
  - `state = deferred`
  - `latest_surface_status = healthy`
  - `latest_surface_action = deferred_due_to_attempt_budget`
  - `terminal_reason = attempt_duration_budget_exhausted`
  - `snapshot_pages_per_step = 1024`
  - `snapshot_max_attempt_duration_ms = 120000`
  - `attempt_duration_ms = 120005`
  - `backup_step_count = 14000`
  - `backup_copied_page_count = 2048`
  - `backup_total_page_count = 320813`
  - `busy_retry_count = 0`
  - `locked_retry_count = 0`

Что это значит:

- главный server-side sharp edge действительно закрыт:
  - path больше не выглядит как many-minute ambiguous hang
  - live run now exits with an honest bounded transient outcome
- но practical closure еще нет:
  - this rerun did not finish a fresh large-journal snapshot
  - latest healthy surface was retained, not advanced
- честный текущий вывод:
  - snapshot path стал operationally truthful
  - snapshot timer пока все еще нельзя возвращать в `enabled`, если expectation
    — completed fresh snapshot cadence rather than repeated bounded defer

### 19.2 Live server closure of the snapshot branch (`2026-03-24`)

Фактический live rerun после rollout `4148969`:

- `discovery_recent_raw_snapshot --scheduled --json` был повторно прогнан на
  том же live journal
- terminal outcome:
  - `state = written`
  - `latest_surface_status = healthy`
  - `latest_surface_action = refreshed_from_source`
  - `terminal_reason = written`
  - `source_total_bytes = 1917220544`
  - `snapshot_pages_per_step = 1024`
  - `snapshot_max_attempt_duration_ms = 120000`
  - `attempt_duration_ms = 7927`
  - `backup_step_count = 343`
  - `backup_copied_page_count = 350752`
  - `backup_total_page_count = 350752`
  - `snapshot_bytes = 1436680192`
- produced files:
  - `discovery_recent_raw_20260324T200613Z.sqlite`
  - `discovery_recent_raw_20260324T200613Z.json`
  - refreshed `latest.sqlite`
  - refreshed `latest.json`
- `latest.sqlite` was published via hard link from the fresh archive snapshot
  (link count `2`)
- after this rerun:
  - `copybot-discovery-recent-raw-snapshot.timer` was returned to
    `enabled/active`

Что это значит:

- snapshot ветка на проде теперь реально закрыта operationally
- fresh large-journal snapshot cadence на этом сервере больше не theoretical
- snapshot path больше не является active blocker для incident closure
- remaining open problem теперь уже не в snapshot, а в source/path ветке и
  выходе из `bootstrap_degraded`

## 20. Program-scoped gap-fill batch (`2026-03-24`)

Реализован новый production path:

- добавлен отдельный bounded bin:
  - `crates/discovery/src/bin/discovery_raw_gap_fill_program_history.rs`
- source contract:
  - только `QuickNode` / `quicknode_blocks_rpc`
  - тот же program-scoped block-history path, который уже был доказан
    validation-веткой
  - hidden fallback на wallet-history / Helius не добавлялся
- config contract:
  - новый отдельный block `program_history_gap_fill`
  - отдельные knobs для `request_timeout_ms`,
    `max_requests_per_second`, `retry_429_*`,
    `max_slots_to_scan`, `sampling_segments`,
    `max_blocks_to_fetch`,
    `max_candidate_transactions_to_parse`,
    `output_dir`, `output_retention`
- output contract:
  - standalone recent-raw sqlite output
  - latest + archive surface:
    `state/discovery_restore/gap_fill_program_history`
  - tool не пишет в active runtime DB
- terminal semantics:
  - `complete_sufficient_for_healthy_restore`
  - `complete_but_insufficient_for_healthy_restore`
  - `not_proven_due_to_scan_budget`
  - `not_proven_due_to_cost_budget`
  - `not_proven_due_to_provider_throttling`
  - `non_viable_source_contract`
  - incomplete outcomes intentionally withhold replayable rows, so partial
    sampled output cannot fake `healthy/trading_ready`

Regression / operator proof:

- `cargo test -p copybot-config --lib`
- `cargo test -p copybot-discovery --bin discovery_program_history_source_validate`
- `cargo test -p copybot-discovery --bin discovery_raw_gap_fill_program_history`
- `cargo test -p copybot-discovery --bin discovery_runtime_restore`
- `bash tools/discovery_gap_fill_operator_contract_smoke_test.sh`

Synthetic restore proof closed:

- valid program-gap-fill output replayed together with recent journal can produce:
  - `raw_coverage_satisfied = true`
  - `runtime_mode = healthy`
  - `verdict = trading_ready`
- partial / budget-limited program-gap-fill remains non-trading-ready

Статус аудита:

- принят
- новый program-scoped gap-fill path действительно отделен от active runtime DB
- partial / bounded incomplete outcomes не materialize replayable rows
- restore integration и operator contract подтверждены целевыми тестами

### 20.1 Live rollout result (`2026-03-25`)

Серверный rollout нового program-scoped gap-fill path сделан:

- live repo / binaries updated to commit `3ca84e8`
- `discovery_raw_gap_fill_program_history` and `discovery_runtime_restore`
  were rebuilt in `target/release`
- live config now includes a valid `[program_history_gap_fill]` block
- `solana-copy-bot.service` remained `active`
- `copybot-discovery-recent-raw-snapshot.timer` remained `active`

Точный live result:

- `discovery_raw_gap_fill_program_history` was run against:
  - runtime DB:
    `/var/www/solana-copy-bot/state/live_runtime_20260324T134339Z.db`
  - config:
    `/etc/solana-copy-bot/live.server.toml`
- outer bounded runtime budget used for the live run:
  - `timeout 1200`
- the tool did **not** return its own terminal JSON verdict before the outer
  timeout fired
- the process exited only with outer timeout `124`
- no replayable output surface was materialized:
  - `/var/www/solana-copy-bot/state/discovery_restore/gap_fill_program_history`
    was still absent after the run

What this means:

- repo-level production gap-fill path is implemented and deployed
- source-validation uncertainty is already closed
- the remaining blocker is now narrower:
  - not source viability
  - not snapshot durability
  - not restore wiring
  - but practical completion of `discovery_raw_gap_fill_program_history` on the
    real live window

Current honest reading:

- we are not back in the old giant-replay chaos
- but this live gap-fill path is still not operationally closed
- the next engineering step, if we continue, should be only about making this
  specific live run complete with its own bounded terminal contract

### 20.2 Audit sync: resumable attempt-budget contract for live gap-fill (`2026-03-25`)

Статус аудита:

- принят
- `discovery_raw_gap_fill_program_history` now has its own bounded incomplete
  outcome `not_proven_due_to_attempt_budget`
- incomplete runs persist explicit resumable state in
  `in_progress.sqlite` / `in_progress.json`
- replayable output is still withheld until a completed publish step
- targeted tests confirm:
  - terminal JSON without relying on outer shell timeout
  - measurable forward progress across repeated attempts
  - final publish only on completed outcome

### 20.3 Live rerun after resumable rollout (`2026-03-25`)

Server rollout status:

- live repo / binaries were updated to commit `d2a6253`
- `discovery_raw_gap_fill_program_history` and `discovery_runtime_restore`
  were rebuilt in `target/release`
- live services remained stable:
  - `solana-copy-bot.service = active`
  - `copybot-discovery-recent-raw-snapshot.timer = active`

The first direct live rerun immediately exposed an operator-path bug:

- `[program_history_gap_fill].output_dir` in
  `/etc/solana-copy-bot/live.server.toml` was still relative:
  `state/discovery_restore/gap_fill_program_history`
- this tool resolves relative output paths from the config directory
  `/etc/solana-copy-bot`, not from `/var/www/solana-copy-bot`
- the first run therefore failed trying to create:
  `/etc/solana-copy-bot/state/discovery_restore/gap_fill_program_history`
- the exact live error was:
  - `failed creating /etc/solana-copy-bot/state/discovery_restore/gap_fill_program_history`
  - `Permission denied (os error 13)`

That live config bug was then fixed on the server by switching
`program_history_gap_fill.output_dir` to the absolute state path:

- `/var/www/solana-copy-bot/state/discovery_restore/gap_fill_program_history`

After that fix, the new resumable contract became real on the live server.

Observed live rerun with bounded overrides:

- command:
  - `discovery_raw_gap_fill_program_history --max-slot-batches-per-attempt 64 --max-blocks-to-fetch 200 --json`
- attempt `1` returned its own terminal JSON without outer shell timeout:
  - `verdict = not_proven_due_to_cost_budget`
  - `current_phase = awaiting_next_attempt`
  - `attempt_number = 1`
  - `next_batch_start_slot = 407461317`
  - `attempt_scanned_batches = 1`
  - `attempt_scanned_blocks = 200`
  - `staged_rows = 8385`
  - `replayable_output = false`
- attempt `2` returned its own terminal JSON again with cumulative progress:
  - `verdict = not_proven_due_to_cost_budget`
  - `attempt_number = 2`
  - `cumulative_across_attempts = true`
  - `next_batch_start_slot = 407461517`
  - `scanned_batches = 2`
  - `scanned_blocks = 400`
  - `staged_rows = 15573`
  - `replayable_output = false`

What this proves:

- the old “dies only via outer timeout” behavior is gone for this bounded live
  tuning
- resumable `in_progress.sqlite` / `in_progress.json` state is real on the
  server
- repeated attempts make forward progress on the real incident window

What it does **not** prove:

- it does not yet give replayable output
- it does not yet close `raw_coverage_satisfied`
- it does not yet make the path practical for incident closure

Current honest reading:

- the remaining blocker is no longer correctness, validation, or snapshot
  durability
- the remaining blocker is practical throughput of the real
  program-history gap-fill path
- with the current live tuning, progress per long attempt is still far too slow
  for a useful closure cadence

### 20.4 Audit sync: throughput hardening for live program-gap-fill (`2026-03-25`)

Статус аудита:

- принят
- `discovery_raw_gap_fill_program_history` keeps the accepted resumable
  safety contract:
  - bounded terminal JSON on incomplete runs
  - durable `in_progress.sqlite` / `in_progress.json`
  - no replayable output before completed publish
- throughput-focused code changes are now in place:
  - `getBlock` uses `encoding = json` instead of `jsonParsed`
  - swap parse reuses block context instead of rewrapping/cloning transaction
    payloads
  - repeated attempts can reuse resolved slot bounds from progress state instead
    of paying slot-bound resolution again
- new operator-visible telemetry was added for live bottleneck inspection:
  - `resolved_bounds_reused_from_progress`
  - `block_fetch_encoding`
  - `dominant_phase`
  - `resolve_slot_bounds_ms`
  - `attempt_frontier_start_slot`
  - `attempt_frontier_end_slot`
  - `attempt_frontier_advanced_slots`
  - `attempt_block_list_ms`
  - `attempt_block_fetch_ms`
  - `attempt_candidate_filter_ms`
  - `attempt_swap_parse_ms`
  - `attempt_sqlite_stage_ms`

Regression coverage rerun:

- `cargo test -p copybot-discovery --bin discovery_raw_gap_fill_program_history`
- `cargo test -p copybot-discovery --bin discovery_runtime_restore`
- `bash tools/discovery_gap_fill_operator_contract_smoke_test.sh`

Current reading:

- this batch does not by itself prove live practical closure
- but it is a valid and accepted throughput-hardening step
- the next required action is server-side rerun and inspection of the new
  phase/latency telemetry on the real incident window

### 20.5 Audit sync: block-fetch throughput + progress-state compatibility (`2026-03-25`)

Статус аудита:

- принят
- `discovery_raw_gap_fill_program_history` now adds explicit
  `block_fetch_concurrency` control for the live QuickNode path
- resumable / safety semantics remain intact:
  - bounded terminal JSON
  - durable `in_progress.sqlite` / `in_progress.json`
  - no replayable output before full completion
- old progress-state handling is now safer:
  - legacy `in_progress.json` missing newly added telemetry fields can still be
    read via backward-compatible defaults
  - truly unreadable progress state no longer kills the binary with a raw JSON
    parse failure; it is reset with
    `progress_reset_reason = program_history_gap_fill_progress_reset_unreadable_state`

Regression coverage rerun:

- `cargo test -p copybot-config --lib`
- `cargo test -p copybot-discovery --bin discovery_raw_gap_fill_program_history`
- `cargo test -p copybot-discovery --bin discovery_runtime_restore`
- `bash tools/discovery_gap_fill_operator_contract_smoke_test.sh`

Current reading:

- this batch is a valid next hardening step
- it should make the next live rerun more informative in two concrete ways:
  - either `attempt_block_fetch_ms` drops materially under the same bounded run
  - or the live telemetry will prove that the practical frontier is now limited
    by dense-window `max_blocks_to_fetch`, not by avoidable fetch latency

### 20.6 Live provider compare-runs after throughput hardening (`2026-03-25`)

Additional provider tests were run after the resumable / throughput-hardening
path was already in place.

QuickNode remained the live reference point:

- same bounded live contract:
  - `--max-slot-batches-per-attempt 64`
  - `--max-blocks-to-fetch 200`
- observed steady-state server result:
  - `attempt_block_fetch_ms = 18874`
  - `attempt_frontier_advanced_slots = 200`
  - `verdict = not_proven_due_to_cost_budget`
- observed larger bounded run:
  - `--max-blocks-to-fetch 600`
  - `attempt_block_fetch_ms = 52039`
  - `attempt_frontier_advanced_slots = 600`
  - still no replayable output

Alchemy compare-run:

- same bounded `200`-block contract, isolated into a separate output surface
- first attempt:
  - `resolve_slot_bounds_ms = 54557`
  - `attempt_block_fetch_ms = 26784`
  - `attempt_frontier_advanced_slots = 200`
  - `verdict = not_proven_due_to_cost_budget`
- second attempt with reused bounds:
  - `resolve_slot_bounds_ms = 0`
  - `attempt_block_fetch_ms = 30075`
  - `attempt_frontier_advanced_slots = 200`
  - `verdict = not_proven_due_to_cost_budget`
- reading:
  - bounded `Alchemy` runs looked more stable than the long `QuickNode` runs
  - but they were slower on the actual bounded contract and did not improve
    frontier advance

ANKR compare-run:

- failed immediately with:
  - `verdict = non_viable_source_contract`
  - `Block 407480552 cleaned up, does not exist on node. First available block: 408632723`
- reading:
  - the tested endpoint is not archive-grade for the missing incident window
  - it cannot be used for this restore path

Infura compare-run:

- same bounded `200`-block contract
- no terminal JSON returned even after more than two minutes
- no progress/output files were materialized
- reading:
  - `Infura` is not a practical source on the current
    `program_history_gap_fill` contract

Honest provider conclusion after these compare-runs:

- `QuickNode` remains the fastest provider actually observed on this exact tool
  contract
- `QuickNode` is also the one that still intermittently fails the historical
  path with `503`
- `Alchemy` is slower
- `ANKR` does not provide the required archive depth
- `Infura` did not produce a usable bounded run
- none of the tested providers produced a practical timely recovery source on
  the full incident window

### 20.7 Local Mac Studio compare-run (`2026-03-25`)

The “maybe the production host is too weak” hypothesis was tested directly.

What was done:

- a local run was executed on a `Mac Studio M3 Ultra / 96 GB`
- the exact same QuickNode HTTP endpoint was used
- the same live incident window was used
- the current server progress state was copied locally
- a minimal local runtime DB was created with the same
  `discovery_recent_raw_restore_state`, so the comparison isolated provider /
  fetch cost from the production host itself

Observed local bounded results:

- first local run returned after the local copied progress state was reset as
  incompatible because of path mismatch in the copied output metadata
  - `attempt_block_fetch_ms = 20787`
  - `attempt_frontier_advanced_slots = 200`
- second local steady-state run:
  - `resolved_bounds_reused_from_progress = true`
  - `attempt_block_fetch_ms = 24618`
  - `attempt_frontier_advanced_slots = 200`

Compared with the steady-state live server result:

- live server:
  - `attempt_block_fetch_ms = 18874`
  - `attempt_frontier_advanced_slots = 200`
- local Mac Studio:
  - `attempt_block_fetch_ms = 24618`
  - `attempt_frontier_advanced_slots = 200`

Reading:

- the hypothesis that the production host is the primary bottleneck is not
  supported
- the dominant constraint remains provider-side historical `getBlock`
  throughput / reliability, not local CPU or RAM
- moving the same QuickNode path onto a much stronger machine did not produce
  the expected breakthrough

### 20.8 Decision: stop active provider / gap-fill experiments for now (`2026-03-25`)

Current decision:

- stop active provider-swap / gap-fill experiment work for now
- do not invest more calendar time into this branch unless a truly different
  archive-grade source contract appears
- keep the already-built tooling and documentation; do not delete it
- keep the production server in the current safe bridge posture:
  - `runtime_mode = bootstrap_degraded`
  - `execution.enabled = false`
  - `copybot-discovery-recent-raw-snapshot.timer = active`

Why this decision is justified:

- snapshot durability is already operationally closed
- restore tooling is already operationally closed
- source viability research is already closed
- resumable gap-fill research is already closed
- provider compare-runs and local host compare-runs did not uncover a practical
  timely recovery source
- the remaining blocker is no longer lack of engineering structure; it is the
  practical cost / reliability of provider-side historical block fetch for the
  missing incident window

What stays true after freezing this branch:

- the incident is not yet closed in the `healthy / trading_ready` sense
- live accumulation can continue in the background
- the team no longer needs to freeze product development behind this recovery
  branch
- future engineering focus should return to the main roadmap captured in
  `ROAD_TO_PRODUCTION_v2.md`

### 20.9 Live server stability / recent-raw write verification (`2026-03-25`)

After the provider / gap-fill branch was frozen, one additional direct
server-health verification was run to answer the practical question:

> “Is the server actually still writing recent raw, or can we wake up in three
> days and discover that nothing was being persisted?”

What was checked:

- explicit server-side build without stopping the live app:
  - `~/.cargo/bin/cargo build --release -p copybot-discovery --bin discovery_raw_gap_fill_program_history --bin discovery_status`
- live unit state during the same check:
  - `solana-copy-bot.service = active`
  - `copybot-discovery-recent-raw-snapshot.timer = active`
  - `copybot-discovery-runtime-export.timer = active`
- live ingestion / writer logs from `journalctl -u solana-copy-bot.service`
- direct SQLite reads from the live recent-raw journal
- the latest timer-triggered `discovery_recent_raw_snapshot` run

Observed facts:

- build succeeded on the server without stopping the bot
- live log slice still showed active ingestion:
  - `grpc_message_total` increasing
  - `grpc_transaction_updates_total` increasing
  - `swaps_seen` increasing
  - sampled live telemetry reported `rpc_429 = 0` and `rpc_5xx = 0`
- direct SQLite verification on
  `/var/www/solana-copy-bot/state/discovery_recent_raw.db` confirmed
  real recent-raw forward progress:
  - at `2026-03-25T13:44:28Z` the latest `observed_swaps` row was:
    - `rowid = 8270426`
    - `ts = 2026-03-25T13:43:40.433157287+00:00`
    - `slot = 408776640`
  - at `2026-03-25T13:45:38Z` the latest `observed_swaps` row was:
    - `rowid = 8276618`
    - `ts = 2026-03-25T13:44:57.514693140+00:00`
    - `slot = 408776834`
  - observed delta over `70s`:
    - `+6192` rows
    - slot frontier moved forward
    - latest raw timestamp moved forward
- latest timer-triggered snapshot service was also healthy:
  - `copybot-discovery-recent-raw-snapshot.service` completed
    `status=0/SUCCESS` at `2026-03-25 13:42:54 UTC`
  - emitted snapshot metadata included:
    - `row_count = 8233319`
    - `covered_through_cursor.ts_utc = 2026-03-25T13:36:30.059096747Z`
    - `covered_through_cursor.slot = 408775557`

Reading:

- the live server is currently stable
- the recent-raw journal is currently still being populated
- the recent-raw snapshot timer is currently still executing successfully
- this is a sampled real-time health confirmation, not a promise that no later
  operational incident can happen

Current branch status after this verification:

- active provider / gap-fill experiments remain paused
- the live server stays in the safe bridge posture:
  - `runtime_mode = bootstrap_degraded`
  - `execution.enabled = false`
- the restore / snapshot branch is no longer the active coding focus
- the primary development roadmap is again `ROAD_TO_PRODUCTION_v2.md`
