# DISCOVERY RUNTIME RESTORE PLAN

Date: 2026-03-23
Status: Canonical, Batch 1-3 implemented; live server is currently in bootstrap-degraded bridge mode

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

- код Batch 1-3 раскатан на live server в коммите `40fff47`
- старый giant replay path больше не является активным runtime path
- `solana-copy-bot.service` снова запущен и live ingestion снова идет
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
- server-side fresh-DB restore drill уже прошел на этих поверхностях:
  - `journal_available = true`
  - `journal_replayed = true`
  - `journal_covers_artifact_cursor = true`
  - `replayed_rows = 61887`
  - `raw_coverage_satisfied = false`
  - final verdict = `bootstrap_degraded`

Что это значит честно:

- сервер больше не мертв и не сидит на старом giant replay path
- restore chain уже существует и реально исполним на проде
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

Отдельно важно зафиксировать текущий live sharp edge:

- `copybot-discovery-runtime-export.timer` можно держать включенным
- `copybot-discovery-recent-raw-snapshot.timer` на этом сервере сейчас
  выключен
- причина не в giant replay и не в stale artifact, а в том, что snapshot path
  под реальными live writes оказался пока неоперационным
- значит operational contract на проде сейчас асимметричен:
  - artifact cadence уже автоматизирован
  - recent raw snapshot cadence пока требует дополнительного hardening

## 15. Что код должен сделать дальше, чтобы реально закрыть инцидент

Ниже не новый большой “план восстановления всего мира”, а узкий список того,
что действительно нужно, чтобы закрыть оставшийся gap.

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
2. этот gap-fill должен наполнять `recent raw journal` или fresh restore target,
   а не возвращать giant replay в boot path
3. gap-fill должен быть ограничен runtime horizon, а не всей историей
4. после gap-fill должен существовать явный operator flow:
   - fresh DB
   - artifact restore
   - recent raw journal replay
   - gap-fill apply if needed
   - final verdict

Иными словами, следующий код должен не “улучшать bootstrap-degraded”, а
довести path до честного выхода в `healthy` без многодневной пассивной надежды.

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
