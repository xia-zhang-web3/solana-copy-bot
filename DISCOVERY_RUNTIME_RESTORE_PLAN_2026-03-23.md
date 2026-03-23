# DISCOVERY RUNTIME RESTORE PLAN

Date: 2026-03-23
Status: Canonical

## 0. Суть

Цель этого плана одна:

- больше никогда не дропать runtime DB и не собирать все заново из нуля

Главный принцип:

- runtime DB должна стать disposable
- source of truth для восстановления должен быть вне нее

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

Нужно честно признать:

- если prebuilt `recent raw journal` не существовал до инцидента,
  one-button healthy restore для этого первого инцидента еще невозможен
- тогда первый подъем до healthy неизбежно имеет one-time bootstrap cost:
  либо recent raw backfill, либо live ingestion до достаточного fresh raw window
- это допустимо только как разовый переходный шаг
- после внедрения `recent raw journal` такой сценарий повторяться уже не должен

Действия:

1. Старую DB убрать из active runtime path.
2. Не брать из нее кошельки.
3. Поднять fresh runtime DB.
4. `execution.enabled = false`.
5. Наполнить новую DB только свежими `observed_swaps`.
6. Ждать только fresh runtime truth.

Источник для пункта 5 только такой:

1. либо recent raw backfill за актуальный lookback
2. либо live ingestion с накоплением свежего raw window

Чего делать нельзя:

1. не bootstrap старых кошельков для торговли
2. не считать clone от 2026-03-09 свежим truth source
3. не пытаться “долечить” старую DB как production runtime

## 6. P1: что кодить в ближайшие 2 дня

### Task 1. Runtime artifact export/import

Добавить:

- [`crates/discovery/src/bin/discovery_runtime_export.rs`](/Users/blacktower/Documents/solana-copy-bot/crates/discovery/src/bin/discovery_runtime_export.rs)
- [`crates/discovery/src/bin/discovery_runtime_restore.rs`](/Users/blacktower/Documents/solana-copy-bot/crates/discovery/src/bin/discovery_runtime_restore.rs)

Поддержать через:

- [`crates/storage/src/discovery.rs`](/Users/blacktower/Documents/solana-copy-bot/crates/storage/src/discovery.rs)

Обязательный контракт Task 1:

1. export должен писать `exported_at`
2. restore должен валидировать freshness артефакта против текущего runtime gate
3. normal restore не должен переписывать `last_published_at` на `now`
4. если артефакт stale:
   - normal restore должен отказать как trading-ready path
   - допускается только явный `--bootstrap-degraded` режим
5. `--bootstrap-degraded`:
   - не маркирует stale artifact как recent truth
   - не должен открывать торговлю
   - должен существовать только как incident tool, а не нормальный steady-state path

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

Если позже появится лучший storage, его можно заменить.
Но V1 контракт должен быть конкретным уже сейчас.

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
- active follow wallets не пусты или runtime честно в bounded degraded
- aggregate readiness не участвует в verdict

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
