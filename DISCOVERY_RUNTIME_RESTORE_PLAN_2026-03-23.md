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

## 3. Что такое recent raw journal

Это отдельное durable хранилище свежих `observed_swaps`, независимое от
runtime DB.

Нужно хранить:

- bounded recent raw swaps для текущего runtime horizon
- достаточно данных, чтобы после restore не ждать многодневный replay

Без этого журнала:

- restore почти всегда придет только в `degraded`
- если fresh rebuild снова не сойдется, инцидент не закрыт

## 4. Жесткие правила

1. Старые кошельки нельзя использовать как runtime truth.
2. Старую DB нельзя использовать как торговый source of truth после потери freshness.
3. Aggregate/backfill нельзя пускать в boot path.
4. Если restore требует giant replay истории, это не restore, а провал архитектуры.
5. Если runtime DB сломалась, ее убирают в архив и поднимают новую.

## 5. P0: что делать сейчас

Считать, что текущей runtime truth у тебя нет.

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

### Task 2. Recent raw journal

Добавить отдельный durable путь для recent `observed_swaps`.

Минимально допустимый вариант:

- export/import bounded recent `observed_swaps`

Лучший вариант:

- отдельный sidecar/secondary store для recent raw journal

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

## 7. P2: что должно появиться после этого

1. Регулярный export runtime artifact по расписанию.
2. Регулярный export recent raw journal по расписанию.
3. Restore drill на чистую DB.
4. Документированный RTO/RPO.

Минимальный target:

- RTO: минуты, не дни
- RPO: ограничен интервалом artifact export

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
