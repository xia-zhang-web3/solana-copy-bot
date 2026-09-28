# Первый отбор: сохранённый INCONCLUSIVE

Этот отчёт сохраняет результат collector01. Новое отдельно разрешённое
HTTP version1 окно завершено; актуальное решение и классификация находятся в
[collector02 report](SOURCE_SELECTION_V1_READ_RESULT_RU.md). Collector01,
его ledger и deadline не сбрасывались; draft continuation не запускался.

Дата: 2026-09-28. Решение: есть ли хотя бы один подтверждённый свежий источник
с classic SPL BUY/SELL, проходящий нынешний декодер, для следующего technical
cohort? Возможные исходы: остановленный принятый пакет с источником либо
NO_COMPATIBLE_SOURCE/INCONCLUSIVE с причиной. **Исход этого окна INCONCLUSIVE.**

## Что выполнено

- Прочитан переданный `SOURCE_SELECTION_NEXT_PROMPT_RU.md`. Разрешённый предел
  HTTP-only отбора: 512 attempts / 20480 CU / 10752000 nanoUSD / 900 секунд;
  stream, signer, daemon и торговля запрещены. Зафиксированы старые 626 attempts /
  9830 CU / 5160750 nanoUSD и stream 59581108989 bytes / 62 connections.
- В отдельном приватном `source-selection-20260928-01` подготовлены reader,
  typed selection, независимые raw transfer checks и replay. Каждый HTTP
  запрос резервируется в ledger до отправки. Нет автоматических retries;
  allowed methods строго read-only, код и прежние израсходованные пакеты не смешаны.
- CPMM исправлен в будущей Run15 конфигурации на
  `CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C`, проверенный по
  [официальному Raydium SDK](https://raw.githubusercontent.com/raydium-io/raydium-sdk-V2/master/src/common/programId.ts).
  Правильные AMM v4 и PumpSwap, classic-only и финансовые guards сохранены.
- Test-only raw RPC adapter вызывает текущий `decode_yellowstone_swap_facts`.
  Реальные header, static/loaded addresses, err, balances, instruction bytes и
  inner stackHeight переносятся без подстановки успешного err или глубины2.
  Отсутствующий version — явный отказ; разрешены только legacy/v0.
- Установлен прежний matching GitHub Actions artifact `da3c7300…` с 88
  проверенными миграциями в новый Run15 scaffold; rollback `b1cf0816…` сохранён.
  Это доказательство идентичности и прежней приёмки, не новая совместимость
  daemon с v1 или GREEN следующего live stream.

## Реальное выполнение и причина остановки

Первый outbound: **2026-09-28 11:17:55 UTC**. Выполнение закончилось через
**16,002 секунды**, без четырёхчасового сеанса и без stream. Успешный finalized
`getSlot` вернул head **451303264**. Все следующие **64 getBlock** для слотов
451303264…451303201, `encoding=json`, `transactionDetails=full`,
`rewards=false`, `maxSupportedTransactionVersion=0`, вернули HTTP200 и
JSON-RPC error **−32015**. Нет ни одного полученного блока, parent-chain proof,
кандидата, history coverage или replay нового source sample.

Поэтому нельзя утверждать, что рынок пуст, источник отсутствует или исправление
CPMM уже проверено настоящей CPMM-транзакцией. Реальный old-ID refusal/new-ID
decode остаётся непроверенным; имеющийся локальный CPMM control синтетический
и проверяет только interest filter.

Код −32015 означает unsupported transaction version согласно
[Solana client constant](https://docs.rs/solana-client/latest/solana_client/rpc_custom_error/constant.JSON_RPC_SERVER_ERROR_UNSUPPORTED_TRANSACTION_VERSION.html).
[Официальное обновление Solana](https://solana.com/upgrades/larger-transaction-sizes)
указывает activation v1 15 сентября, необходимость числового max version1
для чтения и отказ всего getBlock при хотя бы одной v1 транзакции. Это объяснение
совместимо со всеми 64 отказами, но **вывод о конкретной v1 в каждом из этих
блоков является inference**: collector сохранил код/HTTP status/размер/params,
а исходное текстовое сообщение RPC не сохранил. Номер требуемой версии из
этих ответов восстановить нельзя. Этот диагностический пробел признан.

Проверена конкретная граница текущего кода: RPC source/backfill запрашивает
version0; pinned Yellowstone proto12.0.0 не содержит Message.config.
Официальная документация называет proto12.6.0 первой версией с этим полем.
Простое повышение HTTP параметра не доказывает совместимость исполнения,
учёта комиссий или следующего stream. Production runtime/dependencies в этом
batch не изменялись; широкая миграция v1 не выполнялась.

## Учёт и состояние

Ledger независимо проверен: 1 getSlot + 64 getBlock = **65 attempts / 2580 CU /
1354500 nanoUSD ($0,0013545)**. Cumulative: **691 attempts / 12410 CU /
6515250 nanoUSD ($0,00651525)** HTTP/RPC. Исторические rows сохранены;
stream59581108989bytes/62connections не вырос. Это модель расходов, не invoice.
Неиспользованный резерв до первоначального потолка: 447 attempts / 17900 CU /
9397500 nanoUSD; цену дополнительной попытки ограничивает оставшийся budget.
Исходный deadline **11:32:55 UTC** не сбрасывается автоматически.

Run15: STOP, `NOT_READY_PENDING_SOURCES`, пустой state, источников0; финансовый
ledger, authority, lease, clocks, source binding, ready seal и пять runtime
контейнеров отсутствуют. Плата поиска остаётся в отдельном cumulative ledger
и должна переноситься при дальнейшей подготовке. Активационная команда не
готова; финансовый preflight откажет до создания acceptance seal. Used Run14
и прежний source probe не изменялись.

## Проверки и независимое решение

- Reader/selection **12/12 PASS**, включая write-ahead до фактического send
  boundary, ceiling512, deadline, error/oversize, all-null INCONCLUSIVE,
  частично недоступный sample и stale head. Независимо воспроизведены также
  cached slot mismatch и history blockTime mismatch: источник не выбирается.
- Adapter **5/5 controls PASS**. Version1 synthetic record отдельно явно
  отказан; это не replay настоящей v1 транзакции. Build target:
  `copybot-ingestion --lib`, test/debug, общий Cargo cache. Warm build2,56с;
  app/release/workspace не собирались. Новых зависимостей0.
- Новый pending-package/carryover **5/5 PASS**; matching artifact, rollback,
  semantic config delta и pending preflight проверены. Installer отказы из-за
  noexec tmpfs сохранены; успешная установка использует существующий tooling
  container network=none, без signer/provider/state mounts.
- Независимый reviewer проверил новые collector/decoder границы, фактические
  65 ответы и ledger, Run15 STOP/пустоту/отсутствие activation и binary binding.
  **INCONCLUSIVE принят; Run15 не активировать.** Тесты не заменяют новые source facts.

## Единственное следующее решение

Подготовлен `continue_read_v1.py`: только HTTP max version1 для mixed block,
с явным исключением v1 из кандидатов нынешнего decoder. Требует отдельного
scope-bound amendment; сохраняет прежние counters и первоначальный deadline,
не создаёт новый clock и не включает торговлю. Сохраняет безопасный RPC reason.
Узкий независимый review draft PASS; causal clock check подтверждает, что
время локальной подготовки расходует исходное окно. Разрешение ещё не получено.
Без amendment или после deadline отказывает до provider вызова. После
исчерпания исходного окна нужен отдельно определённый новый read-only scope;
самовольно повторять used collector или расширять версии исполнения нельзя.

Даже подтверждение legacy/v0 source sample не снимает найденную неопределённость
нового формата live stream. Перед ready-пакетом требуется конкретное решение
об обработке version1 в runtime и причинная проверка этой границы; при изменении
deployable code нужен новый matching artifact. Прибыльность и автоматическое
копирование по-прежнему не доказаны.

Файлы: два новых test-only Rust модуля252/292строки и +4 test wiring;
private reader183, facts113, replay51, selection248строк; новые зависимости0,
waivers0. Подробнее: [RESULT.json](/Users/tigranambarcumyan/.codex/private/native-handoff-local-experiment-01/source-selection-20260928-01/RESULT.json),
[offline scaffold evidence](/Users/tigranambarcumyan/.codex/private/native-handoff-local-experiment-01/technical-cohort-15/evidence/OFFLINE_PREPARATION.json).
