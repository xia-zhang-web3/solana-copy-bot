# HTTP version1: найден один совместимый источник; Run15 остаётся STOP

Дата: 2026-09-28. Вопрос решения: позволяет ли разрешённое HTTP-чтение
с maxSupportedTransactionVersion=1 найти свежие classic SPL BUY и SELL,
совместимые с нынешним декодером? Исход: **SOURCE_SAMPLE_CONFIRMED**, один
кошелёк. Это приёмка HTTP source facts; готовность live daemon не заявляется.

## Объём разрешения и фактический сбор

Пользователь разрешил новое окно900с от первого нового запроса в оставшемся
бюджете447 attempts /17900CU /9397500nanoUSD, включая максимум64 новых
getBlock. Stream, daemon, signer и торговля запрещены; Run15 оставить STOP.
Старый collector01, его отказ −32015, ledger и clocks сохранены без сброса.

Новый collector02 начал outbound **12:01:41.436571 UTC**, deadline
**12:16:41.436571 UTC**; завершил **12:08:00.333748 UTC**, через378.897с.
Выполнены88 запросов: getSlot1, getBlock64, getSignaturesForAddress4,
getTransaction17, getMultipleAccounts2. Все ответы успешны, RPC-отказов0.
Причина завершения: достигнут разрешённый максимум64 новых getBlock.

Получены64 finalized блока451313088…451313025 с непрерывными parent-связями,
71803 транзакции. Их blockTime охватывает **12:01:15…12:01:32 UTC, 17 секунд
рынка**. 6мин19с — время HTTP-сбора и локального разбора, не длительность
непрерывного наблюдения рынка. Для кандидатов отдельно проверена возвращённая
история100 signatures и до32 успешных транзакций за час; HTTP sample не
доказывает транспорт, задержку, полную историю или непрерывный live stream.

## Классификация источников

| Кошелёк | Факт и решение |
| --- | --- |
| `7EoQc9N9QrGMf6JR2j8yy4rmY1ZsexPuAXoTCQtDDbSF` | Подтверждены разные classic SPL BUY и SELL одного mint через Raydium AMM v4; оба legacy, exact amounts, err=null. Пригоден по проверенному HTTP sample. |
| `96tmCcZX83ppV8p3stTuiFcKwyPYy4fXmsdD7Q2y8JR4` | Есть совместимый BUY; в одной успешной свежей транзакции из history100 парный SELL не подтверждён. Данных недостаточно для выбора. |
| `CUkKyTN2GnkLKSucTF24qjhFn6jgSDAUQHF3RzLKhsZf` | Есть совместимый BUY; в16 рассмотренных успешных транзакциях парный BUY/SELL не подтверждён. Данных недостаточно. |
| `AXA2k9FKgTyUaaQSAvB7qVHUsnRJJST2rsmF6EFDZBHZ` | Есть совместимый BUY; в4 рассмотренных успешных транзакциях пары нет. Данных недостаточно. |

Прежние три закреплённых источника не выбираются: ранее проверенные swaps
используют Token2022, возвращённая активность устарела. Их probe не повторялся.
Это решение по имеющимся ответам, не доказательство постоянной неактивности.

Выбранный mint: `DVb1znJKBVJzcuzbgvcG3cSghf2i1YzdJqoJFb7ZdQuX`.
Отдельный getMultipleAccounts719 доказывает classic SPL owner,
неисполняемый initialized mint82bytes, decimals9, context_slot451313967.
Свежая SELL signature присутствует в finalized history726 с тем же slot/time.

| Сторона | UTC / slot | Обмен SOL lamports | Обмен target raw, decimals9 |
| --- | --- | ---: | ---: |
| BUY | 11:15:10 /451302689 | 6120191 | 163956354 |
| SELL | 12:01:24 /451313058 | 9832310 | 264731434 |

BUY signature: `4gubodBAMxTWiTt79JMXRyKMfSAMzfh7EWwKsGkEa4u4y67d9qMwDmLEUVWTdRgh4YhTwvq9kqGYAuoAAf1SjosT`.
SELL signature: `5qUFxPr8EZusxeKyDUWeDvRtkfJtjiYNyNqAQBxC442SvDFQsG6cebbrZvkf9aeTmiMgtBULYGBsagaXyDmi6oD4`.

На фиксации12:05:57.965312 UTC последняя SELL имела возраст273.965с;
BUY→SELL интервал2774с, обе сделки в пределах последнего часа.
Доказан signer/ownership, направление, raw balances и classic CPI transfers.
Обмен соответствует owned WSOL: BUY47656411→41536220, SELL41536220→51368530.
Native wallet в каждой сделке уменьшился только на fee75000lamports;
его изменение не подменяет сумму обмена. Количество SELL больше данного BUY:
**полный отдельный цикл и его P/L не доказаны**, complete_round_trip_proven=false.
Свежесть подтверждена на момент сбора, будущая активность не гарантирована.

## Фильтры, версия и реальные отказы

HTTP читает mixed blocks с max version1; candidate policy по-прежнему
допускает только legacy/v0. До первого outbound добавлен отказ при наличии
`message.transactionConfig`, независимо от version и значения поля.
Отрицательный контроль проверяет legacy/0/1 и config={}/null, без facts.
В настоящих блоках8476 version1/config transactions явно отказаны adapter.

3078 swaps распознаны нынешним decoder: только7 имеют одновременно classic
target rows и exact amounts. Суммы не выводились из округлённого UI balance:
отсутствие exact amounts остаётся запретом нынешнего cohort BUY/source SELL.
Token2022, незаинтересованные программы, Failed, Vote и attribution misses
сохранены отдельно; отсутствие подходящей пары в sample не означает пустой рынок.

**52 реальные CPMM транзакции** декодируются с официальным адресом
`CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C`, но получают
UninterestedProgram со старым неверным адресом. Это причинная проверка
конфигурации на настоящих ответах, не гарантия исполнимости каждой сделки.

## Расходы, сохранность и Run15

Новые расходы: **88 attempts /3460CU /1816500nanoUSD ($0.0018165)**.
Cumulative HTTP/RPC: **779 attempts /15870CU /8331750nanoUSD ($0.00833175)**.
Все691 прежних ledger rows перенесены точно; quick_check=ok, totals равны сумме
rows. Остаток данного разрешения359 attempts /14440CU /7581000nanoUSD;
новых getBlock осталось0. Неиспользованный бюджет не означает новое разрешение
после deadline или право повторить использованный collector.
Stream59581108989bytes /62connections и общий provider ceiling$50 сохранены.
Все суммы — принятая модель, не invoice провайдера.

Run15: STOP, state пустой, финансовые authority/lease/clocks/ledger/seal и
runtime-контейнеры отсутствуют. Выбранный source сохранён как evidence,
runtime wallet list остаётся пустым до отдельной финализации. Старые расходы
должны переноситься из cumulative ledger02; accepted installation не менялась.
Финансовые пределы не изменены: до4ч, BUY≤0.01SOL×1, source SELL×1,
priority≤50000lamports, slippage≤50bps, loss cap0.02SOL, native floor≥160200031.

## Проверки и ограничение запуска

Reader/selection15/15 PASS; до запросов transactionConfig control1/1 PASS
и отдельное независимое повторение1/1. Излишнее копирование полного блока
для каждой transaction исправлено во время сбора; проверка сохранения
slot/time/loaded keys1/1 PASS. Выбранная пара независимо повторно replayed
финальным binary и raw ownership/amounts проверены без provider-вызовов.
Независимый review **PASS для HTTP source sample**; проверки metadata/loaded
keys2/2 PASS. Отдельная ошибочная гипотеза reviewer о номере Raydium opcode
сохранена и исправлена по настоящим bytes/CPI. Readiness Run15 не принято.

Точные targets/profile: `copybot-ingestion --lib`, test/debug, shared Cargo
cache; warm builds8.64с и4.34с. App/release/workspace build **NONE**; production
code/dependencies не менялись. Новые test modules264/312 строк,
test wiring+4; private reader214, facts115, replay51, selection247 строк.
Новых зависимостей0, waivers0. Publication и rollout в этом HTTP batch не нужны.

Сняты HTTP version-read blocker и отсутствие хотя бы одного подтверждённого
source sample. **Остаётся граница live stream:** pinned Yellowstone proto12.0.0
не сохраняет Message.config; безопасная реакция нынешнего runtime на реальные
version1 blocks не доказана. HTTP adapter её не исправляет. Run15 не запускать.
Следующий ограниченный шаг — причинно проверить и при необходимости исправить
runtime обработку неподдерживаемой версии на сохранённом corpus, затем решить
matching artifact/finalization. Новое платное окно для этого не требуется.
Автоматическое копирование и прибыльность остаются непроверенными.

Evidence: [RESULT.json](/Users/tigranambarcumyan/.codex/private/native-handoff-local-experiment-01/source-selection-20260928-02/RESULT.json),
[independent raw source review](/Users/tigranambarcumyan/.codex/private/native-handoff-local-experiment-01/source-selection-20260928-02/INDEPENDENT_SOURCE_REVIEW.json),
[replay/CPMM addendum](/Users/tigranambarcumyan/.codex/private/native-handoff-local-experiment-01/source-selection-20260928-02/INDEPENDENT_REPLAY_SCOPE_ADDENDUM.json),
[Run15 STOP and carryover reference](/Users/tigranambarcumyan/.codex/private/native-handoff-local-experiment-01/technical-cohort-15/evidence/READONLY_SELECTION_02.json).
Предыдущий отказ сохранён в [collector01 report](SOURCE_SELECTION_RESULT_RU.md).
