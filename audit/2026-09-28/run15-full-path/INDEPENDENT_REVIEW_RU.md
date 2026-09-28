# Run15: независимая проверка изменённых денежных путей

Дата: 2026-09-28. Reviewer: отдельный агент `run15_versions`.
Исходная база: `53203c3ddf242c55e713fc2d5471ab8f90dd916a`.

## Решение и граница

Вопрос проверки: содержит ли изменённый Raydium/Jupiter/inventory/recovery путь
конкретный дефект, препятствующий выпуску matching app artifact для остановленного
Run15? Возможные решения: ACCEPT_SCOPED либо HOLD с воспроизводимой причиной.
Условие завершения: критические diff прочитаны, новые causal controls независимо
повторены, реальные данные отделены от моделей, неподтверждённый rollout не принят.

**ACCEPT_SCOPED: изменённые денежные пути и полный offline causal сценарий
приняты в указанном объёме. Конкретного оставшегося code blocker не найдено.**
Следующее разрешённое действие: matching `copybot-app/release` artifact из GitHub
Actions, установка в Run15 под STOP и финальный локальный preflight. Это ещё
не приёмка установленного пакета, финансовая активация или доказательство копии.

Собственные изменения reviewer в version/config ingestion проверяет другой
рецензент. Здесь они не получают self-approval. Полный сценарий независимо
проверяет их совместное поведение на mixed input.

## Критические semantics

- Storage inventory требует полного parent snapshot обоих token programs,
  точного context/hash и raw balances. Prefix legacy/v0/v1 учитывается только
  при достаточной metadata/ownership; неизвестный эффект отказывает. Config/v1
  financial target не разрешён. Source AMM v4 proof связывает подпись, кошелёк,
  mint, ABI, classic token custody и exact effects. Никаких ручных финансовых
  исправлений DB или выдуманного whole-wallet denominator не найдено.
- Выбор доли использует exact `u128` floor с подтверждённым follower quantity.
  Частичный SELL сохраняет остаток и его обязательства; он не становится CLOSED.
- Finalized source SELL и follower BUY proof связаны с signature/slot/payer,
  classic token owners, vaults, amounts и custody CPI. Неправильные owner,
  program, amount, version и arbitrary route отказывают.
- Cohort signing/build gate допускает только ограниченный Jupiter→Raydium BUY
  с требуемыми wallet/mint/ATA, BUY≤10m lamports и slippage≤50bps. Decoder
  источника не переносит эти bot caps на размер лидерской сделки. Native floor,
  protected capital и durable submit gates не ослаблены.
- Новый exact attribution wrapped BUY требует полного WSOL lifecycle witness,
  точных CPI/balances и canonical route/event authority. Fresh target требует
  ATA derivation и полного classic creation/ownership/rent witness. Прежний
  Pump path остаётся на своём pre-existing target profile. Missing lifecycle
  не превращается в cash-minus-fees inference.
- RPC jsonParsed projection находится только в test modules. Она связывает
  model keys с decoded wire, сохраняет balances/custody/raw facts и переводит
  известные classic lifecycle инструкции. Production receipt parser не изменён.
  Неполная старая projection воспроизводит `receipt_token_creation_unproven`;
  missing init/wrong owner остаются Unresolved.
- UNKNOWN recovery не создаёт send2 и не закрывает/уменьшает позицию до receipt.
  После restart выполняется reconciliation. Receipt затем приводит к реальному
  accounting path; исходная история и ownership остаются сохранёнными.

## Независимые проверки

Reviewer ничего не компилировал: **build NONE**, использованы существующие
совместимые test/debug binaries общего cache. Workspace suite не запускался.

| Проверка | Результат |
| --- | --- |
| Storage metadata boundaries + actual saved mixed corpus | 5/5 PASS |
| Finalized source/BUY wire/anchor controls, включая fresh target | 5/5 PASS |
| Jupiter/Raydium exact attribution и отрицательные lifecycle/ownership/route controls | 4/4 PASS |
| Actual receipt parser: raw/missing/wrong lifecycle + correct projection | 1/1 PASS |
| Source finalization/carryover helpers | 5/5 PASS |
| Architecture guard external module разрешён, inline module запрещён | 2/2 PASS |
| Полный daemon scenario, ordinary + UNKNOWN/restart в одном тесте | 1/1 PASS, 92.20s |

Полный сценарий независимо повторён в отдельной temporary DB через фактический
`ExecutionCanaryRunner::process_tick`, AssociationConsumer/replay, source proof,
inventory, quote/build/simulation/send/receipt/accounting. До bot anchor доходят
сохранённый source BUY и затем фактический source SELL block. Admission вручную
не добавляется и не исправляется.

Binary: `copybot_app-bc705dec22348b08`, SHA256
`7c3a1b9daf73d49d4e8707fd0dd95e239a7a6d8559bd6e109739f98e9fbac0e0`.
`RUST_MIN_STACK=8388608`; test/debug со scoped opt-level2 только для
storage-core/core-types/serde_json/rusqlite. Это устраняет unoptimized test graph
overhead при неизменяемом пятисекундном production preparation deadline.
Production profiles и deadline не изменены; этот binary не deployable artifact.

Отдельный evidence directory:
`/Users/tigranambarcumyan/.codex/private/native-handoff-local-experiment-01/run15-full-path-prep/independent-causal-review-01`.

| Ветка | BUY sends | SELL sends после restart | Config misses | V1 Admissions | Tail parents |
| --- | ---: | ---: | ---: | ---: | ---: |
| confirmed partial | 1 | 1 | 143 | 0 | 180 |
| UNKNOWN → restart → receipt | 1 | 1 | 143 | 0 | 227 |

Обе SQLite DB: `quick_check=ok`, ровно3 durable Admission identities, один SELL
handoff, один dispatch, одна fractional decision. Simulation и send независимо
ожидали новый **persisted parent commit после durable handoff**; утверждение
проверено для обоих RPC. Поток продолжался во время исполнения SELL.

Перед появлением UNKNOWN receipt нет cash settlement, полная model quantity
остаётся открытой; completed background recovery наблюдается после restart.
После receipt продано10475397raw, сохранено257418781raw, повторной отправки нет.

Evidence SHA256:

- `full-path-unknown-false.json`:
  `6613335dc71ce882e40f9325eb351271ba05eb0bc1297abffe91be91c3789eea`.
- `full-path-unknown-true.json`:
  `010fe897bce7083eed74804af125170a5c3194cbd5a14930640616b7df99efeb`.

## Реальные факты и модель

Реальны сохранённые BUY response727 и mixed SELL block response725:
source slots451302689/451313058, SELL index775, raw amounts/signatures/metadata.
В SELL block794transactions, перед target133config/v1, всего143config/v1.
Actual source SELL numerator **N264731434raw**. Фактический prebalance выбранного
source account равен6770149697raw.

**Whole-wallet D6770149697 — модель**, поскольку полные исторические parent
program pages не были собраны. H267894178raw — также model follower BUY,
согласованный с сохранённой source BUY price при model input10m lamports.
Тогда `floor(H×N/D)=10475397`, остаток257418781. Доля не выдаётся за доказанный
исторический whole-wallet balance.

Gap-spanning10368parent headers, complete historical program pages, follower
Jupiter BUY с fresh target ATA и SELL external execution boundary — модели.
SELL bundle намеренно non-DEX; quote/simulation/receipt моделируются loopback
сервером. Использован disposable model signer, production ключи не используются.
Replay ускоренный, логический темп2.5blocks/s; четырёхчасового live stream нет.

Fresh target rent2039280lamports отделён и сохраняется при partial SELL.
Model wallet cash следует BUY post balance987941720 и SELL fee19000.
BUY receipt decomposition и результирующая cash settlement остаются **Unresolved**;
тест не объявляет полный раздельный P&L или прибыльность доказанными.

## Helpers, зависимости и ограничения

Source binding/carryover сохраняет691старую строку и88новых: всего779attempts,
15870CU/8331750nanoUSD. Stream59581108989bytes/62connections сохраняется.
Подмена old row даже при равных итогах, чужой ledger и exhausted collection scope
отказывают. Run15 остаётся новым остановленным пакетом без activation clocks.

Architecture helper теперь считает только inline test body, а внешний cfg(test)
module не считает inline; отрицательный контроль сохранён. Test-only точные
app dev pins prost0.14.3/proto12.6.0 используют уже необходимый ingestion graph;
новых normal app dependency declarations нет. Требуемые proto/transitive updates
и их версии отдельно рассматривает независимый version reviewer.

Новые проверенные production modules: максимум298lines, новые full-path tests
максимум345lines. Test wiring app_tests.rs726lines≤800; новые тесты вынесены.
Production600/test800limits соблюдены; oversized waiver не нужен.

Лимиты не изменены: BUY≤0.01SOL×1, sourceSELL≤1, до4ч, cumulative providers≤$50,
slippage50bps, priority≤50000lamports, loss≤0.02SOL, native floor≥160200031,
submit attempts1. Первый принятый candidate расходует BUY slot; partial source
SELL может оставить остаток, дополнительный forced exit не разрешён.

Paid provider calls, stream и live trades при независимой проверке: **0**.
Main dirty tree сохранён. Pre-existing accepted paths не переаудировались.
Matching release/install/preflight и future source freshness/continuity не входят
в эту приёмку. Run15 до завершения stopped rollout остаётся под STOP.
