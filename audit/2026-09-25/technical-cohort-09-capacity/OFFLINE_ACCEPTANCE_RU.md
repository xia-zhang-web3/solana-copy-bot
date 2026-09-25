# Run09: вместимость block stream и durable parent graph

## Решение

Run08 использован: сохранённый `APP_EXITED`, финансовый `NO_SIGNAL`, без orders,
dispatches и отправок. Его `BlockCapacity` при cache count32/TTL60с подтверждён
отдельным offline тестом actual association `push`: 33-й уникальный блок при
400 мс/блок отказан через 12,8 с. Старый пакет не активировался повторно.

Для Run09 выбран ограниченный cache count192/encoded bytes512 MiB,
metadata96 MiB, durable inbox count500000/bytes512 MiB, app memory2 GiB.
TTL60с не сокращён. Остальные финансовые и provider caps не расширялись.
При 400 мс/блок в rolling TTL удерживается около151 блока. Синтетический
full-block envelope равен 3 145 222 bytes и содержит 1411 посторонних
transactions; 151 таких encoded блоков занимают около453 MiB. Для этих
границ остаётся запас как по count, так и по encoded bytes.

Один сквозной offline replay провёл 450 full blocks за первые 180с и далее
непрерывную parent-цепочку до 36000 blocks/14400с. Через durable bridge и
настоящий daemon runner прошли source BUY, bot BUY anchor, поздний source SELL,
quote, simulation, mock submit, receipt, cash accounting и restart без
второй отправки. SELL остался частичным: 2500/10000 raw, остаток 7500 raw,
как требует тестовая source сделка. Missing bot anchor/parent edge отказывают;
старые parent conflict/duplicate и ACK tests сохранены и прошли.

Измеренный peak RSS отдельного release test process со стандартным стеком:
1 603 534 848 bytes; до лимита 2 GiB осталось 543 948 800 bytes. Это
измерение на macOS, не
оценка размера Linux daemon в live потоке. Фактические полные Run08 block
payloads не были сохранены: 3,145 MiB — явно синтетический envelope, не
заявление о live максимуме. Полный replay показал logical usage 180038 rows/
161156810 bytes; проекция с запасом на четыре часа — 181062 rows/
177934026 bytes, ниже caps 500000/536870912. Размеры encoded cache,
logical SQLite meter и RSS измерялись/учитывались отдельно.

## Узкий кодовый ремонт

Reader повторно использует в пределах одной проверки уже валидированный
parent block и hash, сохраняя полный charge каждой зависимости, конфликтные
исходы и проверку parent paths. Strict quote хранит SHA256 полного canonical
snapshot при длинном binding; короткий формат не изменился. Длинный snapshot
готовится один раз. Во время RPC повторные guard проверки используют
SQLite `data_version` только для того же неизменённого внешнего commit epoch;
после чужого commit выполняется полная проверка. Финальный dispatch всегда
повторно проверяет полный граф в `IMMEDIATE` transaction перед send.
Отдельный WAL test доказал, что финальную версию надо читать после завершения
read transaction. Старый quote TTL5с, owner/amount/fee/native limits и
UNKNOWN/no-resend не менялись.

## Проверки и предел доказательства

- Full 450-full/36000-block replay: PASS, 90,61с direct process со
  стандартным стеком; один mock
  `sendTransaction`, подтверждённый partial settlement, restart без второго.
- Old32 `BlockCapacity` test: PASS; parent order/protocol tests 10/10 PASS;
  SQLite version tests 2/2 PASS; strict quote contract 9/9 PASS.
- Денежный path проверен serial `fractional_financial_tests` 4/4 PASS,
  включая foreign decision change/полную fallback проверку. Параллельный
  запуск этого общего fixture дал `fraction_collection_capacity` из-за
  пересечения тестовых данных; последовательный целевой прогон прошёл.
- Независимый review scoped diff принял cache/parent/ACK/quote/dispatch и
  memory evidence. Локальный тест не является live сделкой или доказательством
  доходности. Matching CI artifact, изолированный остановленный пакет и
  финальный local preflight проверяются отдельно перед командой владельцу.

Изменённый build target: `copybot-app` release и затронутые storage/ingestion
тесты. Новая прямая зависимость `sha2` только для storage-core, уже была в
Cargo.lock. Production файлы остались ниже400 строк; новые/изменённые тесты
не превышают500 строк. Нет waiver по размеру или новым тяжёлым зависимостям.
