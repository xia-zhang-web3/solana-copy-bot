# Run15: транспорт и восстановление после обрыва

## Решение и граница

Задача: сохранить причину обрыва и восстановить подтверждённую parent-цепочку
между BUY и source SELL без повторной отправки. Прежние финансовые пути,
фильтры и лимиты не переаудировались и не расширялись.

Run15 использован и остановлен: STOP установлен, пять контейнеров exited,
orders/dispatch/receipt/fill/position/decision = 0. Supervisor сохранил
`normal_stop`; launcher `LIVE_RESULT.json` отсутствует. Это ограничение
диагностики сохранено; новый результат старого запуска не создавался.
Историческая причина `Internal` остаётся UNKNOWN.

## Изменения

- Bounded telemetry различает Internal/DataLoss/EOF и сохраняет sanitized
  `error_message`, причины, возраст соединения и received/emitted/durable slots.
- Canonical relay при EOF дочитывает буфер и закрывает направление отдельно.
  Локально воспроизведена потеря 32768 queued bytes; исправлена. Это не
  доказательство причины исторических Internal от внешнего соединения.
- Migration0089 добавляет immutable scope и atomic durable replay cursor.
  Cursor связан с точными child/parent/hash и полными Info; SQLite commit и
  readback происходят до ACK. Исходные финансовые строки не переписываются.
- После reconnect/restart daemon запрашивает inclusive `from_slot` из SQLite,
  получает полные блоки и проверяет overlap до свежих Admission. Полнота,
  parent/hash, индекс, signature и full Info проверяются до выпуска событий.
  Отсутствие replay, partial/conflicting block и unavailable history дают
  явный gap/отказ. Принятие subscribe само по себе не доказывает непрерывность.
- Сохранённые первые Admission и UNKNOWN не улучшаются повторной доставкой.
  Read-only replay scope разрешён только без финансовой активации.
- Подготовлен отдельный остановленный transport probe: постоянный STOP,
  signer/authority отсутствуют, quote/submit отключены. Оригинальный Run15
  и его authority/clocks/DB/ledger не переиспользуются.

## Проверки

Только affected targets; workspace suite и платных запросов не было.

- `copybot-app/test` debug compile: 29,95с; shared Cargo cache переиспользован.
- Genuine tonic → обе canonical relay → DeliveryReceiver: 2048 parents,
  33,77MB protobuf, Internal/DataLoss/EOF; независимая проверка принята.
- Новые recovery negatives: 6/6; SQLite checkpoint: 4/4;
  два затронутых migration/rollback controls: PASS независимо.
- Новый настоящий daemon scenario: 2,96с, независимо 3,04с; BUY1/SELL1. После confirmed bot BUY
  injected Internal, exact replay anchor, source SELL и продолжающийся поток.
  SELL UNKNOWN → restart → receipt reconciliation, send остался один.
  Частичный остаток 257418781 raw сохранён. Это offline-модель, не live receipt;
  денежная decomposition в модели Unresolved, P/L не объявлен.
- Relay/probe controls: drain, credit, STOP, connection cap, immutable clock,
  permission negative, WAL reader, cumulative budget и optional-failure result.
- Architecture guards `--changed` и `--all`: PASS.

Новый build target: **copybot-app/release**, нормальный matching GitHub Actions
artifact. Установка и финальный stopped preflight ещё выполняются; этот файл
не является READY seal или разрешением платного запуска.

Production dependencies не изменились. Две dev-only зависимости tonic/futures
уже присутствовали в locked ingestion graph: нужны настоящему loopback transport
тесту; новые версии/пакеты не добавлены. Guard разрешает только эти точные specs.
Все затронутые файлы проходят hard size/test-placement constraints; waiver нет.

## История, место и расходы

Удалён только ненужный ignored isolated `target/debug`: освобождено
41834713088 bytes. Старые финансовые DB, evidence, accepted artifacts и rollback
сохранены. Активный shared target не очищался.

Подтверждённый cumulative baseline: HTTP996 /19020CU /$0,0099855 модели,
stream83722698801 accounted bytes. Это модель, не выписка провайдера.
Новый расход подготовки: stream/RPC/signature/submit0.

## Следующий шаг

После matching install + независимого preflight можно отдельно решить,
запускать ли read-only probe: до480с от первого upstream attempt, ≤4GiB
включая connection headroom, ≤3attempts, ≤$0,40 модели внутри общих $50,
HTTP/CU/signature/submit0. Один app restart сохраняет DB/clock/лимиты.
При неизвестных terminal metrics полный grant резервируется, расход не обнуляется.
Новых торговых команд пока нет. Причина живых Internal и фактическое качество
provider replay требуют внешнего наблюдения; восьмиминутная проверка не доказывает
четырёхчасовую устойчивость или актуальность стратегии.

Private evidence: `run15-stream-repair-01`, `run15-transport-offline` и
`run15-transport-probe-01` под `.codex/private/native-handoff-local-experiment-01`.
