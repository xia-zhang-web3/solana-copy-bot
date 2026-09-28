# Run15: HTTP recovery, локальная приёмка и остановленная установка

Дата: 2026-09-29. Цель — восстановить непрерывность после разрыва между BUY и source SELL. Вопрос независимой проверки: позволяют ли изменённые recovery/identity/financial gates безопасно перейти к одному короткому read-only live probe? Решение относится к подготовке; live recovery и прибыльность не доказаны.

## Реализовано

- После разрыва gRPC подключается без отвергнутого full-block `from_slot`; отдельный bounded reader сохраняет новый вход, пока confirmed `getBlocks/getBlock` восстанавливают цепочку до live anchor.
- Проверяются parent/hash, полные транзакции/index, overlap, skipped slots и durable commit/ACK. Missing/null/конфликт не становятся завершённой цепочкой. HTTP не повторяет отказавший запрос автоматически.
- Production normalizer сохраняет transaction/config markers, ошибки текущего pinned ABI, fee, rewards, returnData, balances и UI float bits. Первый Info/facts не переписывается. Полные request/response evidence архивируются общим broker до доставки; резервация расходов — до возможного outbound.
- Recovered BUY сохраняет исходный block clock и не занимает одноразовый слот свежего BUY. HTTP финансовую поддержку v1 не включает.
- Shared continuity hold действует в основном daemon, native BUY, signer/dispatch SELL. Включается до diagnostic await, при live refusal и синхронно при Drop. Снимается после подтверждённого durable anchor; уже отправленные BUY/SELL продолжают reconciliation.
- Ранний проверенный signer-фильтр исключает чужие individual updates. Полные блоки, известные/ожидающие signatures и признаки конфликтов сохраняются. Оплачиваемый входящий объём фильтр не уменьшает.

## Причинные offline проверки

| Проверка | Результат |
| --- | --- |
| Настоящие tonic → два canonical relay → daemon → HTTP → SELL → mock receipt/accounting | PASS; BUY1, SELL1, UNKNOWN/restart без send2; частичный остаток сохранён |
| Конфликт последнего anchor после recovered source SELL | PASS; BUY1, source SELL распознан, SELL send0 |
| Missing/null/branch conflict; restart в середине catchup; backpressure refusal | 3/3 PASS; только проверенный partial progress, hold до ожидающего diagnostic emit |
| Signer ownership/current/pending/durable conflicts, interrupted anchor, synchronous Drop | 4/4 PASS |
| Normalizer/HTTP refusal/size/order controls | 6/6 PASS; отдельные corpus725/779 PASS |
| Stale BUY → reopen → последующий live BUY | PASS; старый BUY не расходует слот |
| Broker reserve, STOP, allowlist, RPC error/raw attribution | PASS; независимые бюджетные controls PASS |
| Broker 8 MiB × 4 concurrent, каждый role 256 MiB | PASS; backend peak179474432, front115707904 bytes; OOM0 |
| Остановленный package preflight helper | 7/7 controls PASS; actual CLI, Docker Desktop paths и nested state mount; установленная проверка отдельно ниже |

Сохранённые HTTP requests просили `rewards=false`: original null rewards правильно отвергаются как неполный ответ `rewards=true`. Только в offline копии моделируются empty rewards/partitions; исторические rewards этим не доказаны. Транзакции corpus сохранены; новые rent/fee факты из модели не объявляются live фактами.

Парное sustained измерение: два периода примерно по20 секунд, fixed3 blocks/s, по60 полных блоков/82590 transactions и12330 individual updates через настоящий транспорт и SQLite. Обработка individual updates занимала5.472 секунды; whole update durations уменьшились10.793→5.330 секунды. Source→durable ACK max359→279ms в этом одном парном измерении. Входящий объём обоих режимов около336.8MB. Все checkpoints сохранены, финансовые строки0, reconnect0. Это elapsed stage timings native macOS debug, не OS CPU и не четырёхчасовой live/cgroup тест. HTTP fetch/normalize не входят в отдельный update timer; полное HTTP catchup доказано денежным сценарием.

## Установка и состояние

Exact build target/profile: **copybot-app/release**, только GitHub Actions artifact принятого опубликованного SHA. Локально — scoped test/debug targets в общем Cargo cache, без workspace suite/release build. Dependencies0, миграции не менялись;89 миграций сохраняются. Новые production модули меньше400 строк; затронутые файлы остаются ниже hard limits; новые tests вынесены отдельно. Применимые architecture/CI и checksum proof должны быть записаны в итоговом evidence, не заменяются локальными PASS.

Пакет: `/Users/tigranambarcumyan/.codex/private/native-handoff-local-experiment-01/run15-http-recovery-probe-02`. Permanent STOP, HTTP_STOP и STREAM_STOP; пять созданных контейнеров, без старой DB/authority/clocks/ledger и без signer. Matching artifact, rollback и установленный local preflight фиксируются в `evidence/INSTALLATION_AND_PREFLIGHT.json` пакета. Этот файл является итоговым состоянием установки; до его PASS подготовка не завершена.

Исходные Run15/probe01 consumed, остановлены и не переиспользуются.16 исторических hashes и cumulative расходы сохраняются: HTTP996/19020CU/$0.0099855; stream85026403600 accounted bytes; cumulative model$7.928686412. Новых платных provider действий/боевых подписей/отправок в этой подготовке0.

## Реальные ограничения и одно следующее решение

Внешняя причина `DataLoss lagged`, TLS/live endpoint throughput и реальные HTTP rewards ещё не проверены. Разрыв посреди уже зарезервированного, но не отправленного SELL может оставить явный HOLD/no-resend; этот batch не разрешает отпускать обязательство или автоматически создавать новый SELL. Rollback хранит предыдущий accepted artifact/config под STOP; новую DB с RecoveredBlock provenance старому daemon не подключать.

Предложение владельцу после matching installation/preflight: **один read-only probe ≤480 секунд от первого upstream attempt, stream≤4GiB, максимум3 upstream подключения и1 restart; HTTP≤1024 attempts/40960CU, response≤8MiB, concurrency4**. Модель: stream≤$0.40 + HTTP≤$0.021504 =≤$0.421504 внутри прежних общих$50. HTTP raw evidence отдельно может занять до8GiB;4GiB — только stream cap. [Alchemy CU](https://www.alchemy.com/docs/reference/compute-unit-costs), [Solana getBlock](https://solana.com/docs/rpc/http/getblock).

Пункт8 принятого задания требует отдельного решения владельца на этот новый платный шаг. Подготовка его не запускает. До решения — STOP, финансовая authority отсутствует, signatures/submissions0. Цель probe — фактический validated catchup/live anchor и bounded backlog; торговое окно не является следующим шагом.

Подробные local/independent logs: `/Users/tigranambarcumyan/.codex/private/native-handoff-local-experiment-01/run15-http-recovery-01/evidence/` (INDEPENDENT_REVIEW.json, THROUGHPUT_SUMMARY.json, HTTP money/negative/corpus/controls и сохранённые причины preparation failures).
