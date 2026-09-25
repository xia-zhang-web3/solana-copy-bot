# Run09: SELL при продолжающемся parent stream

## Решение

Независимый review принял узкий кодовый ремонт concurrent SELL для offline
scope. Финансовую активацию Run09 сохраняем в HOLD до matching GitHub Actions
`copybot-app/release`, установки в тот же неиспользованный пакет, нового seal и
локального preflight. Старый `0ed4176` установленный release нужен для
rollback. Run07 и Run08 остаются использованными и не повторяются.

Обычный commit постороннего parent block после reserve или complete больше не
считается окончательным изменением торгового snapshot. Важные денежные
границы остались: полный parent graph и ownership под `IMMEDIATE` при reserve,
complete и финальном dispatch перед единственным send. Реальное изменение
связанного parent/owner/amount по-прежнему приводит к отказу. Сохранённый
handoff без dispatch явно удерживается, а UNKNOWN и restart не дают второй
send. Во время RPC проверяется состояние lease, pending token, claim и
обязательные бюджетные ограничения; полный граф повторно проверяется в
атомарных денежных точках. Quote TTL 5 секунд не расширен.

## Целевые проверки

- Детерминированные parent-only commits после reserve и complete проходят без
  ложного отказа. Relevant parent conflicts до reserve, во время build и
  после complete отказывают на соответствующей атомарной границе.
- Денежный fixture `fractional_financial_tests`: 8/8 PASS; transport fixture
  с commit во время RPC collection: 7/7 PASS последовательно.
- Release-profile offline replay: 36 000 синтетических блоков за 14 400
  **логических** секунд, последние 150 полных блоков при 400 мс/блок и
  продолжающиеся реальные SQLite parent commits во время simulation и
  blockhash. `ExecutionCanaryRunner.process_tick` дал один mock SELL,
  подтверждённый receipt/accounting и отсутствие второго send после restart.
  Прогон занял 166,73 секунды wall time; `claim_busy=0`, quote→dispatch
  2785 мс, до quote deadline оставалось 2214 мс. Logical usage: 180 233
  rows/161 295 715 bytes; четырёхчасовая проекция с запасом:
  181 257 rows/178 072 931 bytes, ниже принятых caps.
- Отдельно прошёл короткий настоящий daemon tick после quote window;
  `architecture_guard.sh --changed` и `git diff --check` — PASS.

На macOS для отдельного release test process поздняя выборка RSS составила
1 606 640 KiB при лимите app container 2 GiB. Это не Linux cgroup measurement
и не live верхняя граница; исходные full payloads Run08 не сохранены.
Mock replay не подтверждает реальную задержку провайдера, исполнение live SELL
или доходность. Платных provider calls, подписей и отправок в подготовке нет.

## Объём изменения

Только SELL preparation/ownership, fractional progress и причинные fixtures.
Build target: `copybot-app` release. Config, миграции, финансовые лимиты,
зависимости и Discovery не менялись. Production файлы ниже 400 строк,
изменённые тесты ниже 500; waiver не требуется. Независимый reviewer проверил
changed graph/owner/dispatch/restart paths и принял этот offline scope.

Следующее действие: commit принятого diff, matching GitHub Actions artifact,
installation/rollback binding в остановленный Run09 и local package preflight.
