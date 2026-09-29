# HTTP backend: узкая корректирующая подготовка

Дата: 2026-09-29. Вопрос решения: устраняет ли подготовка подтверждённые CA/CWD/
диагностические дефекты и готов ли новый остановленный read-only пакет?
Критерий: настоящий локальный HTTPS через backend и Rust adapter, проверяющий CA,
отрицательные контроли; независимый review; matching CI artifact установлен,
реальный container context совпадает с preflight. После ответа проверку закончить.

## Изменённый путь

- Backend использует явный проверяющий SSLContext с сохранённым SSL_CERT_FILE.
- Preflight требует действительный CA context, точный backend command/image/mount,
  WorkingDir=/opt/copybot фактического app-контейнера; тестовый upstream запрещён.
- Failed/refused outcome сохраняет метод, reservation id, фазу, тип причины и
  известный HTTP status. Неведомый status остаётся null; резерв не возвращается.
- Произвольные HTTP/RPC сообщения, URL и credentials не попадают в диагностику.
  Успешные full block facts сохраняются. Rust сначала читает типизированный broker
  отказ; успешный ответ и RPC error по-прежнему требуют точных jsonrpc/id.
- HTTP transport выделен в маленький модуль для ограничения размера entrypoint.

## Проверка и границы

5 Rust diagnostic controls PASS. Настоящий Rust adapter → broker → local HTTPS:
trusted CA / untrusted CA / hostname mismatch / HTTP503 / wrong RPC id PASS.
Cached Python image с настоящим backend command прошёл те же пять режимов;
проверка VERIFY_REQUIRED и hostname включена. Публичных CA128; local fixture добавляет
один собственный CA. Independent checks и helper controls записаны отдельно.

Локальная первая проверка fixture отказала из-за слишком длинного Unix socket path;
причина сохранена, socket перенесён в собственный короткий temporary directory,
повторена только затронутая проверка. Provider calls/signatures/submissions=0.
Неизменённые money/recovery/throughput сценарии повторно локально не запускались.
Build target: copybot-app, profile release, GitHub Actions; стандартные CI gates
по ARTIFACT_DEPLOY сохранены. Локально только copybot-ingestion/lib, test profile.
Dependencies/migrations: 0. Main dirty tree и использованные пакеты не менялись.

## Состояние и следующий шаг

Новый private package: run15-http-recovery-probe-03. STOP/HTTP_STOP/STREAM_STOP;
пять контейнеров created, StartedAt=zero; state/clock/ATTEMPT/authority отсутствуют.
Matching artifact/install/preflight ещё в работе; это пока не READY.
Carryover: $7.960362665 model, HTTP997/19030CU, stream85366468402bytes;
actual invoice UNKNOWN. Failed probe02 reservation сохранён. Новый расход0.

Точная первичная live ошибка probe02 потеряна и остаётся UNKNOWN. HTTP recovery
на живом endpoint, автоматическое копирование и прибыльность пока не доказаны.
Ранее принятый HOLD для зарезервированного, но не отправленного SELL сохраняется.
После установленного READY следующий платный read-only probe требует отдельного
решения владельца: до8минут/4GiB/3connections/1restart/1024HTTP/40960CU,
≤$0.421504 внутри общего$50. Stream/HTTP/подписи/сделки сейчас не разрешены.

[Подробные локальные доказательства](/Users/tigranambarcumyan/.codex/private/native-handoff-local-experiment-01/run15-http-corrective-01/evidence/).
