# Read-only HTTP recovery probe: остановлен, восстановление не доказано

Дата: 2026-09-29. Независимое решение: **STOPPED_SAFE_HTTP_RECOVERY_NOT_PROVEN**.

## Действительно выполнено

- Одно owner-разрешённое оплачиваемое окно: **60,676 секунды** от первого upstream attempt до остановки stream backend. Восемь минут — верхний предел, а не достигнутая длительность.
- **2** upstream подключения из разрешённых 3; **1** предусмотренный runtime restart. Clock и ATTEMPT не сбрасывались. Restart relays после первого upstream не выполнялся.
- **111** подтверждённых parent-записей: slots451567359–451567469, все110 parent/hash связей совпадают, gaps0 и contradictions0.
- При остановке перед restart durable head достиг451567469 и from_slot451567468. Тот же head сохранился после restart.
- Первая HTTP резервация: getBlocks,10CU,$0,00000525 по модели. getBlock не выполнялся. Ответ/причина broker не архивированы; успешной HTTP-догрузки, recovered ACK и нового live anchor нет.
- Daemon завершился `READ_ONLY_APP_EXITED`, exit1: `confirmed_http_recovery_refused` → `http_recovery_response_identity`. Durable inbox сохраняет Rejected(HttpRecoveryRefused), состояние не объявлено непрерывным.
- Проверены19 финансовых/owner/cohort/receipt таблиц: все0. Подписей и отправок0. SQLite quick_check=ok.

## Измеренная обработка

За первое короткое окно telemetry: reader queue peak1block/4,123,489bytes; dequeue age max423µs; block update max46,555µs; durable ACK max41,252µs; reconnects0. Снимок примерно на28-й секунде: daemon380,3MiB/3GiB и CPU6,86%; stream relays около14–15% каждый. Это короткое наблюдение; устойчивость в течение8мин/4часов не доказана. Не наблюдались внешние DataLoss/lagged в этом коротком окне.

## Найденные конкретные дефекты

1. **WorkingDir подготовки.** Первая локальная startup phase завершилась до любых upstream attempts: относительный `state/discovery_recent_raw.db` открывался при WorkingDir пустом. У принятого network-none startup стенда был `/opt/copybot`. Исправлен только рабочий каталог app контейнера. Исходные RESULT/LEDGER/ATTEMPT/БД и stoppedfailedapp сохранены. Независимый zero-outbound guard разрешил продолжение того же ATTEMPT; счётчики и разрешённый runtime restart не сбрасывались. Именно это продолжение впервые подключилось к провайдеру.
2. **TLS HTTP backend.** Тот же cached image и readonly CA mount в network-none проверке: actual HTTPSConnection default context содержит0CA, хотя сохранённый bundle содержит128CA. Задание `SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt` загружает128CA, VERIFY_REQUIRED и hostname check сохраняются. Это подтверждённый локальный дефект конфигурации. Точный текст именно живого HTTP отказа потерян, поэтому нельзя объявлять TLS единственной доказанной причиной этого отказа.
3. **Потеря HTTP диагностики.** Broker возвращает локальный failed/refused HTTP response без JSON-RPC id; adapter проверяет id до разбора HTTP status/reason. Ошибка masking подтверждена source inspection; failed broker path не архивирует reason/request outcome. В живом evidence осталась резервация getBlocks и последующий identity отказ, без upstream body/status.

## Расход и остановка

Наблюдалось337,967,650bytes; сохранённый headroom2,097,152bytes; accounted340,064,802bytes. Stream model$0,031671003 + HTTP reservation$0,00000525 = **$0,031676253**. Накопленный model **$7,960362665** внутри прежних$50; HTTP997 attempts/19030CU cumulative. Actual invoice UNKNOWN; неуспешная HTTP резервация не обнулена.

Все5 текущих контейнеров и первый failedstartup app остановлены, OOMfalse/restarts0. STOP, STREAM_STOP, HTTP_STOP сохранены, lease истёк. Все16 исходных historical hashes совпадают. Run15/probe01 и текущий probe consumed; повтор этим разрешением не покрыт. Пакет содержит `USED_DO_NOT_REPEAT.txt` и `LIVE_STATUS.json`.

## Проверки и объём изменений

Build/profile **NONE**: использован установленный matching copybot-app/release61db22bd; binary/runtime/dependencies/migrations не менялись. Добавлены только task-owned private controller modules и dedicated offline controls; 11/11 root и независимых проверок, плюс5 независимых отрицательных controls PASS. Старые52 установленных helper hashes сохранены. Единственная container repair — WorkingDir, при сохранении image/network/limits/mounts/DB. Все module файлы меньше400строк; waiver0.

В clean worktree обновлены этот отчёт и текущая секция PROJECT_RECOVERY_PLAN.md; основное dirty дерево не редактировалось. CI/build/provider во время последующей offline диагностики0.

## Следующий связный ремонт

Перед новым live решением: подключить существующий CA bundle к HTTP backend, проверять WorkingDir и effective TLS binding в package preflight, сохранять broker failed/refused outcome и исходный HTTP status/reason в daemon до общей identity ошибки. Causal offline check должен использовать настоящий broker/error response; successful JSON-RPC identity/chain/ownership guards сохраняются. Не повторять неизменённые suites или платный сбор. Изменение только контейнерного TLS/env не требует нового app artifact; изменение daemon HTTP diagnostics требует matching copybot-app/release и целевых gates. Новый платный probe потребует отдельного owner разрешения после остановленной подготовки.

Автоматическое копирование и прибыльность не доказаны.
