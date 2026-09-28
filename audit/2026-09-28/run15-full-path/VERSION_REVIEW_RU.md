# Run15: независимая проверка version/config boundary

Дата: 2026-09-28. Reviewer: отдельный агент `run15_inventory`, не автор
изменений ingestion/version. Исходная база: `53203c3ddf242c55e713fc2d5471ab8f90dd916a`.

## Решение и объём

Вопрос: сохраняется ли V1 marker до runtime admission и исключается ли V1 swap
без потери mixed block, parent/index и control events? Завершение проверки:
прочитать изменённый boundary и dependencies, независимо выполнить новые
portable controls и реальный локальный tonic transport control.

**ACCEPT_SCOPED: изменённый version/config ingestion path принят.**
Конкретного оставшегося дефекта в этом diff не найдено. Следующее действие —
выпуск matching `copybot-app/release` artifact и остановленная установка в Run15.
Artifact/install, финансовая активация и live provider continuity сюда не входят.

## Семантика и dependencies

- Proto 12.0.0 не содержит `Message.config`; proto 12.6.0 содержит optional
  `TransactionConfig config = 7`. Обновление нужно до tonic decode: после
  отбрасывания неизвестного поля bridge восстановить его не мог.
- `yellowstone-grpc-client` остаётся 12.0.0; его зависимость proto допускает
  12.6.0. Точный proto pin в ingestion и operators согласован; обязательные
  новые поля request имеют `None`, новые test server methods не используются.
  Stream filters и transport budgets не расширены.
- `siphasher`, `solana-pubkey` и их зависимости обязательны для выбранного
  proto 12.6.0. Его build dependency требует anyhow >=1.0.103, поэтому переход
  с прежнего 1.0.101 необходим. Общего cargo update не обнаружено.
- Config presence отклоняется независимо от `versioned=true/false`, до старых
  legacy/V0 cardinality bounds. Неподдерживаемый посторонний swap не попадает
  в Admissions/history, но остаётся ограничен envelope/input cap.
- Изменённый config у уже удерживаемой подписи сохраняет конфликт provider
  identity и явный Unresolved; он не становится новой принятой сделкой.
- Полный mixed block остаётся в association path: parent links и исходный
  transaction index сохраняются; Reset/End не исчезают. V1 sending не добавлен.
  Raw financial targets по-прежнему допускают только legacy/V0 без config;
  неизвестная версия/config не становится разрешением сделки. Metadata-only
  prefix effects и денежные семантики приняты отдельно в
  [INDEPENDENT_REVIEW_RU.md](INDEPENDENT_REVIEW_RU.md), без self-approval
  reviewer на собственные storage изменения.

## Независимые причинные проверки

Использован уже построенный test/debug binary общего cache
`copybot_ingestion-93ce8c0acad5e5e8`; **build NONE**, workspace suite не запускался.
После этих проверок прочитан финальный staged version diff: проверенные guards
не менялись, ритуального повторного запуска нет.

| Проверка | Результат |
| --- | --- |
| Proto roundtrip/config отказ при обоих значениях versioned | PASS |
| Mixed protobuf: без V1 Admissions, parent/index/Ping/Reset/End сохраняются | PASS |
| Config conflict у retained signature остаётся явным | PASS |
| Реальный client/tonic loopback: config сохраняется до admission | PASS |

Последний контроль причинный: server передаёт V1 clone валидного swap и legacy
swap в mixed block. Потеря tag 7 дала бы второй Admission; получены один Admission,
один `UnsupportedMessageConfig` и parent event. Это настоящий локальный tonic
codec/transport, без внешнего stream или provider calls.

Изменённые production modules имеют 203/226/300 строк; новые version tests
411/105 строк. File/test-placement ограничения выполнены; operator alignment
остался совместимостью protobuf API и не добавил operator logic в daemon.

Ограничение: локальные wire/replay controls не доказывают, что внешний provider
всегда передаёт marker, и не доказывают непрерывность будущего stream. Новых
provider calls, подписей, отправок, финансовой активации или установки reviewer
не выполнял. Готовность установленного пакета принимается отдельно.
