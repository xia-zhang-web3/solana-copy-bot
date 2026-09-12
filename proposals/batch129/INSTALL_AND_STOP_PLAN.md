# №129: install/stop/rollback order — future separate authorization

Сейчас install/restart/server0. Это порядок с обязательными pending inputs,
не готовая команда для неизвестного host. Matching CI artifact необходим; local
source/tree и executables127/128 не deployable artifacts.

1. Получить настоящее CI proof из BUILD_AND_PUBLISH_PLAN, затем отдельное разрешение
   на bounded server preflight. Сопоставить actual unit/binary/config/DB с inputs;
   зафиксировать planned app artifact, previous_artifact_id и rollback choice.
2. До install решить совместимость previous binary с prospective schema/config
   либо конкретный pretrade consistent-restore/recovery вариант. Unknown — STOP.
   Не получать compatibility запуском старого бинарника на рабочей БД.
3. После отдельного разрешения окна остановить всех DB writers и их restart/timer
   triggers согласованным service-control планом. `Restart=always` в template:
   failed startup не должен уйти в бесконечный restart. Actual control/recovery
   команды согласуются по discovered units. Проверить quiescence и pending/open
   ownership; автоматические repairs/trade exits вне technical scope.
4. До migration/writer start сделать и проверить согласованные consistent DB и
   config/env/unit backups. Удостоверить DB/WAL/schema/financial identity и hashes.
   Финансовые records после backup запрещают слепой restore старой DB.
5. Применить только accepted fail-closed delta к actual config/env; сохранить
   experiment ID/state/mode и все omitted settings. Проверить syntax, exact delta,
   file permissions и prospective effective flags=false после всех overrides.
6. Upload только matching app `.tar.gz` и `.tar.gz.sha256` из проверенного CI;
   повторить checksum на сервере, полный verifier/manifest/bin-set/migration proof.
   Объявить exact package/binary/service и paths. Сначала installer --dry-run.
7. Установить app; проверить INSTALL_COMPLETE, manifest SHA, binary links/hash,
   весь extracted migration set и resolver target. Installer связывает migrations,
   а не применяет SQLite SQL. Writers пока остановлены.
8. Только при всех prerequisites и отдельном install/start разрешении запустить
   один контролируемый fail-closed daemon start; разрешение №129/commit-CI этого
   не включает. Startup открывает/меняет DB и применяет non-deferred pending SQL.
9. Postflight: новый PID/binary SHA, ActiveState/SubState/NRestarts, bounded
   sanitized startup/error logs, actual effective flags=false, schema_migrations
   и deferred-index state против плана, unchanged financial ownership/Unknown
   holds. Technical fail-closed proof — не trading green, landing или net-profit.

## Future install commands, после заполненного preflight

Переменные — verified actual inputs, не угадывать `/var/www`/host/service.
Tools должны быть exact accepted checkout/version; выполнение команд сейчас0.

```bash
set -euo pipefail
: "${INSTALL_DIR:?actual bin directory}"
: "${APP_ARTIFACT:?verified uploaded full app tarball absolute path}"
: "${APPROVED_SHA:?real CI/source SHA}"
(cd "$(dirname "$APP_ARTIFACT")" && shasum -a 256 -c "$(basename "$APP_ARTIFACT").sha256")
tools/install_operator_artifacts.sh --dry-run \
  --expect-package copybot-app --expect-profile release \
  --expect-target x86_64-unknown-linux-gnu --install-dir "$INSTALL_DIR" \
  "$APP_ARTIFACT"
tools/install_operator_artifacts.sh \
  --expect-package copybot-app --expect-profile release \
  --expect-target x86_64-unknown-linux-gnu --install-dir "$INSTALL_DIR" \
  "$APP_ARTIFACT"
```

Перед mutating строкой повторно подтвердить approved SHA в artifact manifest,
полный checksum set и весь migration bundle. `--allow-dirty` не применять.
Installer не принимает `--expect-sha`: SHA отдельно проверяется по manifest и run.
Migrations link должен разрешаться из actual `system.migrations_dir`, с учётом
WorkingDirectory/config path; существующая unmanaged migrations directory требует
конкретного installer preflight решения, не удаления наугад.

## Stop / rollback

Первая technical RED останавливает дальнейшую установку/запуск. Если новый writer
уже стартовал, прекратить auto-restarts согласованным service-control действием,
сохранить свежую DB/WAL/log identity и новые финансовые факты. Не rearm, не funding,
не retry/start в цикле. Сообщить конкретный RED и следующий необходимый выбор.

До schema application возврат previous package всё равно требует verified
previous complete manifest/checksums/config и service preflight. После startup
SQL previous consumer compatibility либо approved restore/recovery обязательны.
CI synthetic rollback fixture не является previous production binary proof.

```bash
: "${PREVIOUS_ARTIFACT_ID:?verified complete installed previous app release}"
tools/install_operator_artifacts.sh --dry-run \
  --expect-package copybot-app --expect-profile release \
  --expect-target x86_64-unknown-linux-gnu --install-dir "$INSTALL_DIR" \
  --rollback "$PREVIOUS_ARTIFACT_ID"
tools/install_operator_artifacts.sh \
  --expect-package copybot-app --expect-profile release \
  --expect-target x86_64-unknown-linux-gnu --install-dir "$INSTALL_DIR" \
  --rollback "$PREVIOUS_ARTIFACT_ID"
```

Это binary/migrations-link rollback, SQLite rollback0. DB restore здесь намеренно
не представлен как универсальная команда: его input зависит от actual financial
records и consistent backup identity. Не устанавливать до конкретного решения.
После новых receipts/unknown/positions старую DB backup не накатывать.

Если отдельно выбран optional operators125: та же full-package verify/dry-run/
install схема с package `copybot-operators`, profile `operator-release`, тем же
approved SHA и полным default набором10. Daemon restart для operator-only
install/rollback не требуется. Это не добавляет право запуска финансовых operators.
