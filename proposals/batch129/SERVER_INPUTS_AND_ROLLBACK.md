# №129: будущий bounded read-only server preflight

Сейчас SSH/server inventory0. Этот этап НЕ входит в следующий запрос commit/push/CI.
Не выполнять команды до отдельного разрешения и получения actual host/service/paths.
Repo snapshot не источник текущих server facts. Первая техническая RED — STOP,
не повторять запросы/repair/rearm/funding автоматически.

## Ещё не полученные точные inputs

| Input | Кто/откуда должен предоставить | Что блокирует отсутствие |
|---|---|---|
| actual host, service unit и все writers/triggers | владелец, затем systemd inventory | server access и quiesce plan |
| current executable path/hash, current manifest/commit | service PID + installed release | identity и previous_artifact_id |
| previous complete artifact и совместимая schema/config | verified installed manifest/release | безопасный rollback |
| actual --config/env config path, WorkingDirectory | unit/drop-ins + process argv/env | effective config |
| EnvironmentFile/drop-in paths и порядок overrides | current systemd definitions | доказательство трёх false flags |
| DB path/identity/schema_migrations/deferred state | loaded config + read-only SQLite | полный pending set |
| pending/unknown/open ownership и experiment state | canonical DB + applicable existing readers | остановка writers и recovery |
| disk/memory/WAL/build state | bounded host observation | место и окно установки |
| consistent DB/config backups и checked identities | отдельный approved stopped-writer backup | pretrade restore option |

Никакой actual previous_artifact_id, host, service или production schema здесь не
подставлен из истории. Template paths находятся в CONFIG_PROPOSAL, не в inputs.

## Ограниченный набор metadata-команд

Будущий оператор работает на отдельно разрешённом host. Каждый блок под внешним
timeout≤15s, один запуск, без фонового polling; отсутствие timeout/tool — STOP.
`SERVICE`, `INSTALL_DIR`, `DB_PATH`, `EXPECTED_CURRENT_SHA` задать только actual inputs.

```bash
set -euo pipefail
: "${SERVICE:?actual unit}"
: "${INSTALL_DIR:?actual installed bin directory}"
: "${DB_PATH:?actual resolved SQLite path}"
timeout 15s systemctl show "$SERVICE" \
  -p Id -p LoadState -p ActiveState -p SubState -p MainPID -p NRestarts \
  -p FragmentPath -p DropInPaths -p WorkingDirectory -p EnvironmentFiles
timeout 15s readlink -f "$INSTALL_DIR/copybot-app"
timeout 15s shasum -a 256 "$INSTALL_DIR/copybot-app"
timeout 15s python3 - "$INSTALL_DIR/operator-artifact-current-copybot-app.json" <<'PY'
import json, sys
m = json.load(open(sys.argv[1]))
print(json.dumps({k: m.get(k) for k in
    ('artifact_id', 'git_sha', 'git_dirty', 'package', 'profile', 'target',
     'expected_binaries', 'migration_bundle')}, sort_keys=True))
PY
timeout 15s df -Pk "$INSTALL_DIR" "$DB_PATH"
timeout 15s free -m
```

Running cargo/rustc inventory, без command-line arguments:

```bash
timeout 15s python3 - <<'PY'
import subprocess
for row in subprocess.check_output(['ps', '-eo', 'pid=,comm='], text=True).splitlines():
    fields = row.split()
    if len(fields) == 2 and fields[1].rsplit('/', 1)[-1] in ('cargo', 'rustc'):
        print(row)
PY
```

Не выводить полный `ps ... args`, `systemctl show Environment`, `systemctl cat`,
raw `/proc/PID/environ`, env files, credential files или необработанные журналы.
Manifest whitelist не заменяет checksum verification всего previous package:
проверить его `SHA256SUMS`, INSTALL_COMPLETE, expected bins, release symlinks и
hash actual PID executable. Checkout commit отдельно записывается read-only, если
checkout существует; не приравнивать его к SHA установленного бинарника.

## Config path и текущие non-secret effective flags

Будущий запуск на Linux с actual running PID. Если процесс отсутствует, current
effective flags Unknown: не стартовать его ради inventory. Source systemd unit/
drop-ins проверить приватно по whitelist директив, сохранить только paths/hashes,
число источников и решения по overrides. Credentials не печатать.

```bash
: "${PID:?actual MainPID from the selected service}"
timeout 15s python3 - "$PID" <<'PY'
import json, pathlib, sys, tomllib
root = pathlib.Path('/proc') / str(int(sys.argv[1]))
env = dict(x.split(b'=', 1) for x in (root/'environ').read_bytes().split(b'\0') if b'=' in x)
argv = [x.decode() for x in (root/'cmdline').read_bytes().split(b'\0') if x]
paths = []
for i, arg in enumerate(argv[1:], 1):
    if arg == '--config':
        assert i + 1 < len(argv) and argv[i+1] and not argv[i+1].startswith('--'), 'STOP malformed config option'
        paths.append(argv[i+1])
    elif arg.startswith('--config='):
        value = arg.split('=', 1)[1]
        assert value, 'STOP empty config option'
        paths.append(value)
assert len(paths) <= 1, 'STOP ambiguous config options'
selected = env.get(b'SOLANA_COPY_BOT_CONFIG')
assert selected is not None or len(paths) == 1, 'STOP unresolved explicit config path'
path = pathlib.Path(selected.decode() if selected is not None else paths[0])
if not path.is_absolute(): path = (root/'cwd').resolve() / path
d = tomllib.loads(path.read_text()); e = d.get('execution', {})
out = {'config_path': str(path.resolve())}
for key, name in [('enabled', b'SOLANA_COPY_BOT_EXECUTION_ENABLED'),
                  ('canary_tiny_submit_enabled', b'SOLANA_COPY_BOT_EXECUTION_CANARY_TINY_SUBMIT_ENABLED')]:
    value = e.get(key, False)
    if name in env:
        raw = env[name].decode().strip().lower()
        assert raw in ('0','false','no','off','1','true','yes','on'), 'STOP invalid boolean'
        value = raw in ('1','true','yes','on')
    assert isinstance(value, bool)
    out[key] = value
out['tiny_experiment.activate'] = e.get('tiny_experiment', {}).get('activate', False)
assert isinstance(out['tiny_experiment.activate'], bool)
print(json.dumps(out, sort_keys=True))
PY
```

Это ограниченный source-based resolver трёх flags текущего процесса, не запуск
Rust loader, не full config validation и не proof будущего restart. При wrapper,
неоднозначных argv/path/encoding или изменённом после startup config — Unknown/STOP.
Для intended fail-closed installation все три поля должны быть false после полного
разбора prospective systemd overrides; потом подтвердить на новом PID. Hash config
до/после чтения должен совпадать; service не должен сменить PID во время inventory.

## SQLite read-only, bounded, без repair

Сначала выполнить schema query из MIGRATION_MANIFEST и сравнить с bundle. Затем
в одной read transaction получить bounded summaries (без wallet/URL/key values):

```sql
SELECT status, COUNT(*) FROM orders GROUP BY status;
SELECT state, COUNT(*) FROM positions GROUP BY state;
SELECT COUNT(*) AS unresolved_dispatch FROM execution_canary_unresolved_dispatch;
SELECT COUNT(*) AS receipt_facts FROM execution_canary_receipt_facts;
SELECT state, COUNT(*) FROM execution_tiny_experiment GROUP BY state;
SELECT COUNT(*) AS unresolved_reservations
FROM execution_tiny_reservations WHERE actual_fee IS NULL;
```

Connection `mode=ro`, `PRAGMA query_only=ON`, no immutable=1 for live WAL, busy
timeout≤2s и total deadline≤15s. Schema-dependent missing table — Unknown,
не zero. Не создавать missing schema, не checkpoint/VACUUM/backfill.
Counts — только triage, не canonical ownership proof. При pending/OPEN/unknown
сохранить private exact order/signature/receipt/position bindings и провести
существующую canonical read-only reconciliation перед выбором stop/restore.
Не прекращать историю или holds ради технической установки.

## Rollback decision до install

Binary `--rollback` переключает installed package и migration links, но SQLite
schema не возвращает. Actual previous binary/schema compatibility Unknown без
actual previous version. Upgrade evidence127 не доказывает backward compatibility.
Если нет конкретного proof previous consumer на prospective schema, install
останавливается до отдельно согласованного pretrade restore/recovery решения.

Consistent DB backup должен включать WAL-committed state через SQLite backup API
или согласованный stopped-writer backup; plain cp live.db недостаточен. Зафиксировать
backup ID, SHA256/size, schema list, financial identity/readback, config/env/unit
private backup hashes, source DB identity, UTC и перечень остановленных writers.
Проверить integrity на disposable backup copy и достаточное disk headroom.
При любом изменении financial records после backup старую DB не накатывать:
сохранить unknown/receipts/positions и перейти к отдельной recovery/reconciliation.
