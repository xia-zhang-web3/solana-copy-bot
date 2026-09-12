# №129: fail-closed configuration proposal

Actual server config неизвестен; это reviewable proposal, не current acceptance.
Working configs, EnvironmentFile, unit/drop-ins, DB и credentials не изменены.

| Known repository template | Proposal | Future deployment binding |
|---|---|---|
| configs/live.toml | fail-closed.delta.toml | только если выбран actual --config/env path |
| configs/prod.toml | fail-closed.delta.toml | только если выбран actual --config/env path |
| ops/server_templates/live.server.toml.example | live.server.fail-closed.toml.example | template /etc/solana-copy-bot/live.server.toml |
| ops/server_templates/app.env.example | app.fail-closed.env.example | template /etc/solana-copy-bot/app.env |
| ops/server_templates/solana-copy-bot.service | без unit delta | template solana-copy-bot.service; actual name неизвестен |

Все names/paths справа — bindings из repo template, не обнаруженные server facts.
Полный proposed TOML получен из server template с единственными semantic additions:
`execution.canary_tiny_submit_enabled=false` и
`execution.tiny_experiment.activate=false`; `execution.enabled=false` уже был.
Provider placeholders в template остаются placeholders, не credentials.

Delta TOML — инструкция semantic merge, не отдельный полноценный AppConfig и не
файл для blind append. В actual файле изменить существующие keys/table, добавить
table лишь при её отсутствии, затем parse и сравнить весь semantic delta.
Нельзя заменить actual server config repo template: потеряются настоящие paths,
identity и параметры. `tiny_experiment` вложен в `[execution.tiny_experiment]`;
root `[tiny_experiment]` — неверное место для этого loader.

Требуемый итог после ВСЕХ overrides:

```text
execution.enabled = false
execution.canary_tiny_submit_enabled = false
execution.tiny_experiment.activate = false
```

Все omitted fields сохраняются. Existing experiment id/policy_mode/wallet/state,
deadline, B/F, reservations, Unknown holds и финансовая история не сбрасываются.
Не добавлять synthetic B/F/ID128; не выбирать новый ID/режим ради включения.
Пример без id применяется только к repo template без существующей identity.
Actual id сохраняется даже при `activate=false`; SQL rows не изменяются.

## Loader и systemd precedence

`app_main.rs` передаёт CLI `--config` как default path; `loader.rs` сначала выбирает
`SOLANA_COPY_BOT_CONFIG`, если он задан, и только иначе CLI/default path. Затем
`loader_env.rs`/`loader_discovery_shadow_env.rs` применяют env overrides, потом
normalization/validation. Следовательно, сам `--config` не гарантирует выбранный файл.

Точные boolean overrides:

```text
SOLANA_COPY_BOT_EXECUTION_ENABLED=false
SOLANA_COPY_BOT_EXECUTION_CANARY_TINY_SUBMIT_ENABLED=false
```

`parse_env_bool` понимает 0/false/no/off и 1/true/yes/on с trim/lowercase;
proposal использует только literal false. Invalid value — RED, не default false.
Env override для tiny_experiment.activate отсутствует в existing loader; менять
именно вложенный TOML key. Не придумывать новое env имя.

Repo service: WorkingDirectory=/var/www/solana-copy-bot,
EnvironmentFile=/etc/solana-copy-bot/app.env,
ExecStart=/var/www/solana-copy-bot/bin/copybot-app --config
/etc/solana-copy-bot/live.server.toml. Actual unit, drop-ins, EnvironmentFile order,
manager environment/PassEnvironment/UnsetEnvironment, wrapper и argv требуют
будущего read-only preflight. Drop-in Environment= сам по себе не доказывает
победу над EnvironmentFile; очистить противоречия в конкретном accepted proposal.

Acceptance note перед restart: дата/оператор, exact artifact SHA/id, actual unit,
config/env/drop-in paths и hashes, diff только трёх flags, resolved config path,
результат bool resolution и unchanged identity/state. Values остальных env/URLs/
keys не печатать. Running process env доказывает только текущий процесс; будущая
startup environment проверяется отдельно после согласованного изменения источников,
затем при postflight нового PID. Unknown precedence/path — STOP до запуска.

`durable_association_v1` по `config/association_delivery.rs` всё ещё требует оба
execution flags=false. Technical installation не закрывает execution bridge и
не разрешает сменить legacy/durable режим. Future activation описана отдельно.
