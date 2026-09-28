# Run15 transport repair: CI followup

## Что остановило выпуск

Accepted transport diff `ebcbbee9f12a95d44c3f78696dc0657a35d3052f` ещё не установлен
как новый daemon. Artifact run36468562818 завершился exit101:1207 тестов PASS,
четыре FAILED,50 ignored. Все четыре ошибки — `replay_scope_invalid` в существующих
cohort daemon tests. Новый validator ошибочно требовал непустую Raydium partition
и непустую PumpSwap partition одновременно. Это регрессия проверки валидного
single-family config; её не покрывал новый mixed-family локальный сценарий.

Core-types validator теперь допускает пустую family partition, сохраняя её
точную identity, sort/unique/count/string bounds. Wallets и interested programs
остаются обязательными. Scope equality, SQLite ownership и все trade gates
сохраняются. Financial DB не исправлялась вручную.

Новые boundary controls3/3 PASS; четыре реально упавших daemon controls4/4 PASS
локально после app debug compile11.55с. Прежний независимый full-family money
proof применим: его вход/семантика не менялись. Новый matching release ещё нужен.

## Storage CI

Run36468542693: все189 test blocks,752PASS/0FAIL/8ignored; compile64с,
полный test step7м20с. Job cancelled по10-minute timeout при post-test tar/zstd
сохранении5589855359-byte cache. Отказ и оригинальные логи сохранены.

Workflow использует `actions/cache/restore@v4` с теми же paths/key/restore-prefix.
Он переиспользует кэш и завершает job после обязательных tests, без post-save.
Timeout10мин, unconditional full locked package и failure propagation сохранены.
Независимый contract6/6PASS; реальный shell mock сохраняет Cargo exit0/37/101
и отсутствие executable127. Официальный v4 action не содержит post/save hook:
[actions/cache restore](https://github.com/actions/cache/blob/v4/restore/action.yml).

Новый hosted gate требуется до объявления CI зелёным. Build target остаётся
copybot-app/release; workspace suite локально не запускалась. Новых dependencies,
платных stream/RPC, финансовой активации, реальных подписей/отправок0.

## Доказательства и следующий шаг

Private `run15-stream-repair-01/checks`: исходные CI logs, scope/app targeted tests,
workflow controls и причины подготовки. CLI summaries потеряли части logs с ANSI;
полный direct job log сохранён отдельно и прочитан как данные.

Независимый review validation и workflow принят; публикация узкого followup →
успешный matching artifact → остановленная установка → installed network-none
preflight → отдельное решение владельца о read-only stream probe.

## Повторный timeout и cache-only исправление

Run36471043876 снова CANCELLED: unpack того же5589855359-byte target cache
занял3м24с, после него thin storage recompile1м38с. До timeout575PASS/0FAIL;
остальные tests не завершены. Это не полный PASS. Старый меньший cache уже
недоступен. Workflow теперь восстанавливает только cargo registry/git в новом
namespace `cargo-storage-registry`; key/prefix не допускают старый target archive.
Первый hosted run будет cache miss/cold thin target. Полный locked storage gate,
отдельный target, failure propagation и10мин сохранены. Новый gate требуется.
Независимый concrete workflow controls6/6PASS; runtime/dependencies не менялись.

Appartifact fd4c86714525da04fe8415c2e4772203f10e1507 run36471088698 SUCCESS,
release compile74с. Новый workflow-only commit не меняет runtime/migrations/lock: 
matching fd4 artifact переиспользуется, новая app сборка не нужна. Проверены
archive/binary hashes, clean manifest/полный bin set/89 migrations; остановленная
установка в отдельный read-only probe сохраняет e7 rollback. READY ещё не выдан.
