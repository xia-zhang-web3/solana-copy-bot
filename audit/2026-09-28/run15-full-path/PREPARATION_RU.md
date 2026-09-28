# Run15: подготовка полного Raydium BUY→SELL

Дата: 2026-09-28. Исходный commit: 53203c3ddf242c55e713fc2d5471ab8f90dd916a.

## Решение и критерий

Вопрос: устраняются ли доказанные несовместимости proto/config, mixed prefix,
AMM v4 source SELL и follower BUY anchor штатным путём daemon?
Приёмка требует causal offline PASS, независимого review, exact-SHA app release
и остановленной установки с локальным preflight. Полный локальный сценарий PASS;
независимый повтор PASS92.20с; оба scoped review приняты, artifact/install PENDING.
Финансовая активация отдельно за владельцем. [Money review](INDEPENDENT_REVIEW_RU.md),
[version review](VERSION_REVIEW_RU.md).

## Узкий ремонт

- Yellowstone proto 12.6 сохраняет Message.config на tonic decode boundary.
  Config любого version tag не становится Admission; parent/index/control события
  сохраняются, посторонний config не прекращает сеанс.
- Prefix legacy/v0/v1 metadata влияет на inventory только по проверенным raw
  balances и ownership. Неизвестное влияние явно отказывает; v1 не разрешён
  как финансовый target или sending format.
- Direct AMM v4 source proof сверяет ABI, два classic SPL CPI, user/vault owners,
  mint, decimals и exact raw effects. BUY anchor поддерживает ограниченный
  one-hop Jupiter→Raydium; несовместимый follower route отказывает до отправки.
- Полный сценарий выявил дополнительный MissingExactAmounts у wrapped follower
BUY: native cash fallback не доказывает сумму обмена. Узкий exact attribution
по CPI/lifecycle/balances исправлен и проверен. Новый target ATA требует отдельного
  полного создания/ownership/rent witness; Pump path не меняется. Admission вручную
  не дополняется.

## Данные и модели

Реальный источник: 7EoQc9N9QrGMf6JR2j8yy4rmY1ZsexPuAXoTCQtDDbSF.
Mint: DVb1znJKBVJzcuzbgvcG3cSghf2i1YzdJqoJFb7ZdQuX, classic, decimals9.
BUY response727: 6120191lamports→163956354raw, slot451302689.
SELL response725: 264731434raw→9832310lamports, slot451313058/index775.
Подписи, слоты, raw sums и meta err сохранены. Перед SELL находятся133 config;
в целом блоке143. Источник был свежим на фиксации12:05:57.965UTC; это не
предсказание будущей активности и не полный roundtrip.

Follower с новым target ATA, complete whole-wallet parent pages, gap-spanning parent headers и
SELL external execution boundary — модели. Whole-wallet D6770149697 не является
собранным историческим фактом, хотя prebalance выбранного source account равен
этому числу. Follower H267894178 следует сохранённой цене BUY; planned model
SELL floor(H×264731434/D)=10475397, residual257418781. Это partial, не CLOSED.

Replay ускоренный с логическим темпом2.5blocks/s. Никакого четырёхчасового live
stream здесь нет. Simulation/send отдельно ждут новый persisted parent commit
после durable handoff. UNKNOWN не освобождает ownership; restart не создаёт send2.

## Проверки и ограничения подготовки

Локальные targets: copybot-ingestion/lib, copybot-storage-core/lib,
copybot-app/bin copybot-app, test/debug, общий существующий cache.
Deployable target: copybot-app/release, GitHub Actions, только после scoped review.
Никакой workspace suite или production local release не запускался.

Принятые version controls3+tonic1, storage4+savedprefix1, app proof5,
Jupiter decoder4 (8 положительных вариантов и отрицательные границы),
source/carryover helpers5 и architecture regression controls2+existing6 прошли.
Полный causal сценарий PASS за89.83с: BUY1/SELL1, partial residual257418781,
143 config/0 v1 Admissions, continued parent commits, UNKNOWN→restart→settlement
без повторной отправки. Независимое повторение PASS92.20с отдельным evidence root.

Тестовый executable SHA2567c3a1b9daf73d49d4e8707fd0dd95e239a7a6d8559bd6e109739f98e9fbac0e0.
В обычном debug repeated10k-parent snapshots исчерпали неизменённый quoteTTL5с.
Причина и замеры сохранены; test/debug opt-level2 только storage-core/core-types/
serde_json/rusqlite позволил пройти causal сценарий. Production profiles и TTL
не менялись; это не обещание live performance. BUY decomposition остаётся
Unresolved/legacy_unclassified; модельный partial cash settlement не доказательство
раздельного исторического BUY P/L. Test-only jsonParsed projection доказан отдельным
контролем raw/missing-init/wrong-owner; production receipt policy не ослаблялась.

Причины местных ошибок сохранены в private run15-full-path-prep: config shape,
price/slippage model, protected floor, неверная replay cadence и MissingExactAmounts.
Исправления модели не ослабляют production gates.

Run15 STOP; финальная DB пуста, authority/clocks не активированы.
Cumulative779HTTP/15870CU/8331750nanoUSD и stream59581108989bytes/62connections
перенесены штатным carryover helper. Новые provider/stream/trade calls0.
BUY≤0.01SOL×1, sourceSELL≤1, до4ч, общий provider ceiling$50; slippage50bps,
priority≤50000lamports, loss≤0.02SOL, native floor≥160200031, submit attempts1.
Первый принятый BUY candidate расходует слот, даже если следующий gate откажет.
Source fractional SELL может оставить позицию; forced exit не разрешён.

## Зависимости и размер

Proto12.0→12.6 и согласованный operator pin необходимы до потери config marker.
Client остаётся12.0. Anyhow1.0.101→1.0.104 требуется build-dependency нового proto.
Точечный lock добавляет31transitive entries, из них9active normal packages,
22optional/cross-target. App normal dependency declarations не добавлены;
точные prost/proto dev pins повторно используют имеющийся graph для wire test.
Architecture helper теперь отличает external cfg(test) module от inline body;
отрицательный inline control сохранён. Новые production/test modules малы,
oversize waiver0. Architecture --changed64files и --all PASS, git diff --check PASS.
New production modules≤298lines, causal test345, projection125+tests66,
app_tests726≤800. Подробный line-count inventory сохранён в private evidence;
в текущем source/test/tool delta55files; точный commit stat после приёмки.

## Исправление artifact CI

Первый exact-SHA run36436898783: app gate1210PASS/1FAIL/49ignored за408.54с;
release build не начался. Storage Semantic и Architecture Guard прошли.
Отказ `fraction_prefix_owned_metadata_missing` обнаружил неполный старый synthetic
prefix: второй source account writable, но без неизменных pre/post30000raw.
Fixture дополнен этими metadata; production guard не изменён. Сохраняются
denominator40001 и raw249; focused PASS0.18с, warm test/debug build17.20с.
Причина и полные job logs сохранены private. GitHub CLI cached partial log
не содержал отказ; raw job log получен с разрешением ANSI, вывод очищен локально.
