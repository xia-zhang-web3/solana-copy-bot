# Закрытые эксперименты: исторические команды

Обновлено: 2026-09-05. Основные отчёты и команды находятся в
[рабочем runbook](../../../../LIVE_CANARY_REPORT_RUNBOOK.md).

Эти три CLI исключены из обычной сборки и production artifact
`copybot-operators`. Код расчётов и regression tests сохранены, чтобы можно было
воспроизвести отрицательные результаты, а не повторять те же эксперименты.

| Команда | Почему убрана из обычного набора |
|---|---|
| `copybot_exit_policy_sim` | Observed-price модель завышала эффект раннего выхода; blind 30m exit получил NO-GO |
| `copybot_exit_policy_shadow_quote_report` | Executable diagnostic той же закрытой exit-policy гипотезы |
| `copybot_entry_side_filter_backtest` | Track-A reconstructable entry filters закрыты с отрицательным результатом |

Основания: разделы Blind 30m Exit Backstop, Conditional Price-Decay Exit и
Track-A Reconstructable Entry Filters в
[STRATEGY_EXPERIMENT_LEDGER.md](../../../../STRATEGY_EXPERIMENT_LEDGER.md).
Новая гипотеза требует новых данных; наличие старой команды её не открывает.

Для проверки доступности исторического entrypoint на локальной машине:

```bash
cargo check --locked -p copybot-operators --features legacy-reports \
  --bin copybot_exit_policy_sim \
  --bin copybot_exit_policy_shadow_quote_report \
  --bin copybot_entry_side_filter_backtest
```

Для воспроизведения использовать `cargo run --locked -p copybot-operators
--features legacy-reports --bin <имя> -- <аргументы исходного эксперимента>`
с соответствующим snapshot БД. CLI аргументы смотреть в сохранённом исходнике
и evidence эксперимента; эти отчёты не реализуют общий `--help`.

`tools/package_bins.py` и штатный artifact builder намеренно выбирают только
default features. Отдельный production artifact для этих экспериментов не введён.
Исторические JSON, audit evidence и старые установленные release manifests
сохранены. Уже собранные локальные binaries сами не удаляются.
