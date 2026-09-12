# №129: build and publish plan — draft, not executed

Следующее отдельное разрешение: commit/push exact reviewed candidate и manual CI
только app. Здесь build/CI/network/commit/push0. Release artifact ещё не существует.
Ref, repository, remote и будущий commit SHA — owner/preflight inputs; новый branch
не придумывается. Immutable content tree указан во внешнем RELEASE_CANDIDATE_MANIFEST.

| Package | Binaries | Profile | Target | Scope |
|---|---|---|---|---|
| copybot-app | copybot-app | release | x86_64-unknown-linux-gnu | обязательный |
| copybot-operators | весь default набор10 ниже | operator-release | x86_64-unknown-linux-gnu | отдельный optional125 |

Optional125 нужен только при заявленной необходимости уже принятых receipt-based
отчётов. Изменённые125 consumers: `copybot_execution_canary_quote_pnl` и
`copybot_execution_tiny_economics`. Новая приёмка125 не нужна. App artifact не
обновляет operators; operators не блокирует app. Другие packages вне scope.

Default operators, точно Cargo/default features и `tools/package_bins.py`:

```text
copybot_execution_ata_sweep
copybot_execution_canary_quote_pnl
copybot_execution_canary_readiness
copybot_execution_tiny_economics
copybot_execution_tiny_writeoff
copybot_first_sell_full_exit_report
copybot_leader_copyability_report
copybot_track_b_entry_quote_report
copybot_universe_de_risk_report
copybot_yellowstone_source_probe
```

`legacy-reports` не включать. Исключены `copybot_entry_side_filter_backtest`,
`copybot_exit_policy_shadow_quote_report`, `copybot_exit_policy_sim`.
Partial package artifacts запрещены; наличие финансовых operator binaries в
полном package не разрешает их запуск. Operator-only install/revert без daemon restart.

## Manual dispatch после разрешённых commit/push

Все shell-блоки — будущие команды, сейчас не исполнялись. Начать в clean checkout
реального approved commit; inputs задать из решения владельца и результата commit.

```bash
set -euo pipefail
: "${REPOSITORY:?owner supplies OWNER/REPO}"
: "${APPROVED_REF:?owner supplies existing pushed ref}"
: "${APPROVED_SHA:?real approved commit SHA after commit}"
: "${APPROVED_TREE:?reviewed full content tree from manifest}"
test "$(git rev-parse HEAD)" = "$APPROVED_SHA"
test "$(git rev-parse HEAD^{tree})" = "$APPROVED_TREE"
git diff --exit-code
git diff --cached --exit-code
test -z "$(git ls-files --others --exclude-standard)"
gh workflow run operator-artifacts.yml --repo "$REPOSITORY" \
  --ref "$APPROVED_REF" -f package=copybot-app
```

Ровно один selected package в dispatch. Push/PR этот workflow не запускают;
matrix отсутствует. Если optional125 отдельно включён владельцем, второй dispatch
на тот же approved ref/SHA (не запускать этот блок для app-only scope):

```bash
gh workflow run operator-artifacts.yml --repo "$REPOSITORY" \
  --ref "$APPROVED_REF" -f package=copybot-operators
```

Для каждого выбранного package отдельно установить `PACKAGE`, `PROFILE`, `RUN_ID`.
App: `PACKAGE=copybot-app`, `PROFILE=release`; optional125:
`PACKAGE=copybot-operators`, `PROFILE=operator-release`. Owner выбирает run созданного
dispatch по ID/времени, event/package; не брать просто latest, не повторять dispatch
при сомнении. Получить список и проверить выбранный run:

```bash
gh run list --repo "$REPOSITORY" --workflow operator-artifacts.yml \
  --event workflow_dispatch --commit "$APPROVED_SHA" --limit 20 \
  --json databaseId,headSha,headBranch,event,createdAt,status,conclusion
: "${RUN_ID:?the exact selected dispatch run ID}"
test "$(gh api "repos/$REPOSITORY/actions/runs/$RUN_ID" --jq .head_sha)" = "$APPROVED_SHA"
test "$(gh api "repos/$REPOSITORY/actions/runs/$RUN_ID" --jq .event)" = workflow_dispatch
test "$(gh api "repos/$REPOSITORY/actions/runs/$RUN_ID" --jq .path)" = .github/workflows/operator-artifacts.yml
gh run watch "$RUN_ID" --repo "$REPOSITORY" --exit-status
test "$(gh api "repos/$REPOSITORY/actions/runs/$RUN_ID" --jq .head_sha)" = "$APPROVED_SHA"
test "$(gh api "repos/$REPOSITORY/actions/runs/$RUN_ID" --jq .conclusion)" = success
```

Ref мог сдвинуться: только `run.head_sha == APPROVED_SHA` связывает build с решением.
Первый технический RED прекращает дальнейшие действия, без automatic retry/repair.

## Checks и бюджеты, которые CI обязан выполнить

Workflow сохраняется byte-identical127: `RUN_CHECKS=1`, full default bin set,
`ARCH_GUARD_DIFF_RANGE=HEAD^..HEAD` (empty tree для первого commit), changed/all guards.
Builder определяет наличие lib через locked metadata. App — bin-only, operators — lib.

```text
tools/architecture_guard.sh --changed
tools/architecture_guard.sh --all
cargo test --locked -p copybot-app --bin copybot-app -- --test-threads=1
cargo test --locked -p copybot-operators --lib -- --test-threads=1
cargo test --locked -p copybot-operators --tests -- --test-threads=1
```

Последние две строки относятся только к operators dispatch. 94 scoped127 и local129
не заменяют полного package test profile. Builder использует `cargo build --locked
--profile "$PROFILE" --target x86_64-unknown-linux-gnu -p "$PACKAGE"` с каждым default
`--bin`; запускать только документированный CI `tools/build_operator_artifacts.sh`.
`ALLOW_DIRTY=1`, `RUN_CHECKS=0`, обход проверок, Docker/local release здесь запрещены.

Действующие AGENTS budgets: app cold≤20min/warm≤5min; Discovery/operator
cold≤8min/warm≤90s; storage-only cold≤12min/warm≤2min (не выбран).
CI job timeout30min — аварийный предел job, не новый budget. Измерить actual build
duration/cache state; превышение соответствующего бюджета — architecture RED.
Не увеличивать timeout и не объявлять cached/dev timings release acceptance.
CI дополнительно делает verify, install --dry-run и disposable install/rollback
proof с synthetic previous artifact. Это не совместимость реального server rollback.

## Скачать и проверить matching artifact локально

```bash
: "${PACKAGE:?selected package}"
: "${PROFILE:?matching profile}"
: "${DOWNLOAD_DIR:?fresh absolute directory outside checkout}"
ARTIFACT_ID="$PACKAGE-$APPROVED_SHA"
gh run download "$RUN_ID" --repo "$REPOSITORY" \
  --name "$ARTIFACT_ID-linux-x86_64" --dir "$DOWNLOAD_DIR"
(cd "$DOWNLOAD_DIR" && shasum -a 256 -c "$ARTIFACT_ID.tar.gz.sha256")
: "${EXTRACT_DIR:?fresh absolute directory outside checkout}"
mkdir -p "$EXTRACT_DIR"
tar -xzf "$DOWNLOAD_DIR/$ARTIFACT_ID.tar.gz" -C "$EXTRACT_DIR"
RELEASE_DIR="$EXTRACT_DIR/$ARTIFACT_ID"
(cd "$RELEASE_DIR" && shasum -a 256 -c SHA256SUMS)
python3 tools/verify_operator_artifact.py "$RELEASE_DIR" \
  --expect-package "$PACKAGE" --expect-profile "$PROFILE" \
  --expect-target x86_64-unknown-linux-gnu --enforce-workspace-bin-check
python3 - "$RELEASE_DIR/build-manifest.json" "$APPROVED_SHA" "$ARTIFACT_ID" <<'PY'
import json, sys
m = json.load(open(sys.argv[1]))
assert m['git_sha'] == sys.argv[2] and m['artifact_id'] == sys.argv[3]
assert m['git_dirty'] is False
PY
```

На macOS локальная проверка заканчивается здесь: installer проверяет Linux
host target даже при `--dry-run`. Обязательный dry-run выполняет существующий
Ubuntu CI step `Dry-run operator install`; сохранить его PASS в CI evidence.
Следующий дополнительный блок допустим только на matching Linux x86_64 builder,
не на этом Mac; он не заменяет CI proof и не разрешает доступ к серверу.

```bash
: "${DRY_RUN_INSTALL_DIR:?fresh absolute disposable bin directory}"
mkdir -p "$DRY_RUN_INSTALL_DIR"
tools/install_operator_artifacts.sh --dry-run \
  --expect-package "$PACKAGE" --expect-profile "$PROFILE" \
  --expect-target x86_64-unknown-linux-gnu --install-dir "$DRY_RUN_INSTALL_DIR" \
  "$DOWNLOAD_DIR/$ARTIFACT_ID.tar.gz"
```

Verifier сравнивает exact names/count, package/target/profile, checks, clean SHA,
hashes и app migration SQL membership/bytes с этим approved checkout.
Сохранить run metadata/log, duration/cache state, внешний archive checksum,
build-manifest/SHA256SUMS и app migration_bundle.sha256. Artifact checksum заранее
не вычисляется: он возникнет после настоящего CI. Цепочка доказательства:
reviewed content tree → реальный commit SHA → exact run.head_sha → manifest и
полные binary/bundle checksums. Следующий этап только после этого — отдельный
bounded server preflight и конкретное решение об install/rollback.
