# Запрос владельцу — черновик для куратора, не отправлен

После независимой приёмки №129 прошу разрешить один этап: commit/push ровно
reviewed release candidate и manual CI `operator-artifacts.yml`, package
`copybot-app`, bin `copybot-app`, profile `release`, target
`x86_64-unknown-linux-gnu`. Сейчас публикация запрещена; кодер этот запрос не отправляет.

Основание: accepted127 canonical source2395/code1384/4absences; source0 PASS128;
готовый full content tree, точный staging manifest и diff №129. Новый runtime/
strategy/schema/dependency delta относительно127 отсутствует. Proposed additions
— correction ARTIFACT_DEPLOY и отдельный fail-closed config/runbook proposal.
Три false flags относятся к proposal, а не неизвестной текущей server config.

Review attachments во внешней private evidence directory №129:

- RELEASE_CANDIDATE_MANIFEST.json: полный tree manifest, режимы, SHA256 и provenance;
- STAGING_FILES.tsv и STAGING_PATHS.zlist: точный A/M/D file list относительно
  действительного parent 7c7236755c9e2055718ef33091eb589f9bb8b301;
- RELEASE_CANDIDATE.patch: полный future commit diff от этого parent tree;
- PREPARATION.patch: только №129 поверх accepted source/current policy baseline;
- SOURCE_BASE.json, POLICY_CONTEXT.json, LOCAL_CHECKS.json, PATCH_ROUNDTRIP.json.

Полный tree не включает scratch/targets/keys/temporary128/evidence archives/seals
или четыре curator operational docs. Existing reviewed bank-harness source23/24
из canonical127 сохраняется как source fixtures; его targets/evidence не включены.
Tracked пустой recovery_tmp_check.db исключён из candidate; main файл сохранён.
Unchanged tracked support/config/UI files перенесены из named parent с hashes.
AGENTS/BUILD_POLICY и остальные architecture context docs перенесены из текущего
policy без новых правок129; это отдельно обозначено в manifest, не новая policy.
Unrelated dirty README/LIVE_CANARY_REPORT_RUNBOOK в commit не захватываются.

Ref/remote/repository задаёт владелец: имена branch не придуманы. Будущий commit
SHA появится только после разрешённого commit. Approved content tree → actual
commit SHA → run.head_sha → checksummed artifact manifest фиксируются последовательно.

## Точные будущие staging/commit/push команды

Выполнять только после отдельного разрешения владельца и независимого review.
`PUBLISH_DIR` — новый isolated clean checkout, не dirty main; `HANDOFF_DIR` —
эта verified private evidence directory/её byte-identical передача вне checkout.
`REMOTE`, `PUSH_DESTINATION` (полный remote ref), `APPROVED_REF` (соответствующий
workflow ref) и `REPOSITORY` — owner inputs. При remote divergence не discard/force:
остановиться для integration с новым reviewed tree/manifest.

```bash
set -euo pipefail
: "${PUBLISH_DIR:?fresh isolated checkout directory}"
: "${HANDOFF_DIR:?verified batch129 evidence directory}"
: "${REMOTE:?owner-selected Git remote}"
: "${PUSH_DESTINATION:?owner-selected full remote ref}"
: "${APPROVED_REF:?corresponding workflow ref}"
BASE_SHA=7c7236755c9e2055718ef33091eb589f9bb8b301
APPROVED_TREE="$(python3 - "$HANDOFF_DIR/RELEASE_CANDIDATE_MANIFEST.json" <<'PY'
import json, sys
print(json.load(open(sys.argv[1]))['candidate_tree'])
PY
)"
git worktree add --detach "$PUBLISH_DIR" "$BASE_SHA"
test "$(git -C "$PUBLISH_DIR" rev-parse HEAD)" = "$BASE_SHA"
git -C "$PUBLISH_DIR" diff --exit-code
git -C "$PUBLISH_DIR" diff --cached --exit-code
test -z "$(git -C "$PUBLISH_DIR" ls-files --others --exclude-standard)"
git -C "$PUBLISH_DIR" apply --check "$HANDOFF_DIR/RELEASE_CANDIDATE.patch"
git -C "$PUBLISH_DIR" apply --index "$HANDOFF_DIR/RELEASE_CANDIDATE.patch"
python3 - "$PUBLISH_DIR" "$HANDOFF_DIR/STAGING_PATHS.zlist" <<'PY'
import pathlib, subprocess, sys
got = subprocess.check_output(['git','-C',sys.argv[1],'diff','--cached',
                              '--no-renames','--name-only','-z'])
want = pathlib.Path(sys.argv[2]).read_bytes()
assert sorted(got.split(b'\0')) == sorted(want.split(b'\0'))
PY
test "$(git -C "$PUBLISH_DIR" write-tree)" = "$APPROVED_TREE"
git -C "$PUBLISH_DIR" diff --cached --check
git -C "$PUBLISH_DIR" commit -m "Prepare fail-closed release"
APPROVED_SHA="$(git -C "$PUBLISH_DIR" rev-parse HEAD)"
test "$(git -C "$PUBLISH_DIR" rev-parse HEAD^{tree})" = "$APPROVED_TREE"
git -C "$PUBLISH_DIR" diff --exit-code
git -C "$PUBLISH_DIR" diff --cached --exit-code
test -z "$(git -C "$PUBLISH_DIR" ls-files --others --exclude-standard)"
git -C "$PUBLISH_DIR" push "$REMOTE" "HEAD:$PUSH_DESTINATION"
```

`git apply --index` staging ограничен проверенным patch; не `git add` всего dirty.
Перед push обязательна owner проверка destination/ref и remote ancestry; никаких
force pushes. После push перейти в этот clean checkout и выполнить app-only manual
dispatch/download/verification по BUILD_AND_PUBLISH_PLAN; tree совпадение проверить
ещё раз. Hooks не должны изменить approved tree: mismatch — STOP до push.

Optional125: если владелец отдельно заявляет необходимость уже принятых отчётов,
добавить только второй package `copybot-operators` на тот же approved commit,
profile operator-release и полный default набор10. Это самостоятельный scope,
не новая приёмка125 и не обязательное условие app CI.

**SSH/server inventory/install/restart/trading не входят в это разрешение.**
После настоящего matching CI artifact следующий отдельный этап — bounded server
preflight, заполненные actual inputs и конкретное install/rollback решение.
Future tiny activation и financial prerequisites остаются отдельными.
