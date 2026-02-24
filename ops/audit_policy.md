# Audit Policy

## Goal
Reduce feedback loop time for small changes while keeping phase-close quality gates strict.

## Modes

### 1) Quick Audit
Use for:
- Small fixes (roughly up to 200 LOC)
- Docs/runbook edits
- Guard/message tweaks with no cross-crate behavior change

Command:

```bash
bash tools/audit_quick.sh
```

Runs:
- `cargo test -p copybot-executor -q`
- `bash tools/executor_contract_smoke_test.sh`

### 2) Standard Audit
Use for:
- Auth / route / submit / simulate logic
- Cross-file changes that are still limited in scope

Command:

```bash
bash tools/audit_standard.sh [<git-diff-range>]
```

Examples:

```bash
bash tools/audit_standard.sh HEAD~1..HEAD
bash tools/audit_standard.sh origin/main..HEAD
```

Runs:
- Quick audit
- `cargo test -p <changed-crate> -q` for crates touched in the diff
- `tools/ops_scripts_smoke_test.sh` only when operational scripts/docs are touched (`tools/`, `ops/`, `README.md`, `ROAD_TO_PRODUCTION.md`)

### 3) Full Audit
Use only for:
- Phase close
- Merge to `main`
- Rollout/signoff tags

Command:

```bash
bash tools/audit_full.sh
```

Runs:
- Quick audit
- `cargo test --workspace -q`
- full `tools/ops_scripts_smoke_test.sh`

## Anti-duplication Rules
1. Do not run full audit for every small commit.
2. If the same SHA already has fresh PASS artifacts from the implementer, auditor does spot-check + diff review instead of rerunning full-suite.
3. Full audit is required once per phase-close/signoff boundary, not per slice commit.
4. Heavy regressions should run in CI/nightly; local audit should stay mode-appropriate.

