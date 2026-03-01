# Executor Phase 2B — Slice 202

Date: 2026-02-28

## Scope

Added manifest checksum parity output for helper scripts that already write artifact manifests:

- `tools/executor_preflight.sh`
- `tools/executor_signer_rotation_report.sh`
- `tools/adapter_secret_rotation_report.sh`

## Changes

1. Each helper now computes and emits `manifest_sha256` whenever `OUTPUT_DIR` is set.
2. `tools/ops_scripts_smoke_test.sh` coverage updated to assert:
   - `manifest_sha256` field format is valid sha256
   - `manifest_sha256` matches on-disk `artifact_manifest`
   - existing report/summary hash fields still match their artifact files

## Verification

- `bash -n tools/executor_preflight.sh tools/executor_signer_rotation_report.sh tools/adapter_secret_rotation_report.sh tools/ops_scripts_smoke_test.sh` — PASS
- `cargo check -p copybot-executor -q` — PASS
- Targeted helper checks:
  - executor preflight artifact export includes `manifest_sha256` and matches file
  - adapter secret rotation artifact export includes `manifest_sha256` and matches file
  - executor signer rotation artifact export includes `manifest_sha256` and matches file

Note: full `tools/ops_scripts_smoke_test.sh` remains blocked in this environment by a pre-existing `/var` vs `/private/var` expectation in an unrelated evidence-bundle path assertion.
