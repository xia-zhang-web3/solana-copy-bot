use anyhow::{anyhow, bail, Context, Result};
use copybot_discovery::{
    DiscoveryService, PersistedRebuildRowCrossInvocationDiffDiagnostic,
    PersistedRebuildRowCrossInvocationDiffReasonClass,
    PersistedRebuildRowCrossInvocationDiffStage, PersistedRebuildRowDriverCompareDiagnostic,
    PersistedRebuildRowSlowEventCaptureDiagnostic,
    PersistedRebuildRowStepMetaIsolatedSharedDiagnostic,
    PersistedRebuildRowStepMetaFullContextDiffDiagnostic,
    PersistedRebuildRowStepMetaFullOrchestrationDiffDiagnostic,
    PersistedRebuildRowSharedOrderingDiffDiagnostic,
    PersistedRebuildRowSharedPathDiffDiagnostic,
    PersistedRebuildRowSharedSequenceCompareDiagnostic,
    PersistedRebuildRowStepMetaCompareDiagnostic, RawPersistedRebuildRowProbeDiagnostic,
    ReplayCheckpointRuntimeDbDiagnostic, ReplayCheckpointRuntimeDbOnlyMode,
};
use copybot_storage::SqliteStore;
use serde_json::{Map, Value};
use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

const DEFAULT_RUNTIME_DB_DIAGNOSTIC_BUDGET_MS: u64 = 30_000;
const DEFAULT_SLOW_EVENT_CAPTURE_THRESHOLD_MS: u64 = 1_000;
const USAGE: &str = "usage: discovery_replay_checkpoint_diagnose [--inspect-persisted-rebuild-row-meta-lite | --inspect-persisted-rebuild-row-meta | --inspect-persisted-rebuild-state | --explain-publishable-checkpoint-blocker | --compare-sol-leg-source-vs-checkpoint | --explain-replay-sol-leg-incomplete | --probe-persisted-rebuild-row-raw | --probe-persisted-rebuild-row-driver-compare | --probe-persisted-rebuild-row-step-meta-detail | --probe-persisted-rebuild-row-shared-sequence-detail | --probe-persisted-rebuild-row-shared-path-diff | --probe-persisted-rebuild-row-step-meta-full-context-diff | --probe-persisted-rebuild-row-step-meta-full-orchestration-diff | --probe-persisted-rebuild-row-shared-ordering-diff | --probe-persisted-rebuild-row-cross-invocation-diff | --capture-persisted-rebuild-row-slow-event] --runtime-db <path> [--recent-raw-db <path>] [--budget-ms <ms>] [--threshold-ms <ms>] [--json]";

fn main() -> Result<()> {
    let Some(config) = parse_args()? else {
        println!("{USAGE}");
        return Ok(());
    };
    let output = run(config)?;
    println!("{output}");
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    InspectPersistedRebuildRowMetaLite,
    InspectPersistedRebuildRowMeta,
    InspectPersistedRebuildState,
    ExplainPublishableCheckpointBlocker,
    CompareSolLegSourceVsCheckpoint,
    ExplainReplaySolLegIncomplete,
    ProbePersistedRebuildRowRaw,
    ProbePersistedRebuildRowDriverCompare,
    ProbePersistedRebuildRowStepMetaDetail,
    ProbePersistedRebuildRowSharedSequenceDetail,
    ProbePersistedRebuildRowSharedPathDiff,
    ProbePersistedRebuildRowStepMetaFullContextDiff,
    ProbePersistedRebuildRowStepMetaFullOrchestrationDiff,
    ProbePersistedRebuildRowSharedOrderingDiff,
    ProbePersistedRebuildRowCrossInvocationDiff,
    CapturePersistedRebuildRowSlowEvent,
    ProbePersistedRebuildRowStepMetaIsolatedSharedInternal,
}

#[derive(Debug, Clone)]
struct Config {
    mode: Mode,
    runtime_db: PathBuf,
    recent_raw_db: Option<PathBuf>,
    json: bool,
    budget_ms: u64,
    threshold_ms: u64,
}

fn parse_args() -> Result<Option<Config>> {
    parse_args_from(env::args().skip(1))
}

fn parse_args_from<I>(args: I) -> Result<Option<Config>>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut mode: Option<Mode> = None;
    let mut runtime_db: Option<PathBuf> = None;
    let mut recent_raw_db: Option<PathBuf> = None;
    let mut json = false;
    let mut budget_ms = DEFAULT_RUNTIME_DB_DIAGNOSTIC_BUDGET_MS;
    let mut threshold_ms = DEFAULT_SLOW_EVENT_CAPTURE_THRESHOLD_MS;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--inspect-persisted-rebuild-row-meta-lite" => set_mode(
                &mut mode,
                Mode::InspectPersistedRebuildRowMetaLite,
                arg.as_str(),
            )?,
            "--inspect-persisted-rebuild-row-meta" => set_mode(
                &mut mode,
                Mode::InspectPersistedRebuildRowMeta,
                arg.as_str(),
            )?,
            "--inspect-persisted-rebuild-state" => {
                set_mode(&mut mode, Mode::InspectPersistedRebuildState, arg.as_str())?
            }
            "--explain-publishable-checkpoint-blocker" => set_mode(
                &mut mode,
                Mode::ExplainPublishableCheckpointBlocker,
                arg.as_str(),
            )?,
            "--compare-sol-leg-source-vs-checkpoint" => set_mode(
                &mut mode,
                Mode::CompareSolLegSourceVsCheckpoint,
                arg.as_str(),
            )?,
            "--explain-replay-sol-leg-incomplete" => {
                set_mode(&mut mode, Mode::ExplainReplaySolLegIncomplete, arg.as_str())?
            }
            "--probe-persisted-rebuild-row-raw" => {
                set_mode(&mut mode, Mode::ProbePersistedRebuildRowRaw, arg.as_str())?
            }
            "--probe-persisted-rebuild-row-driver-compare" => set_mode(
                &mut mode,
                Mode::ProbePersistedRebuildRowDriverCompare,
                arg.as_str(),
            )?,
            "--probe-persisted-rebuild-row-step-meta-detail" => set_mode(
                &mut mode,
                Mode::ProbePersistedRebuildRowStepMetaDetail,
                arg.as_str(),
            )?,
            "--probe-persisted-rebuild-row-shared-sequence-detail" => set_mode(
                &mut mode,
                Mode::ProbePersistedRebuildRowSharedSequenceDetail,
                arg.as_str(),
            )?,
            "--probe-persisted-rebuild-row-shared-path-diff" => set_mode(
                &mut mode,
                Mode::ProbePersistedRebuildRowSharedPathDiff,
                arg.as_str(),
            )?,
            "--probe-persisted-rebuild-row-step-meta-full-context-diff" => set_mode(
                &mut mode,
                Mode::ProbePersistedRebuildRowStepMetaFullContextDiff,
                arg.as_str(),
            )?,
            "--probe-persisted-rebuild-row-step-meta-full-orchestration-diff" => set_mode(
                &mut mode,
                Mode::ProbePersistedRebuildRowStepMetaFullOrchestrationDiff,
                arg.as_str(),
            )?,
            "--probe-persisted-rebuild-row-shared-ordering-diff" => set_mode(
                &mut mode,
                Mode::ProbePersistedRebuildRowSharedOrderingDiff,
                arg.as_str(),
            )?,
            "--probe-persisted-rebuild-row-cross-invocation-diff" => set_mode(
                &mut mode,
                Mode::ProbePersistedRebuildRowCrossInvocationDiff,
                arg.as_str(),
            )?,
            "--capture-persisted-rebuild-row-slow-event" => set_mode(
                &mut mode,
                Mode::CapturePersistedRebuildRowSlowEvent,
                arg.as_str(),
            )?,
            "--probe-persisted-rebuild-row-step-meta-isolated-shared-internal" => set_mode(
                &mut mode,
                Mode::ProbePersistedRebuildRowStepMetaIsolatedSharedInternal,
                arg.as_str(),
            )?,
            "--runtime-db" => {
                runtime_db = Some(PathBuf::from(parse_string_arg(
                    "--runtime-db",
                    args.next(),
                )?))
            }
            "--recent-raw-db" => {
                recent_raw_db = Some(PathBuf::from(parse_string_arg(
                    "--recent-raw-db",
                    args.next(),
                )?))
            }
            "--budget-ms" => {
                budget_ms = parse_u64_arg("--budget-ms", args.next())?;
            }
            "--threshold-ms" => {
                threshold_ms = parse_u64_arg("--threshold-ms", args.next())?;
            }
            "--json" => json = true,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    let mode = mode.ok_or_else(|| anyhow!("missing required mode flag"))?;
    if matches!(
        mode,
        Mode::CompareSolLegSourceVsCheckpoint | Mode::ExplainReplaySolLegIncomplete
    ) && recent_raw_db.is_none()
    {
        bail!("--recent-raw-db is required for the selected mode");
    }

    Ok(Some(Config {
        mode,
        runtime_db: runtime_db.ok_or_else(|| anyhow!("missing required --runtime-db"))?,
        recent_raw_db,
        json,
        budget_ms,
        threshold_ms,
    }))
}

fn set_mode(current: &mut Option<Mode>, mode: Mode, flag: &str) -> Result<()> {
    if current.replace(mode).is_some() {
        bail!("only one mode flag can be specified; conflicting flag: {flag}");
    }
    Ok(())
}

fn parse_string_arg(flag: &str, value: Option<String>) -> Result<String> {
    let raw = value.ok_or_else(|| anyhow!("missing value for {flag}"))?;
    let trimmed = raw.trim().to_string();
    if trimmed.is_empty() {
        bail!("{flag} cannot be empty");
    }
    Ok(trimmed)
}

fn parse_u64_arg(flag: &str, value: Option<String>) -> Result<u64> {
    let raw = parse_string_arg(flag, value)?;
    raw.parse::<u64>()
        .with_context(|| format!("invalid integer value for {flag}: {raw}"))
}

fn open_store_read_only(path: &Path) -> Result<SqliteStore> {
    SqliteStore::open_read_only(path)
        .with_context(|| format!("failed opening sqlite db read-only {}", path.display()))
}

fn render_json(value: &Value, pretty: bool) -> Result<String> {
    if pretty {
        serde_json::to_string_pretty(value).context("failed serializing diagnostics json")
    } else {
        serde_json::to_string(value).context("failed serializing diagnostics json")
    }
}

fn flatten_json_value(map: &mut Map<String, Value>, value: Value) -> Result<()> {
    let Value::Object(object) = value else {
        bail!("expected object value while flattening diagnostic json");
    };
    for (key, value) in object {
        map.insert(key, value);
    }
    Ok(())
}

fn render_runtime_db_diagnostic_json(
    diagnostic: &ReplayCheckpointRuntimeDbDiagnostic,
) -> Result<Value> {
    let mut map = Map::new();
    map.insert(
        "diagnostic_stage".to_string(),
        serde_json::to_value(diagnostic.diagnostic_stage)?,
    );
    map.insert(
        "diagnostic_budget_exhausted".to_string(),
        Value::Bool(diagnostic.diagnostic_budget_exhausted),
    );
    map.insert(
        "diagnostic_meta_substage".to_string(),
        serde_json::to_value(diagnostic.diagnostic_meta_substage)?,
    );
    map.insert(
        "diagnostic_skipped_stages".to_string(),
        serde_json::to_value(&diagnostic.diagnostic_skipped_stages)?,
    );
    map.insert(
        "diagnostic_open_db_elapsed_ms".to_string(),
        serde_json::to_value(diagnostic.diagnostic_open_db_elapsed_ms)?,
    );
    map.insert(
        "diagnostic_load_row_meta_elapsed_ms".to_string(),
        serde_json::to_value(diagnostic.diagnostic_load_row_meta_elapsed_ms)?,
    );
    map.insert(
        "diagnostic_load_row_elapsed_ms".to_string(),
        serde_json::to_value(diagnostic.diagnostic_load_row_elapsed_ms)?,
    );
    map.insert(
        "diagnostic_parse_state_elapsed_ms".to_string(),
        serde_json::to_value(diagnostic.diagnostic_parse_state_elapsed_ms)?,
    );
    map.insert(
        "diagnostic_classify_blocker_elapsed_ms".to_string(),
        serde_json::to_value(diagnostic.diagnostic_classify_blocker_elapsed_ms)?,
    );
    map.insert(
        "diagnostic_total_elapsed_ms".to_string(),
        Value::Number(diagnostic.diagnostic_total_elapsed_ms.into()),
    );
    map.insert(
        "meta_state_json_bytes_requested".to_string(),
        Value::Bool(diagnostic.meta_state_json_bytes_requested),
    );
    map.insert(
        "meta_open_db_elapsed_ms".to_string(),
        serde_json::to_value(diagnostic.meta_open_db_elapsed_ms)?,
    );
    map.insert(
        "meta_check_table_exists_elapsed_ms".to_string(),
        serde_json::to_value(diagnostic.meta_check_table_exists_elapsed_ms)?,
    );
    map.insert(
        "meta_query_elapsed_ms".to_string(),
        serde_json::to_value(diagnostic.meta_query_elapsed_ms)?,
    );
    map.insert(
        "meta_parse_phase_elapsed_ms".to_string(),
        serde_json::to_value(diagnostic.meta_parse_phase_elapsed_ms)?,
    );
    map.insert(
        "meta_parse_updated_at_elapsed_ms".to_string(),
        serde_json::to_value(diagnostic.meta_parse_updated_at_elapsed_ms)?,
    );
    map.insert(
        "meta_state_json_bytes_eval_elapsed_ms".to_string(),
        serde_json::to_value(diagnostic.meta_state_json_bytes_eval_elapsed_ms)?,
    );
    map.insert(
        "meta_total_elapsed_ms".to_string(),
        serde_json::to_value(diagnostic.meta_total_elapsed_ms)?,
    );
    flatten_json_value(&mut map, serde_json::to_value(&diagnostic.row_meta)?)?;
    if let Some(inspection) = &diagnostic.inspection {
        flatten_json_value(&mut map, serde_json::to_value(inspection)?)?;
    }
    if let Some(explanation) = &diagnostic.blocker_explanation {
        flatten_json_value(&mut map, serde_json::to_value(explanation)?)?;
    }
    Ok(Value::Object(map))
}

fn render_raw_probe_json(diagnostic: &RawPersistedRebuildRowProbeDiagnostic) -> Result<Value> {
    serde_json::to_value(diagnostic).context("failed serializing raw persisted rebuild row probe")
}

fn render_driver_compare_json(
    diagnostic: &PersistedRebuildRowDriverCompareDiagnostic,
) -> Result<Value> {
    serde_json::to_value(diagnostic)
        .context("failed serializing persisted rebuild row driver compare probe")
}

fn render_step_meta_compare_json(
    diagnostic: &PersistedRebuildRowStepMetaCompareDiagnostic,
) -> Result<Value> {
    serde_json::to_value(diagnostic)
        .context("failed serializing persisted rebuild row step-meta compare probe")
}

fn render_shared_sequence_compare_json(
    diagnostic: &PersistedRebuildRowSharedSequenceCompareDiagnostic,
) -> Result<Value> {
    serde_json::to_value(diagnostic)
        .context("failed serializing persisted rebuild row shared-sequence compare probe")
}

fn render_shared_path_diff_json(
    diagnostic: &PersistedRebuildRowSharedPathDiffDiagnostic,
) -> Result<Value> {
    serde_json::to_value(diagnostic)
        .context("failed serializing persisted rebuild row shared-path diff probe")
}

fn render_step_meta_full_context_diff_json(
    diagnostic: &PersistedRebuildRowStepMetaFullContextDiffDiagnostic,
) -> Result<Value> {
    serde_json::to_value(diagnostic)
        .context("failed serializing persisted rebuild row step-meta full-context diff probe")
}

fn render_step_meta_full_orchestration_diff_json(
    diagnostic: &PersistedRebuildRowStepMetaFullOrchestrationDiffDiagnostic,
) -> Result<Value> {
    serde_json::to_value(diagnostic)
        .context("failed serializing persisted rebuild row step-meta full-orchestration diff probe")
}

fn render_shared_ordering_diff_json(
    diagnostic: &PersistedRebuildRowSharedOrderingDiffDiagnostic,
) -> Result<Value> {
    serde_json::to_value(diagnostic)
        .context("failed serializing persisted rebuild row shared-ordering diff probe")
}

fn render_step_meta_isolated_shared_json(
    diagnostic: &PersistedRebuildRowStepMetaIsolatedSharedDiagnostic,
) -> Result<Value> {
    serde_json::to_value(diagnostic)
        .context("failed serializing persisted rebuild row isolated-shared step-meta probe")
}

fn render_cross_invocation_diff_json(
    diagnostic: &PersistedRebuildRowCrossInvocationDiffDiagnostic,
) -> Result<Value> {
    serde_json::to_value(diagnostic)
        .context("failed serializing persisted rebuild row cross-invocation diff probe")
}

fn render_slow_event_capture_json(
    diagnostic: &PersistedRebuildRowSlowEventCaptureDiagnostic,
) -> Result<Value> {
    serde_json::to_value(diagnostic)
        .context("failed serializing persisted rebuild row slow-event capture")
}

#[derive(Debug, Clone, Copy)]
enum CrossInvocationChildSurface {
    IsolatedShared,
    FullShared,
}

#[derive(Debug, Clone)]
struct CrossInvocationChildOutcome {
    step_meta_elapsed_ms: Option<u64>,
    row_exists: Option<bool>,
    budget_exhausted: bool,
}

fn build_cross_invocation_diff_diagnostic(
    started_at: std::time::Instant,
    stage: PersistedRebuildRowCrossInvocationDiffStage,
    budget_exhausted: bool,
    isolated_first: CrossInvocationChildOutcome,
    full_first: CrossInvocationChildOutcome,
    isolated_repeat: CrossInvocationChildOutcome,
    full_repeat: CrossInvocationChildOutcome,
    each_child_is_fresh_process: bool,
    mode_level_independent_invocations: bool,
    invocation_order: Vec<String>,
) -> PersistedRebuildRowCrossInvocationDiffDiagnostic {
    let mut diagnostic = PersistedRebuildRowCrossInvocationDiffDiagnostic {
        cross_invocation_diff_stage: stage,
        cross_invocation_diff_budget_exhausted: budget_exhausted,
        cross_invocation_diff_total_elapsed_ms: started_at
            .elapsed()
            .as_millis()
            .min(u64::MAX as u128) as u64,
        cross_invocation_diff_isolated_child_first_step_meta_elapsed_ms: isolated_first
            .step_meta_elapsed_ms,
        cross_invocation_diff_isolated_child_first_row_exists: isolated_first.row_exists,
        cross_invocation_diff_isolated_child_first_fresh_process: each_child_is_fresh_process,
        cross_invocation_diff_full_child_first_step_meta_elapsed_ms: full_first.step_meta_elapsed_ms,
        cross_invocation_diff_full_child_first_row_exists: full_first.row_exists,
        cross_invocation_diff_full_child_first_fresh_process: each_child_is_fresh_process,
        cross_invocation_diff_isolated_child_repeat_step_meta_elapsed_ms: isolated_repeat
            .step_meta_elapsed_ms,
        cross_invocation_diff_isolated_child_repeat_row_exists: isolated_repeat.row_exists,
        cross_invocation_diff_isolated_child_repeat_fresh_process: each_child_is_fresh_process,
        cross_invocation_diff_full_child_repeat_step_meta_elapsed_ms: full_repeat.step_meta_elapsed_ms,
        cross_invocation_diff_full_child_repeat_row_exists: full_repeat.row_exists,
        cross_invocation_diff_full_child_repeat_fresh_process: each_child_is_fresh_process,
        cross_invocation_diff_children_use_exact_existing_surfaces: true,
        cross_invocation_diff_mode_level_independent_invocations:
            mode_level_independent_invocations,
        cross_invocation_diff_invocation_order: invocation_order,
        cross_invocation_diff_reason_class:
            PersistedRebuildRowCrossInvocationDiffReasonClass::CrossInvocationDiffUnprovenDueToBudgetExhausted,
        cross_invocation_diff_explanation:
            "cross-invocation diff has not been classified yet".to_string(),
    };
    let (reason_class, explanation) =
        DiscoveryService::classify_persisted_rebuild_row_cross_invocation_diff(&diagnostic);
    diagnostic.cross_invocation_diff_reason_class = reason_class;
    diagnostic.cross_invocation_diff_explanation = explanation;
    diagnostic
}

fn extract_optional_u64_field(json: &Value, field: &str) -> Result<Option<u64>> {
    match json.get(field) {
        Some(Value::Null) | None => Ok(None),
        Some(Value::Number(number)) => number
            .as_u64()
            .map(Some)
            .ok_or_else(|| anyhow!("expected u64 field {field}")),
        Some(other) => bail!("expected numeric or null field {field}, got {other}"),
    }
}

fn extract_optional_bool_field(json: &Value, field: &str) -> Result<Option<bool>> {
    match json.get(field) {
        Some(Value::Null) | None => Ok(None),
        Some(Value::Bool(value)) => Ok(Some(*value)),
        Some(other) => bail!("expected bool or null field {field}, got {other}"),
    }
}

fn resolve_cross_invocation_self_exec_path() -> Result<Option<PathBuf>> {
    if let Ok(path) = env::var("COPYBOT_DISCOVERY_DIAGNOSE_SELF_EXEC_PATH") {
        return Ok(Some(PathBuf::from(path)));
    }
    #[cfg(test)]
    {
        if env::var_os("COPYBOT_DISCOVERY_DIAGNOSE_FORCE_SELF_EXEC").is_none() {
            return Ok(None);
        }
    }
    Ok(Some(
        env::current_exe().context("failed resolving current executable path")?,
    ))
}

fn run_cross_invocation_child_subprocess(
    exec_path: &Path,
    surface: CrossInvocationChildSurface,
    runtime_db: &Path,
    budget_ms: u64,
) -> Result<CrossInvocationChildOutcome> {
    let mode_flag = match surface {
        CrossInvocationChildSurface::IsolatedShared => {
            "--probe-persisted-rebuild-row-step-meta-isolated-shared-internal"
        }
        CrossInvocationChildSurface::FullShared => "--probe-persisted-rebuild-row-step-meta-detail",
    };
    let output = Command::new(exec_path)
        .arg(mode_flag)
        .arg("--runtime-db")
        .arg(runtime_db)
        .arg("--budget-ms")
        .arg(budget_ms.to_string())
        .arg("--json")
        .output()
        .with_context(|| {
            format!(
                "failed running cross-invocation child probe {} via {}",
                mode_flag,
                exec_path.display()
            )
        })?;
    if !output.status.success() {
        bail!(
            "cross-invocation child probe {} failed: status={:?} stdout={} stderr={}",
            mode_flag,
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let json: Value = serde_json::from_slice(&output.stdout)
        .with_context(|| format!("failed parsing child probe json for {mode_flag}"))?;
    match surface {
        CrossInvocationChildSurface::IsolatedShared => Ok(CrossInvocationChildOutcome {
            step_meta_elapsed_ms: extract_optional_u64_field(
                &json,
                "step_meta_isolated_shared_step_meta_elapsed_ms",
            )?,
            row_exists: extract_optional_bool_field(&json, "step_meta_isolated_shared_row_exists")?,
            budget_exhausted: json
                .get("step_meta_isolated_shared_budget_exhausted")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        }),
        CrossInvocationChildSurface::FullShared => Ok(CrossInvocationChildOutcome {
            step_meta_elapsed_ms: extract_optional_u64_field(
                &json,
                "step_meta_shared_connection_step_meta_elapsed_ms",
            )?,
            row_exists: extract_optional_bool_field(&json, "step_meta_shared_connection_row_exists")?,
            budget_exhausted: json
                .get("step_meta_compare_budget_exhausted")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        }),
    }
}

#[cfg(test)]
fn run_cross_invocation_child_inline(
    surface: CrossInvocationChildSurface,
    runtime_db: &Path,
    budget_ms: u64,
) -> Result<CrossInvocationChildOutcome> {
    match surface {
        CrossInvocationChildSurface::IsolatedShared => {
            let diagnostic =
                DiscoveryService::probe_persisted_rebuild_row_step_meta_isolated_shared_read_only(
                    runtime_db, budget_ms,
                )?;
            Ok(CrossInvocationChildOutcome {
                step_meta_elapsed_ms: diagnostic.step_meta_isolated_shared_step_meta_elapsed_ms,
                row_exists: diagnostic.step_meta_isolated_shared_row_exists,
                budget_exhausted: diagnostic.step_meta_isolated_shared_budget_exhausted,
            })
        }
        CrossInvocationChildSurface::FullShared => {
            let diagnostic =
                DiscoveryService::probe_persisted_rebuild_row_step_meta_compare_read_only(
                    runtime_db, budget_ms,
                )?;
            Ok(CrossInvocationChildOutcome {
                step_meta_elapsed_ms: diagnostic.step_meta_shared_connection_step_meta_elapsed_ms,
                row_exists: diagnostic.step_meta_shared_connection_row_exists,
                budget_exhausted: diagnostic.step_meta_compare_budget_exhausted,
            })
        }
    }
}

fn run_cross_invocation_diff(config: &Config) -> Result<Value> {
    let started_at = std::time::Instant::now();
    let deadline = started_at + std::time::Duration::from_millis(config.budget_ms);
    let remaining_budget_ms = |deadline: std::time::Instant| {
        deadline
            .saturating_duration_since(std::time::Instant::now())
            .as_millis()
            .min(u64::MAX as u128) as u64
    };
    let invocation_order = vec![
        "isolated_child_first".to_string(),
        "full_child_first".to_string(),
        "isolated_child_repeat".to_string(),
        "full_child_repeat".to_string(),
    ];

    let run_child = |surface: CrossInvocationChildSurface| -> Result<CrossInvocationChildOutcome> {
        let budget_ms = remaining_budget_ms(deadline);
        if budget_ms == 0 {
            return Ok(CrossInvocationChildOutcome {
                step_meta_elapsed_ms: None,
                row_exists: None,
                budget_exhausted: true,
            });
        }
        if let Some(exec_path) = resolve_cross_invocation_self_exec_path()? {
            run_cross_invocation_child_subprocess(&exec_path, surface, &config.runtime_db, budget_ms)
        } else {
            #[cfg(test)]
            {
                run_cross_invocation_child_inline(surface, &config.runtime_db, budget_ms)
            }
            #[cfg(not(test))]
            {
                bail!("cross-invocation mode requires a resolvable self executable path");
            }
        }
    };

    let isolated_first = run_child(CrossInvocationChildSurface::IsolatedShared)?;
    let full_first = if isolated_first.budget_exhausted {
        CrossInvocationChildOutcome {
            step_meta_elapsed_ms: None,
            row_exists: None,
            budget_exhausted: true,
        }
    } else {
        run_child(CrossInvocationChildSurface::FullShared)?
    };
    let isolated_repeat = if full_first.budget_exhausted {
        CrossInvocationChildOutcome {
            step_meta_elapsed_ms: None,
            row_exists: None,
            budget_exhausted: true,
        }
    } else {
        run_child(CrossInvocationChildSurface::IsolatedShared)?
    };
    let full_repeat = if isolated_repeat.budget_exhausted {
        CrossInvocationChildOutcome {
            step_meta_elapsed_ms: None,
            row_exists: None,
            budget_exhausted: true,
        }
    } else {
        run_child(CrossInvocationChildSurface::FullShared)?
    };

    let stage = if isolated_first.budget_exhausted {
        PersistedRebuildRowCrossInvocationDiffStage::IsolatedChildFirst
    } else if full_first.budget_exhausted {
        PersistedRebuildRowCrossInvocationDiffStage::FullChildFirst
    } else if isolated_repeat.budget_exhausted {
        PersistedRebuildRowCrossInvocationDiffStage::IsolatedChildRepeat
    } else if full_repeat.budget_exhausted {
        PersistedRebuildRowCrossInvocationDiffStage::FullChildRepeat
    } else {
        PersistedRebuildRowCrossInvocationDiffStage::Complete
    };
    let actual_child_processes = resolve_cross_invocation_self_exec_path()?.is_some();
    render_cross_invocation_diff_json(&build_cross_invocation_diff_diagnostic(
        started_at,
        stage,
        isolated_first.budget_exhausted
            || full_first.budget_exhausted
            || isolated_repeat.budget_exhausted
            || full_repeat.budget_exhausted,
        isolated_first,
        full_first,
        isolated_repeat,
        full_repeat,
        actual_child_processes,
        actual_child_processes,
        invocation_order,
    ))
}

fn run(config: Config) -> Result<String> {
    let output = match config.mode {
        Mode::InspectPersistedRebuildRowMetaLite => render_runtime_db_diagnostic_json(
            &DiscoveryService::diagnose_runtime_db_replay_checkpoint_read_only(
                &config.runtime_db,
                ReplayCheckpointRuntimeDbOnlyMode::RowMetaLite,
                config.budget_ms,
            )?,
        )?,
        Mode::InspectPersistedRebuildRowMeta => render_runtime_db_diagnostic_json(
            &DiscoveryService::diagnose_runtime_db_replay_checkpoint_read_only(
                &config.runtime_db,
                ReplayCheckpointRuntimeDbOnlyMode::RowMeta,
                config.budget_ms,
            )?,
        )?,
        Mode::InspectPersistedRebuildState => render_runtime_db_diagnostic_json(
            &DiscoveryService::diagnose_runtime_db_replay_checkpoint_read_only(
                &config.runtime_db,
                ReplayCheckpointRuntimeDbOnlyMode::Inspect,
                config.budget_ms,
            )?,
        )?,
        Mode::ExplainPublishableCheckpointBlocker => render_runtime_db_diagnostic_json(
            &DiscoveryService::diagnose_runtime_db_replay_checkpoint_read_only(
                &config.runtime_db,
                ReplayCheckpointRuntimeDbOnlyMode::ExplainPublishableCheckpointBlocker,
                config.budget_ms,
            )?,
        )?,
        Mode::CompareSolLegSourceVsCheckpoint => {
            let runtime_store = open_store_read_only(&config.runtime_db)?;
            let recent_raw_db = config
                .recent_raw_db
                .as_deref()
                .expect("validated in parse_args");
            let recent_raw_store = open_store_read_only(recent_raw_db)?;
            serde_json::to_value(
                DiscoveryService::compare_sol_leg_source_vs_checkpoint_read_only(
                    &runtime_store,
                    &recent_raw_store,
                )?,
            )?
        }
        Mode::ExplainReplaySolLegIncomplete => {
            let runtime_store = open_store_read_only(&config.runtime_db)?;
            let recent_raw_db = config
                .recent_raw_db
                .as_deref()
                .expect("validated in parse_args");
            let recent_raw_store = open_store_read_only(recent_raw_db)?;
            serde_json::to_value(
                DiscoveryService::explain_replay_sol_leg_incomplete_read_only(
                    &runtime_store,
                    &recent_raw_store,
                )?,
            )?
        }
        Mode::ProbePersistedRebuildRowRaw => render_raw_probe_json(
            &DiscoveryService::probe_persisted_rebuild_row_raw_read_only(
                &config.runtime_db,
                config.budget_ms,
            )?,
        )?,
        Mode::ProbePersistedRebuildRowDriverCompare => render_driver_compare_json(
            &DiscoveryService::probe_persisted_rebuild_row_driver_compare_read_only(
                &config.runtime_db,
                config.budget_ms,
            )?,
        )?,
        Mode::ProbePersistedRebuildRowStepMetaDetail => render_step_meta_compare_json(
            &DiscoveryService::probe_persisted_rebuild_row_step_meta_compare_read_only(
                &config.runtime_db,
                config.budget_ms,
            )?,
        )?,
        Mode::ProbePersistedRebuildRowSharedSequenceDetail => render_shared_sequence_compare_json(
            &DiscoveryService::probe_persisted_rebuild_row_shared_sequence_compare_read_only(
                &config.runtime_db,
                config.budget_ms,
            )?,
        )?,
        Mode::ProbePersistedRebuildRowSharedPathDiff => render_shared_path_diff_json(
            &DiscoveryService::probe_persisted_rebuild_row_shared_path_diff_read_only(
                &config.runtime_db,
                config.budget_ms,
            )?,
        )?,
        Mode::ProbePersistedRebuildRowStepMetaFullContextDiff => {
            render_step_meta_full_context_diff_json(
                &DiscoveryService::probe_persisted_rebuild_row_step_meta_full_context_diff_read_only(
                    &config.runtime_db,
                    config.budget_ms,
                )?,
            )?
        }
        Mode::ProbePersistedRebuildRowStepMetaFullOrchestrationDiff => {
            render_step_meta_full_orchestration_diff_json(
                &DiscoveryService::probe_persisted_rebuild_row_step_meta_full_orchestration_diff_read_only(
                    &config.runtime_db,
                    config.budget_ms,
                )?,
            )?
        }
        Mode::ProbePersistedRebuildRowSharedOrderingDiff => render_shared_ordering_diff_json(
            &DiscoveryService::probe_persisted_rebuild_row_shared_ordering_diff_read_only(
                &config.runtime_db,
                config.budget_ms,
            )?,
        )?,
        Mode::ProbePersistedRebuildRowCrossInvocationDiff => run_cross_invocation_diff(&config)?,
        Mode::CapturePersistedRebuildRowSlowEvent => render_slow_event_capture_json(
            &DiscoveryService::capture_persisted_rebuild_row_slow_event_read_only(
                &config.runtime_db,
                config.threshold_ms,
                config.budget_ms,
            )?,
        )?,
        Mode::ProbePersistedRebuildRowStepMetaIsolatedSharedInternal => {
            render_step_meta_isolated_shared_json(
                &DiscoveryService::probe_persisted_rebuild_row_step_meta_isolated_shared_read_only(
                    &config.runtime_db,
                    config.budget_ms,
                )?,
            )?
        }
    };
    render_json(&output, config.json)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use copybot_storage::{DiscoveryPersistedRebuildPhase, DiscoveryPersistedRebuildStateRow};
    use std::sync::OnceLock;

    fn make_test_store(name: &str) -> Result<(tempfile::TempDir, PathBuf)> {
        let temp = tempfile::tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join(name);
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;
        Ok((temp, db_path))
    }

    #[test]
    fn parse_args_requires_recent_raw_db_for_compare_modes_stage1() -> Result<()> {
        let error = parse_args_from([
            "--compare-sol-leg-source-vs-checkpoint".to_string(),
            "--runtime-db".to_string(),
            "runtime.sqlite".to_string(),
        ])
        .expect_err("compare mode should require recent raw db");
        assert!(error
            .to_string()
            .contains("--recent-raw-db is required for the selected mode"));
        Ok(())
    }

    #[test]
    fn parse_args_rejects_multiple_mode_flags_stage1() -> Result<()> {
        let error = parse_args_from([
            "--inspect-persisted-rebuild-row-meta-lite".to_string(),
            "--inspect-persisted-rebuild-row-meta".to_string(),
            "--runtime-db".to_string(),
            "runtime.sqlite".to_string(),
        ])
        .expect_err("multiple mode flags should be rejected");
        assert!(error
            .to_string()
            .contains("only one mode flag can be specified"));
        Ok(())
    }

    #[test]
    fn parse_args_accepts_budget_ms_stage1() -> Result<()> {
        let config = parse_args_from([
            "--inspect-persisted-rebuild-row-meta-lite".to_string(),
            "--runtime-db".to_string(),
            "runtime.sqlite".to_string(),
            "--budget-ms".to_string(),
            "123".to_string(),
        ])?
        .expect("config should parse");
        assert_eq!(config.budget_ms, 123);
        assert_eq!(config.mode, Mode::InspectPersistedRebuildRowMetaLite);
        Ok(())
    }

    #[test]
    fn parse_args_accepts_raw_probe_mode_stage1() -> Result<()> {
        let config = parse_args_from([
            "--probe-persisted-rebuild-row-raw".to_string(),
            "--runtime-db".to_string(),
            "runtime.sqlite".to_string(),
        ])?
        .expect("config should parse");
        assert_eq!(config.mode, Mode::ProbePersistedRebuildRowRaw);
        Ok(())
    }

    #[test]
    fn parse_args_accepts_driver_compare_mode_stage1() -> Result<()> {
        let config = parse_args_from([
            "--probe-persisted-rebuild-row-driver-compare".to_string(),
            "--runtime-db".to_string(),
            "runtime.sqlite".to_string(),
        ])?
        .expect("config should parse");
        assert_eq!(config.mode, Mode::ProbePersistedRebuildRowDriverCompare);
        Ok(())
    }

    #[test]
    fn parse_args_accepts_step_meta_detail_mode_stage1() -> Result<()> {
        let config = parse_args_from([
            "--probe-persisted-rebuild-row-step-meta-detail".to_string(),
            "--runtime-db".to_string(),
            "runtime.sqlite".to_string(),
        ])?
        .expect("config should parse");
        assert_eq!(config.mode, Mode::ProbePersistedRebuildRowStepMetaDetail);
        Ok(())
    }

    #[test]
    fn parse_args_accepts_shared_sequence_detail_mode_stage1() -> Result<()> {
        let config = parse_args_from([
            "--probe-persisted-rebuild-row-shared-sequence-detail".to_string(),
            "--runtime-db".to_string(),
            "runtime.sqlite".to_string(),
        ])?
        .expect("config should parse");
        assert_eq!(
            config.mode,
            Mode::ProbePersistedRebuildRowSharedSequenceDetail
        );
        Ok(())
    }

    #[test]
    fn parse_args_accepts_shared_path_diff_mode_stage1() -> Result<()> {
        let config = parse_args_from([
            "--probe-persisted-rebuild-row-shared-path-diff".to_string(),
            "--runtime-db".to_string(),
            "runtime.sqlite".to_string(),
        ])?
        .expect("config should parse");
        assert_eq!(config.mode, Mode::ProbePersistedRebuildRowSharedPathDiff);
        Ok(())
    }

    #[test]
    fn parse_args_accepts_step_meta_full_context_diff_mode_stage1() -> Result<()> {
        let config = parse_args_from([
            "--probe-persisted-rebuild-row-step-meta-full-context-diff".to_string(),
            "--runtime-db".to_string(),
            "runtime.sqlite".to_string(),
        ])?
        .expect("config should parse");
        assert_eq!(
            config.mode,
            Mode::ProbePersistedRebuildRowStepMetaFullContextDiff
        );
        Ok(())
    }

    #[test]
    fn parse_args_accepts_step_meta_full_orchestration_diff_mode_stage1() -> Result<()> {
        let config = parse_args_from([
            "--probe-persisted-rebuild-row-step-meta-full-orchestration-diff".to_string(),
            "--runtime-db".to_string(),
            "runtime.sqlite".to_string(),
        ])?
        .expect("config should parse");
        assert_eq!(
            config.mode,
            Mode::ProbePersistedRebuildRowStepMetaFullOrchestrationDiff
        );
        Ok(())
    }

    #[test]
    fn parse_args_accepts_shared_ordering_diff_mode_stage1() -> Result<()> {
        let config = parse_args_from([
            "--probe-persisted-rebuild-row-shared-ordering-diff".to_string(),
            "--runtime-db".to_string(),
            "runtime.sqlite".to_string(),
        ])?
        .expect("config should parse");
        assert_eq!(config.mode, Mode::ProbePersistedRebuildRowSharedOrderingDiff);
        Ok(())
    }

    #[test]
    fn parse_args_accepts_cross_invocation_diff_mode_stage1() -> Result<()> {
        let config = parse_args_from([
            "--probe-persisted-rebuild-row-cross-invocation-diff".to_string(),
            "--runtime-db".to_string(),
            "runtime.sqlite".to_string(),
        ])?
        .expect("config should parse");
        assert_eq!(config.mode, Mode::ProbePersistedRebuildRowCrossInvocationDiff);
        Ok(())
    }

    #[test]
    fn parse_args_accepts_slow_event_capture_mode_stage1() -> Result<()> {
        let config = parse_args_from([
            "--capture-persisted-rebuild-row-slow-event".to_string(),
            "--runtime-db".to_string(),
            "runtime.sqlite".to_string(),
            "--threshold-ms".to_string(),
            "2222".to_string(),
        ])?
        .expect("config should parse");
        assert_eq!(config.mode, Mode::CapturePersistedRebuildRowSlowEvent);
        assert_eq!(config.threshold_ms, 2222);
        Ok(())
    }

    #[test]
    fn run_replay_row_meta_lite_reports_missing_checkpoint_json_stage1() -> Result<()> {
        let (_temp, runtime_db) = make_test_store("diagnose-meta-runtime.sqlite")?;
        let output = run(Config {
            mode: Mode::InspectPersistedRebuildRowMetaLite,
            runtime_db,
            recent_raw_db: None,
            json: true,
            budget_ms: DEFAULT_RUNTIME_DB_DIAGNOSTIC_BUDGET_MS,
            threshold_ms: DEFAULT_SLOW_EVENT_CAPTURE_THRESHOLD_MS,
        })?;
        let json: serde_json::Value =
            serde_json::from_str(&output).context("invalid json output")?;
        assert_eq!(json["persisted_rebuild_row_exists"], false);
        assert_eq!(json["diagnostic_budget_exhausted"], false);
        assert_eq!(json["diagnostic_stage"], "complete");
        assert_eq!(json["meta_state_json_bytes_requested"], false);
        assert!(json["meta_check_table_exists_elapsed_ms"].is_number());
        assert!(json["meta_total_elapsed_ms"].is_number());
        assert!(json["persisted_rebuild_state_json_bytes"].is_null());
        Ok(())
    }

    #[test]
    fn run_replay_row_meta_reports_missing_checkpoint_json_stage1() -> Result<()> {
        let (_temp, runtime_db) = make_test_store("diagnose-meta-with-size-runtime.sqlite")?;
        let output = run(Config {
            mode: Mode::InspectPersistedRebuildRowMeta,
            runtime_db,
            recent_raw_db: None,
            json: true,
            budget_ms: DEFAULT_RUNTIME_DB_DIAGNOSTIC_BUDGET_MS,
            threshold_ms: DEFAULT_SLOW_EVENT_CAPTURE_THRESHOLD_MS,
        })?;
        let json: serde_json::Value =
            serde_json::from_str(&output).context("invalid json output")?;
        assert_eq!(json["persisted_rebuild_row_exists"], false);
        assert_eq!(json["diagnostic_budget_exhausted"], false);
        assert_eq!(json["diagnostic_stage"], "complete");
        assert_eq!(json["meta_state_json_bytes_requested"], true);
        Ok(())
    }

    #[test]
    fn run_raw_probe_reports_missing_checkpoint_json_stage1() -> Result<()> {
        let (_temp, runtime_db) = make_test_store("diagnose-raw-probe-runtime.sqlite")?;
        let output = run(Config {
            mode: Mode::ProbePersistedRebuildRowRaw,
            runtime_db,
            recent_raw_db: None,
            json: true,
            budget_ms: DEFAULT_RUNTIME_DB_DIAGNOSTIC_BUDGET_MS,
            threshold_ms: DEFAULT_SLOW_EVENT_CAPTURE_THRESHOLD_MS,
        })?;
        let json: serde_json::Value =
            serde_json::from_str(&output).context("invalid json output")?;
        assert_eq!(json["raw_persisted_rebuild_row_exists"], false);
        assert_eq!(
            json["raw_persisted_rebuild_row_reason_class"],
            "row_missing"
        );
        assert!(json["raw_runtime_db_page_size"].is_number());
        assert!(json["raw_runtime_db_page_count"].is_number());
        assert!(json["raw_runtime_db_freelist_count"].is_number());
        assert!(json["raw_runtime_db_journal_mode"].is_string());
        assert!(json["raw_runtime_db_locking_mode"].is_string());
        Ok(())
    }

    #[test]
    fn run_driver_compare_reports_missing_checkpoint_json_stage1() -> Result<()> {
        let (_temp, runtime_db) = make_test_store("diagnose-driver-compare-runtime.sqlite")?;
        let store = SqliteStore::open(Path::new(&runtime_db))?;
        assert!(store.load_discovery_persisted_rebuild_state()?.is_none());
        drop(store);
        let output = run(Config {
            mode: Mode::ProbePersistedRebuildRowDriverCompare,
            runtime_db,
            recent_raw_db: None,
            json: true,
            budget_ms: DEFAULT_RUNTIME_DB_DIAGNOSTIC_BUDGET_MS,
            threshold_ms: DEFAULT_SLOW_EVENT_CAPTURE_THRESHOLD_MS,
        })?;
        let json: serde_json::Value =
            serde_json::from_str(&output).context("invalid json output")?;
        assert_eq!(json["driver_compare_row_exists"], false);
        assert_eq!(json["driver_compare_budget_exhausted"], false);
        assert_eq!(json["driver_compare_reason_class"], "driver_row_missing");
        assert!(json["driver_compare_explanation"]
            .as_str()
            .is_some_and(|value| value.contains("is missing on the read-only runtime connection")));
        assert!(json["driver_compare_prepare_exists_elapsed_ms"].is_number());
        assert!(json["driver_compare_step_exists_elapsed_ms"].is_number());
        Ok(())
    }

    #[test]
    fn run_step_meta_detail_reports_missing_checkpoint_json_stage1() -> Result<()> {
        let (_temp, runtime_db) = make_test_store("diagnose-step-meta-detail-runtime.sqlite")?;
        let store = SqliteStore::open(Path::new(&runtime_db))?;
        assert!(store.load_discovery_persisted_rebuild_state()?.is_none());
        drop(store);
        let output = run(Config {
            mode: Mode::ProbePersistedRebuildRowStepMetaDetail,
            runtime_db,
            recent_raw_db: None,
            json: true,
            budget_ms: DEFAULT_RUNTIME_DB_DIAGNOSTIC_BUDGET_MS,
            threshold_ms: DEFAULT_SLOW_EVENT_CAPTURE_THRESHOLD_MS,
        })?;
        let json: serde_json::Value =
            serde_json::from_str(&output).context("invalid json output")?;
        assert_eq!(json["step_meta_shared_connection_row_exists"], false);
        assert_eq!(
            json["step_meta_compare_reason_class"],
            "step_meta_row_missing"
        );
        assert!(json["step_meta_compare_explanation"]
            .as_str()
            .is_some_and(|value| value.contains("is missing on the runtime db")));
        Ok(())
    }

    #[test]
    fn run_shared_sequence_detail_reports_missing_checkpoint_json_stage1() -> Result<()> {
        let (_temp, runtime_db) =
            make_test_store("diagnose-shared-sequence-detail-runtime.sqlite")?;
        let store = SqliteStore::open(Path::new(&runtime_db))?;
        assert!(store.load_discovery_persisted_rebuild_state()?.is_none());
        drop(store);
        let output = run(Config {
            mode: Mode::ProbePersistedRebuildRowSharedSequenceDetail,
            runtime_db,
            recent_raw_db: None,
            json: true,
            budget_ms: DEFAULT_RUNTIME_DB_DIAGNOSTIC_BUDGET_MS,
            threshold_ms: DEFAULT_SLOW_EVENT_CAPTURE_THRESHOLD_MS,
        })?;
        let json: serde_json::Value =
            serde_json::from_str(&output).context("invalid json output")?;
        assert_eq!(
            json["shared_sequence_baseline_with_exists_row_exists"],
            false
        );
        assert_eq!(
            json["shared_sequence_compare_reason_class"],
            "shared_sequence_row_missing"
        );
        assert!(json["shared_sequence_compare_explanation"]
            .as_str()
            .is_some_and(|value| value.contains("is missing on the runtime db")));
        Ok(())
    }

    #[test]
    fn run_shared_path_diff_reports_missing_checkpoint_json_stage1() -> Result<()> {
        let (_temp, runtime_db) = make_test_store("diagnose-shared-path-diff-runtime.sqlite")?;
        let store = SqliteStore::open(Path::new(&runtime_db))?;
        assert!(store.load_discovery_persisted_rebuild_state()?.is_none());
        drop(store);
        let output = run(Config {
            mode: Mode::ProbePersistedRebuildRowSharedPathDiff,
            runtime_db,
            recent_raw_db: None,
            json: true,
            budget_ms: DEFAULT_RUNTIME_DB_DIAGNOSTIC_BUDGET_MS,
            threshold_ms: DEFAULT_SLOW_EVENT_CAPTURE_THRESHOLD_MS,
        })?;
        let json: serde_json::Value =
            serde_json::from_str(&output).context("invalid json output")?;
        assert_eq!(
            json["shared_path_diff_reason_class"],
            "shared_path_diff_row_missing"
        );
        assert!(json["shared_path_diff_explanation"]
            .as_str()
            .is_some_and(|value| value.contains("is missing on the runtime db")));
        assert_eq!(
            json["shared_path_diff_step_meta_detail_loads_connection_facts_before_meta_query"],
            true
        );
        assert_eq!(
            json["shared_path_diff_shared_sequence_loads_connection_facts_before_meta_query"],
            true
        );
        Ok(())
    }

    #[test]
    fn run_step_meta_full_context_diff_reports_missing_checkpoint_json_stage1() -> Result<()> {
        let (_temp, runtime_db) =
            make_test_store("diagnose-step-meta-full-context-diff-runtime.sqlite")?;
        let store = SqliteStore::open(Path::new(&runtime_db))?;
        assert!(store.load_discovery_persisted_rebuild_state()?.is_none());
        drop(store);
        let output = run(Config {
            mode: Mode::ProbePersistedRebuildRowStepMetaFullContextDiff,
            runtime_db,
            recent_raw_db: None,
            json: true,
            budget_ms: DEFAULT_RUNTIME_DB_DIAGNOSTIC_BUDGET_MS,
            threshold_ms: DEFAULT_SLOW_EVENT_CAPTURE_THRESHOLD_MS,
        })?;
        let json: serde_json::Value =
            serde_json::from_str(&output).context("invalid json output")?;
        assert_eq!(
            json["step_meta_full_context_diff_reason_class"],
            "step_meta_full_context_diff_row_missing"
        );
        assert!(json["step_meta_full_context_diff_explanation"]
            .as_str()
            .is_some_and(|value| value.contains("is missing on the runtime db")));
        Ok(())
    }

    #[test]
    fn run_step_meta_full_orchestration_diff_reports_missing_checkpoint_json_stage1(
    ) -> Result<()> {
        let (_temp, runtime_db) =
            make_test_store("diagnose-step-meta-full-orchestration-diff-runtime.sqlite")?;
        let store = SqliteStore::open(Path::new(&runtime_db))?;
        assert!(store.load_discovery_persisted_rebuild_state()?.is_none());
        drop(store);
        let output = run(Config {
            mode: Mode::ProbePersistedRebuildRowStepMetaFullOrchestrationDiff,
            runtime_db,
            recent_raw_db: None,
            json: true,
            budget_ms: DEFAULT_RUNTIME_DB_DIAGNOSTIC_BUDGET_MS,
            threshold_ms: DEFAULT_SLOW_EVENT_CAPTURE_THRESHOLD_MS,
        })?;
        let json: serde_json::Value =
            serde_json::from_str(&output).context("invalid json output")?;
        assert_eq!(
            json["step_meta_full_orchestration_diff_reason_class"],
            "step_meta_full_orchestration_diff_row_missing"
        );
        assert!(json["step_meta_full_orchestration_diff_explanation"]
            .as_str()
            .is_some_and(|value| value.contains("is missing on the runtime db")));
        Ok(())
    }

    #[test]
    fn run_shared_ordering_diff_reports_missing_checkpoint_json_stage1() -> Result<()> {
        let (_temp, runtime_db) = make_test_store("diagnose-shared-ordering-diff-runtime.sqlite")?;
        let store = SqliteStore::open(Path::new(&runtime_db))?;
        assert!(store.load_discovery_persisted_rebuild_state()?.is_none());
        drop(store);
        let output = run(Config {
            mode: Mode::ProbePersistedRebuildRowSharedOrderingDiff,
            runtime_db,
            recent_raw_db: None,
            json: true,
            budget_ms: DEFAULT_RUNTIME_DB_DIAGNOSTIC_BUDGET_MS,
            threshold_ms: DEFAULT_SLOW_EVENT_CAPTURE_THRESHOLD_MS,
        })?;
        let json: serde_json::Value =
            serde_json::from_str(&output).context("invalid json output")?;
        assert_eq!(
            json["shared_ordering_diff_reason_class"],
            "shared_ordering_diff_row_missing"
        );
        assert!(json["shared_ordering_diff_explanation"]
            .as_str()
            .is_some_and(|value| value.contains("is missing on the runtime db")));
        Ok(())
    }

    #[test]
    fn run_cross_invocation_diff_reports_missing_checkpoint_json_stage1() -> Result<()> {
        let (_temp, runtime_db) = make_test_store("diagnose-cross-invocation-diff-runtime.sqlite")?;
        let store = SqliteStore::open(Path::new(&runtime_db))?;
        assert!(store.load_discovery_persisted_rebuild_state()?.is_none());
        drop(store);
        let output = run(Config {
            mode: Mode::ProbePersistedRebuildRowCrossInvocationDiff,
            runtime_db,
            recent_raw_db: None,
            json: true,
            budget_ms: DEFAULT_RUNTIME_DB_DIAGNOSTIC_BUDGET_MS,
            threshold_ms: DEFAULT_SLOW_EVENT_CAPTURE_THRESHOLD_MS,
        })?;
        let json: serde_json::Value =
            serde_json::from_str(&output).context("invalid json output")?;
        assert_eq!(
            json["cross_invocation_diff_reason_class"],
            "cross_invocation_diff_row_missing"
        );
        assert_eq!(json["cross_invocation_diff_mode_level_independent_invocations"], false);
        Ok(())
    }

    #[test]
    fn run_slow_event_capture_reports_missing_checkpoint_json_stage1() -> Result<()> {
        let (_temp, runtime_db) = make_test_store("diagnose-slow-event-capture-runtime.sqlite")?;
        let output = run(Config {
            mode: Mode::CapturePersistedRebuildRowSlowEvent,
            runtime_db,
            recent_raw_db: None,
            json: true,
            budget_ms: DEFAULT_RUNTIME_DB_DIAGNOSTIC_BUDGET_MS,
            threshold_ms: DEFAULT_SLOW_EVENT_CAPTURE_THRESHOLD_MS,
        })?;
        let json: serde_json::Value =
            serde_json::from_str(&output).context("invalid json output")?;
        assert_eq!(json["slow_event_capture_observed"], false);
        assert_eq!(json["slow_event_capture_reason_class"], "slow_event_row_missing");
        assert_eq!(
            json["slow_event_capture_surface_identity"],
            "probe_persisted_rebuild_row_step_meta_full_orchestration_diff_read_only"
        );
        assert!(json["slow_event_capture_busy_timeout_ms"].is_number());
        assert!(json["slow_event_capture_runtime_db_path"].is_string());
        Ok(())
    }

    fn ensure_discovery_diagnose_binary_path_stage1() -> Result<PathBuf> {
        static PATH: OnceLock<PathBuf> = OnceLock::new();
        if let Some(path) = PATH.get() {
            return Ok(path.clone());
        }
        let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()
            .context("failed to resolve workspace root")?;
        let status = Command::new("cargo")
            .arg("build")
            .arg("-j")
            .arg("1")
            .arg("-p")
            .arg("copybot-discovery")
            .arg("--bin")
            .arg("discovery_replay_checkpoint_diagnose")
            .current_dir(&workspace_root)
            .status()
            .context("failed to build discovery_replay_checkpoint_diagnose for self-exec test")?;
        assert!(status.success());
        let mut path = workspace_root.join("target/debug/discovery_replay_checkpoint_diagnose");
        if cfg!(windows) {
            path.set_extension("exe");
        }
        let _ = PATH.set(path.clone());
        Ok(path)
    }

    #[test]
    fn run_cross_invocation_diff_can_use_actual_fresh_child_processes_stage1() -> Result<()> {
        let (_temp, runtime_db) = make_test_store("diagnose-cross-invocation-actual-child.sqlite")?;
        let store = SqliteStore::open(Path::new(&runtime_db))?;
        let now = Utc::now();
        store.upsert_discovery_persisted_rebuild_state(&DiscoveryPersistedRebuildStateRow {
            phase: DiscoveryPersistedRebuildPhase::Replay,
            window_start: now,
            horizon_end: now,
            metrics_window_start: now,
            phase_cursor: None,
            prepass_rows_processed: 0,
            prepass_pages_processed: 0,
            replay_rows_processed: 0,
            replay_pages_processed: 0,
            chunks_completed: 0,
            state_json: "{}".to_string(),
            started_at: now,
            updated_at: now,
        })?;
        drop(store);
        let binary_path = ensure_discovery_diagnose_binary_path_stage1()?;
        unsafe {
            env::set_var(
                "COPYBOT_DISCOVERY_DIAGNOSE_SELF_EXEC_PATH",
                binary_path.as_os_str(),
            );
            env::set_var("COPYBOT_DISCOVERY_DIAGNOSE_FORCE_SELF_EXEC", "1");
        }
        let output = run(Config {
            mode: Mode::ProbePersistedRebuildRowCrossInvocationDiff,
            runtime_db,
            recent_raw_db: None,
            json: true,
            budget_ms: DEFAULT_RUNTIME_DB_DIAGNOSTIC_BUDGET_MS,
            threshold_ms: DEFAULT_SLOW_EVENT_CAPTURE_THRESHOLD_MS,
        })?;
        unsafe {
            env::remove_var("COPYBOT_DISCOVERY_DIAGNOSE_SELF_EXEC_PATH");
            env::remove_var("COPYBOT_DISCOVERY_DIAGNOSE_FORCE_SELF_EXEC");
        }
        let json: serde_json::Value =
            serde_json::from_str(&output).context("invalid json output")?;
        assert_eq!(json["cross_invocation_diff_mode_level_independent_invocations"], true);
        assert_eq!(json["cross_invocation_diff_isolated_child_first_fresh_process"], true);
        assert_eq!(json["cross_invocation_diff_full_child_first_fresh_process"], true);
        assert_eq!(
            json["cross_invocation_diff_invocation_order"],
            serde_json::json!([
                "isolated_child_first",
                "full_child_first",
                "isolated_child_repeat",
                "full_child_repeat"
            ])
        );
        Ok(())
    }

    #[test]
    fn run_inspect_persisted_rebuild_state_reports_stage_timings_for_empty_store_stage1(
    ) -> Result<()> {
        let (_temp, runtime_db) = make_test_store("diagnose-read-only-runtime.sqlite")?;
        let output = run(Config {
            mode: Mode::InspectPersistedRebuildState,
            runtime_db,
            recent_raw_db: None,
            json: true,
            budget_ms: DEFAULT_RUNTIME_DB_DIAGNOSTIC_BUDGET_MS,
            threshold_ms: DEFAULT_SLOW_EVENT_CAPTURE_THRESHOLD_MS,
        })?;
        let json: serde_json::Value =
            serde_json::from_str(&output).context("invalid json output")?;
        assert_eq!(json["persisted_rebuild_row_exists"], false);
        assert_eq!(json["persisted_rebuild_checkpoint_exists"], false);
        assert!(json["diagnostic_open_db_elapsed_ms"].is_number());
        assert!(json["diagnostic_load_row_meta_elapsed_ms"].is_number());
        assert!(json["diagnostic_load_row_elapsed_ms"].is_null());
        assert_eq!(json["diagnostic_stage"], "complete");
        Ok(())
    }

    #[test]
    fn run_runtime_db_only_reports_budget_exhaustion_stage_stage1() -> Result<()> {
        let (_temp, runtime_db) = make_test_store("diagnose-budget-runtime.sqlite")?;
        let output = run(Config {
            mode: Mode::InspectPersistedRebuildState,
            runtime_db,
            recent_raw_db: None,
            json: true,
            budget_ms: 0,
            threshold_ms: DEFAULT_SLOW_EVENT_CAPTURE_THRESHOLD_MS,
        })?;
        let json: serde_json::Value =
            serde_json::from_str(&output).context("invalid json output")?;
        assert_eq!(json["diagnostic_budget_exhausted"], true);
        assert_eq!(json["diagnostic_stage"], "open_db_read_only");
        Ok(())
    }

    #[test]
    fn run_explain_replay_sol_leg_incomplete_reports_missing_checkpoint_json_stage1() -> Result<()>
    {
        let (_runtime_temp, runtime_db) = make_test_store("diagnose-runtime.sqlite")?;
        let (_recent_temp, recent_raw_db) = make_test_store("diagnose-recent-raw.sqlite")?;
        let output = run(Config {
            mode: Mode::ExplainReplaySolLegIncomplete,
            runtime_db,
            recent_raw_db: Some(recent_raw_db),
            json: true,
            budget_ms: DEFAULT_RUNTIME_DB_DIAGNOSTIC_BUDGET_MS,
            threshold_ms: DEFAULT_SLOW_EVENT_CAPTURE_THRESHOLD_MS,
        })?;
        let json: serde_json::Value =
            serde_json::from_str(&output).context("invalid json output")?;
        assert_eq!(
            json["replay_sol_leg_incomplete_reason_class"],
            "missing_persisted_checkpoint"
        );
        assert_eq!(json["persisted_rebuild_checkpoint_exists"], false);
        Ok(())
    }

    #[test]
    fn run_exposes_public_phase_helper_surface_stage1() {
        assert_eq!(
            copybot_storage::DiscoveryPersistedRebuildPhase::Replay.as_str(),
            "replay"
        );
    }
}
