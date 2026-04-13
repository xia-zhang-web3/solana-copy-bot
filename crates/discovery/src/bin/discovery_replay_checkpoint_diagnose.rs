use anyhow::{anyhow, bail, Context, Result};
use copybot_discovery::{
    DiscoveryService, ReplayCheckpointRuntimeDbDiagnostic, ReplayCheckpointRuntimeDbOnlyMode,
};
use copybot_storage::SqliteStore;
use serde_json::{Map, Value};
use std::env;
use std::path::{Path, PathBuf};

const DEFAULT_RUNTIME_DB_DIAGNOSTIC_BUDGET_MS: u64 = 30_000;
const USAGE: &str = "usage: discovery_replay_checkpoint_diagnose [--inspect-persisted-rebuild-row-meta | --inspect-persisted-rebuild-state | --explain-publishable-checkpoint-blocker | --compare-sol-leg-source-vs-checkpoint | --explain-replay-sol-leg-incomplete] --runtime-db <path> [--recent-raw-db <path>] [--budget-ms <ms>] [--json]";

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
    InspectPersistedRebuildRowMeta,
    InspectPersistedRebuildState,
    ExplainPublishableCheckpointBlocker,
    CompareSolLegSourceVsCheckpoint,
    ExplainReplaySolLegIncomplete,
}

#[derive(Debug, Clone)]
struct Config {
    mode: Mode,
    runtime_db: PathBuf,
    recent_raw_db: Option<PathBuf>,
    json: bool,
    budget_ms: u64,
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

    while let Some(arg) = args.next() {
        match arg.as_str() {
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
    flatten_json_value(&mut map, serde_json::to_value(&diagnostic.row_meta)?)?;
    if let Some(inspection) = &diagnostic.inspection {
        flatten_json_value(&mut map, serde_json::to_value(inspection)?)?;
    }
    if let Some(explanation) = &diagnostic.blocker_explanation {
        flatten_json_value(&mut map, serde_json::to_value(explanation)?)?;
    }
    Ok(Value::Object(map))
}

fn run(config: Config) -> Result<String> {
    let output = match config.mode {
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
    };
    render_json(&output, config.json)
}

#[cfg(test)]
mod tests {
    use super::*;

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
            "--inspect-persisted-rebuild-row-meta".to_string(),
            "--inspect-persisted-rebuild-state".to_string(),
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
            "--inspect-persisted-rebuild-row-meta".to_string(),
            "--runtime-db".to_string(),
            "runtime.sqlite".to_string(),
            "--budget-ms".to_string(),
            "123".to_string(),
        ])?
        .expect("config should parse");
        assert_eq!(config.budget_ms, 123);
        Ok(())
    }

    #[test]
    fn run_replay_row_meta_reports_missing_checkpoint_json_stage1() -> Result<()> {
        let (_temp, runtime_db) = make_test_store("diagnose-meta-runtime.sqlite")?;
        let output = run(Config {
            mode: Mode::InspectPersistedRebuildRowMeta,
            runtime_db,
            recent_raw_db: None,
            json: true,
            budget_ms: DEFAULT_RUNTIME_DB_DIAGNOSTIC_BUDGET_MS,
        })?;
        let json: serde_json::Value =
            serde_json::from_str(&output).context("invalid json output")?;
        assert_eq!(json["persisted_rebuild_row_exists"], false);
        assert_eq!(json["diagnostic_budget_exhausted"], false);
        assert_eq!(json["diagnostic_stage"], "complete");
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
