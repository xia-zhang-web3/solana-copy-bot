use anyhow::{anyhow, bail, Context, Result};
use copybot_discovery::{
    DiscoveryService, PersistedReplayCheckpointInspection, PublishableCheckpointBlockerExplanation,
    ReplaySolLegIncompleteDiagnostic, ReplaySolLegSourceVsCheckpointDiagnostic,
};
use copybot_storage::SqliteStore;
use std::env;
use std::path::{Path, PathBuf};

const USAGE: &str = "usage: discovery_replay_checkpoint_diagnose [--inspect-persisted-rebuild-state | --explain-publishable-checkpoint-blocker | --compare-sol-leg-source-vs-checkpoint | --explain-replay-sol-leg-incomplete] --runtime-db <path> [--recent-raw-db <path>] [--json]";

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

    while let Some(arg) = args.next() {
        match arg.as_str() {
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

fn open_store(path: &Path) -> Result<SqliteStore> {
    SqliteStore::open(path).with_context(|| format!("failed opening sqlite db {}", path.display()))
}

fn render_json<T: serde::Serialize>(value: &T, pretty: bool) -> Result<String> {
    if pretty {
        serde_json::to_string_pretty(value).context("failed serializing diagnostics json")
    } else {
        serde_json::to_string(value).context("failed serializing diagnostics json")
    }
}

fn run(config: Config) -> Result<String> {
    let runtime_store = open_store(&config.runtime_db)?;
    match config.mode {
        Mode::InspectPersistedRebuildState => {
            let output: PersistedReplayCheckpointInspection =
                DiscoveryService::inspect_persisted_rebuild_state_read_only(&runtime_store)?;
            render_json(&output, config.json)
        }
        Mode::ExplainPublishableCheckpointBlocker => {
            let output: PublishableCheckpointBlockerExplanation =
                DiscoveryService::explain_publishable_checkpoint_blocker_read_only(&runtime_store)?;
            render_json(&output, config.json)
        }
        Mode::CompareSolLegSourceVsCheckpoint => {
            let recent_raw_db = config
                .recent_raw_db
                .as_deref()
                .expect("validated in parse_args");
            let recent_raw_store = open_store(recent_raw_db)?;
            let output: ReplaySolLegSourceVsCheckpointDiagnostic =
                DiscoveryService::compare_sol_leg_source_vs_checkpoint_read_only(
                    &runtime_store,
                    &recent_raw_store,
                )?;
            render_json(&output, config.json)
        }
        Mode::ExplainReplaySolLegIncomplete => {
            let recent_raw_db = config
                .recent_raw_db
                .as_deref()
                .expect("validated in parse_args");
            let recent_raw_store = open_store(recent_raw_db)?;
            let output: ReplaySolLegIncompleteDiagnostic =
                DiscoveryService::explain_replay_sol_leg_incomplete_read_only(
                    &runtime_store,
                    &recent_raw_store,
                )?;
            render_json(&output, config.json)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use copybot_storage::DiscoveryPersistedRebuildPhase;

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
            "--inspect-persisted-rebuild-state".to_string(),
            "--explain-publishable-checkpoint-blocker".to_string(),
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
    fn run_explain_replay_sol_leg_incomplete_reports_missing_checkpoint_json_stage1() -> Result<()>
    {
        let (_runtime_temp, runtime_db) = make_test_store("diagnose-runtime.sqlite")?;
        let (_recent_temp, recent_raw_db) = make_test_store("diagnose-recent-raw.sqlite")?;
        let output = run(Config {
            mode: Mode::ExplainReplaySolLegIncomplete,
            runtime_db,
            recent_raw_db: Some(recent_raw_db),
            json: true,
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
    fn run_inspect_persisted_rebuild_state_is_read_only_for_empty_store_stage1() -> Result<()> {
        let (_temp, runtime_db) = make_test_store("diagnose-read-only-runtime.sqlite")?;
        let store = SqliteStore::open(Path::new(&runtime_db))?;
        assert!(store
            .load_discovery_persisted_rebuild_state_read_only()?
            .is_none());
        let output = run(Config {
            mode: Mode::InspectPersistedRebuildState,
            runtime_db: runtime_db.clone(),
            recent_raw_db: None,
            json: true,
        })?;
        let json: serde_json::Value =
            serde_json::from_str(&output).context("invalid json output")?;
        assert_eq!(json["persisted_rebuild_checkpoint_exists"], false);
        let store_after = SqliteStore::open(Path::new(&runtime_db))?;
        assert!(store_after
            .load_discovery_persisted_rebuild_state_read_only()?
            .is_none());
        assert_eq!(
            DiscoveryPersistedRebuildPhase::Replay.as_str(),
            "replay",
            "keep a public phase helper in scope so the bin test compiles against the same public storage surface used by the operator tool"
        );
        Ok(())
    }
}
