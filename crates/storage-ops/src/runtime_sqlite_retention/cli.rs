use anyhow::{anyhow, bail, Context, Result};
use copybot_config::load_from_path;
use serde::Serialize;
use std::path::PathBuf;
use std::process::Command;

use super::common::resolve_db_path;

pub(super) const USAGE: &str = "usage: copybot_runtime_sqlite_retention_maintenance --config <path> --json [--db-path <path>] [--service-name <name>] [--allow-service-active] [--commit] [--observed-retention-days <n>] [--observed-batch-size <n>] [--max-observed-rows <n>] [--canary-retention-days <n>] [--canary-batch-size <n>] [--max-canary-rows <n>] [--create-canary-ts-indexes] [--checkpoint-truncate] [--vacuum-into <path>] [--rebuild-into <path>]";

const DEFAULT_SERVICE_NAME: &str = "solana-copy-bot.service";
const DEFAULT_OBSERVED_BATCH_SIZE: u64 = 10_000;
const DEFAULT_CANARY_BATCH_SIZE: u64 = 1_000;
const DEFAULT_CANARY_RETENTION_DAYS: u32 = 30;
const MAX_BATCH_SIZE: u64 = 100_000;

#[derive(Debug, Clone)]
pub(super) struct Cli {
    pub(super) config_path: PathBuf,
    pub(super) db_path_override: Option<PathBuf>,
    pub(super) service_name: String,
    pub(super) json: bool,
    pub(super) allow_service_active: bool,
    pub(super) commit: bool,
    pub(super) observed_retention_days: Option<u32>,
    pub(super) observed_batch_size: u64,
    pub(super) max_observed_rows: u64,
    pub(super) canary_retention_days: u32,
    pub(super) canary_batch_size: u64,
    pub(super) max_canary_rows: u64,
    pub(super) create_canary_ts_indexes: bool,
    pub(super) checkpoint_truncate: bool,
    pub(super) vacuum_into: Option<PathBuf>,
    pub(super) rebuild_into: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ServiceState {
    pub(super) active_state: String,
    pub(super) active: bool,
    pub(super) substate: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct RuntimeSqliteRetentionReport {
    pub(super) production_green: bool,
    pub(super) committed: bool,
    pub(super) service_name: String,
    pub(super) service_active_state: Option<String>,
    pub(super) service_active: Option<bool>,
    pub(super) service_substate: Option<String>,
    pub(super) runtime_db_path: Option<String>,
    pub(super) before_db_bytes: Option<u64>,
    pub(super) before_wal_bytes: Option<u64>,
    pub(super) before_shm_bytes: Option<u64>,
    pub(super) after_db_bytes: Option<u64>,
    pub(super) after_wal_bytes: Option<u64>,
    pub(super) after_shm_bytes: Option<u64>,
    pub(super) observed_cutoff: Option<String>,
    pub(super) observed_deleted_rows: u64,
    pub(super) observed_delete_batches: u64,
    pub(super) canary_cutoff: Option<String>,
    pub(super) canary_event_deleted_rows: u64,
    pub(super) canary_provider_sample_deleted_rows: u64,
    pub(super) canary_shadow_gate_deleted_rows: u64,
    pub(super) canary_delete_batches: u64,
    pub(super) canary_ts_indexes_created: bool,
    pub(super) checkpoint_truncate_attempted: bool,
    pub(super) checkpoint_busy: Option<i64>,
    pub(super) checkpoint_log_frames: Option<i64>,
    pub(super) checkpoint_checkpointed_frames: Option<i64>,
    pub(super) vacuum_into: Option<String>,
    pub(super) vacuum_attempted: bool,
    pub(super) rebuild_into: Option<String>,
    pub(super) rebuild_attempted: bool,
    pub(super) rebuild_tables_copied: u64,
    pub(super) rebuild_rows_copied: u64,
    pub(super) rebuild_observed_rows_copied: u64,
    pub(super) rebuild_sol_leg_rows_copied: u64,
    pub(super) rebuild_canary_rows_copied: u64,
    pub(super) rebuild_indexes_created: u64,
    pub(super) rebuild_triggers_created: u64,
    pub(super) rebuild_views_created: u64,
    pub(super) rebuild_integrity_check: Option<String>,
    pub(super) rebuild_foreign_key_violations: Option<u64>,
    pub(super) maintenance_outcome: String,
    pub(super) reason: String,
    pub(super) service_safe_next_action: String,
    pub(super) error: Option<String>,
}

impl RuntimeSqliteRetentionReport {
    pub(super) fn exit_code(&self) -> i32 {
        match self.maintenance_outcome.as_str() {
            "completed" | "dry_run" | "skipped_noop" => 0,
            _ => 1,
        }
    }
}

impl Cli {
    pub(super) fn default_for_error() -> Self {
        Self {
            config_path: PathBuf::new(),
            db_path_override: None,
            service_name: DEFAULT_SERVICE_NAME.to_string(),
            json: true,
            allow_service_active: false,
            commit: false,
            observed_retention_days: None,
            observed_batch_size: DEFAULT_OBSERVED_BATCH_SIZE,
            max_observed_rows: 0,
            canary_retention_days: DEFAULT_CANARY_RETENTION_DAYS,
            canary_batch_size: DEFAULT_CANARY_BATCH_SIZE,
            max_canary_rows: 0,
            create_canary_ts_indexes: false,
            checkpoint_truncate: false,
            vacuum_into: None,
            rebuild_into: None,
        }
    }
}

pub(super) fn parse_args_from<I>(args: I) -> Result<Option<Cli>>
where
    I: IntoIterator<Item = String>,
{
    let mut cli = Cli::default_for_error();
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => cli.config_path = PathBuf::from(parse_string("--config", args.next())?),
            "--db-path" => {
                cli.db_path_override = Some(PathBuf::from(parse_string("--db-path", args.next())?))
            }
            "--service-name" => cli.service_name = parse_string("--service-name", args.next())?,
            "--json" => cli.json = true,
            "--allow-service-active" => cli.allow_service_active = true,
            "--commit" => cli.commit = true,
            "--observed-retention-days" => {
                cli.observed_retention_days =
                    Some(parse_u32("--observed-retention-days", args.next())?);
            }
            "--observed-batch-size" => {
                cli.observed_batch_size = parse_u64("--observed-batch-size", args.next())?;
            }
            "--max-observed-rows" => {
                cli.max_observed_rows = parse_u64("--max-observed-rows", args.next())?;
            }
            "--canary-retention-days" => {
                cli.canary_retention_days = parse_u32("--canary-retention-days", args.next())?;
            }
            "--canary-batch-size" => {
                cli.canary_batch_size = parse_u64("--canary-batch-size", args.next())?;
            }
            "--max-canary-rows" => {
                cli.max_canary_rows = parse_u64("--max-canary-rows", args.next())?;
            }
            "--create-canary-ts-indexes" => cli.create_canary_ts_indexes = true,
            "--checkpoint-truncate" => cli.checkpoint_truncate = true,
            "--vacuum-into" => {
                cli.vacuum_into = Some(PathBuf::from(parse_string("--vacuum-into", args.next())?));
            }
            "--rebuild-into" => {
                cli.rebuild_into =
                    Some(PathBuf::from(parse_string("--rebuild-into", args.next())?));
            }
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }
    if cli.config_path.as_os_str().is_empty() {
        bail!("missing required --config");
    }
    Ok(Some(cli))
}

pub(super) fn validate_cli(cli: &Cli) -> Result<()> {
    if !cli.json {
        bail!("--json is required for retention maintenance");
    }
    if cli.observed_batch_size == 0 || cli.observed_batch_size > MAX_BATCH_SIZE {
        bail!("--observed-batch-size must be in 1..={MAX_BATCH_SIZE}");
    }
    if cli.canary_batch_size == 0 || cli.canary_batch_size > MAX_BATCH_SIZE {
        bail!("--canary-batch-size must be in 1..={MAX_BATCH_SIZE}");
    }
    if cli.canary_retention_days == 0 {
        bail!("--canary-retention-days must be greater than zero");
    }
    if let Some(days) = cli.observed_retention_days {
        if days == 0 {
            bail!("--observed-retention-days must be greater than zero");
        }
    }
    if let Some(path) = &cli.vacuum_into {
        if path.exists() {
            bail!("--vacuum-into output already exists: {}", path.display());
        }
        if path
            .parent()
            .is_none_or(|parent| parent.as_os_str().is_empty())
        {
            bail!("--vacuum-into must include an output directory");
        }
    }
    if let Some(path) = &cli.rebuild_into {
        if path.exists() {
            bail!("--rebuild-into output already exists: {}", path.display());
        }
        if path
            .parent()
            .is_none_or(|parent| parent.as_os_str().is_empty())
        {
            bail!("--rebuild-into must include an output directory");
        }
        if cli.vacuum_into.is_some() {
            bail!("--rebuild-into cannot be combined with --vacuum-into");
        }
        if cli.max_observed_rows > 0
            || cli.max_canary_rows > 0
            || cli.create_canary_ts_indexes
            || cli.checkpoint_truncate
        {
            bail!("--rebuild-into cannot be combined with delete, index, or checkpoint mutations");
        }
    }
    Ok(())
}

pub(super) fn resolve_runtime_db_path(cli: &Cli) -> Result<(PathBuf, u32)> {
    if let (Some(db_path), Some(observed_days)) =
        (&cli.db_path_override, cli.observed_retention_days)
    {
        return Ok((db_path.clone(), observed_days.max(1)));
    }
    let loaded = load_from_path(&cli.config_path)
        .with_context(|| format!("failed loading config {}", cli.config_path.display()))?;
    let db_path = cli
        .db_path_override
        .clone()
        .unwrap_or_else(|| resolve_db_path(&cli.config_path, &loaded.sqlite.path));
    let observed_days = cli
        .observed_retention_days
        .unwrap_or(loaded.discovery.observed_swaps_retention_days.max(1));
    Ok((db_path, observed_days))
}

pub(super) fn load_service_state_from_systemctl(service_name: &str) -> Result<ServiceState> {
    let output = Command::new("systemctl")
        .args([
            "show",
            service_name,
            "--property=ActiveState",
            "--property=SubState",
            "--no-pager",
        ])
        .output()
        .with_context(|| format!("failed to run systemctl show {service_name}"))?;
    if !output.status.success() {
        bail!(
            "systemctl show {service_name} failed with status {:?}: {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    parse_systemctl_show_output(&String::from_utf8_lossy(&output.stdout))
}

pub(super) fn parse_systemctl_show_output(output: &str) -> Result<ServiceState> {
    let mut active_state = None;
    let mut substate = None;
    for line in output.lines() {
        if let Some(value) = line.strip_prefix("ActiveState=") {
            active_state = Some(value.trim().to_string());
        } else if let Some(value) = line.strip_prefix("SubState=") {
            substate = Some(value.trim().to_string());
        }
    }
    let active_state =
        active_state.ok_or_else(|| anyhow!("systemctl output missing ActiveState"))?;
    Ok(ServiceState {
        active: active_state == "active",
        active_state,
        substate,
    })
}

fn parse_string(flag: &str, value: Option<String>) -> Result<String> {
    let raw = value.ok_or_else(|| anyhow!("missing value for {flag}"))?;
    let value = raw.trim();
    if value.is_empty() {
        bail!("{flag} cannot be empty");
    }
    Ok(value.to_string())
}

fn parse_u64(flag: &str, value: Option<String>) -> Result<u64> {
    let raw = parse_string(flag, value)?;
    raw.parse::<u64>()
        .with_context(|| format!("{flag} must be an unsigned integer; got {raw}"))
}

fn parse_u32(flag: &str, value: Option<String>) -> Result<u32> {
    let raw = parse_string(flag, value)?;
    raw.parse::<u32>()
        .with_context(|| format!("{flag} must be an unsigned integer; got {raw}"))
}
