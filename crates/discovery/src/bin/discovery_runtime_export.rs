use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::load_from_path;
use copybot_discovery::runtime_restore_ops::{
    artifact_archive_path, artifact_latest_path, copy_atomic, load_json, prune_rotated_archives,
    resolve_db_path, resolve_relative_to_config, write_json_atomic, ARTIFACT_ARCHIVE_PREFIX,
    ARTIFACT_ARCHIVE_SUFFIX,
};
use copybot_discovery::{
    DiscoveryService, RecentRawCatchUpDiagnostic, RecentRawPromotedRetentionContractDiagnostic,
    RecentRawPromotionBlockerDiagnostic, RecentRawReplacementProgressContractDiagnostic,
    RecentRawReplacementPromotionContractDiagnostic, RecentRawSourceWindowContractDiagnostic,
    RecentRawStagedBirthDiagnostic, RecentRawStagedLineageDiagnostic,
    RecentRawStagedRegressionDiagnostic, RecentRawStagedWindowSeedingDiagnostic,
};
use copybot_storage::{DiscoveryRuntimeArtifact, SqliteStore};
use serde::Serialize;
use std::env;
use std::path::{Path, PathBuf};

const USAGE: &str = "usage:
  discovery_runtime_export --config <path> [--db-path <path>] (--output <path> | --scheduled) [--force] [--json] [--now <rfc3339>]
  discovery_runtime_export --explain-recent-raw-promotion-blocker --state-root <path> [--json]
  discovery_runtime_export --explain-recent-raw-catch-up-status --state-root <path> [--json]
  discovery_runtime_export --explain-recent-raw-source-window-contract --state-root <path> [--json]
  discovery_runtime_export --explain-recent-raw-promoted-retention-contract --state-root <path> [--json]
  discovery_runtime_export --explain-recent-raw-replacement-promotion-contract --state-root <path> [--json]
  discovery_runtime_export --explain-recent-raw-replacement-progress-contract --state-root <path> [--json]
  discovery_runtime_export --explain-recent-raw-staged-lineage --state-root <path> [--json]
  discovery_runtime_export --explain-recent-raw-staged-regression --state-root <path> [--json]
  discovery_runtime_export --explain-recent-raw-staged-window-seeding --state-root <path> [--json]
  discovery_runtime_export --explain-recent-raw-staged-birth --state-root <path> [--json]";

fn main() -> Result<()> {
    let Some(command) = parse_args()? else {
        println!("{USAGE}");
        return Ok(());
    };
    println!("{}", run_command(command)?);
    Ok(())
}

#[derive(Debug, Clone)]
struct Config {
    config_path: PathBuf,
    db_path: Option<PathBuf>,
    output_path: Option<PathBuf>,
    scheduled: bool,
    force: bool,
    json: bool,
    now: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct ExplainRecentRawPromotionBlockerConfig {
    state_root: PathBuf,
    json: bool,
}

#[derive(Debug, Clone)]
struct ExplainRecentRawCatchUpStatusConfig {
    state_root: PathBuf,
    json: bool,
}

#[derive(Debug, Clone)]
struct ExplainRecentRawSourceWindowContractConfig {
    state_root: PathBuf,
    json: bool,
}

#[derive(Debug, Clone)]
struct ExplainRecentRawPromotedRetentionContractConfig {
    state_root: PathBuf,
    json: bool,
}

#[derive(Debug, Clone)]
struct ExplainRecentRawReplacementPromotionContractConfig {
    state_root: PathBuf,
    json: bool,
}

#[derive(Debug, Clone)]
struct ExplainRecentRawReplacementProgressContractConfig {
    state_root: PathBuf,
    json: bool,
}

#[derive(Debug, Clone)]
struct ExplainRecentRawStagedLineageConfig {
    state_root: PathBuf,
    json: bool,
}

#[derive(Debug, Clone)]
struct ExplainRecentRawStagedRegressionConfig {
    state_root: PathBuf,
    json: bool,
}

#[derive(Debug, Clone)]
struct ExplainRecentRawStagedBirthConfig {
    state_root: PathBuf,
    json: bool,
}

#[derive(Debug, Clone)]
struct ExplainRecentRawStagedWindowSeedingConfig {
    state_root: PathBuf,
    json: bool,
}

#[derive(Debug, Clone)]
enum Command {
    Export(Config),
    ExplainRecentRawPromotionBlocker(ExplainRecentRawPromotionBlockerConfig),
    ExplainRecentRawCatchUpStatus(ExplainRecentRawCatchUpStatusConfig),
    ExplainRecentRawSourceWindowContract(ExplainRecentRawSourceWindowContractConfig),
    ExplainRecentRawPromotedRetentionContract(ExplainRecentRawPromotedRetentionContractConfig),
    ExplainRecentRawReplacementPromotionContract(
        ExplainRecentRawReplacementPromotionContractConfig,
    ),
    ExplainRecentRawReplacementProgressContract(ExplainRecentRawReplacementProgressContractConfig),
    ExplainRecentRawStagedLineage(ExplainRecentRawStagedLineageConfig),
    ExplainRecentRawStagedRegression(ExplainRecentRawStagedRegressionConfig),
    ExplainRecentRawStagedBirth(ExplainRecentRawStagedBirthConfig),
    ExplainRecentRawStagedWindowSeeding(ExplainRecentRawStagedWindowSeedingConfig),
}

#[derive(Debug, Clone, Serialize)]
struct ExportOutput {
    event: String,
    state: String,
    config_path: String,
    db_path: String,
    output_path: String,
    archive_path: Option<String>,
    cadence_minutes: Option<u64>,
    retention: Option<usize>,
    pruned_archive_paths: Vec<String>,
    exported_at: DateTime<Utc>,
    publication_runtime_mode: String,
    publication_reason: String,
    publication_truth_complete: bool,
    fresh_under_export_gate: bool,
    last_published_at: Option<DateTime<Utc>>,
    last_published_window_start: Option<DateTime<Utc>>,
    published_wallet_count: usize,
    wallet_metrics_snapshot_rows: usize,
    fresh_under_current_gate: bool,
    runtime_cursor_ts: DateTime<Utc>,
    runtime_cursor_slot: u64,
    runtime_cursor_signature: String,
}

fn parse_args() -> Result<Option<Command>> {
    parse_args_from(env::args().skip(1))
}

fn parse_args_from<I>(args: I) -> Result<Option<Command>>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut explain_recent_raw_promotion_blocker = false;
    let mut explain_recent_raw_catch_up_status = false;
    let mut explain_recent_raw_source_window_contract = false;
    let mut explain_recent_raw_promoted_retention_contract = false;
    let mut explain_recent_raw_replacement_promotion_contract = false;
    let mut explain_recent_raw_replacement_progress_contract = false;
    let mut explain_recent_raw_staged_lineage = false;
    let mut explain_recent_raw_staged_regression = false;
    let mut explain_recent_raw_staged_birth = false;
    let mut explain_recent_raw_staged_window_seeding = false;
    let mut config_path: Option<PathBuf> = None;
    let mut state_root: Option<PathBuf> = None;
    let mut db_path: Option<PathBuf> = None;
    let mut output_path: Option<PathBuf> = None;
    let mut scheduled = false;
    let mut force = false;
    let mut json = false;
    let mut now: Option<DateTime<Utc>> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--explain-recent-raw-promotion-blocker" => {
                explain_recent_raw_promotion_blocker = true;
            }
            "--explain-recent-raw-catch-up-status" => {
                explain_recent_raw_catch_up_status = true;
            }
            "--explain-recent-raw-source-window-contract" => {
                explain_recent_raw_source_window_contract = true;
            }
            "--explain-recent-raw-promoted-retention-contract" => {
                explain_recent_raw_promoted_retention_contract = true;
            }
            "--explain-recent-raw-replacement-promotion-contract" => {
                explain_recent_raw_replacement_promotion_contract = true;
            }
            "--explain-recent-raw-replacement-progress-contract" => {
                explain_recent_raw_replacement_progress_contract = true;
            }
            "--explain-recent-raw-staged-lineage" => {
                explain_recent_raw_staged_lineage = true;
            }
            "--explain-recent-raw-staged-regression" => {
                explain_recent_raw_staged_regression = true;
            }
            "--explain-recent-raw-staged-birth" => {
                explain_recent_raw_staged_birth = true;
            }
            "--explain-recent-raw-staged-window-seeding" => {
                explain_recent_raw_staged_window_seeding = true;
            }
            "--config" => {
                config_path = Some(PathBuf::from(parse_string_arg("--config", args.next())?))
            }
            "--state-root" => {
                state_root = Some(PathBuf::from(parse_string_arg(
                    "--state-root",
                    args.next(),
                )?))
            }
            "--db-path" => {
                db_path = Some(PathBuf::from(parse_string_arg("--db-path", args.next())?))
            }
            "--output" => {
                output_path = Some(PathBuf::from(parse_string_arg("--output", args.next())?))
            }
            "--scheduled" => scheduled = true,
            "--force" => force = true,
            "--json" => json = true,
            "--now" => now = Some(parse_ts_arg("--now", args.next())?),
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    let explain_mode_count = usize::from(explain_recent_raw_promotion_blocker)
        + usize::from(explain_recent_raw_catch_up_status)
        + usize::from(explain_recent_raw_source_window_contract)
        + usize::from(explain_recent_raw_promoted_retention_contract)
        + usize::from(explain_recent_raw_replacement_promotion_contract)
        + usize::from(explain_recent_raw_replacement_progress_contract)
        + usize::from(explain_recent_raw_staged_lineage)
        + usize::from(explain_recent_raw_staged_regression)
        + usize::from(explain_recent_raw_staged_birth)
        + usize::from(explain_recent_raw_staged_window_seeding);
    if explain_mode_count > 1 {
        bail!(
            "--explain-recent-raw-promotion-blocker, --explain-recent-raw-catch-up-status, --explain-recent-raw-source-window-contract, --explain-recent-raw-promoted-retention-contract, --explain-recent-raw-replacement-promotion-contract, --explain-recent-raw-replacement-progress-contract, --explain-recent-raw-staged-lineage, --explain-recent-raw-staged-regression, --explain-recent-raw-staged-window-seeding, and --explain-recent-raw-staged-birth are mutually exclusive"
        );
    }

    if explain_recent_raw_promotion_blocker {
        if config_path.is_some()
            || db_path.is_some()
            || output_path.is_some()
            || scheduled
            || force
            || now.is_some()
        {
            bail!(
                "--explain-recent-raw-promotion-blocker only accepts --state-root and optional --json"
            );
        }
        return Ok(Some(Command::ExplainRecentRawPromotionBlocker(
            ExplainRecentRawPromotionBlockerConfig {
                state_root: state_root.ok_or_else(|| anyhow!("missing required --state-root"))?,
                json,
            },
        )));
    }

    if explain_recent_raw_catch_up_status {
        if config_path.is_some()
            || db_path.is_some()
            || output_path.is_some()
            || scheduled
            || force
            || now.is_some()
        {
            bail!("--explain-recent-raw-catch-up-status only accepts --state-root and optional --json");
        }
        return Ok(Some(Command::ExplainRecentRawCatchUpStatus(
            ExplainRecentRawCatchUpStatusConfig {
                state_root: state_root.ok_or_else(|| anyhow!("missing required --state-root"))?,
                json,
            },
        )));
    }

    if explain_recent_raw_source_window_contract {
        if config_path.is_some()
            || db_path.is_some()
            || output_path.is_some()
            || scheduled
            || force
            || now.is_some()
        {
            bail!("--explain-recent-raw-source-window-contract only accepts --state-root and optional --json");
        }
        return Ok(Some(Command::ExplainRecentRawSourceWindowContract(
            ExplainRecentRawSourceWindowContractConfig {
                state_root: state_root.ok_or_else(|| anyhow!("missing required --state-root"))?,
                json,
            },
        )));
    }

    if explain_recent_raw_promoted_retention_contract {
        if config_path.is_some()
            || db_path.is_some()
            || output_path.is_some()
            || scheduled
            || force
            || now.is_some()
        {
            bail!("--explain-recent-raw-promoted-retention-contract only accepts --state-root and optional --json");
        }
        return Ok(Some(Command::ExplainRecentRawPromotedRetentionContract(
            ExplainRecentRawPromotedRetentionContractConfig {
                state_root: state_root.ok_or_else(|| anyhow!("missing required --state-root"))?,
                json,
            },
        )));
    }

    if explain_recent_raw_replacement_promotion_contract {
        if config_path.is_some()
            || db_path.is_some()
            || output_path.is_some()
            || scheduled
            || force
            || now.is_some()
        {
            bail!("--explain-recent-raw-replacement-promotion-contract only accepts --state-root and optional --json");
        }
        return Ok(Some(Command::ExplainRecentRawReplacementPromotionContract(
            ExplainRecentRawReplacementPromotionContractConfig {
                state_root: state_root.ok_or_else(|| anyhow!("missing required --state-root"))?,
                json,
            },
        )));
    }

    if explain_recent_raw_replacement_progress_contract {
        if config_path.is_some()
            || db_path.is_some()
            || output_path.is_some()
            || scheduled
            || force
            || now.is_some()
        {
            bail!("--explain-recent-raw-replacement-progress-contract only accepts --state-root and optional --json");
        }
        return Ok(Some(Command::ExplainRecentRawReplacementProgressContract(
            ExplainRecentRawReplacementProgressContractConfig {
                state_root: state_root.ok_or_else(|| anyhow!("missing required --state-root"))?,
                json,
            },
        )));
    }

    if explain_recent_raw_staged_lineage {
        if config_path.is_some()
            || db_path.is_some()
            || output_path.is_some()
            || scheduled
            || force
            || now.is_some()
        {
            bail!(
                "--explain-recent-raw-staged-lineage only accepts --state-root and optional --json"
            );
        }
        return Ok(Some(Command::ExplainRecentRawStagedLineage(
            ExplainRecentRawStagedLineageConfig {
                state_root: state_root.ok_or_else(|| anyhow!("missing required --state-root"))?,
                json,
            },
        )));
    }

    if explain_recent_raw_staged_regression {
        if config_path.is_some()
            || db_path.is_some()
            || output_path.is_some()
            || scheduled
            || force
            || now.is_some()
        {
            bail!("--explain-recent-raw-staged-regression only accepts --state-root and optional --json");
        }
        return Ok(Some(Command::ExplainRecentRawStagedRegression(
            ExplainRecentRawStagedRegressionConfig {
                state_root: state_root.ok_or_else(|| anyhow!("missing required --state-root"))?,
                json,
            },
        )));
    }

    if explain_recent_raw_staged_birth {
        if config_path.is_some()
            || db_path.is_some()
            || output_path.is_some()
            || scheduled
            || force
            || now.is_some()
        {
            bail!(
                "--explain-recent-raw-staged-birth only accepts --state-root and optional --json"
            );
        }
        return Ok(Some(Command::ExplainRecentRawStagedBirth(
            ExplainRecentRawStagedBirthConfig {
                state_root: state_root.ok_or_else(|| anyhow!("missing required --state-root"))?,
                json,
            },
        )));
    }

    if explain_recent_raw_staged_window_seeding {
        if config_path.is_some()
            || db_path.is_some()
            || output_path.is_some()
            || scheduled
            || force
            || now.is_some()
        {
            bail!("--explain-recent-raw-staged-window-seeding only accepts --state-root and optional --json");
        }
        return Ok(Some(Command::ExplainRecentRawStagedWindowSeeding(
            ExplainRecentRawStagedWindowSeedingConfig {
                state_root: state_root.ok_or_else(|| anyhow!("missing required --state-root"))?,
                json,
            },
        )));
    }

    if state_root.is_some() {
        bail!(
            "--state-root requires --explain-recent-raw-promotion-blocker, --explain-recent-raw-catch-up-status, --explain-recent-raw-source-window-contract, --explain-recent-raw-promoted-retention-contract, --explain-recent-raw-replacement-promotion-contract, --explain-recent-raw-replacement-progress-contract, --explain-recent-raw-staged-lineage, --explain-recent-raw-staged-regression, --explain-recent-raw-staged-window-seeding, or --explain-recent-raw-staged-birth"
        );
    }
    if scheduled == output_path.is_some() {
        bail!("exactly one of --output or --scheduled must be provided");
    }

    Ok(Some(Command::Export(Config {
        config_path: config_path.ok_or_else(|| anyhow!("missing required --config"))?,
        db_path,
        output_path,
        scheduled,
        force,
        json,
        now: now.unwrap_or_else(Utc::now),
    })))
}

fn parse_ts_arg(flag: &str, value: Option<String>) -> Result<DateTime<Utc>> {
    let raw = parse_string_arg(flag, value)?;
    DateTime::parse_from_rfc3339(&raw)
        .map(|ts| ts.with_timezone(&Utc))
        .with_context(|| format!("invalid {flag} rfc3339 timestamp: {raw}"))
}

fn parse_string_arg(flag: &str, value: Option<String>) -> Result<String> {
    let raw = value.ok_or_else(|| anyhow!("missing value for {flag}"))?;
    let trimmed = raw.trim().to_string();
    if trimmed.is_empty() {
        bail!("{flag} cannot be empty");
    }
    Ok(trimmed)
}

fn run_command(command: Command) -> Result<String> {
    match command {
        Command::Export(config) => run(config),
        Command::ExplainRecentRawPromotionBlocker(config) => {
            let diagnostic = DiscoveryService::explain_recent_raw_promotion_blocker_read_only(
                &config.state_root,
            )?;
            if config.json {
                serde_json::to_string_pretty(&diagnostic)
                    .context("failed serializing recent_raw promotion blocker json")
            } else {
                Ok(render_recent_raw_promotion_blocker_human(&diagnostic))
            }
        }
        Command::ExplainRecentRawCatchUpStatus(config) => {
            let diagnostic =
                DiscoveryService::explain_recent_raw_catch_up_status_read_only(&config.state_root)?;
            if config.json {
                serde_json::to_string_pretty(&diagnostic)
                    .context("failed serializing recent_raw catch-up status json")
            } else {
                Ok(render_recent_raw_catch_up_status_human(&diagnostic))
            }
        }
        Command::ExplainRecentRawSourceWindowContract(config) => {
            let diagnostic = DiscoveryService::explain_recent_raw_source_window_contract_read_only(
                &config.state_root,
            )?;
            if config.json {
                serde_json::to_string_pretty(&diagnostic)
                    .context("failed serializing recent_raw source window contract json")
            } else {
                Ok(render_recent_raw_source_window_contract_human(&diagnostic))
            }
        }
        Command::ExplainRecentRawPromotedRetentionContract(config) => {
            let diagnostic =
                DiscoveryService::explain_recent_raw_promoted_retention_contract_read_only(
                    &config.state_root,
                )?;
            if config.json {
                serde_json::to_string_pretty(&diagnostic)
                    .context("failed serializing recent_raw promoted retention contract json")
            } else {
                Ok(render_recent_raw_promoted_retention_contract_human(
                    &diagnostic,
                ))
            }
        }
        Command::ExplainRecentRawReplacementPromotionContract(config) => {
            let diagnostic =
                DiscoveryService::explain_recent_raw_replacement_promotion_contract_read_only(
                    &config.state_root,
                )?;
            if config.json {
                serde_json::to_string_pretty(&diagnostic)
                    .context("failed serializing recent_raw replacement promotion contract json")
            } else {
                Ok(render_recent_raw_replacement_promotion_contract_human(
                    &diagnostic,
                ))
            }
        }
        Command::ExplainRecentRawReplacementProgressContract(config) => {
            let diagnostic =
                DiscoveryService::explain_recent_raw_replacement_progress_contract_read_only(
                    &config.state_root,
                )?;
            if config.json {
                serde_json::to_string_pretty(&diagnostic)
                    .context("failed serializing recent_raw replacement progress contract json")
            } else {
                Ok(render_recent_raw_replacement_progress_contract_human(
                    &diagnostic,
                ))
            }
        }
        Command::ExplainRecentRawStagedLineage(config) => {
            let diagnostic =
                DiscoveryService::explain_recent_raw_staged_lineage_read_only(&config.state_root)?;
            if config.json {
                serde_json::to_string_pretty(&diagnostic)
                    .context("failed serializing recent_raw staged lineage json")
            } else {
                Ok(render_recent_raw_staged_lineage_human(&diagnostic))
            }
        }
        Command::ExplainRecentRawStagedRegression(config) => {
            let diagnostic = DiscoveryService::explain_recent_raw_staged_regression_read_only(
                &config.state_root,
            )?;
            if config.json {
                serde_json::to_string_pretty(&diagnostic)
                    .context("failed serializing recent_raw staged regression json")
            } else {
                Ok(render_recent_raw_staged_regression_human(&diagnostic))
            }
        }
        Command::ExplainRecentRawStagedBirth(config) => {
            let diagnostic =
                DiscoveryService::explain_recent_raw_staged_birth_read_only(&config.state_root)?;
            if config.json {
                serde_json::to_string_pretty(&diagnostic)
                    .context("failed serializing recent_raw staged birth json")
            } else {
                Ok(render_recent_raw_staged_birth_human(&diagnostic))
            }
        }
        Command::ExplainRecentRawStagedWindowSeeding(config) => {
            let diagnostic = DiscoveryService::explain_recent_raw_staged_window_seeding_read_only(
                &config.state_root,
            )?;
            if config.json {
                serde_json::to_string_pretty(&diagnostic)
                    .context("failed serializing recent_raw staged window seeding json")
            } else {
                Ok(render_recent_raw_staged_window_seeding_human(&diagnostic))
            }
        }
    }
}

fn run(config: Config) -> Result<String> {
    let loaded_config = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let db_path = resolve_db_path(
        &config.config_path,
        config.db_path.as_deref(),
        &loaded_config.sqlite.path,
    );
    let store = SqliteStore::open(Path::new(&db_path))
        .with_context(|| format!("failed opening sqlite db {}", db_path.display()))?;
    let discovery = DiscoveryService::new(
        loaded_config.discovery.clone(),
        loaded_config.shadow.clone(),
    );

    let output = if config.scheduled {
        run_scheduled(
            &config,
            &loaded_config.runtime_restore_ops.artifact_dir,
            loaded_config.runtime_restore_ops.artifact_cadence_minutes,
            loaded_config.runtime_restore_ops.artifact_retention,
            &db_path,
            &store,
            &discovery,
        )?
    } else {
        let output_path = resolve_relative_to_config(
            &config.config_path,
            config
                .output_path
                .as_deref()
                .expect("validated explicit output path"),
        );
        let artifact =
            export_runtime_artifact(&store, &discovery, config.now).context("artifact export")?;
        let freshness = discovery.assess_runtime_artifact_freshness(&artifact, config.now);
        write_json_atomic(&output_path, &artifact)
            .with_context(|| format!("failed writing {}", output_path.display()))?;
        render_output(
            "written",
            &config.config_path,
            &db_path,
            &output_path,
            None,
            None,
            None,
            &[],
            &artifact,
            freshness.fresh_under_export_gate,
            freshness.fresh_under_current_gate,
        )
    };

    if config.json {
        serde_json::to_string_pretty(&output).context("failed serializing export output json")
    } else {
        Ok(render_human(&output))
    }
}

fn run_scheduled(
    config: &Config,
    configured_artifact_dir: &str,
    cadence_minutes: u64,
    retention: usize,
    db_path: &Path,
    store: &SqliteStore,
    discovery: &DiscoveryService,
) -> Result<ExportOutput> {
    let artifact_dir =
        resolve_relative_to_config(&config.config_path, Path::new(configured_artifact_dir));
    let latest_path = artifact_latest_path(&artifact_dir);
    if !config.force && latest_path.exists() {
        let latest_artifact: DiscoveryRuntimeArtifact = load_json(&latest_path)?;
        if config
            .now
            .signed_duration_since(latest_artifact.exported_at)
            < Duration::minutes(cadence_minutes.max(1) as i64)
        {
            let freshness =
                discovery.assess_runtime_artifact_freshness(&latest_artifact, config.now);
            return Ok(render_output(
                "skipped_not_due",
                &config.config_path,
                db_path,
                &latest_path,
                None,
                Some(cadence_minutes),
                Some(retention),
                &[],
                &latest_artifact,
                freshness.fresh_under_export_gate,
                freshness.fresh_under_current_gate,
            ));
        }
    }

    let artifact =
        export_runtime_artifact(store, discovery, config.now).context("artifact export")?;
    let archive_path = artifact_archive_path(&artifact_dir, config.now);
    write_json_atomic(&archive_path, &artifact)
        .with_context(|| format!("failed writing {}", archive_path.display()))?;
    copy_atomic(&archive_path, &latest_path)
        .with_context(|| format!("failed updating {}", latest_path.display()))?;
    let pruned = prune_rotated_archives(
        &artifact_dir,
        ARTIFACT_ARCHIVE_PREFIX,
        ARTIFACT_ARCHIVE_SUFFIX,
        retention,
    )?;
    let freshness = discovery.assess_runtime_artifact_freshness(&artifact, config.now);
    Ok(render_output(
        "written",
        &config.config_path,
        db_path,
        &latest_path,
        Some(&archive_path),
        Some(cadence_minutes),
        Some(retention),
        &pruned,
        &artifact,
        freshness.fresh_under_export_gate,
        freshness.fresh_under_current_gate,
    ))
}

fn export_runtime_artifact(
    store: &SqliteStore,
    discovery: &DiscoveryService,
    now: DateTime<Utc>,
) -> Result<DiscoveryRuntimeArtifact> {
    store.export_discovery_runtime_artifact(now, discovery.publication_freshness_gate())
}

fn render_output(
    state: &str,
    config_path: &Path,
    db_path: &Path,
    output_path: &Path,
    archive_path: Option<&Path>,
    cadence_minutes: Option<u64>,
    retention: Option<usize>,
    pruned_archive_paths: &[PathBuf],
    artifact: &DiscoveryRuntimeArtifact,
    fresh_under_export_gate: bool,
    fresh_under_current_gate: bool,
) -> ExportOutput {
    ExportOutput {
        event: "discovery_runtime_export".to_string(),
        state: state.to_string(),
        config_path: config_path.display().to_string(),
        db_path: db_path.display().to_string(),
        output_path: output_path.display().to_string(),
        archive_path: archive_path.map(|path| path.display().to_string()),
        cadence_minutes,
        retention,
        pruned_archive_paths: pruned_archive_paths
            .iter()
            .map(|path| path.display().to_string())
            .collect(),
        exported_at: artifact.exported_at,
        publication_runtime_mode: artifact.publication_state.runtime_mode.as_str().to_string(),
        publication_reason: artifact.publication_state.reason.clone(),
        publication_truth_complete: artifact.publication_state.has_complete_publication_truth(),
        fresh_under_export_gate,
        last_published_at: artifact.publication_state.last_published_at,
        last_published_window_start: artifact.publication_state.last_published_window_start,
        published_wallet_count: artifact
            .publication_state
            .published_wallet_ids
            .as_ref()
            .map(Vec::len)
            .unwrap_or(0),
        wallet_metrics_snapshot_rows: artifact.published_wallet_metrics_snapshot.len(),
        fresh_under_current_gate,
        runtime_cursor_ts: artifact.runtime_cursor.ts_utc,
        runtime_cursor_slot: artifact.runtime_cursor.slot,
        runtime_cursor_signature: artifact.runtime_cursor.signature.clone(),
    }
}

fn render_human(output: &ExportOutput) -> String {
    [
        format!("event={}", output.event),
        format!("state={}", output.state),
        format!("config_path={}", output.config_path),
        format!("db_path={}", output.db_path),
        format!("output_path={}", output.output_path),
        format!(
            "archive_path={}",
            output.archive_path.as_deref().unwrap_or("null")
        ),
        format!(
            "cadence_minutes={}",
            output
                .cadence_minutes
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "retention={}",
            output
                .retention
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!("pruned_archives={}", output.pruned_archive_paths.len()),
        format!("exported_at={}", output.exported_at.to_rfc3339()),
        format!(
            "publication_runtime_mode={}",
            output.publication_runtime_mode
        ),
        format!("publication_reason={}", output.publication_reason),
        format!(
            "publication_truth_complete={}",
            output.publication_truth_complete
        ),
        format!("fresh_under_export_gate={}", output.fresh_under_export_gate),
        format!(
            "last_published_at={}",
            format_optional_ts(output.last_published_at.as_ref())
        ),
        format!(
            "last_published_window_start={}",
            format_optional_ts(output.last_published_window_start.as_ref())
        ),
        format!("published_wallet_count={}", output.published_wallet_count),
        format!(
            "wallet_metrics_snapshot_rows={}",
            output.wallet_metrics_snapshot_rows
        ),
        format!(
            "fresh_under_current_gate={}",
            output.fresh_under_current_gate
        ),
        format!(
            "runtime_cursor_ts={}",
            output.runtime_cursor_ts.to_rfc3339()
        ),
        format!("runtime_cursor_slot={}", output.runtime_cursor_slot),
        format!(
            "runtime_cursor_signature={}",
            output.runtime_cursor_signature
        ),
    ]
    .join("\n")
}

fn format_optional_ts(value: Option<&DateTime<Utc>>) -> String {
    value
        .map(DateTime::<Utc>::to_rfc3339)
        .unwrap_or_else(|| "null".to_string())
}

fn render_recent_raw_promotion_blocker_human(
    diagnostic: &RecentRawPromotionBlockerDiagnostic,
) -> String {
    [
        "event=discovery_recent_raw_promotion_blocker".to_string(),
        format!("snapshot_dir={}", diagnostic.recent_raw_snapshot_dir),
        format!(
            "promotion_blocker_observed={}",
            diagnostic.recent_raw_promotion_blocker_observed
        ),
        format!(
            "promotion_ready_now={}",
            diagnostic.recent_raw_promotion_ready_now
        ),
        format!(
            "reason_class={}",
            serde_json::to_string(&diagnostic.recent_raw_promotion_reason_class)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!("promoted_exists={}", diagnostic.recent_raw_promoted_exists),
        format!("staged_exists={}", diagnostic.recent_raw_staged_exists),
        format!(
            "staged_newer_than_promoted={}",
            diagnostic
                .recent_raw_staged_newer_than_promoted
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "stage3_truth_blocked_by_promotion={}",
            diagnostic.recent_raw_stage3_truth_blocked_by_promotion
        ),
        format!(
            "stage3_current_fresh_healthy_evidence_possible={}",
            diagnostic.recent_raw_stage3_current_fresh_healthy_evidence_possible
        ),
        format!(
            "explanation={}",
            diagnostic.recent_raw_promotion_explanation
        ),
    ]
    .join("\n")
}

fn render_recent_raw_catch_up_status_human(diagnostic: &RecentRawCatchUpDiagnostic) -> String {
    [
        "event=discovery_recent_raw_catch_up_status".to_string(),
        format!("snapshot_dir={}", diagnostic.recent_raw_snapshot_dir),
        format!(
            "catch_up_status_observed={}",
            diagnostic.recent_raw_catch_up_status_observed
        ),
        format!(
            "reason_class={}",
            serde_json::to_string(&diagnostic.recent_raw_catch_up_reason_class)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "source_vs_staged_row_lag={}",
            diagnostic
                .recent_raw_source_vs_staged_row_lag
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "source_vs_promoted_row_lag={}",
            diagnostic
                .recent_raw_source_vs_promoted_row_lag
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "staged_advancing={}",
            diagnostic
                .recent_raw_staged_advancing
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "staged_ahead_of_promoted={}",
            diagnostic
                .recent_raw_staged_ahead_of_promoted
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "catch_up_progressing={}",
            diagnostic.recent_raw_catch_up_progressing
        ),
        format!(
            "catch_up_losing_to_source={}",
            diagnostic.recent_raw_catch_up_losing_to_source
        ),
        format!(
            "catch_up_indeterminate={}",
            diagnostic.recent_raw_catch_up_indeterminate
        ),
        format!("explanation={}", diagnostic.recent_raw_catch_up_explanation),
    ]
    .join("\n")
}

fn render_recent_raw_staged_lineage_human(diagnostic: &RecentRawStagedLineageDiagnostic) -> String {
    [
        "event=discovery_recent_raw_staged_lineage".to_string(),
        format!("snapshot_dir={}", diagnostic.recent_raw_snapshot_dir),
        format!(
            "staged_lineage_observed={}",
            diagnostic.recent_raw_staged_lineage_observed
        ),
        format!(
            "reason_class={}",
            serde_json::to_string(&diagnostic.recent_raw_staged_lineage_reason_class)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "same_source_db_as_promoted={}",
            diagnostic
                .recent_raw_staged_same_source_db_as_promoted
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "cursor_relation_to_promoted={}",
            serde_json::to_string(&diagnostic.recent_raw_staged_cursor_relation_to_promoted)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "cursor_relation_basis={}",
            serde_json::to_string(&diagnostic.recent_raw_staged_cursor_relation_basis)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "cursor_relation_explanation={}",
            diagnostic.recent_raw_staged_cursor_relation_explanation
        ),
        format!(
            "cursor_ts_relation_to_promoted={}",
            serde_json::to_string(&diagnostic.recent_raw_staged_cursor_ts_relation_to_promoted)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "cursor_slot_relation_to_promoted={}",
            serde_json::to_string(&diagnostic.recent_raw_staged_cursor_slot_relation_to_promoted)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "cursor_signature_equal_to_promoted={}",
            diagnostic
                .recent_raw_staged_cursor_signature_equal_to_promoted
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "row_count_relation_to_promoted={}",
            serde_json::to_string(&diagnostic.recent_raw_staged_row_count_relation_to_promoted)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "covered_since_relation_to_promoted={}",
            serde_json::to_string(&diagnostic.recent_raw_staged_covered_since_relation_to_promoted)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "staged_monotonic_relative_to_promoted={}",
            diagnostic
                .recent_raw_staged_monotonic_relative_to_promoted
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "staged_regressed_relative_to_promoted={}",
            diagnostic
                .recent_raw_staged_regressed_relative_to_promoted
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "staged_closer_to_source_than_promoted={}",
            diagnostic
                .recent_raw_staged_closer_to_source_than_promoted
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "explanation={}",
            diagnostic.recent_raw_staged_lineage_explanation
        ),
    ]
    .join("\n")
}

fn render_recent_raw_staged_regression_human(
    diagnostic: &RecentRawStagedRegressionDiagnostic,
) -> String {
    [
        "event=discovery_recent_raw_staged_regression".to_string(),
        format!("snapshot_dir={}", diagnostic.recent_raw_snapshot_dir),
        format!(
            "staged_regression_observed={}",
            diagnostic.recent_raw_staged_regression_observed
        ),
        format!(
            "reason_class={}",
            serde_json::to_string(&diagnostic.recent_raw_staged_regression_reason_class)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "promoted_snapshot_path={}",
            diagnostic.recent_raw_promoted_snapshot_path
        ),
        format!(
            "promoted_metadata_path={}",
            diagnostic.recent_raw_promoted_metadata_path
        ),
        format!(
            "staged_snapshot_path={}",
            diagnostic.recent_raw_staged_snapshot_path
        ),
        format!(
            "staged_metadata_path={}",
            diagnostic.recent_raw_staged_metadata_path
        ),
        format!(
            "staged_candidate_count={}",
            diagnostic.recent_raw_staged_candidate_count
        ),
        format!(
            "multiple_staged_candidates_present={}",
            diagnostic.recent_raw_multiple_staged_candidates_present
        ),
        format!(
            "selected_staged_is_latest_candidate={}",
            diagnostic
                .recent_raw_selected_staged_is_latest_candidate
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "selected_staged_created_after_promoted={}",
            diagnostic
                .recent_raw_selected_staged_created_after_promoted
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "selected_staged_frontier_behind_promoted_before_comparison={}",
            diagnostic
                .recent_raw_selected_staged_frontier_behind_promoted_before_comparison
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "selected_staged_completed_after_creation={}",
            diagnostic
                .recent_raw_selected_staged_completed_after_creation
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "selection_reason={}",
            diagnostic.recent_raw_staged_selection_reason
        ),
        format!(
            "explanation={}",
            diagnostic.recent_raw_staged_regression_explanation
        ),
    ]
    .join("\n")
}

fn render_recent_raw_staged_birth_human(diagnostic: &RecentRawStagedBirthDiagnostic) -> String {
    [
        "event=discovery_recent_raw_staged_birth".to_string(),
        format!("snapshot_dir={}", diagnostic.recent_raw_snapshot_dir),
        format!(
            "staged_birth_observed={}",
            diagnostic.recent_raw_staged_birth_observed
        ),
        format!(
            "staged_birth_proven_from_current_artifacts={}",
            diagnostic.recent_raw_staged_birth_proven_from_current_artifacts
        ),
        format!(
            "reason_class={}",
            serde_json::to_string(&diagnostic.recent_raw_staged_birth_reason_class)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "staged_birth_snapshot_path={}",
            diagnostic.recent_raw_staged_birth_snapshot_path
        ),
        format!(
            "staged_birth_metadata_path={}",
            diagnostic.recent_raw_staged_birth_metadata_path
        ),
        format!(
            "staged_birth_created_after_promoted={}",
            diagnostic
                .recent_raw_staged_birth_created_after_promoted
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "staged_birth_window_later_start_than_promoted={}",
            diagnostic
                .recent_raw_staged_birth_window_later_start_than_promoted
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "staged_birth_window_narrower_or_older_than_promoted={}",
            diagnostic
                .recent_raw_staged_birth_window_narrower_or_older_than_promoted
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "staged_birth_manifest_matches_sqlite_content={}",
            diagnostic
                .recent_raw_staged_birth_manifest_matches_sqlite_content
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "staged_birth_manifest_sqlite_match_unproven={}",
            diagnostic.recent_raw_staged_birth_manifest_sqlite_match_unproven
        ),
        format!(
            "explanation={}",
            diagnostic.recent_raw_staged_birth_explanation
        ),
    ]
    .join("\n")
}

fn render_recent_raw_staged_window_seeding_human(
    diagnostic: &RecentRawStagedWindowSeedingDiagnostic,
) -> String {
    [
        "event=discovery_recent_raw_staged_window_seeding".to_string(),
        format!("snapshot_dir={}", diagnostic.recent_raw_snapshot_dir),
        format!(
            "staged_window_seeding_observed={}",
            diagnostic.recent_raw_staged_window_seeding_observed
        ),
        format!(
            "reason_class={}",
            serde_json::to_string(&diagnostic.recent_raw_staged_window_seeding_reason_class)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "staged_start_matches_promoted_start={}",
            diagnostic
                .recent_raw_staged_start_matches_promoted_start
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "staged_start_matches_source_start={}",
            diagnostic
                .recent_raw_staged_start_matches_source_start
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "staged_start_matches_current_window_cutoff={}",
            diagnostic
                .recent_raw_staged_start_matches_current_window_cutoff
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "historical_seeding_basis_proven_from_current_artifacts={}",
            diagnostic
                .recent_raw_staged_window_historical_seeding_basis_proven_from_current_artifacts
        ),
        format!(
            "staged_start_matches_neither_promoted_nor_source={}",
            diagnostic
                .recent_raw_staged_start_matches_neither_promoted_nor_source
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "staged_start_current_evidence_basis={}",
            serde_json::to_string(&diagnostic.recent_raw_staged_start_current_evidence_basis)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "promoted_can_seed_staged_progress_under_current_code={}",
            diagnostic
                .recent_raw_promoted_can_seed_staged_progress_under_current_code
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "promoted_seed_blocked_by_source_contract_mismatch={}",
            diagnostic
                .recent_raw_promoted_seed_blocked_by_source_contract_mismatch
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "promoted_supersedes_staged_progress={}",
            diagnostic
                .recent_raw_promoted_supersedes_staged_progress
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "staged_manifest_matches_sqlite_content={}",
            diagnostic
                .recent_raw_staged_manifest_matches_sqlite_content
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "staged_manifest_sqlite_match_unproven={}",
            diagnostic.recent_raw_staged_manifest_sqlite_match_unproven
        ),
        format!(
            "current_evidence_explanation={}",
            diagnostic.recent_raw_staged_start_current_evidence_explanation
        ),
        format!(
            "explanation={}",
            diagnostic.recent_raw_staged_window_seeding_explanation
        ),
    ]
    .join("\n")
}

fn render_recent_raw_source_window_contract_human(
    diagnostic: &RecentRawSourceWindowContractDiagnostic,
) -> String {
    [
        "event=discovery_recent_raw_source_window_contract".to_string(),
        format!("snapshot_dir={}", diagnostic.recent_raw_snapshot_dir),
        format!(
            "source_window_contract_observed={}",
            diagnostic.recent_raw_source_window_contract_observed
        ),
        format!(
            "reason_class={}",
            serde_json::to_string(&diagnostic.recent_raw_source_window_contract_reason_class)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "source_window_probe_bounded={}",
            diagnostic.recent_raw_source_window_probe_bounded
        ),
        format!(
            "source_window_probe_mode={}",
            serde_json::to_string(&diagnostic.recent_raw_source_window_probe_mode)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "source_cached_state_matches_bounded_probe={}",
            diagnostic
                .recent_raw_source_cached_state_matches_bounded_probe
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "source_start_later_than_promoted={}",
            diagnostic
                .recent_raw_source_start_later_than_promoted
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "source_contract_currently_excludes_older_rows={}",
            diagnostic
                .recent_raw_source_contract_currently_excludes_older_rows
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "source_window_matches_current_bounded_contract={}",
            diagnostic
                .recent_raw_source_window_matches_current_bounded_contract
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "promoted_reflects_older_still_promoted_window={}",
            diagnostic
                .recent_raw_promoted_reflects_older_still_promoted_window
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "source_window_contract_basis={}",
            serde_json::to_string(&diagnostic.recent_raw_source_window_contract_basis)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "source_cached_state_matches_scanned_rows={}",
            diagnostic
                .recent_raw_source_cached_state_matches_scanned_rows
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "source_prune_activity_recorded={}",
            diagnostic
                .recent_raw_source_prune_activity_recorded
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "source_window_probe_explanation={}",
            diagnostic.recent_raw_source_window_probe_explanation
        ),
        format!(
            "source_window_contract_basis_explanation={}",
            diagnostic.recent_raw_source_window_contract_basis_explanation
        ),
        format!(
            "explanation={}",
            diagnostic.recent_raw_source_window_contract_explanation
        ),
    ]
    .join("\n")
}

fn render_recent_raw_promoted_retention_contract_human(
    diagnostic: &RecentRawPromotedRetentionContractDiagnostic,
) -> String {
    [
        "event=discovery_recent_raw_promoted_retention_contract".to_string(),
        format!("snapshot_dir={}", diagnostic.recent_raw_snapshot_dir),
        format!(
            "promoted_retention_observed={}",
            diagnostic.recent_raw_promoted_retention_observed
        ),
        format!(
            "reason_class={}",
            serde_json::to_string(&diagnostic.recent_raw_promoted_retention_reason_class)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "promoted_start_older_than_current_source={}",
            diagnostic
                .recent_raw_promoted_start_older_than_current_source
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "promoted_currently_retained_as_truth={}",
            diagnostic
                .recent_raw_promoted_currently_retained_as_truth
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "promoted_has_current_contract_invalidation_rule={}",
            diagnostic
                .recent_raw_promoted_has_current_contract_invalidation_rule
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "promoted_invalidated_by_current_source_window_shift={}",
            diagnostic
                .recent_raw_promoted_invalidated_by_current_source_window_shift
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "stage3_truth_currently_depends_on_retained_older_promoted_surface={}",
            diagnostic
                .recent_raw_stage3_truth_currently_depends_on_retained_older_promoted_surface
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "stage3_truth_can_advance_without_new_promotion={}",
            diagnostic
                .recent_raw_stage3_truth_can_advance_without_new_promotion
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "promotion_reason_class={}",
            serde_json::to_string(&diagnostic.recent_raw_promotion_reason_class)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "source_window_reason_class={}",
            serde_json::to_string(&diagnostic.recent_raw_source_window_contract_reason_class)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "promoted_retention_basis={}",
            serde_json::to_string(&diagnostic.recent_raw_promoted_retention_basis)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "promoted_retention_basis_explanation={}",
            diagnostic.recent_raw_promoted_retention_basis_explanation
        ),
        format!(
            "explanation={}",
            diagnostic.recent_raw_promoted_retention_explanation
        ),
    ]
    .join("\n")
}

fn render_recent_raw_replacement_promotion_contract_human(
    diagnostic: &RecentRawReplacementPromotionContractDiagnostic,
) -> String {
    [
        "event=discovery_recent_raw_replacement_promotion_contract".to_string(),
        format!("snapshot_dir={}", diagnostic.recent_raw_snapshot_dir),
        format!(
            "replacement_promotion_observed={}",
            diagnostic.recent_raw_replacement_promotion_observed
        ),
        format!(
            "reason_class={}",
            serde_json::to_string(&diagnostic.recent_raw_replacement_promotion_reason_class)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "replacement_candidate_exists={}",
            diagnostic.recent_raw_replacement_candidate_exists
        ),
        format!(
            "replacement_candidate_source_db_matches_promoted={}",
            diagnostic
                .recent_raw_replacement_candidate_source_db_matches_promoted
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "replacement_candidate_start_matches_current_source={}",
            diagnostic
                .recent_raw_replacement_candidate_start_matches_current_source
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "replacement_candidate_complete_against_current_source={}",
            diagnostic
                .recent_raw_replacement_candidate_complete_against_current_source
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "replacement_candidate_promotable_now={}",
            diagnostic
                .recent_raw_replacement_candidate_promotable_now
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "replacement_candidate_covered_through_relation_to_promoted={}",
            serde_json::to_string(
                &diagnostic.recent_raw_replacement_candidate_covered_through_relation_to_promoted
            )
            .unwrap_or_else(|_| "\"unknown\"".to_string())
            .trim_matches('"')
        ),
        format!(
            "replacement_candidate_row_count_relation_to_promoted={}",
            serde_json::to_string(
                &diagnostic.recent_raw_replacement_candidate_row_count_relation_to_promoted
            )
            .unwrap_or_else(|_| "\"unknown\"".to_string())
            .trim_matches('"')
        ),
        format!(
            "replacement_promotion_would_retire_current_promoted_truth={}",
            diagnostic
                .recent_raw_replacement_promotion_would_retire_current_promoted_truth
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "stage3_blocked_on_replacement_candidate={}",
            diagnostic
                .recent_raw_stage3_blocked_on_replacement_candidate
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "promotion_reason_class={}",
            serde_json::to_string(&diagnostic.recent_raw_promotion_reason_class)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "promotion_ready_now={}",
            diagnostic.recent_raw_promotion_ready_now
        ),
        format!(
            "explanation={}",
            diagnostic.recent_raw_replacement_promotion_explanation
        ),
    ]
    .join("\n")
}

fn render_recent_raw_replacement_progress_contract_human(
    diagnostic: &RecentRawReplacementProgressContractDiagnostic,
) -> String {
    [
        "event=discovery_recent_raw_replacement_progress_contract".to_string(),
        format!("snapshot_dir={}", diagnostic.recent_raw_snapshot_dir),
        format!(
            "replacement_progress_observed={}",
            diagnostic.recent_raw_replacement_progress_observed
        ),
        format!(
            "reason_class={}",
            serde_json::to_string(&diagnostic.recent_raw_replacement_progress_reason_class)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "replacement_candidate_exists={}",
            diagnostic.recent_raw_replacement_candidate_exists
        ),
        format!(
            "replacement_candidate_created_at={}",
            format_optional_ts(
                diagnostic
                    .recent_raw_replacement_candidate_created_at
                    .as_ref()
            )
        ),
        format!(
            "replacement_candidate_row_count={}",
            diagnostic
                .recent_raw_replacement_candidate_row_count
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "replacement_candidate_covered_through={}",
            diagnostic
                .recent_raw_replacement_candidate_covered_through
                .as_ref()
                .map(|value| serde_json::to_string(value).unwrap_or_else(|_| "null".to_string()))
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "replacement_candidate_last_batch_completed_at={}",
            format_optional_ts(
                diagnostic
                    .recent_raw_replacement_candidate_last_batch_completed_at
                    .as_ref()
            )
        ),
        format!(
            "replacement_candidate_completed_after_creation={}",
            diagnostic
                .recent_raw_replacement_candidate_completed_after_creation
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "replacement_previous_candidate_exists={}",
            diagnostic.recent_raw_replacement_previous_candidate_exists
        ),
        format!(
            "replacement_candidate_same_identity_as_previous_attempt={}",
            diagnostic
                .recent_raw_replacement_candidate_same_identity_as_previous_attempt
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "replacement_candidate_row_count_advanced={}",
            diagnostic
                .recent_raw_replacement_candidate_row_count_advanced
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "replacement_candidate_covered_through_advanced={}",
            diagnostic
                .recent_raw_replacement_candidate_covered_through_advanced
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "replacement_candidate_last_batch_completed_at_advanced={}",
            diagnostic
                .recent_raw_replacement_candidate_last_batch_completed_at_advanced
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "replacement_candidate_advancing={}",
            diagnostic
                .recent_raw_replacement_candidate_advancing
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "replacement_candidate_stalled={}",
            diagnostic
                .recent_raw_replacement_candidate_stalled
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "replacement_candidate_reset_or_recreated={}",
            diagnostic
                .recent_raw_replacement_candidate_reset_or_recreated
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "replacement_manifest_matches_sqlite_content={}",
            diagnostic
                .recent_raw_replacement_manifest_matches_sqlite_content
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "replacement_manifest_sqlite_match_unproven={}",
            diagnostic.recent_raw_replacement_manifest_sqlite_match_unproven
        ),
        format!(
            "replacement_candidate_complete_against_current_source={}",
            diagnostic
                .recent_raw_replacement_candidate_complete_against_current_source
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "replacement_promotion_reason_class={}",
            serde_json::to_string(&diagnostic.recent_raw_replacement_promotion_reason_class)
                .unwrap_or_else(|_| "\"unknown\"".to_string())
                .trim_matches('"')
        ),
        format!(
            "explanation={}",
            diagnostic.recent_raw_replacement_progress_explanation
        ),
    ]
    .join("\n")
}

#[cfg(test)]
mod tests {
    use super::{
        load_json, parse_args_from, run, run_command, write_json_atomic, Command, Config,
        ExplainRecentRawCatchUpStatusConfig, ExplainRecentRawPromotedRetentionContractConfig,
        ExplainRecentRawPromotionBlockerConfig, ExplainRecentRawReplacementProgressContractConfig,
        ExplainRecentRawReplacementPromotionContractConfig,
        ExplainRecentRawSourceWindowContractConfig, ExplainRecentRawStagedBirthConfig,
        ExplainRecentRawStagedLineageConfig, ExplainRecentRawStagedRegressionConfig,
        ExplainRecentRawStagedWindowSeedingConfig,
    };
    use anyhow::{Context, Result};
    use chrono::{DateTime, Duration, Utc};
    use copybot_storage::{
        DiscoveryPublicationStateUpdate, DiscoveryRuntimeArtifact, DiscoveryRuntimeCursor,
        DiscoveryRuntimeMode, SqliteStore, WalletMetricRow, WalletUpsertRow,
    };
    use serde_json::Value;
    use std::fs;
    use std::path::{Path, PathBuf};
    use tempfile::tempdir;

    #[test]
    fn parse_args_from_accepts_scheduled_force_json_and_now() {
        let parsed = parse_args_from(vec![
            "--config".to_string(),
            "configs/live.toml".to_string(),
            "--db-path".to_string(),
            "state/live.db".to_string(),
            "--scheduled".to_string(),
            "--force".to_string(),
            "--json".to_string(),
            "--now".to_string(),
            "2026-03-23T12:00:00Z".to_string(),
        ])
        .expect("parse should succeed")
        .expect("command should be present");
        let Command::Export(parsed) = parsed else {
            panic!("expected export command");
        };

        assert_eq!(parsed.config_path, PathBuf::from("configs/live.toml"));
        assert_eq!(parsed.db_path, Some(PathBuf::from("state/live.db")));
        assert!(parsed.output_path.is_none());
        assert!(parsed.scheduled);
        assert!(parsed.force);
        assert!(parsed.json);
        assert_eq!(parsed.now.to_rfc3339(), "2026-03-23T12:00:00+00:00");
    }

    #[test]
    fn parse_args_from_accepts_recent_raw_promotion_blocker_mode() {
        let parsed = parse_args_from(vec![
            "--explain-recent-raw-promotion-blocker".to_string(),
            "--state-root".to_string(),
            "/tmp/state".to_string(),
            "--json".to_string(),
        ])
        .expect("parse should succeed")
        .expect("command should be present");
        let Command::ExplainRecentRawPromotionBlocker(parsed) = parsed else {
            panic!("expected explain command");
        };
        assert_eq!(parsed.state_root, PathBuf::from("/tmp/state"));
        assert!(parsed.json);
    }

    #[test]
    fn parse_args_from_accepts_recent_raw_catch_up_status_mode() {
        let parsed = parse_args_from(vec![
            "--explain-recent-raw-catch-up-status".to_string(),
            "--state-root".to_string(),
            "/tmp/state".to_string(),
            "--json".to_string(),
        ])
        .expect("parse should succeed")
        .expect("command should be present");
        let Command::ExplainRecentRawCatchUpStatus(parsed) = parsed else {
            panic!("expected catch-up command");
        };
        assert_eq!(parsed.state_root, PathBuf::from("/tmp/state"));
        assert!(parsed.json);
    }

    #[test]
    fn parse_args_from_accepts_recent_raw_staged_lineage_mode() {
        let parsed = parse_args_from(vec![
            "--explain-recent-raw-staged-lineage".to_string(),
            "--state-root".to_string(),
            "/tmp/state".to_string(),
            "--json".to_string(),
        ])
        .expect("parse should succeed")
        .expect("command should be present");
        let Command::ExplainRecentRawStagedLineage(parsed) = parsed else {
            panic!("expected staged lineage command");
        };
        assert_eq!(parsed.state_root, PathBuf::from("/tmp/state"));
        assert!(parsed.json);
    }

    #[test]
    fn parse_args_from_accepts_recent_raw_staged_regression_mode() {
        let parsed = parse_args_from(vec![
            "--explain-recent-raw-staged-regression".to_string(),
            "--state-root".to_string(),
            "/tmp/state".to_string(),
            "--json".to_string(),
        ])
        .expect("parse should succeed")
        .expect("command should be present");
        let Command::ExplainRecentRawStagedRegression(parsed) = parsed else {
            panic!("expected staged regression command");
        };
        assert_eq!(parsed.state_root, PathBuf::from("/tmp/state"));
        assert!(parsed.json);
    }

    #[test]
    fn parse_args_from_accepts_recent_raw_staged_birth_mode() {
        let parsed = parse_args_from(vec![
            "--explain-recent-raw-staged-birth".to_string(),
            "--state-root".to_string(),
            "/tmp/state".to_string(),
            "--json".to_string(),
        ])
        .expect("parse should succeed")
        .expect("command should be present");
        let Command::ExplainRecentRawStagedBirth(parsed) = parsed else {
            panic!("expected staged birth command");
        };
        assert_eq!(parsed.state_root, PathBuf::from("/tmp/state"));
        assert!(parsed.json);
    }

    #[test]
    fn parse_args_from_accepts_recent_raw_staged_window_seeding_mode() {
        let parsed = parse_args_from(vec![
            "--explain-recent-raw-staged-window-seeding".to_string(),
            "--state-root".to_string(),
            "/tmp/state".to_string(),
            "--json".to_string(),
        ])
        .expect("parse should succeed")
        .expect("command should be present");
        let Command::ExplainRecentRawStagedWindowSeeding(parsed) = parsed else {
            panic!("expected staged window seeding command");
        };
        assert_eq!(parsed.state_root, PathBuf::from("/tmp/state"));
        assert!(parsed.json);
    }

    #[test]
    fn parse_args_from_accepts_recent_raw_source_window_contract_mode() {
        let parsed = parse_args_from(vec![
            "--explain-recent-raw-source-window-contract".to_string(),
            "--state-root".to_string(),
            "/tmp/state".to_string(),
            "--json".to_string(),
        ])
        .expect("parse should succeed")
        .expect("command should be present");
        let Command::ExplainRecentRawSourceWindowContract(parsed) = parsed else {
            panic!("expected source window contract command");
        };
        assert_eq!(parsed.state_root, PathBuf::from("/tmp/state"));
        assert!(parsed.json);
    }

    #[test]
    fn parse_args_from_accepts_recent_raw_promoted_retention_contract_mode() {
        let parsed = parse_args_from(vec![
            "--explain-recent-raw-promoted-retention-contract".to_string(),
            "--state-root".to_string(),
            "/tmp/state".to_string(),
            "--json".to_string(),
        ])
        .expect("parse should succeed")
        .expect("command should be present");
        let Command::ExplainRecentRawPromotedRetentionContract(parsed) = parsed else {
            panic!("expected promoted retention contract command");
        };
        assert_eq!(parsed.state_root, PathBuf::from("/tmp/state"));
        assert!(parsed.json);
    }

    #[test]
    fn parse_args_from_accepts_recent_raw_replacement_promotion_contract_mode() {
        let parsed = parse_args_from(vec![
            "--explain-recent-raw-replacement-promotion-contract".to_string(),
            "--state-root".to_string(),
            "/tmp/state".to_string(),
            "--json".to_string(),
        ])
        .expect("parse should succeed")
        .expect("command should be present");
        let Command::ExplainRecentRawReplacementPromotionContract(parsed) = parsed else {
            panic!("expected replacement promotion contract command");
        };
        assert_eq!(parsed.state_root, PathBuf::from("/tmp/state"));
        assert!(parsed.json);
    }

    #[test]
    fn parse_args_from_accepts_recent_raw_replacement_progress_contract_mode() {
        let parsed = parse_args_from(vec![
            "--explain-recent-raw-replacement-progress-contract".to_string(),
            "--state-root".to_string(),
            "/tmp/state".to_string(),
            "--json".to_string(),
        ])
        .expect("parse should succeed")
        .expect("command should be present");
        let Command::ExplainRecentRawReplacementProgressContract(parsed) = parsed else {
            panic!("expected replacement progress contract command");
        };
        assert_eq!(parsed.state_root, PathBuf::from("/tmp/state"));
        assert!(parsed.json);
    }

    #[test]
    fn run_command_recent_raw_promotion_blocker_renders_json() -> Result<()> {
        let fixture = make_fixture("runtime-export-recent-raw-promotion-blocker")?;
        let state_root = fixture
            .config_path
            .parent()
            .expect("config parent")
            .join("state");
        let recent_raw_dir = state_root.join("discovery_restore/recent_raw");
        fs::create_dir_all(&recent_raw_dir)?;
        seed_recent_raw_source_state(&fixture.store, &fixture.db_path, &recent_raw_dir)?;
        let promoted_path = recent_raw_dir.join("latest.sqlite");
        let promoted_metadata_path = recent_raw_dir.join("latest.json");
        let staged_path = recent_raw_dir.join(".discovery_recent_raw_staged.sqlite.archive-staged");
        let staged_metadata_path =
            recent_raw_dir.join(".discovery_recent_raw_staged.sqlite.archive-staged.json");
        fs::write(&promoted_path, b"promoted")?;
        fs::write(&staged_path, b"staged")?;
        write_json_atomic(
            &promoted_metadata_path,
            &serde_json::json!({
                "created_at": "2026-04-14T08:00:00Z",
                "source_db_path": fixture.db_path.display().to_string(),
                "snapshot_path": promoted_path.display().to_string(),
                "row_count": 1,
                "covered_since": "2026-04-14T07:55:00Z",
                "covered_through_cursor": {
                    "ts_utc": "2026-04-14T07:55:00Z",
                    "slot": parse_ts("2026-04-14T07:55:00Z")?.timestamp() as u64,
                    "signature": "sig-promoted"
                },
                "last_batch_completed_at": "2026-04-14T08:00:00Z",
                "updated_at": "2026-04-14T08:00:00Z",
                "snapshot_bytes": 8
            }),
        )?;
        write_json_atomic(
            &staged_metadata_path,
            &serde_json::json!({
                "created_at": "2026-04-14T08:05:00Z",
                "source_db_path": fixture.db_path.display().to_string(),
                "snapshot_path": staged_path.display().to_string(),
                "row_count": 2,
                "covered_since": "2026-04-14T07:55:00Z",
                "covered_through_cursor": {
                    "ts_utc": "2026-04-14T07:56:00Z",
                    "slot": parse_ts("2026-04-14T07:56:00Z")?.timestamp() as u64,
                    "signature": "sig-staged"
                },
                "last_batch_completed_at": "2026-04-14T08:05:00Z",
                "updated_at": "2026-04-14T08:05:00Z",
                "snapshot_bytes": 6
            }),
        )?;

        let output = run_command(Command::ExplainRecentRawPromotionBlocker(
            ExplainRecentRawPromotionBlockerConfig {
                state_root,
                json: true,
            },
        ))?;
        let parsed: Value = serde_json::from_str(&output)?;
        assert_eq!(
            parsed["recent_raw_promotion_reason_class"],
            "recent_raw_promotion_ready_now"
        );
        assert_eq!(parsed["recent_raw_promoted_exists"], true);
        assert_eq!(parsed["recent_raw_staged_exists"], true);
        Ok(())
    }

    #[test]
    fn run_command_recent_raw_catch_up_status_renders_json() -> Result<()> {
        let fixture = make_fixture("runtime-export-recent-raw-catch-up-status")?;
        let state_root = fixture
            .config_path
            .parent()
            .expect("config parent")
            .join("state");
        let recent_raw_dir = state_root.join("discovery_restore/recent_raw");
        fs::create_dir_all(&recent_raw_dir)?;
        seed_recent_raw_source_state(&fixture.store, &fixture.db_path, &recent_raw_dir)?;
        let promoted_path = recent_raw_dir.join("latest.sqlite");
        let promoted_metadata_path = recent_raw_dir.join("latest.json");
        let staged_path = recent_raw_dir.join(".discovery_recent_raw_staged.sqlite.archive-staged");
        let staged_metadata_path =
            recent_raw_dir.join(".discovery_recent_raw_staged.sqlite.archive-staged.json");
        fs::write(&promoted_path, b"promoted")?;
        fs::write(&staged_path, b"staged")?;
        write_json_atomic(
            &promoted_metadata_path,
            &serde_json::json!({
                "created_at": "2026-04-14T08:00:00Z",
                "source_db_path": fixture.db_path.display().to_string(),
                "snapshot_path": promoted_path.display().to_string(),
                "row_count": 1,
                "covered_since": "2026-04-14T07:55:00Z",
                "covered_through_cursor": {
                    "ts_utc": "2026-04-14T07:55:00Z",
                    "slot": parse_ts("2026-04-14T07:55:00Z")?.timestamp() as u64,
                    "signature": "sig-promoted"
                },
                "last_batch_completed_at": "2026-04-14T08:00:00Z",
                "updated_at": "2026-04-14T08:00:00Z",
                "snapshot_bytes": 8
            }),
        )?;
        write_json_atomic(
            &staged_metadata_path,
            &serde_json::json!({
                "created_at": "2026-04-14T08:05:00Z",
                "source_db_path": fixture.db_path.display().to_string(),
                "snapshot_path": staged_path.display().to_string(),
                "row_count": 2,
                "covered_since": "2026-04-14T07:55:00Z",
                "covered_through_cursor": {
                    "ts_utc": "2026-04-14T07:56:00Z",
                    "slot": parse_ts("2026-04-14T07:56:00Z")?.timestamp() as u64,
                    "signature": "sig-staged"
                },
                "last_batch_completed_at": "2026-04-14T08:05:00Z",
                "updated_at": "2026-04-14T08:05:00Z",
                "snapshot_bytes": 6
            }),
        )?;

        let output = run_command(Command::ExplainRecentRawCatchUpStatus(
            ExplainRecentRawCatchUpStatusConfig {
                state_root,
                json: true,
            },
        ))?;
        let parsed: Value = serde_json::from_str(&output)?;
        assert_eq!(
            parsed["recent_raw_catch_up_reason_class"],
            "recent_raw_catch_up_caught_up"
        );
        assert_eq!(parsed["recent_raw_promoted_exists"], true);
        assert_eq!(parsed["recent_raw_staged_exists"], true);
        assert_eq!(parsed["recent_raw_catch_up_progressing"], false);
        Ok(())
    }

    #[test]
    fn run_command_recent_raw_staged_lineage_renders_json() -> Result<()> {
        let fixture = make_fixture("runtime-export-recent-raw-staged-lineage")?;
        let state_root = fixture
            .config_path
            .parent()
            .expect("config parent")
            .join("state");
        let recent_raw_dir = state_root.join("discovery_restore/recent_raw");
        fs::create_dir_all(&recent_raw_dir)?;
        seed_recent_raw_source_state(&fixture.store, &fixture.db_path, &recent_raw_dir)?;
        let promoted_path = recent_raw_dir.join("latest.sqlite");
        let promoted_metadata_path = recent_raw_dir.join("latest.json");
        let staged_path = recent_raw_dir.join(".discovery_recent_raw_staged.sqlite.archive-staged");
        let staged_metadata_path =
            recent_raw_dir.join(".discovery_recent_raw_staged.sqlite.archive-staged.json");
        fs::write(&promoted_path, b"promoted")?;
        fs::write(&staged_path, b"staged")?;
        write_json_atomic(
            &promoted_metadata_path,
            &serde_json::json!({
                "created_at": "2026-04-14T08:00:00Z",
                "source_db_path": fixture.db_path.display().to_string(),
                "snapshot_path": promoted_path.display().to_string(),
                "row_count": 1,
                "covered_since": "2026-04-14T07:55:00Z",
                "covered_through_cursor": {
                    "ts_utc": "2026-04-14T07:55:00Z",
                    "slot": parse_ts("2026-04-14T07:55:00Z")?.timestamp() as u64,
                    "signature": "sig-promoted"
                },
                "last_batch_completed_at": "2026-04-14T08:00:00Z",
                "updated_at": "2026-04-14T08:00:00Z",
                "snapshot_bytes": 8
            }),
        )?;
        write_json_atomic(
            &staged_metadata_path,
            &serde_json::json!({
                "created_at": "2026-04-14T08:05:00Z",
                "source_db_path": fixture.db_path.display().to_string(),
                "snapshot_path": staged_path.display().to_string(),
                "row_count": 2,
                "covered_since": "2026-04-14T07:55:00Z",
                "covered_through_cursor": {
                    "ts_utc": "2026-04-14T07:56:00Z",
                    "slot": parse_ts("2026-04-14T07:56:00Z")?.timestamp() as u64,
                    "signature": "sig-staged"
                },
                "last_batch_completed_at": "2026-04-14T08:05:00Z",
                "updated_at": "2026-04-14T08:05:00Z",
                "snapshot_bytes": 6
            }),
        )?;

        let output = run_command(Command::ExplainRecentRawStagedLineage(
            ExplainRecentRawStagedLineageConfig {
                state_root,
                json: true,
            },
        ))?;
        let parsed: Value = serde_json::from_str(&output)?;
        assert_eq!(
            parsed["recent_raw_staged_lineage_reason_class"],
            "recent_raw_staged_lineage_monotonic_but_incomplete"
        );
        assert_eq!(parsed["recent_raw_staged_same_source_db_as_promoted"], true);
        assert_eq!(
            parsed["recent_raw_staged_cursor_relation_to_promoted"],
            "ahead"
        );
        assert_eq!(
            parsed["recent_raw_staged_cursor_relation_basis"],
            "direct_covered_through_cursor_comparison"
        );
        assert_eq!(
            parsed["recent_raw_staged_cursor_ts_relation_to_promoted"],
            "ahead"
        );
        assert_eq!(
            parsed["recent_raw_staged_cursor_slot_relation_to_promoted"],
            "ahead"
        );
        assert_eq!(
            parsed["recent_raw_staged_cursor_signature_equal_to_promoted"],
            false
        );
        assert!(parsed["recent_raw_staged_cursor_relation_explanation"]
            .as_str()
            .unwrap_or_default()
            .contains("direct covered-through cursor comparison"));
        Ok(())
    }

    #[test]
    fn run_command_recent_raw_staged_regression_renders_json() -> Result<()> {
        let fixture = make_fixture("runtime-export-recent-raw-staged-regression")?;
        let state_root = fixture
            .config_path
            .parent()
            .expect("config parent")
            .join("state");
        let recent_raw_dir = state_root.join("discovery_restore/recent_raw");
        fs::create_dir_all(&recent_raw_dir)?;
        seed_recent_raw_source_state(&fixture.store, &fixture.db_path, &recent_raw_dir)?;
        let promoted_path = recent_raw_dir.join("latest.sqlite");
        let promoted_metadata_path = recent_raw_dir.join("latest.json");
        let staged_path = recent_raw_dir.join(".discovery_recent_raw_staged.sqlite.archive-staged");
        let staged_metadata_path =
            recent_raw_dir.join(".discovery_recent_raw_staged.sqlite.archive-staged.json");
        fs::write(&promoted_path, b"promoted")?;
        fs::write(&staged_path, b"staged")?;
        write_json_atomic(
            &promoted_metadata_path,
            &serde_json::json!({
                "created_at": "2026-04-14T08:00:00Z",
                "source_db_path": fixture.db_path.display().to_string(),
                "snapshot_path": promoted_path.display().to_string(),
                "row_count": 2,
                "covered_since": "2026-04-14T07:55:00Z",
                "covered_through_cursor": {
                    "ts_utc": "2026-04-14T07:56:00Z",
                    "slot": parse_ts("2026-04-14T07:56:00Z")?.timestamp() as u64,
                    "signature": "sig-promoted"
                },
                "last_batch_completed_at": "2026-04-14T08:00:00Z",
                "updated_at": "2026-04-14T08:00:00Z",
                "snapshot_bytes": 8
            }),
        )?;
        write_json_atomic(
            &staged_metadata_path,
            &serde_json::json!({
                "created_at": "2026-04-14T08:05:00Z",
                "source_db_path": fixture.db_path.display().to_string(),
                "snapshot_path": staged_path.display().to_string(),
                "row_count": 1,
                "covered_since": "2026-04-14T07:55:00Z",
                "covered_through_cursor": {
                    "ts_utc": "2026-04-14T07:55:00Z",
                    "slot": parse_ts("2026-04-14T07:55:00Z")?.timestamp() as u64,
                    "signature": "sig-staged"
                },
                "last_batch_completed_at": "2026-04-14T08:05:00Z",
                "updated_at": "2026-04-14T08:05:00Z",
                "snapshot_bytes": 6
            }),
        )?;

        let output = run_command(Command::ExplainRecentRawStagedRegression(
            ExplainRecentRawStagedRegressionConfig {
                state_root,
                json: true,
            },
        ))?;
        let parsed: Value = serde_json::from_str(&output)?;
        assert_eq!(
            parsed["recent_raw_staged_regression_reason_class"],
            "recent_raw_staged_regression_artifact_itself_already_behind"
        );
        assert_eq!(
            parsed["recent_raw_selected_staged_created_after_promoted"],
            true
        );
        assert_eq!(
            parsed["recent_raw_selected_staged_frontier_behind_promoted_before_comparison"],
            true
        );
        assert_eq!(
            parsed["recent_raw_selected_staged_completed_after_creation"],
            false
        );
        assert_eq!(parsed["recent_raw_staged_candidate_count"], 1);
        assert_eq!(
            parsed["recent_raw_multiple_staged_candidates_present"],
            false
        );
        Ok(())
    }

    #[test]
    fn run_command_recent_raw_staged_birth_renders_json() -> Result<()> {
        let fixture = make_fixture("runtime-export-recent-raw-staged-birth")?;
        let state_root = fixture
            .config_path
            .parent()
            .expect("config parent")
            .join("state");
        let recent_raw_dir = state_root.join("discovery_restore/recent_raw");
        fs::create_dir_all(&recent_raw_dir)?;
        seed_recent_raw_source_state(&fixture.store, &fixture.db_path, &recent_raw_dir)?;
        let promoted_path = recent_raw_dir.join("latest.sqlite");
        let promoted_metadata_path = recent_raw_dir.join("latest.json");
        let staged_path = recent_raw_dir.join(".discovery_recent_raw_staged.sqlite.archive-staged");
        let staged_metadata_path =
            recent_raw_dir.join(".discovery_recent_raw_staged.sqlite.archive-staged.json");
        fs::write(&promoted_path, b"promoted")?;
        write_json_atomic(
            &promoted_metadata_path,
            &serde_json::json!({
                "created_at": "2026-04-14T08:00:00Z",
                "source_db_path": fixture.db_path.display().to_string(),
                "snapshot_path": promoted_path.display().to_string(),
                "row_count": 2,
                "covered_since": "2026-04-14T07:55:00Z",
                "covered_through_cursor": {
                    "ts_utc": "2026-04-14T07:56:00Z",
                    "slot": parse_ts("2026-04-14T07:56:00Z")?.timestamp() as u64,
                    "signature": "sig-promoted"
                },
                "last_batch_completed_at": "2026-04-14T08:00:00Z",
                "updated_at": "2026-04-14T08:00:00Z",
                "snapshot_bytes": 8
            }),
        )?;
        write_recent_raw_snapshot_sqlite_content(
            &staged_path,
            &[recent_raw_swap(
                "raw-wallet",
                "sig-staged",
                parse_ts("2026-04-14T07:55:00Z")?,
            )],
            parse_ts("2026-04-14T08:05:00Z")?,
        )?;
        write_json_atomic(
            &staged_metadata_path,
            &serde_json::json!({
                "created_at": "2026-04-14T08:05:00Z",
                "source_db_path": fixture.db_path.display().to_string(),
                "snapshot_path": staged_path.display().to_string(),
                "row_count": 1,
                "covered_since": "2026-04-14T07:55:00Z",
                "covered_through_cursor": {
                    "ts_utc": "2026-04-14T07:55:00Z",
                    "slot": parse_ts("2026-04-14T07:55:00Z")?.timestamp() as u64,
                    "signature": "sig-staged"
                },
                "last_batch_completed_at": "2026-04-14T08:05:00Z",
                "updated_at": "2026-04-14T08:05:00Z",
                "snapshot_bytes": 6
            }),
        )?;

        let output = run_command(Command::ExplainRecentRawStagedBirth(
            ExplainRecentRawStagedBirthConfig {
                state_root,
                json: true,
            },
        ))?;
        let parsed: Value = serde_json::from_str(&output)?;
        assert_eq!(
            parsed["recent_raw_staged_birth_reason_class"],
            "recent_raw_staged_current_artifact_manifest_and_sqlite_content_agree_but_behind"
        );
        assert_eq!(
            parsed["recent_raw_staged_birth_proven_from_current_artifacts"],
            false
        );
        assert_eq!(
            parsed["recent_raw_staged_birth_manifest_matches_sqlite_content"],
            true
        );
        assert_eq!(
            parsed["recent_raw_staged_birth_manifest_sqlite_match_unproven"],
            false
        );
        assert_eq!(
            parsed["recent_raw_staged_birth_covered_through_relation_to_promoted"],
            "behind"
        );
        Ok(())
    }

    #[test]
    fn run_command_recent_raw_staged_window_seeding_renders_json() -> Result<()> {
        let fixture = make_fixture("runtime-export-recent-raw-staged-window-seeding")?;
        let state_root = fixture
            .config_path
            .parent()
            .expect("config parent")
            .join("state");
        let recent_raw_dir = state_root.join("discovery_restore/recent_raw");
        fs::create_dir_all(&recent_raw_dir)?;
        fixture.store.insert_recent_raw_journal_batch(
            &[
                recent_raw_swap(
                    "raw-wallet",
                    "sig-source-early",
                    parse_ts("2026-04-14T07:54:00Z")?,
                ),
                recent_raw_swap(
                    "raw-wallet",
                    "sig-source-late",
                    parse_ts("2026-04-14T07:56:00Z")?,
                ),
            ],
            parse_ts("2026-04-14T08:06:00Z")?,
        )?;
        let latest_path = recent_raw_dir.join("latest.sqlite");
        let latest_metadata_path = recent_raw_dir.join("latest.json");
        let staged_path = recent_raw_dir.join(".discovery_recent_raw_staged.sqlite.archive-staged");
        let staged_metadata_path =
            recent_raw_dir.join(".discovery_recent_raw_staged.sqlite.archive-staged.json");

        let promoted_manifest = serde_json::json!({
            "created_at": "2026-04-14T08:00:00Z",
            "source_db_path": fixture.db_path.display().to_string(),
            "snapshot_path": latest_path.display().to_string(),
            "row_count": 1,
            "covered_since": "2026-04-14T07:55:00Z",
            "covered_through_cursor": {
                "ts_utc": "2026-04-14T07:56:00Z",
                "slot": parse_ts("2026-04-14T07:56:00Z")?.timestamp() as u64,
                "signature": "sig-promoted"
            },
            "last_batch_completed_at": "2026-04-14T08:00:00Z",
            "updated_at": "2026-04-14T08:00:00Z",
            "snapshot_bytes": 8u64
        });
        fs::write(&latest_path, b"snapshot")
            .with_context(|| format!("failed writing {}", latest_path.display()))?;
        write_json_atomic(&latest_metadata_path, &promoted_manifest)?;
        write_recent_raw_snapshot_sqlite_content(
            &staged_path,
            &[recent_raw_swap(
                "raw-wallet",
                "sig-staged",
                parse_ts("2026-04-14T07:55:00Z")?,
            )],
            parse_ts("2026-04-14T08:03:00Z")?,
        )?;
        let staged_bytes = fs::metadata(&staged_path)
            .with_context(|| format!("failed stat {}", staged_path.display()))?
            .len();
        let staged_manifest = serde_json::json!({
            "created_at": "2026-04-14T08:03:00Z",
            "source_db_path": fixture.db_path.display().to_string(),
            "snapshot_path": staged_path.display().to_string(),
            "row_count": 1,
            "covered_since": "2026-04-14T07:55:00Z",
            "covered_through_cursor": {
                "ts_utc": "2026-04-14T07:55:00Z",
                "slot": parse_ts("2026-04-14T07:55:00Z")?.timestamp() as u64,
                "signature": "sig-staged"
            },
            "last_batch_completed_at": "2026-04-14T08:03:00Z",
            "updated_at": "2026-04-14T08:03:00Z",
            "snapshot_bytes": staged_bytes
        });
        write_json_atomic(&staged_metadata_path, &staged_manifest)?;

        let rendered = run_command(Command::ExplainRecentRawStagedWindowSeeding(
            ExplainRecentRawStagedWindowSeedingConfig {
                state_root: state_root.clone(),
                json: true,
            },
        ))?;
        let parsed: Value = serde_json::from_str(&rendered)?;
        assert_eq!(
            parsed["recent_raw_staged_window_seeding_reason_class"],
            "recent_raw_staged_window_current_start_matches_promoted_start"
        );
        assert_eq!(
            parsed["recent_raw_staged_start_matches_promoted_start"],
            true
        );
        assert_eq!(
            parsed["recent_raw_staged_start_current_evidence_basis"],
            "matches_promoted_start"
        );
        assert_eq!(
            parsed
                ["recent_raw_staged_window_historical_seeding_basis_proven_from_current_artifacts"],
            false
        );
        assert_eq!(
            parsed["recent_raw_promoted_can_seed_staged_progress_under_current_code"],
            true
        );
        assert_eq!(
            parsed["recent_raw_staged_manifest_matches_sqlite_content"],
            true
        );
        Ok(())
    }

    #[test]
    fn run_command_recent_raw_source_window_contract_renders_json() -> Result<()> {
        let fixture = make_fixture("runtime-export-recent-raw-source-window-contract")?;
        let state_root = fixture
            .config_path
            .parent()
            .expect("config parent")
            .join("state");
        let recent_raw_dir = state_root.join("discovery_restore/recent_raw");
        fs::create_dir_all(&recent_raw_dir)?;
        fixture.store.insert_recent_raw_journal_batch(
            &[
                recent_raw_swap(
                    "raw-wallet",
                    "sig-source-old",
                    parse_ts("2026-04-14T07:54:00Z")?,
                ),
                recent_raw_swap(
                    "raw-wallet",
                    "sig-source-a",
                    parse_ts("2026-04-14T07:56:00Z")?,
                ),
                recent_raw_swap(
                    "raw-wallet",
                    "sig-source-b",
                    parse_ts("2026-04-14T07:57:00Z")?,
                ),
            ],
            parse_ts("2026-04-14T08:06:00Z")?,
        )?;
        fixture.store.prune_recent_raw_journal_before_batch(
            parse_ts("2026-04-14T07:56:00Z")?,
            32,
            parse_ts("2026-04-14T08:07:00Z")?,
        )?;
        let latest_path = recent_raw_dir.join("latest.sqlite");
        let latest_metadata_path = recent_raw_dir.join("latest.json");
        let promoted_manifest = serde_json::json!({
            "created_at": "2026-04-14T08:00:00Z",
            "source_db_path": fixture.db_path.display().to_string(),
            "snapshot_path": latest_path.display().to_string(),
            "row_count": 2,
            "covered_since": "2026-04-14T07:55:00Z",
            "covered_through_cursor": {
                "ts_utc": "2026-04-14T07:57:00Z",
                "slot": parse_ts("2026-04-14T07:57:00Z")?.timestamp() as u64,
                "signature": "sig-promoted"
            },
            "last_batch_completed_at": "2026-04-14T08:00:00Z",
            "updated_at": "2026-04-14T08:00:00Z",
            "snapshot_bytes": 8u64
        });
        fs::write(&latest_path, b"snapshot")
            .with_context(|| format!("failed writing {}", latest_path.display()))?;
        write_json_atomic(&latest_metadata_path, &promoted_manifest)?;

        let rendered = run_command(Command::ExplainRecentRawSourceWindowContract(
            ExplainRecentRawSourceWindowContractConfig {
                state_root,
                json: true,
            },
        ))?;
        let parsed: Value = serde_json::from_str(&rendered)?;
        assert_eq!(
            parsed["recent_raw_source_window_contract_reason_class"],
            "recent_raw_source_window_current_contract_excludes_older_rows"
        );
        assert_eq!(parsed["recent_raw_source_start_later_than_promoted"], true);
        assert_eq!(
            parsed["recent_raw_source_contract_currently_excludes_older_rows"],
            true
        );
        assert_eq!(
            parsed["recent_raw_source_window_matches_current_bounded_contract"],
            true
        );
        assert_eq!(parsed["recent_raw_source_window_probe_bounded"], true);
        assert_eq!(
            parsed["recent_raw_source_window_probe_mode"],
            "bounded_index_edges"
        );
        assert_eq!(
            parsed["recent_raw_source_cached_state_matches_bounded_probe"],
            true
        );
        assert!(parsed["recent_raw_source_bounded_probe_covered_since"].is_string());
        assert!(parsed["recent_raw_source_bounded_probe_covered_through"].is_object());
        assert!(parsed["recent_raw_source_scanned_covered_since"].is_null());
        assert!(parsed["recent_raw_source_scanned_covered_through"].is_null());
        assert!(parsed["recent_raw_source_scanned_row_count"].is_null());
        assert!(parsed["recent_raw_source_cached_state_matches_scanned_rows"].is_null());
        assert_eq!(
            parsed["recent_raw_promoted_reflects_older_still_promoted_window"],
            true
        );
        Ok(())
    }

    #[test]
    fn run_command_recent_raw_promoted_retention_contract_renders_json() -> Result<()> {
        let fixture = make_fixture("runtime-export-recent-raw-promoted-retention-contract")?;
        let state_root = fixture
            .config_path
            .parent()
            .expect("config parent")
            .join("state");
        let recent_raw_dir = state_root.join("discovery_restore/recent_raw");
        fs::create_dir_all(&recent_raw_dir)?;
        fixture.store.insert_recent_raw_journal_batch(
            &[
                recent_raw_swap(
                    "raw-wallet",
                    "sig-source-old",
                    parse_ts("2026-04-14T07:54:00Z")?,
                ),
                recent_raw_swap(
                    "raw-wallet",
                    "sig-source-a",
                    parse_ts("2026-04-14T07:56:00Z")?,
                ),
                recent_raw_swap(
                    "raw-wallet",
                    "sig-source-b",
                    parse_ts("2026-04-14T07:57:00Z")?,
                ),
            ],
            parse_ts("2026-04-14T08:06:00Z")?,
        )?;
        fixture.store.prune_recent_raw_journal_before_batch(
            parse_ts("2026-04-14T07:56:00Z")?,
            32,
            parse_ts("2026-04-14T08:07:00Z")?,
        )?;
        let latest_path = recent_raw_dir.join("latest.sqlite");
        let latest_metadata_path = recent_raw_dir.join("latest.json");
        let promoted_manifest = serde_json::json!({
            "created_at": "2026-04-14T08:00:00Z",
            "source_db_path": fixture.db_path.display().to_string(),
            "snapshot_path": latest_path.display().to_string(),
            "row_count": 2,
            "covered_since": "2026-04-14T07:55:00Z",
            "covered_through_cursor": {
                "ts_utc": "2026-04-14T07:57:00Z",
                "slot": parse_ts("2026-04-14T07:57:00Z")?.timestamp() as u64,
                "signature": "sig-promoted"
            },
            "last_batch_completed_at": "2026-04-14T08:00:00Z",
            "updated_at": "2026-04-14T08:00:00Z",
            "snapshot_bytes": 8u64
        });
        fs::write(&latest_path, b"snapshot")
            .with_context(|| format!("failed writing {}", latest_path.display()))?;
        write_json_atomic(&latest_metadata_path, &promoted_manifest)?;

        let rendered = run_command(Command::ExplainRecentRawPromotedRetentionContract(
            ExplainRecentRawPromotedRetentionContractConfig {
                state_root,
                json: true,
            },
        ))?;
        let parsed: Value = serde_json::from_str(&rendered)?;
        assert_eq!(
            parsed["recent_raw_promoted_retention_reason_class"],
            "recent_raw_promoted_retained_by_design_despite_older_window"
        );
        assert_eq!(
            parsed["recent_raw_promoted_currently_retained_as_truth"],
            true
        );
        assert_eq!(
            parsed["recent_raw_promoted_has_current_contract_invalidation_rule"],
            false
        );
        assert_eq!(
            parsed["recent_raw_promoted_invalidated_by_current_source_window_shift"],
            false
        );
        assert_eq!(
            parsed["recent_raw_promoted_start_older_than_current_source"],
            true
        );
        assert_eq!(
            parsed["recent_raw_stage3_truth_currently_depends_on_retained_older_promoted_surface"],
            true
        );
        assert_eq!(
            parsed["recent_raw_stage3_truth_can_advance_without_new_promotion"],
            false
        );
        assert_eq!(
            parsed["recent_raw_promotion_reason_class"],
            "recent_raw_promotion_blocked_by_missing_staged_snapshot"
        );
        assert_eq!(
            parsed["recent_raw_source_window_contract_reason_class"],
            "recent_raw_source_window_current_contract_excludes_older_rows"
        );
        Ok(())
    }

    #[test]
    fn run_command_recent_raw_replacement_promotion_contract_renders_json() -> Result<()> {
        let fixture = make_fixture("runtime-export-recent-raw-replacement-promotion-contract")?;
        let state_root = fixture
            .config_path
            .parent()
            .expect("config parent")
            .join("state");
        let recent_raw_dir = state_root.join("discovery_restore/recent_raw");
        fs::create_dir_all(&recent_raw_dir)?;
        fixture.store.insert_recent_raw_journal_batch(
            &[
                recent_raw_swap(
                    "raw-wallet",
                    "sig-source-old",
                    parse_ts("2026-04-14T07:55:00Z")?,
                ),
                recent_raw_swap(
                    "raw-wallet",
                    "sig-promoted",
                    parse_ts("2026-04-14T07:56:00Z")?,
                ),
                recent_raw_swap(
                    "raw-wallet",
                    "sig-source",
                    parse_ts("2026-04-14T07:57:00Z")?,
                ),
            ],
            parse_ts("2026-04-14T08:06:00Z")?,
        )?;
        fixture.store.prune_recent_raw_journal_before_batch(
            parse_ts("2026-04-14T07:56:00Z")?,
            32,
            parse_ts("2026-04-14T08:07:00Z")?,
        )?;
        let latest_path = recent_raw_dir.join("latest.sqlite");
        let latest_metadata_path = recent_raw_dir.join("latest.json");
        let staged_path = recent_raw_dir.join(".discovery_recent_raw_staged.sqlite.archive-staged");
        let staged_metadata_path =
            recent_raw_dir.join(".discovery_recent_raw_staged.sqlite.archive-staged.json");

        let promoted_manifest = serde_json::json!({
            "created_at": "2026-04-14T08:00:00Z",
            "source_db_path": fixture.db_path.display().to_string(),
            "snapshot_path": latest_path.display().to_string(),
            "row_count": 2,
            "covered_since": "2026-04-14T07:55:00Z",
            "covered_through_cursor": {
                "ts_utc": "2026-04-14T07:56:00Z",
                "slot": parse_ts("2026-04-14T07:56:00Z")?.timestamp() as u64,
                "signature": "sig-promoted"
            },
            "last_batch_completed_at": "2026-04-14T08:00:00Z",
            "updated_at": "2026-04-14T08:00:00Z",
            "snapshot_bytes": 8u64
        });
        fs::write(&latest_path, b"snapshot")
            .with_context(|| format!("failed writing {}", latest_path.display()))?;
        write_json_atomic(&latest_metadata_path, &promoted_manifest)?;
        write_recent_raw_snapshot_sqlite_content(
            &staged_path,
            &[recent_raw_swap(
                "raw-wallet",
                "sig-promoted",
                parse_ts("2026-04-14T07:56:00Z")?,
            )],
            parse_ts("2026-04-14T08:05:00Z")?,
        )?;
        let staged_bytes = fs::metadata(&staged_path)
            .with_context(|| format!("failed stat {}", staged_path.display()))?
            .len();
        let staged_manifest = serde_json::json!({
            "created_at": "2026-04-14T08:05:00Z",
            "source_db_path": fixture.db_path.display().to_string(),
            "snapshot_path": staged_path.display().to_string(),
            "row_count": 1,
            "covered_since": "2026-04-14T07:56:00Z",
            "covered_through_cursor": {
                "ts_utc": "2026-04-14T07:56:00Z",
                "slot": parse_ts("2026-04-14T07:56:00Z")?.timestamp() as u64,
                "signature": "sig-promoted"
            },
            "last_batch_completed_at": "2026-04-14T08:05:00Z",
            "updated_at": "2026-04-14T08:05:00Z",
            "snapshot_bytes": staged_bytes
        });
        write_json_atomic(&staged_metadata_path, &staged_manifest)?;

        let rendered = run_command(Command::ExplainRecentRawReplacementPromotionContract(
            ExplainRecentRawReplacementPromotionContractConfig {
                state_root,
                json: true,
            },
        ))?;
        let parsed: Value = serde_json::from_str(&rendered)?;
        assert_eq!(
            parsed["recent_raw_replacement_promotion_reason_class"],
            "recent_raw_replacement_candidate_incomplete_against_current_source"
        );
        assert_eq!(parsed["recent_raw_replacement_candidate_exists"], true);
        assert_eq!(
            parsed["recent_raw_replacement_candidate_source_db_matches_promoted"],
            true
        );
        assert_eq!(
            parsed["recent_raw_replacement_candidate_start_matches_current_source"],
            true
        );
        assert_eq!(
            parsed["recent_raw_replacement_candidate_complete_against_current_source"],
            false
        );
        assert_eq!(
            parsed["recent_raw_replacement_candidate_promotable_now"],
            false
        );
        assert_eq!(
            parsed["recent_raw_replacement_promotion_would_retire_current_promoted_truth"],
            true
        );
        assert_eq!(
            parsed["recent_raw_stage3_blocked_on_replacement_candidate"],
            true
        );
        assert_eq!(
            parsed["recent_raw_promotion_reason_class"],
            "recent_raw_promotion_blocked_by_incomplete_staged_coverage"
        );
        Ok(())
    }

    #[test]
    fn run_command_recent_raw_replacement_progress_contract_renders_json() -> Result<()> {
        let fixture = make_fixture("runtime-export-recent-raw-replacement-progress-contract")?;
        let state_root = fixture
            .config_path
            .parent()
            .expect("config parent")
            .join("state");
        let recent_raw_dir = state_root.join("discovery_restore/recent_raw");
        fs::create_dir_all(&recent_raw_dir)?;
        fixture.store.insert_recent_raw_journal_batch(
            &[
                recent_raw_swap(
                    "raw-wallet",
                    "sig-source-a",
                    parse_ts("2026-04-14T07:56:00Z")?,
                ),
                recent_raw_swap(
                    "raw-wallet",
                    "sig-source-b",
                    parse_ts("2026-04-14T07:57:00Z")?,
                ),
                recent_raw_swap(
                    "raw-wallet",
                    "sig-source-c",
                    parse_ts("2026-04-14T07:58:00Z")?,
                ),
            ],
            parse_ts("2026-04-14T08:12:00Z")?,
        )?;
        let latest_path = recent_raw_dir.join("latest.sqlite");
        let latest_metadata_path = recent_raw_dir.join("latest.json");
        let staged_path = recent_raw_dir.join(".discovery_recent_raw_staged.sqlite.archive-staged");
        let staged_metadata_path =
            recent_raw_dir.join(".discovery_recent_raw_staged.sqlite.archive-staged.json");
        let previous_staged_path =
            recent_raw_dir.join(".discovery_recent_raw_staged.sqlite.archive-staged.prev");
        let previous_staged_metadata_path =
            recent_raw_dir.join(".discovery_recent_raw_staged.sqlite.archive-staged.prev.json");

        let promoted_manifest = serde_json::json!({
            "created_at": "2026-04-14T08:00:00Z",
            "source_db_path": fixture.db_path.display().to_string(),
            "snapshot_path": latest_path.display().to_string(),
            "row_count": 2,
            "covered_since": "2026-04-14T07:55:00Z",
            "covered_through_cursor": {
                "ts_utc": "2026-04-14T07:56:00Z",
                "slot": parse_ts("2026-04-14T07:56:00Z")?.timestamp() as u64,
                "signature": "sig-promoted"
            },
            "last_batch_completed_at": "2026-04-14T08:00:00Z",
            "updated_at": "2026-04-14T08:00:00Z",
            "snapshot_bytes": 8u64
        });
        fs::write(&latest_path, b"snapshot")
            .with_context(|| format!("failed writing {}", latest_path.display()))?;
        write_json_atomic(&latest_metadata_path, &promoted_manifest)?;
        write_recent_raw_snapshot_sqlite_content(
            &staged_path,
            &[
                recent_raw_swap(
                    "raw-wallet",
                    "sig-source-a",
                    parse_ts("2026-04-14T07:56:00Z")?,
                ),
                recent_raw_swap(
                    "raw-wallet",
                    "sig-source-b",
                    parse_ts("2026-04-14T07:57:00Z")?,
                ),
            ],
            parse_ts("2026-04-14T08:10:00Z")?,
        )?;
        let staged_bytes = fs::metadata(&staged_path)
            .with_context(|| format!("failed stat {}", staged_path.display()))?
            .len();
        let staged_manifest = serde_json::json!({
            "created_at": "2026-04-14T08:05:00Z",
            "source_db_path": fixture.db_path.display().to_string(),
            "snapshot_path": staged_path.display().to_string(),
            "row_count": 2,
            "covered_since": "2026-04-14T07:56:00Z",
            "covered_through_cursor": {
                "ts_utc": "2026-04-14T07:57:00Z",
                "slot": parse_ts("2026-04-14T07:57:00Z")?.timestamp() as u64,
                "signature": "sig-source-b"
            },
            "last_batch_completed_at": "2026-04-14T08:10:00Z",
            "updated_at": "2026-04-14T08:10:00Z",
            "snapshot_bytes": staged_bytes
        });
        write_json_atomic(&staged_metadata_path, &staged_manifest)?;
        fs::write(&previous_staged_path, b"snapshot")
            .with_context(|| format!("failed writing {}", previous_staged_path.display()))?;
        let previous_staged_manifest = serde_json::json!({
            "created_at": "2026-04-14T08:05:00Z",
            "source_db_path": fixture.db_path.display().to_string(),
            "snapshot_path": previous_staged_path.display().to_string(),
            "row_count": 1,
            "covered_since": "2026-04-14T07:56:00Z",
            "covered_through_cursor": {
                "ts_utc": "2026-04-14T07:56:00Z",
                "slot": parse_ts("2026-04-14T07:56:00Z")?.timestamp() as u64,
                "signature": "sig-source-a"
            },
            "last_batch_completed_at": "2026-04-14T08:05:00Z",
            "updated_at": "2026-04-14T08:05:00Z",
            "snapshot_bytes": 8u64
        });
        write_json_atomic(&previous_staged_metadata_path, &previous_staged_manifest)?;

        let rendered = run_command(Command::ExplainRecentRawReplacementProgressContract(
            ExplainRecentRawReplacementProgressContractConfig {
                state_root,
                json: true,
            },
        ))?;
        let parsed: Value = serde_json::from_str(&rendered)?;
        assert_eq!(
            parsed["recent_raw_replacement_progress_reason_class"],
            "recent_raw_replacement_progress_advancing_but_incomplete"
        );
        assert_eq!(parsed["recent_raw_replacement_candidate_exists"], true);
        assert_eq!(
            parsed["recent_raw_replacement_candidate_same_identity_as_previous_attempt"],
            true
        );
        assert_eq!(
            parsed["recent_raw_replacement_candidate_row_count_advanced"],
            true
        );
        assert_eq!(
            parsed["recent_raw_replacement_candidate_covered_through_advanced"],
            true
        );
        assert_eq!(
            parsed["recent_raw_replacement_candidate_last_batch_completed_at_advanced"],
            true
        );
        assert_eq!(
            parsed["recent_raw_replacement_candidate_completed_after_creation"],
            true
        );
        assert_eq!(parsed["recent_raw_replacement_candidate_advancing"], true);
        assert_eq!(parsed["recent_raw_replacement_candidate_stalled"], false);
        assert_eq!(
            parsed["recent_raw_replacement_candidate_reset_or_recreated"],
            false
        );
        assert_eq!(
            parsed["recent_raw_replacement_promotion_reason_class"],
            "recent_raw_replacement_candidate_incomplete_against_current_source"
        );
        Ok(())
    }

    #[test]
    fn run_writes_runtime_artifact_json() -> Result<()> {
        let fixture = make_fixture("runtime-export")?;
        let now = parse_ts("2026-03-23T12:10:00Z")?;
        seed_runtime_export_source(&fixture.store, now)?;

        let output = run(Config {
            config_path: fixture.config_path.clone(),
            db_path: None,
            output_path: Some(PathBuf::from("artifacts/runtime-export.json")),
            scheduled: false,
            force: false,
            json: false,
            now,
        })?;

        assert!(output.contains("event=discovery_runtime_export"));
        let artifact_path = fixture
            .config_path
            .parent()
            .expect("config parent")
            .join("artifacts/runtime-export.json");
        let artifact: DiscoveryRuntimeArtifact = load_json(&artifact_path)?;
        assert_eq!(artifact.runtime_cursor.signature, "runtime-export-cursor");
        assert_eq!(
            artifact.publication_state.last_published_at,
            Some(now - Duration::minutes(5))
        );
        assert_eq!(artifact.published_wallet_metrics_snapshot.len(), 2);
        Ok(())
    }

    #[test]
    fn run_exports_fresh_complete_fail_closed_publication_truth() -> Result<()> {
        let fixture = make_fixture("runtime-export-fail-closed-publication-truth")?;
        let now = parse_ts("2026-03-23T12:10:00Z")?;
        seed_runtime_export_source(&fixture.store, now)?;
        let metrics_window_start = metrics_window_start(now);
        fixture
            .store
            .set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
                runtime_mode: DiscoveryRuntimeMode::FailClosed,
                reason: "fail_closed_current_runtime_but_recent_publication_truth_is_complete"
                    .to_string(),
                last_published_at: Some(now - Duration::minutes(5)),
                last_published_window_start: Some(metrics_window_start),
                published_scoring_source: Some("raw_window".to_string()),
                published_wallet_ids: Some(vec!["wallet-alpha".to_string()]),
            })?;

        let output_json = run(Config {
            config_path: fixture.config_path.clone(),
            db_path: None,
            output_path: Some(PathBuf::from("artifacts/runtime-export-fail-closed.json")),
            scheduled: false,
            force: false,
            json: true,
            now,
        })?;

        let output: serde_json::Value =
            serde_json::from_str(&output_json).context("failed parsing json")?;
        assert_eq!(output["state"], "written");
        assert_eq!(output["publication_runtime_mode"], "fail_closed");
        assert_eq!(output["publication_truth_complete"], true);
        assert_eq!(output["fresh_under_export_gate"], true);

        let artifact_path = fixture
            .config_path
            .parent()
            .expect("config parent")
            .join("artifacts/runtime-export-fail-closed.json");
        let artifact: DiscoveryRuntimeArtifact = load_json(&artifact_path)?;
        assert_eq!(
            artifact.publication_state.runtime_mode,
            DiscoveryRuntimeMode::FailClosed
        );
        assert_eq!(
            artifact.publication_state.last_published_at,
            Some(now - Duration::minutes(5))
        );
        Ok(())
    }

    #[test]
    fn run_refuses_stale_publication_truth_with_actionable_diagnostics() -> Result<()> {
        let fixture = make_fixture("runtime-export-stale-publication-truth")?;
        let now = parse_ts("2026-03-23T12:10:00Z")?;
        seed_runtime_export_source(&fixture.store, now)?;
        fixture
            .store
            .set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
                runtime_mode: DiscoveryRuntimeMode::Healthy,
                reason: "stale_publication_truth_for_export".to_string(),
                last_published_at: Some(now - Duration::hours(2)),
                last_published_window_start: Some(metrics_window_start(now)),
                published_scoring_source: Some("raw_window".to_string()),
                published_wallet_ids: Some(vec!["wallet-alpha".to_string()]),
            })?;

        let error = run(Config {
            config_path: fixture.config_path.clone(),
            db_path: None,
            output_path: Some(PathBuf::from("artifacts/runtime-export-stale.json")),
            scheduled: false,
            force: false,
            json: false,
            now,
        })
        .expect_err("stale publication truth must refuse export");
        let error_text = format!("{error:#}");
        assert!(error_text.contains("requires fresh publication truth under export gate"));
        assert!(error_text.contains("runtime_mode=healthy"));
        assert!(error_text.contains("fresh_under_export_gate=false"));
        assert!(error_text.contains("published_wallet_count=1"));
        Ok(())
    }

    #[test]
    fn run_refuses_incomplete_publication_truth_with_actionable_diagnostics() -> Result<()> {
        let fixture = make_fixture("runtime-export-incomplete-publication-truth")?;
        let now = parse_ts("2026-03-23T12:10:00Z")?;
        seed_runtime_export_source(&fixture.store, now)?;
        fixture
            .store
            .set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
                runtime_mode: DiscoveryRuntimeMode::FailClosed,
                reason: "missing_wallet_ids".to_string(),
                last_published_at: Some(now - Duration::minutes(5)),
                last_published_window_start: Some(metrics_window_start(now)),
                published_scoring_source: Some("raw_window".to_string()),
                published_wallet_ids: Some(Vec::new()),
            })?;

        let error = run(Config {
            config_path: fixture.config_path.clone(),
            db_path: None,
            output_path: Some(PathBuf::from("artifacts/runtime-export-incomplete.json")),
            scheduled: false,
            force: false,
            json: false,
            now,
        })
        .expect_err("incomplete publication truth must refuse export");
        let error_text = format!("{error:#}");
        assert!(error_text.contains("requires complete publication truth"));
        assert!(error_text.contains("runtime_mode=fail_closed"));
        assert!(error_text.contains("missing_fields=published_wallet_ids"));
        Ok(())
    }

    #[test]
    fn scheduled_run_writes_latest_and_prunes_archives() -> Result<()> {
        let fixture = make_fixture("runtime-export-scheduled")?;
        let first_now = parse_ts("2026-03-23T12:10:00Z")?;
        let second_now = parse_ts("2026-03-23T12:21:00Z")?;
        let third_now = parse_ts("2026-03-23T12:32:00Z")?;
        seed_runtime_export_source(&fixture.store, third_now)?;

        for now in [first_now, second_now, third_now] {
            run(Config {
                config_path: fixture.config_path.clone(),
                db_path: None,
                output_path: None,
                scheduled: true,
                force: true,
                json: false,
                now,
            })?;
        }

        let archive_dir = fixture
            .config_path
            .parent()
            .expect("config parent")
            .join("state/discovery_restore/artifacts");
        let archives = std::fs::read_dir(&archive_dir)?
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| {
                        name.starts_with("discovery_runtime_") && name.ends_with(".json")
                    })
            })
            .collect::<Vec<_>>();
        assert_eq!(archives.len(), 2, "retention should prune oldest archive");
        assert!(archive_dir.join("latest.json").exists());
        Ok(())
    }

    #[test]
    fn scheduled_run_skips_when_cadence_not_elapsed() -> Result<()> {
        let fixture = make_fixture("runtime-export-skip")?;
        let now = parse_ts("2026-03-23T12:10:00Z")?;
        seed_runtime_export_source(&fixture.store, now)?;

        run(Config {
            config_path: fixture.config_path.clone(),
            db_path: None,
            output_path: None,
            scheduled: true,
            force: false,
            json: false,
            now,
        })?;

        let skipped = run(Config {
            config_path: fixture.config_path.clone(),
            db_path: None,
            output_path: None,
            scheduled: true,
            force: false,
            json: true,
            now: now + Duration::minutes(3),
        })?;
        let output: serde_json::Value =
            serde_json::from_str(&skipped).context("failed parsing json")?;
        assert_eq!(output["state"], "skipped_not_due");
        Ok(())
    }

    struct Fixture {
        store: SqliteStore,
        config_path: PathBuf,
        db_path: PathBuf,
        _temp: tempfile::TempDir,
    }

    fn make_fixture(name: &str) -> Result<Fixture> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join(format!("{name}.db"));
        let config_path = temp.path().join(format!("{name}.toml"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(&db_path)?;
        store.run_migrations(&migration_dir)?;
        std::fs::write(
            &config_path,
            format!(
                "[sqlite]\npath = \"{}\"\n\n[runtime_restore_ops]\nartifact_retention = 2\nartifact_cadence_minutes = 10\n\n[discovery]\nscoring_window_days = 7\nrefresh_seconds = 600\nmetric_snapshot_interval_seconds = 1800\nmax_window_swaps_in_memory = 8\nmax_fetch_swaps_per_cycle = 8\nmax_fetch_pages_per_cycle = 5\nfetch_time_budget_ms = 1000\nobserved_swaps_retention_days = 14\n",
                db_path.display()
            ),
        )
        .context("failed writing config")?;
        Ok(Fixture {
            store,
            config_path,
            db_path,
            _temp: temp,
        })
    }

    fn seed_runtime_export_source(store: &SqliteStore, now: DateTime<Utc>) -> Result<()> {
        let metrics_window_start = metrics_window_start(now);
        store.persist_discovery_cycle(
            &[
                WalletUpsertRow {
                    wallet_id: "wallet-alpha".to_string(),
                    first_seen: now - Duration::days(3),
                    last_seen: now - Duration::minutes(2),
                    status: "candidate".to_string(),
                },
                WalletUpsertRow {
                    wallet_id: "wallet-beta".to_string(),
                    first_seen: now - Duration::days(2),
                    last_seen: now - Duration::minutes(1),
                    status: "observed".to_string(),
                },
            ],
            &[
                WalletMetricRow {
                    wallet_id: "wallet-alpha".to_string(),
                    window_start: metrics_window_start,
                    pnl: 2.8,
                    win_rate: 0.8,
                    trades: 6,
                    closed_trades: 6,
                    hold_median_seconds: 120,
                    score: 1.0,
                    buy_total: 6,
                    tradable_ratio: 1.0,
                    rug_ratio: 0.0,
                },
                WalletMetricRow {
                    wallet_id: "wallet-beta".to_string(),
                    window_start: metrics_window_start,
                    pnl: 0.3,
                    win_rate: 0.5,
                    trades: 4,
                    closed_trades: 4,
                    hold_median_seconds: 240,
                    score: 0.1,
                    buy_total: 4,
                    tradable_ratio: 0.5,
                    rug_ratio: 0.25,
                },
            ],
            &["wallet-alpha".to_string()],
            true,
            true,
            now - Duration::minutes(5),
            "seed_runtime_export",
        )?;
        store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: DiscoveryRuntimeMode::Healthy,
            reason: "seed_runtime_export".to_string(),
            last_published_at: Some(now - Duration::minutes(5)),
            last_published_window_start: Some(metrics_window_start),
            published_scoring_source: Some("raw_window".to_string()),
            published_wallet_ids: Some(vec!["wallet-alpha".to_string()]),
        })?;
        store.upsert_discovery_runtime_cursor(&DiscoveryRuntimeCursor {
            ts_utc: now - Duration::minutes(1),
            slot: 42,
            signature: "runtime-export-cursor".to_string(),
        })?;
        Ok(())
    }

    fn seed_recent_raw_source_state(
        store: &SqliteStore,
        _db_path: &Path,
        recent_raw_dir: &Path,
    ) -> Result<()> {
        let now = parse_ts("2026-04-14T08:06:00Z")?;
        store.insert_recent_raw_journal_batch(
            &[
                recent_raw_swap(
                    "raw-wallet",
                    "sig-promoted",
                    parse_ts("2026-04-14T07:55:00Z")?,
                ),
                recent_raw_swap(
                    "raw-wallet",
                    "sig-staged",
                    parse_ts("2026-04-14T07:56:00Z")?,
                ),
            ],
            now,
        )?;
        std::fs::create_dir_all(recent_raw_dir)
            .with_context(|| format!("failed creating {}", recent_raw_dir.display()))?;
        Ok(())
    }

    fn write_recent_raw_snapshot_sqlite_content(
        snapshot_path: &Path,
        swaps: &[copybot_core_types::SwapEvent],
        completed_at: DateTime<Utc>,
    ) -> Result<()> {
        if snapshot_path.exists() {
            fs::remove_file(snapshot_path)
                .with_context(|| format!("failed removing {}", snapshot_path.display()))?;
        }
        let store = SqliteStore::open(snapshot_path)
            .with_context(|| format!("failed opening {}", snapshot_path.display()))?;
        store.insert_recent_raw_journal_batch(swaps, completed_at)?;
        Ok(())
    }

    fn metrics_window_start(now: DateTime<Utc>) -> DateTime<Utc> {
        let interval_seconds = 1_800_i64;
        let bucketed_ts = now.timestamp().div_euclid(interval_seconds) * interval_seconds;
        let bucketed_now = DateTime::<Utc>::from_timestamp(bucketed_ts, 0).unwrap_or(now);
        bucketed_now - Duration::days(7)
    }

    fn parse_ts(raw: &str) -> Result<DateTime<Utc>> {
        Ok(DateTime::parse_from_rfc3339(raw)?.with_timezone(&Utc))
    }

    fn recent_raw_swap(
        wallet: &str,
        signature: &str,
        ts_utc: DateTime<Utc>,
    ) -> copybot_core_types::SwapEvent {
        copybot_core_types::SwapEvent {
            wallet: wallet.to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenMint1111111111111111111111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: signature.to_string(),
            slot: ts_utc.timestamp().max(0) as u64,
            ts_utc,
            exact_amounts: None,
        }
    }
}
