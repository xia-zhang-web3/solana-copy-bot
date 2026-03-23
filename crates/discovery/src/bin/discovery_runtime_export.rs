use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::load_from_path;
use copybot_discovery::runtime_restore_ops::{
    artifact_archive_path, artifact_latest_path, copy_atomic, load_json, prune_rotated_archives,
    resolve_db_path, resolve_relative_to_config, write_json_atomic, ARTIFACT_ARCHIVE_PREFIX,
    ARTIFACT_ARCHIVE_SUFFIX,
};
use copybot_discovery::DiscoveryService;
use copybot_storage::{DiscoveryRuntimeArtifact, SqliteStore};
use serde::Serialize;
use std::env;
use std::path::{Path, PathBuf};

const USAGE: &str = "usage: discovery_runtime_export --config <path> [--db-path <path>] (--output <path> | --scheduled) [--force] [--json] [--now <rfc3339>]";

fn main() -> Result<()> {
    let Some(config) = parse_args()? else {
        println!("{USAGE}");
        return Ok(());
    };
    println!("{}", run(config)?);
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
    last_published_at: Option<DateTime<Utc>>,
    last_published_window_start: Option<DateTime<Utc>>,
    published_wallet_count: usize,
    wallet_metrics_snapshot_rows: usize,
    fresh_under_current_gate: bool,
    runtime_cursor_ts: DateTime<Utc>,
    runtime_cursor_slot: u64,
    runtime_cursor_signature: String,
}

fn parse_args() -> Result<Option<Config>> {
    parse_args_from(env::args().skip(1))
}

fn parse_args_from<I>(args: I) -> Result<Option<Config>>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut config_path: Option<PathBuf> = None;
    let mut db_path: Option<PathBuf> = None;
    let mut output_path: Option<PathBuf> = None;
    let mut scheduled = false;
    let mut force = false;
    let mut json = false;
    let mut now: Option<DateTime<Utc>> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(PathBuf::from(parse_string_arg("--config", args.next())?))
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

    if scheduled == output_path.is_some() {
        bail!("exactly one of --output or --scheduled must be provided");
    }

    Ok(Some(Config {
        config_path: config_path.ok_or_else(|| anyhow!("missing required --config"))?,
        db_path,
        output_path,
        scheduled,
        force,
        json,
        now: now.unwrap_or_else(Utc::now),
    }))
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
        write_json_atomic(&output_path, &artifact)
            .with_context(|| format!("failed writing {}", output_path.display()))?;
        let freshness = discovery.assess_runtime_artifact_freshness(&artifact, config.now);
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

#[cfg(test)]
mod tests {
    use super::{load_json, parse_args_from, run, Config};
    use anyhow::{Context, Result};
    use chrono::{DateTime, Duration, Utc};
    use copybot_storage::{
        DiscoveryPublicationStateUpdate, DiscoveryRuntimeArtifact, DiscoveryRuntimeCursor,
        DiscoveryRuntimeMode, SqliteStore, WalletMetricRow, WalletUpsertRow,
    };
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
        .expect("config should be present");

        assert_eq!(parsed.config_path, PathBuf::from("configs/live.toml"));
        assert_eq!(parsed.db_path, Some(PathBuf::from("state/live.db")));
        assert!(parsed.output_path.is_none());
        assert!(parsed.scheduled);
        assert!(parsed.force);
        assert!(parsed.json);
        assert_eq!(parsed.now.to_rfc3339(), "2026-03-23T12:00:00+00:00");
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

    fn metrics_window_start(now: DateTime<Utc>) -> DateTime<Utc> {
        let interval_seconds = 1_800_i64;
        let bucketed_ts = now.timestamp().div_euclid(interval_seconds) * interval_seconds;
        let bucketed_now = DateTime::<Utc>::from_timestamp(bucketed_ts, 0).unwrap_or(now);
        bucketed_now - Duration::days(7)
    }

    fn parse_ts(raw: &str) -> Result<DateTime<Utc>> {
        Ok(DateTime::parse_from_rfc3339(raw)?.with_timezone(&Utc))
    }
}
