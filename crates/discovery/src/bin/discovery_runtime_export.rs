use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::load_from_path;
use copybot_discovery::DiscoveryService;
use copybot_storage::{DiscoveryRuntimeArtifact, SqliteStore};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

const USAGE: &str =
    "usage: discovery_runtime_export --config <path> [--db-path <path>] --output <path> [--now <rfc3339>]";

fn main() -> Result<()> {
    let Some(config) = parse_args()? else {
        println!("{USAGE}");
        return Ok(());
    };
    let output = run(config)?;
    println!("{output}");
    Ok(())
}

#[derive(Debug, Clone)]
struct Config {
    config_path: PathBuf,
    db_path: Option<PathBuf>,
    output_path: PathBuf,
    now: DateTime<Utc>,
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
            "--now" => now = Some(parse_ts_arg("--now", args.next())?),
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    Ok(Some(Config {
        config_path: config_path.ok_or_else(|| anyhow!("missing required --config"))?,
        db_path,
        output_path: output_path.ok_or_else(|| anyhow!("missing required --output"))?,
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

fn resolve_db_path(
    config_path: &Path,
    db_path_override: Option<&Path>,
    configured_db_path: &str,
) -> PathBuf {
    if let Some(db_path_override) = db_path_override {
        return db_path_override.to_path_buf();
    }
    resolve_relative_to_config(config_path, Path::new(configured_db_path))
}

fn resolve_relative_to_config(config_path: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        return path.to_path_buf();
    }
    config_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(path)
}

fn run(config: Config) -> Result<String> {
    let loaded_config = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let db_path = resolve_db_path(
        &config.config_path,
        config.db_path.as_deref(),
        &loaded_config.sqlite.path,
    );
    let output_path = resolve_relative_to_config(&config.config_path, &config.output_path);
    let store = SqliteStore::open(Path::new(&db_path))
        .with_context(|| format!("failed opening sqlite db {}", db_path.display()))?;
    let discovery = DiscoveryService::new(
        loaded_config.discovery.clone(),
        loaded_config.shadow.clone(),
    );
    let artifact = store
        .export_discovery_runtime_artifact(config.now, discovery.publication_freshness_gate())?;
    let artifact_json = serde_json::to_string_pretty(&artifact)
        .context("failed serializing discovery runtime artifact json")?;
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed creating {}", parent.display()))?;
    }
    fs::write(&output_path, artifact_json)
        .with_context(|| format!("failed writing {}", output_path.display()))?;
    let freshness = discovery.assess_runtime_artifact_freshness(&artifact, config.now);
    Ok(render_human(
        &config.config_path,
        &db_path,
        &output_path,
        &artifact,
        freshness.fresh_under_current_gate,
    ))
}

fn render_human(
    config_path: &Path,
    db_path: &Path,
    output_path: &Path,
    artifact: &DiscoveryRuntimeArtifact,
    fresh_under_current_gate: bool,
) -> String {
    [
        "event=discovery_runtime_export".to_string(),
        format!("config_path={}", config_path.display()),
        format!("db_path={}", db_path.display()),
        format!("output_path={}", output_path.display()),
        format!("exported_at={}", artifact.exported_at.to_rfc3339()),
        format!(
            "last_published_at={}",
            format_optional_ts(artifact.publication_state.last_published_at.as_ref())
        ),
        format!(
            "last_published_window_start={}",
            format_optional_ts(
                artifact
                    .publication_state
                    .last_published_window_start
                    .as_ref()
            )
        ),
        format!(
            "published_wallet_count={}",
            artifact
                .publication_state
                .published_wallet_ids
                .as_ref()
                .map(Vec::len)
                .unwrap_or(0)
        ),
        format!(
            "wallet_metrics_snapshot_rows={}",
            artifact.published_wallet_metrics_snapshot.len()
        ),
        format!("fresh_under_current_gate={fresh_under_current_gate}"),
        format!(
            "runtime_cursor_ts={}",
            artifact.runtime_cursor.ts_utc.to_rfc3339()
        ),
        format!("runtime_cursor_slot={}", artifact.runtime_cursor.slot),
        format!(
            "runtime_cursor_signature={}",
            artifact.runtime_cursor.signature
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
    use super::{parse_args_from, run, Config};
    use anyhow::{Context, Result};
    use chrono::{DateTime, Duration, Utc};
    use copybot_storage::{
        DiscoveryPublicationStateUpdate, DiscoveryRuntimeArtifact, DiscoveryRuntimeCursor,
        DiscoveryRuntimeMode, SqliteStore, WalletMetricRow, WalletUpsertRow,
    };
    use std::path::{Path, PathBuf};
    use tempfile::tempdir;

    #[test]
    fn parse_args_from_accepts_output_db_override_and_now() {
        let parsed = parse_args_from(vec![
            "--config".to_string(),
            "configs/live.toml".to_string(),
            "--db-path".to_string(),
            "state/live.db".to_string(),
            "--output".to_string(),
            "artifacts/runtime.json".to_string(),
            "--now".to_string(),
            "2026-03-23T12:00:00Z".to_string(),
        ])
        .expect("parse should succeed")
        .expect("config should be present");

        assert_eq!(parsed.config_path, PathBuf::from("configs/live.toml"));
        assert_eq!(parsed.db_path, Some(PathBuf::from("state/live.db")));
        assert_eq!(parsed.output_path, PathBuf::from("artifacts/runtime.json"));
        assert_eq!(parsed.now.to_rfc3339(), "2026-03-23T12:00:00+00:00");
    }

    #[test]
    fn run_writes_runtime_artifact_json() -> Result<()> {
        let fixture = make_fixture("runtime-export")?;
        let now = parse_ts("2026-03-23T12:10:00Z")?;
        let metrics_window_start = metrics_window_start(now);
        fixture.store.persist_discovery_cycle(
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
        fixture
            .store
            .set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
                runtime_mode: DiscoveryRuntimeMode::Healthy,
                reason: "seed_runtime_export".to_string(),
                last_published_at: Some(now - Duration::minutes(5)),
                last_published_window_start: Some(metrics_window_start),
                published_scoring_source: Some("raw_window".to_string()),
                published_wallet_ids: Some(vec!["wallet-alpha".to_string()]),
            })?;
        fixture
            .store
            .upsert_discovery_runtime_cursor(&DiscoveryRuntimeCursor {
                ts_utc: now - Duration::minutes(1),
                slot: 42,
                signature: "runtime-export-cursor".to_string(),
            })?;

        let output = run(Config {
            config_path: fixture.config_path.clone(),
            db_path: None,
            output_path: PathBuf::from("artifacts/runtime-export.json"),
            now,
        })?;

        assert!(output.contains("event=discovery_runtime_export"));
        let artifact_path = fixture
            .config_path
            .parent()
            .expect("config parent")
            .join("artifacts/runtime-export.json");
        let artifact: DiscoveryRuntimeArtifact = serde_json::from_str(
            &std::fs::read_to_string(&artifact_path)
                .with_context(|| format!("failed reading {}", artifact_path.display()))?,
        )
        .context("failed parsing exported runtime artifact")?;
        assert_eq!(artifact.runtime_cursor.signature, "runtime-export-cursor");
        assert_eq!(
            artifact.publication_state.last_published_at,
            Some(now - Duration::minutes(5))
        );
        assert_eq!(artifact.published_wallet_metrics_snapshot.len(), 2);
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
                "[sqlite]\npath = \"{}\"\n\n[discovery]\nscoring_window_days = 7\nrefresh_seconds = 600\nmetric_snapshot_interval_seconds = 1800\nmax_window_swaps_in_memory = 8\nmax_fetch_swaps_per_cycle = 8\nmax_fetch_pages_per_cycle = 5\nfetch_time_budget_ms = 1000\nobserved_swaps_retention_days = 14\n",
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
