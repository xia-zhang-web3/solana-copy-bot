use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::load_from_path;
use copybot_discovery::{
    cutover_readiness::DiscoveryCutoverReadiness, operator_status::DiscoveryOperatorCursor,
    DiscoveryService,
};
use copybot_storage::SqliteStore;
use std::env;
use std::path::{Path, PathBuf};

const USAGE: &str = "usage: discovery_cutover_readiness --config <path> [--db-path <path>] [--json] [--now <rfc3339>]";

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
    json: bool,
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
            "--json" => json = true,
            "--now" => now = Some(parse_ts_arg("--now", args.next())?),
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    Ok(Some(Config {
        config_path: config_path.ok_or_else(|| anyhow!("missing required --config"))?,
        db_path,
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

fn resolve_db_path(
    config_path: &Path,
    db_path_override: Option<&Path>,
    configured_db_path: &str,
) -> PathBuf {
    if let Some(db_path_override) = db_path_override {
        return db_path_override.to_path_buf();
    }
    let configured_db_path = PathBuf::from(configured_db_path);
    if configured_db_path.is_absolute() {
        return configured_db_path;
    }
    config_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(configured_db_path)
}

fn run(config: Config) -> Result<String> {
    let loaded_config = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let db_path = resolve_db_path(
        &config.config_path,
        config.db_path.as_deref(),
        &loaded_config.sqlite.path,
    );
    let store = SqliteStore::open_read_only(Path::new(&db_path))
        .with_context(|| format!("failed opening sqlite db {}", db_path.display()))?;
    let discovery = DiscoveryService::new(
        loaded_config.discovery.clone(),
        loaded_config.shadow.clone(),
    );
    let readiness = discovery.cutover_readiness(&store, config.now)?;
    if config.json {
        serde_json::to_string_pretty(&readiness)
            .context("failed serializing discovery cutover readiness json")
    } else {
        Ok(render_human(&config.config_path, &db_path, &readiness))
    }
}

fn render_human(
    config_path: &Path,
    db_path: &Path,
    readiness: &DiscoveryCutoverReadiness,
) -> String {
    [
        "event=discovery_cutover_readiness".to_string(),
        format!("config_path={}", config_path.display()),
        format!("db_path={}", db_path.display()),
        format!("now={}", readiness.now.to_rfc3339()),
        format!("verdict={}", readiness.verdict),
        format!("blockers={}", format_blockers(&readiness.blockers)),
        format!(
            "runtime_truth.runtime_state={}",
            readiness.runtime_truth.runtime_state
        ),
        format!(
            "runtime_truth.runtime_mode={}",
            readiness.runtime_truth.runtime_mode
        ),
        format!(
            "runtime_truth.scoring_source={}",
            readiness.runtime_truth.scoring_source
        ),
        format!(
            "runtime_truth.active_follow_wallets={}",
            readiness.runtime_truth.active_follow_wallets
        ),
        format!(
            "runtime_truth.raw_window_state={}",
            readiness.runtime_truth.raw_window_state
        ),
        format!(
            "runtime_truth.false_healthy_detected={}",
            readiness.runtime_truth.false_healthy_detected
        ),
        format!(
            "publication_truth.runtime_mode={}",
            format_optional_str(readiness.publication_truth.runtime_mode.as_deref())
        ),
        format!(
            "publication_truth.latest_publication_ts={}",
            format_optional_ts(readiness.publication_truth.latest_publication_ts.as_ref())
        ),
        format!(
            "publication_truth.publication_age_seconds={}",
            format_optional_u64(readiness.publication_truth.publication_age_seconds)
        ),
        format!(
            "publication_truth.recent_publication_truth_available={}",
            readiness
                .publication_truth
                .recent_publication_truth_available
        ),
        format!(
            "publication_truth.published_wallet_count={}",
            readiness.publication_truth.published_wallet_count
        ),
        format!(
            "publication_truth.published_scoring_source={}",
            format_optional_str(
                readiness
                    .publication_truth
                    .published_scoring_source
                    .as_deref()
            )
        ),
        format!(
            "offline_recovery.state={}",
            readiness.offline_recovery.state
        ),
        format!(
            "offline_recovery.effective_reads_ready={}",
            readiness.offline_recovery.effective_reads_ready
        ),
        format!(
            "offline_recovery.read_blockers={}",
            format_blockers(&readiness.offline_recovery.read_blockers)
        ),
        format!(
            "offline_recovery.cursor={}",
            format_optional_cursor(readiness.offline_recovery.cursor.as_ref())
        ),
        format!(
            "offline_recovery.covered_through_cursor={}",
            format_optional_cursor(readiness.offline_recovery.covered_through_cursor.as_ref())
        ),
    ]
    .join("\n")
}

fn format_blockers(blockers: &[String]) -> String {
    if blockers.is_empty() {
        "none".to_string()
    } else {
        blockers.join(",")
    }
}

fn format_optional_ts(value: Option<&DateTime<Utc>>) -> String {
    value
        .map(DateTime::<Utc>::to_rfc3339)
        .unwrap_or_else(|| "null".to_string())
}

fn format_optional_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "null".to_string())
}

fn format_optional_str(value: Option<&str>) -> String {
    value.unwrap_or("null").to_string()
}

fn format_optional_cursor(value: Option<&DiscoveryOperatorCursor>) -> String {
    value
        .map(|cursor| {
            format!(
                "ts_utc={},slot={},signature={}",
                cursor.ts_utc.to_rfc3339(),
                cursor.slot,
                cursor.signature
            )
        })
        .unwrap_or_else(|| "null".to_string())
}

#[cfg(test)]
mod tests {
    use super::{parse_args_from, render_human, run, Config};
    use anyhow::{Context, Result};
    use chrono::{DateTime, Duration, Utc};
    use copybot_storage::{
        DiscoveryPublicationStateUpdate, DiscoveryRuntimeCursor, SqliteStore, WalletMetricRow,
        WalletUpsertRow,
    };
    use serde_json::Value;
    use std::path::{Path, PathBuf};
    use tempfile::tempdir;

    #[test]
    fn parse_args_from_accepts_json_db_override_and_now() {
        let parsed = parse_args_from(vec![
            "--config".to_string(),
            "configs/live.toml".to_string(),
            "--db-path".to_string(),
            "state/live.db".to_string(),
            "--json".to_string(),
            "--now".to_string(),
            "2026-03-23T12:00:00Z".to_string(),
        ])
        .expect("parse should succeed")
        .expect("config should be present");
        assert_eq!(parsed.config_path, PathBuf::from("configs/live.toml"));
        assert_eq!(parsed.db_path, Some(PathBuf::from("state/live.db")));
        assert!(parsed.json);
        assert_eq!(parsed.now.to_rfc3339(), "2026-03-23T12:00:00+00:00");
    }

    #[test]
    fn run_json_reports_ready_when_runtime_publication_and_offline_recovery_are_clear() -> Result<()>
    {
        let fixture = make_fixture("discovery-cutover-ready")?;
        let now = parse_ts("2026-03-23T12:00:00Z")?;
        seed_active_follow_wallet(&fixture.store, "wallet_ready", now - Duration::days(7))?;
        seed_healthy_runtime_and_publication(&fixture.store, now, "wallet_ready")?;
        seed_aggregate_ready(&fixture.store, now)?;

        let output = run(Config {
            config_path: fixture.config_path.clone(),
            db_path: None,
            json: true,
            now,
        })?;
        let parsed: Value = serde_json::from_str(&output)?;
        assert_eq!(parsed.get("verdict").and_then(Value::as_str), Some("ready"));
        assert_eq!(
            parsed
                .get("blockers")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(0)
        );
        assert_eq!(
            parsed
                .pointer("/runtime_truth/runtime_state")
                .and_then(Value::as_str),
            Some("healthy_runtime_truth")
        );
        assert_eq!(
            parsed
                .pointer("/offline_recovery/effective_reads_ready")
                .and_then(Value::as_bool),
            Some(true)
        );
        Ok(())
    }

    #[test]
    fn run_json_stays_not_ready_when_offline_recovery_is_ready_but_runtime_is_not_healthy(
    ) -> Result<()> {
        let fixture = make_fixture("discovery-cutover-runtime-blocked")?;
        let now = parse_ts("2026-03-23T12:00:00Z")?;
        seed_active_follow_wallet(&fixture.store, "wallet_published", now - Duration::days(7))?;
        fixture
            .store
            .set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
                runtime_mode: copybot_storage::DiscoveryRuntimeMode::Healthy,
                reason: "recent_publication".to_string(),
                last_published_at: Some(now - Duration::minutes(2)),
                last_published_window_start: Some(now - Duration::days(7)),
                published_scoring_source: Some("raw_window".to_string()),
                published_wallet_ids: Some(vec!["wallet_published".to_string()]),
            })?;
        seed_aggregate_ready(&fixture.store, now)?;

        let output = run(Config {
            config_path: fixture.config_path.clone(),
            db_path: None,
            json: true,
            now,
        })?;
        let parsed: Value = serde_json::from_str(&output)?;
        assert_eq!(
            parsed.get("verdict").and_then(Value::as_str),
            Some("not_ready")
        );
        let blockers = parsed
            .get("blockers")
            .and_then(Value::as_array)
            .expect("blockers should be an array");
        assert!(blockers
            .iter()
            .any(|value| value.as_str() == Some("runtime_truth_not_healthy")));
        assert!(blockers
            .iter()
            .any(|value| value.as_str() == Some("runtime_scoring_source_not_raw_window")));
        assert_eq!(
            parsed
                .pointer("/offline_recovery/effective_reads_ready")
                .and_then(Value::as_bool),
            Some(true)
        );
        Ok(())
    }

    #[test]
    fn render_human_reports_active_follow_and_offline_recovery_blockers() -> Result<()> {
        let fixture = make_fixture("discovery-cutover-blockers")?;
        let now = parse_ts("2026-03-23T12:00:00Z")?;
        seed_healthy_runtime_and_publication(&fixture.store, now, "wallet_unfollowed")?;
        seed_aggregate_ready(&fixture.store, now)?;
        let backfill_cursor = DiscoveryRuntimeCursor {
            ts_utc: now - Duration::hours(2),
            slot: 77,
            signature: "offline-backfill-cursor".to_string(),
        };
        fixture
            .store
            .set_discovery_scoring_backfill_progress(now - Duration::days(10), &backfill_cursor)?;
        fixture
            .store
            .set_discovery_scoring_backfill_source_protection(
                now - Duration::days(10),
                now + Duration::minutes(30),
            )?;

        let output = run(Config {
            config_path: fixture.config_path.clone(),
            db_path: None,
            json: false,
            now,
        })?;
        assert!(output.contains("verdict=not_ready"));
        assert!(output.contains("blockers=active_follow_wallets_empty"));
        assert!(output.contains("false_healthy_detected"));
        assert!(output.contains("offline_recovery_backfill_in_progress"));
        assert!(output.contains("offline_recovery.effective_reads_ready=false"));
        assert!(output.contains("offline_recovery.state=backfill_in_progress"));

        let discovery = copybot_discovery::DiscoveryService::new(
            copybot_config::load_from_path(&fixture.config_path)?.discovery,
            copybot_config::load_from_path(&fixture.config_path)?.shadow,
        );
        let store = SqliteStore::open_read_only(Path::new(&fixture.db_path))?;
        let readiness = discovery.cutover_readiness(&store, now)?;
        let rendered = render_human(&fixture.config_path, &fixture.db_path, &readiness);
        assert_eq!(rendered, output);
        Ok(())
    }

    struct Fixture {
        store: SqliteStore,
        db_path: PathBuf,
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
                "[sqlite]\npath = \"{}\"\n\n[discovery]\nscoring_window_days = 7\nrefresh_seconds = 600\nmetric_snapshot_interval_seconds = 1800\nmax_window_swaps_in_memory = 8\nmax_fetch_swaps_per_cycle = 8\nmax_fetch_pages_per_cycle = 5\nfetch_time_budget_ms = 1000\nobserved_swaps_retention_days = 14\nscoring_aggregates_write_enabled = true\nscoring_aggregates_enabled = true\n",
                db_path.display()
            ),
        )
        .context("failed writing config")?;
        Ok(Fixture {
            store,
            db_path,
            config_path,
            _temp: temp,
        })
    }

    fn seed_active_follow_wallet(
        store: &SqliteStore,
        wallet_id: &str,
        window_start: DateTime<Utc>,
    ) -> Result<()> {
        store.persist_discovery_cycle(
            &[WalletUpsertRow {
                wallet_id: wallet_id.to_string(),
                first_seen: window_start,
                last_seen: window_start + Duration::minutes(1),
                status: "active".to_string(),
            }],
            &[WalletMetricRow {
                wallet_id: wallet_id.to_string(),
                window_start,
                pnl: 1.0,
                win_rate: 1.0,
                trades: 4,
                closed_trades: 4,
                hold_median_seconds: 60,
                score: 1.0,
                buy_total: 4,
                tradable_ratio: 1.0,
                rug_ratio: 0.0,
            }],
            &[wallet_id.to_string()],
            true,
            true,
            window_start + Duration::minutes(2),
            "seed_active_follow_wallet",
        )?;
        Ok(())
    }

    fn seed_healthy_runtime_and_publication(
        store: &SqliteStore,
        now: DateTime<Utc>,
        wallet_id: &str,
    ) -> Result<()> {
        store.insert_observed_swap(&swap(
            wallet_id,
            "healthy-swap",
            now - Duration::minutes(5),
            "So11111111111111111111111111111111111111112",
            "TokenHealthy111111111111111111111111111111",
            1.0,
            100.0,
        ))?;
        store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: copybot_storage::DiscoveryRuntimeMode::Healthy,
            reason: "healthy_refresh".to_string(),
            last_published_at: Some(now - Duration::minutes(1)),
            last_published_window_start: Some(now - Duration::days(7)),
            published_scoring_source: Some("raw_window".to_string()),
            published_wallet_ids: Some(vec![wallet_id.to_string()]),
        })?;
        Ok(())
    }

    fn seed_aggregate_ready(store: &SqliteStore, now: DateTime<Utc>) -> Result<()> {
        store.set_discovery_scoring_covered_since(now - Duration::days(10))?;
        store.set_discovery_scoring_covered_through_cursor(&DiscoveryRuntimeCursor {
            ts_utc: now - Duration::seconds(30),
            slot: 999,
            signature: "covered-through-ready".to_string(),
        })?;
        Ok(())
    }

    fn parse_ts(raw: &str) -> Result<DateTime<Utc>> {
        Ok(DateTime::parse_from_rfc3339(raw)?.with_timezone(&Utc))
    }

    fn swap(
        wallet: &str,
        signature: &str,
        ts_utc: DateTime<Utc>,
        token_in: &str,
        token_out: &str,
        amount_in: f64,
        amount_out: f64,
    ) -> copybot_core_types::SwapEvent {
        copybot_core_types::SwapEvent {
            wallet: wallet.to_string(),
            dex: "raydium".to_string(),
            token_in: token_in.to_string(),
            token_out: token_out.to_string(),
            amount_in,
            amount_out,
            signature: signature.to_string(),
            slot: ts_utc.timestamp().max(0) as u64,
            ts_utc,
            exact_amounts: None,
        }
    }
}
