use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::load_from_path;
use copybot_discovery::{
    wallet_freshness_audit::{WalletFreshnessHistoryReport, DEFAULT_HISTORY_CAPTURE_LIMIT},
    DiscoveryService,
};
use copybot_storage::SqliteStore;
use std::env;
use std::path::{Path, PathBuf};

const USAGE: &str = "usage: discovery_wallet_freshness_report --config <path> [--db-path <path>] [--json] [--now <rfc3339>] [--limit <count>] [--recent-horizon-seconds <seconds>]";

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
    limit: usize,
    recent_horizon_seconds: Option<u64>,
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
    let mut limit = DEFAULT_HISTORY_CAPTURE_LIMIT;
    let mut recent_horizon_seconds: Option<u64> = None;

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
            "--limit" => {
                limit = parse_usize_arg("--limit", args.next())?;
            }
            "--recent-horizon-seconds" => {
                recent_horizon_seconds =
                    Some(parse_u64_arg("--recent-horizon-seconds", args.next())?);
            }
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    Ok(Some(Config {
        config_path: config_path.ok_or_else(|| anyhow!("missing required --config"))?,
        db_path,
        json,
        now: now.unwrap_or_else(Utc::now),
        limit: limit.max(1),
        recent_horizon_seconds,
    }))
}

fn parse_string_arg(flag: &str, value: Option<String>) -> Result<String> {
    let raw = value.ok_or_else(|| anyhow!("missing value for {flag}"))?;
    let trimmed = raw.trim().to_string();
    if trimmed.is_empty() {
        bail!("{flag} cannot be empty");
    }
    Ok(trimmed)
}

fn parse_ts_arg(flag: &str, value: Option<String>) -> Result<DateTime<Utc>> {
    let raw = parse_string_arg(flag, value)?;
    DateTime::parse_from_rfc3339(&raw)
        .map(|ts| ts.with_timezone(&Utc))
        .with_context(|| format!("invalid {flag} rfc3339 timestamp: {raw}"))
}

fn parse_usize_arg(flag: &str, value: Option<String>) -> Result<usize> {
    let raw = parse_string_arg(flag, value)?;
    raw.parse::<usize>()
        .with_context(|| format!("invalid {flag} usize value: {raw}"))
}

fn parse_u64_arg(flag: &str, value: Option<String>) -> Result<u64> {
    let raw = parse_string_arg(flag, value)?;
    raw.parse::<u64>()
        .with_context(|| format!("invalid {flag} u64 value: {raw}"))
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
    let store = SqliteStore::open(Path::new(&db_path))
        .with_context(|| format!("failed opening sqlite db {}", db_path.display()))?;
    let discovery = DiscoveryService::new(
        loaded_config.discovery.clone(),
        loaded_config.shadow.clone(),
    );
    let recent_horizon_seconds = config.recent_horizon_seconds.unwrap_or_else(|| {
        discovery.default_wallet_freshness_history_recent_horizon_seconds(config.limit)
    });
    let report = discovery.wallet_freshness_history_report_with_horizon(
        &store,
        config.now,
        config.limit,
        recent_horizon_seconds,
    )?;
    if config.json {
        serde_json::to_string_pretty(&report)
            .context("failed serializing discovery wallet freshness history json")
    } else {
        Ok(render_human(&config.config_path, &db_path, &report))
    }
}

fn render_human(
    config_path: &Path,
    db_path: &Path,
    report: &WalletFreshnessHistoryReport,
) -> String {
    [
        "event=discovery_wallet_freshness_report".to_string(),
        format!("config_path={}", config_path.display()),
        format!("db_path={}", db_path.display()),
        format!("generated_at={}", report.generated_at.to_rfc3339()),
        format!("captures_requested={}", report.captures_requested),
        format!("captures_loaded={}", report.captures_loaded),
        format!("captures_considered={}", report.captures_considered),
        format!(
            "captures_within_recent_horizon={}",
            report.captures_within_recent_horizon
        ),
        format!("recent_horizon_seconds={}", report.recent_horizon_seconds),
        format!(
            "latest_capture_age_seconds={}",
            report
                .latest_capture_age_seconds
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "stale_captures_excluded_from_verdict={}",
            report.stale_captures_excluded_from_verdict
        ),
        format!(
            "stale_captures_excluded_count={}",
            report.stale_captures_excluded_count
        ),
        format!("verdict={}", report.verdict.as_str()),
        format!("reason={}", report.reason),
        format!("fresh_capture_count={}", report.fresh_capture_count),
        format!("drifting_capture_count={}", report.drifting_capture_count),
        format!("stale_capture_count={}", report.stale_capture_count),
        format!(
            "insufficient_raw_capture_count={}",
            report.insufficient_raw_capture_count
        ),
        format!(
            "no_publication_truth_capture_count={}",
            report.no_publication_truth_capture_count
        ),
        format!(
            "exact_published_current_match_count={}",
            report.exact_published_current_match_count
        ),
        format!(
            "exact_active_current_match_count={}",
            report.exact_active_current_match_count
        ),
        format!(
            "active_follow_change_count={}",
            report.active_follow_change_count
        ),
        format!(
            "current_raw_change_count={}",
            report.current_raw_change_count
        ),
        format!(
            "rotation_evidence_capture_count={}",
            report.rotation_evidence_capture_count
        ),
        format!(
            "shadow_signal_present_capture_count={}",
            report.shadow_signal_present_capture_count
        ),
        format!(
            "broad_shadow_signal_capture_count={}",
            report.broad_shadow_signal_capture_count
        ),
    ]
    .join("\n")
}

#[cfg(test)]
mod tests {
    use super::{parse_args_from, run, Config};
    use anyhow::{Context, Result};
    use chrono::{DateTime, Utc};
    use copybot_storage::{DiscoveryWalletFreshnessCaptureWrite, SqliteStore};
    use std::fs;
    use std::path::Path;
    use tempfile::tempdir;

    #[test]
    fn parse_args_reads_limit() -> Result<()> {
        let config = parse_args_from([
            "--config".to_string(),
            "/tmp/live.toml".to_string(),
            "--limit".to_string(),
            "7".to_string(),
            "--recent-horizon-seconds".to_string(),
            "1800".to_string(),
        ])?
        .expect("config should parse");
        assert_eq!(config.limit, 7);
        assert_eq!(config.recent_horizon_seconds, Some(1800));
        Ok(())
    }

    #[test]
    fn run_emits_json_wallet_freshness_history_report() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("wallet-freshness-report-bin.db");
        let config_path = temp.path().join("wallet-freshness-report.toml");
        let store = open_store(&db_path)?;
        seed_capture(&store, "2026-03-25T12:00:00Z", "fresh_current")?;
        seed_capture(&store, "2026-03-25T11:50:00Z", "fresh_current")?;
        seed_capture(&store, "2026-03-25T11:40:00Z", "fresh_current")?;
        fs::write(
            &config_path,
            format!(
                "[sqlite]\npath = \"{}\"\n\n[discovery]\nscoring_window_days = 2\nrefresh_seconds = 600\nmetric_snapshot_interval_seconds = 1800\nmax_window_swaps_in_memory = 128\nmax_fetch_swaps_per_cycle = 128\nmax_fetch_pages_per_cycle = 8\nfetch_time_budget_ms = 1000\nobserved_swaps_retention_days = 14\nfollow_top_n = 20\nmin_score = 0.0\nmin_trades = 1\nmin_active_days = 1\nmin_leader_notional_sol = 0.0\nmin_buy_count = 1\nmin_tradable_ratio = 0.0\nmax_rug_ratio = 1.0\nthin_market_min_unique_traders = 1\n\n[execution]\nenabled = false\n",
                db_path.display()
            ),
        )?;

        let output = run(Config {
            config_path,
            db_path: None,
            json: true,
            now: ts("2026-03-25T12:00:00Z"),
            limit: 3,
            recent_horizon_seconds: Some(3600),
        })?;
        let json: serde_json::Value =
            serde_json::from_str(&output).context("json output must parse")?;
        assert_eq!(json["verdict"], "partially_validated_but_low_rotation");
        assert_eq!(json["captures_considered"], 3);
        assert_eq!(json["captures_within_recent_horizon"], 3);
        assert_eq!(json["stale_captures_excluded_from_verdict"], false);
        Ok(())
    }

    fn open_store(path: &Path) -> Result<SqliteStore> {
        let mut store = SqliteStore::open(path)?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;
        Ok(store)
    }

    fn seed_capture(store: &SqliteStore, captured_at_raw: &str, verdict: &str) -> Result<()> {
        let captured_at = ts(captured_at_raw);
        store.append_discovery_wallet_freshness_capture(&DiscoveryWalletFreshnessCaptureWrite {
            captured_at,
            recent_cycles: 3,
            verdict: verdict.to_string(),
            reason: "seed".to_string(),
            publication_age_seconds: Some(60),
            raw_truth_sufficient: true,
            raw_truth_reason: "full_scoring_window_raw_truth_available".to_string(),
            shadow_signal_verdict: "shadow_signals_present_but_concentrated".to_string(),
            shadow_signal_reason: "seed-shadow".to_string(),
            published_wallet_ids: vec!["wallet-alpha".to_string(), "wallet-beta".to_string()],
            active_follow_wallet_ids: vec!["wallet-alpha".to_string(), "wallet-beta".to_string()],
            current_raw_top_wallet_ids: vec!["wallet-alpha".to_string(), "wallet-beta".to_string()],
            audit_json: serde_json::json!({
                "now": captured_at,
                "window_start": captured_at - chrono::Duration::days(2),
                "verdict": verdict,
                "reason": "seed",
                "follow_top_n": 2,
                "publication_truth_available": true,
                "publication_runtime_mode": "healthy",
                "publication_recent_under_gate": true,
                "latest_publication_ts": captured_at,
                "publication_age_seconds": 60,
                "latest_publication_window_start": captured_at - chrono::Duration::days(2),
                "published_scoring_source": "raw_window_persisted_stream",
                "published_wallet_ids": ["wallet-alpha", "wallet-beta"],
                "active_follow_wallet_ids": ["wallet-alpha", "wallet-beta"],
                "current_raw_top_wallet_ids": ["wallet-alpha", "wallet-beta"],
                "published_vs_current_raw": {
                    "left_count": 2,
                    "right_count": 2,
                    "overlap_count": 2,
                    "exact_match": true,
                    "only_left": [],
                    "only_right": []
                },
                "active_follow_vs_current_raw": {
                    "left_count": 2,
                    "right_count": 2,
                    "overlap_count": 2,
                    "exact_match": true,
                    "only_left": [],
                    "only_right": []
                },
                "active_follow_vs_published": {
                    "left_count": 2,
                    "right_count": 2,
                    "overlap_count": 2,
                    "exact_match": true,
                    "only_left": [],
                    "only_right": []
                },
                "raw_truth": {
                    "sufficient": true,
                    "reason": "full_scoring_window_raw_truth_available",
                    "observed_swaps_loaded": 10,
                    "eligible_wallet_count": 2,
                    "top_wallet_count": 2,
                    "short_retention_configured": false,
                    "covered_since": captured_at - chrono::Duration::days(2),
                    "covered_through_cursor": {
                        "ts_utc": captured_at,
                        "slot": 1,
                        "signature": "sig"
                    },
                    "covered_through_lag_seconds": 30,
                    "tail_fresh_within_runtime_lag": true,
                    "runtime_freshness_lag_seconds": 600,
                    "total_observed_swaps_rows": 10
                },
                "rotation": {
                    "signal_available": true,
                    "reason": null,
                    "cycles_requested": 3,
                    "cycles_completed": 3,
                    "sample_interval_seconds": 600,
                    "overlap_with_previous_cycle": 2,
                    "entered_since_previous_cycle": [],
                    "left_since_previous_cycle": [],
                    "stable_wallets_across_cycles": ["wallet-alpha", "wallet-beta"],
                    "unique_wallet_count_across_cycles": 2,
                    "samples": []
                }
            })
            .to_string(),
            shadow_signal_json: serde_json::json!({
                "recent_window_start": captured_at - chrono::Duration::minutes(30),
                "recent_window_end": captured_at,
                "selected_wallet_ids": ["wallet-alpha", "wallet-beta"],
                "selected_wallet_count": 2,
                "selected_wallets_with_recent_raw_activity": 1,
                "selected_wallets_with_recent_shadow_signal": 1,
                "recent_raw_swap_count": 3,
                "recent_shadow_signal_count": 1,
                "recent_raw_activity_wallet_ids": ["wallet-alpha"],
                "recent_shadow_signal_wallet_ids": ["wallet-alpha"],
                "recent_raw_activity_by_wallet": [{
                    "wallet_id": "wallet-alpha",
                    "row_count": 3,
                    "latest_ts": captured_at
                }],
                "recent_shadow_signal_by_wallet": [{
                    "wallet_id": "wallet-alpha",
                    "row_count": 1,
                    "latest_ts": captured_at
                }],
                "raw_activity_top_wallet_share": 1.0,
                "shadow_signal_top_wallet_share": 1.0,
                "raw_activity_broadly_distributed": false,
                "shadow_signal_broadly_distributed": false,
                "verdict": "shadow_signals_present_but_concentrated",
                "reason": "seed-shadow"
            })
            .to_string(),
        })?;
        Ok(())
    }

    fn ts(raw: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(raw)
            .expect("timestamp")
            .with_timezone(&Utc)
    }
}
