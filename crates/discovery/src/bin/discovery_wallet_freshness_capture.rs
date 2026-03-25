use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::load_from_path;
use copybot_discovery::{
    wallet_freshness_audit::{WalletFreshnessCaptureSnapshot, DEFAULT_RECENT_CYCLES},
    DiscoveryService,
};
use copybot_storage::SqliteStore;
use serde::Serialize;
use std::env;
use std::path::{Path, PathBuf};
use std::time::Instant;

const USAGE: &str = "usage: discovery_wallet_freshness_capture --config <path> [--db-path <path>] [--json] [--now <rfc3339>] [--recent-cycles <count>] [--shadow-evidence-lookback-seconds <seconds>]  # manual/debug Stage 3 capture";

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
    recent_cycles: usize,
    shadow_evidence_lookback_seconds: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
struct CaptureRunOutput {
    event: &'static str,
    mode: &'static str,
    status: &'static str,
    complete: bool,
    config_path: String,
    db_path: String,
    capture_duration_ms: u64,
    raw_truth_build_duration_ms: u64,
    shadow_signal_duration_ms: u64,
    persistence_duration_ms: u64,
    dominant_phase: String,
    shadow_evidence_lookback_seconds: Option<u64>,
    #[serde(flatten)]
    snapshot: WalletFreshnessCaptureSnapshot,
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
    let mut recent_cycles = DEFAULT_RECENT_CYCLES;
    let mut shadow_evidence_lookback_seconds: Option<u64> = None;

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
            "--recent-cycles" => {
                recent_cycles = parse_usize_arg("--recent-cycles", args.next())?;
            }
            "--shadow-evidence-lookback-seconds" => {
                shadow_evidence_lookback_seconds = Some(parse_u64_arg(
                    "--shadow-evidence-lookback-seconds",
                    args.next(),
                )?);
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
        recent_cycles: recent_cycles.max(1),
        shadow_evidence_lookback_seconds,
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
    let parsed = raw
        .parse::<u64>()
        .with_context(|| format!("invalid {flag} u64 value: {raw}"))?;
    if parsed == 0 {
        bail!("{flag} must be greater than zero");
    }
    Ok(parsed)
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
    let capture_started = Instant::now();
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
    let computed = discovery.wallet_freshness_capture_snapshot_measured_with_lookback(
        &store,
        config.now,
        config.recent_cycles,
        config.shadow_evidence_lookback_seconds,
    )?;
    let persist_started = Instant::now();
    let persisted =
        store.append_discovery_wallet_freshness_capture(&computed.snapshot.to_storage_write()?)?;
    let persistence_duration_ms = persist_started.elapsed().as_millis() as u64;
    let persisted_snapshot =
        copybot_discovery::wallet_freshness_audit::wallet_freshness_capture_from_row(persisted)?;
    let output = CaptureRunOutput {
        event: "discovery_wallet_freshness_capture",
        mode: "manual_debug",
        status: "completed",
        complete: true,
        config_path: config.config_path.display().to_string(),
        db_path: db_path.display().to_string(),
        capture_duration_ms: capture_started.elapsed().as_millis() as u64,
        raw_truth_build_duration_ms: computed.raw_truth_build_duration_ms,
        shadow_signal_duration_ms: computed.shadow_signal_duration_ms,
        persistence_duration_ms,
        dominant_phase: dominant_phase(
            computed.raw_truth_build_duration_ms,
            computed.shadow_signal_duration_ms,
            persistence_duration_ms,
        )
        .to_string(),
        shadow_evidence_lookback_seconds: persisted_snapshot
            .shadow_signal
            .evidence_lookback_seconds,
        snapshot: persisted_snapshot,
    };
    if config.json {
        serde_json::to_string_pretty(&output)
            .context("failed serializing discovery wallet freshness capture json")
    } else {
        Ok(render_human(&output))
    }
}

fn dominant_phase(
    raw_truth_build_duration_ms: u64,
    shadow_signal_duration_ms: u64,
    persistence_duration_ms: u64,
) -> &'static str {
    if raw_truth_build_duration_ms >= shadow_signal_duration_ms
        && raw_truth_build_duration_ms >= persistence_duration_ms
    {
        "raw_truth_build"
    } else if shadow_signal_duration_ms >= persistence_duration_ms {
        "shadow_signal_evidence"
    } else {
        "persistence"
    }
}

fn render_human(output: &CaptureRunOutput) -> String {
    [
        format!("event={}", output.event),
        format!("mode={}", output.mode),
        format!("status={}", output.status),
        format!("complete={}", output.complete),
        format!("config_path={}", output.config_path),
        format!("db_path={}", output.db_path),
        format!("capture_duration_ms={}", output.capture_duration_ms),
        format!(
            "raw_truth_build_duration_ms={}",
            output.raw_truth_build_duration_ms
        ),
        format!(
            "shadow_signal_duration_ms={}",
            output.shadow_signal_duration_ms
        ),
        format!("persistence_duration_ms={}", output.persistence_duration_ms),
        format!("dominant_phase={}", output.dominant_phase),
        format!(
            "shadow_evidence_lookback_seconds={}",
            output
                .shadow_evidence_lookback_seconds
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "capture_id={}",
            output
                .snapshot
                .capture_id
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!("captured_at={}", output.snapshot.captured_at.to_rfc3339()),
        format!("recent_cycles={}", output.snapshot.recent_cycles),
        format!("audit_verdict={}", output.snapshot.audit.verdict.as_str()),
        format!("audit_reason={}", output.snapshot.audit.reason),
        format!(
            "shadow_signal_verdict={}",
            output.snapshot.shadow_signal.verdict.as_str()
        ),
        format!(
            "shadow_signal_reason={}",
            output.snapshot.shadow_signal.reason
        ),
        format!(
            "published_wallet_count={}",
            output.snapshot.audit.published_wallet_ids.len()
        ),
        format!(
            "active_follow_wallet_count={}",
            output.snapshot.audit.active_follow_wallet_ids.len()
        ),
        format!(
            "current_raw_top_wallet_count={}",
            output.snapshot.audit.current_raw_top_wallet_ids.len()
        ),
        format!(
            "selected_wallets_with_recent_raw_activity={}",
            output
                .snapshot
                .shadow_signal
                .selected_wallets_with_recent_raw_activity
        ),
        format!(
            "selected_wallets_with_recent_shadow_signal={}",
            output
                .snapshot
                .shadow_signal
                .selected_wallets_with_recent_shadow_signal
        ),
        format!(
            "recent_shadow_signal_count={}",
            output.snapshot.shadow_signal.recent_shadow_signal_count
        ),
    ]
    .join("\n")
}

#[cfg(test)]
mod tests {
    use super::{parse_args_from, run, Config};
    use anyhow::{Context, Result};
    use chrono::{DateTime, Duration, Utc};
    use copybot_core_types::{CopySignalRow, SwapEvent, COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE};
    use copybot_storage::{DiscoveryPublicationStateUpdate, DiscoveryRuntimeMode, SqliteStore};
    use std::fs;
    use std::path::Path;
    use tempfile::tempdir;

    #[test]
    fn parse_args_reads_recent_cycles() -> Result<()> {
        let config = parse_args_from([
            "--config".to_string(),
            "/tmp/live.toml".to_string(),
            "--recent-cycles".to_string(),
            "5".to_string(),
            "--shadow-evidence-lookback-seconds".to_string(),
            "960".to_string(),
        ])?
        .expect("config should parse");
        assert_eq!(config.recent_cycles, 5);
        assert_eq!(config.shadow_evidence_lookback_seconds, Some(960));
        Ok(())
    }

    #[test]
    fn run_persists_json_wallet_freshness_capture() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("wallet-freshness-capture-bin.db");
        let config_path = temp.path().join("wallet-freshness-capture.toml");
        let store = open_store(&db_path)?;
        let now = ts("2026-03-25T12:00:00Z");
        seed_ranked_wallet_window(&store, now)?;
        seed_publication_truth(&store, now)?;
        store.activate_follow_wallet("wallet-alpha", now, "test-follow")?;
        store.activate_follow_wallet("wallet-beta", now, "test-follow")?;
        store.insert_copy_signal(&CopySignalRow {
            signal_id: "shadow:sig-alpha".to_string(),
            wallet_id: "wallet-alpha".to_string(),
            side: "buy".to_string(),
            token: "mint-a".to_string(),
            notional_sol: 0.25,
            notional_lamports: None,
            notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
            ts: now - Duration::seconds(30),
            status: "shadow_recorded".to_string(),
        })?;
        fs::write(
            &config_path,
            format!(
                "[sqlite]\npath = \"{}\"\n\n[discovery]\nscoring_window_days = 5\nrefresh_seconds = 600\nmetric_snapshot_interval_seconds = 1800\nmax_window_swaps_in_memory = 128\nmax_fetch_swaps_per_cycle = 128\nmax_fetch_pages_per_cycle = 8\nfetch_time_budget_ms = 1000\nobserved_swaps_retention_days = 14\nfollow_top_n = 20\nmin_score = 0.0\nmin_trades = 1\nmin_active_days = 1\nmin_leader_notional_sol = 0.0\nmin_buy_count = 1\nmin_tradable_ratio = 0.0\nmax_rug_ratio = 1.0\nthin_market_min_unique_traders = 1\n\n[execution]\nenabled = false\n",
                db_path.display()
            ),
        )?;

        let output = run(Config {
            config_path,
            db_path: None,
            json: true,
            now,
            recent_cycles: 3,
            shadow_evidence_lookback_seconds: Some(1_800),
        })?;
        let json: serde_json::Value =
            serde_json::from_str(&output).context("json output must parse")?;
        assert_eq!(json["status"], "completed");
        assert_eq!(json["mode"], "manual_debug");
        assert_eq!(json["complete"], true);
        assert_eq!(json["audit"]["verdict"], "fresh_current");
        assert_eq!(
            json["shadow_signal"]["verdict"],
            "shadow_signals_present_but_concentrated"
        );
        assert_eq!(json["capture_id"], 1);
        assert!(json["capture_duration_ms"].is_number());
        assert!(json["raw_truth_build_duration_ms"].is_number());
        assert!(json["shadow_signal_duration_ms"].is_number());
        assert!(json["persistence_duration_ms"].is_number());
        assert!(json["dominant_phase"].is_string());
        assert_eq!(json["shadow_evidence_lookback_seconds"], 1800);
        assert_eq!(json["shadow_signal"]["evidence_lookback_seconds"], 1800);
        Ok(())
    }

    fn open_store(path: &Path) -> Result<SqliteStore> {
        let mut store = SqliteStore::open(path)?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;
        Ok(store)
    }

    fn seed_ranked_wallet_window(store: &SqliteStore, now: DateTime<Utc>) -> Result<()> {
        let coverage_start = now - Duration::days(5);
        for (wallet_idx, (wallet_id, mint, trades, offset_minutes)) in [
            ("wallet-alpha", "mint-a", 3usize, 0i64),
            ("wallet-beta", "mint-b", 2usize, 10i64),
        ]
        .into_iter()
        .enumerate()
        {
            store.insert_observed_swap(&SwapEvent {
                signature: format!("{wallet_id}-head"),
                wallet: wallet_id.to_string(),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: mint.to_string(),
                amount_in: 1.0,
                amount_out: 10.0,
                slot: 10000 + wallet_idx as u64,
                ts_utc: coverage_start + Duration::minutes(wallet_idx as i64),
                exact_amounts: None,
            })?;
            for trade_idx in 1..trades {
                store.insert_observed_swap(&SwapEvent {
                    signature: format!("{wallet_id}-{trade_idx}"),
                    wallet: wallet_id.to_string(),
                    dex: "raydium".to_string(),
                    token_in: "So11111111111111111111111111111111111111112".to_string(),
                    token_out: mint.to_string(),
                    amount_in: 1.0,
                    amount_out: 10.0,
                    slot: 10100 + wallet_idx as u64 * 100 + trade_idx as u64,
                    ts_utc: now - Duration::minutes(offset_minutes + trade_idx as i64),
                    exact_amounts: None,
                })?;
            }
        }
        Ok(())
    }

    fn seed_publication_truth(store: &SqliteStore, now: DateTime<Utc>) -> Result<()> {
        store.set_discovery_publication_state(&DiscoveryPublicationStateUpdate {
            runtime_mode: DiscoveryRuntimeMode::Healthy,
            reason: "test-publication".to_string(),
            last_published_at: Some(now - Duration::seconds(60)),
            last_published_window_start: Some(now - Duration::days(5)),
            published_scoring_source: Some("raw_window_persisted_stream".to_string()),
            published_wallet_ids: Some(vec!["wallet-alpha".to_string(), "wallet-beta".to_string()]),
        })
    }

    fn ts(raw: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(raw)
            .expect("timestamp")
            .with_timezone(&Utc)
    }
}
