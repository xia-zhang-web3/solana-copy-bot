use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::SwapEvent;
use copybot_storage::{
    DiscoveryPublicationStateUpdate, DiscoveryRuntimeCursor, DiscoveryRuntimeMode, SqliteStore,
    WalletMetricRow, WalletUpsertRow,
};
use serde::Serialize;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

const USAGE: &str =
    "usage: discovery_restore_demo_fixture --output-dir <path> [--now <rfc3339>] [--json]";

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
    output_dir: PathBuf,
    now: DateTime<Utc>,
    json: bool,
}

#[derive(Debug, Clone, Serialize)]
struct FixtureOutput {
    event: String,
    output_dir: String,
    config_path: String,
    runtime_db_path: String,
    journal_db_path: String,
}

fn parse_args() -> Result<Option<Config>> {
    parse_args_from(env::args().skip(1))
}

fn parse_args_from<I>(args: I) -> Result<Option<Config>>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut output_dir: Option<PathBuf> = None;
    let mut now: Option<DateTime<Utc>> = None;
    let mut json = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--output-dir" => {
                output_dir = Some(PathBuf::from(parse_string_arg(
                    "--output-dir",
                    args.next(),
                )?))
            }
            "--now" => now = Some(parse_ts_arg("--now", args.next())?),
            "--json" => json = true,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    Ok(Some(Config {
        output_dir: output_dir.ok_or_else(|| anyhow!("missing required --output-dir"))?,
        now: now.unwrap_or_else(Utc::now),
        json,
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
    fs::create_dir_all(&config.output_dir)
        .with_context(|| format!("failed creating {}", config.output_dir.display()))?;
    let runtime_db_path = config.output_dir.join("source_runtime.db");
    let journal_db_path = config.output_dir.join("source_recent_raw.db");
    let config_path = config.output_dir.join("drill.toml");
    let migration_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

    fs::write(
        &config_path,
        format!(
            "[sqlite]\npath = \"{}\"\n\n[recent_raw_journal]\npath = \"{}\"\nretention_safety_buffer_days = 2\nwriter_queue_capacity_batches = 64\nreplay_batch_size = 1024\n\n[runtime_restore_ops]\nartifact_dir = \"state/discovery_restore/artifacts\"\nartifact_retention = 16\nartifact_cadence_minutes = 10\njournal_snapshot_dir = \"state/discovery_restore/recent_raw\"\njournal_snapshot_retention = 16\njournal_snapshot_cadence_minutes = 10\ndrill_workspace_dir = \"state/discovery_restore/drills\"\n\n[discovery]\nscoring_window_days = 5\nrefresh_seconds = 600\nmetric_snapshot_interval_seconds = 1800\nmax_window_swaps_in_memory = 32\nmax_fetch_swaps_per_cycle = 32\nmax_fetch_pages_per_cycle = 5\nfetch_time_budget_ms = 1000\nobserved_swaps_retention_days = 14\n\n[execution]\nenabled = false\n",
            runtime_db_path.display(),
            journal_db_path.display()
        ),
    )
    .with_context(|| format!("failed writing {}", config_path.display()))?;
    let policy_fingerprint = publication_selection_policy_fingerprint(&config_path)?;

    let mut runtime_store = SqliteStore::open(&runtime_db_path)?;
    runtime_store.run_migrations(&migration_dir)?;
    seed_runtime_source(&runtime_store, config.now, &policy_fingerprint)?;

    let journal_store = SqliteStore::open(&journal_db_path)?;
    seed_recent_raw_journal(&journal_store, config.now)?;

    let output = FixtureOutput {
        event: "discovery_restore_demo_fixture".to_string(),
        output_dir: config.output_dir.display().to_string(),
        config_path: config_path.display().to_string(),
        runtime_db_path: runtime_db_path.display().to_string(),
        journal_db_path: journal_db_path.display().to_string(),
    };
    if config.json {
        serde_json::to_string_pretty(&output).context("failed serializing fixture output json")
    } else {
        Ok([
            format!("event={}", output.event),
            format!("output_dir={}", output.output_dir),
            format!("config_path={}", output.config_path),
            format!("runtime_db_path={}", output.runtime_db_path),
            format!("journal_db_path={}", output.journal_db_path),
        ]
        .join("\n"))
    }
}

fn seed_runtime_source(
    store: &SqliteStore,
    now: DateTime<Utc>,
    policy_fingerprint: &str,
) -> Result<()> {
    let published_window_start = metrics_window_start(now, 5, 1800);
    store.persist_discovery_cycle(
        &[WalletUpsertRow {
            wallet_id: "wallet-restore".to_string(),
            first_seen: published_window_start - Duration::days(1),
            last_seen: now - Duration::minutes(1),
            status: "candidate".to_string(),
        }],
        &[WalletMetricRow {
            wallet_id: "wallet-restore".to_string(),
            window_start: published_window_start,
            pnl: 3.2,
            win_rate: 0.8,
            trades: 10,
            closed_trades: 10,
            hold_median_seconds: 120,
            score: 0.9,
            buy_total: 10,
            tradable_ratio: 1.0,
            rug_ratio: 0.0,
        }],
        &["wallet-restore".to_string()],
        true,
        true,
        now - Duration::minutes(2),
        "discovery_restore_demo_fixture",
    )?;
    store.set_discovery_publication_state_with_options(
        &DiscoveryPublicationStateUpdate {
            runtime_mode: DiscoveryRuntimeMode::Healthy,
            reason: "raw_window".to_string(),
            last_published_at: Some(now - Duration::minutes(2)),
            last_published_window_start: Some(published_window_start),
            published_scoring_source: Some("raw_window".to_string()),
            published_wallet_ids: Some(vec!["wallet-restore".to_string()]),
        },
        false,
        Some(policy_fingerprint),
    )?;
    store.upsert_discovery_runtime_cursor(&DiscoveryRuntimeCursor {
        ts_utc: now - Duration::minutes(1),
        slot: 42,
        signature: "sig-restore".to_string(),
    })?;
    Ok(())
}

fn publication_selection_policy_fingerprint(config_path: &Path) -> Result<String> {
    let loaded_config = copybot_config::load_from_path(config_path)
        .with_context(|| format!("failed loading {}", config_path.display()))?;
    let config = loaded_config.discovery;
    Ok(format!(
        concat!(
            "follow_top_n={};",
            "scoring_window_days={};",
            "decay_window_days={};",
            "min_leader_notional_sol={:.6};",
            "min_trades={};",
            "min_active_days={};",
            "min_score={:.6};",
            "max_tx_per_minute={};",
            "min_buy_count={};",
            "min_tradable_ratio={:.6};",
            "require_open_positions_for_publication={};",
            "max_rug_ratio={:.6};",
            "rug_lookahead_seconds={};",
            "thin_market_min_volume_sol={:.6};",
            "thin_market_min_unique_traders={}"
        ),
        config.follow_top_n,
        config.scoring_window_days,
        config.decay_window_days,
        config.min_leader_notional_sol,
        config.min_trades,
        config.min_active_days,
        config.min_score,
        config.max_tx_per_minute,
        config.min_buy_count,
        config.min_tradable_ratio,
        config.require_open_positions_for_publication,
        config.max_rug_ratio,
        config.rug_lookahead_seconds,
        config.thin_market_min_volume_sol,
        config.thin_market_min_unique_traders,
    ))
}

fn seed_recent_raw_journal(store: &SqliteStore, now: DateTime<Utc>) -> Result<()> {
    store.insert_recent_raw_journal_batch(
        &[
            make_swap("sig-window-start", now - Duration::days(5), 40),
            make_swap("sig-restore", now - Duration::minutes(1), 42),
            make_swap("sig-post", now, 43),
        ],
        now,
    )?;
    Ok(())
}

fn make_swap(signature: &str, ts_utc: DateTime<Utc>, slot: u64) -> SwapEvent {
    SwapEvent {
        wallet: "wallet-restore".to_string(),
        dex: "raydium".to_string(),
        token_in: "So11111111111111111111111111111111111111112".to_string(),
        token_out: format!("token-{signature}"),
        amount_in: 1.0,
        amount_out: 10.0,
        signature: signature.to_string(),
        slot,
        ts_utc,
        exact_amounts: None,
    }
}

fn metrics_window_start(
    now: DateTime<Utc>,
    scoring_window_days: i64,
    metric_snapshot_interval_seconds: i64,
) -> DateTime<Utc> {
    let bucketed_ts = now
        .timestamp()
        .div_euclid(metric_snapshot_interval_seconds.max(1))
        * metric_snapshot_interval_seconds.max(1);
    let bucketed_now = DateTime::<Utc>::from_timestamp(bucketed_ts, 0).unwrap_or(now);
    bucketed_now - Duration::days(scoring_window_days.max(1))
}
