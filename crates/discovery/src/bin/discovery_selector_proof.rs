use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::load_from_path;
use copybot_discovery::{
    DiscoverySelectorProofReport, DiscoverySelectorProofRequest, DiscoveryService,
};
use copybot_storage::SqliteStore;
use std::env;
use std::fs;
use std::path::PathBuf;

const USAGE: &str =
    "usage: discovery_selector_proof --db <path> --config <path> --now <rfc3339> [--json]";

fn main() -> Result<()> {
    let Some(config) = parse_args()? else {
        println!("{USAGE}");
        return Ok(());
    };
    let report = run(&config)?;
    println!("{}", render_output(&report, config.json)?);
    Ok(())
}

#[derive(Debug, Clone)]
struct Config {
    db_path: PathBuf,
    config_path: PathBuf,
    now: DateTime<Utc>,
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
    let mut db_path: Option<PathBuf> = None;
    let mut config_path: Option<PathBuf> = None;
    let mut now: Option<DateTime<Utc>> = None;
    let mut json = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--db" => db_path = Some(PathBuf::from(parse_string_arg("--db", args.next())?)),
            "--config" => {
                config_path = Some(PathBuf::from(parse_string_arg("--config", args.next())?))
            }
            "--now" => now = Some(parse_ts_arg("--now", args.next())?),
            "--json" => json = true,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    Ok(Some(Config {
        db_path: db_path.ok_or_else(|| anyhow!("missing required --db"))?,
        config_path: config_path.ok_or_else(|| anyhow!("missing required --config"))?,
        now: now.ok_or_else(|| anyhow!("missing required --now"))?,
        json,
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

fn run(config: &Config) -> Result<DiscoverySelectorProofReport> {
    let loaded_config = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let config_bytes = fs::read(&config.config_path)
        .with_context(|| format!("failed reading config {}", config.config_path.display()))?;
    let store = SqliteStore::open_read_only_immutable(&config.db_path)
        .with_context(|| format!("failed opening sqlite db {}", config.db_path.display()))?;
    let discovery = DiscoveryService::new(
        loaded_config.discovery.clone(),
        loaded_config.shadow.clone(),
    );

    discovery.selector_proof_report(
        &store,
        &DiscoverySelectorProofRequest {
            db_path: config.db_path.clone(),
            config_path: config.config_path.clone(),
            config_bytes,
            fixed_now_utc: config.now,
        },
    )
}

fn format_optional_ts(value: Option<DateTime<Utc>>) -> String {
    value
        .map(|ts| ts.to_rfc3339())
        .unwrap_or_else(|| "null".to_string())
}

fn format_optional_u32(value: Option<u32>) -> String {
    value
        .map(|count| count.to_string())
        .unwrap_or_else(|| "null".to_string())
}

fn render_output(report: &DiscoverySelectorProofReport, json: bool) -> Result<String> {
    if json {
        return serde_json::to_string_pretty(report)
            .context("failed serializing selector proof json");
    }

    Ok(format!(
        concat!(
            "event=discovery_selector_proof\n",
            "db_path={db_path}\n",
            "db_file_size_bytes={db_file_size_bytes}\n",
            "db_file_mtime_utc={db_file_mtime_utc}\n",
            "db_page_size={db_page_size}\n",
            "db_page_count={db_page_count}\n",
            "db_read_only_open_confirmed={db_read_only_open_confirmed}\n",
            "config_path={config_path}\n",
            "config_fingerprint_method={config_fingerprint_method}\n",
            "config_fingerprint={config_fingerprint}\n",
            "fixed_now_utc={fixed_now_utc}\n",
            "rpc_enabled={rpc_enabled}\n",
            "metrics_window_start_utc={metrics_window_start_utc}\n",
            "observed_swaps_loaded={observed_swaps_loaded}\n",
            "wallets_seen={wallets_seen}\n",
            "eligible_wallet_count={eligible_wallet_count}\n",
            "observed_swaps_window_min_ts_utc={observed_swaps_window_min_ts_utc}\n",
            "observed_swaps_window_max_ts_utc={observed_swaps_window_max_ts_utc}\n",
            "observed_swaps_buy_count={observed_swaps_buy_count}\n",
            "observed_swaps_sell_count={observed_swaps_sell_count}\n",
            "observed_swaps_distinct_wallet_count={observed_swaps_distinct_wallet_count}\n",
            "observed_swaps_distinct_buy_mint_count={observed_swaps_distinct_buy_mint_count}\n",
            "wallet_activity_days_window_min_day_utc={wallet_activity_days_window_min_day_utc}\n",
            "wallet_activity_days_window_max_day_utc={wallet_activity_days_window_max_day_utc}\n",
            "wallet_activity_days_rows_for_seen_wallets={wallet_activity_days_rows_for_seen_wallets}\n",
            "wallet_activity_days_distinct_wallets_for_seen_wallets={wallet_activity_days_distinct_wallets_for_seen_wallets}\n",
            "wallet_activity_days_min_active_days_among_seen_wallets={wallet_activity_days_min_active_days_among_seen_wallets}\n",
            "wallet_activity_days_max_active_days_among_seen_wallets={wallet_activity_days_max_active_days_among_seen_wallets}\n",
            "token_quality_cache_rows_for_seen_buy_mints={token_quality_cache_rows_for_seen_buy_mints}\n",
            "token_quality_cache_distinct_mints_for_seen_buy_mints={token_quality_cache_distinct_mints_for_seen_buy_mints}\n",
            "token_quality_cache_fresh_rows_for_seen_buy_mints={token_quality_cache_fresh_rows_for_seen_buy_mints}\n",
            "token_quality_cache_stale_rows_for_seen_buy_mints={token_quality_cache_stale_rows_for_seen_buy_mints}\n",
            "token_quality_cache_missing_rows_for_seen_buy_mints={token_quality_cache_missing_rows_for_seen_buy_mints}\n",
            "token_quality_cache_min_fetched_at_utc_for_seen_buy_mints={token_quality_cache_min_fetched_at_utc_for_seen_buy_mints}\n",
            "token_quality_cache_max_fetched_at_utc_for_seen_buy_mints={token_quality_cache_max_fetched_at_utc_for_seen_buy_mints}\n",
            "ranked_wallets={ranked_wallets:?}\n",
            "rejected_wallet_sample_limit={rejected_wallet_sample_limit}\n",
            "rejected_wallet_samples={rejected_wallet_samples:?}\n",
            "reject_breakdown={reject_breakdown:?}\n",
            "token_quality_coverage={token_quality_coverage:?}\n",
            "selector_universe_explanation={selector_universe_explanation}"
        ),
        db_path = report.db_path.as_str(),
        db_file_size_bytes = report.db_file_size_bytes,
        db_file_mtime_utc = report.db_file_mtime_utc.to_rfc3339(),
        db_page_size = report.db_page_size,
        db_page_count = report.db_page_count,
        db_read_only_open_confirmed = report.db_read_only_open_confirmed,
        config_path = report.config_path.as_str(),
        config_fingerprint_method = report.config_fingerprint_method.as_str(),
        config_fingerprint = report.config_fingerprint.as_str(),
        fixed_now_utc = report.fixed_now_utc.to_rfc3339(),
        rpc_enabled = report.rpc_enabled,
        metrics_window_start_utc = report
            .metrics_window_start_utc
            .map(|ts| ts.to_rfc3339())
            .unwrap_or_else(|| "null".to_string()),
        observed_swaps_loaded = report
            .observed_swaps_loaded
            .map(|count| count.to_string())
            .unwrap_or_else(|| "null".to_string()),
        wallets_seen = report
            .wallets_seen
            .map(|count| count.to_string())
            .unwrap_or_else(|| "null".to_string()),
        eligible_wallet_count = report
            .eligible_wallet_count
            .map(|count| count.to_string())
            .unwrap_or_else(|| "null".to_string()),
        observed_swaps_window_min_ts_utc = format_optional_ts(report.observed_swaps_window_min_ts_utc),
        observed_swaps_window_max_ts_utc = format_optional_ts(report.observed_swaps_window_max_ts_utc),
        observed_swaps_buy_count = report.observed_swaps_buy_count,
        observed_swaps_sell_count = report.observed_swaps_sell_count,
        observed_swaps_distinct_wallet_count = report.observed_swaps_distinct_wallet_count,
        observed_swaps_distinct_buy_mint_count = report.observed_swaps_distinct_buy_mint_count,
        wallet_activity_days_window_min_day_utc =
            format_optional_ts(report.wallet_activity_days_window_min_day_utc),
        wallet_activity_days_window_max_day_utc =
            format_optional_ts(report.wallet_activity_days_window_max_day_utc),
        wallet_activity_days_rows_for_seen_wallets = report.wallet_activity_days_rows_for_seen_wallets,
        wallet_activity_days_distinct_wallets_for_seen_wallets =
            report.wallet_activity_days_distinct_wallets_for_seen_wallets,
        wallet_activity_days_min_active_days_among_seen_wallets =
            format_optional_u32(report.wallet_activity_days_min_active_days_among_seen_wallets),
        wallet_activity_days_max_active_days_among_seen_wallets =
            format_optional_u32(report.wallet_activity_days_max_active_days_among_seen_wallets),
        token_quality_cache_rows_for_seen_buy_mints =
            report.token_quality_cache_rows_for_seen_buy_mints,
        token_quality_cache_distinct_mints_for_seen_buy_mints =
            report.token_quality_cache_distinct_mints_for_seen_buy_mints,
        token_quality_cache_fresh_rows_for_seen_buy_mints =
            report.token_quality_cache_fresh_rows_for_seen_buy_mints,
        token_quality_cache_stale_rows_for_seen_buy_mints =
            report.token_quality_cache_stale_rows_for_seen_buy_mints,
        token_quality_cache_missing_rows_for_seen_buy_mints =
            report.token_quality_cache_missing_rows_for_seen_buy_mints,
        token_quality_cache_min_fetched_at_utc_for_seen_buy_mints =
            format_optional_ts(report.token_quality_cache_min_fetched_at_utc_for_seen_buy_mints),
        token_quality_cache_max_fetched_at_utc_for_seen_buy_mints =
            format_optional_ts(report.token_quality_cache_max_fetched_at_utc_for_seen_buy_mints),
        ranked_wallets = &report.ranked_wallets,
        rejected_wallet_sample_limit = report.rejected_wallet_sample_limit,
        rejected_wallet_samples = &report.rejected_wallet_samples,
        reject_breakdown = &report.reject_breakdown,
        token_quality_coverage = &report.token_quality_coverage,
        selector_universe_explanation = report.selector_universe_explanation.as_str(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use copybot_core_types::SwapEvent;
    use copybot_discovery::DISCOVERY_SELECTOR_PROOF_CONFIG_FINGERPRINT_METHOD;
    use copybot_storage::{SqliteStore, WalletActivityDayRow};
    use serde_json::Value;
    use std::path::Path;
    use tempfile::TempDir;

    const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

    #[test]
    fn parse_args_reads_required_selector_proof_flags() -> Result<()> {
        let parsed = parse_args_from([
            "--db".to_string(),
            "/tmp/example.db".to_string(),
            "--config".to_string(),
            "/tmp/example.toml".to_string(),
            "--now".to_string(),
            "2026-04-21T16:07:06Z".to_string(),
            "--json".to_string(),
        ])?
        .expect("config should parse");

        assert_eq!(parsed.db_path, PathBuf::from("/tmp/example.db"));
        assert_eq!(parsed.config_path, PathBuf::from("/tmp/example.toml"));
        assert_eq!(parsed.now.to_rfc3339(), "2026-04-21T16:07:06+00:00");
        assert!(parsed.json);
        Ok(())
    }

    #[test]
    fn proof_report_populates_batch2_selector_fields_on_synthetic_fixture() -> Result<()> {
        let fixture = TestFixture::new(FixtureSeed::Ranked)?;
        let report = run(&fixture.config(false))?;

        assert_eq!(report.db_path, fixture.db_path.display().to_string());
        assert_eq!(
            report.config_path,
            fixture.config_path.display().to_string()
        );
        assert_eq!(
            report.config_fingerprint_method,
            DISCOVERY_SELECTOR_PROOF_CONFIG_FINGERPRINT_METHOD
        );
        assert_eq!(report.fixed_now_utc, fixture.now);
        assert!(!report.rpc_enabled);
        assert!(report.db_file_size_bytes > 0);
        assert!(report.db_page_size > 0);
        assert!(report.db_page_count > 0);
        assert!(report.db_read_only_open_confirmed);
        assert_eq!(
            report.metrics_window_start_utc.map(|ts| ts.to_rfc3339()),
            Some("2026-04-16T16:00:00+00:00".to_string())
        );
        assert_eq!(report.observed_swaps_loaded, Some(4));
        assert_eq!(report.wallets_seen, Some(2));
        assert_eq!(report.eligible_wallet_count, Some(2));
        assert_eq!(
            report
                .observed_swaps_window_min_ts_utc
                .map(|ts| ts.to_rfc3339()),
            Some("2026-04-21T13:07:06+00:00".to_string())
        );
        assert_eq!(
            report
                .observed_swaps_window_max_ts_utc
                .map(|ts| ts.to_rfc3339()),
            Some("2026-04-21T15:37:06+00:00".to_string())
        );
        assert_eq!(report.observed_swaps_buy_count, 2);
        assert_eq!(report.observed_swaps_sell_count, 2);
        assert_eq!(report.observed_swaps_distinct_wallet_count, 2);
        assert_eq!(report.observed_swaps_distinct_buy_mint_count, 2);
        assert_eq!(
            report
                .wallet_activity_days_window_min_day_utc
                .map(|ts| ts.to_rfc3339()),
            Some("2026-04-21T00:00:00+00:00".to_string())
        );
        assert_eq!(
            report
                .wallet_activity_days_window_max_day_utc
                .map(|ts| ts.to_rfc3339()),
            Some("2026-04-21T00:00:00+00:00".to_string())
        );
        assert_eq!(report.wallet_activity_days_rows_for_seen_wallets, 2);
        assert_eq!(
            report.wallet_activity_days_distinct_wallets_for_seen_wallets,
            2
        );
        assert_eq!(
            report.wallet_activity_days_min_active_days_among_seen_wallets,
            Some(1)
        );
        assert_eq!(
            report.wallet_activity_days_max_active_days_among_seen_wallets,
            Some(1)
        );
        assert_eq!(report.token_quality_cache_rows_for_seen_buy_mints, 2);
        assert_eq!(
            report.token_quality_cache_distinct_mints_for_seen_buy_mints,
            2
        );
        assert_eq!(report.token_quality_cache_fresh_rows_for_seen_buy_mints, 2);
        assert_eq!(report.token_quality_cache_stale_rows_for_seen_buy_mints, 0);
        assert_eq!(
            report.token_quality_cache_missing_rows_for_seen_buy_mints,
            0
        );
        assert_eq!(report.rejected_wallet_sample_limit, 10);
        assert!(report.rejected_wallet_samples.is_empty());
        Ok(())
    }

    #[test]
    fn ranked_fixture_yields_non_empty_ranked_wallets_with_deterministic_ordering() -> Result<()> {
        let fixture = TestFixture::new(FixtureSeed::Ranked)?;
        let first = run(&fixture.config(true))?;
        let second = run(&fixture.config(true))?;

        assert!(
            !first.ranked_wallets.is_empty(),
            "ranked fixture must yield top wallets"
        );
        assert_eq!(
            first.ranked_wallets,
            vec!["wallet-alpha".to_string(), "wallet-beta".to_string()]
        );
        assert_eq!(
            first.selector_universe_explanation,
            "non_empty_ranked_universe"
        );
        assert_eq!(first.ranked_wallets, second.ranked_wallets);
        assert_eq!(first, second);
        assert_eq!(
            render_output(&first, true)?,
            render_output(&second, true)?,
            "json output must remain stable for the same db/config/now inputs"
        );
        Ok(())
    }

    #[test]
    fn zero_swap_fixture_reports_empty_due_to_no_observed_swaps_in_window() -> Result<()> {
        let fixture = TestFixture::new(FixtureSeed::NoSwaps)?;
        let report = run(&fixture.config(false))?;

        assert_eq!(report.observed_swaps_loaded, Some(0));
        assert_eq!(report.wallets_seen, Some(0));
        assert_eq!(
            report.selector_universe_explanation,
            "empty_due_to_no_observed_swaps_in_window"
        );
        assert!(report.rejected_wallet_samples.is_empty());
        Ok(())
    }

    #[test]
    fn gated_empty_fixture_reports_rejected_samples_and_gate_explanation() -> Result<()> {
        let fixture = TestFixture::new(FixtureSeed::GatedEmpty)?;
        let report = run(&fixture.config(false))?;

        assert_eq!(report.wallets_seen, Some(1));
        assert_eq!(report.eligible_wallet_count, Some(0));
        assert_eq!(
            report.selector_universe_explanation,
            "empty_due_to_selector_gates_on_seen_wallets"
        );
        assert!(!report.rejected_wallet_samples.is_empty());
        assert_eq!(
            report.rejected_wallet_samples[0].wallet_id,
            "wallet-gated".to_string()
        );
        Ok(())
    }

    #[test]
    fn persisted_activity_day_coverage_takes_precedence_over_swap_derived_days() -> Result<()> {
        let fixture = TestFixture::new(FixtureSeed::PersistedActivityCoverageDiff)?;
        let report = run(&fixture.config(false))?;

        assert_eq!(
            report
                .wallet_activity_days_window_min_day_utc
                .map(|ts| ts.to_rfc3339()),
            Some("2026-04-19T00:00:00+00:00".to_string())
        );
        assert_eq!(
            report
                .wallet_activity_days_window_max_day_utc
                .map(|ts| ts.to_rfc3339()),
            Some("2026-04-21T00:00:00+00:00".to_string())
        );
        assert_eq!(report.wallet_activity_days_rows_for_seen_wallets, 3);
        assert_eq!(
            report.wallet_activity_days_distinct_wallets_for_seen_wallets,
            2
        );
        assert_eq!(
            report.wallet_activity_days_min_active_days_among_seen_wallets,
            Some(1)
        );
        assert_eq!(
            report.wallet_activity_days_max_active_days_among_seen_wallets,
            Some(2)
        );
        assert_eq!(
            report
                .observed_swaps_window_min_ts_utc
                .map(|ts| ts.to_rfc3339()),
            Some("2026-04-21T13:07:06+00:00".to_string()),
            "swap coverage must remain tied to the swap window, not the persisted day backfill"
        );
        Ok(())
    }

    #[test]
    fn proof_path_confirms_read_only_open() -> Result<()> {
        let fixture = TestFixture::new(FixtureSeed::Ranked)?;
        let report = run(&fixture.config(false))?;

        assert!(report.db_read_only_open_confirmed);
        Ok(())
    }

    #[test]
    fn quality_gap_fixture_reports_honest_token_quality_coverage_and_reject_breakdown() -> Result<()>
    {
        let fixture = TestFixture::new(FixtureSeed::QualityGaps)?;
        let report = run(&fixture.config(false))?;

        assert_eq!(
            report.token_quality_coverage.get("buy_mints_total"),
            Some(&3),
            "coverage must count exact distinct buy mints"
        );
        assert_eq!(
            report.token_quality_coverage.get("fresh_cache_hits"),
            Some(&1)
        );
        assert_eq!(
            report.token_quality_coverage.get("stale_cache_hits"),
            Some(&1)
        );
        assert_eq!(
            report.token_quality_coverage.get("missing_cache_entries"),
            Some(&1)
        );
        assert_eq!(
            report.reject_breakdown.get("low_tradable_ratio"),
            Some(&2),
            "missing and stale quality wallets should fail tradable ratio"
        );
        assert_eq!(
            report.reject_breakdown.get("missing_token_quality"),
            Some(&1)
        );
        assert_eq!(report.reject_breakdown.get("stale_token_quality"), Some(&1));
        assert_eq!(report.token_quality_cache_rows_for_seen_buy_mints, 2);
        assert_eq!(
            report.token_quality_cache_distinct_mints_for_seen_buy_mints,
            2
        );
        assert_eq!(report.token_quality_cache_fresh_rows_for_seen_buy_mints, 1);
        assert_eq!(report.token_quality_cache_stale_rows_for_seen_buy_mints, 1);
        assert_eq!(
            report.token_quality_cache_missing_rows_for_seen_buy_mints,
            1
        );
        assert_eq!(
            report
                .token_quality_cache_min_fetched_at_utc_for_seen_buy_mints
                .map(|ts| ts.to_rfc3339()),
            Some("2026-04-21T15:07:06+00:00".to_string())
        );
        assert_eq!(
            report
                .token_quality_cache_max_fetched_at_utc_for_seen_buy_mints
                .map(|ts| ts.to_rfc3339()),
            Some("2026-04-21T16:06:06+00:00".to_string())
        );
        Ok(())
    }

    #[test]
    fn proof_path_does_not_create_wal_or_shm_side_files_on_fixture() -> Result<()> {
        let fixture = TestFixture::new(FixtureSeed::Ranked)?;
        let wal_path = sidecar_path(&fixture.db_path, "-wal");
        let shm_path = sidecar_path(&fixture.db_path, "-shm");
        assert!(
            !wal_path.exists(),
            "fixture should start without a wal file"
        );
        assert!(
            !shm_path.exists(),
            "fixture should start without a shm file"
        );

        let _report = run(&fixture.config(false))?;

        assert!(!wal_path.exists(), "proof path must not create a wal file");
        assert!(!shm_path.exists(), "proof path must not create a shm file");
        Ok(())
    }

    #[test]
    fn json_output_contains_all_required_batch3_fields() -> Result<()> {
        let fixture = TestFixture::new(FixtureSeed::Ranked)?;
        let report = run(&fixture.config(true))?;
        let json = render_output(&report, true)?;
        let parsed: Value = serde_json::from_str(&json)?;

        for field in [
            "db_path",
            "db_file_size_bytes",
            "db_file_mtime_utc",
            "db_page_size",
            "db_page_count",
            "db_read_only_open_confirmed",
            "config_path",
            "config_fingerprint_method",
            "config_fingerprint",
            "fixed_now_utc",
            "rpc_enabled",
            "metrics_window_start_utc",
            "observed_swaps_loaded",
            "wallets_seen",
            "eligible_wallet_count",
            "observed_swaps_window_min_ts_utc",
            "observed_swaps_window_max_ts_utc",
            "observed_swaps_buy_count",
            "observed_swaps_sell_count",
            "observed_swaps_distinct_wallet_count",
            "observed_swaps_distinct_buy_mint_count",
            "wallet_activity_days_window_min_day_utc",
            "wallet_activity_days_window_max_day_utc",
            "wallet_activity_days_rows_for_seen_wallets",
            "wallet_activity_days_distinct_wallets_for_seen_wallets",
            "wallet_activity_days_min_active_days_among_seen_wallets",
            "wallet_activity_days_max_active_days_among_seen_wallets",
            "token_quality_cache_rows_for_seen_buy_mints",
            "token_quality_cache_distinct_mints_for_seen_buy_mints",
            "token_quality_cache_fresh_rows_for_seen_buy_mints",
            "token_quality_cache_stale_rows_for_seen_buy_mints",
            "token_quality_cache_missing_rows_for_seen_buy_mints",
            "token_quality_cache_min_fetched_at_utc_for_seen_buy_mints",
            "token_quality_cache_max_fetched_at_utc_for_seen_buy_mints",
            "ranked_wallets",
            "rejected_wallet_sample_limit",
            "rejected_wallet_samples",
            "reject_breakdown",
            "token_quality_coverage",
            "selector_universe_explanation",
        ] {
            assert!(parsed.get(field).is_some(), "missing json field {field}");
        }
        assert!(parsed["metrics_window_start_utc"].is_string());
        assert!(parsed["observed_swaps_loaded"].is_u64());
        assert!(parsed["wallets_seen"].is_u64());
        assert!(parsed["eligible_wallet_count"].is_u64());
        assert!(parsed["rejected_wallet_samples"].is_array());
        assert!(parsed["selector_universe_explanation"].is_string());
        Ok(())
    }

    #[test]
    fn repeated_runs_with_fixed_now_produce_identical_json() -> Result<()> {
        let fixture = TestFixture::new(FixtureSeed::Ranked)?;
        let first = render_output(&run(&fixture.config(true))?, true)?;
        let second = render_output(&run(&fixture.config(true))?, true)?;

        assert_eq!(first, second);
        Ok(())
    }

    #[derive(Debug, Clone, Copy)]
    enum FixtureSeed {
        Ranked,
        QualityGaps,
        GatedEmpty,
        NoSwaps,
        PersistedActivityCoverageDiff,
    }

    struct TestFixture {
        _tempdir: TempDir,
        db_path: PathBuf,
        config_path: PathBuf,
        now: DateTime<Utc>,
    }

    impl TestFixture {
        fn new(seed: FixtureSeed) -> Result<Self> {
            let tempdir = TempDir::new().context("failed creating tempdir")?;
            let db_path = tempdir.path().join("selector-proof.sqlite");
            let config_path = tempdir.path().join("selector-proof.toml");
            let now = DateTime::parse_from_rfc3339("2026-04-21T16:07:06Z")
                .map(|ts| ts.with_timezone(&Utc))
                .context("failed parsing fixed fixture now")?;
            create_sqlite_fixture(&db_path, seed, now)?;
            copy_live_config_fixture(&config_path)?;
            rewrite_test_config(&config_path)?;
            Ok(Self {
                _tempdir: tempdir,
                db_path,
                config_path,
                now,
            })
        }

        fn config(&self, json: bool) -> Config {
            Config {
                db_path: self.db_path.clone(),
                config_path: self.config_path.clone(),
                now: self.now,
                json,
            }
        }
    }

    fn create_sqlite_fixture(path: &Path, seed: FixtureSeed, now: DateTime<Utc>) -> Result<()> {
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(path)
            .with_context(|| format!("failed creating sqlite fixture {}", path.display()))?;
        store.run_migrations(&migration_dir).with_context(|| {
            format!("failed running migrations from {}", migration_dir.display())
        })?;

        match seed {
            FixtureSeed::Ranked => seed_ranked_fixture(&store, now)?,
            FixtureSeed::QualityGaps => seed_quality_gap_fixture(&store, now)?,
            FixtureSeed::GatedEmpty => seed_gated_empty_fixture(&store, now)?,
            FixtureSeed::NoSwaps => {}
            FixtureSeed::PersistedActivityCoverageDiff => {
                seed_persisted_activity_coverage_diff_fixture(&store, now)?
            }
        }
        Ok(())
    }

    fn copy_live_config_fixture(destination: &Path) -> Result<()> {
        let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../configs/live.toml");
        fs::copy(&source, destination).with_context(|| {
            format!(
                "failed copying config fixture {} -> {}",
                source.display(),
                destination.display()
            )
        })?;
        Ok(())
    }

    fn rewrite_test_config(path: &Path) -> Result<()> {
        let raw = fs::read_to_string(path)
            .with_context(|| format!("failed reading {}", path.display()))?;
        let updated = raw
            .replace("min_trades = 10", "min_trades = 2")
            .replace("min_active_days = 3", "min_active_days = 1")
            .replace("min_score = 0.4", "min_score = 0.0")
            .replace("min_buy_count = 10", "min_buy_count = 1")
            .replace(
                "require_open_positions_for_publication = true",
                "require_open_positions_for_publication = false",
            )
            .replace("max_rug_ratio = 0.60", "max_rug_ratio = 1.0")
            .replace(
                "thin_market_min_volume_sol = 3.0",
                "thin_market_min_volume_sol = 0.0",
            )
            .replace(
                "thin_market_min_unique_traders = 10",
                "thin_market_min_unique_traders = 0",
            )
            .replace("min_liquidity_sol = 1.0", "min_liquidity_sol = 0.0")
            .replace("min_volume_5m_sol = 0.5", "min_volume_5m_sol = 0.0")
            .replace("min_unique_traders_5m = 1", "min_unique_traders_5m = 0");
        fs::write(path, updated)
            .with_context(|| format!("failed writing rewritten config {}", path.display()))?;
        Ok(())
    }

    fn seed_ranked_fixture(store: &SqliteStore, now: DateTime<Utc>) -> Result<()> {
        store.insert_observed_swaps_batch_with_activity_days(&[
            buy_swap(
                "sig-alpha-buy",
                "wallet-alpha",
                "TokenAlpha111",
                now - Duration::hours(3),
                1,
            ),
            sell_swap(
                "sig-alpha-sell",
                "wallet-alpha",
                "TokenAlpha111",
                now - Duration::hours(2),
                2,
                1.8,
            ),
            buy_swap(
                "sig-beta-buy",
                "wallet-beta",
                "TokenBeta111",
                now - Duration::hours(1),
                3,
            ),
            sell_swap(
                "sig-beta-sell",
                "wallet-beta",
                "TokenBeta111",
                now - Duration::minutes(30),
                4,
                1.1,
            ),
        ])?;
        store.upsert_token_quality_cache(
            "TokenAlpha111",
            Some(100),
            Some(10.0),
            Some(3_600),
            now - Duration::minutes(1),
        )?;
        store.upsert_token_quality_cache(
            "TokenBeta111",
            Some(100),
            Some(10.0),
            Some(3_600),
            now - Duration::minutes(1),
        )?;
        Ok(())
    }

    fn seed_quality_gap_fixture(store: &SqliteStore, now: DateTime<Utc>) -> Result<()> {
        store.insert_observed_swaps_batch_with_activity_days(&[
            buy_swap(
                "sig-good-buy",
                "wallet-good",
                "TokenGood111",
                now - Duration::hours(3),
                1,
            ),
            sell_swap(
                "sig-good-sell",
                "wallet-good",
                "TokenGood111",
                now - Duration::hours(2),
                2,
                1.7,
            ),
            buy_swap(
                "sig-missing-buy",
                "wallet-missing",
                "TokenMissing111",
                now - Duration::hours(1),
                3,
            ),
            sell_swap(
                "sig-missing-sell",
                "wallet-missing",
                "TokenMissing111",
                now - Duration::minutes(50),
                4,
                1.2,
            ),
            buy_swap(
                "sig-stale-buy",
                "wallet-stale",
                "TokenStale111",
                now - Duration::minutes(40),
                5,
            ),
            sell_swap(
                "sig-stale-sell",
                "wallet-stale",
                "TokenStale111",
                now - Duration::minutes(20),
                6,
                1.3,
            ),
        ])?;
        store.upsert_token_quality_cache(
            "TokenGood111",
            Some(100),
            Some(10.0),
            Some(3_600),
            now - Duration::minutes(1),
        )?;
        store.upsert_token_quality_cache(
            "TokenStale111",
            Some(100),
            Some(10.0),
            Some(3_600),
            now - Duration::hours(1),
        )?;
        Ok(())
    }

    fn seed_gated_empty_fixture(store: &SqliteStore, now: DateTime<Utc>) -> Result<()> {
        store.insert_observed_swaps_batch_with_activity_days(&[buy_swap(
            "sig-gated-buy",
            "wallet-gated",
            "TokenGated111",
            now - Duration::minutes(20),
            1,
        )])?;
        Ok(())
    }

    fn seed_persisted_activity_coverage_diff_fixture(
        store: &SqliteStore,
        now: DateTime<Utc>,
    ) -> Result<()> {
        seed_ranked_fixture(store, now)?;
        store.upsert_wallet_activity_days(&[WalletActivityDayRow {
            wallet_id: "wallet-alpha".to_string(),
            activity_day: (now - Duration::days(2)).date_naive(),
            last_seen: now - Duration::days(2) + Duration::hours(4),
        }])?;
        Ok(())
    }

    fn buy_swap(
        signature: &str,
        wallet: &str,
        token_out: &str,
        ts_utc: DateTime<Utc>,
        slot: u64,
    ) -> SwapEvent {
        SwapEvent {
            signature: signature.to_string(),
            wallet: wallet.to_string(),
            dex: "raydium".to_string(),
            token_in: SOL_MINT.to_string(),
            token_out: token_out.to_string(),
            amount_in: 1.0,
            amount_out: 100.0,
            exact_amounts: None,
            slot,
            ts_utc,
        }
    }

    fn sell_swap(
        signature: &str,
        wallet: &str,
        token_in: &str,
        ts_utc: DateTime<Utc>,
        slot: u64,
        amount_out: f64,
    ) -> SwapEvent {
        SwapEvent {
            signature: signature.to_string(),
            wallet: wallet.to_string(),
            dex: "raydium".to_string(),
            token_in: token_in.to_string(),
            token_out: SOL_MINT.to_string(),
            amount_in: 100.0,
            amount_out,
            exact_amounts: None,
            slot,
            ts_utc,
        }
    }

    fn sidecar_path(path: &Path, suffix: &str) -> PathBuf {
        PathBuf::from(format!("{}{}", path.display(), suffix))
    }
}
