use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_config::{DiscoveryConfig, ShadowConfig, DISCOVERY_V2_SCORING_SOURCE};
use copybot_discovery_v2::{
    build_discovery_v2_wallet_report, DiscoveryV2FilterStatus, DiscoveryV2LivePortfolioStatus,
    DiscoveryV2ScanStatus, DiscoveryV2Status, DiscoveryV2WalletMetric,
    DiscoveryV2WalletReportOptions,
};
use copybot_storage_core::{ensure_discovery_v2_schema, SqliteDiscoveryStore};
use tempfile::tempdir;

fn test_store() -> Result<(tempfile::TempDir, SqliteDiscoveryStore)> {
    let dir = tempdir()?;
    let store = SqliteDiscoveryStore::open(dir.path().join("runtime.db"))?;
    ensure_discovery_v2_schema(&store)?;
    Ok((dir, store))
}

fn policy() -> (DiscoveryConfig, ShadowConfig) {
    let mut discovery = DiscoveryConfig::default();
    discovery.follow_top_n = 1;
    discovery.min_score = 0.25;
    discovery.min_trades = 3;
    discovery.min_active_days = 1;
    discovery.min_buy_count = 2;
    discovery.min_leader_notional_sol = 0.5;
    discovery.min_tradable_ratio = 0.50;
    discovery.require_open_positions_for_publication = true;
    discovery.live_portfolio_gate_enabled = true;
    discovery.min_live_sol_balance = 0.05;
    discovery.min_live_portfolio_value_sol = 0.25;
    let mut shadow = ShadowConfig::default();
    shadow.quality_gates_enabled = true;
    shadow.min_token_age_seconds = 30;
    shadow.min_holders = 5;
    shadow.min_liquidity_sol = 1.0;
    shadow.min_volume_5m_sol = 0.5;
    shadow.min_unique_traders_5m = 1;
    (discovery, shadow)
}

fn status(now: DateTime<Utc>) -> DiscoveryV2Status {
    let accepted = metric("wallet-a", now, 0.81, true, Vec::new());
    let rejected = metric(
        "wallet-b",
        now,
        0.0,
        false,
        vec!["low_tradable_ratio".to_string()],
    );
    DiscoveryV2Status {
        source: DISCOVERY_V2_SCORING_SOURCE.to_string(),
        now,
        build_elapsed_ms: 12,
        window_start: now - Duration::hours(24),
        window_minutes: 24 * 60,
        max_tail_lag_seconds: 1_200,
        tail: None,
        coverage_sample: None,
        scan: DiscoveryV2ScanStatus {
            max_rows: 100,
            time_budget_ms: 5_000,
            rows_scanned: 10,
            unique_wallets: 2,
            max_rows_exhausted: false,
            time_budget_exhausted: false,
            budget_exhausted: false,
        },
        live_portfolio: Some(DiscoveryV2LivePortfolioStatus {
            enabled: true,
            checked_wallets: 2,
            accepted_wallets: 1,
            rejected_wallets: 1,
            rpc_failures: 0,
            max_wallets: 10,
            rpc_missing: false,
        }),
        filters: DiscoveryV2FilterStatus {
            total_wallets: 2,
            eligible_wallets: 1,
            rejected_wallets: 1,
            reject_breakdown: [("low_tradable_ratio".to_string(), 1)].into(),
        },
        wallet_metrics_total: 2,
        wallet_metrics_returned: 2,
        wallet_metrics_truncated: false,
        wallet_metrics: vec![accepted, rejected],
        candidate_wallets: vec!["wallet-a".to_string()],
        execution_enabled: false,
        execution_disabled: true,
        blockers: Vec::new(),
        production_green: true,
        policy_fingerprint: "test-policy".to_string(),
    }
}

fn metric(
    wallet_id: &str,
    now: DateTime<Utc>,
    score: f64,
    eligible: bool,
    reject_reasons: Vec<String>,
) -> DiscoveryV2WalletMetric {
    DiscoveryV2WalletMetric {
        wallet_id: wallet_id.to_string(),
        trades: 4,
        active_days: 1,
        buys: 3,
        sells: 1,
        max_buy_notional_sol: 1.2,
        pnl_sol: 0.42,
        win_rate: 0.5,
        closed_trades: 1,
        hold_median_seconds: 60,
        buy_total: 3,
        tradable_ratio: if eligible { 1.0 } else { 0.25 },
        missing_quality_evidence_buys: 0,
        rug_ratio: 0.0,
        rug_lookahead_evaluated: 3,
        rug_lookahead_unevaluated: 0,
        live_sol_balance: Some(0.08),
        live_token_value_sol: Some(0.35),
        live_token_positions: Some(2),
        live_tradable_token_positions: Some(2),
        eligible,
        reject_reasons,
        first_seen: now - Duration::hours(2),
        last_seen: now - Duration::minutes(5),
        score,
    }
}

#[test]
fn wallet_report_shows_active_follow_and_filter_evidence() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = DateTime::parse_from_rfc3339("2026-05-14T12:00:00+00:00")?.with_timezone(&Utc);
    store.activate_follow_wallet("wallet-a", now, "test")?;
    let (discovery, shadow) = policy();

    let report = build_discovery_v2_wallet_report(
        &store,
        &discovery,
        &shadow,
        status(now),
        DiscoveryV2WalletReportOptions {
            now: now + Duration::seconds(30),
            limit: 5,
            include_rejected: true,
        },
    )?;

    assert!(report.production_green);
    assert_eq!(report.thresholds.min_tradable_ratio, 0.50);
    assert_eq!(report.wallets.len(), 1);
    assert_eq!(report.wallets[0].wallet_id, "wallet-a");
    assert!(report.wallets[0].active_follow);
    assert!(report.wallets[0].filters.passed_all);
    assert!(report.wallets[0].filters.live_portfolio_pass);
    assert_eq!(report.top_rejected_wallets.len(), 1);
    assert_eq!(report.top_rejected_wallets[0].wallet_id, "wallet-b");
    assert!(!report.top_rejected_wallets[0].filters.tradable_ratio_pass);
    assert!(!report.top_rejected_wallets[0].filters.passed_all);
    Ok(())
}

#[test]
fn wallet_report_rejects_candidate_missing_metric() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = DateTime::parse_from_rfc3339("2026-05-14T12:00:00+00:00")?.with_timezone(&Utc);
    let (discovery, shadow) = policy();
    let mut status = status(now);
    status.wallet_metrics.clear();

    let err = build_discovery_v2_wallet_report(
        &store,
        &discovery,
        &shadow,
        status,
        DiscoveryV2WalletReportOptions {
            now,
            limit: 5,
            include_rejected: false,
        },
    )
    .expect_err("candidate without metric must fail closed");

    assert!(err.to_string().contains("candidate wallet is missing"));
    Ok(())
}
