use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_config::{DiscoveryConfig, ShadowConfig};
use copybot_core_types::SwapEvent;
use copybot_discovery_v2::{build_discovery_v2_status, DiscoveryV2BuildOptions};
use copybot_storage_core::{ensure_discovery_v2_schema, SqliteDiscoveryStore};
use tempfile::tempdir;

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

fn ts(raw: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(raw)?.with_timezone(&Utc))
}

#[test]
fn status_scan_does_not_materialize_sell_only_wallet_metrics() -> Result<()> {
    let dir = tempdir()?;
    let store = SqliteDiscoveryStore::open(dir.path().join("runtime.db"))?;
    ensure_discovery_v2_schema(&store)?;
    let now = ts("2026-05-16T10:00:00Z")?;
    let token = "StatusScanToken11111111111111111111111111";
    let sell_only_token = "SellOnlyToken111111111111111111111111111";
    let tail_token = "TailToken1111111111111111111111111111111";
    store.insert_observed_swaps_batch(&[
        buy(
            "coverage_wallet",
            tail_token,
            "sig-coverage",
            1,
            now - Duration::hours(25),
        ),
        sell(
            "sell_only_wallet",
            sell_only_token,
            "sig-sell-only",
            10,
            now - Duration::minutes(20),
        ),
        buy(
            "candidate_wallet",
            token,
            "sig-candidate-buy",
            11,
            now - Duration::minutes(10),
        ),
        sell(
            "candidate_wallet",
            token,
            "sig-candidate-sell",
            12,
            now - Duration::minutes(9),
        ),
        buy(
            "candidate_wallet",
            token,
            "sig-candidate-open-buy",
            13,
            now - Duration::minutes(8),
        ),
        sell(
            "sell_only_wallet",
            token,
            "sig-sell-only-noise",
            14,
            now - Duration::minutes(7),
        ),
        buy(
            "tail_wallet",
            tail_token,
            "sig-tail",
            15,
            now - Duration::minutes(1),
        ),
    ])?;
    store.upsert_token_quality_cache(token, Some(5), Some(1.0), Some(60), now)?;
    store.upsert_token_quality_cache(tail_token, Some(5), Some(1.0), Some(60), now)?;
    let (discovery, shadow) = policy();
    let status = build_discovery_v2_status(&store, &discovery, &shadow, options(now))?;

    assert!(status.production_green, "{:?}", status.blockers);
    assert_eq!(
        status.candidate_wallets,
        vec!["candidate_wallet".to_string()]
    );
    assert_eq!(status.scan.rows_scanned, 6);
    assert_eq!(status.scan.unique_wallets, 2);
    assert!(status
        .wallet_metrics
        .iter()
        .all(|metric| metric.wallet_id != "sell_only_wallet"));
    Ok(())
}

fn policy() -> (DiscoveryConfig, ShadowConfig) {
    let mut discovery = DiscoveryConfig::default();
    discovery.min_leader_notional_sol = 0.0;
    discovery.min_trades = 3;
    discovery.min_active_days = 1;
    discovery.min_score = 0.0;
    discovery.min_buy_count = 1;
    discovery.follow_top_n = 1;
    discovery.min_tradable_ratio = 0.25;
    discovery.require_open_positions_for_publication = true;
    discovery.max_rug_ratio = 0.60;
    discovery.rug_lookahead_seconds = 10;
    discovery.thin_market_min_volume_sol = 0.5;
    discovery.thin_market_min_unique_traders = 1;
    let mut shadow = ShadowConfig::default();
    shadow.quality_gates_enabled = true;
    shadow.min_token_age_seconds = 30;
    shadow.min_holders = 5;
    shadow.min_liquidity_sol = 1.0;
    shadow.min_volume_5m_sol = 0.5;
    shadow.min_unique_traders_5m = 1;
    (discovery, shadow)
}

fn options(now: DateTime<Utc>) -> DiscoveryV2BuildOptions {
    DiscoveryV2BuildOptions {
        now,
        window_minutes: 24 * 60,
        max_tail_lag_seconds: 1_200,
        max_rows: 100,
        time_budget_ms: 5_000,
        execution_enabled: false,
        live_portfolio_rpc_url: None,
    }
}

fn buy(wallet: &str, token: &str, signature: &str, slot: u64, ts_utc: DateTime<Utc>) -> SwapEvent {
    swap(wallet, SOL_MINT, token, 1.0, 10.0, signature, slot, ts_utc)
}

fn sell(wallet: &str, token: &str, signature: &str, slot: u64, ts_utc: DateTime<Utc>) -> SwapEvent {
    swap(wallet, token, SOL_MINT, 10.0, 1.2, signature, slot, ts_utc)
}

fn swap(
    wallet: &str,
    token_in: &str,
    token_out: &str,
    amount_in: f64,
    amount_out: f64,
    signature: &str,
    slot: u64,
    ts_utc: DateTime<Utc>,
) -> SwapEvent {
    SwapEvent {
        wallet: wallet.to_string(),
        dex: "test".to_string(),
        token_in: token_in.to_string(),
        token_out: token_out.to_string(),
        amount_in,
        amount_out,
        signature: signature.to_string(),
        slot,
        ts_utc,
        exact_amounts: None,
    }
}
