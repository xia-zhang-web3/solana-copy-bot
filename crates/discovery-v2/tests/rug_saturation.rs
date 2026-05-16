use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_config::{DiscoveryConfig, ShadowConfig};
use copybot_core_types::SwapEvent;
use copybot_discovery_v2::{build_discovery_v2_status, DiscoveryV2BuildOptions};
use copybot_storage_core::{ensure_discovery_v2_schema, SqliteDiscoveryStore};
use tempfile::tempdir;

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

#[test]
fn rug_lookahead_saturation_preserves_non_rugged_result() -> Result<()> {
    let dir = tempdir()?;
    let store = SqliteDiscoveryStore::open(dir.path().join("runtime.db"))?;
    ensure_discovery_v2_schema(&store)?;
    let now = DateTime::parse_from_rfc3339("2026-05-03T10:00:00Z")?.with_timezone(&Utc);
    let token = "SaturatedRugToken11111111111111111111111";
    let wallet = "rug_saturation_wallet";
    let buy_ts = now - Duration::seconds(75);
    let mut swaps = vec![
        tail_coverage_swap("sig-rug-saturation-coverage", 1, now - Duration::hours(25)),
        buy_swap(wallet, token, "sig-rug-saturation-buy", 10, buy_ts),
    ];
    for idx in 0u64..120 {
        swaps.push(sell_swap(
            &format!("rug_saturation_noise_{idx}"),
            token,
            &format!("sig-rug-saturation-noise-{idx}"),
            11 + idx,
            buy_ts + Duration::seconds((1 + (idx % 45)) as i64),
        ));
    }
    swaps.push(tail_coverage_swap(
        "sig-rug-saturation-tail",
        500,
        now - Duration::seconds(5),
    ));
    store.insert_observed_swaps_batch(&swaps)?;
    store.upsert_token_quality_cache(token, Some(5), Some(1.0), Some(60), now)?;

    let (mut discovery, shadow) = strict_policy();
    discovery.thin_market_min_volume_sol = 0.5;
    discovery.thin_market_min_unique_traders = 2;
    discovery.rug_lookahead_seconds = 60;
    let mut build_options = options(now);
    build_options.max_rows = 200;

    let status = build_discovery_v2_status(&store, &discovery, &shadow, build_options)?;

    assert!(status.production_green, "{:?}", status.blockers);
    assert_eq!(status.candidate_wallets, vec![wallet.to_string()]);
    let metric = status
        .wallet_metrics
        .iter()
        .find(|metric| metric.wallet_id == wallet)
        .expect("candidate metric");
    assert_eq!(metric.rug_lookahead_evaluated, 1);
    assert_eq!(metric.rug_lookahead_unevaluated, 0);
    assert_eq!(metric.rug_ratio, 0.0);
    Ok(())
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

fn strict_policy() -> (DiscoveryConfig, ShadowConfig) {
    let mut discovery = DiscoveryConfig::default();
    discovery.min_leader_notional_sol = 0.0;
    discovery.min_trades = 1;
    discovery.min_active_days = 1;
    discovery.min_score = 0.0;
    discovery.min_buy_count = 1;
    discovery.follow_top_n = 1;
    discovery.min_tradable_ratio = 0.25;
    discovery.require_open_positions_for_publication = true;
    discovery.max_rug_ratio = 0.60;
    discovery.rug_lookahead_seconds = 60;
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

fn buy_swap(
    wallet: &str,
    token_mint: &str,
    signature: &str,
    slot: u64,
    ts_utc: DateTime<Utc>,
) -> SwapEvent {
    SwapEvent {
        wallet: wallet.to_string(),
        dex: "test".to_string(),
        token_in: SOL_MINT.to_string(),
        token_out: token_mint.to_string(),
        amount_in: 1.0,
        amount_out: 10.0,
        signature: signature.to_string(),
        slot,
        ts_utc,
        exact_amounts: None,
    }
}

fn sell_swap(
    wallet: &str,
    token_mint: &str,
    signature: &str,
    slot: u64,
    ts_utc: DateTime<Utc>,
) -> SwapEvent {
    SwapEvent {
        wallet: wallet.to_string(),
        dex: "test".to_string(),
        token_in: token_mint.to_string(),
        token_out: SOL_MINT.to_string(),
        amount_in: 10.0,
        amount_out: 1.0,
        signature: signature.to_string(),
        slot,
        ts_utc,
        exact_amounts: None,
    }
}

fn tail_coverage_swap(signature: &str, slot: u64, ts_utc: DateTime<Utc>) -> SwapEvent {
    buy_swap(
        "tail_wallet",
        "TailCoverageToken11111111111111111111111111",
        signature,
        slot,
        ts_utc,
    )
}
