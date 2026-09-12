const WALLET_SLOW: &str = "SSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSS";
const WALLET_C: &str = "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC";
const WALLET_B: &str = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB";
const WALLET_A: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_config::{DiscoveryConfig, ShadowConfig};
use copybot_core_types::SwapEvent;
use copybot_discovery_v2::{build_discovery_v2_status, DiscoveryV2BuildOptions};
use copybot_storage_core::{ensure_discovery_v2_schema, SqliteDiscoveryStore};
use tempfile::tempdir;

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

#[test]
fn live_portfolio_gate_replaces_drained_wallet_with_live_token_holder() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = DateTime::parse_from_rfc3339("2026-05-03T10:00:00Z")?.with_timezone(&Utc);
    let token_a = "DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD";
    let token_b = "EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE";
    let token_c = "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF";
    store.insert_observed_swaps_batch(&[
        tail_coverage_swap("sig-coverage-floor", 9, now - Duration::hours(25)),
        swap_with_token(WALLET_A, token_a, "sig-a", 10, now - Duration::minutes(4)),
        swap_with_token(WALLET_B, token_b, "sig-b", 11, now - Duration::minutes(3)),
        swap_with_token(WALLET_C, token_c, "sig-c", 12, now - Duration::minutes(2)),
        tail_coverage_swap("sig-tail", 13, now - Duration::minutes(1)),
    ])?;
    insert_quality_for_token(&store, token_a, now, Some(1.0))?;
    insert_quality_for_token(&store, token_b, now, Some(1.0))?;
    insert_quality_for_token(&store, token_c, now, Some(1.0))?;
    let (mut discovery, shadow) = strict_policy();
    discovery.follow_top_n = 2;
    discovery.live_portfolio_gate_enabled = true;
    discovery.min_live_sol_balance = 0.25;
    discovery.live_portfolio_max_wallets = 3;
    discovery.live_portfolio_max_token_accounts = 8;
    let rpc = start_live_portfolio_rpc(token_b);
    let mut build_options = options(now);
    build_options.live_portfolio_rpc_url = Some(rpc.url.clone());

    let status = build_discovery_v2_status(&store, &discovery, &shadow, build_options)?;
    let calls = rpc.finish();
    assert_eq!(calls.len(), 9);

    assert!(
        status.production_green,
        "blockers={:?} candidates={:?} live={:?}",
        status.blockers, status.candidate_wallets, status.live_portfolio
    );
    assert_eq!(
        status.candidate_wallets,
        vec![WALLET_B.to_string(), WALLET_C.to_string()]
    );
    let live = status.live_portfolio.expect("live portfolio status");
    assert_eq!(live.checked_wallets, 3);
    assert_eq!(live.accepted_wallets, 2);
    assert_eq!(live.rejected_wallets, 1);
    let drained = status
        .wallet_metrics
        .iter()
        .find(|metric| metric.wallet_id == WALLET_A)
        .expect("wallet_a metric");
    assert!(drained
        .reject_reasons
        .contains(&"capital_drained_after_window".to_string()));
    let token_holder = status
        .wallet_metrics
        .iter()
        .find(|metric| metric.wallet_id == WALLET_B)
        .expect("wallet_b metric");
    assert!(token_holder.live_token_value_sol.unwrap_or_default() >= 0.25);
    assert_eq!(token_holder.live_tradable_token_positions, Some(1));
    Ok(())
}

#[test]
fn live_portfolio_gate_reserves_slow_hold_slots_after_baseline_accepts() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = DateTime::parse_from_rfc3339("2026-05-03T11:00:00Z")?.with_timezone(&Utc);
    let token_a = "SlowLiveTokenA111111111111111111111111111";
    let token_b = "SlowLiveTokenB222222222222222222222222222";
    let token_slow = "SlowLiveTokenS333333333333333333333333333";
    store.insert_observed_swaps_batch(&[
        tail_coverage_swap("sig-slow-live-coverage", 9, now - Duration::hours(25)),
        swap_with_token(
            WALLET_A,
            token_a,
            "sig-slow-live-a",
            10,
            now - Duration::minutes(4),
        ),
        swap_with_token(
            WALLET_B,
            token_b,
            "sig-slow-live-b",
            11,
            now - Duration::minutes(3),
        ),
        swap_with_token(
            WALLET_SLOW,
            token_slow,
            "sig-slow-live-buy",
            12,
            now - Duration::minutes(90),
        ),
        sell_with_token(
            WALLET_SLOW,
            token_slow,
            "sig-slow-live-sell",
            13,
            now - Duration::minutes(20),
        ),
        tail_coverage_swap("sig-slow-live-tail", 14, now - Duration::minutes(1)),
    ])?;
    insert_quality_for_token(&store, token_a, now, Some(1.0))?;
    insert_quality_for_token(&store, token_b, now, Some(1.0))?;
    insert_quality_for_token(&store, token_slow, now, Some(1.0))?;
    let (mut discovery, shadow) = strict_policy();
    discovery.follow_top_n = 2;
    discovery.live_portfolio_gate_enabled = true;
    discovery.live_portfolio_max_wallets = 2;
    discovery.live_portfolio_max_token_accounts = 8;
    discovery.min_live_sol_balance = 0.25;
    discovery.slow_hold_wallets_enabled = true;
    discovery.slow_hold_top_m = 1;
    discovery.slow_hold_min_hold_median_seconds = 30 * 60;
    discovery.slow_hold_min_trades = 2;
    discovery.slow_hold_min_buy_count = 1;
    discovery.slow_hold_min_score = 0.0;
    let rpc = rpc::RpcStub::start(|request| {
        rpc::result(if request["method"] == "getBalance" {
            serde_json::json!(300_000_000u64)
        } else {
            serde_json::json!([])
        })
        .into()
    });
    let mut build_options = options(now);
    build_options.live_portfolio_rpc_url = Some(rpc.url.clone());

    let status = build_discovery_v2_status(&store, &discovery, &shadow, build_options)?;
    let calls = rpc.finish();
    assert_eq!(calls.len(), 9);

    assert!(
        status.production_green,
        "blockers={:?} candidates={:?} live={:?}",
        status.blockers, status.candidate_wallets, status.live_portfolio
    );
    assert_eq!(
        status.candidate_wallets,
        vec![
            WALLET_A.to_string(),
            WALLET_B.to_string(),
            WALLET_SLOW.to_string()
        ]
    );
    assert_eq!(
        status
            .candidate_wallet_sources
            .iter()
            .map(|source| (source.wallet_id.as_str(), source.source_cohort.as_str()))
            .collect::<Vec<_>>(),
        vec![
            (WALLET_A, "baseline"),
            (WALLET_B, "baseline"),
            (WALLET_SLOW, "slow_hold")
        ]
    );
    let live = status.live_portfolio.expect("live portfolio status");
    assert_eq!(live.checked_wallets, 3);
    assert_eq!(live.accepted_wallets, 3);
    Ok(())
}

fn test_store() -> Result<(tempfile::TempDir, SqliteDiscoveryStore)> {
    let dir = tempdir()?;
    let store = SqliteDiscoveryStore::open(dir.path().join("runtime.db"))?;
    ensure_discovery_v2_schema(&store)?;
    Ok((dir, store))
}

fn swap_with_token(
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

fn sell_with_token(
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
        amount_out: 1.2,
        signature: signature.to_string(),
        slot,
        ts_utc,
        exact_amounts: None,
    }
}

fn tail_coverage_swap(signature: &str, slot: u64, ts_utc: DateTime<Utc>) -> SwapEvent {
    swap_with_token(
        "tail_wallet",
        "TailCoverageToken11111111111111111111111111",
        signature,
        slot,
        ts_utc,
    )
}

fn insert_quality_for_token(
    store: &SqliteDiscoveryStore,
    token_mint: &str,
    now: DateTime<Utc>,
    liquidity_sol: Option<f64>,
) -> Result<()> {
    store.upsert_token_quality_cache(token_mint, Some(5), liquidity_sol, Some(60), now)
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

fn start_live_portfolio_rpc(token_b: &str) -> rpc::RpcStub {
    let token_b = token_b.to_string();
    rpc::RpcStub::start(move |request| {
        let wallet = request["params"][0].as_str().unwrap();
        if request["method"] == "getBalance" {
            return rpc::result(serde_json::json!(if wallet == WALLET_C {
                300_000_000u64
            } else {
                0
            }))
            .into();
        }
        let rows = if wallet == WALLET_B && request["params"][1]["programId"] == rpc::CLASSIC {
            vec![rpc::account(
                wallet,
                &token_b,
                &rpc::key('J'),
                rpc::CLASSIC,
                "10000000",
                6,
            )]
        } else {
            vec![]
        };
        rpc::result(serde_json::json!(rows)).into()
    })
}

#[path = "live_portfolio/fixture.rs"]
mod fixture;
#[path = "live_portfolio/inventory_cases.rs"]
mod inventory_cases;
#[path = "live_portfolio/materialized_cases.rs"]
mod materialized_cases;
#[path = "live_portfolio/rpc.rs"]
mod rpc;

#[path = "live_portfolio/protocol_cases.rs"]
mod protocol_cases;

#[path = "live_portfolio/price_controls.rs"]
mod price_controls;
#[path = "live_portfolio/price_fixture.rs"]
mod price_fixture;
#[path = "live_portfolio/price_freshness.rs"]
mod price_freshness;
#[path = "live_portfolio/price_reuse.rs"]
mod price_reuse;

#[path = "live_portfolio/price_numbers.rs"]
mod price_numbers;
