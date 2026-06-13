use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_config::{DiscoveryConfig, ShadowConfig};
use copybot_core_types::SwapEvent;
use copybot_discovery_v2::{build_discovery_v2_status, DiscoveryV2BuildOptions};
use copybot_storage_core::{
    ensure_discovery_v2_schema, ExecutionQuoteCanaryEventInsert, SqliteDiscoveryStore,
};
use tempfile::tempdir;

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

#[test]
fn executable_feedback_rejects_wallet_with_unfollowable_live_pnl() -> Result<()> {
    let dir = tempdir()?;
    let mut store = SqliteDiscoveryStore::open(dir.path().join("runtime.db"))?;
    store.run_migrations(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    ensure_discovery_v2_schema(&store)?;

    let now = DateTime::parse_from_rfc3339("2026-05-14T10:00:00Z")?.with_timezone(&Utc);
    let bad_wallet = "wallet_executable_feedback_bad";
    let good_wallet = "wallet_executable_feedback_good";
    let replacement_wallet = "wallet_executable_feedback_replacement";
    let token_a = "ExecutableFeedbackBadToken111111111111";
    let token_b = "ExecutableFeedbackGoodToken22222222222";
    let token_c = "ExecutableFeedbackReplacementToken3333";

    seed_candidate_swaps(
        &store,
        now,
        [
            (bad_wallet, token_a, "sig-executable-feedback-bad", 10),
            (good_wallet, token_b, "sig-executable-feedback-good", 20),
            (
                replacement_wallet,
                token_c,
                "sig-executable-feedback-replacement",
                30,
            ),
        ],
    )?;
    for token in [token_a, token_b, token_c] {
        store.upsert_token_quality_cache(token, Some(5), Some(1.0), Some(60), now)?;
    }
    for index in 0..10 {
        let opened = now - Duration::hours(1) + Duration::seconds(index);
        let closed = opened + Duration::seconds(30);
        let signal_id = format!("executable-feedback-shadow-close-{index}");
        store.insert_shadow_closed_trade(
            &signal_id, bad_wallet, token_a, 1000.0, 0.20, 0.21, 0.01, opened, closed,
        )?;
        let close_id = close_id_for_signal(&store, &signal_id)?;
        store.record_execution_quote_canary_event(&quote_event(
            &format!("executable-feedback-buy-{index}"),
            Some(format!("executable-feedback-buy-signal-{index}")),
            None,
            bad_wallet,
            token_a,
            "buy",
            opened,
            Some(opened),
            "10000000",
            "1000",
            "would_execute",
        ))?;
        store.record_execution_quote_canary_event(&quote_event(
            &format!("executable-feedback-sell-{index}"),
            Some(format!("executable-feedback-sell-signal-{index}")),
            Some(close_id),
            bad_wallet,
            token_a,
            "sell",
            closed,
            Some(closed),
            "1000",
            "9000000",
            "would_execute",
        ))?;
    }

    let (mut discovery, shadow) = strict_policy();
    discovery.follow_top_n = 2;
    discovery.executable_wallet_filter_enabled = true;
    discovery.executable_wallet_filter_min_samples = 10;
    discovery.executable_wallet_filter_max_pnl_sol = 0.0;
    discovery.executable_wallet_filter_max_flip_rate = 0.40;
    let status = build_discovery_v2_status(&store, &discovery, &shadow, options(now))?;

    assert!(status.production_green, "{:?}", status.blockers);
    assert_eq!(status.candidate_wallets.len(), 2);
    assert!(!status.candidate_wallets.contains(&bad_wallet.to_string()));
    assert!(status.candidate_wallets.contains(&good_wallet.to_string()));
    assert!(status
        .candidate_wallets
        .contains(&replacement_wallet.to_string()));
    assert_eq!(
        status
            .filters
            .reject_breakdown
            .get("executable_feedback_negative"),
        Some(&1)
    );
    let bad_metric = status
        .wallet_metrics
        .iter()
        .find(|metric| metric.wallet_id == bad_wallet)
        .expect("bad wallet metric retained");
    assert!(!bad_metric.eligible);
    assert!(bad_metric
        .reject_reasons
        .contains(&"executable_feedback_negative".to_string()));
    assert_eq!(bad_metric.executable_feedback_samples, Some(10));
    assert!(bad_metric
        .executable_feedback_pnl_after_fee_sol
        .is_some_and(|pnl| pnl <= -0.0099 && pnl >= -0.0101));
    assert_eq!(bad_metric.executable_feedback_flip_rate, Some(1.0));
    Ok(())
}

fn swap(
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

fn seed_candidate_swaps<const N: usize>(
    store: &SqliteDiscoveryStore,
    now: DateTime<Utc>,
    candidates: [(&str, &str, &str, u64); N],
) -> Result<()> {
    let mut swaps = vec![swap(
        "tail_wallet",
        candidates[0].1,
        "sig-executable-feedback-coverage",
        1,
        now - Duration::hours(25),
    )];
    for (wallet, token, signature, slot) in candidates {
        swaps.push(swap(
            wallet,
            token,
            signature,
            slot,
            now - Duration::minutes(10) + Duration::seconds(slot as i64),
        ));
    }
    swaps.push(swap(
        "tail_wallet",
        candidates[0].1,
        "sig-executable-feedback-tail",
        99,
        now - Duration::minutes(2),
    ));
    store.insert_observed_swaps_batch(&swaps)?;
    Ok(())
}

fn close_id_for_signal(store: &SqliteDiscoveryStore, signal_id: &str) -> Result<i64> {
    Ok(store
        .list_execution_quote_canary_close_candidates_for_signal(signal_id, 10)?
        .into_iter()
        .next()
        .expect("close candidate")
        .id)
}

#[allow(clippy::too_many_arguments)]
fn quote_event(
    event_id: &str,
    signal_id: Option<String>,
    shadow_closed_trade_id: Option<i64>,
    wallet_id: &str,
    token: &str,
    side: &str,
    request_ts: DateTime<Utc>,
    signal_ts: Option<DateTime<Utc>>,
    in_raw: &str,
    out_raw: &str,
    decision_status: &str,
) -> ExecutionQuoteCanaryEventInsert {
    ExecutionQuoteCanaryEventInsert {
        event_id: event_id.to_string(),
        signal_id,
        shadow_closed_trade_id,
        wallet_id: wallet_id.to_string(),
        token: token.to_string(),
        side: side.to_string(),
        quote_status: "ok".to_string(),
        request_ts,
        signal_ts,
        decision_delay_ms: Some(10),
        quote_latency_ms: Some(20),
        leader_notional_sol: Some(0.2),
        quote_in_amount_raw: Some(in_raw.to_string()),
        quote_out_amount_raw: Some(out_raw.to_string()),
        quote_response_json: None,
        quote_price_sol: None,
        shadow_price_sol: None,
        slippage_bps: None,
        price_impact_pct: None,
        route_plan_json: Some("[{\"swapInfo\":{\"label\":\"Metis\"}}]".to_string()),
        priority_fee_status: Some("ok".to_string()),
        priority_fee_lamports: Some(0),
        priority_fee_json: None,
        decision_status: Some(decision_status.to_string()),
        decision_reason: Some("test".to_string()),
        error: None,
    }
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
