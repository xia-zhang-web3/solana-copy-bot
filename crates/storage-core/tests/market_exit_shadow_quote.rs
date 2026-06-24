use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::TokenQuantity;
use copybot_storage_core::{
    ExecutionQuoteCanaryEventInsert, SqliteStore, SHADOW_CLOSE_CONTEXT_STALE_QUOTE_PRICE,
};
use tempfile::tempdir;

fn ts(raw: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(raw)
        .expect("timestamp")
        .with_timezone(&Utc)
}

fn open_migrated_store(name: &str) -> Result<SqliteStore> {
    let dir = tempdir()?;
    let db_path = dir.keep().join(format!("{name}.db"));
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    Ok(store)
}

#[test]
fn market_exit_shadow_quote_candidates_skip_only_diag_events() -> Result<()> {
    let store = open_migrated_store("market-exit-shadow-quote")?;
    let now = ts("2026-05-30T08:00:00Z");
    store.insert_shadow_closed_trade_exact(
        "close-market-diag",
        "leader-wallet",
        "TokenMint",
        12.34,
        Some(TokenQuantity::new(1_234, 2)),
        0.2,
        0.24,
        0.04,
        now - Duration::minutes(2),
        now - Duration::seconds(20),
    )?;

    let prefix = "quote:market-exit-shadow-diag:";
    let candidates =
        store.list_market_exit_shadow_quote_candidates(now - Duration::minutes(1), prefix, 10)?;
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].qty_raw.as_deref(), Some("1234"));
    assert_eq!(candidates[0].qty_decimals, Some(2));

    let event = sell_quote_event(&format!("{prefix}{}", candidates[0].id), now);
    store.record_execution_quote_canary_event(&event)?;
    let candidates =
        store.list_market_exit_shadow_quote_candidates(now - Duration::minutes(1), prefix, 10)?;

    assert!(
        candidates.is_empty(),
        "market-exit diagnostic event should deduplicate its own close"
    );
    Ok(())
}

#[test]
fn market_exit_shadow_quote_candidates_require_market_context() -> Result<()> {
    let store = open_migrated_store("market-exit-shadow-quote-context")?;
    let now = ts("2026-05-30T08:00:00Z");
    store.insert_shadow_closed_trade_exact_with_context(
        "close-stale",
        "leader-wallet",
        "TokenMint",
        12.34,
        Some(TokenQuantity::new(1_234, 2)),
        0.2,
        0.24,
        0.04,
        SHADOW_CLOSE_CONTEXT_STALE_QUOTE_PRICE,
        now - Duration::minutes(2),
        now - Duration::seconds(20),
    )?;

    let candidates = store.list_market_exit_shadow_quote_candidates(
        now - Duration::minutes(1),
        "quote:market-exit-shadow-diag:",
        10,
    )?;
    assert!(candidates.is_empty());
    Ok(())
}

fn sell_quote_event(event_id: &str, now: DateTime<Utc>) -> ExecutionQuoteCanaryEventInsert {
    ExecutionQuoteCanaryEventInsert {
        event_id: event_id.to_string(),
        signal_id: None,
        shadow_closed_trade_id: None,
        wallet_id: "leader-wallet".to_string(),
        token: "TokenMint".to_string(),
        side: "sell".to_string(),
        quote_status: "ok".to_string(),
        request_ts: now,
        signal_ts: Some(now),
        decision_delay_ms: Some(0),
        quote_latency_ms: Some(10),
        leader_notional_sol: Some(0.24),
        quote_in_amount_raw: Some("1234".to_string()),
        quote_out_amount_raw: Some("240000000".to_string()),
        quote_response_json: Some("{\"routePlan\":[]}".to_string()),
        quote_price_sol: Some(0.0194),
        shadow_price_sol: Some(0.0194),
        slippage_bps: Some(0.0),
        price_impact_pct: Some(0.01),
        route_plan_json: Some("[]".to_string()),
        priority_fee_status: None,
        priority_fee_lamports: None,
        priority_fee_json: None,
        decision_status: None,
        decision_reason: None,
        error: None,
    }
}
