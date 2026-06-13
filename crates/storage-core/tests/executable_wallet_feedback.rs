use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_storage_core::{ExecutionQuoteCanaryEventInsert, SqliteStore};
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
fn executable_wallet_feedback_aggregates_follower_pnl_and_flips() -> Result<()> {
    let store = open_migrated_store("executable-wallet-feedback")?;
    let now = ts("2026-06-13T10:00:00Z");
    for index in 0..10 {
        let opened = now - Duration::hours(1) + Duration::seconds(index);
        let closed = opened + Duration::seconds(30);
        let signal_id = format!("shadow-close-{index}");
        let token = format!("ExecutableFeedbackToken{index}");
        store.insert_shadow_closed_trade(
            &signal_id,
            "bad-wallet",
            &token,
            1000.0,
            0.20,
            0.21,
            0.01,
            opened,
            closed,
        )?;
        let close_id = close_id_for_signal(&store, &signal_id)?;
        store.record_execution_quote_canary_event(&quote_event(
            &format!("buy-{index}"),
            Some(format!("buy-signal-{index}")),
            None,
            "bad-wallet",
            &token,
            "buy",
            opened,
            Some(opened),
            "10000000",
            "1000",
            "would_execute",
        ))?;
        store.record_execution_quote_canary_event(&quote_event(
            &format!("sell-{index}"),
            Some(format!("sell-signal-{index}")),
            Some(close_id),
            "bad-wallet",
            &token,
            "sell",
            opened + Duration::seconds(30),
            Some(closed),
            "1000",
            "9000000",
            "would_execute",
        ))?;
    }

    let feedback = store.executable_wallet_feedback_since(now - Duration::hours(48))?;
    let bad = feedback.get("bad-wallet").expect("bad wallet feedback");
    assert_eq!(bad.samples, 10);
    assert_eq!(bad.shadow_positive_executable_negative, 10);
    assert_close(bad.flip_rate().expect("flip rate"), 1.0);
    assert_close(bad.quote_adjusted_pnl_after_priority_fee_sol, -0.01);
    Ok(())
}

fn close_id_for_signal(store: &SqliteStore, signal_id: &str) -> Result<i64> {
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

fn assert_close(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() < 0.000_000_001,
        "actual={actual} expected={expected}"
    );
}
