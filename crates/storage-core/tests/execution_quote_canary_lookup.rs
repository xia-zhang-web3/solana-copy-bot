use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::{CopySignalRow, Lamports, COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS};
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
fn latest_execution_quote_canary_entry_event_loads_metadata_for_signal() -> Result<()> {
    let store = open_migrated_store("execution-quote-canary-entry-lookup")?;
    let now = ts("2026-06-02T08:00:00Z");
    let signal = signal("buy-lookup", now);
    store.insert_copy_signal(&signal)?;
    store.record_execution_quote_canary_event(&quote_event(
        "quote:entry:old",
        &signal,
        now,
        "111",
    ))?;
    store.record_execution_quote_canary_event(&quote_event(
        "quote:entry:new",
        &signal,
        now + Duration::seconds(1),
        "222",
    ))?;

    let loaded = store
        .load_latest_execution_quote_canary_entry_event(&signal.signal_id)?
        .expect("latest entry event should load");

    assert_eq!(loaded.event_id, "quote:entry:new");
    assert_eq!(loaded.quote_out_amount_raw.as_deref(), Some("222"));
    assert_eq!(
        loaded.quote_response_json.as_deref(),
        Some("{\"loadedLongtailToken\":true}")
    );
    assert_eq!(loaded.priority_fee_lamports, Some(12_345));
    assert_eq!(
        loaded.route_plan_json.as_deref(),
        Some("[{\"swapInfo\":{\"label\":\"Metis\"}}]")
    );
    Ok(())
}

fn signal(signal_id: &str, ts: DateTime<Utc>) -> CopySignalRow {
    CopySignalRow {
        signal_id: signal_id.to_string(),
        wallet_id: "leader-wallet".to_string(),
        side: "buy".to_string(),
        token: "TokenMint".to_string(),
        notional_sol: 0.2,
        notional_lamports: Some(Lamports::new(200_000_000)),
        notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
        ts,
        status: "shadow_recorded".to_string(),
    }
}

fn quote_event(
    event_id: &str,
    signal: &CopySignalRow,
    request_ts: DateTime<Utc>,
    out_amount: &str,
) -> ExecutionQuoteCanaryEventInsert {
    ExecutionQuoteCanaryEventInsert {
        event_id: event_id.to_string(),
        signal_id: Some(signal.signal_id.clone()),
        shadow_closed_trade_id: None,
        wallet_id: signal.wallet_id.clone(),
        token: signal.token.clone(),
        side: "buy".to_string(),
        quote_status: "ok".to_string(),
        request_ts,
        signal_ts: Some(signal.ts),
        decision_delay_ms: Some(7),
        quote_latency_ms: Some(11),
        leader_notional_sol: Some(signal.notional_sol),
        quote_in_amount_raw: Some("10000000".to_string()),
        quote_out_amount_raw: Some(out_amount.to_string()),
        quote_response_json: Some("{\"loadedLongtailToken\":true}".to_string()),
        quote_price_sol: Some(0.081),
        shadow_price_sol: Some(0.08),
        slippage_bps: Some(125.0),
        price_impact_pct: Some(0.01),
        route_plan_json: Some("[{\"swapInfo\":{\"label\":\"Metis\"}}]".to_string()),
        priority_fee_status: Some("ok".to_string()),
        priority_fee_lamports: Some(12_345),
        priority_fee_json: Some("{\"recommended\":12345}".to_string()),
        decision_status: Some("would_execute".to_string()),
        decision_reason: Some("within_slippage_limit".to_string()),
        error: None,
    }
}
