use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::{
    CopySignalRow, Lamports, TokenQuantity, COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS,
};
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

#[test]
fn close_submit_retry_events_require_open_position_and_no_order() -> Result<()> {
    let store = open_migrated_store("execution-quote-canary-close-retry")?;
    let now = ts("2026-06-02T08:00:00Z");
    let buy_signal = signal("buy-retry", now - Duration::seconds(10));
    store.insert_copy_signal(&buy_signal)?;
    let buy = store.reserve_execution_canary_order(
        &buy_signal.signal_id,
        "metis",
        now - Duration::seconds(5),
    )?;
    store.record_execution_canary_open_position(
        &buy.order.order_id,
        "TokenMint",
        50.0,
        Some(TokenQuantity::new(50, 0)),
        0.01,
        now,
    )?;
    let signal = CopySignalRow {
        side: "sell".to_string(),
        ..signal("sell-retry", now)
    };
    store.insert_copy_signal(&signal)?;
    let mut event = quote_event("quote:close:retry", &signal, now, "12000000");
    event.side = "sell".to_string();
    event.shadow_closed_trade_id = Some(42);
    event.quote_in_amount_raw = Some("50".to_string());
    store.record_execution_quote_canary_event(&event)?;

    let stale = store
        .list_execution_quote_canary_close_submit_retry_event_ids(now + Duration::seconds(1), 10)?;
    assert!(stale.is_empty());

    let fresh = store
        .list_execution_quote_canary_close_submit_retry_event_ids(now - Duration::seconds(1), 10)?;
    assert_eq!(fresh, vec!["quote:close:retry".to_string()]);

    store.reserve_execution_canary_order(&signal.signal_id, "metis", now)?;
    let already_reserved = store
        .list_execution_quote_canary_close_submit_retry_event_ids(now - Duration::seconds(1), 10)?;
    assert!(already_reserved.is_empty());
    Ok(())
}

#[test]
fn close_submit_retry_events_ignore_quotes_before_current_buy_submit() -> Result<()> {
    let store = open_migrated_store("execution-quote-canary-close-retry-buy-boundary")?;
    let now = ts("2026-06-02T08:00:00Z");
    let buy_signal = signal("buy-boundary", now);
    store.insert_copy_signal(&buy_signal)?;
    let buy = store.reserve_execution_canary_order(&buy_signal.signal_id, "metis", now)?;
    store.record_execution_canary_open_position(
        &buy.order.order_id,
        "TokenMint",
        50.0,
        Some(TokenQuantity::new(50, 0)),
        0.01,
        now + Duration::minutes(3),
    )?;

    let before_signal = CopySignalRow {
        side: "sell".to_string(),
        ..signal("sell-before-buy-submit", now - Duration::minutes(1))
    };
    store.insert_copy_signal(&before_signal)?;
    let mut before_event = quote_event(
        "quote:close:before-buy-submit",
        &before_signal,
        now - Duration::minutes(1),
        "12000000",
    );
    before_event.side = "sell".to_string();
    before_event.shadow_closed_trade_id = Some(41);
    before_event.quote_in_amount_raw = Some("50".to_string());
    store.record_execution_quote_canary_event(&before_event)?;

    let late_quote_stale_signal = CopySignalRow {
        side: "sell".to_string(),
        ..signal("sell-before-buy-late-quote", now - Duration::seconds(10))
    };
    store.insert_copy_signal(&late_quote_stale_signal)?;
    let mut late_quote_stale_event = quote_event(
        "quote:close:before-buy-late-quote",
        &late_quote_stale_signal,
        now + Duration::minutes(5),
        "12000000",
    );
    late_quote_stale_event.side = "sell".to_string();
    late_quote_stale_event.shadow_closed_trade_id = Some(43);
    late_quote_stale_event.quote_in_amount_raw = Some("50".to_string());
    store.record_execution_quote_canary_event(&late_quote_stale_event)?;

    let after_signal = CopySignalRow {
        side: "sell".to_string(),
        ..signal("sell-after-buy-submit", now + Duration::seconds(30))
    };
    store.insert_copy_signal(&after_signal)?;
    let mut after_event = quote_event(
        "quote:close:after-buy-submit",
        &after_signal,
        now + Duration::seconds(30),
        "12000000",
    );
    after_event.side = "sell".to_string();
    after_event.shadow_closed_trade_id = Some(42);
    after_event.quote_in_amount_raw = Some("50".to_string());
    store.record_execution_quote_canary_event(&after_event)?;

    let retry_events = store
        .list_execution_quote_canary_close_submit_retry_event_ids(now - Duration::minutes(2), 10)?;

    assert_eq!(
        retry_events,
        vec!["quote:close:after-buy-submit".to_string()]
    );
    Ok(())
}

#[test]
fn owned_sell_signal_candidates_use_buy_signal_boundary() -> Result<()> {
    let store = open_migrated_store("execution-quote-canary-owned-sell-buy-boundary")?;
    let now = ts("2026-06-02T08:00:00Z");
    let buy_signal = signal("buy-boundary-owned", now);
    store.insert_copy_signal(&buy_signal)?;
    let buy = store.reserve_execution_canary_order(&buy_signal.signal_id, "metis", now)?;
    store.record_execution_canary_open_position(
        &buy.order.order_id,
        "TokenMint",
        50.0,
        Some(TokenQuantity::new(50, 0)),
        0.01,
        now + Duration::minutes(3),
    )?;

    let stale_sell = CopySignalRow {
        side: "sell".to_string(),
        ..signal("sell-before-buy-owned", now - Duration::seconds(1))
    };
    store.insert_copy_signal(&stale_sell)?;
    let fast_sell = CopySignalRow {
        side: "sell".to_string(),
        ..signal(
            "sell-after-buy-before-confirm-owned",
            now + Duration::seconds(1),
        )
    };
    store.insert_copy_signal(&fast_sell)?;

    let candidates = store.list_execution_quote_canary_owned_sell_signal_candidate_ids(
        "shadow_recorded",
        now - Duration::minutes(1),
        10,
    )?;

    assert_eq!(
        candidates,
        vec!["sell-after-buy-before-confirm-owned".to_string()]
    );
    Ok(())
}

#[test]
fn close_submit_retry_events_include_recovery_orphan_open_positions() -> Result<()> {
    let store = open_migrated_store("execution-quote-canary-close-retry-recovery-orphan")?;
    let now = ts("2026-06-02T08:00:00Z");
    store.record_execution_canary_open_position(
        "recovery-orphan:TokenMint:5000:test",
        "TokenMint",
        50.0,
        Some(TokenQuantity::new(50, 0)),
        0.01,
        now - Duration::minutes(10),
    )?;

    let before_signal = CopySignalRow {
        side: "sell".to_string(),
        ..signal("sell-recovery-before-open", now - Duration::minutes(20))
    };
    store.insert_copy_signal(&before_signal)?;
    let mut before_event = quote_event(
        "quote:close:recovery-before-open",
        &before_signal,
        now - Duration::minutes(20),
        "12000000",
    );
    before_event.side = "sell".to_string();
    before_event.quote_in_amount_raw = Some("50".to_string());
    store.record_execution_quote_canary_event(&before_event)?;

    let after_signal = CopySignalRow {
        side: "sell".to_string(),
        ..signal("sell-recovery-after-open", now)
    };
    store.insert_copy_signal(&after_signal)?;
    let mut after_event = quote_event(
        "quote:close:recovery-after-open",
        &after_signal,
        now,
        "12000000",
    );
    after_event.side = "sell".to_string();
    after_event.quote_in_amount_raw = Some("50".to_string());
    store.record_execution_quote_canary_event(&after_event)?;

    let missing_priority_signal = CopySignalRow {
        side: "sell".to_string(),
        ..signal("sell-recovery-missing-priority", now + Duration::seconds(1))
    };
    store.insert_copy_signal(&missing_priority_signal)?;
    let mut missing_priority_event = quote_event(
        "quote:close:recovery-missing-priority",
        &missing_priority_signal,
        now + Duration::seconds(1),
        "12000000",
    );
    missing_priority_event.side = "sell".to_string();
    missing_priority_event.quote_in_amount_raw = Some("50".to_string());
    missing_priority_event.priority_fee_status = None;
    missing_priority_event.priority_fee_lamports = None;
    store.record_execution_quote_canary_event(&missing_priority_event)?;

    let retry_events = store
        .list_execution_quote_canary_close_submit_retry_event_ids(now - Duration::hours(1), 10)?;
    let priority_retry_events = store
        .list_execution_quote_canary_close_priority_fee_retry_event_ids(
            now - Duration::hours(1),
            10,
        )?;

    assert_eq!(
        retry_events,
        vec![
            "quote:close:recovery-after-open".to_string(),
            "quote:close:recovery-missing-priority".to_string()
        ]
    );
    assert_eq!(
        priority_retry_events,
        vec!["quote:close:recovery-missing-priority".to_string()]
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
