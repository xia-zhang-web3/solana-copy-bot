use anyhow::Result;
use chrono::{Duration, Utc};
use copybot_core_types::{
    CopySignalRow, Lamports, TokenQuantity, COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS,
};
use copybot_storage_core::{ExecutionQuoteCanaryEventInsert, SqliteStore};

#[test]
fn tagged_cu_price_survives_sqlite_without_total_and_stops_both_retry_selectors() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let mut store = SqliteStore::open(dir.path().join("fee.db"))?;
    store.run_migrations(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    store.record_execution_canary_open_position(
        "recovery-orphan:fee",
        "TokenMint",
        1.0,
        Some(TokenQuantity::new(1, 0)),
        0.01,
        now - Duration::minutes(1),
    )?;
    let tagged = r#"{"version":1,"source":"qn_estimatePriorityFees","api_version":2,"field":"recommended","unit":"micro_lamports_per_compute_unit","value":600000}"#;
    for side in ["buy", "sell"] {
        let signal = CopySignalRow {
            signal_id: side.into(),
            wallet_id: "leader".into(),
            side: side.into(),
            token: "TokenMint".into(),
            notional_sol: 0.01,
            notional_lamports: Some(Lamports::new(10_000_000)),
            notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.into(),
            ts: now,
            status: "shadow_recorded".into(),
        };
        store.insert_copy_signal(&signal)?;
        let id = format!("quote:entry:{side}");
        store.record_execution_quote_canary_event(&event(&id, &signal))?;
        let before = if side == "buy" {
            store
                .list_execution_quote_canary_entry_priority_fee_retry_candidates(
                    "shadow_recorded",
                    now,
                    10,
                )?
                .len()
        } else {
            store
                .list_execution_quote_canary_close_priority_fee_retry_event_ids(now, 10)?
                .len()
        };
        assert_eq!(before, 1);
        assert!(store.mark_execution_quote_canary_priority_fee_ok(&id, None, Some(tagged))?);
        let stored = store.load_execution_quote_canary_event_by_id(&id)?.unwrap();
        assert_eq!(stored.priority_fee_lamports, None);
        assert_eq!(stored.priority_fee_status.as_deref(), Some("ok"));
        assert_eq!(stored.priority_fee_json.as_deref(), Some(tagged));
        assert!(stored.error.is_none());
        let after = if side == "buy" {
            store
                .list_execution_quote_canary_entry_priority_fee_retry_candidates(
                    "shadow_recorded",
                    now,
                    10,
                )?
                .len()
        } else {
            store
                .list_execution_quote_canary_close_priority_fee_retry_event_ids(now, 10)?
                .len()
        };
        assert_eq!(after, 0);
        assert!(store
            .mark_execution_quote_canary_priority_fee_ok(&id, Some(u64::MAX), Some(tagged))
            .is_err());
        assert_eq!(
            store
                .load_execution_quote_canary_event_by_id(&id)?
                .unwrap()
                .priority_fee_lamports,
            None
        );
    }
    Ok(())
}

#[test]
fn historical_untagged_sample_is_not_relabelled_by_storage_or_lookup() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let mut store = SqliteStore::open(dir.path().join("legacy.db"))?;
    store.run_migrations(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let signal = CopySignalRow {
        signal_id: "old".into(),
        wallet_id: "leader".into(),
        side: "buy".into(),
        token: "TokenMint".into(),
        notional_sol: 0.01,
        notional_lamports: Some(Lamports::new(10_000_000)),
        notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.into(),
        ts: Utc::now(),
        status: "shadow_recorded".into(),
    };
    store.insert_copy_signal(&signal)?;
    let mut old = event("quote:entry:old", &signal);
    old.priority_fee_status = Some("ok".into());
    old.priority_fee_lamports = Some(600000);
    old.priority_fee_json = Some(r#"{"recommended":600000}"#.into());
    store.record_execution_quote_canary_event(&old)?;
    assert!(store
        .list_execution_quote_canary_entry_priority_fee_retry_candidates(
            "shadow_recorded",
            signal.ts,
            10
        )?
        .is_empty());
    let loaded = store
        .load_execution_quote_canary_event_by_id(&old.event_id)?
        .unwrap();
    assert_eq!(loaded.priority_fee_json, old.priority_fee_json);
    assert_eq!(loaded.priority_fee_lamports, old.priority_fee_lamports);
    Ok(())
}

fn event(id: &str, signal: &CopySignalRow) -> ExecutionQuoteCanaryEventInsert {
    ExecutionQuoteCanaryEventInsert {
        http_request_started_ts: None,
        quote_response_available_ts: None,
        event_id: id.into(),
        signal_id: Some(signal.signal_id.clone()),
        shadow_closed_trade_id: None,
        wallet_id: signal.wallet_id.clone(),
        token: signal.token.clone(),
        side: signal.side.clone(),
        quote_status: "ok".into(),
        request_ts: signal.ts,
        signal_ts: Some(signal.ts),
        decision_delay_ms: None,
        quote_latency_ms: None,
        leader_notional_sol: None,
        quote_in_amount_raw: Some("10000000".into()),
        quote_out_amount_raw: Some("1".into()),
        quote_response_json: None,
        quote_price_sol: Some(0.01),
        shadow_price_sol: None,
        slippage_bps: Some(0.0),
        price_impact_pct: None,
        route_plan_json: Some("[]".into()),
        priority_fee_status: Some("skipped".into()),
        priority_fee_lamports: None,
        priority_fee_json: Some("{".into()),
        decision_status: Some("would_execute".into()),
        decision_reason: None,
        error: Some("transient fee error".into()),
    }
}
