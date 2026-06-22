use super::*;

#[test]
fn entry_quote_shadow_diagnostic_contract_runs_without_canary_enabled() {
    let mut config = ExecutionConfig::default();
    config.entry_quote_shadow_diagnostic_enabled = true;
    config.entry_quote_shadow_diagnostic_batch_limit = 0;

    let error = crate::config_contract::validate_execution_canary_contract(&config)
        .expect_err("entry quote diagnostics must validate outside canary mode");

    assert!(error
        .to_string()
        .contains("entry_quote_shadow_diagnostic_batch_limit"));
}

#[test]
fn entry_quote_shadow_diagnostic_events_do_not_replace_canonical_entry_quotes() -> Result<()> {
    let db_path = unique_execution_canary_test_path("entry-diag-isolated");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let signal = copybot_core_types::CopySignalRow {
        signal_id: "shadow:sig-entry-diag:leader-wallet:buy:TokenMint".to_string(),
        wallet_id: "leader-wallet".to_string(),
        side: "buy".to_string(),
        token: "TokenMint".to_string(),
        notional_sol: 0.2,
        notional_lamports: Some(Lamports::new(200_000_000)),
        notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
        ts: now,
        status: "shadow_recorded".to_string(),
    };
    store.insert_copy_signal(&signal)?;

    store.record_execution_quote_canary_event(&quote_event(
        &signal,
        &crate::entry_quote_shadow_diagnostic::entry_quote_shadow_diagnostic_event_id(
            &signal.signal_id,
        ),
        now,
    ))?;

    assert!(
        store
            .load_latest_execution_quote_canary_entry_event(&signal.signal_id)?
            .is_none(),
        "diagnostic quote must not be loaded as canonical entry quote"
    );
    let candidates = store.list_execution_quote_canary_entry_candidates(
        "shadow_recorded",
        now - chrono::Duration::seconds(1),
        10,
    )?;
    assert_eq!(
        candidates.len(),
        1,
        "diagnostic quote must not block canary quote"
    );

    let canonical_event_id = format!("quote:entry:{}", signal.signal_id);
    store.record_execution_quote_canary_event(&quote_event(&signal, &canonical_event_id, now))?;
    let loaded = store
        .load_latest_execution_quote_canary_entry_event(&signal.signal_id)?
        .expect("canonical quote should load");
    assert_eq!(loaded.event_id, canonical_event_id);

    let candidates = store.list_execution_quote_canary_entry_candidates(
        "shadow_recorded",
        now - chrono::Duration::seconds(1),
        10,
    )?;
    assert!(
        candidates.is_empty(),
        "canonical quote should block canary quote retry"
    );

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

fn quote_event(
    signal: &copybot_core_types::CopySignalRow,
    event_id: &str,
    now: chrono::DateTime<Utc>,
) -> copybot_storage_core::ExecutionQuoteCanaryEventInsert {
    copybot_storage_core::ExecutionQuoteCanaryEventInsert {
        event_id: event_id.to_string(),
        signal_id: Some(signal.signal_id.clone()),
        shadow_closed_trade_id: None,
        wallet_id: signal.wallet_id.clone(),
        token: signal.token.clone(),
        side: "buy".to_string(),
        quote_status: "ok".to_string(),
        request_ts: now,
        signal_ts: Some(signal.ts),
        decision_delay_ms: Some(0),
        quote_latency_ms: Some(10),
        leader_notional_sol: Some(signal.notional_sol),
        quote_in_amount_raw: Some("200000000".to_string()),
        quote_out_amount_raw: Some("1000000".to_string()),
        quote_response_json: Some("{\"routePlan\":[]}".to_string()),
        quote_price_sol: Some(0.0002),
        shadow_price_sol: Some(0.0002),
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
