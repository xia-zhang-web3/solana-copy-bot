use super::*;
use copybot_core_types::TokenQuantity;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[test]
fn market_exit_shadow_quote_contract_runs_without_canary_enabled() {
    let mut config = ExecutionConfig::default();
    config.market_exit_shadow_quote_enabled = true;
    config.market_exit_shadow_quote_batch_limit = 0;

    let error = crate::config_contract::validate_execution_canary_contract(&config)
        .expect_err("market exit diagnostics must validate outside canary mode");

    assert!(error
        .to_string()
        .contains("market_exit_shadow_quote_batch_limit"));
}

#[test]
fn market_exit_shadow_quote_events_do_not_replace_canonical_close_quotes() -> Result<()> {
    let db_path = unique_execution_canary_test_path("market-exit-diag-isolated");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    store.insert_shadow_closed_trade_exact(
        "close-market-diag",
        "leader-wallet",
        "TokenMint",
        12.34,
        Some(TokenQuantity::new(1_234, 2)),
        0.20,
        0.24,
        0.04,
        now - chrono::Duration::minutes(2),
        now - chrono::Duration::seconds(20),
    )?;

    let candidates = store
        .list_execution_quote_canary_close_candidates(now - chrono::Duration::minutes(5), 10)?;
    assert_eq!(candidates.len(), 1);
    let close = candidates[0].clone();

    store.record_execution_quote_canary_event(&sell_quote_event(
        &crate::market_exit_shadow_quote::market_exit_shadow_quote_event_id(close.id),
        None,
        None,
        now,
    ))?;
    let candidates = store
        .list_execution_quote_canary_close_candidates(now - chrono::Duration::minutes(5), 10)?;
    assert_eq!(
        candidates.len(),
        1,
        "diagnostic market exit quote must not block canonical close quote"
    );

    let canonical_event_id = format!("quote:close:{}", close.id);
    store.record_execution_quote_canary_event(&sell_quote_event(
        &canonical_event_id,
        Some(&close.signal_id),
        Some(close.id),
        now,
    ))?;
    let candidates = store
        .list_execution_quote_canary_close_candidates(now - chrono::Duration::minutes(5), 10)?;
    assert!(
        candidates.is_empty(),
        "canonical close quote should block close quote retry"
    );

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn market_exit_shadow_quote_rejects_price_ratio_outlier() -> Result<()> {
    let db_path = unique_execution_canary_test_path("market-exit-diag-ratio-outlier");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    store.insert_shadow_closed_trade_exact(
        "close-market-diag-outlier",
        "leader-wallet",
        "TokenMint",
        1.0,
        Some(TokenQuantity::new(1_000_000, 6)),
        0.20,
        0.20,
        0.0,
        now - chrono::Duration::minutes(2),
        now - chrono::Duration::seconds(20),
    )?;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let quote_base_url = format!("http://{}", listener.local_addr()?);
    let quote_server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("quote request");
        let mut buffer = [0_u8; 2048];
        let _ = socket.read(&mut buffer).await.expect("read quote request");
        let body = r#"{"inAmount":"1000000","outAmount":"20000000000","priceImpactPct":"0.01","routePlan":[]}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        socket
            .write_all(response.as_bytes())
            .await
            .expect("write quote response");
    });

    let mut execution = ExecutionConfig::default();
    execution.market_exit_shadow_quote_enabled = true;
    execution.market_exit_shadow_quote_batch_limit = 5;
    execution.market_exit_shadow_quote_max_close_age_seconds = 300;
    execution.quote_canary_base_url = quote_base_url;
    execution.quote_canary_sell_slippage_bps = 500;
    execution.quote_canary_timeout_ms = 1_000;
    let diagnostic =
        crate::market_exit_shadow_quote::MarketExitShadowQuoteDiagnostic::new(execution);

    let summary = diagnostic.process_tick(&store, now).await?;
    quote_server.await?;

    assert_eq!(summary.checked, 1);
    assert_eq!(summary.inserted, 1);
    assert_eq!(summary.quote_ok, 0);
    assert_eq!(summary.quote_error, 1);

    let conn = Connection::open(&db_path)?;
    let (status, error, quote_price): (String, Option<String>, Option<f64>) = conn.query_row(
        "SELECT quote_status, error, quote_price_sol
         FROM execution_quote_canary_events
         WHERE event_id LIKE 'quote:market-exit-shadow-diag:%'",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    assert_eq!(status, "error");
    assert!(error
        .as_deref()
        .unwrap_or_default()
        .contains("market_exit_quote_ratio_out_of_bounds"));
    assert_eq!(quote_price, None);

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn market_exit_shadow_quote_rejects_mismatched_exact_qty_sidecar() -> Result<()> {
    let db_path = unique_execution_canary_test_path("market-exit-diag-sidecar-outlier");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    store.insert_shadow_closed_trade_exact(
        "close-market-diag-sidecar",
        "leader-wallet",
        "TokenMint",
        1.0,
        Some(TokenQuantity::new(1_000_000, 3)),
        0.20,
        0.20,
        0.0,
        now - chrono::Duration::minutes(2),
        now - chrono::Duration::seconds(20),
    )?;

    let mut execution = ExecutionConfig::default();
    execution.market_exit_shadow_quote_enabled = true;
    execution.market_exit_shadow_quote_batch_limit = 5;
    execution.market_exit_shadow_quote_max_close_age_seconds = 300;
    let diagnostic =
        crate::market_exit_shadow_quote::MarketExitShadowQuoteDiagnostic::new(execution);

    let summary = diagnostic.process_tick(&store, now).await?;

    assert_eq!(summary.checked, 1);
    assert_eq!(summary.inserted, 1);
    assert_eq!(summary.quote_ok, 0);
    assert_eq!(summary.quote_error, 1);

    let conn = Connection::open(&db_path)?;
    let (status, error, quote_price): (String, Option<String>, Option<f64>) = conn.query_row(
        "SELECT quote_status, error, quote_price_sol
         FROM execution_quote_canary_events
         WHERE event_id LIKE 'quote:market-exit-shadow-diag:%'",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    assert_eq!(status, "error");
    assert!(error
        .as_deref()
        .unwrap_or_default()
        .contains("qty_raw/decimals mismatch"));
    assert_eq!(quote_price, None);

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

fn sell_quote_event(
    event_id: &str,
    signal_id: Option<&str>,
    shadow_closed_trade_id: Option<i64>,
    now: chrono::DateTime<Utc>,
) -> copybot_storage_core::ExecutionQuoteCanaryEventInsert {
    copybot_storage_core::ExecutionQuoteCanaryEventInsert {
        event_id: event_id.to_string(),
        signal_id: signal_id.map(ToString::to_string),
        shadow_closed_trade_id,
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
