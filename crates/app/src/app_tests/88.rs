use super::*;
use chrono::{DateTime, Utc};
use copybot_core_types::TokenQuantity;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn stale_lot_cleanup_materializes_lossy_quote_for_open_canary_position() -> Result<()> {
    let (store, db_path) = make_test_store("stale-lot-quote-loss-canary-materialize")?;
    let now = DateTime::parse_from_rfc3339("2026-03-10T12:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    let opened_ts = now - chrono::Duration::hours(8);
    let lot_id = store.insert_shadow_lot_exact(
        "wallet-a",
        "token-a",
        0.5,
        Some(TokenQuantity::new(500_000, 6)),
        0.25,
        opened_ts,
    )?;
    store.record_execution_canary_open_position(
        "confirmed-canary-buy-order",
        "token-a",
        0.01,
        Some(TokenQuantity::new(10_000, 6)),
        0.01,
        opened_ts + chrono::Duration::seconds(5),
    )?;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let quote_base_url = format!("http://{}", listener.local_addr()?);
    let quote_server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("quote request");
        let mut buffer = [0_u8; 2048];
        let _ = socket.read(&mut buffer).await.expect("read quote request");
        let body = r#"{"inAmount":"500000","outAmount":"100000000","priceImpactPct":"0.01","routePlan":[]}"#;
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
    execution.quote_canary_enabled = true;
    execution.quote_canary_base_url = quote_base_url;
    execution.quote_canary_sell_slippage_bps = 500;
    execution.quote_canary_timeout_ms = 1_000;
    let quote_pricer = StaleCloseQuotePricer::new(execution);

    let mut open_pairs = store.list_shadow_open_pairs()?;
    let stats = close_stale_shadow_lots(
        &store,
        &mut open_pairs,
        6,
        12,
        false,
        true,
        Some(&quote_pricer),
        now,
    )
    .await?;
    quote_server.await?;

    assert_eq!(stats.quote_closed, 1);
    assert_eq!(stats.quote_loss_deferred, 0);
    assert_eq!(stats.terminal_zero_closed, 0);
    assert!(!store.has_shadow_lots("wallet-a", "token-a")?);
    assert!(!open_pairs.contains(&("wallet-a".to_string(), "token-a".to_string())));
    assert_eq!(
        store.risk_event_count_by_type("shadow_stale_close_quote_loss_deferred")?,
        0
    );
    assert_eq!(
        store.risk_event_count_by_type("shadow_stale_close_quote_price")?,
        1
    );

    let signal_id = format!("stale-close-{}-{}", lot_id, now.timestamp_millis());
    assert_eq!(
        store.shadow_closed_trade_close_context(&signal_id)?,
        Some(copybot_storage_core::SHADOW_CLOSE_CONTEXT_STALE_QUOTE_PRICE.to_string())
    );
    let candidates = store.list_execution_quote_canary_owned_stale_close_candidates(
        now - chrono::Duration::minutes(1),
        10,
    )?;
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].signal_id, signal_id);
    assert_eq!(candidates[0].token, "token-a");

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn stale_lot_cleanup_skips_mismatched_exact_qty_sidecar_as_outlier() -> Result<()> {
    let (store, db_path) = make_test_store("stale-lot-quote-sidecar-outlier")?;
    let now = DateTime::parse_from_rfc3339("2026-03-10T12:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    let opened_ts = now - chrono::Duration::hours(8);
    store.insert_shadow_lot_exact(
        "wallet-a",
        "token-a",
        0.5,
        Some(TokenQuantity::new(500_000, 3)),
        0.25,
        opened_ts,
    )?;

    let mut execution = ExecutionConfig::default();
    execution.quote_canary_enabled = true;
    execution.quote_canary_timeout_ms = 1_000;
    let quote_pricer = StaleCloseQuotePricer::new(execution);

    let mut open_pairs = store.list_shadow_open_pairs()?;
    let stats = close_stale_shadow_lots(
        &store,
        &mut open_pairs,
        6,
        12,
        false,
        false,
        Some(&quote_pricer),
        now,
    )
    .await?;

    assert_eq!(stats.quote_closed, 0);
    assert_eq!(stats.terminal_zero_closed, 0);
    assert_eq!(stats.skipped_unpriced, 1);
    assert!(store.has_shadow_lots("wallet-a", "token-a")?);
    assert_eq!(
        store.risk_event_count_by_type("shadow_stale_close_quote_outlier")?,
        1
    );
    let (trades, _) = store.shadow_realized_pnl_since(now - chrono::Duration::days(1))?;
    assert_eq!(trades, 0);

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn stale_lot_cleanup_skips_absurd_high_quote_as_outlier_not_zero() -> Result<()> {
    let (store, db_path) = make_test_store("stale-lot-quote-high-outlier")?;
    let now = DateTime::parse_from_rfc3339("2026-03-10T12:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    let opened_ts = now - chrono::Duration::hours(20);
    store.insert_shadow_lot_exact(
        "wallet-a",
        "token-a",
        0.5,
        Some(TokenQuantity::new(500_000, 6)),
        0.25,
        opened_ts,
    )?;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let quote_base_url = format!("http://{}", listener.local_addr()?);
    let quote_server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("quote request");
        let mut buffer = [0_u8; 2048];
        let _ = socket.read(&mut buffer).await.expect("read quote request");
        let body = r#"{"inAmount":"500000","outAmount":"20000000000","priceImpactPct":"0.01","routePlan":[]}"#;
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
    execution.quote_canary_enabled = true;
    execution.quote_canary_base_url = quote_base_url;
    execution.quote_canary_sell_slippage_bps = 500;
    execution.quote_canary_timeout_ms = 1_000;
    let quote_pricer = StaleCloseQuotePricer::new(execution);

    let mut open_pairs = store.list_shadow_open_pairs()?;
    let stats = close_stale_shadow_lots(
        &store,
        &mut open_pairs,
        6,
        12,
        false,
        false,
        Some(&quote_pricer),
        now,
    )
    .await?;
    quote_server.await?;

    assert_eq!(stats.quote_closed, 0);
    assert_eq!(stats.terminal_zero_closed, 0);
    assert_eq!(stats.skipped_unpriced, 1);
    assert!(store.has_shadow_lots("wallet-a", "token-a")?);
    assert_eq!(
        store.risk_event_count_by_type("shadow_stale_close_quote_outlier")?,
        1
    );
    let (trades, _) = store.shadow_realized_pnl_since(now - chrono::Duration::days(1))?;
    assert_eq!(trades, 0);

    let _ = std::fs::remove_file(db_path);
    Ok(())
}
