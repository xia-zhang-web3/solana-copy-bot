use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn fresh_submit_quote_preserves_pump_fun_metadata_before_completion() -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base_url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let mut socket = listener.accept().await.expect("accept quote request").0;
        let mut buf = vec![0; 4096];
        let n = socket.read(&mut buf).await.expect("read quote request");
        let request = String::from_utf8_lossy(&buf[..n]);
        assert!(request.starts_with("GET /pump-fun/quote?"));
        assert!(request.contains("mint=TokenMint"));
        assert!(request.contains("type=BUY"));
        assert!(request.contains("amount=10000000"));
        write_http_json(
            &mut socket,
            r#"{"quote":{"mint":"TokenMint","bondingCurve":"BondingCurve","type":"BUY","inAmount":"10000000","inTokenAddress":"So11111111111111111111111111111111111111112","outAmount":"246912","outTokenAddress":"TokenMint","meta":{"isCompleted":false,"outDecimals":3,"inDecimals":9}}}"#,
        )
        .await;
    });
    let mut config = copybot_config::ExecutionConfig::default();
    config.canary_buy_size_sol = 0.01;
    config.quote_canary_base_url = base_url;
    config.quote_canary_buy_slippage_bps = 1_000;
    config.quote_canary_timeout_ms = 1_000;
    let signal = copybot_core_types::CopySignalRow {
        signal_id: "shadow:test:leader:buy:TokenMint".to_string(),
        wallet_id: "leader".to_string(),
        side: "buy".to_string(),
        token: "TokenMint".to_string(),
        notional_sol: 0.2,
        notional_lamports: Some(copybot_core_types::Lamports::new(200_000_000)),
        notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
        ts: Utc::now(),
        status: "shadow_recorded".to_string(),
    };
    let metadata = crate::execution_submit_adapter::ExecutionBuildPlanMetadata {
        quote_source: Some(
            crate::execution_quote_provider_selection::QUOTE_SOURCE_PUMP_FUN_PAID.to_string(),
        ),
        quote_event_id: Some("quote:entry:shadow:test".to_string()),
        quote_status: Some("ok".to_string()),
        quote_in_amount_raw: Some("10000000".to_string()),
        quote_out_amount_raw: Some("123456".to_string()),
        quote_response_json: Some(
            r#"{"quote":{"outAmount":"123456","meta":{"outDecimals":3}}}"#.to_string(),
        ),
        quote_price_sol: Some(0.081),
        price_impact_pct: Some(0.01),
        route_plan_json: Some(r#"[{"swapInfo":{"label":"Pump.fun Paid"}}]"#.to_string()),
        priority_fee_source: Some("test".to_string()),
        priority_fee_status: Some("ok".to_string()),
        priority_fee_lamports: Some(10_000),
        priority_fee_json: Some(r#"{"recommended":10000}"#.to_string()),
        slippage_bps: Some(125.0),
        decision_status: Some("would_execute".to_string()),
        decision_reason: Some("within_slippage_limit".to_string()),
    };
    let http = reqwest::Client::new();

    let refreshed = crate::execution_build_plan_refresh::refresh_tiny_buy_build_plan_metadata(
        &http, &config, &signal, metadata,
    )
    .await?;
    server.await?;

    assert_eq!(
        refreshed.quote_source.as_deref(),
        Some(crate::execution_quote_provider_selection::QUOTE_SOURCE_PUMP_FUN_PAID)
    );
    assert_eq!(refreshed.quote_out_amount_raw.as_deref(), Some("246912"));
    assert_eq!(
        refreshed.decision_reason.as_deref(),
        Some(crate::execution_build_plan_refresh::FRESH_SUBMIT_QUOTE_WITHIN_SLIPPAGE)
    );
    assert_eq!(refreshed.decision_status.as_deref(), Some("would_execute"));
    assert!(refreshed.slippage_bps.unwrap_or(f64::INFINITY) < 0.0);
    assert!(refreshed
        .quote_response_json
        .as_deref()
        .unwrap_or_default()
        .contains("\"_copybot\":{\"outDecimals\":3}"));

    Ok(())
}

#[tokio::test]
async fn fresh_submit_quote_switches_completed_pump_fun_to_generic_metis() -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base_url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let mut socket = listener.accept().await.expect("accept pump quote").0;
        let mut buf = vec![0; 4096];
        let n = socket.read(&mut buf).await.expect("read pump quote");
        let request = String::from_utf8_lossy(&buf[..n]);
        assert!(request.starts_with("GET /pump-fun/quote?"));
        write_http_json(
            &mut socket,
            r#"{"quote":{"mint":"TokenMint","bondingCurve":"BondingCurve","type":"BUY","inAmount":"10000000","inTokenAddress":"So11111111111111111111111111111111111111112","outAmount":"123456","outTokenAddress":"TokenMint","meta":{"isCompleted":true,"outDecimals":3,"inDecimals":9}}}"#,
        )
        .await;

        let mut socket = listener.accept().await.expect("accept generic quote").0;
        let n = socket.read(&mut buf).await.expect("read generic quote");
        let request = String::from_utf8_lossy(&buf[..n]);
        assert!(request.starts_with("GET /quote?"));
        assert!(request.contains("inputMint=So11111111111111111111111111111111111111112"));
        assert!(request.contains("outputMint=TokenMint"));
        write_http_json(
            &mut socket,
            r#"{"inputMint":"So11111111111111111111111111111111111111112","inAmount":"10000000","outputMint":"TokenMint","outAmount":"246912","otherAmountThreshold":"222220","swapMode":"ExactIn","slippageBps":1000,"platformFee":null,"priceImpactPct":"0.01","routePlan":[{"swapInfo":{"label":"Pump.fun Amm"}}]}"#,
        )
        .await;
    });
    let mut config = copybot_config::ExecutionConfig::default();
    config.canary_buy_size_sol = 0.01;
    config.quote_canary_base_url = base_url;
    config.quote_canary_buy_slippage_bps = 1_000;
    config.quote_canary_timeout_ms = 1_000;
    let signal = copybot_core_types::CopySignalRow {
        signal_id: "shadow:test:leader:buy:TokenMint".to_string(),
        wallet_id: "leader".to_string(),
        side: "buy".to_string(),
        token: "TokenMint".to_string(),
        notional_sol: 0.2,
        notional_lamports: Some(copybot_core_types::Lamports::new(200_000_000)),
        notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
        ts: Utc::now(),
        status: "shadow_recorded".to_string(),
    };
    let http = reqwest::Client::new();

    let refreshed = crate::execution_build_plan_refresh::refresh_tiny_buy_build_plan_metadata(
        &http,
        &config,
        &signal,
        pump_fun_metadata(),
    )
    .await?;
    server.await?;

    assert_eq!(
        refreshed.quote_source.as_deref(),
        Some(crate::execution_quote_provider_selection::QUOTE_SOURCE_GENERIC_METIS)
    );
    assert_eq!(refreshed.quote_out_amount_raw.as_deref(), Some("246912"));
    assert_eq!(refreshed.decision_status.as_deref(), Some("would_execute"));
    Ok(())
}

#[tokio::test]
async fn fresh_submit_quote_preserves_generic_public_metadata() -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let public_url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let mut socket = listener.accept().await.expect("accept public quote").0;
        let mut buf = vec![0; 4096];
        let n = socket.read(&mut buf).await.expect("read public quote");
        let request = String::from_utf8_lossy(&buf[..n]);
        assert!(request.starts_with("GET /quote?"));
        assert!(request.contains("inputMint=So11111111111111111111111111111111111111112"));
        assert!(request.contains("outputMint=TokenMint"));
        assert!(!request.contains("x-api-key"));
        write_http_json(
            &mut socket,
            r#"{"inputMint":"So11111111111111111111111111111111111111112","inAmount":"10000000","outputMint":"TokenMint","outAmount":"246912","otherAmountThreshold":"222220","swapMode":"ExactIn","slippageBps":1000,"platformFee":null,"priceImpactPct":"0.01","routePlan":[{"swapInfo":{"label":"Pump.fun Amm"}}]}"#,
        )
        .await;
    });
    let mut config = copybot_config::ExecutionConfig::default();
    config.canary_buy_size_sol = 0.01;
    config.quote_canary_base_url = "http://127.0.0.1:9".to_string();
    config.quote_canary_public_parallel_enabled = true;
    config.quote_canary_public_base_url = public_url;
    config.quote_canary_buy_slippage_bps = 1_000;
    config.quote_canary_timeout_ms = 1_000;
    let signal = copybot_core_types::CopySignalRow {
        signal_id: "shadow:test:leader:buy:TokenMint".to_string(),
        wallet_id: "leader".to_string(),
        side: "buy".to_string(),
        token: "TokenMint".to_string(),
        notional_sol: 0.2,
        notional_lamports: Some(copybot_core_types::Lamports::new(200_000_000)),
        notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
        ts: Utc::now(),
        status: "shadow_recorded".to_string(),
    };
    let http = reqwest::Client::new();

    let refreshed = crate::execution_build_plan_refresh::refresh_tiny_buy_build_plan_metadata(
        &http,
        &config,
        &signal,
        generic_public_metadata(),
    )
    .await?;
    server.await?;

    assert_eq!(
        refreshed.quote_source.as_deref(),
        Some(crate::execution_quote_provider_selection::QUOTE_SOURCE_GENERIC_PUBLIC)
    );
    assert_eq!(refreshed.quote_out_amount_raw.as_deref(), Some("246912"));
    assert_eq!(refreshed.decision_status.as_deref(), Some("would_execute"));
    Ok(())
}

#[tokio::test]
async fn fresh_submit_quote_retries_pump_fun_when_generic_metis_loses_route() -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base_url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let mut socket = listener.accept().await.expect("accept generic quote").0;
        let mut buf = vec![0; 4096];
        let n = socket.read(&mut buf).await.expect("read generic quote");
        let request = String::from_utf8_lossy(&buf[..n]);
        assert!(request.starts_with("GET /quote?"));
        assert!(request.contains("outputMint=TokenMint"));
        write_http_status_json(
            &mut socket,
            400,
            r#"{"error":"NO_ROUTES_FOUND","message":"No routes found"}"#,
        )
        .await;

        let mut socket = listener.accept().await.expect("accept pump quote").0;
        let n = socket.read(&mut buf).await.expect("read pump quote");
        let request = String::from_utf8_lossy(&buf[..n]);
        assert!(request.starts_with("GET /pump-fun/quote?"));
        assert!(request.contains("mint=TokenMint"));
        assert!(request.contains("type=BUY"));
        assert!(request.contains("amount=10000000"));
        write_http_json(
            &mut socket,
            r#"{"quote":{"mint":"TokenMint","bondingCurve":"BondingCurve","type":"BUY","inAmount":"10000000","inTokenAddress":"So11111111111111111111111111111111111111112","outAmount":"246912","outTokenAddress":"TokenMint","meta":{"isCompleted":false,"outDecimals":3,"inDecimals":9}}}"#,
        )
        .await;
    });
    let mut config = copybot_config::ExecutionConfig::default();
    config.canary_buy_size_sol = 0.01;
    config.quote_canary_base_url = base_url;
    config.quote_canary_pump_fun_parallel_enabled = true;
    config.quote_canary_buy_slippage_bps = 1_000;
    config.quote_canary_timeout_ms = 1_000;
    let signal = copybot_core_types::CopySignalRow {
        signal_id: "shadow:test:leader:buy:TokenMint".to_string(),
        wallet_id: "leader".to_string(),
        side: "buy".to_string(),
        token: "TokenMint".to_string(),
        notional_sol: 0.2,
        notional_lamports: Some(copybot_core_types::Lamports::new(200_000_000)),
        notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
        ts: Utc::now(),
        status: "shadow_recorded".to_string(),
    };
    let mut metadata = generic_public_metadata();
    metadata.quote_source =
        Some(crate::execution_quote_provider_selection::QUOTE_SOURCE_GENERIC_METIS.to_string());
    let http = reqwest::Client::new();

    let refreshed = crate::execution_build_plan_refresh::refresh_tiny_buy_build_plan_metadata(
        &http, &config, &signal, metadata,
    )
    .await?;
    server.await?;

    assert_eq!(
        refreshed.quote_source.as_deref(),
        Some(crate::execution_quote_provider_selection::QUOTE_SOURCE_PUMP_FUN_PAID)
    );
    assert_eq!(refreshed.quote_status.as_deref(), Some("ok"));
    assert_eq!(refreshed.quote_out_amount_raw.as_deref(), Some("246912"));
    assert_eq!(refreshed.decision_status.as_deref(), Some("would_execute"));
    assert_eq!(
        refreshed.decision_reason.as_deref(),
        Some(crate::execution_build_plan_refresh::FRESH_SUBMIT_QUOTE_WITHIN_SLIPPAGE)
    );
    Ok(())
}

async fn write_http_json(socket: &mut tokio::net::TcpStream, body: &str) {
    write_http_status_json(socket, 200, body).await;
}

async fn write_http_status_json(socket: &mut tokio::net::TcpStream, status: u16, body: &str) {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        _ => "OK",
    };
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        body.len(), body
    );
    socket
        .write_all(response.as_bytes())
        .await
        .expect("write quote response");
}

fn pump_fun_metadata() -> crate::execution_submit_adapter::ExecutionBuildPlanMetadata {
    crate::execution_submit_adapter::ExecutionBuildPlanMetadata {
        quote_source: Some(
            crate::execution_quote_provider_selection::QUOTE_SOURCE_PUMP_FUN_PAID.to_string(),
        ),
        quote_event_id: Some("quote:entry:shadow:test".to_string()),
        quote_status: Some("ok".to_string()),
        quote_in_amount_raw: Some("10000000".to_string()),
        quote_out_amount_raw: Some("123456".to_string()),
        quote_response_json: Some(
            r#"{"quote":{"outAmount":"123456","meta":{"outDecimals":3}}}"#.to_string(),
        ),
        quote_price_sol: Some(0.081),
        price_impact_pct: Some(0.01),
        route_plan_json: Some(r#"[{"swapInfo":{"label":"Pump.fun Paid"}}]"#.to_string()),
        priority_fee_source: Some("test".to_string()),
        priority_fee_status: Some("ok".to_string()),
        priority_fee_lamports: Some(10_000),
        priority_fee_json: Some(r#"{"recommended":10000}"#.to_string()),
        slippage_bps: Some(125.0),
        decision_status: Some("would_execute".to_string()),
        decision_reason: Some("within_slippage_limit".to_string()),
    }
}

fn generic_public_metadata() -> crate::execution_submit_adapter::ExecutionBuildPlanMetadata {
    let mut metadata = pump_fun_metadata();
    metadata.quote_source =
        Some(crate::execution_quote_provider_selection::QUOTE_SOURCE_GENERIC_PUBLIC.to_string());
    metadata.route_plan_json = Some(r#"[{"swapInfo":{"label":"Pump.fun Amm"}}]"#.to_string());
    metadata.quote_response_json = Some(
        r#"{"inputMint":"So11111111111111111111111111111111111111112","inAmount":"10000000","outputMint":"TokenMint","outAmount":"123456","otherAmountThreshold":"117283","swapMode":"ExactIn","slippageBps":500,"platformFee":null,"priceImpactPct":"0.01","routePlan":[{"swapInfo":{"label":"Pump.fun Amm"}}],"_copybot":{"outDecimals":3}}"#.to_string(),
    );
    metadata
}
