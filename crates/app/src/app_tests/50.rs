use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn quote_canary_uses_public_quote_when_paid_generic_is_not_tradable() -> Result<()> {
    let (store, db_path) = make_test_store("quote-provider-public-fallback")?;
    let primary_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let public_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let primary_url = format!("http://{}", primary_listener.local_addr()?);
    let public_url = format!("http://{}", public_listener.local_addr()?);
    let primary_server = tokio::spawn(async move {
        for _ in 0..4 {
            let request = read_provider_fallback_request(&primary_listener).await;
            assert!(request.body.starts_with("GET /quote?"));
            write_provider_fallback_status(
                request.socket,
                400,
                r#"{"error":"The token TokenMint is not tradable","errorCode":"TOKEN_NOT_TRADABLE"}"#,
            )
            .await;
        }
    });
    let public_server = tokio::spawn(async move {
        let request = read_provider_fallback_request(&public_listener).await;
        assert!(request.body.starts_with("GET /quote?"));
        write_provider_fallback_status(
            request.socket,
            200,
            r#"{"inAmount":"200000000","outAmount":"1000000","priceImpactPct":"0.01","routePlan":[{"swapInfo":{"label":"Pump.fun Amm"}}]}"#,
        )
        .await;
    });

    let now = Utc::now();
    let signal = provider_fallback_signal(now);
    store.insert_observed_swap(&provider_fallback_observed_swap(now))?;
    store.insert_copy_signal(&signal)?;
    let mut config = ExecutionConfig::default();
    config.quote_canary_enabled = true;
    config.quote_canary_base_url = primary_url;
    config.quote_canary_public_parallel_enabled = true;
    config.quote_canary_public_base_url = public_url;
    config.quote_canary_buy_size_sol = 0.2;
    config.quote_canary_buy_slippage_bps = 500;
    config.quote_canary_timeout_ms = 1_000;
    let runner = crate::execution_quote_canary::ExecutionQuoteCanaryRunner::new(config);

    let summary = runner
        .process_recorded_shadow_signal(
            &store,
            &copybot_shadow::ShadowSignalResult {
                signal_id: signal.signal_id.clone(),
                wallet_id: signal.wallet_id.clone(),
                side: "buy".to_string(),
                token: signal.token.clone(),
                notional_sol: signal.notional_sol,
                latency_ms: 25,
                closed_qty: 0.0,
                realized_pnl_sol: 0.0,
                has_open_lots_after_signal: Some(true),
            },
            now,
        )
        .await?;
    primary_server.await?;
    public_server.await?;

    assert_eq!(summary.entry_inserted, 1);
    assert_eq!(summary.would_execute, 1);
    let event = store
        .load_latest_execution_quote_canary_entry_event(&signal.signal_id)?
        .expect("entry quote");
    assert_eq!(event.quote_status, "ok");
    assert_eq!(event.decision_status.as_deref(), Some("would_execute"));
    assert_eq!(event.quote_out_amount_raw.as_deref(), Some("1000000"));
    let generic = store
        .load_execution_quote_canary_provider_sample(
            &event.event_id,
            copybot_storage_core::PROVIDER_GENERIC_METIS,
        )?
        .expect("generic metis sample");
    let public = store
        .load_execution_quote_canary_provider_sample(
            &event.event_id,
            copybot_storage_core::PROVIDER_GENERIC_PUBLIC,
        )?
        .expect("public sample");
    assert_eq!(generic.quote_status, "error");
    assert_eq!(public.quote_status, "ok");

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn quote_canary_sell_uses_public_quote_when_paid_generic_is_not_tradable() -> Result<()> {
    let (store, db_path) = make_test_store("quote-provider-public-fallback-sell")?;
    let primary_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let public_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let primary_url = format!("http://{}", primary_listener.local_addr()?);
    let public_url = format!("http://{}", public_listener.local_addr()?);
    let primary_server = tokio::spawn(async move {
        for _ in 0..4 {
            let request = read_provider_fallback_request(&primary_listener).await;
            assert!(request.body.starts_with("GET /quote?"));
            write_provider_fallback_status(
                request.socket,
                400,
                r#"{"error":"The token TokenMint is not tradable","errorCode":"TOKEN_NOT_TRADABLE"}"#,
            )
            .await;
        }
    });
    let public_server = tokio::spawn(async move {
        let request = read_provider_fallback_request(&public_listener).await;
        assert!(request.body.starts_with("GET /quote?"));
        write_provider_fallback_status(
            request.socket,
            200,
            r#"{"inAmount":"1000000","outAmount":"190000000","priceImpactPct":"0.01","routePlan":[{"swapInfo":{"label":"Pump.fun Amm"}}]}"#,
        )
        .await;
    });

    let now = Utc::now();
    let signal_id = "shadow:sig-provider-fallback-sell:leader-wallet:sell:TokenMint";
    store.insert_shadow_closed_trade_exact(
        signal_id,
        "leader-wallet",
        "TokenMint",
        1.0,
        Some(copybot_core_types::TokenQuantity::new(1_000_000, 6)),
        0.2,
        0.19,
        -0.01,
        now - chrono::Duration::minutes(3),
        now - chrono::Duration::milliseconds(25),
    )?;
    let mut config = ExecutionConfig::default();
    config.quote_canary_enabled = true;
    config.quote_canary_base_url = primary_url;
    config.quote_canary_public_parallel_enabled = true;
    config.quote_canary_public_base_url = public_url;
    config.quote_canary_sell_slippage_bps = 500;
    config.quote_canary_timeout_ms = 1_000;
    let runner = crate::execution_quote_canary::ExecutionQuoteCanaryRunner::new(config);

    let summary = runner
        .process_recorded_shadow_signal(
            &store,
            &copybot_shadow::ShadowSignalResult {
                signal_id: signal_id.to_string(),
                wallet_id: "leader-wallet".to_string(),
                side: "sell".to_string(),
                token: "TokenMint".to_string(),
                notional_sol: 0.19,
                latency_ms: 25,
                closed_qty: 1.0,
                realized_pnl_sol: -0.01,
                has_open_lots_after_signal: Some(false),
            },
            now,
        )
        .await?;
    primary_server.await?;
    public_server.await?;

    assert_eq!(summary.close_inserted, 1);
    assert_eq!(summary.would_execute, 1);
    let event = store
        .execution_quote_canary_provider_selection_summary(
            now,
            now - chrono::Duration::seconds(1),
            10,
        )?
        .latest
        .into_iter()
        .find(|event| event.side == "sell")
        .expect("sell quote event");
    assert_eq!(
        event.selected_provider,
        copybot_storage_core::PROVIDER_GENERIC_PUBLIC
    );
    assert_eq!(event.generic_metis_status.as_deref(), Some("error"));
    assert_eq!(event.generic_public_status.as_deref(), Some("ok"));

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

struct ProviderFallbackRequest {
    socket: tokio::net::TcpStream,
    body: String,
}

async fn read_provider_fallback_request(
    listener: &tokio::net::TcpListener,
) -> ProviderFallbackRequest {
    let (mut socket, _) = listener.accept().await.expect("http request");
    let mut buffer = [0_u8; 4096];
    let read = socket.read(&mut buffer).await.expect("read request");
    ProviderFallbackRequest {
        socket,
        body: String::from_utf8_lossy(&buffer[..read]).to_string(),
    }
}

async fn write_provider_fallback_status(
    mut socket: tokio::net::TcpStream,
    status: u16,
    body: &str,
) {
    let response = format!(
        "HTTP/1.1 {status} OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    socket
        .write_all(response.as_bytes())
        .await
        .expect("write response");
}

fn provider_fallback_signal(now: chrono::DateTime<Utc>) -> copybot_core_types::CopySignalRow {
    copybot_core_types::CopySignalRow {
        signal_id: "shadow:sig-provider-fallback:leader-wallet:buy:TokenMint".to_string(),
        wallet_id: "leader-wallet".to_string(),
        side: "buy".to_string(),
        token: "TokenMint".to_string(),
        notional_sol: 0.2,
        notional_lamports: Some(Lamports::new(200_000_000)),
        notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
        ts: now - chrono::Duration::milliseconds(25),
        status: "shadow_recorded".to_string(),
    }
}

fn provider_fallback_observed_swap(now: chrono::DateTime<Utc>) -> copybot_core_types::SwapEvent {
    copybot_core_types::SwapEvent {
        wallet: "leader-wallet".to_string(),
        dex: "pumpfun".to_string(),
        token_in: crate::execution_quote_canary_helpers::SOL_MINT.to_string(),
        token_out: "TokenMint".to_string(),
        amount_in: 0.2,
        amount_out: 1.0,
        signature: "sig-provider-fallback".to_string(),
        slot: 42,
        ts_utc: now - chrono::Duration::milliseconds(25),
        exact_amounts: Some(copybot_core_types::ExactSwapAmounts {
            amount_in_raw: "200000000".to_string(),
            amount_in_decimals: 9,
            amount_out_raw: "1000000".to_string(),
            amount_out_decimals: 6,
        }),
    }
}
