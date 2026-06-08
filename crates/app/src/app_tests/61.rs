use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn rpc_confirmation_boundary_pending_keeps_buy_unfilled() -> Result<()> {
    let db_path = unique_rpc_boundary_test_path("pending-buy");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let order_id = submitted_rpc_boundary_order(&store, "pending-buy", "buy", "tx-pending", now)?;
    let rpc_url = serve_rpc_boundary_response(
        "tx-pending",
        r#"{"jsonrpc":"2.0","id":"execution-confirmation","result":{"value":[null]}}"#,
    )
    .await?;

    let outcome: crate::execution_submit_adapter::ExecutionConfirmationBoundaryOutcome =
        crate::execution_submit_adapter::record_execution_rpc_confirmation_boundary(
            &store,
            &reqwest::Client::new(),
            &rpc_url,
            &order_id,
            boundary_buy_fill(&order_id, now + chrono::Duration::seconds(8)),
            now + chrono::Duration::seconds(7),
            1_000,
        )
        .await?;
    let order = store
        .load_execution_canary_order(&order_id)?
        .expect("order should exist");

    assert_eq!(outcome.pending, 1);
    assert_eq!(outcome.confirmed, 0);
    assert_eq!(
        outcome.reason.as_deref(),
        Some("rpc_signature_status_missing")
    );
    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_SUBMITTED
    );
    assert_eq!(store.execution_canary_open_position_count()?, 0);

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn rpc_confirmation_boundary_confirmed_buy_opens_fill() -> Result<()> {
    let db_path = unique_rpc_boundary_test_path("confirmed-buy");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let order_id = submitted_rpc_boundary_order(&store, "confirmed-buy", "buy", "tx-buy", now)?;
    let rpc_url = serve_rpc_boundary_response(
        "tx-buy",
        r#"{"jsonrpc":"2.0","id":"execution-confirmation","result":{"context":{"slot":600},"value":[{"slot":601,"confirmations":null,"err":null,"confirmationStatus":"finalized"}]}}"#,
    )
    .await?;

    let outcome = crate::execution_submit_adapter::record_execution_rpc_confirmation_boundary(
        &store,
        &reqwest::Client::new(),
        &rpc_url,
        &order_id,
        boundary_buy_fill(&order_id, now + chrono::Duration::seconds(8)),
        now + chrono::Duration::seconds(7),
        1_000,
    )
    .await?;
    let order = store
        .load_execution_canary_order(&order_id)?
        .expect("order should exist");

    assert_eq!(outcome.confirmed, 1);
    assert_eq!(outcome.pending, 0);
    assert_eq!(outcome.confirmation_status.as_deref(), Some("finalized"));
    assert_eq!(outcome.slot, Some(601));
    assert_eq!(outcome.buy_opened, 1);
    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_CONFIRMED
    );
    assert_eq!(order.confirm_ts, Some(now + chrono::Duration::seconds(7)));
    assert_eq!(store.execution_canary_open_position_count()?, 1);

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn rpc_confirmation_boundary_confirmed_sell_closes_confirmed_position() -> Result<()> {
    let db_path = unique_rpc_boundary_test_path("confirmed-sell");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let buy_order_id = submitted_rpc_boundary_order(&store, "owned-buy", "buy", "tx-buy", now)?;
    store.mark_execution_canary_confirmed(&buy_order_id, now + chrono::Duration::seconds(4))?;
    crate::execution_submit_adapter::record_confirmed_fill_accounting(
        &store,
        boundary_buy_fill(&buy_order_id, now + chrono::Duration::seconds(5)),
    )?;
    let sell_order_id = submitted_rpc_boundary_order(
        &store,
        "owned-sell",
        "sell",
        "tx-sell",
        now + chrono::Duration::minutes(1),
    )?;
    let rpc_url = serve_rpc_boundary_response(
        "tx-sell",
        r#"{"jsonrpc":"2.0","id":"execution-confirmation","result":{"value":[{"slot":777,"confirmations":null,"err":null,"confirmationStatus":"confirmed"}]}}"#,
    )
    .await?;

    let outcome = crate::execution_submit_adapter::record_execution_rpc_confirmation_boundary(
        &store,
        &reqwest::Client::new(),
        &rpc_url,
        &sell_order_id,
        boundary_sell_fill(
            &sell_order_id,
            now + chrono::Duration::minutes(1) + chrono::Duration::seconds(8),
        ),
        now + chrono::Duration::minutes(1) + chrono::Duration::seconds(7),
        1_000,
    )
    .await?;

    assert_eq!(outcome.confirmed, 1);
    assert_eq!(outcome.confirmation_status.as_deref(), Some("confirmed"));
    assert_eq!(outcome.sell_closed, 1);
    assert_eq!(outcome.sell_no_position, 0);
    assert_eq!(outcome.closed_qty, 10.0);
    assert!((outcome.pnl_sol - 0.2).abs() < 1e-9);
    assert!(store
        .load_execution_canary_open_position("TokenMint")?
        .is_none());

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn rpc_confirmation_boundary_transaction_error_leaves_fill_unmodified() -> Result<()> {
    let db_path = unique_rpc_boundary_test_path("tx-error");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let order_id = submitted_rpc_boundary_order(&store, "tx-error", "buy", "tx-error", now)?;
    let rpc_url = serve_rpc_boundary_response(
        "tx-error",
        r#"{"jsonrpc":"2.0","id":"execution-confirmation","result":{"value":[{"slot":700,"confirmations":null,"err":{"InstructionError":[0,"Custom"]},"confirmationStatus":"finalized"}]}}"#,
    )
    .await?;

    let error = crate::execution_submit_adapter::record_execution_rpc_confirmation_boundary(
        &store,
        &reqwest::Client::new(),
        &rpc_url,
        &order_id,
        boundary_buy_fill(&order_id, now + chrono::Duration::seconds(8)),
        now + chrono::Duration::seconds(7),
        1_000,
    )
    .await
    .expect_err("transaction error must not confirm fill accounting");
    let order = store
        .load_execution_canary_order(&order_id)?
        .expect("order should exist");

    assert!(format!("{error:#}").contains("confirmation RPC transaction_error"));
    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_SUBMITTED
    );
    assert_eq!(store.execution_canary_open_position_count()?, 0);

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn tiny_submit_reconciliation_confirmed_buy_is_idempotent() -> Result<()> {
    let db_path = unique_rpc_boundary_test_path("reconcile-buy");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let order_id = submitted_rpc_boundary_order(&store, "reconcile-buy", "buy", "tx-buy", now)?;
    record_rpc_boundary_build_metadata(&store, &order_id, now, 0.1)?;
    let rpc_url = serve_rpc_boundary_response(
        "tx-buy",
        r#"{"jsonrpc":"2.0","id":"execution-confirmation","result":{"value":[{"slot":900,"confirmations":null,"err":null,"confirmationStatus":"finalized"}]}}"#,
    )
    .await?;

    let first = crate::execution_submit_adapter::reconcile_execution_tiny_submit_confirmation(
        &store,
        &rpc_boundary_reconcile_config(),
        &order_id,
        &reqwest::Client::new(),
        &rpc_url,
        now + chrono::Duration::seconds(7),
        1_000,
    )
    .await?;
    let second = crate::execution_submit_adapter::reconcile_execution_tiny_submit_confirmation(
        &store,
        &rpc_boundary_reconcile_config(),
        &order_id,
        &reqwest::Client::new(),
        "",
        now + chrono::Duration::seconds(8),
        1_000,
    )
    .await?;

    assert_eq!(first.confirmation_confirmed, 1);
    assert_eq!(first.buy_opened, 1);
    assert_eq!(second.confirmation_confirmed, 1);
    assert_eq!(second.buy_opened, 0);
    assert_eq!(
        second.reason.as_deref(),
        Some("submitted_reconcile_already_confirmed")
    );
    assert_eq!(store.execution_canary_open_position_count()?, 1);

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn tiny_submit_reconciliation_confirmed_sell_is_idempotent() -> Result<()> {
    let db_path = unique_rpc_boundary_test_path("reconcile-sell");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    store.record_execution_canary_open_position(
        "existing-confirmed-buy",
        "TokenMint",
        10.0,
        Some(copybot_core_types::TokenQuantity::new(10_000, 3)),
        0.8,
        now,
    )?;
    let order_id = submitted_rpc_boundary_order(
        &store,
        "reconcile-sell",
        "sell",
        "tx-sell",
        now + chrono::Duration::minutes(1),
    )?;
    record_rpc_boundary_build_metadata(&store, &order_id, now, 0.12)?;
    let rpc_url = serve_rpc_boundary_response(
        "tx-sell",
        r#"{"jsonrpc":"2.0","id":"execution-confirmation","result":{"value":[{"slot":901,"confirmations":null,"err":null,"confirmationStatus":"confirmed"}]}}"#,
    )
    .await?;

    let first = crate::execution_submit_adapter::reconcile_execution_tiny_submit_confirmation(
        &store,
        &rpc_boundary_reconcile_config(),
        &order_id,
        &reqwest::Client::new(),
        &rpc_url,
        now + chrono::Duration::minutes(1) + chrono::Duration::seconds(7),
        1_000,
    )
    .await?;
    let second = crate::execution_submit_adapter::reconcile_execution_tiny_submit_confirmation(
        &store,
        &rpc_boundary_reconcile_config(),
        &order_id,
        &reqwest::Client::new(),
        "",
        now + chrono::Duration::minutes(1) + chrono::Duration::seconds(8),
        1_000,
    )
    .await?;

    assert_eq!(first.confirmation_confirmed, 1);
    assert_eq!(first.sell_closed, 1);
    assert_eq!(second.confirmation_confirmed, 1);
    assert_eq!(second.sell_closed, 0);
    assert!(store
        .load_execution_canary_open_position("TokenMint")?
        .is_none());

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

async fn serve_rpc_boundary_response(expected_signature: &str, body: &str) -> Result<String> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let rpc_url = format!("http://{}", listener.local_addr()?);
    let expected_signature = expected_signature.to_string();
    let body = body.to_string();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("http request");
        let mut buffer = [0_u8; 8192];
        let read = socket.read(&mut buffer).await.expect("read request");
        let request = String::from_utf8_lossy(&buffer[..read]);
        assert!(request.contains("\"method\":\"getSignatureStatuses\""));
        assert!(request.contains(&expected_signature));
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        socket
            .write_all(response.as_bytes())
            .await
            .expect("write response");
    });
    Ok(rpc_url)
}

fn record_rpc_boundary_build_metadata(
    store: &SqliteStore,
    order_id: &str,
    now: chrono::DateTime<Utc>,
    quote_price_sol: f64,
) -> Result<()> {
    let order = store
        .load_execution_canary_order(order_id)?
        .expect("order should exist");
    store.record_execution_canary_build_plan_metadata(
        &copybot_storage_core::ExecutionCanaryBuildPlanMetadata {
            order_id: order.order_id,
            signal_id: order.signal_id,
            client_order_id: order.client_order_id,
            recorded_ts: now,
            quote_source: Some("test".to_string()),
            quote_event_id: Some("quote:test".to_string()),
            quote_status: Some("ok".to_string()),
            quote_in_amount_raw: Some("1000000000".to_string()),
            quote_out_amount_raw: Some("10000".to_string()),
            quote_response_json: Some(
                r#"{"quote":{"outAmountUi":10.0,"meta":{"outDecimals":3}}}"#.to_string(),
            ),
            quote_price_sol: Some(quote_price_sol),
            price_impact_pct: Some(0.01),
            route_plan_json: Some("[{\"swapInfo\":{\"label\":\"Pump.fun Amm\"}}]".to_string()),
            priority_fee_source: Some("test".to_string()),
            priority_fee_status: Some("ok".to_string()),
            priority_fee_lamports: Some(10_000),
            priority_fee_json: Some("{\"recommended\":10000}".to_string()),
            slippage_bps: Some(10.0),
            decision_status: Some("would_execute".to_string()),
            decision_reason: Some("within_slippage_limit".to_string()),
        },
    )?;
    Ok(())
}

fn rpc_boundary_reconcile_config() -> ExecutionConfig {
    let mut config = ExecutionConfig::default();
    config.canary_buy_size_sol = 1.0;
    config.canary_wallet_pubkey = "DryRunWallet11111111111111111111111111111111".to_string();
    config.quote_canary_buy_slippage_bps = 1_000;
    config.quote_canary_sell_slippage_bps = 500;
    config
}

fn submitted_rpc_boundary_order(
    store: &SqliteStore,
    name: &str,
    side: &str,
    tx_signature: &str,
    now: chrono::DateTime<Utc>,
) -> Result<String> {
    let signal = rpc_boundary_signal(name, side, now);
    store.insert_copy_signal(&signal)?;
    let reserve = store.reserve_execution_canary_order(&signal.signal_id, "metis-canary", now)?;
    store
        .mark_execution_canary_built(&reserve.order.order_id, now + chrono::Duration::seconds(1))?;
    store.mark_execution_canary_simulated(
        &reserve.order.order_id,
        now + chrono::Duration::seconds(2),
        copybot_storage_core::EXECUTION_SIMULATION_STATUS_PASSED,
        None,
    )?;
    store.mark_execution_canary_submitted(
        &reserve.order.order_id,
        now + chrono::Duration::seconds(3),
        tx_signature,
    )?;
    Ok(reserve.order.order_id)
}

fn boundary_buy_fill(
    order_id: &str,
    fill_ts: chrono::DateTime<Utc>,
) -> crate::execution_submit_adapter::ExecutionConfirmedFill {
    crate::execution_submit_adapter::ExecutionConfirmedFill::Buy(
        crate::execution_submit_adapter::ExecutionConfirmedBuyFill {
            order_id: order_id.to_string(),
            token: "TokenMint".to_string(),
            qty: 10.0,
            qty_exact: Some(copybot_core_types::TokenQuantity::new(10_000, 3)),
            cost_sol: 1.0,
            fill_ts,
        },
    )
}

fn boundary_sell_fill(
    order_id: &str,
    fill_ts: chrono::DateTime<Utc>,
) -> crate::execution_submit_adapter::ExecutionConfirmedFill {
    crate::execution_submit_adapter::ExecutionConfirmedFill::Sell(
        crate::execution_submit_adapter::ExecutionConfirmedSellFill {
            order_id: order_id.to_string(),
            token: "TokenMint".to_string(),
            target_qty: 10.0,
            target_qty_exact: Some(copybot_core_types::TokenQuantity::new(10_000, 3)),
            exit_price_sol: 0.12,
            dust_qty_epsilon: 0.001,
            fill_ts,
        },
    )
}

fn rpc_boundary_signal(
    name: &str,
    side: &str,
    ts: chrono::DateTime<Utc>,
) -> copybot_core_types::CopySignalRow {
    copybot_core_types::CopySignalRow {
        signal_id: format!("shadow:sig-rpc-boundary:leader-wallet:{name}:TokenMint"),
        wallet_id: "leader-wallet".to_string(),
        side: side.to_string(),
        token: "TokenMint".to_string(),
        notional_sol: 0.2,
        notional_lamports: Some(Lamports::new(200_000_000)),
        notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
        ts,
        status: "shadow_recorded".to_string(),
    }
}

fn unique_rpc_boundary_test_path(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "copybot-app-rpc-boundary-{name}-{}-{nanos}.db",
        std::process::id()
    ))
}
