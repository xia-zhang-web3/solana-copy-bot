use super::*;
use copybot_core_types::{
    CopySignalRow, Lamports, TokenQuantity, COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn orphan_position_recovery_restores_only_known_execution_token() -> Result<()> {
    let db_path = unique_orphan_recovery_test_path("known-token");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let historical_opened_ts = now - chrono::Duration::minutes(20);
    store.record_execution_canary_open_position(
        "old-confirmed-buy",
        "KnownMint",
        10.0,
        Some(TokenQuantity::new(10_000, 3)),
        0.01,
        historical_opened_ts,
    )?;
    store.close_execution_canary_open_position(
        "KnownMint",
        10.0,
        Some(TokenQuantity::new(10_000, 3)),
        0.001,
        1e-9,
        now - chrono::Duration::minutes(10),
    )?;
    let (rpc_url, server) = serve_orphan_token_accounts().await?;
    let mut config = orphan_recovery_config(&rpc_url);

    let summary =
        crate::execution_canary_route::process_tiny_submit_orphan_position_recovery_sweep(
            &config, &store, now,
        )
        .await?
        .expect("orphan recovery should run");
    server.await??;
    let known = store
        .load_execution_canary_open_position("KnownMint")?
        .expect("known token should be recovered");
    let unknown = store.load_execution_canary_open_position("UnknownMint")?;

    assert_eq!(summary.orphan_recovery_checked, 2);
    assert_eq!(summary.orphan_recovery_recovered, 1);
    assert_eq!(summary.orphan_recovery_skipped_no_history, 1);
    assert_eq!(summary.open_positions, 1);
    assert_eq!(known.qty_exact, Some(TokenQuantity::new(12_345, 3)));
    assert_eq!(known.cost_lamports, Some(Lamports::new(10_000_000)));
    assert_eq!(known.opened_ts, historical_opened_ts);
    assert!(unknown.is_none());

    let _ = std::fs::remove_file(db_path);
    config.priority_fee_canary_rpc_url.clear();
    Ok(())
}

#[tokio::test]
async fn orphan_position_recovery_skips_recent_terminal_write_off_token() -> Result<()> {
    let db_path = unique_orphan_recovery_test_path("terminal-write-off-skip");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let token = "TerminalMint";
    let historical_opened_ts = now - chrono::Duration::hours(3);
    store.record_execution_canary_open_position(
        "old-confirmed-buy",
        token,
        12.345,
        Some(TokenQuantity::new(12_345, 3)),
        0.01,
        historical_opened_ts,
    )?;
    store.close_execution_canary_open_position(
        token,
        12.345,
        Some(TokenQuantity::new(12_345, 3)),
        0.001,
        1e-9,
        now - chrono::Duration::hours(2),
    )?;
    let signal = terminal_sell_signal(token, now - chrono::Duration::hours(1));
    store.insert_copy_signal(&signal)?;
    let reserve = store.reserve_execution_canary_order(
        &signal.signal_id,
        crate::execution_canary_route::CANARY_ROUTE_METIS_SWAP_INSTRUCTIONS_DRY_RUN,
        now - chrono::Duration::minutes(50),
    )?;
    store.mark_execution_canary_failed(
        &reserve.order.order_id,
        now - chrono::Duration::minutes(49),
        copybot_storage_core::EXECUTION_ERROR_BUILD_FAILED,
        "owned_sell_quote_failed: NO_ROUTES_FOUND",
    )?;
    store.mark_execution_canary_terminal_sell_no_route_blocked(
        &reserve.order.order_id,
        "terminal_failed_sell_no_route_written_off",
    )?;
    store.record_execution_canary_open_position(
        "recovery-orphan:TerminalMint:12345:manual",
        token,
        12.345,
        Some(TokenQuantity::new(12_345, 3)),
        0.01,
        historical_opened_ts,
    )?;
    let body = r#"{"jsonrpc":"2.0","id":"execution-canary-orphan-recovery","result":{"value":[{"account":{"data":{"parsed":{"info":{"mint":"TerminalMint","tokenAmount":{"amount":"12345","decimals":3,"uiAmountString":"12.345"}}}}}}]}}"#;
    let (rpc_url, server) = serve_orphan_token_accounts_body(body).await?;
    let config = orphan_recovery_config(&rpc_url);

    let summary =
        crate::execution_canary_route::process_tiny_submit_orphan_position_recovery_sweep(
            &config, &store, now,
        )
        .await?
        .expect("orphan recovery should run");
    server.await??;
    let open = store.load_execution_canary_open_position(token)?;

    assert_eq!(summary.orphan_recovery_checked, 1);
    assert_eq!(summary.orphan_recovery_recovered, 0);
    assert_eq!(summary.orphan_recovery_reconciled, 1);
    assert_eq!(
        summary.skipped_reason.as_deref(),
        Some("orphan_recovery_recent_terminal_write_off")
    );
    assert_eq!(summary.open_positions, 0);
    assert!(open.is_none());

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn orphan_position_recovery_restores_token_2022_balance() -> Result<()> {
    let db_path = unique_orphan_recovery_test_path("token-2022-known-token");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let historical_opened_ts = now - chrono::Duration::minutes(20);
    store.record_execution_canary_open_position(
        "old-confirmed-buy",
        "Token2022Mint",
        10.0,
        Some(TokenQuantity::new(10_000, 3)),
        0.01,
        historical_opened_ts,
    )?;
    store.close_execution_canary_open_position(
        "Token2022Mint",
        10.0,
        Some(TokenQuantity::new(10_000, 3)),
        0.001,
        1e-9,
        now - chrono::Duration::minutes(10),
    )?;
    let token_2022_body = r#"{"jsonrpc":"2.0","id":"execution-canary-orphan-recovery","result":{"value":[{"account":{"data":{"parsed":{"info":{"mint":"Token2022Mint","tokenAmount":{"amount":"12345","decimals":3,"uiAmountString":"12.345"}}}}}}]}}"#;
    let (rpc_url, server) =
        serve_orphan_token_accounts_bodies(EMPTY_ORPHAN_TOKEN_ACCOUNTS_BODY, token_2022_body)
            .await?;
    let config = orphan_recovery_config(&rpc_url);

    let summary =
        crate::execution_canary_route::process_tiny_submit_orphan_position_recovery_sweep(
            &config, &store, now,
        )
        .await?
        .expect("orphan recovery should run");
    server.await??;
    let known = store
        .load_execution_canary_open_position("Token2022Mint")?
        .expect("known Token-2022 token should be recovered");

    assert_eq!(summary.orphan_recovery_checked, 1);
    assert_eq!(summary.orphan_recovery_recovered, 1);
    assert_eq!(summary.open_positions, 1);
    assert_eq!(known.qty_exact, Some(TokenQuantity::new(12_345, 3)));
    assert_eq!(known.opened_ts, historical_opened_ts);

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn orphan_position_recovery_retimestamps_existing_recovery_position() -> Result<()> {
    let db_path = unique_orphan_recovery_test_path("retimestamp-existing");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let historical_opened_ts = now - chrono::Duration::hours(8);
    store.record_execution_canary_open_position(
        "old-confirmed-buy",
        "KnownMint",
        10.0,
        Some(TokenQuantity::new(10_000, 3)),
        0.01,
        historical_opened_ts,
    )?;
    store.close_execution_canary_open_position(
        "KnownMint",
        10.0,
        Some(TokenQuantity::new(10_000, 3)),
        0.001,
        1e-9,
        now - chrono::Duration::hours(7),
    )?;
    store.record_execution_canary_open_position(
        "recovery-orphan:KnownMint:12345:manual",
        "KnownMint",
        12.345,
        Some(TokenQuantity::new(12_345, 3)),
        0.01,
        now,
    )?;
    let (rpc_url, server) = serve_orphan_token_accounts().await?;
    let config = orphan_recovery_config(&rpc_url);

    let summary =
        crate::execution_canary_route::process_tiny_submit_orphan_position_recovery_sweep(
            &config, &store, now,
        )
        .await?
        .expect("orphan recovery should run");
    server.await??;
    let known = store
        .load_execution_canary_open_position("KnownMint")?
        .expect("known token should stay open");

    assert_eq!(summary.orphan_recovery_checked, 2);
    assert_eq!(summary.orphan_recovery_recovered, 0);
    assert_eq!(summary.orphan_recovery_skipped_no_history, 1);
    assert_eq!(known.opened_ts, historical_opened_ts);

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[tokio::test]
async fn orphan_position_recovery_reconciles_recovery_orphan_wallet_balance() -> Result<()> {
    let db_path = unique_orphan_recovery_test_path("reconcile-wallet-balance");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    store.record_execution_canary_open_position(
        "recovery-orphan:ZeroMint:10000:manual",
        "ZeroMint",
        10.0,
        Some(TokenQuantity::new(10_000, 3)),
        0.01,
        now - chrono::Duration::hours(1),
    )?;
    store.record_execution_canary_open_position(
        "recovery-orphan:PartialMint:10000:manual",
        "PartialMint",
        10.0,
        Some(TokenQuantity::new(10_000, 3)),
        0.01,
        now - chrono::Duration::hours(1),
    )?;
    let body = r#"{"jsonrpc":"2.0","id":"execution-canary-orphan-recovery","result":{"value":[{"account":{"data":{"parsed":{"info":{"mint":"ZeroMint","tokenAmount":{"amount":"0","decimals":3,"uiAmountString":"0"}}}}}},{"account":{"data":{"parsed":{"info":{"mint":"PartialMint","tokenAmount":{"amount":"4000","decimals":3,"uiAmountString":"4"}}}}}}]}}"#;
    let (rpc_url, server) = serve_orphan_token_accounts_body(body).await?;
    let config = orphan_recovery_config(&rpc_url);

    let summary =
        crate::execution_canary_route::process_tiny_submit_orphan_position_recovery_sweep(
            &config, &store, now,
        )
        .await?
        .expect("orphan recovery should run");
    server.await??;
    let zero = store.load_execution_canary_open_position("ZeroMint")?;
    let partial = store
        .load_execution_canary_open_position("PartialMint")?
        .expect("partial wallet balance should stay open");

    assert_eq!(summary.orphan_recovery_checked, 1);
    assert_eq!(summary.orphan_recovery_reconciled, 2);
    assert_eq!(summary.open_positions, 1);
    assert!(zero.is_none());
    assert_eq!(partial.qty_exact, Some(TokenQuantity::new(4_000, 3)));
    assert_eq!(partial.cost_lamports, Some(Lamports::new(4_000_000)));

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

async fn serve_orphan_token_accounts() -> Result<(String, tokio::task::JoinHandle<Result<()>>)> {
    let body = r#"{"jsonrpc":"2.0","id":"execution-canary-orphan-recovery","result":{"value":[{"account":{"data":{"parsed":{"info":{"mint":"KnownMint","tokenAmount":{"amount":"12345","decimals":3,"uiAmountString":"12.345"}}}}}},{"account":{"data":{"parsed":{"info":{"mint":"UnknownMint","tokenAmount":{"amount":"5000","decimals":3,"uiAmountString":"5"}}}}}}]}}"#;
    serve_orphan_token_accounts_body(body).await
}

const EMPTY_ORPHAN_TOKEN_ACCOUNTS_BODY: &str =
    r#"{"jsonrpc":"2.0","id":"execution-canary-orphan-recovery","result":{"value":[]}}"#;

async fn serve_orphan_token_accounts_body(
    body: &'static str,
) -> Result<(String, tokio::task::JoinHandle<Result<()>>)> {
    serve_orphan_token_accounts_bodies(body, EMPTY_ORPHAN_TOKEN_ACCOUNTS_BODY).await
}

async fn serve_orphan_token_accounts_bodies(
    spl_token_body: &'static str,
    token_2022_body: &'static str,
) -> Result<(String, tokio::task::JoinHandle<Result<()>>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let rpc_url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        for body in [spl_token_body, token_2022_body] {
            let (mut socket, _) = listener.accept().await?;
            let mut buffer = [0_u8; 4096];
            let read = socket.read(&mut buffer).await?;
            let request = String::from_utf8_lossy(&buffer[..read]);
            assert!(request.contains("getTokenAccountsByOwner"));
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            socket.write_all(response.as_bytes()).await?;
        }
        Ok(())
    });
    Ok((rpc_url, server))
}

fn orphan_recovery_config(rpc_url: &str) -> ExecutionConfig {
    let mut config = ExecutionConfig::default();
    config.canary_enabled = true;
    config.canary_dry_run = true;
    config.canary_tiny_submit_enabled = true;
    config.canary_route =
        crate::execution_canary_route::CANARY_ROUTE_METIS_SWAP_INSTRUCTIONS_DRY_RUN.to_string();
    config.canary_max_open_positions = 10;
    config.canary_wallet_pubkey = "ExecutorPubkey".to_string();
    config.execution_signer_pubkey = "ExecutorPubkey".to_string();
    config.execution_signer_keypair_path = "/tmp/non-empty-keypair.json".to_string();
    config.submit_adapter_http_url = "http://127.0.0.1:9".to_string();
    config.priority_fee_canary_rpc_url = rpc_url.to_string();
    config.swap_transaction_dry_run_enabled = true;
    config.quote_canary_timeout_ms = 1_000;
    config
}

fn terminal_sell_signal(token: &str, ts: chrono::DateTime<Utc>) -> CopySignalRow {
    CopySignalRow {
        signal_id: format!("shadow:terminal-sell:leader:{token}"),
        wallet_id: "leader".to_string(),
        side: "sell".to_string(),
        token: token.to_string(),
        notional_sol: 0.2,
        notional_lamports: Some(Lamports::new(200_000_000)),
        notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
        ts,
        status: "shadow_recorded".to_string(),
    }
}

fn unique_orphan_recovery_test_path(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("copybot-orphan-recovery-{}-{}.db", name, nanos))
}
