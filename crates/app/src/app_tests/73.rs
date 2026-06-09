use super::*;
use copybot_core_types::{Lamports, TokenQuantity};
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
    store.record_execution_canary_open_position(
        "old-confirmed-buy",
        "KnownMint",
        10.0,
        Some(TokenQuantity::new(10_000, 3)),
        0.01,
        now - chrono::Duration::minutes(20),
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
    assert!(unknown.is_none());

    let _ = std::fs::remove_file(db_path);
    config.priority_fee_canary_rpc_url.clear();
    Ok(())
}

async fn serve_orphan_token_accounts() -> Result<(String, tokio::task::JoinHandle<Result<()>>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let rpc_url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await?;
        let mut buffer = [0_u8; 4096];
        let read = socket.read(&mut buffer).await?;
        let request = String::from_utf8_lossy(&buffer[..read]);
        assert!(request.contains("getTokenAccountsByOwner"));
        let body = r#"{"jsonrpc":"2.0","id":"execution-canary-orphan-recovery","result":{"value":[{"account":{"data":{"parsed":{"info":{"mint":"KnownMint","tokenAmount":{"amount":"12345","decimals":3,"uiAmountString":"12.345"}}}}}},{"account":{"data":{"parsed":{"info":{"mint":"UnknownMint","tokenAmount":{"amount":"5000","decimals":3,"uiAmountString":"5"}}}}}}]}}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        socket.write_all(response.as_bytes()).await?;
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

fn unique_orphan_recovery_test_path(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("copybot-orphan-recovery-{}-{}.db", name, nanos))
}
