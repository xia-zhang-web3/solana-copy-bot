use super::*;
use copybot_core_types::{CopySignalRow, Lamports, COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS};

#[tokio::test]
async fn stale_bare_sell_candidate_expires_without_submit() -> Result<()> {
    let db_path = unique_stale_candidate_test_path("stale-bare-sell-candidate");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let signal = stale_candidate_signal("stale-bare-sell-candidate", "sell", now);
    store.insert_copy_signal(&signal)?;
    let reserve = store.reserve_execution_canary_order(
        &signal.signal_id,
        crate::execution_canary_route::CANARY_ROUTE_METIS_SWAP_INSTRUCTIONS_DRY_RUN,
        now,
    )?;
    let mut config = stale_candidate_config();
    config.canary_max_signal_age_seconds = 30;

    let summary = crate::execution_canary_route::process_tiny_submit_reconciliation_sweep(
        &config,
        &store,
        now + chrono::Duration::seconds(60),
    )
    .await?
    .expect("tiny submit route should sweep stale bare sell candidate");
    let order = store
        .load_execution_canary_order(&reserve.order.order_id)?
        .expect("stale sell candidate should remain present");

    assert_eq!(summary.existing, 1);
    assert_eq!(summary.expired, 1);
    assert_eq!(summary.built, 0);
    assert_eq!(summary.simulated, 0);
    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_EXPIRED
    );
    assert_eq!(
        order.err_code.as_deref(),
        Some(copybot_storage_core::EXECUTION_ERROR_EXPIRED)
    );
    assert_eq!(
        order.simulation_error.as_deref(),
        Some("stale_candidate_no_build_metadata")
    );
    assert_eq!(store.execution_canary_open_position_count()?, 0);

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

fn stale_candidate_config() -> ExecutionConfig {
    let mut config = ExecutionConfig::default();
    config.canary_enabled = true;
    config.canary_dry_run = true;
    config.canary_tiny_submit_enabled = true;
    config.canary_route =
        crate::execution_canary_route::CANARY_ROUTE_METIS_SWAP_INSTRUCTIONS_DRY_RUN.to_string();
    config.canary_wallet_pubkey = "ExecutorPubkey".to_string();
    config.execution_signer_pubkey = "ExecutorPubkey".to_string();
    config.execution_signer_keypair_path = "/tmp/non-empty-keypair.json".to_string();
    config.submit_adapter_http_url = "http://127.0.0.1:9".to_string();
    config.quote_canary_base_url = "http://127.0.0.1:9".to_string();
    config.swap_transaction_dry_run_enabled = true;
    config
}

fn stale_candidate_signal(signal_id: &str, side: &str, ts: chrono::DateTime<Utc>) -> CopySignalRow {
    CopySignalRow {
        signal_id: format!("shadow:{signal_id}:leader-wallet:{side}:TokenMint"),
        wallet_id: "leader-wallet".to_string(),
        side: side.to_string(),
        token: "TokenMint".to_string(),
        notional_sol: 0.2,
        notional_lamports: Some(Lamports::new(200_000_000)),
        notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
        ts,
        status: "shadow_recorded".to_string(),
    }
}

fn unique_stale_candidate_test_path(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "copybot-app-stale-candidate-{name}-{}-{nanos}.db",
        std::process::id()
    ))
}
