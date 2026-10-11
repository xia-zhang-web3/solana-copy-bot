//! No ingress service or synthetic swap exists in the owner-only scheduler.
use anyhow::Result;
use chrono::Utc;
use copybot_storage_core::SqliteStore;

#[tokio::test]
async fn execution_only_scheduler_stops_without_ingress_or_discovery() -> Result<()> {
    let root = super::temporary_output_fixture::OutputRoot::new("execution-only-loop")?;
    let path = root.path().join("state.db");
    let mut store = SqliteStore::open(&path)?;
    store.run_migrations(&std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../migrations"))?;
    let mut config = copybot_config::ExecutionConfig::default();
    config.canary_enabled = true;
    config.owner_technical_buy = Some(copybot_config::OwnerTechnicalBuyConfig {
        policy: copybot_config::OWNER_EXECUTION_ROUNDTRIP_V1.into(),
        activate: false,
        run_id: "scheduler-stopped".into(), intent_id: "scheduler-stopped".into(),
        wallet_pubkey: "unused".into(), signer_pubkey: "unused".into(),
        genesis_hash: "unused".into(), mint: "unused".into(), amount_lamports: 10_000_000,
        route: "jupiter_swap_instructions".into(),
        activated_at: Utc::now().to_rfc3339(), expires_at: Utc::now().to_rfc3339(),
        max_priority_fee_lamports: 50_000, min_reserve_lamports: 160_200_031,
        max_slippage_bps: 50, max_daily_loss_lamports: 20_000_000,
        max_open_positions: 1, max_buy_count: 1, source_copy_exit_gate: None,
    });
    let runner = crate::execution_canary::ExecutionCanaryRunner::new(config);
    let stop = async {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        Ok(())
    };
    crate::execution_only_loop::run_until(store, runner, 1, stop).await?;
    let sql = rusqlite::Connection::open(path)?;
    for table in ["copy_signals", "observed_swaps", "followlist", "orders"] {
        let count: i64 = sql.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))?;
        assert_eq!(count, 0, "{table}");
    }
    Ok(())
}
