use super::*;
use crate::shadow_restart_recovery::{
    apply_shadow_restart_recovery_swaps, ShadowRestartRecoverySummary,
};
use chrono::{DateTime, Utc};
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::path::Path;

fn make_restart_recovery_test_store(name: &str) -> Result<(SqliteStore, String)> {
    let unique = format!(
        "{}-{}-{}",
        name,
        std::process::id(),
        Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or_else(|| Utc::now().timestamp_micros() * 1000)
    );
    let db_path = std::env::temp_dir().join(format!("copybot-app-{unique}.db"));
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    store.run_migrations(&migration_dir)?;
    Ok((store, db_path.to_string_lossy().into_owned()))
}

#[tokio::test]
async fn restart_recovery_swaps_close_open_sell_and_persist_observed_swap() -> Result<()> {
    let (store, db_path) = make_restart_recovery_test_store("restart-recovery-apply")?;
    let writer = ObservedSwapWriter::start_for_test(db_path.clone(), 8, 8)?;

    let mut cfg = ShadowConfig::default();
    cfg.copy_notional_sol = 0.5;
    cfg.min_leader_notional_sol = 0.25;
    cfg.max_signal_lag_seconds = 30;
    cfg.quality_gates_enabled = false;
    let shadow = ShadowService::new(cfg);

    let opened_ts = DateTime::parse_from_rfc3339("2026-05-28T12:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    let sell_ts = opened_ts + chrono::Duration::minutes(5);
    store.insert_shadow_lot("leader-wallet", "TokenMint", 500.0, 0.5, opened_ts)?;
    let mut open_shadow_lots = store.list_shadow_open_pairs()?;

    let sell = SwapEvent {
        wallet: "leader-wallet".to_string(),
        dex: "raydium".to_string(),
        token_in: "TokenMint".to_string(),
        token_out: "So11111111111111111111111111111111111111112".to_string(),
        amount_in: 1000.0,
        amount_out: 1.0,
        signature: "sig-restart-recovery-sell".to_string(),
        slot: 10,
        ts_utc: sell_ts,
        exact_amounts: None,
    };

    let mut recent = HashSet::new();
    let mut recent_order = VecDeque::new();
    let mut reason_counts = BTreeMap::new();
    let mut stage_counts = BTreeMap::new();
    let mut summary = ShadowRestartRecoverySummary::default();
    apply_shadow_restart_recovery_swaps(
        &store,
        &writer,
        &shadow,
        vec![sell.clone()],
        &mut open_shadow_lots,
        &mut recent,
        &mut recent_order,
        &mut reason_counts,
        &mut stage_counts,
        &mut summary,
    )
    .await?;

    assert_eq!(summary.sell_candidates, 1);
    assert_eq!(summary.recovered_sells, 1);
    assert_eq!(store.shadow_open_lots_count()?, 0);
    assert!(open_shadow_lots.is_empty());
    let persisted = store.load_observed_swaps_since(sell_ts - chrono::Duration::seconds(1))?;
    assert!(
        persisted
            .iter()
            .any(|swap| swap.signature == "sig-restart-recovery-sell"),
        "recovered sell should also be persisted into observed swaps"
    );

    writer.shutdown()?;
    let _ = std::fs::remove_file(db_path);
    Ok(())
}
