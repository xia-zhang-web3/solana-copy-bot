use super::*;

#[test]
fn prune_discovery_scoring_keeps_old_open_lots_for_late_sell_accounting() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("discovery-scoring-open-lot-retention.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    let buy = SwapEvent {
        signature: "carryover-buy".to_string(),
        wallet: "wallet-carryover".to_string(),
        dex: "raydium".to_string(),
        token_in: "So11111111111111111111111111111111111111112".to_string(),
        token_out: "TokenCarryover111111111111111111111111111".to_string(),
        amount_in: 1.0,
        amount_out: 100.0,
        exact_amounts: None,
        slot: 1,
        ts_utc: DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .expect("ts")
            .with_timezone(&Utc),
    };
    store.insert_observed_swaps_batch(&[buy.clone()])?;
    store.apply_discovery_scoring_batch(&[buy], &DiscoveryAggregateWriteConfig::default())?;

    let cutoff = DateTime::parse_from_rfc3339("2026-02-10T00:00:00Z")
        .expect("ts")
        .with_timezone(&Utc);
    store.prune_discovery_scoring_before(cutoff)?;

    let open_lot_count: i64 =
        store
            .conn
            .query_row("SELECT COUNT(*) FROM wallet_scoring_open_lots", [], |row| {
                row.get(0)
            })?;
    assert_eq!(
        open_lot_count, 1,
        "still-open scoring inventory must survive retention prune until consumed"
    );

    let carryover_count: i64 = store.conn.query_row(
        "SELECT COUNT(*) FROM wallet_scoring_carryover_lots",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(carryover_count, 0);

    let sell = SwapEvent {
        signature: "carryover-sell".to_string(),
        wallet: "wallet-carryover".to_string(),
        dex: "raydium".to_string(),
        token_in: "TokenCarryover111111111111111111111111111".to_string(),
        token_out: "So11111111111111111111111111111111111111112".to_string(),
        amount_in: 100.0,
        amount_out: 2.0,
        exact_amounts: None,
        slot: 2,
        ts_utc: DateTime::parse_from_rfc3339("2026-02-15T00:00:00Z")
            .expect("ts")
            .with_timezone(&Utc),
    };
    store.insert_observed_swaps_batch(&[sell.clone()])?;
    store.apply_discovery_scoring_batch(&[sell], &DiscoveryAggregateWriteConfig::default())?;

    let close_fact_count: i64 = store.conn.query_row(
        "SELECT COUNT(*) FROM wallet_scoring_close_facts
             WHERE sell_signature = 'carryover-sell'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(close_fact_count, 1);
    let (pnl_sol, hold_seconds): (f64, i64) = store.conn.query_row(
        "SELECT pnl_sol, hold_seconds
             FROM wallet_scoring_close_facts
             WHERE sell_signature = 'carryover-sell'
               AND segment_index = 0",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    assert!((pnl_sol - 1.0).abs() < 1e-9);
    assert!(hold_seconds > 0);

    let remaining_open_lots: i64 = store.conn.query_row(
        "SELECT COUNT(*) FROM wallet_scoring_open_lots
             WHERE wallet_id = 'wallet-carryover'
               AND token = 'TokenCarryover111111111111111111111111111'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(remaining_open_lots, 0);
    Ok(())
}

#[test]
fn prune_discovery_scoring_before_batched_chunks_retention_work() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("discovery-scoring-retention-batched.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    let buy_one = SwapEvent {
        signature: "scoring-retention-buy-1".to_string(),
        wallet: "wallet-scoring-retention".to_string(),
        dex: "raydium".to_string(),
        token_in: "So11111111111111111111111111111111111111112".to_string(),
        token_out: "TokenRetention111111111111111111111111111".to_string(),
        amount_in: 1.0,
        amount_out: 100.0,
        exact_amounts: None,
        slot: 1,
        ts_utc: DateTime::parse_from_rfc3339("2026-02-01T00:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc),
    };
    let buy_two = SwapEvent {
        signature: "scoring-retention-buy-2".to_string(),
        wallet: "wallet-scoring-retention".to_string(),
        dex: "raydium".to_string(),
        token_in: "So11111111111111111111111111111111111111112".to_string(),
        token_out: "TokenRetention111111111111111111111111111".to_string(),
        amount_in: 1.0,
        amount_out: 50.0,
        exact_amounts: None,
        slot: 2,
        ts_utc: DateTime::parse_from_rfc3339("2026-02-02T00:01:00Z")
            .expect("timestamp")
            .with_timezone(&Utc),
    };
    let swaps = vec![buy_one, buy_two];
    store.insert_observed_swaps_batch(&swaps)?;
    store.apply_discovery_scoring_batch(&swaps, &DiscoveryAggregateWriteConfig::default())?;

    let cutoff = DateTime::parse_from_rfc3339("2026-02-10T00:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    let summary = store.prune_discovery_scoring_before_batched(cutoff, 3)?;
    assert_eq!(summary.batches, 2);
    assert_eq!(summary.deleted_rows, 6);

    for table in [
        "wallet_scoring_buy_facts",
        "wallet_scoring_tx_minutes",
        "wallet_scoring_days",
    ] {
        let count: i64 =
            store
                .conn
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })?;
        assert_eq!(
            count, 0,
            "expected {table} to be pruned by batched retention"
        );
    }
    Ok(())
}

#[test]
fn observed_swap_batch_with_activity_days_is_atomic_on_activity_upsert_failure() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("observed-swap-activity-atomic.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    store.conn.execute_batch(
        "CREATE TRIGGER fail_wallet_activity_days_insert
             BEFORE INSERT ON wallet_activity_days
             BEGIN
                 SELECT RAISE(FAIL, 'forced wallet activity day failure');
             END;",
    )?;

    let swap = SwapEvent {
        signature: "atomic-activity-fail".to_string(),
        wallet: "wallet-atomic".to_string(),
        dex: "raydium".to_string(),
        token_in: "So11111111111111111111111111111111111111112".to_string(),
        token_out: "TokenAtomic11111111111111111111111111111111".to_string(),
        amount_in: 1.0,
        amount_out: 100.0,
        exact_amounts: None,
        slot: 1,
        ts_utc: DateTime::parse_from_rfc3339("2026-03-08T12:00:00Z")
            .expect("ts")
            .with_timezone(&Utc),
    };

    let error = store
        .insert_observed_swaps_batch_with_activity_days(&[swap.clone()])
        .expect_err("wallet_activity_days failure should abort the whole batch");
    let error_chain = format!("{error:#}");
    assert!(
        error_chain.contains("forced wallet activity day failure"),
        "unexpected atomic batch error: {error_chain}"
    );

    let swaps = store.load_observed_swaps_since(
        DateTime::parse_from_rfc3339("2026-03-08T11:59:00Z")
            .expect("ts")
            .with_timezone(&Utc),
    )?;
    assert!(
        swaps.is_empty(),
        "observed_swaps insert must roll back when wallet_activity_days upsert fails"
    );
    let counts = store.wallet_active_day_counts_since(
        &["wallet-atomic".to_string()],
        DateTime::parse_from_rfc3339("2026-03-08T00:00:00Z")
            .expect("ts")
            .with_timezone(&Utc),
    )?;
    assert!(
        counts.is_empty(),
        "wallet_activity_days must also remain empty"
    );
    Ok(())
}

#[test]
fn wallet_metrics_window_start_index_migration_is_present() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("wallet-metrics-window-start-index.db");
    let legacy_migrations = temp.path().join("legacy-migrations");
    copy_migrations_through(&legacy_migrations, "0020_execution_foreign_keys.sql")?;

    let mut legacy_store = SqliteStore::open(Path::new(&db_path))?;
    legacy_store.run_migrations(&legacy_migrations)?;
    drop(legacy_store);

    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut migrated_store = SqliteStore::open(Path::new(&db_path))?;
    migrated_store.run_migrations(&migration_dir)?;

    let index_sql: Option<String> = migrated_store
        .conn
        .query_row(
            "SELECT sql
                 FROM sqlite_master
                 WHERE type = 'index' AND name = 'idx_wallet_metrics_window_start'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    assert!(
        index_sql.is_some(),
        "wallet_metrics(window_start) hotfix index must exist after migration"
    );
    Ok(())
}
