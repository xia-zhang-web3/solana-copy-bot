use super::*;

#[test]
fn discovery_scoring_checkpointed_batch_rolls_back_on_failure_before_progress_advance() -> Result<()>
{
    let temp = tempdir().context("failed to create tempdir")?;
    let failing_db_path = temp.path().join("discovery-scoring-checkpoint-fail.db");
    let clean_db_path = temp.path().join("discovery-scoring-checkpoint-clean.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut failing_store = SqliteStore::open(Path::new(&failing_db_path))?;
    let mut clean_store = SqliteStore::open(Path::new(&clean_db_path))?;
    failing_store.run_migrations(&migration_dir)?;
    clean_store.run_migrations(&migration_dir)?;

    let start_ts = DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
        .expect("ts")
        .with_timezone(&Utc);
    let buy_one = make_swap(
        "checkpoint-buy-1",
        "wallet-checkpoint",
        "So11111111111111111111111111111111111111112",
        "TokenCheckpoint111111111111111111111111",
        1.0,
        100.0,
        1,
        start_ts + Duration::minutes(1),
    );
    let buy_two = make_swap(
        "checkpoint-buy-2",
        "wallet-checkpoint",
        "So11111111111111111111111111111111111111112",
        "TokenCheckpoint111111111111111111111111",
        0.5,
        25.0,
        2,
        start_ts + Duration::minutes(2),
    );
    let sell = make_swap(
        "checkpoint-sell-1",
        "wallet-checkpoint",
        "TokenCheckpoint111111111111111111111111",
        "So11111111111111111111111111111111111111112",
        80.0,
        1.6,
        3,
        start_ts + Duration::minutes(3),
    );
    let swaps = vec![buy_one.clone(), buy_two.clone(), sell.clone()];
    failing_store.insert_observed_swaps_batch(&swaps)?;
    clean_store.insert_observed_swaps_batch(&swaps)?;
    let progress_cursor = DiscoveryRuntimeCursor {
        ts_utc: sell.ts_utc,
        slot: sell.slot,
        signature: sell.signature.clone(),
    };
    let config = DiscoveryAggregateWriteConfig {
        max_tx_per_minute: 50,
        rug_lookahead_seconds: 300,
        helius_http_url: None,
        min_token_age_hint_seconds: None,
    };

    SqliteStore::set_discovery_scoring_atomic_checkpoint_failpoint_for_tests(true);
    let error = failing_store
        .apply_discovery_scoring_batch_and_checkpoint_with_timings(
            &swaps,
            &config,
            start_ts,
            &progress_cursor,
        )
        .expect_err("test failpoint should abort the atomic batch");
    assert!(
        format!("{error:#}").contains("after materialization before checkpoint"),
        "unexpected failpoint error: {error:#}",
    );
    assert!(comparable_wallet_scoring_days(&failing_store)?.is_empty());
    assert!(comparable_wallet_scoring_tx_minutes(&failing_store)?.is_empty());
    assert!(comparable_wallet_scoring_buy_facts(&failing_store)?.is_empty());
    assert!(comparable_wallet_scoring_close_facts(&failing_store)?.is_empty());
    assert!(comparable_wallet_scoring_open_lots(&failing_store)?.is_empty());
    assert_eq!(
        failing_store.load_discovery_scoring_backfill_progress()?,
        None
    );

    failing_store.apply_discovery_scoring_batch_and_checkpoint_with_timings(
        &swaps,
        &config,
        start_ts,
        &progress_cursor,
    )?;
    clean_store.apply_discovery_scoring_batch_and_checkpoint_with_timings(
        &swaps,
        &config,
        start_ts,
        &progress_cursor,
    )?;

    assert_eq!(
        comparable_wallet_scoring_days(&clean_store)?,
        comparable_wallet_scoring_days(&failing_store)?,
    );
    assert_eq!(
        comparable_wallet_scoring_tx_minutes(&clean_store)?,
        comparable_wallet_scoring_tx_minutes(&failing_store)?,
    );
    assert_eq!(
        comparable_wallet_scoring_buy_facts(&clean_store)?,
        comparable_wallet_scoring_buy_facts(&failing_store)?,
    );
    assert_eq!(
        comparable_wallet_scoring_close_facts(&clean_store)?,
        comparable_wallet_scoring_close_facts(&failing_store)?,
    );
    assert_eq!(
        comparable_wallet_scoring_open_lots(&clean_store)?,
        comparable_wallet_scoring_open_lots(&failing_store)?,
    );
    assert_eq!(
        clean_store.load_discovery_scoring_backfill_progress()?,
        failing_store.load_discovery_scoring_backfill_progress()?,
    );
    Ok(())
}
