use super::*;

#[test]
fn discovery_scoring_materialization_gap_repair_target_tracks_current_gap() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("discovery-scoring-gap-repair-target.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    let gap_cursor = DiscoveryRuntimeCursor {
        ts_utc: DateTime::parse_from_rfc3339("2026-04-29T02:30:58.295525837Z")
            .expect("ts")
            .with_timezone(&Utc),
        slot: 416346850,
        signature: "gap-a".to_string(),
    };
    let target_cursor = DiscoveryRuntimeCursor {
        ts_utc: DateTime::parse_from_rfc3339("2026-04-30T17:44:20.734423474Z")
            .expect("ts")
            .with_timezone(&Utc),
        slot: 416900001,
        signature: "target-a".to_string(),
    };
    store.set_discovery_scoring_materialization_gap_cursor(&gap_cursor)?;
    store.set_discovery_scoring_materialization_gap_repair_target(&gap_cursor, &target_cursor)?;
    assert_eq!(
        store.load_discovery_scoring_materialization_gap_repair_target()?,
        Some((gap_cursor.clone(), target_cursor.clone()))
    );

    let earlier_gap_cursor = DiscoveryRuntimeCursor {
        ts_utc: gap_cursor.ts_utc - Duration::minutes(1),
        slot: gap_cursor.slot - 1,
        signature: "gap-earlier".to_string(),
    };
    store.set_discovery_scoring_materialization_gap_cursor(&earlier_gap_cursor)?;
    assert_eq!(
        store.load_discovery_scoring_materialization_gap_repair_target()?,
        None,
        "changing the materialization gap cursor must clear stale repair target state"
    );

    let second_target_cursor = DiscoveryRuntimeCursor {
        ts_utc: target_cursor.ts_utc + Duration::minutes(10),
        slot: target_cursor.slot + 1,
        signature: "target-b".to_string(),
    };
    store.set_discovery_scoring_materialization_gap_repair_target(
        &earlier_gap_cursor,
        &second_target_cursor,
    )?;
    store.clear_discovery_scoring_materialization_gap_if_cursor_observed(&earlier_gap_cursor)?;
    assert_eq!(
        store.load_discovery_scoring_materialization_gap_repair_target()?,
        None,
        "clearing the materialization gap must clear persisted repair target state"
    );
    Ok(())
}

#[test]
fn apply_discovery_scoring_batch_records_fifo_buy_and_close_facts() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("discovery-scoring-batch.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    let buy_one = SwapEvent {
        signature: "scoring-buy-1".to_string(),
        wallet: "wallet-scoring".to_string(),
        dex: "raydium".to_string(),
        token_in: "So11111111111111111111111111111111111111112".to_string(),
        token_out: "TokenScoring11111111111111111111111111111".to_string(),
        amount_in: 1.0,
        amount_out: 100.0,
        exact_amounts: None,
        slot: 1,
        ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
            .expect("ts")
            .with_timezone(&Utc),
    };
    let buy_two = SwapEvent {
        signature: "scoring-buy-2".to_string(),
        wallet: "wallet-scoring".to_string(),
        dex: "raydium".to_string(),
        token_in: "So11111111111111111111111111111111111111112".to_string(),
        token_out: "TokenScoring11111111111111111111111111111".to_string(),
        amount_in: 1.0,
        amount_out: 50.0,
        exact_amounts: None,
        slot: 2,
        ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:05:00Z")
            .expect("ts")
            .with_timezone(&Utc),
    };
    let sell = SwapEvent {
        signature: "scoring-sell-1".to_string(),
        wallet: "wallet-scoring".to_string(),
        dex: "raydium".to_string(),
        token_in: "TokenScoring11111111111111111111111111111".to_string(),
        token_out: "So11111111111111111111111111111111111111112".to_string(),
        amount_in: 120.0,
        amount_out: 3.0,
        exact_amounts: None,
        slot: 3,
        ts_utc: DateTime::parse_from_rfc3339("2026-03-06T11:00:00Z")
            .expect("ts")
            .with_timezone(&Utc),
    };
    let swaps = vec![buy_one.clone(), buy_two.clone(), sell.clone()];
    store.insert_observed_swaps_batch(&swaps)?;

    store.apply_discovery_scoring_batch(
        &swaps,
        &DiscoveryAggregateWriteConfig {
            max_tx_per_minute: 50,
            rug_lookahead_seconds: 60,
            helius_http_url: None,
            min_token_age_hint_seconds: None,
        },
    )?;

    let days = store.load_wallet_scoring_days_since(
        DateTime::parse_from_rfc3339("2026-03-06T00:00:00Z")
            .expect("ts")
            .with_timezone(&Utc),
    )?;
    assert_eq!(days.len(), 1);
    assert_eq!(days[0].trades, 3);
    assert!((days[0].spent_sol - 2.0).abs() < 1e-9);
    assert!((days[0].max_buy_notional_sol - 1.0).abs() < 1e-9);

    let buy_facts = store.load_wallet_scoring_buy_facts_since(
        DateTime::parse_from_rfc3339("2026-03-06T00:00:00Z")
            .expect("ts")
            .with_timezone(&Utc),
    )?;
    assert_eq!(buy_facts.len(), 2);

    let close_facts = store.load_wallet_scoring_close_facts_since(
        DateTime::parse_from_rfc3339("2026-03-06T00:00:00Z")
            .expect("ts")
            .with_timezone(&Utc),
    )?;
    assert_eq!(close_facts.len(), 2, "sell should close two FIFO segments");
    let total_pnl: f64 = close_facts.iter().map(|row| row.pnl_sol).sum();
    assert!(
        (total_pnl - 1.6).abs() < 1e-9,
        "expected FIFO pnl split across close facts"
    );

    let (remaining_qty, remaining_cost): (f64, f64) = store.conn.query_row(
        "SELECT qty, cost_sol
             FROM wallet_scoring_open_lots
             WHERE wallet_id = 'wallet-scoring'
               AND token = 'TokenScoring11111111111111111111111111111'",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    assert!((remaining_qty - 30.0).abs() < 1e-9);
    assert!((remaining_cost - 0.6).abs() < 1e-9);
    Ok(())
}

#[test]
fn discovery_scoring_builder_replay_matches_sql_replay_on_representative_fixture() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let sql_db_path = temp.path().join("discovery-scoring-builder-sql.db");
    let builder_db_path = temp.path().join("discovery-scoring-builder-builder.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut sql_store = SqliteStore::open(Path::new(&sql_db_path))?;
    let mut builder_store = SqliteStore::open(Path::new(&builder_db_path))?;
    sql_store.run_migrations(&migration_dir)?;
    builder_store.run_migrations(&migration_dir)?;

    let sol_mint = "So11111111111111111111111111111111111111112";
    let token_a = "TokenBuilderA111111111111111111111111111";
    let token_b = "TokenBuilderB111111111111111111111111111";
    let start_cursor_ts = DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
        .expect("ts")
        .with_timezone(&Utc);
    let day_start = DateTime::parse_from_rfc3339("2026-03-06T00:00:00Z")
        .expect("ts")
        .with_timezone(&Utc);

    let mut observed_swaps = vec![
        make_swap(
            "builder-lookback-a",
            "wallet-lookback-a",
            sol_mint,
            token_a,
            0.7,
            70.0,
            1,
            start_cursor_ts - Duration::minutes(2),
        ),
        make_swap(
            "builder-lookback-b",
            "wallet-lookback-b",
            token_b,
            sol_mint,
            25.0,
            0.4,
            2,
            start_cursor_ts - Duration::minutes(1),
        ),
    ];
    let mut batch_swaps = Vec::new();
    let base_ts = start_cursor_ts + Duration::minutes(1);
    let mut next_slot = 10u64;
    for idx in 0..150usize {
        let token = if idx % 2 == 0 { token_a } else { token_b };
        let ts = base_ts + Duration::seconds((idx as i64) * 45);
        let wallet_main = format!("wallet-main-{}", idx % 5);
        let wallet_peer = format!("wallet-peer-{}", idx % 3);
        let main_buy_notional = 1.0 + ((idx % 3) as f64 * 0.1);
        let peer_buy_notional = 0.4 + ((idx % 2) as f64 * 0.1);
        let later_buy_notional = 0.6 + ((idx % 4) as f64 * 0.05);
        batch_swaps.push(make_swap(
            format!("builder-main-buy-{idx}"),
            wallet_main.clone(),
            sol_mint,
            token,
            main_buy_notional,
            100.0,
            next_slot,
            ts,
        ));
        next_slot += 1;
        batch_swaps.push(make_swap(
            format!("builder-peer-buy-{idx}"),
            wallet_peer,
            sol_mint,
            token,
            peer_buy_notional,
            40.0,
            next_slot,
            ts,
        ));
        next_slot += 1;
        batch_swaps.push(make_swap(
            format!("builder-main-later-buy-{idx}"),
            wallet_main.clone(),
            sol_mint,
            token,
            later_buy_notional,
            30.0,
            next_slot,
            ts + Duration::seconds(15),
        ));
        next_slot += 1;
        batch_swaps.push(make_swap(
            format!("builder-main-sell-{idx}"),
            wallet_main,
            token,
            sol_mint,
            80.0,
            1.8 + ((idx % 5) as f64 * 0.05),
            next_slot,
            ts + Duration::seconds(30),
        ));
        next_slot += 1;
    }
    observed_swaps.extend(batch_swaps.iter().cloned());
    sql_store.insert_observed_swaps_batch(&observed_swaps)?;
    builder_store.insert_observed_swaps_batch(&observed_swaps)?;
    sql_store.upsert_token_quality_cache(
        token_a,
        Some(111),
        Some(12.3),
        Some(3_600),
        start_cursor_ts,
    )?;
    builder_store.upsert_token_quality_cache(
        token_a,
        Some(111),
        Some(12.3),
        Some(3_600),
        start_cursor_ts,
    )?;
    sql_store.upsert_token_quality_cache(
        token_b,
        Some(222),
        Some(24.6),
        Some(7_200),
        start_cursor_ts - Duration::minutes(20),
    )?;
    builder_store.upsert_token_quality_cache(
        token_b,
        Some(222),
        Some(24.6),
        Some(7_200),
        start_cursor_ts - Duration::minutes(20),
    )?;

    let config = DiscoveryAggregateWriteConfig {
        max_tx_per_minute: 50,
        rug_lookahead_seconds: 300,
        helius_http_url: None,
        min_token_age_hint_seconds: None,
    };
    let sql_timings =
        sql_store.apply_discovery_scoring_batch_with_timings(&batch_swaps, &config)?;
    let sql_rug_finalize_ms = sql_store.finalize_discovery_scoring_rug_facts_with_timing(
        batch_swaps.last().expect("fixture must have swaps").ts_utc,
    )?;

    let mut builder =
        builder_store.begin_discovery_scoring_replay_builder(start_cursor_ts, 0, "")?;
    let builder_timings = builder_store.apply_discovery_scoring_builder_batch_with_timings(
        &mut builder,
        &batch_swaps,
        &config,
    )?;
    let builder_rug_finalize_ms = builder_store.finalize_discovery_scoring_rug_facts_with_timing(
        batch_swaps.last().expect("fixture must have swaps").ts_utc,
    )?;

    println!(
            "builder_vs_sql_fixture rows={} sql_prepare_ms={} sql_apply_ms={} sql_rug_finalize_ms={} builder_prepare_ms={} builder_apply_ms={} builder_rug_finalize_ms={}",
            batch_swaps.len(),
            sql_timings.prepare_ms,
            sql_timings.apply_ms,
            sql_rug_finalize_ms,
            builder_timings.prepare_ms,
            builder_timings.apply_ms,
            builder_rug_finalize_ms,
        );

    assert_eq!(
        comparable_wallet_scoring_days(&sql_store)?,
        comparable_wallet_scoring_days(&builder_store)?,
        "wallet_scoring_days diverged between sql and builder replay",
    );
    assert_eq!(
        comparable_wallet_scoring_tx_minutes(&sql_store)?,
        comparable_wallet_scoring_tx_minutes(&builder_store)?,
        "wallet_scoring_tx_minutes diverged between sql and builder replay",
    );
    assert_eq!(
        comparable_wallet_scoring_buy_facts(&sql_store)?,
        comparable_wallet_scoring_buy_facts(&builder_store)?,
        "wallet_scoring_buy_facts diverged between sql and builder replay",
    );
    assert_eq!(
        comparable_wallet_scoring_close_facts(&sql_store)?,
        comparable_wallet_scoring_close_facts(&builder_store)?,
        "wallet_scoring_close_facts diverged between sql and builder replay",
    );
    assert_eq!(
        comparable_wallet_scoring_open_lots(&sql_store)?,
        comparable_wallet_scoring_open_lots(&builder_store)?,
        "wallet_scoring_open_lots diverged between sql and builder replay",
    );

    let sql_snapshot = sql_store.load_wallet_scoring_snapshot_since(day_start)?;
    let builder_snapshot = builder_store.load_wallet_scoring_snapshot_since(day_start)?;
    assert_eq!(
        sql_snapshot.max_tx_counts, builder_snapshot.max_tx_counts,
        "loaded wallet scoring snapshot max_tx_counts diverged between sql and builder replay",
    );

    let boundary_cursor = DiscoveryRuntimeCursor {
        ts_utc: batch_swaps.last().expect("fixture must have swaps").ts_utc,
        slot: batch_swaps.last().expect("fixture must have swaps").slot,
        signature: batch_swaps
            .last()
            .expect("fixture must have swaps")
            .signature
            .clone(),
    };
    assert_eq!(
        sql_store
            .export_discovery_scoring_boundary_seed_snapshot(day_start, &boundary_cursor)?
            .open_lots,
        builder_store
            .export_discovery_scoring_boundary_seed_snapshot(day_start, &boundary_cursor)?
            .open_lots,
        "boundary seed open-lot export diverged between sql and builder replay",
    );

    Ok(())
}

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
