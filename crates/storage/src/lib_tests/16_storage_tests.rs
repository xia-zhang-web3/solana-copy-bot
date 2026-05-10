use super::*;

#[test]
fn wallet_activity_day_counts_since_returns_day_level_counts() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("wallet-activity-days.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    let rows = vec![
        WalletActivityDayRow {
            wallet_id: "wallet-a".to_string(),
            activity_day: NaiveDate::from_ymd_opt(2026, 3, 5).expect("date"),
            last_seen: DateTime::parse_from_rfc3339("2026-03-05T12:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        },
        WalletActivityDayRow {
            wallet_id: "wallet-a".to_string(),
            activity_day: NaiveDate::from_ymd_opt(2026, 3, 6).expect("date"),
            last_seen: DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        },
        WalletActivityDayRow {
            wallet_id: "wallet-b".to_string(),
            activity_day: NaiveDate::from_ymd_opt(2026, 3, 6).expect("date"),
            last_seen: DateTime::parse_from_rfc3339("2026-03-06T08:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        },
        WalletActivityDayRow {
            wallet_id: "wallet-a".to_string(),
            activity_day: NaiveDate::from_ymd_opt(2026, 3, 6).expect("date"),
            last_seen: DateTime::parse_from_rfc3339("2026-03-06T18:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        },
    ];
    store.upsert_wallet_activity_days(&rows)?;

    let counts = store.wallet_active_day_counts_since(
        &["wallet-a".to_string(), "wallet-b".to_string()],
        DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
            .expect("ts")
            .with_timezone(&Utc),
    )?;
    assert_eq!(counts.get("wallet-a"), Some(&1));
    assert!(
        !counts.contains_key("wallet-b"),
        "same-day activity before exact window_start must not be counted"
    );
    Ok(())
}

#[test]
fn backfill_wallet_activity_days_since_uses_existing_observed_swaps() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("wallet-activity-backfill.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    let window_start = DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
        .expect("ts")
        .with_timezone(&Utc);
    store.insert_observed_swap(&SwapEvent {
        signature: "backfill-pre-window".to_string(),
        wallet: "wallet-a".to_string(),
        dex: "raydium".to_string(),
        token_in: "So11111111111111111111111111111111111111112".to_string(),
        token_out: "TokenBackfill111111111111111111111111111111".to_string(),
        amount_in: 1.0,
        amount_out: 100.0,
        exact_amounts: None,
        slot: 1,
        ts_utc: DateTime::parse_from_rfc3339("2026-03-06T08:00:00Z")
            .expect("ts")
            .with_timezone(&Utc),
    })?;
    store.insert_observed_swap(&SwapEvent {
        signature: "backfill-boundary-window".to_string(),
        wallet: "wallet-a".to_string(),
        dex: "raydium".to_string(),
        token_in: "So11111111111111111111111111111111111111112".to_string(),
        token_out: "TokenBackfill111111111111111111111111111111".to_string(),
        amount_in: 1.0,
        amount_out: 100.0,
        exact_amounts: None,
        slot: 2,
        ts_utc: DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
            .expect("ts")
            .with_timezone(&Utc),
    })?;
    store.insert_observed_swap(&SwapEvent {
        signature: "backfill-later-day".to_string(),
        wallet: "wallet-a".to_string(),
        dex: "raydium".to_string(),
        token_in: "So11111111111111111111111111111111111111112".to_string(),
        token_out: "TokenBackfill111111111111111111111111111111".to_string(),
        amount_in: 1.0,
        amount_out: 100.0,
        exact_amounts: None,
        slot: 3,
        ts_utc: DateTime::parse_from_rfc3339("2026-03-07T09:00:00Z")
            .expect("ts")
            .with_timezone(&Utc),
    })?;

    store.backfill_wallet_activity_days_since(window_start)?;

    let counts = store.wallet_active_day_counts_since(&["wallet-a".to_string()], window_start)?;
    assert_eq!(
        counts.get("wallet-a"),
        Some(&2),
        "backfill should use existing observed_swaps at or after the exact window_start"
    );
    Ok(())
}

#[test]
fn wallet_activity_days_reads_reject_noncanonical_last_seen() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp
        .path()
        .join("wallet-activity-days-hidden-bad-last-seen.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    store.conn.execute(
        "INSERT INTO wallet_activity_days(wallet_id, activity_day, last_seen)
         VALUES ('wallet-hidden-bad', '2026-03-06', '2026-03-06T12:00:00Z')",
        [],
    )?;
    let window_start = DateTime::parse_from_rfc3339("2026-03-06T10:00:00+00:00")
        .expect("ts")
        .with_timezone(&Utc);

    for error in [
        store
            .wallet_active_day_counts_since(&["wallet-hidden-bad".to_string()], window_start)
            .expect_err("wallet activity counts must reject noncanonical last_seen"),
        store
            .wallet_activity_days_row_count_since(window_start)
            .expect_err("wallet activity row count must reject noncanonical last_seen"),
        store
            .wallet_activity_day_coverage_since(&["wallet-hidden-bad".to_string()], window_start)
            .expect_err("wallet activity coverage must reject noncanonical last_seen"),
    ] {
        assert!(
            format!("{error:#}").contains("wallet_activity_days.last_seen is not canonical UTC"),
            "unexpected error: {error:#}"
        );
    }
    Ok(())
}

#[test]
fn wallet_activity_days_backfill_rejects_hidden_noncanonical_observed_timestamp() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp
        .path()
        .join("wallet-activity-days-backfill-hidden-bad-ts.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    store.conn.execute(
        "INSERT INTO observed_swaps(signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts)
         VALUES ('wallet-activity-hidden-bad-ts', 'wallet-hidden-bad', 'raydium', 'SOL', 'TOKEN', 1.0, 2.0, 1, '2026-03-06T12:30:00Z')",
        [],
    )?;
    let err = store
        .backfill_wallet_activity_days_since(
            DateTime::parse_from_rfc3339("2026-03-06T10:00:00+00:00")
                .expect("ts")
                .with_timezone(&Utc),
        )
        .expect_err("wallet activity backfill must reject bad observed timestamp before aggregate");
    assert!(
        format!("{err:#}").contains("observed_swaps.ts is not canonical UTC"),
        "unexpected error: {err:#}"
    );
    Ok(())
}

#[test]
fn discovery_scoring_coverage_marker_gates_window_readiness() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("discovery-scoring-coverage.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    let window_start = DateTime::parse_from_rfc3339("2026-03-01T00:00:00Z")
        .expect("ts")
        .with_timezone(&Utc);
    let now = DateTime::parse_from_rfc3339("2026-03-09T12:00:00Z")
        .expect("ts")
        .with_timezone(&Utc);
    let max_lag = Duration::minutes(10);
    assert!(!store.discovery_scoring_ready_for_window(window_start, now, max_lag)?);

    store.set_discovery_scoring_covered_since(window_start - Duration::hours(1))?;
    assert!(
        !store.discovery_scoring_ready_for_window(window_start, now, max_lag)?,
        "covered_since alone must not activate aggregate reads without a near-head watermark"
    );
    store.conn.execute(
        "INSERT INTO discovery_scoring_state(state_key, state_value, updated_at)
             VALUES ('covered_through_ts', ?1, ?2)
             ON CONFLICT(state_key) DO UPDATE SET
                state_value = excluded.state_value,
                updated_at = excluded.updated_at",
        params![
            (now - Duration::minutes(5)).to_rfc3339(),
            Utc::now().to_rfc3339()
        ],
    )?;
    assert!(
            !store.discovery_scoring_ready_for_window(window_start, now, max_lag)?,
            "timestamp-only covered_through state must not enable aggregate reads without the exact cursor"
        );

    store.set_discovery_scoring_covered_through_cursor(&DiscoveryRuntimeCursor {
        ts_utc: now - Duration::minutes(5),
        slot: 42,
        signature: "covered-through-ready".to_string(),
    })?;
    assert!(store.discovery_scoring_ready_for_window(window_start, now, max_lag)?);

    store.set_discovery_scoring_materialization_gap_cursor(&DiscoveryRuntimeCursor {
        ts_utc: now - Duration::minutes(15),
        slot: 100,
        signature: "gap-row".to_string(),
    })?;
    assert!(
            !store.discovery_scoring_ready_for_window(window_start, now, max_lag)?,
            "latched materialization gaps must block aggregate readiness even with near-head watermarks"
        );
    store.clear_discovery_scoring_materialization_gap_if_cursor_observed(
        &DiscoveryRuntimeCursor {
            ts_utc: now - Duration::minutes(30),
            slot: 0,
            signature: String::new(),
        },
    )?;
    assert!(
        !store.discovery_scoring_ready_for_window(window_start, now, max_lag)?,
        "observing an earlier cursor must not clear the exact continuity blocker"
    );
    store.clear_discovery_scoring_materialization_gap_if_cursor_observed(
        &DiscoveryRuntimeCursor {
            ts_utc: now - Duration::minutes(15),
            slot: 100,
            signature: "gap-row".to_string(),
        },
    )?;
    assert!(store.discovery_scoring_ready_for_window(window_start, now, max_lag)?);

    store.set_discovery_scoring_materialization_gap_cursor(&DiscoveryRuntimeCursor {
        ts_utc: now - Duration::minutes(15),
        slot: 100,
        signature: "gap-row-b".to_string(),
    })?;
    store.clear_discovery_scoring_materialization_gap_if_cursor_observed(
        &DiscoveryRuntimeCursor {
            ts_utc: now - Duration::minutes(15),
            slot: 100,
            signature: "zzz-after-gap".to_string(),
        },
    )?;
    assert!(
            !store.discovery_scoring_ready_for_window(window_start, now, max_lag)?,
            "observing a different row at the same timestamp must not clear the exact continuity blocker"
        );
    store.clear_discovery_scoring_materialization_gap_if_cursor_observed(
        &DiscoveryRuntimeCursor {
            ts_utc: now - Duration::minutes(15),
            slot: 100,
            signature: "gap-row-b".to_string(),
        },
    )?;
    assert!(store.discovery_scoring_ready_for_window(window_start, now, max_lag)?);

    store.set_discovery_scoring_covered_since(window_start + Duration::hours(1))?;
    assert!(
        !store.discovery_scoring_ready_for_window(window_start, now, max_lag)?,
        "coverage marker later than window_start must not enable aggregate reads yet"
    );

    store.set_discovery_scoring_covered_since(window_start - Duration::hours(1))?;
    let later_now = now + Duration::hours(3);
    assert!(
        !store.discovery_scoring_ready_for_window(window_start, later_now, max_lag)?,
        "stale covered_through watermark must keep aggregate reads disabled"
    );
    Ok(())
}
