    #[test]
    fn snapshot_into_path_with_policy_completes_under_concurrent_source_writes_when_snapshot_is_pinned(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let source_path = temp.path().join("snapshot-source-pinned.db");
        let destination_path = temp.path().join("snapshot-destination-pinned.db");

        {
            let seed_store = SqliteStore::open(Path::new(&source_path))?;
            seed_store
                .conn
                .execute_batch("CREATE TABLE snapshot_source(id INTEGER PRIMARY KEY, value TEXT);")
                .context("failed creating pinned snapshot source table")?;
            let large_value = "x".repeat(2048);
            for idx in 0..2048 {
                seed_store
                    .conn
                    .execute(
                        "INSERT INTO snapshot_source(id, value) VALUES (?1, ?2)",
                        params![idx, large_value],
                    )
                    .context("failed seeding pinned snapshot source table")?;
            }
        }

        let source_store = SqliteStore::open_read_only(Path::new(&source_path))?;
        let start_barrier = Arc::new(std::sync::Barrier::new(2));
        let stop_writes = Arc::new(AtomicBool::new(false));
        let writer_path = source_path.clone();
        let writer_barrier = start_barrier.clone();
        let writer_stop = stop_writes.clone();
        let writer = thread::spawn(move || -> Result<()> {
            let writer_store = SqliteStore::open(Path::new(&writer_path))?;
            writer_store
                .conn
                .busy_timeout(StdDuration::from_millis(1))
                .context("failed to shorten concurrent writer busy timeout")?;
            writer_barrier.wait();
            let mut counter: i64 = 3_000;
            while !writer_stop.load(Ordering::Relaxed) {
                let row_id = (counter % 256) + 1;
                let _ = writer_store.conn.execute(
                    "UPDATE snapshot_source SET value = ?1 WHERE id = ?2",
                    params![format!("writer-{counter}"), row_id],
                );
                if counter % 16 == 0 {
                    let _ = writer_store.conn.execute(
                        "INSERT INTO snapshot_source(id, value) VALUES (?1, ?2)",
                        params![counter, format!("writer-insert-{counter}")],
                    );
                }
                counter += 1;
            }
            Ok(())
        });

        start_barrier.wait();
        let outcome = source_store.snapshot_into_path_with_policy(
            &destination_path,
            &SqliteSnapshotPolicy {
                busy_timeout: StdDuration::from_millis(5),
                pages_per_step: 256,
                pause_between_steps: StdDuration::from_millis(0),
                retry_backoff_ms: vec![1, 5, 10],
                max_attempt_duration: Some(StdDuration::from_secs(2)),
                pin_source_snapshot: true,
            },
        )?;
        stop_writes.store(true, Ordering::Relaxed);
        writer
            .join()
            .expect("writer thread panicked")
            .context("concurrent writer thread failed")?;

        let SqliteSnapshotOutcome::Written(summary) = outcome else {
            anyhow::bail!("expected pinned source snapshot backup to complete");
        };
        assert!(
            summary.total_page_count > 0,
            "written snapshot must report total page count"
        );
        assert_eq!(
            summary.copied_page_count, summary.total_page_count,
            "written snapshot must report full page coverage"
        );

        let snapshot_store = SqliteStore::open_read_only(&destination_path)?;
        let copied_rows: i64 =
            snapshot_store
                .conn
                .query_row("SELECT COUNT(*) FROM snapshot_source", [], |row| row.get(0))?;
        assert!(
            copied_rows >= 2048,
            "snapshot must contain seeded source rows"
        );
        Ok(())
    }

    #[test]
    fn insert_observed_swaps_batch_returns_insert_flags_in_order() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("observed-swap-batch-flags.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let swap_a = SwapEvent {
            wallet: "wallet-batch".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-a".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-batch-a".to_string(),
            slot: 100,
            ts_utc: now,
            exact_amounts: None,
        };
        let swap_b = SwapEvent {
            wallet: "wallet-batch".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-b".to_string(),
            amount_in: 2.0,
            amount_out: 20.0,
            signature: "sig-batch-b".to_string(),
            slot: 101,
            ts_utc: now + Duration::seconds(1),
            exact_amounts: None,
        };

        let inserted =
            store.insert_observed_swaps_batch(&[swap_a.clone(), swap_a.clone(), swap_b.clone()])?;
        assert_eq!(inserted, vec![true, false, true]);

        let swaps = store.load_observed_swaps_since(now - Duration::seconds(1))?;
        assert_eq!(swaps.len(), 2);
        assert_eq!(swaps[0].signature, "sig-batch-a");
        assert_eq!(swaps[1].signature, "sig-batch-b");
        Ok(())
    }

    #[test]
    fn observed_swap_roundtrip_preserves_exact_amounts() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("observed-swap-exact-roundtrip.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let swap = SwapEvent {
            wallet: "wallet-exact".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-exact".to_string(),
            amount_in: 1.0,
            amount_out: 100.0,
            signature: "sig-exact".to_string(),
            slot: 100,
            ts_utc: now,
            exact_amounts: Some(ExactSwapAmounts {
                amount_in_raw: "1000000000".to_string(),
                amount_in_decimals: 9,
                amount_out_raw: "100000000".to_string(),
                amount_out_decimals: 6,
            }),
        };

        assert!(store.insert_observed_swap(&swap)?);
        let swaps = store.load_observed_swaps_since(now - Duration::seconds(1))?;
        assert_eq!(swaps.len(), 1);
        assert_eq!(swaps[0].exact_amounts, swap.exact_amounts);
        Ok(())
    }

    #[test]
    fn insert_observed_swaps_batch_retries_after_write_lock() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("observed-swap-batch-retry.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let blocker_store = SqliteStore::open(Path::new(&db_path))?;
        blocker_store
            .conn
            .busy_timeout(StdDuration::from_millis(1))
            .context("failed to shorten blocker busy timeout")?;
        blocker_store
            .conn
            .execute_batch("BEGIN IMMEDIATE TRANSACTION")?;

        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let worker_db_path = db_path.clone();
        let worker_barrier = barrier.clone();
        let handle = std::thread::spawn(move || -> Result<()> {
            let worker_store = SqliteStore::open(Path::new(&worker_db_path))?;
            worker_store
                .conn
                .busy_timeout(StdDuration::from_millis(1))
                .context("failed to shorten worker busy timeout")?;
            worker_barrier.wait();
            let now = DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
                .expect("timestamp")
                .with_timezone(&Utc);
            let inserted = worker_store.insert_observed_swaps_batch(&[
                SwapEvent {
                    wallet: "wallet-retry".to_string(),
                    dex: "raydium".to_string(),
                    token_in: "So11111111111111111111111111111111111111112".to_string(),
                    token_out: "token-retry-a".to_string(),
                    amount_in: 1.0,
                    amount_out: 10.0,
                    signature: "sig-observed-swap-batch-retry-a".to_string(),
                    slot: 999,
                    ts_utc: now,
                    exact_amounts: None,
                },
                SwapEvent {
                    wallet: "wallet-retry".to_string(),
                    dex: "raydium".to_string(),
                    token_in: "So11111111111111111111111111111111111111112".to_string(),
                    token_out: "token-retry-b".to_string(),
                    amount_in: 2.0,
                    amount_out: 20.0,
                    signature: "sig-observed-swap-batch-retry-b".to_string(),
                    slot: 1000,
                    ts_utc: now + Duration::seconds(1),
                    exact_amounts: None,
                },
            ])?;
            assert_eq!(inserted, vec![true, true]);
            Ok(())
        });

        barrier.wait();
        std::thread::sleep(StdDuration::from_millis(250));
        blocker_store.conn.execute_batch("COMMIT")?;
        handle
            .join()
            .expect("worker thread panicked")
            .context("worker batch insert should succeed after retry")?;

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        let swaps = verify_store.load_observed_swaps_since(
            DateTime::parse_from_rfc3339("2026-03-06T11:59:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        )?;
        assert_eq!(swaps.len(), 2);
        Ok(())
    }

    #[test]
    fn delete_observed_swaps_before_applies_time_retention_cutoff() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("observed-swap-retention.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let stale_ts = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let recent_ts = DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        store.insert_observed_swap(&SwapEvent {
            wallet: "wallet-retention".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-stale".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-observed-swap-stale".to_string(),
            slot: 1,
            ts_utc: stale_ts,
            exact_amounts: None,
        })?;
        store.insert_observed_swap(&SwapEvent {
            wallet: "wallet-retention".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-recent".to_string(),
            amount_in: 2.0,
            amount_out: 20.0,
            signature: "sig-observed-swap-recent".to_string(),
            slot: 2,
            ts_utc: recent_ts,
            exact_amounts: None,
        })?;

        let deleted = store.delete_observed_swaps_before(recent_ts - Duration::days(1))?;
        assert_eq!(deleted, 1);

        let swaps = store.load_observed_swaps_since(stale_ts - Duration::seconds(1))?;
        assert_eq!(swaps.len(), 1);
        assert_eq!(swaps[0].signature, "sig-observed-swap-recent");
        Ok(())
    }

    #[test]
    fn delete_observed_swaps_before_batched_chunks_retention_work() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("observed-swap-retention-batched.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let stale_one = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let stale_two = stale_one + Duration::minutes(1);
        let stale_three = stale_one + Duration::minutes(2);
        let recent_ts = DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        for (signature, ts, slot) in [
            ("sig-observed-swap-stale-a", stale_one, 1),
            ("sig-observed-swap-stale-b", stale_two, 2),
            ("sig-observed-swap-stale-c", stale_three, 3),
            ("sig-observed-swap-recent", recent_ts, 4),
        ] {
            store.insert_observed_swap(&SwapEvent {
                wallet: "wallet-retention".to_string(),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: format!("token-{signature}"),
                amount_in: 1.0,
                amount_out: 10.0,
                signature: signature.to_string(),
                slot,
                ts_utc: ts,
                exact_amounts: None,
            })?;
        }

        let summary =
            store.delete_observed_swaps_before_batched(recent_ts - Duration::days(1), 1)?;
        assert_eq!(summary.deleted_rows, 3);
        assert_eq!(summary.batches, 3);

        let swaps = store.load_observed_swaps_since(stale_one - Duration::seconds(1))?;
        assert_eq!(swaps.len(), 1);
        assert_eq!(swaps[0].signature, "sig-observed-swap-recent");
        Ok(())
    }

    #[test]
    fn apply_history_retention_preserves_undelivered_warn_events_after_cursor() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("risk-events-retention.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let stale_ts = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let fresh_ts = DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        for (event_id, event_type, severity, ts) in [
            ("info-old", "info_event", "info", stale_ts),
            ("warn-delivered", "warn_event", "warn", stale_ts),
            ("warn-pending", "warn_event", "warn", stale_ts),
            ("warn-fresh", "warn_event", "warn", fresh_ts),
        ] {
            store.conn.execute(
                "INSERT INTO risk_events(event_id, type, severity, ts, details_json)
                 VALUES (?1, ?2, ?3, ?4, NULL)",
                params![event_id, event_type, severity, ts.to_rfc3339()],
            )?;
        }

        let delivered_rowid: i64 = store.conn.query_row(
            "SELECT rowid FROM risk_events WHERE event_id = 'warn-delivered'",
            [],
            |row| row.get(0),
        )?;
        store.upsert_alert_delivery_cursor("webhook", delivered_rowid)?;

        let summary = store.apply_history_retention(
            HistoryRetentionCutoffs {
                risk_events_before: fresh_ts - Duration::days(1),
                copy_signals_before: fresh_ts - Duration::days(1),
                orders_before: fresh_ts - Duration::days(1),
                shadow_closed_trades_before: fresh_ts - Duration::days(1),
            },
            true,
        )?;
        assert_eq!(summary.risk_events_deleted, 2);
        assert_eq!(summary.risk_events_batches, 1);

        let mut stmt = store.conn.prepare(
            "SELECT event_id
             FROM risk_events
             ORDER BY rowid ASC",
        )?;
        let remaining = stmt
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        assert_eq!(remaining, vec!["warn-pending", "warn-fresh"]);
        Ok(())
    }

    #[test]
    fn exact_money_cutover_state_round_trips_and_applies_activation_boundary() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("exact-money-cutover-state.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        assert_eq!(store.exact_money_cutover_ts()?, None);

        let cutover_ts = DateTime::parse_from_rfc3339("2026-03-10T08:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        store.upsert_exact_money_cutover_state(cutover_ts, Some("test-cutover"))?;

        assert_eq!(store.exact_money_cutover_ts()?, Some(cutover_ts));
        assert!(!store.exact_money_cutover_active_at(cutover_ts - Duration::seconds(1))?);
        assert!(store.exact_money_cutover_active_at(cutover_ts)?);
        assert!(store.exact_money_cutover_active_at(cutover_ts + Duration::seconds(1))?);
        Ok(())
    }

    #[test]
    fn apply_history_retention_preserves_warn_events_before_first_alert_delivery() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("risk-events-retention-no-cursor.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let stale_ts = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let fresh_ts = DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        for (event_id, severity, ts) in [
            ("info-old", "info", stale_ts),
            ("warn-old", "warn", stale_ts),
            ("error-old", "error", stale_ts),
            ("warn-fresh", "warn", fresh_ts),
        ] {
            store.conn.execute(
                "INSERT INTO risk_events(event_id, type, severity, ts, details_json)
                 VALUES (?1, 'risk_event', ?2, ?3, NULL)",
                params![event_id, severity, ts.to_rfc3339()],
            )?;
        }

        let summary = store.apply_history_retention(
            HistoryRetentionCutoffs {
                risk_events_before: fresh_ts - Duration::days(1),
                copy_signals_before: fresh_ts - Duration::days(1),
                orders_before: fresh_ts - Duration::days(1),
                shadow_closed_trades_before: fresh_ts - Duration::days(1),
            },
            true,
        )?;
        assert_eq!(summary.risk_events_deleted, 1);
        assert_eq!(summary.risk_events_batches, 1);

        let mut stmt = store.conn.prepare(
            "SELECT event_id
             FROM risk_events
             ORDER BY rowid ASC",
        )?;
        let remaining = stmt
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        assert_eq!(remaining, vec!["warn-old", "error-old", "warn-fresh"]);
        Ok(())
    }

    #[test]
    fn latest_risk_event_by_type_returns_latest_row() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("risk-events-latest.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let first_ts = DateTime::parse_from_rfc3339("2026-03-10T08:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let second_ts = DateTime::parse_from_rfc3339("2026-03-10T08:05:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let third_ts = DateTime::parse_from_rfc3339("2026-03-10T08:06:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        store.insert_risk_event("shadow_risk_pause", "warn", first_ts, Some("{\"seq\":1}"))?;
        store.insert_risk_event("other_event", "warn", second_ts, Some("{\"seq\":2}"))?;
        store.insert_risk_event("shadow_risk_pause", "warn", third_ts, Some("{\"seq\":3}"))?;

        let latest = store
            .latest_risk_event_by_type("shadow_risk_pause")?
            .expect("latest event");
        assert_eq!(latest.event_type, "shadow_risk_pause");
        assert_eq!(latest.ts, third_ts.to_rfc3339());
        assert_eq!(latest.details_json.as_deref(), Some("{\"seq\":3}"));
        assert!(latest.rowid > 0);
        Ok(())
    }
