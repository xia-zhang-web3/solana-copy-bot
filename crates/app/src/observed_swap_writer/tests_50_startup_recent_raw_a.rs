    #[test]
    fn observed_swap_writer_startup_replay_clears_observed_materialization_gap() -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-gap-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;
        let covered_swap = SwapEvent {
            wallet: "wallet-gap".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-gap".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-gap-covered".to_string(),
            slot: 99,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T09:55:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };
        seed_store.insert_observed_swaps_batch(&[covered_swap.clone()])?;
        seed_store.apply_discovery_scoring_batch(
            std::slice::from_ref(&covered_swap),
            &aggregate_write_config(),
        )?;
        seed_store.set_discovery_scoring_covered_through_cursor(&DiscoveryRuntimeCursor {
            ts_utc: covered_swap.ts_utc,
            slot: covered_swap.slot,
            signature: covered_swap.signature.clone(),
        })?;
        let failed_swap = SwapEvent {
            wallet: "wallet-gap".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-gap".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-gap-failed".to_string(),
            slot: 100,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };
        seed_store.insert_observed_swaps_batch(&[failed_swap.clone()])?;
        seed_store.set_discovery_scoring_materialization_gap_cursor(&DiscoveryRuntimeCursor {
            ts_utc: failed_swap.ts_utc,
            slot: failed_swap.slot,
            signature: failed_swap.signature.clone(),
        })?;

        let runtime = Builder::new_current_thread().enable_all().build()?;
        runtime.block_on(async {
            let writer = ObservedSwapWriter::start_with_config(
                db_path
                    .to_str()
                    .context("sqlite path must be valid utf-8")?
                    .to_string(),
                ObservedSwapWriterConfig::for_test(16, 8, true, aggregate_write_config(), None),
            )?;

            let successful_swap = SwapEvent {
                wallet: "wallet-gap".to_string(),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: "token-gap".to_string(),
                amount_in: 1.0,
                amount_out: 10.0,
                signature: "sig-gap-success".to_string(),
                slot: 101,
                ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:05:00Z")
                    .expect("timestamp")
                    .with_timezone(&Utc),
                exact_amounts: None,
            };
            writer.write(&successful_swap).await?;
            wait_for_discovery_scoring_covered_through_at_least(&db_path, successful_swap.ts_utc)?;
            writer.shutdown()?;
            Ok::<(), anyhow::Error>(())
        })?;

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        assert_eq!(
            verify_store.load_discovery_scoring_materialization_gap_cursor()?,
            None,
            "startup replay must clear a latched continuity gap once it reprocesses the exact failed row"
        );
        assert_eq!(
            verify_store.load_discovery_scoring_covered_through()?,
            Some(
                DateTime::parse_from_rfc3339("2026-03-06T10:05:00Z")
                    .expect("timestamp")
                    .with_timezone(&Utc)
            )
        );
        let days = verify_store.load_wallet_scoring_days_since(
            DateTime::parse_from_rfc3339("2026-03-06T00:00:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        )?;
        assert_eq!(days.len(), 1);
        assert_eq!(
            days[0].trades, 3,
            "startup replay must materialize the previously failed row before live writes resume"
        );
        let _ = std::fs::remove_file(db_path);

        Ok(())
    }

    #[test]
    fn observed_swap_writer_reports_terminal_failure_after_fatal_startup_replay_gap_cursor_failure(
    ) -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-startup-gap-fatal-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;

        let covered_swap = SwapEvent {
            wallet: "wallet-startup-gap-fatal".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-startup-gap-fatal".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-startup-gap-covered".to_string(),
            slot: 100,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-15T10:00:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };
        let tail_swap = SwapEvent {
            wallet: "wallet-startup-gap-fatal".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-startup-gap-fatal".to_string(),
            amount_in: 2.0,
            amount_out: 20.0,
            signature: "sig-startup-gap-tail".to_string(),
            slot: 101,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-15T10:05:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };
        seed_store.insert_observed_swaps_batch(&[covered_swap.clone(), tail_swap.clone()])?;
        seed_store
            .apply_discovery_scoring_batch(&[covered_swap.clone()], &aggregate_write_config())?;
        seed_store.set_discovery_scoring_covered_through_cursor(&DiscoveryRuntimeCursor {
            ts_utc: covered_swap.ts_utc,
            slot: covered_swap.slot,
            signature: covered_swap.signature.clone(),
        })?;

        let trigger_conn = Connection::open(Path::new(&db_path))
            .context("failed to open sqlite db for startup gap fatal trigger")?;
        trigger_conn.execute_batch(
            "CREATE TRIGGER fail_wallet_scoring_days_insert
             BEFORE INSERT ON wallet_scoring_days
             BEGIN
                 SELECT RAISE(FAIL, 'forced discovery scoring failure');
             END;
             CREATE TRIGGER fail_discovery_scoring_state_insert
             BEFORE INSERT ON discovery_scoring_state
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let writer = ObservedSwapWriter::start_with_config(
            db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?
                .to_string(),
            ObservedSwapWriterConfig::for_test(16, 8, true, aggregate_write_config(), None),
        )?;
        std::thread::sleep(StdDuration::from_millis(50));

        let error = writer.ensure_running().expect_err(
            "fatal startup replay gap cursor failure must latch before writer accepts live work",
        );
        let error_chain = format!("{error:#}");
        assert!(
            error_chain.contains(super::OBSERVED_SWAP_WRITER_TERMINAL_FAILURE_CONTEXT),
            "unexpected terminal startup replay failure error: {error_chain}"
        );
        assert!(
            error_chain.contains("fatal discovery scoring gap cursor failure"),
            "missing startup replay gap-cursor fatal context: {error_chain}"
        );
        assert!(
            error_chain.contains("xShmMap"),
            "missing fatal sqlite marker: {error_chain}"
        );
        assert!(
            !error_chain.contains("failed replaying discovery scoring rows during aggregate-writer startup catch-up"),
            "fatal gap cursor failure should not be masked by aggregate replay context: {error_chain}"
        );

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        assert_eq!(
            verify_store.load_discovery_scoring_materialization_gap_cursor()?,
            None,
            "fatal startup replay gap cursor failure must leave the materialization gap cursor unset"
        );

        let shutdown_error = writer
            .shutdown()
            .expect_err("shutdown should surface fatal startup replay gap cursor failure");
        let shutdown_chain = format!("{shutdown_error:#}");
        assert!(
            shutdown_chain.contains("fatal discovery scoring gap cursor failure"),
            "unexpected shutdown error: {shutdown_chain}"
        );

        drop(trigger_conn);
        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_replays_tail_gap_before_accepting_live_writes() -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-startup-replay-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;

        let covered_swap = SwapEvent {
            wallet: "wallet-startup-replay".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-startup-replay".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-startup-covered".to_string(),
            slot: 100,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };
        let tail_swap = SwapEvent {
            wallet: "wallet-startup-replay".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-startup-replay".to_string(),
            amount_in: 2.0,
            amount_out: 20.0,
            signature: "sig-startup-tail".to_string(),
            slot: 101,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:05:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };
        seed_store.insert_observed_swaps_batch(&[covered_swap.clone(), tail_swap.clone()])?;
        seed_store
            .apply_discovery_scoring_batch(&[covered_swap.clone()], &aggregate_write_config())?;
        seed_store.set_discovery_scoring_covered_through_cursor(&DiscoveryRuntimeCursor {
            ts_utc: covered_swap.ts_utc,
            slot: covered_swap.slot,
            signature: covered_swap.signature.clone(),
        })?;

        let writer = ObservedSwapWriter::start_with_config(
            db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?
                .to_string(),
            ObservedSwapWriterConfig::for_test(16, 8, true, aggregate_write_config(), None),
        )?;
        writer.shutdown()?;

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        let days = verify_store.load_wallet_scoring_days_since(
            DateTime::parse_from_rfc3339("2026-03-06T00:00:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        )?;
        assert_eq!(days.len(), 1);
        assert_eq!(
            days[0].trades, 2,
            "startup replay must materialize raw tail gap"
        );
        assert_eq!(
            verify_store.load_discovery_scoring_covered_through_cursor()?,
            Some(DiscoveryRuntimeCursor {
                ts_utc: tail_swap.ts_utc,
                slot: tail_swap.slot,
                signature: tail_swap.signature.clone(),
            })
        );
        let _ = std::fs::remove_file(db_path);

        Ok(())
    }

    #[test]
    fn observed_swap_writer_upserts_wallet_activity_days_for_inserted_swaps() -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-activity-days-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;

        let runtime = Builder::new_current_thread().enable_all().build()?;
        runtime.block_on(async {
            let writer = ObservedSwapWriter::start_with_config(
                db_path
                    .to_str()
                    .context("sqlite path must be valid utf-8")?
                    .to_string(),
                ObservedSwapWriterConfig::for_test(16, 8, true, aggregate_write_config(), None),
            )?;

            let swap_day_one = SwapEvent {
                wallet: "wallet-activity".to_string(),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: "token-activity".to_string(),
                amount_in: 1.0,
                amount_out: 10.0,
                signature: "sig-observed-swap-day-1".to_string(),
                slot: 100,
                ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
                    .expect("timestamp")
                    .with_timezone(&Utc),
                exact_amounts: None,
            };
            let swap_day_two = SwapEvent {
                wallet: "wallet-activity".to_string(),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: "token-activity".to_string(),
                amount_in: 2.0,
                amount_out: 20.0,
                signature: "sig-observed-swap-day-2".to_string(),
                slot: 101,
                ts_utc: DateTime::parse_from_rfc3339("2026-03-07T11:00:00Z")
                    .expect("timestamp")
                    .with_timezone(&Utc),
                exact_amounts: None,
            };

            writer.write(&swap_day_one).await?;
            writer.write(&swap_day_two).await?;
            wait_for_discovery_scoring_covered_through_at_least(&db_path, swap_day_two.ts_utc)?;
            writer.shutdown()?;
            Ok::<(), anyhow::Error>(())
        })?;

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        let counts = verify_store.wallet_active_day_counts_since(
            &["wallet-activity".to_string()],
            DateTime::parse_from_rfc3339("2026-03-06T00:00:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        )?;
        assert_eq!(counts.get("wallet-activity"), Some(&2));
        let covered_through = verify_store.load_discovery_scoring_covered_through()?;
        assert_eq!(
            covered_through,
            Some(
                DateTime::parse_from_rfc3339("2026-03-07T11:00:00Z")
                    .expect("timestamp")
                    .with_timezone(&Utc)
            )
        );
        let _ = std::fs::remove_file(db_path);

        Ok(())
    }

    #[test]
    fn recent_raw_journal_writer_persists_inserted_observed_swaps_and_reports_telemetry(
    ) -> Result<()> {
        let unique = format!(
            "copybot-app-recent-raw-journal-writer-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let runtime_db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let journal_db_path = std::env::temp_dir().join(format!("{unique}-recent-raw.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut seed_store = SqliteStore::open(Path::new(&runtime_db_path))?;
        seed_store.run_migrations(&migration_dir)?;

        let runtime = Builder::new_current_thread().enable_all().build()?;
        let recent_ts = Utc::now() - ChronoDuration::days(1);
        runtime.block_on(async {
            let writer = ObservedSwapWriter::start_with_config(
                runtime_db_path
                    .to_str()
                    .context("sqlite path must be valid utf-8")?
                    .to_string(),
                ObservedSwapWriterConfig::for_test(
                    16,
                    8,
                    false,
                    aggregate_write_config(),
                    Some(ObservedSwapRecentRawJournalConfig {
                        sqlite_path: journal_db_path
                            .to_str()
                            .context("journal sqlite path must be valid utf-8")?
                            .to_string(),
                        retention_days: 9,
                        writer_queue_capacity_batches: 8,
                        write_coalesce_max_batches:
                            super::OBSERVED_SWAP_RECENT_RAW_JOURNAL_WRITE_COALESCE_MAX_BATCHES,
                        overflow_capacity_batches: 32,
                        skip_prune_while_backlogged: true,
                        skip_startup_prune: false,
                    }),
                ),
            )?;
            let swap = SwapEvent {
                wallet: "wallet-journal".to_string(),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: "token-journal".to_string(),
                amount_in: 1.0,
                amount_out: 10.0,
                signature: "sig-recent-raw-journal".to_string(),
                slot: 500,
                ts_utc: recent_ts,
                exact_amounts: None,
            };

            writer.write(&swap).await?;
            std::thread::sleep(StdDuration::from_millis(50));
            let snapshot = writer.snapshot();
            assert_eq!(snapshot.journal_queue_capacity_batches, 8);
            assert_eq!(snapshot.journal_queue_depth_batches, 0);
            writer.shutdown()?;
            Ok::<(), anyhow::Error>(())
        })?;

        let journal_store = SqliteStore::open(Path::new(&journal_db_path))?;
        let journal_rows =
            journal_store.load_observed_swaps_since(recent_ts - ChronoDuration::hours(1))?;
        assert_eq!(journal_rows.len(), 1);
        assert_eq!(journal_rows[0].signature, "sig-recent-raw-journal");
        let journal_state = journal_store.recent_raw_journal_state()?;
        assert_eq!(journal_state.row_count, 1);
        assert_eq!(journal_state.last_batch_rows, 1);
        let _ = std::fs::remove_file(runtime_db_path);
        let _ = std::fs::remove_file(journal_db_path);
        Ok(())
    }
