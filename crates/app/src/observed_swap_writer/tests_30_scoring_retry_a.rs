    #[test]
    fn observed_swap_writer_reports_terminal_failure_after_async_batch_insert_failure() -> Result<()>
    {
        let unique = format!(
            "copybot-app-observed-swap-async-health-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;
        let trigger_conn = Connection::open(Path::new(&db_path))
            .context("failed to open sqlite db for async failure trigger")?;
        trigger_conn.execute_batch(
            "CREATE TRIGGER fail_wallet_activity_days_insert
             BEFORE INSERT ON wallet_activity_days
             BEGIN
                 SELECT RAISE(FAIL, 'forced async observed swap failure');
             END;",
        )?;

        let runtime = Builder::new_current_thread().enable_all().build()?;
        let writer = runtime.block_on(async {
            ObservedSwapWriter::start_with_config(
                db_path
                    .to_str()
                    .context("sqlite path must be valid utf-8")?
                    .to_string(),
                ObservedSwapWriterConfig::for_test(16, 8, true, aggregate_write_config(), None),
            )
        })?;

        let failing_swap = SwapEvent {
            wallet: "wallet-async-health".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-async-health".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-observed-swap-async-health".to_string(),
            slot: 210,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-14T13:10:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };

        runtime.block_on(async { writer.enqueue(&failing_swap).await })?;

        let error_chain = wait_for_writer_terminal_failure(&writer)?;
        assert!(
            error_chain.contains(super::OBSERVED_SWAP_WRITER_TERMINAL_FAILURE_CONTEXT),
            "unexpected terminal failure health-check error: {error_chain}"
        );
        assert!(
            error_chain.contains("forced async observed swap failure"),
            "unexpected terminal failure health-check chain: {error_chain}"
        );

        let shutdown_error = writer
            .shutdown()
            .expect_err("shutdown should surface async raw batch insert failure");
        let shutdown_chain = format!("{shutdown_error:#}");
        assert!(
            shutdown_chain.contains("forced async observed swap failure"),
            "unexpected shutdown error: {shutdown_chain}"
        );
        drop(trigger_conn);

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_keeps_raw_path_live_when_discovery_scoring_failure_is_nonfatal(
    ) -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-aggregate-nonfatal-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;
        let trigger_conn = Connection::open(Path::new(&db_path))
            .context("failed to open sqlite db for aggregate nonfatal trigger")?;
        trigger_conn.execute_batch(
            "CREATE TRIGGER fail_wallet_scoring_days_insert
             BEFORE INSERT ON wallet_scoring_days
             BEGIN
                 SELECT RAISE(FAIL, 'database is locked');
             END;",
        )?;

        let runtime = Builder::new_current_thread().enable_all().build()?;
        runtime.block_on(async {
            let writer = ObservedSwapWriter::start_with_config(
                db_path
                    .to_str()
                    .context("sqlite path must be valid utf-8")?
                    .to_string(),
                ObservedSwapWriterConfig::for_test(16, 8, true, aggregate_write_config(), None),
            )?;

            let first_swap = SwapEvent {
                wallet: "wallet-aggregate-nonfatal".to_string(),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: "token-aggregate-nonfatal-a".to_string(),
                amount_in: 1.0,
                amount_out: 10.0,
                signature: "sig-observed-swap-aggregate-nonfatal-a".to_string(),
                slot: 223,
                ts_utc: DateTime::parse_from_rfc3339("2026-03-14T13:23:00Z")
                    .expect("timestamp")
                    .with_timezone(&Utc),
                exact_amounts: None,
            };
            let second_swap = SwapEvent {
                wallet: "wallet-aggregate-nonfatal".to_string(),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: "token-aggregate-nonfatal-b".to_string(),
                amount_in: 2.0,
                amount_out: 20.0,
                signature: "sig-observed-swap-aggregate-nonfatal-b".to_string(),
                slot: 224,
                ts_utc: DateTime::parse_from_rfc3339("2026-03-14T13:24:00Z")
                    .expect("timestamp")
                    .with_timezone(&Utc),
                exact_amounts: None,
            };

            assert!(
                writer.write(&first_swap).await?,
                "first raw insert should succeed despite non-fatal aggregate failure"
            );
            sleep(Duration::from_millis(50)).await;
            writer
                .ensure_running()
                .context("non-fatal aggregate failure must not latch terminal writer failure")?;

            assert!(
                writer.write(&second_swap).await?,
                "second raw insert should still succeed after non-fatal aggregate failure"
            );
            sleep(Duration::from_millis(50)).await;
            writer.ensure_running().context(
                "subsequent non-fatal aggregate failures must leave the raw writer healthy",
            )?;

            writer.shutdown()?;
            Ok::<(), anyhow::Error>(())
        })?;

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        let rows = verify_store.load_observed_swaps_since(
            DateTime::parse_from_rfc3339("2026-03-14T00:00:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        )?;
        assert_eq!(
            rows.iter()
                .filter(|swap| {
                    swap.signature == "sig-observed-swap-aggregate-nonfatal-a"
                        || swap.signature == "sig-observed-swap-aggregate-nonfatal-b"
                })
                .count(),
            2,
            "non-fatal aggregate failures must not block raw observed-swap persistence"
        );

        drop(trigger_conn);
        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_discovery_scoring_rug_finalize_sqlite_lock_is_retryable_stage1(
    ) -> Result<()> {
        let db_path =
            migrated_observed_swap_writer_test_db("copybot-app-observed-swap-rug-lock-retryable")?;
        let (covered_swap, first_replay_swap, _tail_replay_swap) =
            seed_discovery_scoring_rug_finalize_replay_fixture(&db_path)?;
        let trigger_conn = Connection::open(Path::new(&db_path))
            .context("failed to open sqlite db for rug finalize retryable trigger")?;
        trigger_conn.execute_batch(
            "CREATE TRIGGER fail_rug_finalize_update_retryable
             BEFORE UPDATE OF rug_volume_lookahead_sol ON wallet_scoring_buy_facts
             WHEN OLD.buy_signature = 'sig-rug-finalize-replay-first'
             BEGIN
                 SELECT RAISE(ROLLBACK, 'database is locked');
             END;",
        )?;

        let writer = ObservedSwapWriter::start_with_config(
            db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?
                .to_string(),
            ObservedSwapWriterConfig::for_test(16, 8, true, aggregate_write_config(), None),
        )?;

        let gap_cursor = wait_for_discovery_scoring_materialization_gap_cursor(&db_path)?;
        assert_eq!(
            gap_cursor,
            DiscoveryRuntimeCursor {
                ts_utc: first_replay_swap.ts_utc,
                slot: first_replay_swap.slot,
                signature: first_replay_swap.signature.clone(),
            },
            "retryable rug-finalize lock must latch replay gap instead of advancing coverage"
        );
        std::thread::sleep(StdDuration::from_millis(50));
        writer
            .ensure_running()
            .context("retryable rug-finalize sqlite lock must not become terminal")?;

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        assert_eq!(
            verify_store.load_discovery_scoring_covered_through_cursor()?,
            Some(DiscoveryRuntimeCursor {
                ts_utc: covered_swap.ts_utc,
                slot: covered_swap.slot,
                signature: covered_swap.signature.clone(),
            }),
            "coverage must not advance as if retryable rug finalization completed"
        );

        writer.shutdown()?;
        drop(trigger_conn);
        remove_sqlite_test_files(&db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_discovery_scoring_replay_apply_sqlite_lock_succeeds_after_bounded_retry_stage1(
    ) -> Result<()> {
        let db_path = migrated_observed_swap_writer_test_db(
            "copybot-app-observed-swap-replay-apply-lock-retry-success",
        )?;
        let (_covered_swap, _first_replay_swap, tail_replay_swap) =
            seed_discovery_scoring_rug_finalize_replay_fixture(&db_path)?;
        let trigger_conn = Connection::open(Path::new(&db_path))
            .context("failed to open sqlite db for replay apply transient trigger")?;
        let fail_until_ms = sqlite_fail_until_epoch_ms(StdDuration::from_millis(125));
        trigger_conn.execute_batch(&format!(
            "CREATE TRIGGER fail_replay_apply_day_transient
             BEFORE INSERT ON wallet_scoring_days
             WHEN NEW.wallet_id = 'wallet-rug-finalize-replay'
                  AND {} < {fail_until_ms}
             BEGIN
                 SELECT RAISE(ROLLBACK, 'database is locked');
             END;",
            sqlite_epoch_millis_expression()
        ))?;

        let store = SqliteStore::open(Path::new(&db_path))?;
        let started = Instant::now();
        let progress = super::run_aggregate_gap_replay(
            &store,
            &ObservedSwapWriterConfig::for_test(16, 8, true, aggregate_write_config(), None),
            None,
        )?;

        assert!(
            super::elapsed_ms_ceil(started.elapsed()) >= 50,
            "transient apply lock should have forced at least one bounded retry"
        );
        assert!(progress.caught_up_to_tail);
        assert_discovery_scoring_replay_reached_tail(&db_path, &tail_replay_swap)?;

        drop(trigger_conn);
        remove_sqlite_test_files(&db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_discovery_scoring_rug_finalize_sqlite_lock_succeeds_after_bounded_retry_stage1(
    ) -> Result<()> {
        let db_path = migrated_observed_swap_writer_test_db(
            "copybot-app-observed-swap-rug-lock-retry-success",
        )?;
        let (_covered_swap, _first_replay_swap, tail_replay_swap) =
            seed_discovery_scoring_rug_finalize_replay_fixture(&db_path)?;
        let trigger_conn = Connection::open(Path::new(&db_path))
            .context("failed to open sqlite db for rug finalize transient trigger")?;
        let fail_until_ms = sqlite_fail_until_epoch_ms(StdDuration::from_millis(125));
        trigger_conn.execute_batch(&format!(
            "CREATE TRIGGER fail_rug_finalize_update_transient
             BEFORE UPDATE OF rug_volume_lookahead_sol ON wallet_scoring_buy_facts
             WHEN OLD.buy_signature = 'sig-rug-finalize-replay-first'
                  AND {} < {fail_until_ms}
             BEGIN
                 SELECT RAISE(ROLLBACK, 'database is locked');
             END;",
            sqlite_epoch_millis_expression()
        ))?;

        let store = SqliteStore::open(Path::new(&db_path))?;
        let progress = super::run_aggregate_gap_replay(
            &store,
            &ObservedSwapWriterConfig::for_test(16, 8, true, aggregate_write_config(), None),
            None,
        )?;

        assert!(progress.caught_up_to_tail);
        assert_discovery_scoring_replay_reached_tail(&db_path, &tail_replay_swap)?;

        drop(trigger_conn);
        remove_sqlite_test_files(&db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_discovery_scoring_covered_through_update_sqlite_lock_succeeds_after_bounded_retry_stage1(
    ) -> Result<()> {
        let db_path = migrated_observed_swap_writer_test_db(
            "copybot-app-observed-swap-covered-through-lock-retry-success",
        )?;
        let (_covered_swap, _first_replay_swap, tail_replay_swap) =
            seed_discovery_scoring_rug_finalize_replay_fixture(&db_path)?;
        let trigger_conn = Connection::open(Path::new(&db_path))
            .context("failed to open sqlite db for covered-through transient trigger")?;
        let fail_until_ms = sqlite_fail_until_epoch_ms(StdDuration::from_millis(125));
        trigger_conn.execute_batch(&format!(
            "CREATE TRIGGER fail_covered_through_update_transient
             BEFORE UPDATE ON discovery_scoring_state
             WHEN OLD.state_key = 'covered_through_ts'
                  AND {} < {fail_until_ms}
             BEGIN
                 SELECT RAISE(ROLLBACK, 'database is locked');
             END;",
            sqlite_epoch_millis_expression()
        ))?;

        let store = SqliteStore::open(Path::new(&db_path))?;
        let progress = super::run_aggregate_gap_replay(
            &store,
            &ObservedSwapWriterConfig::for_test(16, 8, true, aggregate_write_config(), None),
            None,
        )?;

        assert!(progress.caught_up_to_tail);
        assert_discovery_scoring_replay_reached_tail(&db_path, &tail_replay_swap)?;

        drop(trigger_conn);
        remove_sqlite_test_files(&db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_aggregate_startup_replay_existing_gap_is_bounded_stage1() -> Result<()>
    {
        let db_path =
            migrated_observed_swap_writer_test_db("copybot-app-aggregate-startup-bounded-gap")?;
        let (_covered_swap, first_replay_swap, tail_replay_swap) =
            seed_discovery_scoring_rug_finalize_replay_fixture(&db_path)?;
        let store = SqliteStore::open(Path::new(&db_path))?;
        store.set_discovery_scoring_materialization_gap_cursor(
            &discovery_runtime_cursor_for_swap(&first_replay_swap),
        )?;
        let config = ObservedSwapWriterConfig::for_test_with_aggregate_tuning(
            16,
            1,
            true,
            aggregate_write_config(),
            OBSERVED_SWAP_DISCOVERY_AGGREGATE_WRITE_COALESCE_MAX_BATCHES,
            8,
            true,
            1,
            None,
        );

        let progress = super::run_aggregate_startup_replay(
            db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?,
            &store,
            &config,
        )?;

        assert_eq!(progress.page_count, 1);
        assert!(progress.gap_cursor_loaded);
        assert!(progress.gap_cursor_observed);
        assert!(!progress.reached_repair_target);
        assert_eq!(progress.last_page_rows, 1);
        assert_eq!(
            store.load_discovery_scoring_materialization_gap_cursor()?,
            Some(discovery_runtime_cursor_for_swap(&first_replay_swap)),
            "bounded startup replay must leave gap evidence latched until repair target is reached"
        );
        assert_eq!(
            store.load_discovery_scoring_covered_through_cursor()?,
            Some(discovery_runtime_cursor_for_swap(&first_replay_swap)),
            "bounded startup replay may advance only through the fully applied page"
        );
        assert_ne!(
            store.load_discovery_scoring_covered_through_cursor()?,
            Some(discovery_runtime_cursor_for_swap(&tail_replay_swap)),
            "bounded startup replay must not run unboundedly to tail"
        );

        remove_sqlite_test_files(&db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_aggregate_startup_replay_does_not_clear_gap_before_exact_row_and_target_stage1(
    ) -> Result<()> {
        let db_path = migrated_observed_swap_writer_test_db(
            "copybot-app-aggregate-startup-gap-not-observed",
        )?;
        let store = SqliteStore::open(Path::new(&db_path))?;
        let covered_swap = aggregate_gap_replay_test_swap(
            "sig-startup-bounded-covered",
            300,
            "2026-04-29T02:30:00Z",
        );
        let pre_gap_swap = aggregate_gap_replay_test_swap(
            "sig-startup-bounded-pre-gap",
            416_346_849,
            "2026-04-29T02:30:58.295525837Z",
        );
        let gap_swap = aggregate_gap_replay_test_swap(
            "sig-startup-bounded-gap",
            416_346_850,
            "2026-04-29T02:30:58.295525837Z",
        );
        store.insert_observed_swaps_batch(&[
            covered_swap.clone(),
            pre_gap_swap.clone(),
            gap_swap.clone(),
        ])?;
        store.apply_discovery_scoring_batch(&[covered_swap.clone()], &aggregate_write_config())?;
        store.set_discovery_scoring_covered_through_cursor(&discovery_runtime_cursor_for_swap(
            &covered_swap,
        ))?;
        store.set_discovery_scoring_materialization_gap_cursor(
            &discovery_runtime_cursor_for_swap(&gap_swap),
        )?;
        let config = ObservedSwapWriterConfig::for_test_with_aggregate_tuning(
            16,
            1,
            true,
            aggregate_write_config(),
            OBSERVED_SWAP_DISCOVERY_AGGREGATE_WRITE_COALESCE_MAX_BATCHES,
            8,
            true,
            1,
            None,
        );

        let progress = super::run_aggregate_startup_replay(
            db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?,
            &store,
            &config,
        )?;

        assert_eq!(progress.page_count, 1);
        assert!(!progress.gap_cursor_observed);
        assert!(!progress.reached_repair_target);
        assert_eq!(
            store.load_discovery_scoring_materialization_gap_cursor()?,
            Some(discovery_runtime_cursor_for_swap(&gap_swap)),
            "startup bounded replay must not clear a latch before observing the exact gap row"
        );
        assert_eq!(
            store.load_discovery_scoring_covered_through_cursor()?,
            Some(discovery_runtime_cursor_for_swap(&pre_gap_swap)),
            "startup bounded replay should advance only through the completed pre-gap page"
        );

        remove_sqlite_test_files(&db_path);
        Ok(())
    }
