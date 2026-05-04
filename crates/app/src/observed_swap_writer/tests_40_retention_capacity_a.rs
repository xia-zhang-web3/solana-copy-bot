    #[test]
    fn observed_swap_writer_discovery_scoring_covered_through_update_unknown_error_remains_terminal_stage1(
    ) -> Result<()> {
        let db_path = migrated_observed_swap_writer_test_db(
            "copybot-app-observed-swap-covered-through-unknown-fatal",
        )?;
        let (covered_swap, _first_replay_swap, _tail_replay_swap) =
            seed_discovery_scoring_rug_finalize_replay_fixture(&db_path)?;
        let trigger_conn = Connection::open(Path::new(&db_path))
            .context("failed to open sqlite db for covered-through terminal trigger")?;
        trigger_conn.execute_batch(
            "CREATE TRIGGER fail_covered_through_update_terminal
             BEFORE UPDATE ON discovery_scoring_state
             WHEN OLD.state_key = 'covered_through_ts'
             BEGIN
                 SELECT RAISE(FAIL, 'forced covered_through update failure');
             END;",
        )?;

        let writer = ObservedSwapWriter::start_with_config(
            db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?
                .to_string(),
            ObservedSwapWriterConfig::for_test(16, 8, true, aggregate_write_config(), None),
        )?;

        let error_chain = wait_for_writer_terminal_failure(&writer)?;
        assert!(
            error_chain.contains("failed to run discovery scoring covered_through cursor update"),
            "unknown covered-through update errors must keep explicit terminal context: {error_chain}"
        );
        assert!(
            error_chain.contains("forced covered_through update failure"),
            "unknown covered-through update error detail must remain visible: {error_chain}"
        );

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        assert_eq!(
            verify_store.load_discovery_scoring_covered_through_cursor()?,
            Some(DiscoveryRuntimeCursor {
                ts_utc: covered_swap.ts_utc,
                slot: covered_swap.slot,
                signature: covered_swap.signature.clone(),
            }),
            "terminal covered-through update failure must not advance coverage"
        );

        let shutdown_error = writer
            .shutdown()
            .expect_err("shutdown should surface terminal covered-through update failure");
        let shutdown_chain = format!("{shutdown_error:#}");
        assert!(
            shutdown_chain
                .contains("failed to run discovery scoring covered_through cursor update"),
            "unexpected shutdown error: {shutdown_chain}"
        );
        drop(trigger_conn);
        remove_sqlite_test_files(&db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_discovery_scoring_rug_finalize_unknown_error_remains_terminal_stage1(
    ) -> Result<()> {
        let db_path =
            migrated_observed_swap_writer_test_db("copybot-app-observed-swap-rug-unknown-fatal")?;
        let (covered_swap, _first_replay_swap, _tail_replay_swap) =
            seed_discovery_scoring_rug_finalize_replay_fixture(&db_path)?;
        let trigger_conn = Connection::open(Path::new(&db_path))
            .context("failed to open sqlite db for rug finalize terminal trigger")?;
        trigger_conn.execute_batch(
            "CREATE TRIGGER fail_rug_finalize_update_terminal
             BEFORE UPDATE OF rug_volume_lookahead_sol ON wallet_scoring_buy_facts
             WHEN OLD.buy_signature = 'sig-rug-finalize-replay-first'
             BEGIN
                 SELECT RAISE(FAIL, 'forced rug finalize failure');
             END;",
        )?;

        let writer = ObservedSwapWriter::start_with_config(
            db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?
                .to_string(),
            ObservedSwapWriterConfig::for_test(16, 8, true, aggregate_write_config(), None),
        )?;

        let error_chain = wait_for_writer_terminal_failure(&writer)?;
        assert!(
            error_chain.contains("failed to run discovery scoring rug finalize"),
            "unknown rug-finalize errors must keep explicit terminal context: {error_chain}"
        );
        assert!(
            error_chain.contains("forced rug finalize failure"),
            "unknown rug-finalize error detail must remain visible: {error_chain}"
        );

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        assert_eq!(
            verify_store.load_discovery_scoring_covered_through_cursor()?,
            Some(DiscoveryRuntimeCursor {
                ts_utc: covered_swap.ts_utc,
                slot: covered_swap.slot,
                signature: covered_swap.signature.clone(),
            }),
            "terminal rug-finalize failure must not advance coverage"
        );

        let shutdown_error = writer
            .shutdown()
            .expect_err("shutdown should surface terminal rug-finalize failure");
        let shutdown_chain = format!("{shutdown_error:#}");
        assert!(
            shutdown_chain.contains("failed to run discovery scoring rug finalize"),
            "unexpected shutdown error: {shutdown_chain}"
        );
        drop(trigger_conn);
        remove_sqlite_test_files(&db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_reports_terminal_failure_after_fatal_discovery_scoring_materialization_failure(
    ) -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-aggregate-fatal-{}-{}",
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
            .context("failed to open sqlite db for aggregate fatal trigger")?;
        trigger_conn.execute_batch(
            "CREATE TRIGGER fail_wallet_scoring_days_insert
             BEFORE INSERT ON wallet_scoring_days
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
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
            wallet: "wallet-aggregate-fatal".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-aggregate-fatal".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-observed-swap-aggregate-fatal".to_string(),
            slot: 220,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-14T13:20:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };

        runtime.block_on(async { writer.enqueue(&failing_swap).await })?;

        let error_chain = wait_for_writer_terminal_failure(&writer)?;
        assert!(
            error_chain.contains(super::OBSERVED_SWAP_WRITER_TERMINAL_FAILURE_CONTEXT),
            "unexpected terminal aggregate failure error: {error_chain}"
        );
        assert!(
            error_chain.contains("fatal discovery scoring materialization failure"),
            "missing aggregate fatal context: {error_chain}"
        );
        assert!(
            error_chain.contains("xShmMap"),
            "missing fatal sqlite marker: {error_chain}"
        );

        let shutdown_error = writer
            .shutdown()
            .expect_err("shutdown should surface fatal aggregate materialization failure");
        let shutdown_chain = format!("{shutdown_error:#}");
        assert!(
            shutdown_chain.contains("fatal discovery scoring materialization failure"),
            "unexpected shutdown error: {shutdown_chain}"
        );

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        assert_eq!(
            verify_store
                .load_discovery_scoring_materialization_gap_cursor()?
                .expect("fatal aggregate failure should still latch materialization gap"),
            DiscoveryRuntimeCursor {
                ts_utc: failing_swap.ts_utc,
                slot: failing_swap.slot,
                signature: failing_swap.signature.clone(),
            }
        );
        drop(trigger_conn);
        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_reports_terminal_failure_after_fatal_discovery_scoring_gap_cursor_failure(
    ) -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-gap-fatal-{}-{}",
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
            .context("failed to open sqlite db for gap fatal trigger")?;
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
            wallet: "wallet-gap-fatal".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-gap-fatal".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-observed-swap-gap-fatal".to_string(),
            slot: 221,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-14T13:21:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };

        runtime.block_on(async { writer.enqueue(&failing_swap).await })?;

        let error_chain = wait_for_writer_terminal_failure(&writer)?;
        assert!(
            error_chain.contains(super::OBSERVED_SWAP_WRITER_TERMINAL_FAILURE_CONTEXT),
            "unexpected terminal gap-cursor failure error: {error_chain}"
        );
        assert!(
            error_chain.contains("fatal discovery scoring gap cursor failure"),
            "missing gap-cursor fatal context: {error_chain}"
        );
        assert!(
            error_chain.contains("xShmMap"),
            "missing fatal sqlite marker: {error_chain}"
        );

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        assert_eq!(
            verify_store.load_discovery_scoring_materialization_gap_cursor()?,
            None,
            "fatal gap cursor failure must leave the materialization gap cursor unset"
        );

        let shutdown_error = writer
            .shutdown()
            .expect_err("shutdown should surface fatal gap cursor failure");
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
    fn observed_swap_writer_reports_terminal_failure_after_fatal_discovery_scoring_coverage_watermark_failure(
    ) -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-covered-through-fatal-{}-{}",
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
            .context("failed to open sqlite db for covered-through fatal trigger")?;
        trigger_conn.execute_batch(
            "CREATE TRIGGER fail_discovery_scoring_state_insert
             BEFORE INSERT ON discovery_scoring_state
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
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
            wallet: "wallet-covered-through-fatal".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-covered-through-fatal".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-observed-swap-covered-through-fatal".to_string(),
            slot: 222,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-14T13:22:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };

        runtime.block_on(async { writer.enqueue(&failing_swap).await })?;

        let error_chain = wait_for_writer_terminal_failure(&writer)?;
        assert!(
            error_chain.contains(super::OBSERVED_SWAP_WRITER_TERMINAL_FAILURE_CONTEXT),
            "unexpected terminal coverage watermark failure error: {error_chain}"
        );
        assert!(
            error_chain.contains("fatal discovery scoring coverage watermark failure"),
            "missing coverage watermark fatal context: {error_chain}"
        );
        assert!(
            error_chain.contains("xShmMap"),
            "missing fatal sqlite marker: {error_chain}"
        );

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        assert_eq!(
            verify_store.load_discovery_scoring_covered_through_cursor()?,
            None,
            "fatal coverage watermark failure must leave covered-through cursor unset"
        );

        let shutdown_error = writer
            .shutdown()
            .expect_err("shutdown should surface fatal coverage watermark failure");
        let shutdown_chain = format!("{shutdown_error:#}");
        assert!(
            shutdown_chain.contains("fatal discovery scoring coverage watermark failure"),
            "unexpected shutdown error: {shutdown_chain}"
        );
        drop(trigger_conn);
        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_keeps_retention_out_of_inline_batch_path() -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-retention-{}-{}",
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

            let stale_swap = SwapEvent {
                wallet: "wallet-old".to_string(),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: "token-old".to_string(),
                amount_in: 1.0,
                amount_out: 10.0,
                signature: "sig-observed-swap-old".to_string(),
                slot: 100,
                ts_utc: Utc::now() - ChronoDuration::days(3),
                exact_amounts: None,
            };
            let fresh_swap = SwapEvent {
                wallet: "wallet-new".to_string(),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: "token-new".to_string(),
                amount_in: 2.0,
                amount_out: 20.0,
                signature: "sig-observed-swap-new".to_string(),
                slot: 101,
                ts_utc: Utc::now(),
                exact_amounts: None,
            };

            writer.write(&stale_swap).await?;
            writer.write(&fresh_swap).await?;
            writer.shutdown()?;
            Ok::<(), anyhow::Error>(())
        })?;

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        let swaps_before_maintenance =
            verify_store.load_observed_swaps_since(Utc::now() - ChronoDuration::days(7))?;
        assert_eq!(
            swaps_before_maintenance.len(),
            2,
            "writer should no longer prune stale rows inline while inserting fresh observed swaps"
        );

        let summary = super::run_observed_swap_retention_maintenance_once(
            db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?,
            super::ObservedSwapRetentionConfig::production(1, 7, true),
            None,
        )?;
        assert_eq!(summary.raw_deleted_rows, 1);
        assert_eq!(summary.raw_delete_batches, 1);
        assert_eq!(summary.checkpoint.mode, "passive_runtime");

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        let swaps_after_maintenance =
            verify_store.load_observed_swaps_since(Utc::now() - ChronoDuration::days(7))?;
        assert_eq!(swaps_after_maintenance.len(), 1);
        assert_eq!(
            swaps_after_maintenance[0].signature,
            "sig-observed-swap-new"
        );
        let _ = std::fs::remove_file(db_path);

        Ok(())
    }
