    #[test]
    fn observed_swap_writer_aggregate_startup_replay_resumes_from_persisted_covered_through_after_gap_stage1(
    ) -> Result<()> {
        let db_path = migrated_observed_swap_writer_test_db(
            "copybot-app-aggregate-startup-resume-covered-through",
        )?;
        let store = SqliteStore::open(Path::new(&db_path))?;
        let covered_swap = aggregate_gap_replay_test_swap(
            "sig-startup-resume-covered-base",
            900,
            "2026-04-29T02:30:55Z",
        );
        let gap_swap = aggregate_gap_replay_test_swap(
            "sig-startup-resume-gap",
            901,
            "2026-04-29T02:30:58.295525837Z",
        );
        let persisted_covered_swap = aggregate_gap_replay_test_swap(
            "sig-startup-resume-persisted-covered",
            902,
            "2026-04-29T02:31:00Z",
        );
        let tail_swap =
            aggregate_gap_replay_test_swap("sig-startup-resume-tail", 903, "2026-04-29T02:31:05Z");
        store.insert_observed_swaps_batch(&[
            covered_swap.clone(),
            gap_swap.clone(),
            persisted_covered_swap.clone(),
            tail_swap.clone(),
        ])?;
        store.apply_discovery_scoring_batch(
            &[
                covered_swap.clone(),
                gap_swap.clone(),
                persisted_covered_swap.clone(),
            ],
            &aggregate_write_config(),
        )?;
        store.finalize_discovery_scoring_rug_facts(persisted_covered_swap.ts_utc)?;
        store.set_discovery_scoring_covered_through_cursor(&discovery_runtime_cursor_for_swap(
            &persisted_covered_swap,
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

        assert_eq!(
            progress.page_count, 1,
            "restart-safe startup replay should resume after persisted covered_through and only replay the remaining tail page"
        );
        assert!(
            progress.gap_cursor_observed,
            "persisted coverage beyond the gap reconstructs exact-gap-row proof"
        );
        assert!(
            progress.reached_repair_target,
            "startup replay should reach the frozen repair target from the persisted resume cursor"
        );
        assert_eq!(
            progress
                .last_replay_cursor
                .as_ref()
                .map(|cursor| cursor.signature.as_str()),
            Some(tail_swap.signature.as_str()),
            "startup replay must not restart from the original gap cursor"
        );
        assert_eq!(
            store.load_discovery_scoring_materialization_gap_cursor()?,
            None,
            "reconstructed gap proof may clear only after replay reaches the repair target"
        );
        assert_eq!(
            store.load_discovery_scoring_covered_through_cursor()?,
            Some(discovery_runtime_cursor_for_swap(&tail_swap)),
            "covered_through advances only after the remaining page fully succeeds"
        );

        remove_sqlite_test_files(&db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_aggregate_startup_replay_does_not_reconstruct_gap_proof_when_exact_row_missing_stage1(
    ) -> Result<()> {
        let db_path = migrated_observed_swap_writer_test_db(
            "copybot-app-aggregate-startup-resume-missing-gap",
        )?;
        let store = SqliteStore::open(Path::new(&db_path))?;
        let covered_swap = aggregate_gap_replay_test_swap(
            "sig-startup-missing-gap-covered-base",
            920,
            "2026-04-29T02:45:55Z",
        );
        let missing_gap_swap = aggregate_gap_replay_test_swap(
            "sig-startup-missing-gap-exact",
            921,
            "2026-04-29T02:45:58.295525837Z",
        );
        let persisted_covered_swap = aggregate_gap_replay_test_swap(
            "sig-startup-missing-gap-persisted-covered",
            922,
            "2026-04-29T02:46:00Z",
        );
        let tail_swap = aggregate_gap_replay_test_swap(
            "sig-startup-missing-gap-tail",
            923,
            "2026-04-29T02:46:05Z",
        );
        store.insert_observed_swaps_batch(&[
            covered_swap.clone(),
            persisted_covered_swap.clone(),
            tail_swap.clone(),
        ])?;
        store.apply_discovery_scoring_batch(
            &[covered_swap.clone(), persisted_covered_swap.clone()],
            &aggregate_write_config(),
        )?;
        store.finalize_discovery_scoring_rug_facts(persisted_covered_swap.ts_utc)?;
        store.set_discovery_scoring_covered_through_cursor(&discovery_runtime_cursor_for_swap(
            &persisted_covered_swap,
        ))?;
        store.set_discovery_scoring_materialization_gap_cursor(
            &discovery_runtime_cursor_for_swap(&missing_gap_swap),
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

        assert_eq!(
            progress.page_count, 0,
            "missing exact gap row must prevent replay from resuming after persisted covered_through"
        );
        assert!(
            !progress.gap_cursor_observed,
            "missing exact gap row must not reconstruct gap proof from coverage alone"
        );
        assert!(
            !progress.reached_repair_target,
            "missing exact gap row must not allow repair target completion"
        );
        assert_eq!(
            store.load_discovery_scoring_materialization_gap_cursor()?,
            Some(discovery_runtime_cursor_for_swap(&missing_gap_swap)),
            "missing exact gap row must keep materialization gap latched"
        );
        assert_eq!(
            store.load_discovery_scoring_covered_through_cursor()?,
            Some(discovery_runtime_cursor_for_swap(&persisted_covered_swap)),
            "missing exact gap row must not advance coverage to later tail rows"
        );

        remove_sqlite_test_files(&db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_aggregate_startup_replay_equal_covered_through_starts_from_gap_stage1(
    ) -> Result<()> {
        let db_path =
            migrated_observed_swap_writer_test_db("copybot-app-aggregate-startup-equal-gap")?;
        let store = SqliteStore::open(Path::new(&db_path))?;
        let gap_swap =
            aggregate_gap_replay_test_swap("sig-startup-equal-gap", 910, "2026-04-29T02:40:00Z");
        let tail_swap =
            aggregate_gap_replay_test_swap("sig-startup-equal-tail", 911, "2026-04-29T02:40:05Z");
        store.insert_observed_swaps_batch(&[gap_swap.clone(), tail_swap.clone()])?;
        store.set_discovery_scoring_covered_through_cursor(&discovery_runtime_cursor_for_swap(
            &gap_swap,
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
        assert!(
            progress.gap_cursor_observed,
            "equal covered_through must not reconstruct proof; it must replay and observe the gap row"
        );
        assert!(
            !progress.reached_repair_target,
            "one bounded page from the gap must not reach the later target"
        );
        assert_eq!(
            progress
                .last_replay_cursor
                .as_ref()
                .map(|cursor| cursor.signature.as_str()),
            Some(gap_swap.signature.as_str()),
            "equal covered_through should start from the gap boundary, not skip to tail"
        );
        assert_eq!(
            store.load_discovery_scoring_materialization_gap_cursor()?,
            Some(discovery_runtime_cursor_for_swap(&gap_swap)),
            "equal covered_through must keep the latch until target replay completes"
        );

        remove_sqlite_test_files(&db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_aggregate_gap_repair_phase_events_are_ordered_stage1() -> Result<()> {
        let db_path =
            migrated_observed_swap_writer_test_db("copybot-app-aggregate-repair-phase-events")?;
        let (_covered_swap, first_replay_swap, _tail_replay_swap) =
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
        let telemetry = ObservedSwapWriterTelemetry::default();
        let mut repair_epoch = super::DiscoveryAggregateGapRepairEpoch::default();
        super::clear_discovery_aggregate_phase_events_for_test();

        let ran_slice = super::run_discovery_aggregate_gap_repair_slice(
            db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?,
            &store,
            &config,
            &telemetry,
            true,
            &mut repair_epoch,
        )?;

        assert!(ran_slice);
        let events = super::discovery_aggregate_phase_events_for_test();
        let expected = [
            super::DISCOVERY_AGGREGATE_PHASE_GAP_REPAIR_SLICE_START,
            super::DISCOVERY_AGGREGATE_PHASE_PAGE_FETCH_START,
            super::DISCOVERY_AGGREGATE_PHASE_PAGE_FETCH_END,
            super::DISCOVERY_AGGREGATE_PHASE_APPLY_START,
            super::DISCOVERY_AGGREGATE_PHASE_APPLY_END,
            super::DISCOVERY_AGGREGATE_PHASE_RUG_FINALIZE_START,
            super::DISCOVERY_AGGREGATE_PHASE_RUG_FINALIZE_END,
            super::DISCOVERY_AGGREGATE_PHASE_COVERED_THROUGH_UPDATE_START,
            super::DISCOVERY_AGGREGATE_PHASE_COVERED_THROUGH_UPDATE_END,
            super::DISCOVERY_AGGREGATE_PHASE_GAP_REPAIR_SLICE_END,
        ];
        assert_eq!(
            events, expected,
            "aggregate repair phase telemetry must expose ordered bounded replay stages"
        );

        remove_sqlite_test_files(&db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_discovery_scoring_replay_apply_sqlite_lock_is_retryable_stage1(
    ) -> Result<()> {
        let db_path = migrated_observed_swap_writer_test_db(
            "copybot-app-observed-swap-replay-apply-lock-retryable",
        )?;
        let (covered_swap, first_replay_swap, _tail_replay_swap) =
            seed_discovery_scoring_rug_finalize_replay_fixture(&db_path)?;
        let trigger_conn = Connection::open(Path::new(&db_path))
            .context("failed to open sqlite db for replay apply retryable trigger")?;
        trigger_conn.execute_batch(
            "CREATE TRIGGER fail_replay_apply_day_retryable
             BEFORE INSERT ON wallet_scoring_days
             WHEN NEW.wallet_id = 'wallet-rug-finalize-replay'
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
            "retryable replay apply lock must latch the first failed replay row"
        );
        std::thread::sleep(StdDuration::from_millis(50));
        writer
            .ensure_running()
            .context("retryable replay apply sqlite lock must not become terminal")?;

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        assert_eq!(
            verify_store.load_discovery_scoring_covered_through_cursor()?,
            Some(DiscoveryRuntimeCursor {
                ts_utc: covered_swap.ts_utc,
                slot: covered_swap.slot,
                signature: covered_swap.signature.clone(),
            }),
            "coverage must not advance as if retryable replay apply completed"
        );

        writer.shutdown()?;
        drop(trigger_conn);
        remove_sqlite_test_files(&db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_discovery_scoring_replay_apply_unknown_error_remains_terminal_stage1(
    ) -> Result<()> {
        let db_path = migrated_observed_swap_writer_test_db(
            "copybot-app-observed-swap-replay-apply-unknown-fatal",
        )?;
        let (covered_swap, first_replay_swap, _tail_replay_swap) =
            seed_discovery_scoring_rug_finalize_replay_fixture(&db_path)?;
        let trigger_conn = Connection::open(Path::new(&db_path))
            .context("failed to open sqlite db for replay apply terminal trigger")?;
        trigger_conn.execute_batch(
            "CREATE TRIGGER fail_replay_apply_buy_fact_terminal
             BEFORE INSERT ON wallet_scoring_buy_facts
             WHEN NEW.buy_signature = 'sig-rug-finalize-replay-first'
             BEGIN
                 SELECT RAISE(FAIL, 'forced replay apply failure');
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
            error_chain.contains(
                "failed replaying discovery scoring rows during aggregate-writer startup catch-up"
            ),
            "unknown replay apply errors must keep explicit terminal context: {error_chain}"
        );
        assert!(
            error_chain.contains("forced replay apply failure"),
            "unknown replay apply error detail must remain visible: {error_chain}"
        );

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        assert_eq!(
            verify_store.load_discovery_scoring_materialization_gap_cursor()?,
            Some(DiscoveryRuntimeCursor {
                ts_utc: first_replay_swap.ts_utc,
                slot: first_replay_swap.slot,
                signature: first_replay_swap.signature.clone(),
            }),
            "terminal replay apply failure should keep explicit gap evidence"
        );
        assert_eq!(
            verify_store.load_discovery_scoring_covered_through_cursor()?,
            Some(DiscoveryRuntimeCursor {
                ts_utc: covered_swap.ts_utc,
                slot: covered_swap.slot,
                signature: covered_swap.signature.clone(),
            }),
            "terminal replay apply failure must not advance coverage"
        );

        let shutdown_error = writer
            .shutdown()
            .expect_err("shutdown should surface terminal replay apply failure");
        let shutdown_chain = format!("{shutdown_error:#}");
        assert!(
            shutdown_chain.contains(
                "failed replaying discovery scoring rows during aggregate-writer startup catch-up"
            ),
            "unexpected shutdown error: {shutdown_chain}"
        );
        drop(trigger_conn);
        remove_sqlite_test_files(&db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_discovery_scoring_covered_through_update_sqlite_lock_is_retryable_stage1(
    ) -> Result<()> {
        let db_path = migrated_observed_swap_writer_test_db(
            "copybot-app-observed-swap-covered-through-lock-retryable",
        )?;
        let (covered_swap, first_replay_swap, _tail_replay_swap) =
            seed_discovery_scoring_rug_finalize_replay_fixture(&db_path)?;
        let trigger_conn = Connection::open(Path::new(&db_path))
            .context("failed to open sqlite db for covered-through retryable trigger")?;
        trigger_conn.execute_batch(
            "CREATE TRIGGER fail_covered_through_update_retryable
             BEFORE UPDATE ON discovery_scoring_state
             WHEN OLD.state_key = 'covered_through_ts'
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
            "retryable covered-through update lock must latch the first replay row"
        );
        std::thread::sleep(StdDuration::from_millis(50));
        writer
            .ensure_running()
            .context("retryable covered-through sqlite lock must not become terminal")?;

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        assert_eq!(
            verify_store.load_discovery_scoring_covered_through_cursor()?,
            Some(DiscoveryRuntimeCursor {
                ts_utc: covered_swap.ts_utc,
                slot: covered_swap.slot,
                signature: covered_swap.signature.clone(),
            }),
            "coverage must not advance when covered-through cursor update is retryably locked"
        );

        writer.shutdown()?;
        drop(trigger_conn);
        remove_sqlite_test_files(&db_path);
        Ok(())
    }
