    #[test]
    fn observed_swap_writer_aggregate_startup_pending_does_not_block_raw_insert_stage1(
    ) -> Result<()> {
        let db_path = migrated_observed_swap_writer_test_db(
            "copybot-app-observed-swap-aggregate-startup-pending",
        )?;
        let (startup_sender, startup_receiver) =
            std_mpsc::channel::<std::result::Result<(), String>>();
        let (aggregate_sender, _aggregate_receiver) =
            std_mpsc::sync_channel::<super::DiscoveryAggregateWriteRequest>(4);
        let writer = start_observed_swap_writer_loop_for_startup_test(
            &db_path,
            ObservedSwapWriterConfig::for_test(4, 1, true, aggregate_write_config(), None),
            Some(aggregate_sender),
            Some(startup_receiver),
            None,
            None,
        )?;

        let swap = startup_gate_test_swap("sig-aggregate-startup-pending-raw-insert", 700);
        assert!(
            writer.try_enqueue(&swap)?,
            "raw enqueue should be accepted while aggregate startup is pending"
        );
        let swaps = wait_for_observed_swap_signatures(&db_path, &[&swap.signature])?;
        assert!(
            swaps
                .iter()
                .any(|persisted| persisted.signature == swap.signature),
            "raw observed_swaps insert should not wait for aggregate startup replay"
        );

        startup_sender
            .send(Ok(()))
            .context("failed sending aggregate startup ok")?;
        writer.shutdown()?;
        remove_sqlite_test_files(&db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_recent_raw_startup_pending_does_not_block_raw_insert_stage1(
    ) -> Result<()> {
        let db_path = migrated_observed_swap_writer_test_db(
            "copybot-app-observed-swap-recent-raw-startup-pending",
        )?;
        let (startup_sender, startup_receiver) =
            std_mpsc::channel::<std::result::Result<(), String>>();
        let (journal_sender, _journal_receiver) =
            std_mpsc::sync_channel::<super::RecentRawJournalWriteRequest>(4);
        let writer = start_observed_swap_writer_loop_for_startup_test(
            &db_path,
            ObservedSwapWriterConfig::for_test(4, 1, false, aggregate_write_config(), None),
            None,
            None,
            Some(journal_sender),
            Some(startup_receiver),
        )?;

        let swap = startup_gate_test_swap("sig-recent-raw-startup-pending-raw-insert", 701);
        assert!(
            writer.try_enqueue(&swap)?,
            "raw enqueue should be accepted while recent_raw startup is pending"
        );
        let swaps = wait_for_observed_swap_signatures(&db_path, &[&swap.signature])?;
        assert!(
            swaps
                .iter()
                .any(|persisted| persisted.signature == swap.signature),
            "raw observed_swaps insert should not wait for recent_raw journal startup/prune"
        );

        startup_sender
            .send(Ok(()))
            .context("failed sending recent_raw startup ok")?;
        writer.shutdown()?;
        remove_sqlite_test_files(&db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_downstream_startup_error_stops_fail_closed_stage1() -> Result<()> {
        let db_path =
            migrated_observed_swap_writer_test_db("copybot-app-observed-swap-startup-error")?;
        let (startup_sender, startup_receiver) =
            std_mpsc::channel::<std::result::Result<(), String>>();
        let writer = start_observed_swap_writer_loop_for_startup_test(
            &db_path,
            ObservedSwapWriterConfig::for_test(4, 1, true, aggregate_write_config(), None),
            None,
            Some(startup_receiver),
            None,
            None,
        )?;

        startup_sender
            .send(Err("aggregate startup failed for test".to_string()))
            .context("failed sending aggregate startup error")?;
        let terminal_failure = wait_for_writer_terminal_failure(&writer)?;
        assert!(
            terminal_failure
                .contains("observed swap writer stopping after aggregate startup replay failure"),
            "terminal failure should include aggregate startup context: {terminal_failure}"
        );
        assert!(
            terminal_failure.contains("aggregate startup failed for test"),
            "terminal failure should include downstream startup error: {terminal_failure}"
        );

        let shutdown_error = writer
            .shutdown()
            .expect_err("startup failure should make writer shutdown fail closed");
        let shutdown_error_text = format!("{shutdown_error:#}");
        assert!(
            shutdown_error_text
                .contains("observed swap writer stopping after aggregate startup replay failure"),
            "shutdown error should preserve startup context: {shutdown_error_text}"
        );
        remove_sqlite_test_files(&db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_aggregate_gap_replay_clears_gap_behind_covered_through_after_observing_exact_row_stage1(
    ) -> Result<()> {
        let db_path = migrated_observed_swap_writer_test_db(
            "copybot-app-observed-swap-gap-behind-covered-clear",
        )?;
        let store = SqliteStore::open(Path::new(&db_path))?;
        let gap_swap = aggregate_gap_replay_test_swap(
            "sig-gap-behind-covered-exact",
            800,
            "2026-04-28T13:00:00Z",
        );
        let tail_swap = aggregate_gap_replay_test_swap(
            "sig-gap-behind-covered-tail",
            801,
            "2026-04-28T13:00:05Z",
        );
        store.insert_observed_swaps_batch(&[gap_swap.clone(), tail_swap.clone()])?;
        let gap_cursor = DiscoveryRuntimeCursor {
            ts_utc: gap_swap.ts_utc,
            slot: gap_swap.slot,
            signature: gap_swap.signature.clone(),
        };
        let tail_cursor = DiscoveryRuntimeCursor {
            ts_utc: tail_swap.ts_utc,
            slot: tail_swap.slot,
            signature: tail_swap.signature.clone(),
        };
        store.set_discovery_scoring_covered_through_cursor(&tail_cursor)?;
        store.set_discovery_scoring_materialization_gap_cursor(&gap_cursor)?;

        let progress = super::run_aggregate_gap_replay(
            &store,
            &ObservedSwapWriterConfig::for_test(16, 8, true, aggregate_write_config(), None),
            None,
        )?;

        assert!(
            progress.gap_cursor_observed,
            "gap replay must observe the exact latched row before clearing"
        );
        assert!(
            progress.caught_up_to_tail,
            "unbounded gap replay should catch up to tail before clearing"
        );
        assert_eq!(
            store.load_discovery_scoring_materialization_gap_cursor()?,
            None,
            "gap cursor behind covered_through should clear only after exact row observation and tail catch-up"
        );
        assert_eq!(
            store.load_discovery_scoring_covered_through_cursor()?,
            Some(tail_cursor),
            "gap replay must not move covered_through backwards"
        );
        remove_sqlite_test_files(&db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_aggregate_gap_replay_keeps_gap_behind_covered_through_when_exact_row_missing_stage1(
    ) -> Result<()> {
        let db_path = migrated_observed_swap_writer_test_db(
            "copybot-app-observed-swap-gap-behind-covered-missing",
        )?;
        let store = SqliteStore::open(Path::new(&db_path))?;
        let missing_gap = aggregate_gap_replay_test_swap(
            "sig-gap-behind-covered-missing",
            810,
            "2026-04-28T13:05:00Z",
        );
        let tail_swap = aggregate_gap_replay_test_swap(
            "sig-gap-behind-covered-missing-tail",
            811,
            "2026-04-28T13:05:05Z",
        );
        store.insert_observed_swaps_batch(&[tail_swap.clone()])?;
        let gap_cursor = DiscoveryRuntimeCursor {
            ts_utc: missing_gap.ts_utc,
            slot: missing_gap.slot,
            signature: missing_gap.signature.clone(),
        };
        let tail_cursor = DiscoveryRuntimeCursor {
            ts_utc: tail_swap.ts_utc,
            slot: tail_swap.slot,
            signature: tail_swap.signature.clone(),
        };
        store.set_discovery_scoring_covered_through_cursor(&tail_cursor)?;
        store.set_discovery_scoring_materialization_gap_cursor(&gap_cursor)?;

        let progress = super::run_aggregate_gap_replay(
            &store,
            &ObservedSwapWriterConfig::for_test(16, 8, true, aggregate_write_config(), None),
            None,
        )?;

        assert!(
            !progress.gap_cursor_observed,
            "gap replay must not claim observation when the exact latched row is absent"
        );
        assert!(
            !progress.caught_up_to_tail,
            "replay must stop before claiming tail catch-up when the exact latched gap row is missing"
        );
        assert_eq!(
            store.load_discovery_scoring_materialization_gap_cursor()?,
            Some(gap_cursor),
            "missing exact latched row must keep materialization gap fail-closed"
        );
        assert_eq!(
            store.load_discovery_scoring_covered_through_cursor()?,
            Some(tail_cursor),
            "missing-gap replay must not regress covered_through"
        );
        remove_sqlite_test_files(&db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_aggregate_gap_replay_partial_gap_behind_covered_through_does_not_move_coverage_backwards_stage1(
    ) -> Result<()> {
        let db_path = migrated_observed_swap_writer_test_db(
            "copybot-app-observed-swap-gap-behind-covered-partial",
        )?;
        let store = SqliteStore::open(Path::new(&db_path))?;
        let gap_swap = aggregate_gap_replay_test_swap(
            "sig-gap-behind-covered-partial",
            820,
            "2026-04-28T13:10:00Z",
        );
        let tail_swap = aggregate_gap_replay_test_swap(
            "sig-gap-behind-covered-partial-tail",
            821,
            "2026-04-28T13:10:05Z",
        );
        store.insert_observed_swaps_batch(&[gap_swap.clone(), tail_swap.clone()])?;
        let gap_cursor = DiscoveryRuntimeCursor {
            ts_utc: gap_swap.ts_utc,
            slot: gap_swap.slot,
            signature: gap_swap.signature.clone(),
        };
        let tail_cursor = DiscoveryRuntimeCursor {
            ts_utc: tail_swap.ts_utc,
            slot: tail_swap.slot,
            signature: tail_swap.signature.clone(),
        };
        store.set_discovery_scoring_covered_through_cursor(&tail_cursor)?;
        store.set_discovery_scoring_materialization_gap_cursor(&gap_cursor)?;

        let progress = super::run_aggregate_gap_replay(
            &store,
            &ObservedSwapWriterConfig::for_test(16, 1, true, aggregate_write_config(), None),
            Some(1),
        )?;

        assert!(
            progress.gap_cursor_observed,
            "partial replay should still observe the exact latched row when starting from the gap boundary"
        );
        assert!(
            !progress.caught_up_to_tail,
            "one-row bounded replay should not claim tail catch-up"
        );
        assert_eq!(
            store.load_discovery_scoring_materialization_gap_cursor()?,
            Some(gap_cursor),
            "partial replay must keep gap latched until tail catch-up"
        );
        assert_eq!(
            store.load_discovery_scoring_covered_through_cursor()?,
            Some(tail_cursor),
            "bounded replay from behind covered_through must not regress covered_through"
        );
        remove_sqlite_test_files(&db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_aggregate_gap_repair_clears_at_frozen_target_while_tail_moves_stage1(
    ) -> Result<()> {
        let db_path = migrated_observed_swap_writer_test_db(
            "copybot-app-observed-swap-gap-frozen-target-tail-moves",
        )?;
        let store = SqliteStore::open(Path::new(&db_path))?;
        let covered_swap = aggregate_gap_replay_test_swap(
            "sig-gap-frozen-target-covered",
            824,
            "2026-04-28T13:11:55Z",
        );
        let gap_swap = aggregate_gap_replay_test_swap(
            "sig-gap-frozen-target-exact",
            825,
            "2026-04-28T13:12:00Z",
        );
        let target_swap = aggregate_gap_replay_test_swap(
            "sig-gap-frozen-target-target",
            826,
            "2026-04-28T13:12:05Z",
        );
        store.insert_observed_swaps_batch(&[
            covered_swap.clone(),
            gap_swap.clone(),
            target_swap.clone(),
        ])?;
        store.apply_discovery_scoring_batch(&[covered_swap.clone()], &aggregate_write_config())?;
        let gap_cursor = DiscoveryRuntimeCursor {
            ts_utc: gap_swap.ts_utc,
            slot: gap_swap.slot,
            signature: gap_swap.signature.clone(),
        };
        let target_cursor = DiscoveryRuntimeCursor {
            ts_utc: target_swap.ts_utc,
            slot: target_swap.slot,
            signature: target_swap.signature.clone(),
        };
        store.set_discovery_scoring_covered_through_cursor(&discovery_runtime_cursor_for_swap(
            &covered_swap,
        ))?;
        store.set_discovery_scoring_materialization_gap_cursor(&gap_cursor)?;
        let config = ObservedSwapWriterConfig::for_test_with_aggregate_tuning(
            16,
            1,
            true,
            aggregate_write_config(),
            1,
            16,
            true,
            1,
            None,
        );
        let mut repair_epoch = super::DiscoveryAggregateGapRepairEpoch::default();

        super::run_discovery_aggregate_gap_repair_slice(
            db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?,
            &store,
            &config,
            &ObservedSwapWriterTelemetry::default(),
            true,
            &mut repair_epoch,
        )?;
        assert_eq!(
            repair_epoch.repair_target_cursor,
            Some(target_cursor.clone()),
            "repair epoch must freeze the observed_swaps tail as its target"
        );
        assert_eq!(
            store.load_discovery_scoring_materialization_gap_repair_target()?,
            Some((gap_cursor.clone(), target_cursor.clone())),
            "newly frozen repair target must be persisted before replay continues"
        );
        assert_eq!(
            store.load_discovery_scoring_materialization_gap_cursor()?,
            Some(gap_cursor.clone()),
            "first bounded slice observed the exact gap but has not reached the frozen target"
        );

        let later_swap = aggregate_gap_replay_test_swap(
            "sig-gap-frozen-target-later-live-tail",
            827,
            "2026-04-28T13:12:10Z",
        );
        let later_cursor = DiscoveryRuntimeCursor {
            ts_utc: later_swap.ts_utc,
            slot: later_swap.slot,
            signature: later_swap.signature.clone(),
        };
        store.insert_observed_swaps_batch(&[later_swap])?;
        store.set_discovery_scoring_covered_through_cursor(&later_cursor)?;

        super::run_discovery_aggregate_gap_repair_slice(
            db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?,
            &store,
            &config,
            &ObservedSwapWriterTelemetry::default(),
            true,
            &mut repair_epoch,
        )?;
        assert_eq!(
            store.load_discovery_scoring_materialization_gap_cursor()?,
            None,
            "repair must clear after reaching the frozen target without chasing the moving live tail"
        );
        assert_eq!(
            store.load_discovery_scoring_materialization_gap_repair_target()?,
            None,
            "guarded gap clear must clear the persisted repair target"
        );
        assert_eq!(
            store.load_discovery_scoring_covered_through_cursor()?,
            Some(later_cursor),
            "bounded repair must not regress covered_through when live traffic already advanced it"
        );
        remove_sqlite_test_files(&db_path);
        Ok(())
    }
