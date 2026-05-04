    #[test]
    fn observed_swap_writer_aggregate_gap_repair_reuses_persisted_target_after_restart_stage1(
    ) -> Result<()> {
        let db_path = migrated_observed_swap_writer_test_db(
            "copybot-app-observed-swap-gap-repair-persisted-target",
        )?;
        let store = SqliteStore::open(Path::new(&db_path))?;
        let covered_swap = aggregate_gap_replay_test_swap(
            "sig-gap-persisted-target-covered",
            900,
            "2026-04-29T13:00:00Z",
        );
        let gap_swap = aggregate_gap_replay_test_swap(
            "sig-gap-persisted-target-gap",
            901,
            "2026-04-29T13:00:05Z",
        );
        let persisted_target_swap = aggregate_gap_replay_test_swap(
            "sig-gap-persisted-target-target",
            902,
            "2026-04-29T13:00:10Z",
        );
        let live_tail_swap = aggregate_gap_replay_test_swap(
            "sig-gap-persisted-target-live-tail",
            903,
            "2026-04-29T13:00:15Z",
        );
        store.insert_observed_swaps_batch(&[
            covered_swap.clone(),
            gap_swap.clone(),
            persisted_target_swap.clone(),
            live_tail_swap.clone(),
        ])?;
        store.apply_discovery_scoring_batch(&[covered_swap.clone()], &aggregate_write_config())?;
        let gap_cursor = discovery_runtime_cursor_for_swap(&gap_swap);
        let persisted_target_cursor = discovery_runtime_cursor_for_swap(&persisted_target_swap);
        let live_tail_cursor = discovery_runtime_cursor_for_swap(&live_tail_swap);
        store.set_discovery_scoring_covered_through_cursor(&discovery_runtime_cursor_for_swap(
            &covered_swap,
        ))?;
        store.set_discovery_scoring_materialization_gap_cursor(&gap_cursor)?;
        store.set_discovery_scoring_materialization_gap_repair_target(
            &gap_cursor,
            &persisted_target_cursor,
        )?;
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
            Some(persisted_target_cursor),
            "restart repair must use the persisted frozen target"
        );
        assert_ne!(
            repair_epoch.repair_target_cursor,
            Some(live_tail_cursor),
            "repair target must not be refrozen to the newer live tail"
        );

        remove_sqlite_test_files(&db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_aggregate_gap_repair_resets_epoch_when_gap_cursor_changes_stage1(
    ) -> Result<()> {
        let db_path = migrated_observed_swap_writer_test_db(
            "copybot-app-observed-swap-gap-repair-epoch-reset",
        )?;
        let store = SqliteStore::open(Path::new(&db_path))?;
        let covered_swap = aggregate_gap_replay_test_swap(
            "sig-gap-epoch-reset-covered",
            824,
            "2026-04-28T13:11:55Z",
        );
        let first_gap_swap = aggregate_gap_replay_test_swap(
            "sig-gap-epoch-reset-first",
            825,
            "2026-04-28T13:12:00Z",
        );
        let second_gap_swap = aggregate_gap_replay_test_swap(
            "sig-gap-epoch-reset-second",
            826,
            "2026-04-28T13:12:05Z",
        );
        let target_swap = aggregate_gap_replay_test_swap(
            "sig-gap-epoch-reset-target",
            827,
            "2026-04-28T13:12:10Z",
        );
        store.insert_observed_swaps_batch(&[
            covered_swap.clone(),
            first_gap_swap.clone(),
            second_gap_swap.clone(),
            target_swap.clone(),
        ])?;
        store.apply_discovery_scoring_batch(&[covered_swap.clone()], &aggregate_write_config())?;
        let second_gap_cursor = DiscoveryRuntimeCursor {
            ts_utc: second_gap_swap.ts_utc,
            slot: second_gap_swap.slot,
            signature: second_gap_swap.signature.clone(),
        };
        let first_gap_cursor = DiscoveryRuntimeCursor {
            ts_utc: first_gap_swap.ts_utc,
            slot: first_gap_swap.slot,
            signature: first_gap_swap.signature.clone(),
        };
        store.set_discovery_scoring_covered_through_cursor(&discovery_runtime_cursor_for_swap(
            &covered_swap,
        ))?;
        store.set_discovery_scoring_materialization_gap_cursor(&first_gap_cursor)?;
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
        assert_eq!(repair_epoch.gap_cursor, Some(first_gap_cursor.clone()));
        assert_eq!(
            repair_epoch
                .resume_after_cursor
                .as_ref()
                .map(|cursor| cursor.signature.as_str()),
            Some("sig-gap-epoch-reset-first")
        );

        store.clear_discovery_scoring_materialization_gap_if_cursor_observed(&first_gap_cursor)?;
        store.set_discovery_scoring_materialization_gap_cursor(&second_gap_cursor)?;
        assert_eq!(
            store.load_discovery_scoring_materialization_gap_repair_target()?,
            None,
            "changing the materialization gap cursor must clear the stale persisted repair target"
        );
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
            repair_epoch.gap_cursor,
            Some(second_gap_cursor.clone()),
            "changed gap cursor must discard the previous repair epoch"
        );
        assert_eq!(
            repair_epoch
                .resume_after_cursor
                .as_ref()
                .map(|cursor| cursor.signature.as_str()),
            Some("sig-gap-epoch-reset-second"),
            "new epoch must resume from the new gap cursor, not the stale old cursor"
        );
        assert_eq!(
            store
                .load_discovery_scoring_materialization_gap_repair_target()?
                .map(|(gap, _target)| gap),
            Some(second_gap_cursor.clone()),
            "new repair epoch must persist a target tied to the new materialization gap"
        );
        assert_eq!(
            store.load_discovery_scoring_materialization_gap_cursor()?,
            Some(second_gap_cursor),
            "one bounded slice must not clear before the frozen target is reached"
        );
        remove_sqlite_test_files(&db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_aggregate_gap_repair_partial_slices_keep_latch_until_target_stage1(
    ) -> Result<()> {
        let db_path = migrated_observed_swap_writer_test_db(
            "copybot-app-observed-swap-gap-repair-partial-target",
        )?;
        let store = SqliteStore::open(Path::new(&db_path))?;
        let covered_swap = aggregate_gap_replay_test_swap(
            "sig-gap-partial-target-covered",
            824,
            "2026-04-28T13:11:55Z",
        );
        let gap_swap = aggregate_gap_replay_test_swap(
            "sig-gap-partial-target-exact",
            825,
            "2026-04-28T13:12:00Z",
        );
        let middle_swap = aggregate_gap_replay_test_swap(
            "sig-gap-partial-target-middle",
            826,
            "2026-04-28T13:12:05Z",
        );
        let target_swap = aggregate_gap_replay_test_swap(
            "sig-gap-partial-target-target",
            827,
            "2026-04-28T13:12:10Z",
        );
        store.insert_observed_swaps_batch(&[
            covered_swap.clone(),
            gap_swap.clone(),
            middle_swap.clone(),
            target_swap.clone(),
        ])?;
        store.apply_discovery_scoring_batch(&[covered_swap.clone()], &aggregate_write_config())?;
        let gap_cursor = DiscoveryRuntimeCursor {
            ts_utc: gap_swap.ts_utc,
            slot: gap_swap.slot,
            signature: gap_swap.signature.clone(),
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

        for expected_resume_signature in [
            "sig-gap-partial-target-exact",
            "sig-gap-partial-target-middle",
        ] {
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
                Some(gap_cursor.clone()),
                "partial bounded repair must keep latch until the frozen target is reached"
            );
            assert_eq!(
                repair_epoch
                    .resume_after_cursor
                    .as_ref()
                    .map(|cursor| cursor.signature.as_str()),
                Some(expected_resume_signature)
            );
        }

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
            "repair should clear only after the frozen target row is replayed"
        );
        remove_sqlite_test_files(&db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_aggregate_gap_repair_uses_larger_bounded_page_limit_stage1(
    ) -> Result<()> {
        let db_path = migrated_observed_swap_writer_test_db(
            "copybot-app-observed-swap-gap-repair-page-limit",
        )?;
        let store = SqliteStore::open(Path::new(&db_path))?;
        let covered_swap = aggregate_gap_replay_test_swap(
            "sig-gap-page-limit-covered",
            860,
            "2026-04-28T13:29:55Z",
        );
        let replay_swaps = (0..10)
            .map(|idx| {
                aggregate_gap_replay_test_swap(
                    &format!("sig-gap-page-limit-{idx:02}"),
                    861 + idx as u64,
                    &format!("2026-04-28T13:30:{idx:02}Z"),
                )
            })
            .collect::<Vec<_>>();
        let gap_cursor = discovery_runtime_cursor_for_swap(
            replay_swaps
                .first()
                .context("test replay swaps should not be empty")?,
        );
        let tail_cursor = discovery_runtime_cursor_for_swap(
            replay_swaps
                .last()
                .context("test replay swaps should not be empty")?,
        );
        let mut all_swaps = vec![covered_swap.clone()];
        all_swaps.extend(replay_swaps.iter().cloned());
        store.insert_observed_swaps_batch(&all_swaps)?;
        store.apply_discovery_scoring_batch(&[covered_swap.clone()], &aggregate_write_config())?;
        store.set_discovery_scoring_covered_through_cursor(&discovery_runtime_cursor_for_swap(
            &covered_swap,
        ))?;
        store.set_discovery_scoring_materialization_gap_cursor(&gap_cursor)?;
        let config =
            ObservedSwapWriterConfig::for_test(16, 1, true, aggregate_write_config(), None);
        assert!(
            config.aggregate_idle_replay_max_pages > 8,
            "repair page limit should be raised above the old conservative cap"
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
        let page_fetch_end_count = events
            .iter()
            .filter(|event| **event == super::DISCOVERY_AGGREGATE_PHASE_PAGE_FETCH_END)
            .count();
        assert!(
            page_fetch_end_count > 8,
            "one repair slice should process more than eight bounded pages when needed"
        );
        assert_eq!(
            store.load_discovery_scoring_materialization_gap_cursor()?,
            None,
            "larger bounded slice should reach the frozen target and clear honest gap evidence"
        );
        assert_eq!(
            store.load_discovery_scoring_covered_through_cursor()?,
            Some(tail_cursor),
            "larger bounded repair slice must still advance coverage only through committed pages"
        );

        remove_sqlite_test_files(&db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_aggregate_gap_repair_retryable_apply_lock_preserves_latch_stage1(
    ) -> Result<()> {
        let db_path = migrated_observed_swap_writer_test_db(
            "copybot-app-observed-swap-gap-repair-apply-lock",
        )?;
        let (covered_swap, first_replay_swap, _tail_replay_swap) =
            seed_discovery_scoring_rug_finalize_replay_fixture(&db_path)?;
        let store = SqliteStore::open(Path::new(&db_path))?;
        store.set_discovery_scoring_materialization_gap_cursor(
            &discovery_runtime_cursor_for_swap(&first_replay_swap),
        )?;
        let trigger_conn = Connection::open(Path::new(&db_path))
            .context("failed to open sqlite db for accelerated repair apply lock trigger")?;
        trigger_conn.execute_batch(
            "CREATE TRIGGER fail_accelerated_repair_apply_day_retryable
             BEFORE INSERT ON wallet_scoring_days
             WHEN NEW.wallet_id = 'wallet-rug-finalize-replay'
             BEGIN
                 SELECT RAISE(ROLLBACK, 'database is locked');
             END;",
        )?;
        let config =
            ObservedSwapWriterConfig::for_test(16, 1, true, aggregate_write_config(), None);
        assert!(config.aggregate_idle_replay_max_pages > 8);
        let mut repair_epoch = super::DiscoveryAggregateGapRepairEpoch::default();

        let ran_slice = super::run_discovery_aggregate_gap_repair_slice(
            db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?,
            &store,
            &config,
            &ObservedSwapWriterTelemetry::default(),
            true,
            &mut repair_epoch,
        )?;

        assert!(ran_slice);
        assert_eq!(
            store.load_discovery_scoring_materialization_gap_cursor()?,
            Some(discovery_runtime_cursor_for_swap(&first_replay_swap)),
            "retryable apply lock during an accelerated repair slice must keep gap evidence latched"
        );
        assert_eq!(
            store.load_discovery_scoring_covered_through_cursor()?,
            Some(discovery_runtime_cursor_for_swap(&covered_swap)),
            "retryable apply lock must not advance covered_through"
        );

        drop(trigger_conn);
        remove_sqlite_test_files(&db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_aggregate_gap_repair_runs_under_continuous_hot_traffic_stage1(
    ) -> Result<()> {
        let db_path =
            migrated_observed_swap_writer_test_db("copybot-app-observed-swap-hot-gap-repair")?;
        let store = SqliteStore::open(Path::new(&db_path))?;
        let gap_swap =
            aggregate_gap_replay_test_swap("sig-hot-gap-repair-exact", 830, "2026-04-28T13:15:00Z");
        let tail_swap =
            aggregate_gap_replay_test_swap("sig-hot-gap-repair-tail", 831, "2026-04-28T13:15:05Z");
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
        let (aggregate_sender, aggregate_worker) = start_discovery_aggregate_writer_loop_for_test(
            &db_path,
            ObservedSwapWriterConfig::for_test_with_aggregate_tuning(
                64,
                1,
                true,
                aggregate_write_config(),
                1,
                64,
                true,
                1,
                None,
            ),
            128,
        )?;
        store.set_discovery_scoring_materialization_gap_cursor(&gap_cursor)?;

        let stop = Arc::new(AtomicBool::new(false));
        let sent_count = Arc::new(AtomicUsize::new(0));
        let traffic = start_hot_aggregate_repair_traffic(
            aggregate_sender.clone(),
            Arc::clone(&stop),
            Arc::clone(&sent_count),
        );
        wait_for_hot_aggregate_repair_traffic(&sent_count)?;
        wait_for_materialization_gap_clear(&store)?;
        stop.store(true, Ordering::Relaxed);
        drop(aggregate_sender);
        traffic
            .join()
            .expect("hot aggregate traffic thread panicked");
        aggregate_worker
            .join()
            .expect("aggregate writer test thread panicked")?;

        assert!(
            sent_count.load(Ordering::Relaxed) > 0,
            "test must keep hot aggregate requests flowing while repair clears the gap"
        );
        let covered_through = store
            .load_discovery_scoring_covered_through_cursor()?
            .context("covered_through cursor should remain present")?;
        assert!(
            super::compare_discovery_runtime_cursors(&covered_through, &tail_cursor)
                != std::cmp::Ordering::Less,
            "bounded hot-traffic repair must not regress covered_through"
        );
        remove_sqlite_test_files(&db_path);
        Ok(())
    }
