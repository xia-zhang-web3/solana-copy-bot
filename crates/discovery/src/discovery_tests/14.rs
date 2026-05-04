    #[test]
    fn persisted_stream_reconcile_new_tail_pending_exact_batch_survives_rollover_and_finishes_batch_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-reconcile-new-tail-pending-batch-rollover.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let mut config = bounded_stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 60;
        config.max_fetch_swaps_per_cycle = 20_000;
        config.max_fetch_pages_per_cycle = 1;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let source_now = DateTime::parse_from_rfc3339("2026-03-19T02:10:50Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let target_one_now = source_now + Duration::seconds(60);
        let target_two_now = target_one_now + Duration::seconds(60);
        let source_window_start =
            source_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let source_metrics_window_start = metrics_window_start_for_test(&config, source_now);
        let target_one_window_start =
            target_one_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let target_one_metrics_window_start =
            metrics_window_start_for_test(&config, target_one_now);
        let target_two_window_start =
            target_two_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let target_two_metrics_window_start =
            metrics_window_start_for_test(&config, target_two_now);

        let survivor = "TokenStage1PendingBatchNewTailSurvivor11111".to_string();
        store.insert_observed_swap(&swap(
            "wallet_stage1_pending_batch_new_tail",
            "stage1-pending-batch-new-tail-survivor",
            target_one_window_start + Duration::seconds(5),
            SOL_MINT,
            &survivor,
            1.0,
            10.0,
        ))?;
        let mut new_tail_tokens = Vec::new();
        for idx in 0..STALE_RECONCILE_TOKEN_BATCH_CAP {
            let token = format!("TokenStage1PendingBatchNewTail{idx:05}111111111111");
            store.insert_observed_swap(&swap(
                "wallet_stage1_pending_batch_new_tail",
                &format!("stage1-pending-batch-new-tail-buy-{idx}"),
                source_now + Duration::seconds((idx % 20) as i64 + 1),
                SOL_MINT,
                &token,
                1.0,
                10.0,
            ))?;
            new_tail_tokens.push(token);
        }

        let mut state = discovery.start_persisted_stream_rebuild_state(
            source_window_start,
            source_metrics_window_start,
            source_now,
        );
        state.phase = DiscoveryPersistedRebuildPhase::CollectBuyMints;
        state.payload.collect_buy_mints_prepass_complete = true;
        state.payload.buy_mint_counts = BTreeMap::from([(survivor.clone(), 1u32)]);
        state.payload.unique_buy_mints = vec![survivor];
        assert!(
            discovery.prepare_persisted_stream_rebuild_for_metrics_window_rollover(
                &mut state,
                target_one_window_start,
                target_one_metrics_window_start,
                target_one_now,
            )?
        );
        state.payload.collect_buy_mints_mode = CollectBuyMintsMode::ReconcileNewTail;

        let forced_first_cycle_rows = 8usize;
        arm_test_force_reconcile_new_tail_exact_batch_row_limit(forced_first_cycle_rows);
        let first_phase_advance = discovery.advance_persisted_stream_prepass(
            &store,
            &mut state,
            config.max_fetch_swaps_per_cycle,
            config.max_fetch_pages_per_cycle,
            None,
            Instant::now() + StdDuration::from_secs(5),
        )?;
        state.prepass_rows_processed = state
            .prepass_rows_processed
            .saturating_add(first_phase_advance.rows_processed);
        state.prepass_pages_processed = state
            .prepass_pages_processed
            .saturating_add(first_phase_advance.pages_processed);
        assert_eq!(first_phase_advance.rows_processed, forced_first_cycle_rows);
        assert_eq!(first_phase_advance.pages_processed, 1);
        assert_eq!(
            state.payload.collect_buy_mints_reconcile_new_tail_pending_mints.len(),
            STALE_RECONCILE_TOKEN_BATCH_CAP.saturating_sub(forced_first_cycle_rows),
            "first bounded cycle should persist the remainder of the exact stale new-tail candidate batch instead of forcing the next cycle to rediscover the same candidates"
        );

        store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&state, target_one_now)?,
        )?;
        let (mut resumed, restore_outcome) = discovery
            .load_or_start_persisted_stream_rebuild_state(
                &store,
                target_two_window_start,
                target_two_metrics_window_start,
                target_two_now,
            )?;

        assert_eq!(
            restore_outcome,
            PersistedStreamRebuildRestoreOutcome::ResumedStaleMetricsWindow
        );
        assert_eq!(
            resumed.payload.collect_buy_mints_mode,
            CollectBuyMintsMode::ReconcileNewTail
        );
        assert_eq!(
            resumed.payload.collect_buy_mints_reconcile_new_tail_pending_mints.len(),
            STALE_RECONCILE_TOKEN_BATCH_CAP.saturating_sub(forced_first_cycle_rows),
            "stale-resume across bucket rollover must preserve the persisted exact stale new-tail batch instead of rediscovering it from scratch"
        );

        let resumed_phase_advance = discovery.advance_persisted_stream_prepass(
            &store,
            &mut resumed,
            config.max_fetch_swaps_per_cycle,
            config.max_fetch_pages_per_cycle,
            None,
            Instant::now() + StdDuration::from_secs(5),
        )?;
        assert_eq!(
            resumed_phase_advance.rows_processed,
            STALE_RECONCILE_EXACT_COUNT_BATCH_CAP,
            "once the persisted stale new-tail exact batch is resumed, the next bounded cycle should continue from that batch directly instead of rediscovering candidates from the same stale tail slice"
        );
        assert_eq!(resumed_phase_advance.pages_processed, 1);
        assert_eq!(
            resumed.payload.collect_buy_mints_reconcile_new_tail_pending_mints.len(),
            STALE_RECONCILE_TOKEN_BATCH_CAP
                .saturating_sub(forced_first_cycle_rows)
                .saturating_sub(STALE_RECONCILE_EXACT_COUNT_BATCH_CAP),
            "resumed stale new-tail exact batch should drain the next exact sub-batch without discarding the still-pending remainder"
        );
        assert_eq!(
            resumed
                .payload
                .collect_buy_mints_reconcile_new_tail_cursor_token
                .as_deref(),
            new_tail_tokens
                .get(
                    forced_first_cycle_rows
                        .saturating_add(STALE_RECONCILE_EXACT_COUNT_BATCH_CAP)
                        .saturating_sub(1),
                )
                .map(|token| token.as_str())
        );
        assert!(
            DiscoveryService::state_can_resume_stale_metrics_window_until_exact_checkpoint(
                &resumed
            ),
            "persisted exact stale new-tail batches must remain exact and stale-resumable after rollover-driven resume"
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_reconcile_new_tail_exact_subbatches_reduce_live_like_timeout_pressure_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-reconcile-new-tail-exact-subbatches.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let mut config = bounded_stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 60;
        config.max_fetch_swaps_per_cycle = 20_000;
        config.max_fetch_pages_per_cycle = 5;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let source_now = DateTime::parse_from_rfc3339("2026-03-19T03:10:50Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let target_now = source_now + Duration::seconds(60);
        let source_window_start =
            source_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let source_metrics_window_start = metrics_window_start_for_test(&config, source_now);
        let target_window_start =
            target_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let target_metrics_window_start = metrics_window_start_for_test(&config, target_now);

        let survivor = "TokenStage1ExactSubbatchSurvivor111111111".to_string();
        store.insert_observed_swap(&swap(
            "wallet_stage1_exact_subbatch_new_tail",
            "stage1-exact-subbatch-survivor",
            target_window_start + Duration::seconds(5),
            SOL_MINT,
            &survivor,
            1.0,
            10.0,
        ))?;

        let pending_token_count = STALE_RECONCILE_EXACT_COUNT_BATCH_CAP
            .saturating_mul(config.max_fetch_pages_per_cycle.max(1))
            .saturating_add(7);
        let mut pending_tokens = Vec::new();
        for idx in 0..pending_token_count {
            let token = format!("TokenStage1ExactSubbatch{idx:05}111111111111111");
            store.insert_observed_swap(&swap(
                "wallet_stage1_exact_subbatch_new_tail",
                &format!("stage1-exact-subbatch-buy-{idx}"),
                source_now + Duration::seconds((idx % 20) as i64 + 1),
                SOL_MINT,
                &token,
                1.0,
                10.0,
            ))?;
            pending_tokens.push(token);
        }

        let mut state = discovery.start_persisted_stream_rebuild_state(
            source_window_start,
            source_metrics_window_start,
            source_now,
        );
        state.phase = DiscoveryPersistedRebuildPhase::CollectBuyMints;
        state.payload.collect_buy_mints_prepass_complete = true;
        state.payload.buy_mint_counts = BTreeMap::from([(survivor.clone(), 1u32)]);
        state.payload.unique_buy_mints = vec![survivor];
        assert!(
            discovery.prepare_persisted_stream_rebuild_for_metrics_window_rollover(
                &mut state,
                target_window_start,
                target_metrics_window_start,
                target_now,
            )?
        );
        state.payload.collect_buy_mints_mode = CollectBuyMintsMode::ReconcileNewTail;
        state
            .payload
            .collect_buy_mints_reconcile_new_tail_pending_mints = pending_tokens.clone();

        arm_test_force_reconcile_new_tail_exact_batch_row_limit(
            STALE_RECONCILE_EXACT_COUNT_BATCH_CAP,
        );
        let phase_advance = discovery.advance_persisted_stream_prepass(
            &store,
            &mut state,
            config.max_fetch_swaps_per_cycle,
            config.max_fetch_pages_per_cycle,
            None,
            Instant::now() + StdDuration::from_secs(5),
        )?;

        let expected_rows = STALE_RECONCILE_EXACT_COUNT_BATCH_CAP
            .saturating_mul(config.max_fetch_pages_per_cycle.max(1));
        assert_eq!(
            phase_advance.rows_processed,
            expected_rows,
            "stale new-tail should process multiple exact sub-batches per bounded cycle instead of letting one oversized exact batch query consume the whole cycle"
        );
        assert_eq!(
            phase_advance.pages_processed,
            config.max_fetch_pages_per_cycle
        );
        assert_eq!(
            state.payload.collect_buy_mints_reconcile_new_tail_pending_mints.len(),
            pending_token_count.saturating_sub(expected_rows),
            "processing exact sub-batches should drain the persisted pending batch prefix across all available bounded pages"
        );
        assert_eq!(
            state
                .payload
                .collect_buy_mints_reconcile_new_tail_cursor_token
                .as_deref(),
            pending_tokens
                .get(expected_rows.saturating_sub(1))
                .map(|token| token.as_str())
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_reconcile_expired_head_noisy_bucket_roll_does_not_restart_fresh_scan_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-reconcile-expired-head-noisy-bucket-roll.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let mut config = bounded_stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 60;
        config.max_fetch_swaps_per_cycle = 32;
        config.max_fetch_pages_per_cycle = 1;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let source_now = DateTime::parse_from_rfc3339("2026-03-18T18:20:50Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let target_one_now = source_now + Duration::seconds(60);
        let target_two_now = target_one_now + Duration::seconds(60);
        let source_window_start =
            source_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let source_metrics_window_start = metrics_window_start_for_test(&config, source_now);
        let target_one_window_start =
            target_one_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let target_one_metrics_window_start =
            metrics_window_start_for_test(&config, target_one_now);
        let target_two_window_start =
            target_two_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let target_two_metrics_window_start =
            metrics_window_start_for_test(&config, target_two_now);

        let mut exact_counts = BTreeMap::new();
        for idx in 0..256usize {
            let token = format!("TokenStage1NoisyExpiredHead{idx:04}111111111111");
            store.insert_observed_swap(&swap(
                "wallet_stage1_noisy_expired",
                &format!("stage1-noisy-expired-buy-{idx}"),
                source_window_start + Duration::seconds((idx % 30) as i64),
                SOL_MINT,
                &token,
                1.0,
                10.0,
            ))?;
            exact_counts.insert(token, 1u32);
        }
        let survivor = "TokenStage1NoisyExpiredHeadSurvivor111111".to_string();
        store.insert_observed_swap(&swap(
            "wallet_stage1_noisy_expired",
            "stage1-noisy-expired-survivor",
            target_one_window_start + Duration::seconds(5),
            SOL_MINT,
            &survivor,
            1.0,
            10.0,
        ))?;
        exact_counts.insert(survivor.clone(), 1);

        let mut state = discovery.start_persisted_stream_rebuild_state(
            source_window_start,
            source_metrics_window_start,
            source_now,
        );
        state.phase = DiscoveryPersistedRebuildPhase::CollectBuyMints;
        state.payload.collect_buy_mints_prepass_complete = true;
        state.payload.buy_mint_counts = exact_counts.clone();
        state.payload.unique_buy_mints = exact_counts.keys().cloned().collect();
        assert!(
            discovery.prepare_persisted_stream_rebuild_for_metrics_window_rollover(
                &mut state,
                target_one_window_start,
                target_one_metrics_window_start,
                target_one_now,
            )?
        );

        let phase_advance = discovery.advance_persisted_stream_prepass(
            &store,
            &mut state,
            32,
            1,
            None,
            Instant::now() + StdDuration::from_secs(1),
        )?;
        state.prepass_rows_processed = 2_154_114usize;
        state.prepass_pages_processed = state
            .prepass_pages_processed
            .saturating_add(phase_advance.pages_processed)
            .saturating_add(208);
        state.payload.collect_buy_mints_cursor_token =
            phase_advance.collect_buy_mints_cursor_token.clone();
        assert_eq!(
            state.payload.collect_buy_mints_mode,
            CollectBuyMintsMode::ReconcileExpiredHead
        );
        assert!(state
            .payload
            .collect_buy_mints_reconcile_expired_head_cursor_token
            .is_some());
        store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&state, target_one_now)?,
        )?;

        let (resumed, restore_outcome) = discovery.load_or_start_persisted_stream_rebuild_state(
            &store,
            target_two_window_start,
            target_two_metrics_window_start,
            target_two_now,
        )?;
        assert_eq!(
            restore_outcome,
            PersistedStreamRebuildRestoreOutcome::ResumedStaleMetricsWindow
        );
        assert!(
            resumed.prepass_rows_processed >= state.prepass_rows_processed,
            "bucket rollover during noisy in-progress reconcile must preserve accumulated prepass progress instead of resetting to a fresh scan baseline"
        );
        assert!(
            resumed.metrics_window_start == target_one_metrics_window_start,
            "large noisy stale-bucket reconcile must stay on the frozen target until an exact carry-forward checkpoint becomes available instead of restarting fresh on the new bucket"
        );
        assert!(
            resumed.phase != DiscoveryPersistedRebuildPhase::CollectBuyMints
                || resumed.payload.collect_buy_mints_mode != CollectBuyMintsMode::FreshScan,
            "the noisy stale-bucket path must not operationally restart collect_buy_mints from a fresh-scan baseline"
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_reconcile_expired_head_live_like_cycle_advances_exact_token_batches_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-reconcile-expired-head-live-like-batch-advance.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let mut config = bounded_stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 60;
        config.max_fetch_swaps_per_cycle = 20_000;
        config.max_fetch_pages_per_cycle = 5;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let source_now = DateTime::parse_from_rfc3339("2026-03-18T23:00:50Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let target_now = source_now + Duration::seconds(60);
        let source_window_start =
            source_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let source_metrics_window_start = metrics_window_start_for_test(&config, source_now);
        let target_window_start =
            target_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let target_metrics_window_start = metrics_window_start_for_test(&config, target_now);

        let expired_token_count = STALE_RECONCILE_TOKEN_BATCH_CAP
            .saturating_mul(config.max_fetch_pages_per_cycle.max(1))
            .saturating_add(37);
        let mut exact_counts = BTreeMap::new();
        let mut expired_tokens = Vec::new();
        for idx in 0..expired_token_count {
            let token = format!("TokenStage1LiveLikeExpiredHead{idx:05}111111111");
            store.insert_observed_swap(&swap(
                "wallet_stage1_live_like_expired",
                &format!("stage1-live-like-expired-buy-{idx}"),
                source_window_start + Duration::seconds((idx % 30) as i64),
                SOL_MINT,
                &token,
                1.0,
                10.0,
            ))?;
            exact_counts.insert(token.clone(), 1u32);
            expired_tokens.push(token);
        }
        let survivor = "TokenStage1LiveLikeExpiredHeadSurvivor111".to_string();
        store.insert_observed_swap(&swap(
            "wallet_stage1_live_like_expired",
            "stage1-live-like-expired-survivor",
            target_window_start + Duration::seconds(5),
            SOL_MINT,
            &survivor,
            1.0,
            10.0,
        ))?;
        exact_counts.insert(survivor.clone(), 1);

        let mut state = discovery.start_persisted_stream_rebuild_state(
            source_window_start,
            source_metrics_window_start,
            source_now,
        );
        state.phase = DiscoveryPersistedRebuildPhase::CollectBuyMints;
        state.payload.collect_buy_mints_prepass_complete = true;
        state.payload.buy_mint_counts = exact_counts;
        state.payload.unique_buy_mints = expired_tokens
            .iter()
            .cloned()
            .chain(std::iter::once(survivor.clone()))
            .collect();
        assert_sorted_strings(&state.payload.unique_buy_mints);
        assert!(
            discovery.prepare_persisted_stream_rebuild_for_metrics_window_rollover(
                &mut state,
                target_window_start,
                target_metrics_window_start,
                target_now,
            )?
        );

        let phase_advance = discovery.advance_persisted_stream_prepass(
            &store,
            &mut state,
            config.max_fetch_swaps_per_cycle,
            config.max_fetch_pages_per_cycle,
            None,
            Instant::now() + StdDuration::from_secs(5),
        )?;

        let expected_rows = STALE_RECONCILE_EXACT_COUNT_BATCH_CAP
            .saturating_mul(config.max_fetch_pages_per_cycle.max(1));
        assert_eq!(
            phase_advance.rows_processed,
            expected_rows,
            "live-like stale expired-head reconcile should advance by capped exact count sub-batches instead of repeatedly recounting one large expired-head candidate range per bounded page"
        );
        assert_eq!(
            phase_advance.pages_processed,
            config.max_fetch_pages_per_cycle
        );
        assert!(!phase_advance.source_exhausted);
        assert_eq!(
            state.payload.collect_buy_mints_mode,
            CollectBuyMintsMode::ReconcileExpiredHead
        );
        assert_eq!(
            state
                .payload
                .collect_buy_mints_reconcile_expired_head_cursor_token
                .as_deref(),
            expired_tokens
                .get(expected_rows.saturating_sub(1))
                .map(|token| token.as_str())
        );
        assert_eq!(
            state
                .payload
                .collect_buy_mints_reconcile_expired_head_pending_mints
                .len(),
            expired_token_count
                .saturating_add(1)
                .min(STALE_RECONCILE_TOKEN_BATCH_CAP)
                .saturating_sub(expected_rows),
            "live-like stale expired-head reconcile should persist the unprocessed suffix of the current exact candidate batch instead of rediscovering it next cycle"
        );
        assert_eq!(
            state.payload.unique_buy_mints.len(),
            expired_token_count.saturating_sub(expected_rows).saturating_add(1),
            "expired-head reconcile should subtract the bounded candidate token batches while keeping the surviving overlap mint"
        );
        assert!(
            DiscoveryService::state_can_resume_stale_metrics_window_until_exact_checkpoint(&state),
            "batched live-like stale reconcile must stay exact and resumable after the first bounded cycle"
        );
        Ok(())
    }
