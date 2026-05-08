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
