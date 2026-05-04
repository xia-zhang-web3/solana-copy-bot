    #[test]
    fn persisted_stream_reconcile_new_tail_stale_bucket_resumes_until_exact_checkpoint_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-reconcile-new-tail-stale-bucket-resume.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let mut config = bounded_stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 60;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let source_now = DateTime::parse_from_rfc3339("2026-03-18T18:10:50Z")
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

        let token_survives = "TokenStage1StaleNewTailSurvive111111111".to_string();
        let token_new_tail_a = "TokenStage1StaleNewTailA1111111111111".to_string();
        let token_new_tail_b = "TokenStage1StaleNewTailB1111111111111".to_string();
        let token_future = "TokenStage1StaleNewTailFuture11111111".to_string();
        for (idx, (token, ts)) in [
            (
                token_survives.as_str(),
                target_one_window_start + Duration::seconds(5),
            ),
            (token_new_tail_a.as_str(), source_now + Duration::seconds(5)),
            (token_new_tail_b.as_str(), source_now + Duration::seconds(6)),
            (token_future.as_str(), target_one_now + Duration::seconds(5)),
        ]
        .into_iter()
        .enumerate()
        {
            store.insert_observed_swap(&swap(
                "wallet_stage1_reconcile_new_tail",
                &format!("stage1-stale-new-tail-buy-{idx}"),
                ts,
                SOL_MINT,
                token,
                1.0,
                10.0,
            ))?;
        }

        let mut state = discovery.start_persisted_stream_rebuild_state(
            source_window_start,
            source_metrics_window_start,
            source_now,
        );
        state.phase = DiscoveryPersistedRebuildPhase::CollectBuyMints;
        state.payload.collect_buy_mints_prepass_complete = true;
        state.payload.unique_buy_mints = vec![token_survives.clone()];
        state.payload.buy_mint_counts = BTreeMap::from([(token_survives, 1)]);
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
            1,
            2,
            None,
            Instant::now() + StdDuration::from_secs(1),
        )?;
        state.prepass_rows_processed = state
            .prepass_rows_processed
            .saturating_add(phase_advance.rows_processed);
        state.prepass_pages_processed = state
            .prepass_pages_processed
            .saturating_add(phase_advance.pages_processed);
        state.payload.collect_buy_mints_cursor_token =
            phase_advance.collect_buy_mints_cursor_token.clone();
        assert_eq!(
            state.payload.collect_buy_mints_mode,
            CollectBuyMintsMode::ReconcileNewTail
        );
        assert!(state
            .payload
            .collect_buy_mints_reconcile_new_tail_cursor_token
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
        assert_eq!(
            resumed.metrics_window_start,
            target_one_metrics_window_start
        );
        assert_eq!(
            resumed.payload.collect_buy_mints_mode,
            CollectBuyMintsMode::ReconcileNewTail
        );
        assert_eq!(
            resumed
                .payload
                .collect_buy_mints_reconcile_new_tail_cursor_token,
            state
                .payload
                .collect_buy_mints_reconcile_new_tail_cursor_token
        );
        assert_eq!(resumed.prepass_rows_processed, state.prepass_rows_processed);
        Ok(())
    }

    #[test]
    fn persisted_stream_reconcile_new_tail_live_like_cycle_advances_exact_token_batches_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-reconcile-new-tail-live-like-batch-advance.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let mut config = bounded_stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 60;
        config.max_fetch_swaps_per_cycle = 20_000;
        config.max_fetch_pages_per_cycle = 5;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let source_now = DateTime::parse_from_rfc3339("2026-03-19T00:10:50Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let target_now = source_now + Duration::seconds(60);
        let source_window_start =
            source_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let source_metrics_window_start = metrics_window_start_for_test(&config, source_now);
        let target_window_start =
            target_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let target_metrics_window_start = metrics_window_start_for_test(&config, target_now);

        let new_tail_token_count = STALE_RECONCILE_TOKEN_BATCH_CAP
            .saturating_mul(config.max_fetch_pages_per_cycle.max(1))
            .saturating_add(37);
        let survivor = "TokenStage1LiveLikeNewTailSurvivor11111111".to_string();
        store.insert_observed_swap(&swap(
            "wallet_stage1_live_like_new_tail",
            "stage1-live-like-new-tail-survivor",
            target_window_start + Duration::seconds(5),
            SOL_MINT,
            &survivor,
            1.0,
            10.0,
        ))?;
        let mut new_tail_tokens = Vec::new();
        for idx in 0..new_tail_token_count {
            let token = format!("TokenStage1LiveLikeNewTail{idx:05}1111111111111");
            store.insert_observed_swap(&swap(
                "wallet_stage1_live_like_new_tail",
                &format!("stage1-live-like-new-tail-buy-{idx}"),
                source_now + Duration::seconds((idx % 30) as i64 + 1),
                SOL_MINT,
                &token,
                1.0,
                10.0,
            ))?;
            new_tail_tokens.push(token);
        }
        let future_noise = "TokenStage1LiveLikeNewTailFuture1111111".to_string();
        store.insert_observed_swap(&swap(
            "wallet_stage1_live_like_new_tail",
            "stage1-live-like-new-tail-future",
            target_now + Duration::seconds(5),
            SOL_MINT,
            &future_noise,
            1.0,
            10.0,
        ))?;

        let mut state = discovery.start_persisted_stream_rebuild_state(
            source_window_start,
            source_metrics_window_start,
            source_now,
        );
        state.phase = DiscoveryPersistedRebuildPhase::CollectBuyMints;
        state.payload.collect_buy_mints_prepass_complete = true;
        state.payload.buy_mint_counts = BTreeMap::from([(survivor.clone(), 1u32)]);
        state.payload.unique_buy_mints = vec![survivor.clone()];
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
            .collect_buy_mints_reconcile_expired_head_cursor = None;
        state
            .payload
            .collect_buy_mints_reconcile_expired_head_cursor_token = None;

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
            "live-like stale new-tail reconcile should advance by capped exact count sub-batches instead of letting one oversized exact candidate batch monopolize the cycle"
        );
        assert_eq!(
            phase_advance.pages_processed,
            config.max_fetch_pages_per_cycle
        );
        assert!(!phase_advance.source_exhausted);
        assert_eq!(
            state.payload.collect_buy_mints_mode,
            CollectBuyMintsMode::ReconcileNewTail
        );
        assert_eq!(
            state
                .payload
                .collect_buy_mints_reconcile_new_tail_cursor_token
                .as_deref(),
            new_tail_tokens
                .get(expected_rows.saturating_sub(1))
                .map(|token| token.as_str())
        );
        assert_eq!(
            state.payload.unique_buy_mints.len(),
            expected_rows.saturating_add(1),
            "new-tail reconcile should add the bounded candidate token batches while preserving the overlapping carried-forward mint"
        );
        assert_sorted_strings(&state.payload.unique_buy_mints);
        assert!(
            DiscoveryService::state_can_resume_stale_metrics_window_until_exact_checkpoint(&state),
            "batched live-like stale new-tail reconcile must stay exact and resumable after the first bounded cycle"
        );
        Ok(())
    }

    #[test]
    fn persisted_stream_reconcile_new_tail_zero_row_timeout_narrows_slice_and_escapes_stall_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("stage1-reconcile-new-tail-zero-row-stall.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let mut config = bounded_stage1_runtime_config();
        config.metric_snapshot_interval_seconds = 60;
        config.max_fetch_swaps_per_cycle = 20_000;
        config.max_fetch_pages_per_cycle = 8;
        let discovery = DiscoveryService::new(config.clone(), permissive_shadow_quality());
        let source_now = DateTime::parse_from_rfc3339("2026-03-19T01:10:50Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let target_now = source_now + Duration::seconds(60);
        let source_window_start =
            source_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let source_metrics_window_start = metrics_window_start_for_test(&config, source_now);
        let target_window_start =
            target_now - Duration::days(config.scoring_window_days.max(1) as i64);
        let target_metrics_window_start = metrics_window_start_for_test(&config, target_now);

        let survivor = "TokenStage1StalledNewTailSurvivor11111111".to_string();
        store.insert_observed_swap(&swap(
            "wallet_stage1_stalled_new_tail",
            "stage1-stalled-new-tail-survivor",
            target_window_start + Duration::seconds(5),
            SOL_MINT,
            &survivor,
            1.0,
            10.0,
        ))?;
        let mut new_tail_tokens = Vec::new();
        for idx in 0..10usize {
            let token = format!("TokenStage1StalledNewTail{idx:05}1111111111111");
            store.insert_observed_swap(&swap(
                "wallet_stage1_stalled_new_tail",
                &format!("stage1-stalled-new-tail-buy-{idx}"),
                source_now + Duration::seconds((idx % 10) as i64 + 1),
                SOL_MINT,
                &token,
                1.0,
                10.0,
            ))?;
            new_tail_tokens.push(token);
        }

        let processed_prefix_len = 2usize;
        let processed_prefix = &new_tail_tokens[..processed_prefix_len];
        let stalled_tail = &new_tail_tokens[processed_prefix_len..];
        let mut state = discovery.start_persisted_stream_rebuild_state(
            source_window_start,
            source_metrics_window_start,
            source_now,
        );
        state.phase = DiscoveryPersistedRebuildPhase::CollectBuyMints;
        state.payload.collect_buy_mints_prepass_complete = true;
        state.payload.buy_mint_counts = BTreeMap::from_iter(
            std::iter::once((survivor.clone(), 1u32))
                .chain(processed_prefix.iter().cloned().map(|token| (token, 1u32))),
        );
        state.payload.unique_buy_mints = state.payload.buy_mint_counts.keys().cloned().collect();
        assert_sorted_strings(&state.payload.unique_buy_mints);
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
            .collect_buy_mints_reconcile_expired_head_cursor = None;
        state
            .payload
            .collect_buy_mints_reconcile_expired_head_cursor_token = None;
        state
            .payload
            .collect_buy_mints_reconcile_new_tail_cursor_token = processed_prefix.last().cloned();

        let original_cursor = state
            .payload
            .collect_buy_mints_reconcile_new_tail_cursor_token
            .clone()
            .expect("prefix cursor");
        arm_test_force_reconcile_new_tail_zero_row_timeout();
        let stalled_phase_advance = discovery.advance_persisted_stream_prepass(
            &store,
            &mut state,
            config.max_fetch_swaps_per_cycle,
            config.max_fetch_pages_per_cycle,
            None,
            Instant::now() + StdDuration::from_secs(5),
        )?;
        state.prepass_rows_processed = state
            .prepass_rows_processed
            .saturating_add(stalled_phase_advance.rows_processed);
        state.prepass_pages_processed = state
            .prepass_pages_processed
            .saturating_add(stalled_phase_advance.pages_processed);
        assert_eq!(stalled_phase_advance.rows_processed, 0);
        assert_eq!(state.prepass_rows_processed, 0);
        assert_eq!(
            state
                .payload
                .collect_buy_mints_reconcile_new_tail_cursor_token
                .as_deref(),
            Some(original_cursor.as_str()),
            "zero-row timeout must not silently advance the stale cursor past uncounted candidate tokens"
        );
        let expected_narrowed_slice_end = stalled_tail
            .get((stalled_tail.len().saturating_sub(1)) / 2)
            .cloned();
        assert_eq!(
            state
                .payload
                .collect_buy_mints_reconcile_new_tail_slice_end_token,
            expected_narrowed_slice_end,
            "zero-row timeout must persist a narrowed exact token slice for retry instead of repeating the same wide stalled slice forever"
        );

        store.upsert_discovery_persisted_rebuild_state(
            &DiscoveryService::persisted_stream_rebuild_row(&state, target_now)?,
        )?;
        let (mut resumed, restore_outcome) = discovery
            .load_or_start_persisted_stream_rebuild_state(
                &store,
                target_window_start,
                target_metrics_window_start,
                target_now,
            )?;
        assert_eq!(
            restore_outcome,
            PersistedStreamRebuildRestoreOutcome::ResumedExisting
        );
        assert_eq!(
            resumed
                .payload
                .collect_buy_mints_reconcile_new_tail_cursor_token,
            Some(original_cursor.clone())
        );
        assert_eq!(
            resumed
                .payload
                .collect_buy_mints_reconcile_new_tail_slice_end_token,
            expected_narrowed_slice_end
        );

        let resumed_phase_advance = discovery.advance_persisted_stream_prepass(
            &store,
            &mut resumed,
            config.max_fetch_swaps_per_cycle,
            config.max_fetch_pages_per_cycle,
            None,
            Instant::now() + StdDuration::from_secs(5),
        )?;
        assert!(
            resumed_phase_advance.rows_processed > 0,
            "after narrowing the exact token slice, the next bounded cycle must escape the zero-row stall and make durable progress"
        );
        assert!(
            resumed_phase_advance.source_exhausted
                || resumed
                    .payload
                    .collect_buy_mints_reconcile_new_tail_cursor_token
                    .as_deref()
                    > Some(original_cursor.as_str()),
            "resumed stale new-tail reconcile must either advance beyond the previously pinned cursor or finish the remaining stale tail work in the same bounded cycle"
        );
        assert!(
            resumed
                .payload
                .collect_buy_mints_reconcile_new_tail_slice_end_token
                .is_none(),
            "successful retry after a narrowed stale slice should clear the temporary slice cap"
        );
        assert!(
            DiscoveryService::state_can_resume_stale_metrics_window_until_exact_checkpoint(
                &resumed
            ),
            "narrowed-slice stall recovery must preserve exact carry-forward truth and stale-resume safety"
        );
        Ok(())
    }
