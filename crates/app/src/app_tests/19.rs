    #[test]
    fn discovery_critical_backpressure_refresh_is_throttled_within_interval_stage1() -> Result<()> {
        let (store, db_path) =
            make_test_store("discovery-critical-backpressure-refresh-throttled")?;
        seed_test_discovery_critical_target_buy_mints(
            &store,
            &["token-target"],
            &["token-target"],
        )?;

        let follow_snapshot = FollowSnapshot::default();
        let open_shadow_lots = HashSet::new();
        let mut target_buy_mints_old = HashSet::new();
        let mut target_buy_mints_new = HashSet::new();
        let mut old_refresh_attempts = 0usize;
        let mut new_refresh_attempts = 0usize;
        let refresh_now = StdInstant::now();
        let mut refresh_state = DiscoveryCriticalTargetBuyMintsBackpressureRefreshState::default();

        for idx in 0..128usize {
            let mut swap = test_swap(&format!("sig-backpressure-refresh-throttle-{idx:04}"));
            swap.token_out = "token-target".to_string();
            if should_refresh_discovery_critical_target_buy_mints_for_backpressure(
                &follow_snapshot,
                &open_shadow_lots,
                true,
                None,
                refresh_now,
            ) {
                old_refresh_attempts = old_refresh_attempts.saturating_add(1);
                refresh_discovery_critical_target_buy_mints_or_warn(
                    &store,
                    &mut target_buy_mints_old,
                )?;
            }
            assert!(
                irrelevant_observed_swap_requires_discovery_critical_persistence(
                    &swap,
                    &follow_snapshot,
                    &open_shadow_lots,
                    true,
                    &target_buy_mints_old,
                ),
                "old/current path should still classify target-mint SOL buys as discovery-critical after each refresh"
            );

            if should_refresh_discovery_critical_target_buy_mints_for_backpressure(
                &follow_snapshot,
                &open_shadow_lots,
                true,
                Some(&mut refresh_state),
                refresh_now,
            ) {
                new_refresh_attempts = new_refresh_attempts.saturating_add(1);
                refresh_discovery_critical_target_buy_mints_or_warn(
                    &store,
                    &mut target_buy_mints_new,
                )?;
            }
            assert!(
                irrelevant_observed_swap_requires_discovery_critical_persistence(
                    &swap,
                    &follow_snapshot,
                    &open_shadow_lots,
                    true,
                    &target_buy_mints_new,
                ),
                "throttling refresh cadence must preserve exact target-mint discovery-critical classification; it only removes redundant repeated live_runtime reads"
            );
        }

        assert_eq!(
            old_refresh_attempts, 128,
            "old/current path should attempt a live_runtime target-mint refresh on every backpressured discovery-critical irrelevant swap in the same burst"
        );
        assert_eq!(
            new_refresh_attempts, 1,
            "new path must collapse same-burst backpressure refreshes to one live_runtime read per interval"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn load_discovery_critical_target_buy_mints_prefers_exact_candidate_targets_over_broad_unique_buy_universe_stage1(
    ) -> Result<()> {
        let (store, db_path) =
            make_test_store("load-discovery-critical-target-buy-mints-prefers-exact-surface")?;
        seed_test_discovery_critical_target_buy_mints(
            &store,
            &["token-target-a", "token-target-b"],
            &[
                "token-generic-a",
                "token-generic-b",
                "token-target-a",
                "token-target-b",
            ],
        )?;

        let loaded = load_discovery_critical_target_buy_mints(&store)?;
        assert_eq!(
            loaded,
            HashSet::from([
                "token-target-a".to_string(),
                "token-target-b".to_string(),
            ]),
            "once discovery persists an exact candidate target-mint surface, app must prefer it over the much broader unique-buy-mint universe when deciding which irrelevant SOL legs are discovery-critical under pressure"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn load_discovery_critical_target_buy_mints_ignores_broad_unique_buy_universe_when_exact_surface_is_empty_stage1(
    ) -> Result<()> {
        let (store, db_path) =
            make_test_store("load-discovery-critical-target-buy-mints-ignores-broad-fallback")?;
        seed_test_discovery_critical_target_buy_mints(
            &store,
            &[],
            &["token-generic-a", "token-generic-b", "token-target"],
        )?;

        let loaded = load_discovery_critical_target_buy_mints(&store)?;
        assert!(
            loaded.is_empty(),
            "app-side raw-writer pressure handling should not broaden discovery-critical ownership from unique_buy_mints alone when the exact target-mint surface is empty"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn stale_empty_target_set_pending_queue_prunes_generic_backpressure_before_later_target_context_stage1(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("pending-irrelevant-prune-after-target-refresh")?;
        seed_test_discovery_critical_target_buy_mints(
            &store,
            &["token-target"],
            &["token-target"],
        )?;

        let empty_follow_snapshot = FollowSnapshot::default();
        let mut recent_signatures = HashSet::new();
        let mut recent_signature_order = VecDeque::new();
        let mut telemetry = AppConsumerLoopTelemetry::default();
        let processing_started_at = StdInstant::now();
        let backpressure_started_at = StdInstant::now();
        let mut pending_irrelevant_swaps = VecDeque::new();

        for idx in 0..DISCOVERY_CRITICAL_PENDING_IRRELEVANT_SWAP_CAPACITY.saturating_sub(1) {
            let mut generic_swap = test_swap(&format!("sig-generic-pending-{idx:04}"));
            generic_swap.token_out = format!("token-generic-{idx:04}");
            assert!(note_recent_swap_signature(
                &mut recent_signatures,
                &mut recent_signature_order,
                &generic_swap.signature,
            ));
            pending_irrelevant_swaps.push_back(PendingIrrelevantObservedSwap {
                swap: generic_swap,
                discovery_critical: true,
                processing_started_at,
                backpressure_started_at,
                last_backpressure_log_at: None,
            });
        }
        let mut target_swap = test_swap("sig-target-pending");
        target_swap.token_out = "token-target".to_string();
        assert!(note_recent_swap_signature(
            &mut recent_signatures,
            &mut recent_signature_order,
            &target_swap.signature,
        ));
        pending_irrelevant_swaps.push_back(PendingIrrelevantObservedSwap {
            swap: target_swap.clone(),
            discovery_critical: true,
            processing_started_at,
            backpressure_started_at,
            last_backpressure_log_at: None,
        });

        assert!(
            pending_irrelevant_swap_backpressure_blocks_ingestion(&pending_irrelevant_swaps),
            "under the stale empty-target ownership, generic SOL buys can fully contaminate the bounded pending queue and pause ingestion before the later target-mint context is retried"
        );

        let mut refreshed_target_buy_mints = HashSet::new();
        refresh_discovery_critical_target_buy_mints_or_warn(
            &store,
            &mut refreshed_target_buy_mints,
        )?;
        let dropped = prune_noncritical_zero_universe_pending_irrelevant_swaps(
            &mut pending_irrelevant_swaps,
            &mut recent_signatures,
            &mut recent_signature_order,
            &mut telemetry,
            &empty_follow_snapshot,
            &HashSet::new(),
            true,
            &refreshed_target_buy_mints,
        );
        assert_eq!(
            dropped,
            DISCOVERY_CRITICAL_PENDING_IRRELEVANT_SWAP_CAPACITY.saturating_sub(1),
            "once the exact rebuild target mints materialize, the app must drop the stale generic backlog instead of letting it keep the bounded queue full"
        );
        assert_eq!(pending_irrelevant_swaps.len(), 1);
        assert_eq!(
            pending_irrelevant_swaps
                .front()
                .expect("target swap should remain queued")
                .swap
                .signature,
            target_swap.signature,
            "pruning must preserve the still-relevant target-mint market-context swap rather than wiping the queue indiscriminately"
        );
        assert!(
            !pending_irrelevant_swap_backpressure_blocks_ingestion(&pending_irrelevant_swaps),
            "after stale generic backlog is pruned, ingestion should no longer be paused before the later target-mint context can be retried"
        );
        assert!(
            recent_signatures.contains(&target_swap.signature),
            "the preserved target-mint swap must stay inside recent-signature dedupe state until it is retried"
        );
        assert!(
            !recent_signatures.contains("sig-generic-pending-0000"),
            "dropped stale generic backlog must release recent-signature ownership so those swaps do not stay artificially pinned forever"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn broad_unique_buy_universe_target_set_old_loader_keeps_generic_backpressure_and_blocks_later_exact_context_stage1(
    ) -> Result<()> {
        let (store, db_path) =
            make_test_store("pending-irrelevant-broad-unique-buy-universe-targets")?;
        let broad_unique_buy_mints: Vec<String> = (0
            ..DISCOVERY_CRITICAL_PENDING_IRRELEVANT_SWAP_CAPACITY)
            .map(|idx| format!("token-generic-{idx:04}"))
            .chain(std::iter::once("token-target".to_string()))
            .collect();
        let broad_unique_buy_mint_refs: Vec<&str> =
            broad_unique_buy_mints.iter().map(String::as_str).collect();
        seed_test_discovery_critical_target_buy_mints(&store, &[], &broad_unique_buy_mint_refs)?;

        let empty_follow_snapshot = FollowSnapshot::default();
        let mut recent_signatures = HashSet::new();
        let mut recent_signature_order = VecDeque::new();
        let mut telemetry = AppConsumerLoopTelemetry::default();
        let processing_started_at = StdInstant::now();
        let backpressure_started_at = StdInstant::now();
        let mut pending_irrelevant_swaps = VecDeque::new();

        for idx in 0..DISCOVERY_CRITICAL_PENDING_IRRELEVANT_SWAP_CAPACITY.saturating_sub(1) {
            let mut generic_swap = test_swap(&format!("sig-broad-generic-pending-{idx:04}"));
            generic_swap.token_out = format!("token-generic-{idx:04}");
            assert!(note_recent_swap_signature(
                &mut recent_signatures,
                &mut recent_signature_order,
                &generic_swap.signature,
            ));
            pending_irrelevant_swaps.push_back(PendingIrrelevantObservedSwap {
                swap: generic_swap,
                discovery_critical: true,
                processing_started_at,
                backpressure_started_at,
                last_backpressure_log_at: None,
            });
        }
        let mut target_swap = test_swap("sig-broad-target-pending");
        target_swap.token_out = "token-target".to_string();
        assert!(note_recent_swap_signature(
            &mut recent_signatures,
            &mut recent_signature_order,
            &target_swap.signature,
        ));
        pending_irrelevant_swaps.push_back(PendingIrrelevantObservedSwap {
            swap: target_swap,
            discovery_critical: true,
            processing_started_at,
            backpressure_started_at,
            last_backpressure_log_at: None,
        });

        let mut refreshed_target_buy_mints = HashSet::new();
        refresh_discovery_critical_target_buy_mints_or_warn_old(
            &store,
            &mut refreshed_target_buy_mints,
        )?;
        assert_eq!(
            refreshed_target_buy_mints.len(),
            broad_unique_buy_mints.len(),
            "without an exact candidate target surface, refresh still reloads the full unique-buy-mint universe"
        );
        let dropped = prune_noncritical_zero_universe_pending_irrelevant_swaps(
            &mut pending_irrelevant_swaps,
            &mut recent_signatures,
            &mut recent_signature_order,
            &mut telemetry,
            &empty_follow_snapshot,
            &HashSet::new(),
            true,
            &refreshed_target_buy_mints,
        );
        assert_eq!(
            dropped, 0,
            "when the persisted target set is still the whole unique-buy-mint universe, generic backlog remains discovery-critical and cannot be pruned away"
        );
        assert!(
            pending_irrelevant_swap_backpressure_blocks_ingestion(&pending_irrelevant_swaps),
            "the same broad target-mint ownership leaves the bounded pending queue saturated, so later exact target context still cannot make forward progress"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn broad_unique_buy_fallback_recreates_clean_start_raw_writer_full_plateau_stage1() -> Result<()>
    {
        let summary = run_broad_unique_buy_fallback_backpressure_scenario(true)?;
        let raw_writer_full_with_single_inflight_batch =
            OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY + TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE;

        assert!(
            summary.baseline_rows_persisted >= 32,
            "clean checkpoint baseline should permit immediate raw persistence before the broad unique-buy fallback plateau forms: {summary:?}"
        );
        assert!(
            summary.loaded_target_buy_mints >= OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY,
            "the old loader must actually populate a broad discovery-critical target set from unique_buy_mints, otherwise this repro is not exercising the surviving raw-writer class: {summary:?}"
        );
        assert!(
            summary.first_backpressure_pending_requests >= OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY
                && summary.first_backpressure_pending_requests
                    <= raw_writer_full_with_single_inflight_batch,
            "with the broad unique-buy fallback contract, generic SOL-leg buys should still fill the raw writer to a full 4096 queued requests, plus at most one in-flight raw batch before the first backpressure event: {summary:?}"
        );
        assert!(
            summary.max_pending_requests_before_pause >= OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY
                && summary.max_pending_requests_before_pause
                    <= raw_writer_full_with_single_inflight_batch,
            "the same broad fallback request class should keep pending requests pinned at a full raw writer plus at most one in-flight batch while aggregate/journal queues stay zero: {summary:?}"
        );
        assert_eq!(
            summary.pending_irrelevant_queue_depth_at_pause,
            DISCOVERY_CRITICAL_PENDING_IRRELEVANT_SWAP_CAPACITY,
            "after raw pending reaches the full writer, the app should keep buffering the same broad discovery-critical irrelevant class until the local pending queue also reaches 4096: {summary:?}"
        );
        assert!(
            summary.upstream_queue_depth_shortly_after_pause > 0
                && summary.upstream_queue_depth_shortly_after_pause <= 64,
            "the exact incident class needs a still-small upstream queue when the local queue first forces ingestion pause, matching the live 49-depth plateau before later upstream saturation: {summary:?}"
        );
        assert_eq!(summary.upstream_queue_depth_after_escalation, 2_048);
        assert_eq!(summary.journal_queue_depth_at_pause, 0);
        assert!(summary.runtime_wal_bytes_at_pause < 16 * 1024 * 1024);
        assert_eq!(summary.sqlite_write_retry_delta, 0);
        assert_eq!(summary.sqlite_busy_error_delta, 0);
        assert_eq!(summary.dropped_irrelevant_swaps, 0);
        assert!(summary.ingestion_paused_by_pending_irrelevant_queue);
        Ok(())
    }

    #[test]
    fn removing_broad_unique_buy_fallback_prevents_clean_start_raw_writer_full_plateau_stage1(
    ) -> Result<()> {
        let old = run_broad_unique_buy_fallback_backpressure_scenario(true)?;
        let new = run_broad_unique_buy_fallback_backpressure_scenario(false)?;
        let normal_irrelevant_soft_limit = OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY
            .saturating_sub(TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE);
        let raw_writer_full_with_single_inflight_batch =
            OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY + TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE;

        assert!(
            old.loaded_target_buy_mints >= OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY,
            "the old side of the A/B must load a broad fallback target set from unique_buy_mints: old={old:?}"
        );
        assert!(
            old.first_backpressure_pending_requests >= OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY
                && old.first_backpressure_pending_requests
                    <= raw_writer_full_with_single_inflight_batch,
            "the old side of the A/B must reproduce a full raw-writer plateau first: old={old:?}"
        );
        assert_eq!(
            new.loaded_target_buy_mints, 0,
            "with the fallback removed, app-side raw-writer pressure handling should load no discovery-critical targets from unique_buy_mints alone: new={new:?}"
        );
        assert!(
            new.dropped_irrelevant_swaps > 0,
            "without the broad unique-buy fallback, the app must explicitly drop those backpressured generic irrelevant buys instead of buffering them as discovery-critical: new={new:?}"
        );
        assert!(
            new.first_backpressure_pending_requests <= normal_irrelevant_soft_limit,
            "the fix must stop the broad unique-buy fallback from consuming the reserved raw-writer capacity and reduce the first plateau back below 4096: old={old:?} new={new:?}"
        );
        assert_eq!(new.pending_irrelevant_queue_depth_at_pause, 0);
        assert!(!new.ingestion_paused_by_pending_irrelevant_queue);
        assert_eq!(new.upstream_queue_depth_after_escalation, 0);
        assert_eq!(new.journal_queue_depth_at_pause, 0);
        assert_eq!(new.sqlite_write_retry_delta, 0);
        assert_eq!(new.sqlite_busy_error_delta, 0);
        assert!(
            new.runtime_wal_bytes_at_pause <= old.runtime_wal_bytes_at_pause * 2 + 1,
            "the fix should not just trade the raw-writer plateau for runaway WAL growth: old={old:?} new={new:?}"
        );
        Ok(())
    }

    #[test]
    fn enqueue_irrelevant_observed_swap_discovery_critical_uses_reserved_writer_capacity_without_waiting_for_commit_stage1(
    ) -> Result<()> {
        let (_store, db_path) = make_test_store("irrelevant-discovery-critical-priority-enqueue")?;
        let blocker_conn = rusqlite::Connection::open(&db_path)?;
        blocker_conn.busy_timeout(StdDuration::from_millis(1))?;
        blocker_conn.execute_batch("BEGIN IMMEDIATE TRANSACTION")?;

        let sqlite_path = db_path
            .to_str()
            .context("sqlite path must be valid utf-8")?
            .to_string();
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let runtime_handle = std::thread::spawn(move || -> Result<()> {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(async move {
                let writer = ObservedSwapWriter::start_for_test(sqlite_path, 2, 1)?;
                let filler_swap = test_swap("sig-discovery-critical-filler");
                let critical_swap = test_swap("sig-discovery-critical-target");
                let mut recent_signatures = HashSet::new();
                let mut recent_signature_order = VecDeque::new();
                for swap in [&filler_swap, &critical_swap] {
                    assert!(note_recent_swap_signature(
                        &mut recent_signatures,
                        &mut recent_signature_order,
                        &swap.signature,
                    ));
                }

                assert_eq!(
                    enqueue_irrelevant_observed_swap(
                        &writer,
                        &mut recent_signatures,
                        &mut recent_signature_order,
                        &filler_swap,
                        false,
                    )
                    .await?,
                    IrrelevantObservedSwapEnqueueOutcome::Enqueued
                );
                result_tx
                    .send("ready")
                    .expect("main thread should receive ready signal");
                let outcome = enqueue_irrelevant_observed_swap(
                    &writer,
                    &mut recent_signatures,
                    &mut recent_signature_order,
                    &critical_swap,
                    true,
                )
                .await?;
                assert_eq!(outcome, IrrelevantObservedSwapEnqueueOutcome::Enqueued);
                result_tx
                    .send("critical_enqueued")
                    .expect("main thread should receive critical enqueue signal");

                tokio::time::timeout(Duration::from_secs(5), async {
                    loop {
                        writer.ensure_running()?;
                        if writer.snapshot().pending_requests == 0 {
                            break Ok::<(), anyhow::Error>(());
                        }
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                })
                .await
                .context(
                    "writer should drain discovery-critical irrelevant swap after lock release",
                )??;
                writer.shutdown()?;
                Ok::<(), anyhow::Error>(())
            })
        });

        assert_eq!(
            result_rx
                .recv_timeout(StdDuration::from_secs(1))
                .context("runtime thread never reached discovery-critical enqueue")?,
            "ready"
        );
        assert_eq!(
            result_rx
                .recv_timeout(StdDuration::from_millis(100))
                .context("runtime thread never completed discovery-critical enqueue before lock release")?,
            "critical_enqueued",
            "discovery-critical irrelevant swap should return immediately once it claims reserved writer capacity instead of waiting for sqlite durability"
        );

        let verify_before_commit = SqliteStore::open(&db_path)?;
        assert_eq!(
            verify_before_commit
                .load_observed_swaps_since(
                    DateTime::parse_from_rfc3339("2026-03-14T15:59:00Z")
                        .expect("timestamp")
                        .with_timezone(&Utc),
                )?
                .len(),
            0,
            "sqlite lock should still keep discovery-critical irrelevant swaps invisible until the writer thread can commit them"
        );

        blocker_conn.execute_batch("COMMIT")?;
        runtime_handle
            .join()
            .expect("runtime thread panicked")
            .context(
                "discovery-critical irrelevant swap should still persist after lock release",
            )?;

        let verify_store = SqliteStore::open(&db_path)?;
        let persisted = verify_store.load_observed_swaps_since(
            DateTime::parse_from_rfc3339("2026-03-14T15:59:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        )?;
        assert_eq!(persisted.len(), 2);
        assert!(
            persisted
                .iter()
                .any(|swap| swap.signature == "sig-discovery-critical-target"),
            "the discovery-critical irrelevant swap should persist once the durable write unblocks"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }
