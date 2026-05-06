    #[test]
    fn irrelevant_observed_swap_backpressure_diagnostics_distinguish_noncritical_vs_discovery_critical_branch_stage1(
    ) {
        let follow_snapshot = FollowSnapshot::default();
        let open_shadow_lots = HashSet::new();
        let mut writer_snapshot = maintenance_test_writer_snapshot();
        writer_snapshot.pending_requests = TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE;
        let mut ingestion_snapshot = maintenance_test_ingestion_snapshot(0.0);
        ingestion_snapshot.yellowstone_output_queue_depth = 0;
        let started_at = StdInstant::now();
        let pending_irrelevant_swaps = VecDeque::from([PendingIrrelevantObservedSwap {
            swap: test_swap("sig-backpressure-diag-pending"),
            discovery_critical: false,
            processing_started_at: started_at,
            backpressure_started_at: started_at,
            last_backpressure_log_at: None,
        }]);

        let noncritical = snapshot_irrelevant_observed_swap_backpressure_diagnostics(
            IrrelevantObservedSwapBackpressureSourceBranch::Unclassified,
            false,
            &follow_snapshot,
            &open_shadow_lots,
            true,
            &HashSet::new(),
            &pending_irrelevant_swaps,
            &writer_snapshot,
            Some(ingestion_snapshot),
        );
        assert_eq!(noncritical.irrelevant_branch, "irrelevant_unclassified");
        assert!(
            !noncritical.discovery_critical_irrelevant_persistence,
            "non-critical plateau diagnostics must surface the exact try_enqueue branch that matches the live 128 plateau"
        );

        let critical_target_buy_mints = HashSet::from(["token-target".to_string()]);
        let critical = snapshot_irrelevant_observed_swap_backpressure_diagnostics(
            IrrelevantObservedSwapBackpressureSourceBranch::NotFollowed,
            true,
            &follow_snapshot,
            &open_shadow_lots,
            true,
            &critical_target_buy_mints,
            &pending_irrelevant_swaps,
            &writer_snapshot,
            Some(ingestion_snapshot),
        );
        assert_eq!(critical.irrelevant_branch, "irrelevant_not_followed");
        assert!(
            critical.discovery_critical_irrelevant_persistence,
            "discovery-critical diagnostics must surface the reserved enqueue branch so operator logs can prove whether live plateau traffic belongs to it"
        );
        assert_eq!(
            critical.writer_pending_requests, noncritical.writer_pending_requests,
            "branch diagnostics should classify the same queue state differently without mutating writer state"
        );
        assert_eq!(
            critical.yellowstone_output_queue_depth,
            noncritical.yellowstone_output_queue_depth
        );
    }

    #[test]
    fn irrelevant_observed_swap_backpressure_diagnostics_surface_zero_universe_empty_target_context_stage1(
    ) {
        let follow_snapshot = FollowSnapshot::default();
        let open_shadow_lots = HashSet::new();
        let mut writer_snapshot = maintenance_test_writer_snapshot();
        writer_snapshot.pending_requests = TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE;
        let mut ingestion_snapshot = maintenance_test_ingestion_snapshot(0.0);
        ingestion_snapshot.yellowstone_output_queue_depth = 0;
        let started_at = StdInstant::now();
        let pending_irrelevant_swaps = VecDeque::from([
            PendingIrrelevantObservedSwap {
                swap: test_swap("sig-backpressure-empty-target-1"),
                discovery_critical: false,
                processing_started_at: started_at,
                backpressure_started_at: started_at,
                last_backpressure_log_at: None,
            },
            PendingIrrelevantObservedSwap {
                swap: test_swap("sig-backpressure-empty-target-2"),
                discovery_critical: false,
                processing_started_at: started_at,
                backpressure_started_at: started_at,
                last_backpressure_log_at: None,
            },
        ]);

        let zero_universe = snapshot_irrelevant_observed_swap_backpressure_diagnostics(
            IrrelevantObservedSwapBackpressureSourceBranch::Unclassified,
            false,
            &follow_snapshot,
            &open_shadow_lots,
            true,
            &HashSet::new(),
            &pending_irrelevant_swaps,
            &writer_snapshot,
            Some(ingestion_snapshot),
        );
        assert!(
            zero_universe.zero_universe_empty_target_noncritical_context,
            "operator diagnostics must explicitly surface the exact zero-universe empty-target non-critical context instead of requiring log archaeology to infer it"
        );
        assert_eq!(zero_universe.followed_wallet_count, 0);
        assert_eq!(zero_universe.open_shadow_lot_count, 0);
        assert_eq!(zero_universe.discovery_critical_target_buy_mints_count, 0);
        assert_eq!(zero_universe.pending_irrelevant_swap_queue_depth, 2);
        assert_eq!(
            zero_universe.writer_pending_requests,
            TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE
        );
        assert_eq!(zero_universe.yellowstone_output_queue_depth, 0);

        let mut followed_snapshot = FollowSnapshot::default();
        followed_snapshot
            .active
            .insert("wallet-followed".to_string());
        let followed = snapshot_irrelevant_observed_swap_backpressure_diagnostics(
            IrrelevantObservedSwapBackpressureSourceBranch::Unclassified,
            false,
            &followed_snapshot,
            &open_shadow_lots,
            true,
            &HashSet::new(),
            &pending_irrelevant_swaps,
            &writer_snapshot,
            Some(ingestion_snapshot),
        );
        assert!(
            !followed.zero_universe_empty_target_noncritical_context,
            "once the runtime has followed wallets, diagnostics must stop reporting the exact zero-universe empty-target context"
        );
        assert_eq!(followed.followed_wallet_count, 1);
    }

    #[test]
    fn app_consumer_follow_rejected_empty_universe_enqueues_raw_irrelevant_observed_swap_stage1(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("follow-rejected-empty-universe-raw-enqueue")?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            let writer = ObservedSwapWriter::start_for_test(db_path
                    .to_str()
                    .context("sqlite path must be valid utf-8")?
                    .to_string(), OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY, TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE)?;
            let swap = test_swap("sig-follow-rejected-empty-universe-raw");
            let follow_snapshot = FollowSnapshot::default();
            let open_shadow_lots = HashSet::new();
            let relevance = classify_observed_swap_shadow_relevance(
                &swap,
                &follow_snapshot,
                &ShadowScheduler::new(),
                &open_shadow_lots,
            );
            assert!(
                matches!(
                    relevance,
                    ObservedSwapShadowRelevance::IrrelevantNotFollowed(_)
                ),
                "empty follow universe must still classify this parse-valid swap as follow-rejected, not followed/relevant"
            );
            assert!(
                !matches!(relevance, ObservedSwapShadowRelevance::Relevant(_)),
                "raw evidence persistence must not promote a follow-rejected swap into the relevant path"
            );

            let mut telemetry = AppConsumerLoopTelemetry::default();
            telemetry.note_swap_seen();
            telemetry.note_follow_rejected();
            let telemetry_snapshot = telemetry.snapshot_and_reset();
            assert_eq!(telemetry_snapshot.swaps_seen, 1);
            assert_eq!(telemetry_snapshot.follow_rejected, 1);
            assert_eq!(telemetry_snapshot.follow_rejected_ratio, 1.0);

            let discovery_critical = irrelevant_observed_swap_requires_discovery_critical_persistence(
                &swap,
                &follow_snapshot,
                &open_shadow_lots,
                true,
                &HashSet::new(),
            );
            assert!(
                !discovery_critical,
                "empty-target follow-rejected swaps should use the normal raw writer path, not the reserved discovery-critical path"
            );
            let mut recent_signatures = HashSet::new();
            let mut recent_signature_order = VecDeque::new();
            assert!(note_recent_swap_signature(
                &mut recent_signatures,
                &mut recent_signature_order,
                &swap.signature,
            ));
            assert_eq!(
                persist_irrelevant_observed_swap(
                    &writer,
                    &mut recent_signatures,
                    &mut recent_signature_order,
                    &swap,
                    discovery_critical,
                )
                .await?,
                IrrelevantObservedSwapEnqueueOutcome::Enqueued,
                "follow-rejected parse-valid swaps must still feed raw observed-swap persistence when the runtime has no follow universe"
            );
            writer.shutdown()?;
            Ok::<(), anyhow::Error>(())
        })?;

        let persisted = store.load_observed_swaps_since(
            DateTime::parse_from_rfc3339("2026-03-14T15:59:59Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        )?;
        assert!(
            persisted
                .iter()
                .any(|row| row.signature == "sig-follow-rejected-empty-universe-raw"),
            "writer shutdown must flush the follow-rejected swap as raw observed-swap source evidence"
        );
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_file(format!("{}-wal", db_path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", db_path.display()));
        Ok(())
    }

    #[test]
    fn irrelevant_not_followed_without_ownership_surface_enqueues_before_writer_backpressure_stage1(
    ) -> Result<()> {
        let summary = run_irrelevant_observed_swap_no_ownership_surface_saturation_scenario(
            IrrelevantObservedSwapScenarioBranch::NotFollowed,
            false,
        )?;
        assert_eq!(
            summary.first_backpressure_pending_requests, TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE,
            "the exact proven live branch must now reach the raw writer path before normal non-critical writer backpressure applies"
        );
        assert!(
            summary.writer_pending_requests_peak > 0,
            "follow-rejected swaps must be observable at the raw writer instead of starving the Stage 3 source frontier"
        );
        assert_eq!(summary.journal_queue_depth_at_peak, 0);
        assert!(
            summary.accepted_swaps > 0,
            "the reduced live class should enqueue parse-valid raw evidence before any conservative backpressure drop"
        );
        assert!(
            summary.completed_waves > 0,
            "normal writer backpressure remains active after the raw frontier path has been exercised"
        );
        Ok(())
    }

    #[test]
    fn irrelevant_not_followed_without_ownership_surface_old_drop_would_starve_raw_frontier_stage1(
    ) -> Result<()> {
        let old_like = run_irrelevant_observed_swap_no_ownership_surface_saturation_scenario(
            IrrelevantObservedSwapScenarioBranch::NotFollowed,
            true,
        )?;
        let current = run_irrelevant_observed_swap_no_ownership_surface_saturation_scenario(
            IrrelevantObservedSwapScenarioBranch::NotFollowed,
            false,
        )?;
        assert_eq!(
            old_like.first_backpressure_pending_requests, 0,
            "the removed early-drop behavior reproduces the source-frontier starvation shape: no writer backpressure because no writer enqueue"
        );
        assert_eq!(old_like.writer_pending_requests_peak, 0);
        assert_eq!(old_like.accepted_swaps, 0);
        assert!(
            old_like.dropped_swaps > 0,
            "the old-like branch drops follow-rejected raw evidence before the writer can see it"
        );
        assert_eq!(
            current.first_backpressure_pending_requests,
            TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE,
            "current behavior must feed follow-rejected raw evidence to the writer before conservative backpressure"
        );
        assert!(
            current.accepted_swaps > 0,
            "current behavior restores raw source evidence flow for follow-rejected swaps"
        );
        assert!(
            current.writer_pending_requests_peak > old_like.writer_pending_requests_peak,
            "the corrective behavior must materially change the proven writer-idle/source-frontier-freeze seam"
        );
        Ok(())
    }

    #[test]
    fn irrelevant_unclassified_without_ownership_surface_behavior_is_unchanged_stage1() -> Result<()>
    {
        let old = run_irrelevant_observed_swap_no_ownership_surface_saturation_scenario(
            IrrelevantObservedSwapScenarioBranch::Unclassified,
            false,
        )?;
        let new = run_irrelevant_observed_swap_no_ownership_surface_saturation_scenario(
            IrrelevantObservedSwapScenarioBranch::Unclassified,
            true,
        )?;
        assert_eq!(
            old.first_backpressure_pending_requests,
            TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE,
            "the unchanged unclassified branch should still reproduce the old plateau under the same reduced no-ownership state"
        );
        assert_eq!(
            new.first_backpressure_pending_requests, TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE,
            "enabling the new drop contract must not intercept the unclassified branch"
        );
        assert!(old.completed_waves > 0);
        assert!(new.completed_waves > 0);
        Ok(())
    }

    #[test]
    fn irrelevant_not_followed_target_buy_mint_reserved_path_still_enqueues_raw_evidence_stage1(
    ) -> Result<()> {
        let (_store, db_path) =
            make_test_store("irrelevant-not-followed-target-surface-preserved")?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            let writer = ObservedSwapWriter::start_for_test(db_path
                    .to_str()
                    .context("sqlite path must be valid utf-8")?
                    .to_string(), OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY, TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE)?;
            let swap = test_swap("sig-not-followed-target-surface");
            let follow_snapshot = FollowSnapshot::default();
            let open_shadow_lots = HashSet::new();
            let discovery_critical_target_buy_mints = HashSet::from([swap.token_out.clone()]);
            let relevance = classify_observed_swap_shadow_relevance(
                &swap,
                &follow_snapshot,
                &ShadowScheduler::new(),
                &open_shadow_lots,
            );
            assert!(
                matches!(relevance, ObservedSwapShadowRelevance::IrrelevantNotFollowed(_)),
                "the regression must stay on the real irrelevant_not_followed branch"
            );
            let discovery_critical = irrelevant_observed_swap_requires_discovery_critical_persistence(
                &swap,
                &follow_snapshot,
                &open_shadow_lots,
                true,
                &discovery_critical_target_buy_mints,
            );
            assert!(discovery_critical);
            let mut recent_signatures = HashSet::new();
            let mut recent_signature_order = VecDeque::new();
            assert!(note_recent_swap_signature(
                &mut recent_signatures,
                &mut recent_signature_order,
                &swap.signature,
            ));
            assert_eq!(
                persist_irrelevant_observed_swap(
                    &writer,
                    &mut recent_signatures,
                    &mut recent_signature_order,
                    &swap,
                    discovery_critical,
                )
                .await?,
                IrrelevantObservedSwapEnqueueOutcome::Enqueued,
                "target-buy-mint ownership must preserve the previous reserved-path persistence behavior"
            );
            writer.shutdown()?;
            Ok::<(), anyhow::Error>(())
        })?;
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_file(format!("{}-wal", db_path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", db_path.display()));
        Ok(())
    }

    #[test]
    fn app_consumer_relevant_followed_swap_write_path_unchanged_stage1() -> Result<()> {
        let (store, db_path) = make_test_store("followed-relevant-write-path-unchanged")?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            let writer = ObservedSwapWriter::start_for_test(db_path
                    .to_str()
                    .context("sqlite path must be valid utf-8")?
                    .to_string(), OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY, TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE)?;
            let swap = test_swap("sig-followed-relevant-write-unchanged");
            let mut follow_snapshot = FollowSnapshot::default();
            follow_snapshot.active.insert(swap.wallet.clone());
            let relevance = classify_observed_swap_shadow_relevance(
                &swap,
                &follow_snapshot,
                &ShadowScheduler::new(),
                &HashSet::new(),
            );
            assert!(
                matches!(
                    relevance,
                    ObservedSwapShadowRelevance::Relevant(ShadowSwapSide::Buy)
                ),
                "followed wallets must remain on the relevant persistence path"
            );
            let mut recent_signatures = HashSet::new();
            let mut recent_signature_order = VecDeque::new();
            assert!(note_recent_swap_signature(
                &mut recent_signatures,
                &mut recent_signature_order,
                &swap.signature,
            ));
            assert!(
                persist_relevant_observed_swap(
                    &writer,
                    &mut recent_signatures,
                    &mut recent_signature_order,
                    &swap,
                )
                .await?,
                "first followed/relevant observed swap write should still insert"
            );
            writer.shutdown()?;
            Ok::<(), anyhow::Error>(())
        })?;
        let persisted = store.load_observed_swaps_since(
            DateTime::parse_from_rfc3339("2026-03-14T15:59:59Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        )?;
        assert!(
            persisted
                .iter()
                .any(|row| row.signature == "sig-followed-relevant-write-unchanged"),
            "relevant/followed raw write path must remain unchanged"
        );
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_file(format!("{}-wal", db_path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", db_path.display()));
        Ok(())
    }
