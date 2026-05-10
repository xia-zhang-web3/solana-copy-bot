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
                refresh_discovery_critical_target_buy_mints_or_warn_old(
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
                "legacy helper should still classify target-mint SOL buys as discovery-critical for the historical A/B side"
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
                !irrelevant_observed_swap_requires_discovery_critical_persistence(
                    &swap,
                    &follow_snapshot,
                    &open_shadow_lots,
                    true,
                    &target_buy_mints_new,
                ),
                "production refresh must not recover legacy persisted rebuild target mints; V2 runtime cannot let old target-mint state keep an irrelevant swap discovery-critical"
            );
        }

        assert_eq!(
            old_refresh_attempts, 128,
            "legacy helper should attempt a target-mint refresh on every backpressured discovery-critical irrelevant swap in the same burst"
        );
        assert_eq!(
            new_refresh_attempts, 1,
            "new path must collapse same-burst backpressure refreshes to one no-op attempt per interval"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn load_discovery_critical_target_buy_mints_ignores_legacy_exact_candidate_targets_stage1(
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
        assert!(
            loaded.is_empty(),
            "production app must ignore legacy persisted rebuild target mints; Discovery V2 runtime identity must not be extended by old rebuild state"
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
        assert_eq!(dropped, DISCOVERY_CRITICAL_PENDING_IRRELEVANT_SWAP_CAPACITY);
        assert!(pending_irrelevant_swaps.is_empty());
        assert!(
            !pending_irrelevant_swap_backpressure_blocks_ingestion(&pending_irrelevant_swaps),
            "after legacy target-mint refresh is ignored, ingestion should no longer be paused by stale discovery-critical backlog"
        );
        assert!(
            !recent_signatures.contains(&target_swap.signature),
            "the target-mint swap must not stay pinned by legacy rebuild state"
        );
        assert!(
            !recent_signatures.contains("sig-generic-pending-0000"),
            "dropped stale generic backlog must release recent-signature ownership so those swaps do not stay artificially pinned forever"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }
