    #[test]
    fn recent_swap_signature_dedupe_rejects_duplicate_until_evicted() {
        let mut recent_signatures = HashSet::new();
        let mut recent_signature_order = VecDeque::new();

        assert!(note_recent_swap_signature(
            &mut recent_signatures,
            &mut recent_signature_order,
            "sig-1",
        ));
        assert!(
            !note_recent_swap_signature(
                &mut recent_signatures,
                &mut recent_signature_order,
                "sig-1",
            ),
            "duplicate signature should be rejected while still inside bounded dedupe window"
        );

        for idx in 0..RECENT_SWAP_SIGNATURE_DEDUPE_CAPACITY {
            assert!(note_recent_swap_signature(
                &mut recent_signatures,
                &mut recent_signature_order,
                &format!("sig-evict-{idx}"),
            ));
        }

        assert!(
            note_recent_swap_signature(
                &mut recent_signatures,
                &mut recent_signature_order,
                "sig-1",
            ),
            "signature should become admissible again once it is evicted from bounded dedupe state"
        );
    }

    #[test]
    fn recent_swap_signature_dedupe_allows_retry_after_forget() {
        let mut recent_signatures = HashSet::new();
        let mut recent_signature_order = VecDeque::new();

        assert!(note_recent_swap_signature(
            &mut recent_signatures,
            &mut recent_signature_order,
            "sig-retry",
        ));
        assert!(
            !note_recent_swap_signature(
                &mut recent_signatures,
                &mut recent_signature_order,
                "sig-retry",
            ),
            "signature should stay deduped until the failed enqueue rollback forgets it"
        );

        forget_recent_swap_signature(
            &mut recent_signatures,
            &mut recent_signature_order,
            "sig-retry",
        );

        assert!(
            note_recent_swap_signature(
                &mut recent_signatures,
                &mut recent_signature_order,
                "sig-retry",
            ),
            "rollback after failed enqueue should make the signature admissible again"
        );
    }

    #[test]
    fn persist_relevant_observed_swap_rolls_back_recent_signature_on_write_error() -> Result<()> {
        let (_store, db_path) = make_test_store("relevant-observed-swap-write-failure")?;
        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute_batch(
            "CREATE TRIGGER fail_relevant_observed_swap_insert
             BEFORE INSERT ON observed_swaps
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            let writer = ObservedSwapWriter::start(db_path
                    .to_str()
                    .context("sqlite path must be valid utf-8")?
                    .to_string())?;
            let swap = test_swap("sig-relevant-write-failure");
            let mut recent_signatures = HashSet::new();
            let mut recent_signature_order = VecDeque::new();

            assert!(note_recent_swap_signature(
                &mut recent_signatures,
                &mut recent_signature_order,
                &swap.signature,
            ));
            assert!(
                !note_recent_swap_signature(
                    &mut recent_signatures,
                    &mut recent_signature_order,
                    &swap.signature,
                ),
                "signature should stay deduped until the failed relevant write forgets it"
            );

            let error = persist_relevant_observed_swap(
                &writer,
                &mut recent_signatures,
                &mut recent_signature_order,
                &swap,
            )
            .await
            .expect_err("fatal relevant observed-swap write must bubble");
            let error_text = format!("{error:#}");
            assert!(
                error_text.contains("failed to insert observed swap batch with activity days"),
                "expected fatal writer batch error to survive relevant-path helper, got: {error_text}"
            );
            assert!(
                error_text.contains("xShmMap"),
                "expected fatal sqlite I/O marker to survive relevant-path helper, got: {error_text}"
            );

            assert!(
                note_recent_swap_signature(
                    &mut recent_signatures,
                    &mut recent_signature_order,
                    &swap.signature,
                ),
                "failed relevant write must forget the signature so the swap can be retried"
            );

            let shutdown_error = writer
                .shutdown()
                .expect_err("fatal writer failure should surface again on shutdown");
            assert!(
                error_chain_contains(&shutdown_error, "failed to insert observed swap batch"),
                "expected shutdown to surface the writer failure, got: {shutdown_error:#}"
            );

            Ok::<(), anyhow::Error>(())
        })?;

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn persist_relevant_observed_swap_reports_db_duplicate_without_forgetting_signature(
    ) -> Result<()> {
        let (_store, db_path) = make_test_store("relevant-observed-swap-duplicate")?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            let writer = ObservedSwapWriter::start(db_path
                    .to_str()
                    .context("sqlite path must be valid utf-8")?
                    .to_string())?;
            let swap = test_swap("sig-relevant-duplicate");
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
                "first relevant write should insert the swap"
            );

            forget_recent_swap_signature(
                &mut recent_signatures,
                &mut recent_signature_order,
                &swap.signature,
            );
            assert!(note_recent_swap_signature(
                &mut recent_signatures,
                &mut recent_signature_order,
                &swap.signature,
            ));

            assert!(
                !persist_relevant_observed_swap(
                    &writer,
                    &mut recent_signatures,
                    &mut recent_signature_order,
                    &swap,
                )
                .await?,
                "db duplicate should be surfaced as Ok(false)"
            );
            assert!(
                !note_recent_swap_signature(
                    &mut recent_signatures,
                    &mut recent_signature_order,
                    &swap.signature,
                ),
                "db duplicate should keep the signature inside recent dedupe state"
            );

            writer.shutdown()?;
            Ok::<(), anyhow::Error>(())
        })?;

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_shadow_relevance_marks_unclassified_swaps_irrelevant() {
        let mut swap = test_swap("sig-unclassified");
        swap.token_in = "token-a".to_string();
        swap.token_out = "token-b".to_string();

        assert_eq!(
            classify_observed_swap_shadow_relevance(
                &swap,
                &FollowSnapshot::default(),
                &ShadowScheduler::new(),
                &HashSet::new(),
            ),
            ObservedSwapShadowRelevance::IrrelevantUnclassified
        );
    }

    #[test]
    fn observed_swap_shadow_relevance_marks_unfollowed_buy_irrelevant() {
        let swap = test_swap("sig-unfollowed-buy");

        assert_eq!(
            classify_observed_swap_shadow_relevance(
                &swap,
                &FollowSnapshot::default(),
                &ShadowScheduler::new(),
                &HashSet::new(),
            ),
            ObservedSwapShadowRelevance::IrrelevantNotFollowed(ShadowSwapSide::Buy)
        );
    }

    #[test]
    fn observed_swap_shadow_relevance_keeps_followed_buy_relevant() {
        let swap = test_swap("sig-followed-buy");
        let mut follow_snapshot = FollowSnapshot::default();
        follow_snapshot.active.insert(swap.wallet.clone());

        assert_eq!(
            classify_observed_swap_shadow_relevance(
                &swap,
                &follow_snapshot,
                &ShadowScheduler::new(),
                &HashSet::new(),
            ),
            ObservedSwapShadowRelevance::Relevant(ShadowSwapSide::Buy)
        );
    }

    #[test]
    fn observed_swap_shadow_relevance_marks_cold_sell_irrelevant() {
        let mut swap = test_swap("sig-cold-sell");
        swap.token_in = "token-a".to_string();
        swap.token_out = "So11111111111111111111111111111111111111112".to_string();

        assert_eq!(
            classify_observed_swap_shadow_relevance(
                &swap,
                &FollowSnapshot::default(),
                &ShadowScheduler::new(),
                &HashSet::new(),
            ),
            ObservedSwapShadowRelevance::IrrelevantNotFollowed(ShadowSwapSide::Sell)
        );
    }

    #[test]
    fn observed_swap_shadow_relevance_keeps_sell_with_open_lot_relevant() {
        let mut swap = test_swap("sig-sell-open-lot");
        swap.token_in = "token-a".to_string();
        swap.token_out = "So11111111111111111111111111111111111111112".to_string();
        let sell_key = shadow_task_key_for_swap(&swap, ShadowSwapSide::Sell);
        let open_shadow_lots = HashSet::from([(sell_key.wallet.clone(), sell_key.token.clone())]);

        assert_eq!(
            classify_observed_swap_shadow_relevance(
                &swap,
                &FollowSnapshot::default(),
                &ShadowScheduler::new(),
                &open_shadow_lots,
            ),
            ObservedSwapShadowRelevance::Relevant(ShadowSwapSide::Sell)
        );
    }

    #[test]
    fn observed_swap_shadow_relevance_keeps_sell_with_recent_follow_history_relevant() {
        let mut swap = test_swap("sig-sell-recent-follow");
        swap.token_in = "token-a".to_string();
        swap.token_out = "So11111111111111111111111111111111111111112".to_string();
        let mut follow_snapshot = FollowSnapshot::default();
        follow_snapshot.demoted_at.insert(
            swap.wallet.clone(),
            swap.ts_utc - chrono::Duration::seconds(1),
        );

        assert_eq!(
            classify_observed_swap_shadow_relevance(
                &swap,
                &follow_snapshot,
                &ShadowScheduler::new(),
                &HashSet::new(),
            ),
            ObservedSwapShadowRelevance::Relevant(ShadowSwapSide::Sell)
        );
    }

    #[test]
    fn irrelevant_observed_swap_requires_discovery_critical_persistence_only_for_zero_universe_fail_closed_sol_legs_stage1(
    ) {
        let buy_swap = test_swap("sig-discovery-critical-buy");
        let mut sell_swap = test_swap("sig-discovery-critical-sell");
        sell_swap.token_in = "token-sell-critical".to_string();
        sell_swap.token_out = "So11111111111111111111111111111111111111112".to_string();
        let mut non_sol_swap = test_swap("sig-discovery-non-sol");
        non_sol_swap.token_in = "token-a".to_string();
        non_sol_swap.token_out = "token-b".to_string();

        let empty_follow_snapshot = FollowSnapshot::default();
        let populated_follow_snapshot = {
            let mut snapshot = FollowSnapshot::default();
            snapshot.active.insert("wallet-test".to_string());
            snapshot
        };
        let buy_target_mints = HashSet::from([buy_swap.token_out.clone()]);
        let sell_target_mints = HashSet::from([sell_swap.token_in.clone()]);
        let unrelated_target_mints = HashSet::from(["token-other".to_string()]);
        let open_shadow_lots =
            HashSet::from([("wallet-test".to_string(), "token-test".to_string())]);

        assert!(
            !irrelevant_observed_swap_requires_discovery_critical_persistence(
                &buy_swap,
                &empty_follow_snapshot,
                &HashSet::new(),
                true,
                &HashSet::new(),
            )
        );
        assert!(
            !irrelevant_observed_swap_requires_discovery_critical_persistence(
                &sell_swap,
                &empty_follow_snapshot,
                &HashSet::new(),
                true,
                &HashSet::new(),
            )
        );
        assert!(
            !irrelevant_observed_swap_requires_discovery_critical_persistence(
                &buy_swap,
                &empty_follow_snapshot,
                &HashSet::new(),
                false,
                &HashSet::new(),
            )
        );
        assert!(
            !irrelevant_observed_swap_requires_discovery_critical_persistence(
                &buy_swap,
                &populated_follow_snapshot,
                &HashSet::new(),
                true,
                &HashSet::new(),
            )
        );
        assert!(
            !irrelevant_observed_swap_requires_discovery_critical_persistence(
                &buy_swap,
                &empty_follow_snapshot,
                &open_shadow_lots,
                true,
                &HashSet::new(),
            )
        );
        assert!(
            !irrelevant_observed_swap_requires_discovery_critical_persistence(
                &non_sol_swap,
                &empty_follow_snapshot,
                &HashSet::new(),
                true,
                &HashSet::new(),
            )
        );
        assert!(
            irrelevant_observed_swap_requires_discovery_critical_persistence(
                &buy_swap,
                &empty_follow_snapshot,
                &HashSet::new(),
                true,
                &buy_target_mints,
            )
        );
        assert!(
            !irrelevant_observed_swap_requires_discovery_critical_persistence(
                &buy_swap,
                &empty_follow_snapshot,
                &HashSet::new(),
                true,
                &unrelated_target_mints,
            )
        );
        assert!(
            irrelevant_observed_swap_requires_discovery_critical_persistence(
                &sell_swap,
                &empty_follow_snapshot,
                &HashSet::new(),
                true,
                &sell_target_mints,
            )
        );
        assert!(
            !irrelevant_observed_swap_requires_discovery_critical_persistence(
                &sell_swap,
                &empty_follow_snapshot,
                &HashSet::new(),
                true,
                &buy_target_mints,
            )
        );
    }

    #[test]
    fn discovery_critical_irrelevant_backpressure_refresh_narrows_stale_empty_target_set_stage1(
    ) -> Result<()> {
        let (store, db_path) =
            make_test_store("discovery-critical-backpressure-refresh-target-mints")?;
        seed_test_discovery_critical_target_buy_mints(
            &store,
            &["token-target"],
            &["token-generic-a", "token-generic-b", "token-target"],
        )?;

        let empty_follow_snapshot = FollowSnapshot::default();
        let mut stale_target_buy_mints = HashSet::new();
        let mut backpressure_refresh_state =
            DiscoveryCriticalTargetBuyMintsBackpressureRefreshState::default();
        let mut generic_buy = test_swap("sig-generic-buy");
        generic_buy.token_out = "token-generic".to_string();
        let mut target_buy = test_swap("sig-target-buy");
        target_buy.token_out = "token-target".to_string();

        assert!(
            !irrelevant_observed_swap_requires_discovery_critical_persistence(
                &generic_buy,
                &empty_follow_snapshot,
                &HashSet::new(),
                true,
                &stale_target_buy_mints,
            ),
            "current app-side ownership should not escalate a stale empty target set into broad discovery-critical SOL-buy persistence anymore"
        );
        assert!(
            !refresh_discovery_critical_irrelevant_persistence_for_backpressure(
                &store,
                &generic_buy,
                &empty_follow_snapshot,
                &HashSet::new(),
                true,
                &mut stale_target_buy_mints,
                &mut backpressure_refresh_state,
            )?,
            "once the exact rebuild target mints are refreshed from persisted state, the same generic SOL buy must stop being treated as discovery-critical"
        );
        assert_eq!(
            stale_target_buy_mints,
            HashSet::from(["token-target".to_string()]),
            "refresh should narrow the in-memory target mint ownership onto the persisted rebuild exact buy-mint set"
        );
        assert!(
            refresh_discovery_critical_irrelevant_persistence_for_backpressure(
                &store,
                &target_buy,
                &empty_follow_snapshot,
                &HashSet::new(),
                true,
                &mut stale_target_buy_mints,
                &mut backpressure_refresh_state,
            )?,
            "a SOL buy into the persisted rebuild target mint must remain discovery-critical after refresh"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }
