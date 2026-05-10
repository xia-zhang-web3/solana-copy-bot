    #[test]
    fn queue_full_prefers_sell_over_buy_under_mixed_flow() {
        fn make_buy_task(signature: &str, wallet: &str, token: &str) -> ShadowTaskInput {
            ShadowTaskInput {
                swap: SwapEvent {
                    wallet: wallet.to_string(),
                    dex: "pumpswap".to_string(),
                    token_in: "So11111111111111111111111111111111111111112".to_string(),
                    token_out: token.to_string(),
                    amount_in: 1.0,
                    amount_out: 1000.0,
                    signature: signature.to_string(),
                    slot: 1,
                    ts_utc: Utc::now(),
                    exact_amounts: None,
                },
                follow_snapshot: Arc::new(FollowSnapshot::default()),
                key: ShadowTaskKey {
                    wallet: wallet.to_string(),
                    token: token.to_string(),
                },
            }
        }

        fn make_sell_task(signature: &str, wallet: &str, token: &str) -> ShadowTaskInput {
            ShadowTaskInput {
                swap: SwapEvent {
                    wallet: wallet.to_string(),
                    dex: "pumpswap".to_string(),
                    token_in: token.to_string(),
                    token_out: "So11111111111111111111111111111111111111112".to_string(),
                    amount_in: 1000.0,
                    amount_out: 1.0,
                    signature: signature.to_string(),
                    slot: 1,
                    ts_utc: Utc::now(),
                    exact_amounts: None,
                },
                follow_snapshot: Arc::new(FollowSnapshot::default()),
                key: ShadowTaskKey {
                    wallet: wallet.to_string(),
                    token: token.to_string(),
                },
            }
        }

        let mut shadow_scheduler = ShadowScheduler::new();
        let mut shadow_drop_reason_counts: BTreeMap<&'static str, u64> = BTreeMap::new();
        let mut shadow_drop_stage_counts: BTreeMap<&'static str, u64> = BTreeMap::new();
        let mut shadow_queue_full_outcome_counts: BTreeMap<&'static str, u64> = BTreeMap::new();
        let cap = 1usize;

        assert!(shadow_scheduler
            .enqueue_shadow_task(cap, make_buy_task("B0", "wallet-buy", "token-buy"),)
            .is_ok());
        assert_eq!(shadow_scheduler.pending_shadow_task_count, 1);

        let overflow_buy = shadow_scheduler
            .enqueue_shadow_task(cap, make_buy_task("B1", "wallet-buy-2", "token-buy-2"))
            .expect_err("buy should overflow at cap");
        shadow_scheduler.handle_shadow_enqueue_overflow(
            ShadowSwapSide::Buy,
            overflow_buy,
            cap,
            &mut shadow_drop_reason_counts,
            &mut shadow_drop_stage_counts,
            &mut shadow_queue_full_outcome_counts,
        );

        assert_eq!(shadow_scheduler.pending_shadow_task_count, 1);
        assert_eq!(
            shadow_queue_full_outcome_counts.get("queue_full_buy_drop"),
            Some(&1)
        );

        let sell_task = make_sell_task("S1", "wallet-sell", "token-sell");
        let sell_key = sell_task.key.clone();
        shadow_scheduler
            .inflight_shadow_keys
            .insert(sell_key.clone());
        let overflow_sell = shadow_scheduler
            .enqueue_shadow_task(cap, sell_task)
            .expect_err("sell should overflow at cap before policy handling");

        shadow_scheduler.handle_shadow_enqueue_overflow(
            ShadowSwapSide::Sell,
            overflow_sell,
            cap,
            &mut shadow_drop_reason_counts,
            &mut shadow_drop_stage_counts,
            &mut shadow_queue_full_outcome_counts,
        );

        assert_eq!(shadow_scheduler.pending_shadow_task_count, 1);
        assert_eq!(
            shadow_queue_full_outcome_counts.get("queue_full_sell_kept"),
            Some(&1)
        );
        assert!(
            !shadow_queue_full_outcome_counts.contains_key("queue_full_sell_dropped"),
            "sell should be kept when a pending buy can be evicted"
        );

        let queued_sell = shadow_scheduler
            .pending_shadow_tasks
            .get(&sell_key)
            .expect("sell task should remain queued");
        assert_eq!(queued_sell.len(), 1);
        assert!(
            shadow_scheduler
                .ready_shadow_key_set
                .get(&sell_key)
                .is_none(),
            "inflight key must not be marked ready"
        );
    }

    #[test]
    fn causal_holdback_counts_held_sells_against_global_capacity() {
        fn make_buy_task(signature: &str, wallet: &str, token: &str) -> ShadowTaskInput {
            ShadowTaskInput {
                swap: SwapEvent {
                    wallet: wallet.to_string(),
                    dex: "pumpswap".to_string(),
                    token_in: "So11111111111111111111111111111111111111112".to_string(),
                    token_out: token.to_string(),
                    amount_in: 1.0,
                    amount_out: 1000.0,
                    signature: signature.to_string(),
                    slot: 1,
                    ts_utc: Utc::now(),
                    exact_amounts: None,
                },
                follow_snapshot: Arc::new(FollowSnapshot::default()),
                key: ShadowTaskKey {
                    wallet: wallet.to_string(),
                    token: token.to_string(),
                },
            }
        }

        fn make_sell_task(signature: &str, wallet: &str, token: &str) -> ShadowTaskInput {
            ShadowTaskInput {
                swap: SwapEvent {
                    wallet: wallet.to_string(),
                    dex: "pumpswap".to_string(),
                    token_in: token.to_string(),
                    token_out: "So11111111111111111111111111111111111111112".to_string(),
                    amount_in: 1000.0,
                    amount_out: 1.0,
                    signature: signature.to_string(),
                    slot: 1,
                    ts_utc: Utc::now(),
                    exact_amounts: None,
                },
                follow_snapshot: Arc::new(FollowSnapshot::default()),
                key: ShadowTaskKey {
                    wallet: wallet.to_string(),
                    token: token.to_string(),
                },
            }
        }

        let mut shadow_scheduler = ShadowScheduler::new();
        let mut shadow_drop_reason_counts: BTreeMap<&'static str, u64> = BTreeMap::new();
        let mut shadow_drop_stage_counts: BTreeMap<&'static str, u64> = BTreeMap::new();
        let mut shadow_queue_full_outcome_counts: BTreeMap<&'static str, u64> = BTreeMap::new();
        let cap = 1usize;
        let now = Utc::now();

        assert!(shadow_scheduler
            .enqueue_shadow_task(cap, make_buy_task("B0", "wallet-buy", "token-buy"))
            .is_ok());
        assert_eq!(shadow_scheduler.buffered_shadow_task_count(), 1);

        let held_sell = make_sell_task("S1", "wallet-sell", "token-sell");
        let overflow_sell = shadow_scheduler
            .hold_sell_for_causality(cap, held_sell, 2_500, now)
            .expect_err("held sell should respect the global buffered cap");
        shadow_scheduler.handle_held_sell_overflow(
            overflow_sell,
            cap,
            2_500,
            now,
            &mut shadow_drop_reason_counts,
            &mut shadow_drop_stage_counts,
            &mut shadow_queue_full_outcome_counts,
        );

        assert_eq!(shadow_scheduler.pending_shadow_task_count, 0);
        assert_eq!(shadow_scheduler.held_shadow_sell_count(), 1);
        assert_eq!(shadow_scheduler.buffered_shadow_task_count(), 1);
        assert_eq!(
            shadow_scheduler
                .shadow_holdback_counts
                .get("queued_after_buy_evict"),
            Some(&1)
        );
        assert_eq!(
            shadow_queue_full_outcome_counts.get("queue_full_buy_drop"),
            Some(&1)
        );
        assert_eq!(
            shadow_queue_full_outcome_counts.get("queue_full_sell_kept"),
            Some(&1)
        );
    }

    #[test]
    fn release_held_shadow_sells_preserves_buffered_count_accounting() {
        fn make_sell_task(signature: &str, wallet: &str, token: &str) -> ShadowTaskInput {
            ShadowTaskInput {
                swap: SwapEvent {
                    wallet: wallet.to_string(),
                    dex: "pumpswap".to_string(),
                    token_in: token.to_string(),
                    token_out: "So11111111111111111111111111111111111111112".to_string(),
                    amount_in: 1000.0,
                    amount_out: 1.0,
                    signature: signature.to_string(),
                    slot: 1,
                    ts_utc: Utc::now(),
                    exact_amounts: None,
                },
                follow_snapshot: Arc::new(FollowSnapshot::default()),
                key: ShadowTaskKey {
                    wallet: wallet.to_string(),
                    token: token.to_string(),
                },
            }
        }

        let mut shadow_scheduler = ShadowScheduler::new();
        let mut shadow_drop_reason_counts: BTreeMap<&'static str, u64> = BTreeMap::new();
        let mut shadow_drop_stage_counts: BTreeMap<&'static str, u64> = BTreeMap::new();
        let mut shadow_queue_full_outcome_counts: BTreeMap<&'static str, u64> = BTreeMap::new();
        let now = Utc::now();

        assert!(shadow_scheduler
            .hold_sell_for_causality(
                2,
                make_sell_task("S1", "wallet-sell", "token-sell"),
                100,
                now
            )
            .is_ok());
        assert_eq!(shadow_scheduler.pending_shadow_task_count, 0);
        assert_eq!(shadow_scheduler.held_shadow_sell_count(), 1);
        assert_eq!(shadow_scheduler.buffered_shadow_task_count(), 1);

        shadow_scheduler.release_held_shadow_sells(
            &HashSet::new(),
            &mut shadow_drop_reason_counts,
            &mut shadow_drop_stage_counts,
            &mut shadow_queue_full_outcome_counts,
            2,
            now + chrono::Duration::milliseconds(101),
        );

        assert_eq!(shadow_scheduler.pending_shadow_task_count, 1);
        assert_eq!(shadow_scheduler.held_shadow_sell_count(), 0);
        assert_eq!(shadow_scheduler.buffered_shadow_task_count(), 1);
        assert_eq!(
            shadow_scheduler
                .shadow_holdback_counts
                .get("released_expired"),
            Some(&1)
        );
    }

    #[test]
    fn enqueue_shadow_task_respects_global_buffered_capacity_with_held_sells() {
        fn make_buy_task(signature: &str, wallet: &str, token: &str) -> ShadowTaskInput {
            ShadowTaskInput {
                swap: SwapEvent {
                    wallet: wallet.to_string(),
                    dex: "pumpswap".to_string(),
                    token_in: "So11111111111111111111111111111111111111112".to_string(),
                    token_out: token.to_string(),
                    amount_in: 1.0,
                    amount_out: 1000.0,
                    signature: signature.to_string(),
                    slot: 1,
                    ts_utc: Utc::now(),
                    exact_amounts: None,
                },
                follow_snapshot: Arc::new(FollowSnapshot::default()),
                key: ShadowTaskKey {
                    wallet: wallet.to_string(),
                    token: token.to_string(),
                },
            }
        }

        fn make_sell_task(signature: &str, wallet: &str, token: &str) -> ShadowTaskInput {
            ShadowTaskInput {
                swap: SwapEvent {
                    wallet: wallet.to_string(),
                    dex: "pumpswap".to_string(),
                    token_in: token.to_string(),
                    token_out: "So11111111111111111111111111111111111111112".to_string(),
                    amount_in: 1000.0,
                    amount_out: 1.0,
                    signature: signature.to_string(),
                    slot: 1,
                    ts_utc: Utc::now(),
                    exact_amounts: None,
                },
                follow_snapshot: Arc::new(FollowSnapshot::default()),
                key: ShadowTaskKey {
                    wallet: wallet.to_string(),
                    token: token.to_string(),
                },
            }
        }

        let mut shadow_scheduler = ShadowScheduler::new();
        let now = Utc::now();

        assert!(shadow_scheduler
            .hold_sell_for_causality(
                1,
                make_sell_task("S1", "wallet-sell", "token-sell"),
                100,
                now
            )
            .is_ok());
        assert_eq!(shadow_scheduler.buffered_shadow_task_count(), 1);

        assert!(shadow_scheduler
            .enqueue_shadow_task(1, make_buy_task("B1", "wallet-buy", "token-buy"))
            .is_err());
        assert_eq!(shadow_scheduler.pending_shadow_task_count, 0);
        assert_eq!(shadow_scheduler.held_shadow_sell_count(), 1);
        assert_eq!(shadow_scheduler.buffered_shadow_task_count(), 1);
    }

    #[test]
    fn inline_processing_respects_per_key_serialization() {
        let key = ShadowTaskKey {
            wallet: "wallet-a".to_string(),
            token: "token-x".to_string(),
        };
        let mut shadow_scheduler = ShadowScheduler::new();

        assert!(shadow_scheduler.should_process_shadow_inline(
            true,
            false,
            SHADOW_WORKER_POOL_SIZE,
            &key,
        ));

        shadow_scheduler.inflight_shadow_keys.insert(key.clone());
        assert!(!shadow_scheduler.should_process_shadow_inline(
            true,
            false,
            SHADOW_WORKER_POOL_SIZE,
            &key,
        ));

        shadow_scheduler.inflight_shadow_keys.clear();
        shadow_scheduler.pending_shadow_tasks.insert(
            key.clone(),
            VecDeque::from([ShadowTaskInput {
                swap: SwapEvent {
                    wallet: "wallet-a".to_string(),
                    dex: "pumpswap".to_string(),
                    token_in: "So11111111111111111111111111111111111111112".to_string(),
                    token_out: "token-x".to_string(),
                    amount_in: 1.0,
                    amount_out: 1000.0,
                    signature: "sig-queued".to_string(),
                    slot: 1,
                    ts_utc: Utc::now(),
                    exact_amounts: None,
                },
                follow_snapshot: Arc::new(FollowSnapshot::default()),
                key: key.clone(),
            }]),
        );
        assert!(!shadow_scheduler.should_process_shadow_inline(
            true,
            false,
            SHADOW_WORKER_POOL_SIZE,
            &key,
        ));
        assert!(!shadow_scheduler.should_process_shadow_inline(
            true,
            true,
            SHADOW_WORKER_POOL_SIZE,
            &key,
        ));
        assert!(!shadow_scheduler.should_process_shadow_inline(
            false,
            false,
            SHADOW_WORKER_POOL_SIZE,
            &key,
        ));
        assert!(!shadow_scheduler.should_process_shadow_inline(
            true,
            false,
            SHADOW_MAX_CONCURRENT_WORKERS,
            &key,
        ));
    }

    #[test]
    fn follow_snapshot_uses_temporal_promotions_and_demotions() {
        let mut snapshot = FollowSnapshot::default();
        let promoted = DateTime::parse_from_rfc3339("2026-02-15T10:00:00Z")
            .expect("rfc3339")
            .with_timezone(&Utc);
        let demoted = DateTime::parse_from_rfc3339("2026-02-15T11:00:00Z")
            .expect("rfc3339")
            .with_timezone(&Utc);
        snapshot.promoted_at.insert("w".to_string(), promoted);
        snapshot.demoted_at.insert("w".to_string(), demoted);

        assert!(snapshot.is_followed_at("w", promoted + chrono::Duration::minutes(10)));
        assert!(!snapshot.is_followed_at("w", demoted + chrono::Duration::seconds(1)));
    }

    #[test]
    fn startup_follow_snapshot_defers_recovered_active_wallets_until_discovery_publish() {
        let (snapshot, recovered_active_wallets, shadow_strategy_fail_closed) =
            startup_follow_snapshot_from_publication_truth(
                HashSet::from(["wallet-a".to_string(), "wallet-b".to_string()]),
                None,
            );

        assert_eq!(recovered_active_wallets, 2);
        assert!(shadow_strategy_fail_closed);
        assert!(
            snapshot.active.is_empty(),
            "recovered historical followlist must not become runtime truth before discovery publishes fresh or degraded runtime truth"
        );
    }

    #[test]
    fn startup_follow_snapshot_fails_closed_without_recent_publication_even_without_residue() {
        let (snapshot, recovered_active_wallets, shadow_strategy_fail_closed) =
            startup_follow_snapshot_from_publication_truth(HashSet::new(), None);

        assert_eq!(recovered_active_wallets, 0);
        assert!(shadow_strategy_fail_closed);
        assert!(
            snapshot.active.is_empty(),
            "startup must stay fail-closed until a current V2 publication truth exists"
        );
    }

    #[test]
    fn startup_follow_snapshot_uses_recent_published_universe() {
        let recent_active_wallets = HashSet::from(["wallet-a".to_string(), "wallet-b".to_string()]);
        let recent_truth = RuntimePublicationTruthResolution::Recent(
            crate::discovery_runtime::RuntimePublishedUniverseTruth {
                runtime_mode: copybot_storage_core::DiscoveryRuntimeMode::Healthy,
                reason: "recent_publication".to_string(),
                last_published_at: Utc::now(),
                last_published_window_start: Utc::now(),
                published_scoring_source: Some("discovery_v2_operational_window".to_string()),
                published_wallet_ids: recent_active_wallets.iter().cloned().collect(),
            },
        );
        let (snapshot, recovered_active_wallets, shadow_strategy_fail_closed) =
            startup_follow_snapshot_from_publication_truth(
                HashSet::from(["wallet-stale".to_string()]),
                Some(&recent_truth),
            );

        assert_eq!(recovered_active_wallets, 0);
        assert!(!shadow_strategy_fail_closed);
        assert_eq!(snapshot.active, recent_active_wallets);
    }
