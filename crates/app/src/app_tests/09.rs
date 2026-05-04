    #[test]
    fn risk_guard_infra_blocks_when_parser_stall_detected() {
        let mut cfg = RiskConfig::default();
        cfg.shadow_infra_window_minutes = 20;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();
        guard.infra_samples = VecDeque::from([
            IngestionRuntimeSnapshot {
                ts_utc: now - chrono::Duration::minutes(21),
                ws_notifications_enqueued: 10_000,
                ws_notifications_replaced_oldest: 0,
                grpc_message_total: 950_000,
                grpc_transaction_updates_total: 1_000,
                parse_rejected_total: 50,
                grpc_decode_errors: 5,
                rpc_429: 0,
                rpc_5xx: 0,
                ingestion_lag_ms_p95: 2_000,
                yellowstone_output_queue_depth: 0,
                yellowstone_output_queue_capacity: 0,
                yellowstone_output_queue_fill_ratio: 0.0,
                yellowstone_output_oldest_age_ms: 0,
            },
            IngestionRuntimeSnapshot {
                ts_utc: now - chrono::Duration::minutes(10),
                ws_notifications_enqueued: 10_000,
                ws_notifications_replaced_oldest: 0,
                grpc_message_total: 960_000,
                grpc_transaction_updates_total: 1_200,
                parse_rejected_total: 240,
                grpc_decode_errors: 10,
                rpc_429: 0,
                rpc_5xx: 0,
                ingestion_lag_ms_p95: 2_000,
                yellowstone_output_queue_depth: 0,
                yellowstone_output_queue_capacity: 0,
                yellowstone_output_queue_fill_ratio: 0.0,
                yellowstone_output_oldest_age_ms: 0,
            },
            IngestionRuntimeSnapshot {
                ts_utc: now,
                ws_notifications_enqueued: 10_000,
                ws_notifications_replaced_oldest: 0,
                grpc_message_total: 970_000,
                grpc_transaction_updates_total: 1_400,
                parse_rejected_total: 430,
                grpc_decode_errors: 15,
                rpc_429: 0,
                rpc_5xx: 0,
                ingestion_lag_ms_p95: 2_000,
                yellowstone_output_queue_depth: 0,
                yellowstone_output_queue_capacity: 0,
                yellowstone_output_queue_fill_ratio: 0.0,
                yellowstone_output_oldest_age_ms: 0,
            },
        ]);

        let reason = guard
            .compute_infra_block_reason(now)
            .expect("parser-stall pattern over broad window must trigger block");
        assert!(
            reason.contains("parser_stall_for=20m"),
            "unexpected reason: {}",
            reason
        );
    }

    #[test]
    fn risk_guard_infra_parser_stall_does_not_block_below_ratio_threshold() {
        let mut cfg = RiskConfig::default();
        cfg.shadow_infra_window_minutes = 20;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();
        guard.infra_samples = VecDeque::from([
            IngestionRuntimeSnapshot {
                ts_utc: now - chrono::Duration::minutes(21),
                ws_notifications_enqueued: 10_000,
                ws_notifications_replaced_oldest: 0,
                grpc_message_total: 950_000,
                grpc_transaction_updates_total: 1_000,
                parse_rejected_total: 50,
                grpc_decode_errors: 5,
                rpc_429: 0,
                rpc_5xx: 0,
                ingestion_lag_ms_p95: 2_000,
                yellowstone_output_queue_depth: 0,
                yellowstone_output_queue_capacity: 0,
                yellowstone_output_queue_fill_ratio: 0.0,
                yellowstone_output_oldest_age_ms: 0,
            },
            IngestionRuntimeSnapshot {
                ts_utc: now - chrono::Duration::minutes(10),
                ws_notifications_enqueued: 10_000,
                ws_notifications_replaced_oldest: 0,
                grpc_message_total: 960_000,
                grpc_transaction_updates_total: 1_200,
                parse_rejected_total: 120,
                grpc_decode_errors: 10,
                rpc_429: 0,
                rpc_5xx: 0,
                ingestion_lag_ms_p95: 2_000,
                yellowstone_output_queue_depth: 0,
                yellowstone_output_queue_capacity: 0,
                yellowstone_output_queue_fill_ratio: 0.0,
                yellowstone_output_oldest_age_ms: 0,
            },
            IngestionRuntimeSnapshot {
                ts_utc: now,
                ws_notifications_enqueued: 10_000,
                ws_notifications_replaced_oldest: 0,
                grpc_message_total: 970_000,
                grpc_transaction_updates_total: 1_400,
                parse_rejected_total: 190,
                grpc_decode_errors: 15,
                rpc_429: 0,
                rpc_5xx: 0,
                ingestion_lag_ms_p95: 2_000,
                yellowstone_output_queue_depth: 0,
                yellowstone_output_queue_capacity: 0,
                yellowstone_output_queue_fill_ratio: 0.0,
                yellowstone_output_oldest_age_ms: 0,
            },
        ]);

        assert!(
            guard.compute_infra_block_reason(now).is_none(),
            "parser-stall gate must not trigger when error ratio is below threshold"
        );
    }

    #[test]
    fn risk_guard_infra_parser_stall_blocks_at_ratio_threshold_boundary() {
        let mut cfg = RiskConfig::default();
        cfg.shadow_infra_window_minutes = 20;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();
        guard.infra_samples = VecDeque::from([
            IngestionRuntimeSnapshot {
                ts_utc: now - chrono::Duration::minutes(21),
                ws_notifications_enqueued: 10_000,
                ws_notifications_replaced_oldest: 0,
                grpc_message_total: 950_000,
                grpc_transaction_updates_total: 1_000,
                parse_rejected_total: 50,
                grpc_decode_errors: 5,
                rpc_429: 0,
                rpc_5xx: 0,
                ingestion_lag_ms_p95: 2_000,
                yellowstone_output_queue_depth: 0,
                yellowstone_output_queue_capacity: 0,
                yellowstone_output_queue_fill_ratio: 0.0,
                yellowstone_output_oldest_age_ms: 0,
            },
            IngestionRuntimeSnapshot {
                ts_utc: now - chrono::Duration::minutes(10),
                ws_notifications_enqueued: 10_000,
                ws_notifications_replaced_oldest: 0,
                grpc_message_total: 960_000,
                grpc_transaction_updates_total: 1_200,
                parse_rejected_total: 240,
                grpc_decode_errors: 10,
                rpc_429: 0,
                rpc_5xx: 0,
                ingestion_lag_ms_p95: 2_000,
                yellowstone_output_queue_depth: 0,
                yellowstone_output_queue_capacity: 0,
                yellowstone_output_queue_fill_ratio: 0.0,
                yellowstone_output_oldest_age_ms: 0,
            },
            IngestionRuntimeSnapshot {
                ts_utc: now,
                ws_notifications_enqueued: 10_000,
                ws_notifications_replaced_oldest: 0,
                grpc_message_total: 970_000,
                grpc_transaction_updates_total: 1_400,
                parse_rejected_total: 425,
                grpc_decode_errors: 15,
                rpc_429: 0,
                rpc_5xx: 0,
                ingestion_lag_ms_p95: 2_000,
                yellowstone_output_queue_depth: 0,
                yellowstone_output_queue_capacity: 0,
                yellowstone_output_queue_fill_ratio: 0.0,
                yellowstone_output_oldest_age_ms: 0,
            },
        ]);

        let reason = guard
            .compute_infra_block_reason(now)
            .expect("parser-stall gate must trigger at >= 0.95 ratio boundary");
        assert!(
            reason.contains("parser_stall_for=20m"),
            "unexpected reason: {}",
            reason
        );
    }

    #[test]
    fn risk_guard_universe_stops_after_consecutive_breaches() -> Result<()> {
        let (store, db_path) = make_test_store("universe-stop")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_universe_min_active_follow_wallets = 15;
        cfg.shadow_universe_min_eligible_wallets = 80;
        cfg.shadow_universe_breach_cycles = 3;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();

        guard.observe_discovery_cycle(&store, now, 70, 14, None)?;
        assert!(!guard.universe_blocked);
        guard.observe_discovery_cycle(&store, now + chrono::Duration::minutes(3), 70, 14, None)?;
        assert!(!guard.universe_blocked);
        guard.observe_discovery_cycle(&store, now + chrono::Duration::minutes(6), 70, 14, None)?;
        assert!(guard.universe_blocked);

        guard.observe_discovery_cycle(&store, now + chrono::Duration::minutes(9), 150, 30, None)?;
        assert!(!guard.universe_blocked);

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_complete_published_universe_below_policy_minimum_blocks_shadow_buys_stage1(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("universe-stop-complete-published-below-policy")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_universe_min_active_follow_wallets = 15;
        cfg.shadow_universe_min_eligible_wallets = 80;
        cfg.shadow_universe_breach_cycles = 3;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();
        let discovery_output = live_like_published_discovery_output(now, 10, 10, true);

        for minute in [0_i64, 3, 6] {
            guard.observe_discovery_cycle(
                &store,
                now + chrono::Duration::minutes(minute),
                discovery_output.eligible_wallets,
                discovery_output.active_follow_wallets,
                Some(&discovery_output),
            )?;
        }

        assert!(
            guard.universe_blocked,
            "a complete but tiny published universe must still trip the hard shadow policy minimums"
        );
        let decision = guard.can_open_buy(&store, now + chrono::Duration::minutes(7), true);
        match decision {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::Universe,
                detail,
            } => {
                assert!(
                    detail.contains("universe_breach_streak=3"),
                    "unexpected universe block detail: {detail}"
                );
            }
            other => panic!("expected universe block, got {other:?}"),
        }
        let universe_stops = store.list_risk_events_by_type_desc("shadow_risk_universe_stop")?;
        assert_eq!(universe_stops.len(), 1);
        let details_json = universe_stops[0]
            .details_json
            .as_deref()
            .expect("universe stop event must include details_json");
        let details: serde_json::Value = serde_json::from_str(details_json)
            .context("failed to parse universe stop details_json")?;
        assert_eq!(details["active_follow_wallets"], serde_json::json!(10));
        assert_eq!(details["eligible_wallets"], serde_json::json!(10));
        assert_eq!(details["min_active_follow_wallets"], serde_json::json!(15));
        assert_eq!(details["min_eligible_wallets"], serde_json::json!(80));
        assert_eq!(
            details["discovery_runtime_mode"],
            serde_json::json!("degraded")
        );
        assert_eq!(details["raw_window_cap_truncated"], serde_json::json!(true));

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_complete_published_universe_stop_is_threshold_driven_not_cap_truncation_driven_stage1(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("universe-stop-complete-published-threshold-only")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_universe_min_active_follow_wallets = 15;
        cfg.shadow_universe_min_eligible_wallets = 80;
        cfg.shadow_universe_breach_cycles = 3;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();
        let discovery_output = live_like_published_discovery_output(now, 10, 10, false);

        for minute in [0_i64, 3, 6] {
            guard.observe_discovery_cycle(
                &store,
                now + chrono::Duration::minutes(minute),
                discovery_output.eligible_wallets,
                discovery_output.active_follow_wallets,
                Some(&discovery_output),
            )?;
        }

        assert!(
            guard.universe_blocked,
            "the exact live stop class is driven by policy minimums, not by raw-window truncation context"
        );
        match guard.can_open_buy(&store, now + chrono::Duration::minutes(7), true) {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::Universe,
                ..
            } => {}
            other => panic!("expected universe block, got {other:?}"),
        }
        let universe_stops = store.list_risk_events_by_type_desc("shadow_risk_universe_stop")?;
        assert_eq!(universe_stops.len(), 1);
        let details_json = universe_stops[0]
            .details_json
            .as_deref()
            .expect("universe stop event must include details_json");
        let details: serde_json::Value = serde_json::from_str(details_json)
            .context("failed to parse universe stop details_json")?;
        assert!(
            details.get("raw_window_cap_truncated").is_none(),
            "cap-truncation context should be absent here, but the stop must still arm: {details_json}"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_complete_published_universe_at_policy_minimum_allows_shadow_buys_stage1(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("universe-stop-complete-published-at-policy")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_universe_min_active_follow_wallets = 15;
        cfg.shadow_universe_min_eligible_wallets = 80;
        cfg.shadow_universe_breach_cycles = 3;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();
        let discovery_output = live_like_published_discovery_output(now, 15, 80, true);

        for minute in [0_i64, 3, 6] {
            guard.observe_discovery_cycle(
                &store,
                now + chrono::Duration::minutes(minute),
                discovery_output.eligible_wallets,
                discovery_output.active_follow_wallets,
                Some(&discovery_output),
            )?;
        }

        assert!(
            !guard.universe_blocked,
            "a complete published universe that meets the configured minimums must not be blocked"
        );
        match guard.can_open_buy(&store, now + chrono::Duration::minutes(7), true) {
            BuyRiskDecision::Allow => {}
            other => panic!("expected buy risk allow, got {other:?}"),
        }
        assert_eq!(
            store.risk_event_count_by_type("shadow_risk_universe_stop")?,
            0,
            "policy-satisfying complete truth must not emit a universe stop"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_exact_live_nine_wallet_degraded_published_universe_blocks_under_current_policy_stage1(
    ) -> Result<()> {
        let (store, db_path) =
            make_test_store("universe-stop-exact-live-nine-nine-current-policy")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_universe_min_active_follow_wallets = 15;
        cfg.shadow_universe_min_eligible_wallets = 80;
        cfg.shadow_universe_breach_cycles = 3;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();
        let discovery_output = live_like_degraded_published_universe_output(now, 9, 9);

        for minute in [0_i64, 3, 6] {
            guard.observe_discovery_cycle(
                &store,
                now + chrono::Duration::minutes(minute),
                discovery_output.eligible_wallets,
                discovery_output.active_follow_wallets,
                Some(&discovery_output),
            )?;
        }

        assert!(
            guard.universe_blocked,
            "the exact current live class must still block under the shipped 15/80 universe minimums"
        );
        match guard.can_open_buy(&store, now + chrono::Duration::minutes(7), true) {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::Universe,
                ..
            } => {}
            other => panic!("expected universe block, got {other:?}"),
        }
        let universe_stops = store.list_risk_events_by_type_desc("shadow_risk_universe_stop")?;
        assert_eq!(universe_stops.len(), 1);
        let details_json = universe_stops[0]
            .details_json
            .as_deref()
            .expect("universe stop event must include details_json");
        let details: serde_json::Value = serde_json::from_str(details_json)
            .context("failed to parse universe stop details_json")?;
        assert_eq!(details["active_follow_wallets"], serde_json::json!(9));
        assert_eq!(details["eligible_wallets"], serde_json::json!(9));
        assert_eq!(
            details["discovery_runtime_mode"],
            serde_json::json!("degraded")
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_exact_live_nine_wallet_degraded_published_universe_requires_both_minimums_to_move_stage1(
    ) -> Result<()> {
        let now = Utc::now();
        let discovery_output = live_like_degraded_published_universe_output(now, 9, 9);

        let (active_only_store, active_only_db_path) =
            make_test_store("universe-stop-exact-live-nine-nine-active-only")?;
        let mut active_only_cfg = RiskConfig::default();
        active_only_cfg.shadow_universe_min_active_follow_wallets = 9;
        active_only_cfg.shadow_universe_min_eligible_wallets = 80;
        active_only_cfg.shadow_universe_breach_cycles = 3;
        let mut active_only_guard = ShadowRiskGuard::new(active_only_cfg);
        for minute in [0_i64, 3, 6] {
            active_only_guard.observe_discovery_cycle(
                &active_only_store,
                now + chrono::Duration::minutes(minute),
                discovery_output.eligible_wallets,
                discovery_output.active_follow_wallets,
                Some(&discovery_output),
            )?;
        }
        assert!(
            active_only_guard.universe_blocked,
            "lowering only min_active_follow_wallets must still block because eligible_wallets remains below the configured floor"
        );

        let (eligible_only_store, eligible_only_db_path) =
            make_test_store("universe-stop-exact-live-nine-nine-eligible-only")?;
        let mut eligible_only_cfg = RiskConfig::default();
        eligible_only_cfg.shadow_universe_min_active_follow_wallets = 15;
        eligible_only_cfg.shadow_universe_min_eligible_wallets = 9;
        eligible_only_cfg.shadow_universe_breach_cycles = 3;
        let mut eligible_only_guard = ShadowRiskGuard::new(eligible_only_cfg);
        for minute in [0_i64, 3, 6] {
            eligible_only_guard.observe_discovery_cycle(
                &eligible_only_store,
                now + chrono::Duration::minutes(minute),
                discovery_output.eligible_wallets,
                discovery_output.active_follow_wallets,
                Some(&discovery_output),
            )?;
        }
        assert!(
            eligible_only_guard.universe_blocked,
            "lowering only min_eligible_wallets must still block because active_follow_wallets remains below the configured floor"
        );

        let (both_store, both_db_path) =
            make_test_store("universe-stop-exact-live-nine-nine-both-minimums")?;
        let mut both_cfg = RiskConfig::default();
        both_cfg.shadow_universe_min_active_follow_wallets = 9;
        both_cfg.shadow_universe_min_eligible_wallets = 9;
        both_cfg.shadow_universe_breach_cycles = 3;
        let mut both_guard = ShadowRiskGuard::new(both_cfg);
        for minute in [0_i64, 3, 6] {
            both_guard.observe_discovery_cycle(
                &both_store,
                now + chrono::Duration::minutes(minute),
                discovery_output.eligible_wallets,
                discovery_output.active_follow_wallets,
                Some(&discovery_output),
            )?;
        }
        assert!(
            !both_guard.universe_blocked,
            "moving both minimums to the exact current live discovery surface is the smallest policy change that clears the universe stop"
        );
        match both_guard.can_open_buy(&both_store, now + chrono::Duration::minutes(7), true) {
            BuyRiskDecision::Allow => {}
            other => panic!("expected buy risk allow, got {other:?}"),
        }
        assert_eq!(
            both_store.risk_event_count_by_type("shadow_risk_universe_stop")?,
            0,
            "the exact-live 9/9 universe should not emit a universe stop once both minimums match that current discovery surface"
        );

        let _ = std::fs::remove_file(active_only_db_path);
        let _ = std::fs::remove_file(eligible_only_db_path);
        let _ = std::fs::remove_file(both_db_path);
        Ok(())
    }
