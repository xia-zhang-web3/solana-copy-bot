    #[test]
    fn risk_guard_infra_stop_does_not_reemit_when_same_key_detail_changes() -> Result<()> {
        let (store, db_path) = make_test_store("infra-ratio-reemit")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_infra_replaced_ratio_threshold = 0.80;
        cfg.shadow_infra_window_minutes = 20;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();
        guard.infra_samples = VecDeque::from([
            infra_snapshot(now - chrono::Duration::minutes(30), 10_000, 9_000),
            infra_snapshot(now - chrono::Duration::minutes(10), 16_500_000, 14_400_000),
            infra_snapshot(now, 16_500_176, 14_400_134),
        ]);

        for offset in 1..=RISK_INFRA_ACTIVATE_CONSECUTIVE_SAMPLES {
            guard.observe_ingestion_snapshot(
                &store,
                now + chrono::Duration::seconds(20 * offset as i64),
                Some(infra_snapshot(
                    now + chrono::Duration::seconds(20 * offset as i64),
                    16_500_300 + (offset as u64 * 60),
                    14_400_270 + (offset as u64 * 54),
                )),
            )?;
        }
        let first_reason = guard
            .infra_block_reason
            .clone()
            .expect("infra gate should activate after sustained replaced_ratio breaches");

        guard.observe_ingestion_snapshot(
            &store,
            now + chrono::Duration::seconds(140),
            Some(infra_snapshot(
                now + chrono::Duration::seconds(140),
                16_501_000,
                14_400_950,
            )),
        )?;
        let updated_reason = guard
            .infra_block_reason
            .clone()
            .expect("infra gate should remain active for same-key follow-up breach");
        assert_ne!(
            first_reason, updated_reason,
            "reason detail should still refresh even when gating identity stays stable"
        );
        let infra_stops = store.list_risk_events_by_type_desc("shadow_risk_infra_stop")?;
        assert_eq!(
            infra_stops.len(),
            1,
            "dynamic replaced_ratio details must not create duplicate infra stop events"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_infra_blocks_when_no_ingestion_progress_for_complete_window() {
        let mut cfg = RiskConfig::default();
        cfg.shadow_infra_window_minutes = 20;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();
        guard.infra_samples = VecDeque::from([
            IngestionRuntimeSnapshot {
                ts_utc: now - chrono::Duration::minutes(21),
                ws_notifications_enqueued: 10_000,
                ws_notifications_replaced_oldest: 0,
                grpc_message_total: 900_000,
                grpc_transaction_updates_total: 900_000,
                parse_rejected_total: 0,
                grpc_decode_errors: 0,
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
                grpc_message_total: 900_000,
                grpc_transaction_updates_total: 900_000,
                parse_rejected_total: 0,
                grpc_decode_errors: 0,
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
                grpc_message_total: 900_000,
                grpc_transaction_updates_total: 900_000,
                parse_rejected_total: 0,
                grpc_decode_errors: 0,
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
            .expect("no progress over full infra window must trigger block");
        assert!(
            reason.contains("no_ingestion_progress_for=20m"),
            "unexpected reason: {}",
            reason
        );
    }

    #[test]
    fn risk_guard_infra_no_progress_reason_includes_yellowstone_queue_context() {
        let mut cfg = RiskConfig::default();
        cfg.shadow_infra_window_minutes = 20;
        let mut guard = ShadowRiskGuard::new_with_ingestion_source(cfg, "yellowstone_grpc");
        let now = Utc::now();
        guard.infra_samples = VecDeque::from([
            infra_snapshot_with_yellowstone_queue(
                now - chrono::Duration::minutes(21),
                10_000,
                0,
                7,
                10,
                120,
            ),
            infra_snapshot_with_yellowstone_queue(
                now - chrono::Duration::minutes(10),
                10_000,
                0,
                8,
                10,
                240,
            ),
            infra_snapshot_with_yellowstone_queue(now, 10_000, 0, 9, 10, 360),
        ]);

        let reason = guard
            .compute_infra_block_reason(now)
            .expect("yellowstone no-progress gate should still trigger");
        assert!(
            reason.contains("no_ingestion_progress_for=20m"),
            "unexpected reason: {}",
            reason
        );
        assert!(
            reason.contains("yellowstone_output_queue_depth=9"),
            "expected queue depth context, got: {}",
            reason
        );
        assert!(
            reason.contains("yellowstone_output_queue_capacity=10"),
            "expected queue capacity context, got: {}",
            reason
        );
        assert!(
            reason.contains("yellowstone_output_queue_fill_ratio=0.9000"),
            "expected queue fill ratio context, got: {}",
            reason
        );
        assert!(
            reason.contains("yellowstone_output_oldest_age_ms=360"),
            "expected queue oldest-age context, got: {}",
            reason
        );
    }

    #[test]
    fn risk_guard_infra_no_progress_reason_omits_yellowstone_queue_context_for_non_yellowstone() {
        let mut cfg = RiskConfig::default();
        cfg.shadow_infra_window_minutes = 20;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();
        guard.infra_samples = VecDeque::from([
            infra_snapshot_with_yellowstone_queue(
                now - chrono::Duration::minutes(21),
                10_000,
                0,
                7,
                10,
                120,
            ),
            infra_snapshot_with_yellowstone_queue(
                now - chrono::Duration::minutes(10),
                10_000,
                0,
                8,
                10,
                240,
            ),
            infra_snapshot_with_yellowstone_queue(now, 10_000, 0, 9, 10, 360),
        ]);

        let reason = guard
            .compute_infra_block_reason(now)
            .expect("mock-source no-progress gate should still trigger");
        assert!(
            reason.contains("no_ingestion_progress_for=20m"),
            "unexpected reason: {}",
            reason
        );
        assert!(
            !reason.contains("yellowstone_output_queue_depth="),
            "non-yellowstone source must not append yellowstone queue context: {}",
            reason
        );
    }
