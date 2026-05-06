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
