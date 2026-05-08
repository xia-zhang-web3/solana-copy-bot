#[test]
    fn risk_guard_observe_ingestion_snapshot_persists_yellowstone_queue_context_in_infra_stop_details(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("infra-stop-yellowstone-details")?;
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

        for offset in 1..=RISK_INFRA_ACTIVATE_CONSECUTIVE_SAMPLES {
            guard.observe_ingestion_snapshot(
                &store,
                now + chrono::Duration::seconds(20 * offset as i64),
                Some(infra_snapshot_with_yellowstone_queue(
                    now + chrono::Duration::seconds(20 * offset as i64),
                    10_000,
                    0,
                    9,
                    10,
                    360,
                )),
            )?;
        }

        let infra_stops = store.list_risk_events_by_type_desc("shadow_risk_infra_stop")?;
        assert_eq!(infra_stops.len(), 1, "expected one infra stop event");
        let details_json = infra_stops[0]
            .details_json
            .as_deref()
            .expect("infra stop event must include details_json");
        let details: serde_json::Value = serde_json::from_str(details_json)
            .context("failed to parse infra stop details_json")?;
        let reason = details
            .get("reason")
            .and_then(serde_json::Value::as_str)
            .expect("infra stop details must include reason");
        assert!(
            reason.contains("no_ingestion_progress_for=20m"),
            "unexpected reason: {reason}"
        );
        assert_eq!(
            details
                .get("yellowstone_output_queue_depth")
                .and_then(serde_json::Value::as_u64),
            Some(9)
        );
        assert_eq!(
            details
                .get("yellowstone_output_queue_capacity")
                .and_then(serde_json::Value::as_u64),
            Some(10)
        );
        assert_eq!(
            details
                .get("yellowstone_output_oldest_age_ms")
                .and_then(serde_json::Value::as_u64),
            Some(360)
        );
        let fill_ratio = details
            .get("yellowstone_output_queue_fill_ratio")
            .and_then(serde_json::Value::as_f64)
            .expect("yellowstone details must include fill ratio");
        assert!(
            (fill_ratio - 0.9).abs() < f64::EPSILON,
            "unexpected fill ratio: {fill_ratio}"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_observe_ingestion_snapshot_omits_yellowstone_queue_context_in_infra_stop_details_for_non_yellowstone(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("infra-stop-non-yellowstone-details")?;
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

        for offset in 1..=RISK_INFRA_ACTIVATE_CONSECUTIVE_SAMPLES {
            guard.observe_ingestion_snapshot(
                &store,
                now + chrono::Duration::seconds(20 * offset as i64),
                Some(infra_snapshot_with_yellowstone_queue(
                    now + chrono::Duration::seconds(20 * offset as i64),
                    10_000,
                    0,
                    9,
                    10,
                    360,
                )),
            )?;
        }

        let infra_stops = store.list_risk_events_by_type_desc("shadow_risk_infra_stop")?;
        assert_eq!(infra_stops.len(), 1, "expected one infra stop event");
        let details_json = infra_stops[0]
            .details_json
            .as_deref()
            .expect("infra stop event must include details_json");
        let details: serde_json::Value = serde_json::from_str(details_json)
            .context("failed to parse infra stop details_json")?;
        assert!(
            details.get("reason").is_some(),
            "infra stop details must still include reason"
        );
        assert!(
            details.get("yellowstone_output_queue_depth").is_none(),
            "non-yellowstone infra stop details must omit yellowstone queue fields: {details_json}"
        );
        assert!(
            details.get("yellowstone_output_queue_capacity").is_none(),
            "non-yellowstone infra stop details must omit yellowstone queue fields: {details_json}"
        );
        assert!(
            details.get("yellowstone_output_queue_fill_ratio").is_none(),
            "non-yellowstone infra stop details must omit yellowstone queue fields: {details_json}"
        );
        assert!(
            details.get("yellowstone_output_oldest_age_ms").is_none(),
            "non-yellowstone infra stop details must omit yellowstone queue fields: {details_json}"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_infra_no_progress_does_not_block_when_grpc_transaction_updates_advance() {
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
                grpc_message_total: 910_000, // could include pings
                grpc_transaction_updates_total: 910_000,
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
                grpc_message_total: 920_000, // could include pings
                grpc_transaction_updates_total: 920_000,
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

        assert!(
            guard.compute_infra_block_reason(now).is_none(),
            "no-ingestion-progress gate must not trigger while grpc_transaction_updates_total is advancing"
        );
    }

    #[test]
    fn risk_guard_infra_no_progress_still_blocks_when_only_grpc_ping_total_advances() {
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
                grpc_message_total: 910_000,
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
                grpc_message_total: 920_000,
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

        let reason = guard.compute_infra_block_reason(now).expect(
            "no-ingestion-progress gate must trigger when only ping-level grpc traffic advances",
        );
        assert!(
            reason.contains("no_ingestion_progress_for=20m"),
            "unexpected reason: {}",
            reason
        );
    }
