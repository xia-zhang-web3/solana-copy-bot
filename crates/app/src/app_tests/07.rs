    #[test]
    fn discovery_catch_up_scheduler_keeps_writer_queue_depth_as_hard_stop_even_with_pressure_override(
    ) {
        let mut discovery_output = discovery_output_for_catch_up_tests(true);
        discovery_output.persisted_stream_catch_up_pressure_override_requested = true;
        let mut writer_snapshot = maintenance_test_writer_snapshot();
        writer_snapshot.pending_requests =
            DISCOVERY_CATCH_UP_WRITER_PENDING_REQUESTS_THRESHOLD.saturating_add(1);
        writer_snapshot.aggregate_queue_depth_batches = 1;

        assert!(!should_schedule_discovery_catch_up(
            &discovery_output,
            false,
            &writer_snapshot,
            Some(&maintenance_test_ingestion_snapshot(0.0)),
        ));

        writer_snapshot.aggregate_queue_depth_batches = 0;
        writer_snapshot.journal_queue_depth_batches = 1;

        assert!(!should_schedule_discovery_catch_up(
            &discovery_output,
            false,
            &writer_snapshot,
            Some(&maintenance_test_ingestion_snapshot(0.0)),
        ));
    }

    #[test]
    fn discovery_catch_up_scheduler_skips_when_writer_crosses_threshold() {
        let discovery_output = discovery_output_for_catch_up_tests(true);
        let mut writer_snapshot = maintenance_test_writer_snapshot();
        writer_snapshot.pending_requests =
            DISCOVERY_CATCH_UP_WRITER_PENDING_REQUESTS_THRESHOLD.saturating_add(1);

        assert!(!should_schedule_discovery_catch_up(
            &discovery_output,
            false,
            &writer_snapshot,
            Some(&maintenance_test_ingestion_snapshot(0.0)),
        ));

        writer_snapshot.pending_requests = 0;
        writer_snapshot.aggregate_queue_depth_batches = 1;

        assert!(!should_schedule_discovery_catch_up(
            &discovery_output,
            false,
            &writer_snapshot,
            Some(&maintenance_test_ingestion_snapshot(0.0)),
        ));

        writer_snapshot.aggregate_queue_depth_batches = 0;
        writer_snapshot.journal_queue_depth_batches = 1;

        assert!(!should_schedule_discovery_catch_up(
            &discovery_output,
            false,
            &writer_snapshot,
            Some(&maintenance_test_ingestion_snapshot(0.0)),
        ));
    }

    #[test]
    fn sanitize_json_value_escapes_control_characters() {
        let raw = "line1\nline2\rline3\t\"\\\u{0000}";
        assert_eq!(
            sanitize_json_value(raw),
            "line1\\nline2\\rline3\\t\\\"\\\\\\u0000"
        );
    }

    #[test]
    fn validate_execution_risk_contract_rejects_invalid_shadow_caps() {
        let mut risk = RiskConfig::default();
        risk.shadow_soft_exposure_cap_sol = f64::NAN;
        let error = validate_execution_risk_contract(&risk)
            .expect_err("non-finite shadow soft cap must fail risk contract");
        assert!(
            error
                .to_string()
                .contains("risk.shadow_soft_exposure_cap_sol"),
            "unexpected error: {}",
            error
        );

        let mut risk = RiskConfig::default();
        risk.shadow_soft_exposure_resume_below_sol = 11.0;
        let error = validate_execution_risk_contract(&risk)
            .expect_err("soft exposure resume threshold above soft cap must fail");
        assert!(
            error
                .to_string()
                .contains("risk.shadow_soft_exposure_resume_below_sol"),
            "unexpected error: {}",
            error
        );
    }

    #[test]
    fn validate_execution_risk_contract_rejects_invalid_live_loss_limits() {
        let mut risk = RiskConfig::default();
        risk.daily_loss_limit_pct = -0.1;
        let error = validate_execution_risk_contract(&risk)
            .expect_err("negative daily loss limit percent must fail");
        assert!(
            error.to_string().contains("risk.daily_loss_limit_pct"),
            "unexpected error: {}",
            error
        );

        let mut risk = RiskConfig::default();
        risk.max_drawdown_pct = 150.0;
        let error = validate_execution_risk_contract(&risk)
            .expect_err("max drawdown percent above 100 must fail");
        assert!(
            error.to_string().contains("risk.max_drawdown_pct"),
            "unexpected error: {}",
            error
        );
    }

    #[test]
    fn validate_execution_risk_contract_rejects_invalid_shadow_drawdown_order() {
        let mut risk = RiskConfig::default();
        risk.shadow_drawdown_1h_stop_sol = -4.0;
        risk.shadow_drawdown_6h_stop_sol = -1.0;
        risk.shadow_drawdown_24h_stop_sol = -5.0;
        let error = validate_execution_risk_contract(&risk)
            .expect_err("invalid drawdown threshold order must fail");
        assert!(
            error.to_string().contains("drawdown ordering is invalid"),
            "unexpected error: {}",
            error
        );
    }

    #[test]
    fn validate_live_execution_policy_contract_rejects_enabled_execution_after_quarantine() {
        let mut execution = ExecutionConfig::default();
        execution.enabled = true;
        let mut risk = RiskConfig::default();
        risk.execution_buy_cooldown_seconds = 60;

        let error = validate_live_execution_policy_contract(&execution, &risk, "paper")
            .expect_err("copybot-app must reject enabled execution after quarantine");
        assert!(
            error
                .to_string()
                .contains("execution.enabled=true is quarantined in copybot-app"),
            "unexpected error: {}",
            error
        );
    }

    #[test]
    fn validate_live_execution_policy_contract_allows_disabled_execution() {
        let execution = ExecutionConfig::default();
        let risk = RiskConfig::default();

        validate_live_execution_policy_contract(&execution, &risk, "prod-live")
            .expect("disabled execution remains valid after quarantine");
    }

    fn infra_snapshot(
        ts_utc: DateTime<Utc>,
        ws_notifications_enqueued: u64,
        ws_notifications_replaced_oldest: u64,
    ) -> IngestionRuntimeSnapshot {
        IngestionRuntimeSnapshot {
            ts_utc,
            ws_notifications_enqueued,
            ws_notifications_replaced_oldest,
            grpc_message_total: 2_400_000 + ws_notifications_enqueued,
            grpc_transaction_updates_total: 24_000 + ws_notifications_enqueued,
            parse_rejected_total: 0,
            grpc_decode_errors: 0,
            rpc_429: 0,
            rpc_5xx: 0,
            ingestion_lag_ms_p95: 2_000,
            yellowstone_output_queue_depth: 0,
            yellowstone_output_queue_capacity: 0,
            yellowstone_output_queue_fill_ratio: 0.0,
            yellowstone_output_oldest_age_ms: 0,
        }
    }

    fn infra_snapshot_with_yellowstone_queue(
        ts_utc: DateTime<Utc>,
        ws_notifications_enqueued: u64,
        ws_notifications_replaced_oldest: u64,
        yellowstone_output_queue_depth: u64,
        yellowstone_output_queue_capacity: u64,
        yellowstone_output_oldest_age_ms: u64,
    ) -> IngestionRuntimeSnapshot {
        let mut snapshot = infra_snapshot(
            ts_utc,
            ws_notifications_enqueued,
            ws_notifications_replaced_oldest,
        );
        snapshot.yellowstone_output_queue_depth = yellowstone_output_queue_depth;
        snapshot.yellowstone_output_queue_capacity = yellowstone_output_queue_capacity;
        snapshot.yellowstone_output_queue_fill_ratio = if yellowstone_output_queue_capacity == 0 {
            0.0
        } else {
            yellowstone_output_queue_depth as f64 / yellowstone_output_queue_capacity as f64
        };
        snapshot.yellowstone_output_oldest_age_ms = yellowstone_output_oldest_age_ms;
        snapshot
    }

    #[test]
    fn risk_guard_infra_ratio_uses_window_delta_not_cumulative_with_consecutive_hysteresis(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("infra-ratio")?;
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
        assert!(
            guard.compute_infra_block_reason(now).is_none(),
            "rolling delta ratio (134/176 ~= 0.76) should not trigger threshold 0.80"
        );

        for offset in 1..RISK_INFRA_ACTIVATE_CONSECUTIVE_SAMPLES {
            guard.observe_ingestion_snapshot(
                &store,
                now + chrono::Duration::seconds(20 * offset as i64),
                Some(infra_snapshot(
                    now + chrono::Duration::seconds(20 * offset as i64),
                    16_500_300 + (offset as u64 * 40),
                    14_400_270 + (offset as u64 * 36),
                )),
            )?;
            assert!(
                guard.infra_block_reason.is_none(),
                "infra gate should wait for consecutive breaches before activating"
            );
        }
        guard.observe_ingestion_snapshot(
            &store,
            now + chrono::Duration::seconds(20 * RISK_INFRA_ACTIVATE_CONSECUTIVE_SAMPLES as i64),
            Some(infra_snapshot(
                now + chrono::Duration::seconds(
                    20 * RISK_INFRA_ACTIVATE_CONSECUTIVE_SAMPLES as i64,
                ),
                16_500_300 + (RISK_INFRA_ACTIVATE_CONSECUTIVE_SAMPLES as u64 * 40),
                14_400_270 + (RISK_INFRA_ACTIVATE_CONSECUTIVE_SAMPLES as u64 * 36),
            )),
        )?;
        assert!(
            guard.infra_block_reason.is_some(),
            "rolling delta ratio should activate after consecutive breaches"
        );

        for offset in 1..RISK_INFRA_CLEAR_HEALTHY_SAMPLES {
            guard.observe_ingestion_snapshot(
                &store,
                now + chrono::Duration::seconds(400 + 20 * offset as i64),
                Some(infra_snapshot(
                    now + chrono::Duration::seconds(400 + 20 * offset as i64),
                    16_502_000 + (offset as u64 * 200),
                    14_400_900 + (offset as u64 * 5),
                )),
            )?;
            assert!(
                guard.infra_block_reason.is_some(),
                "infra gate should require consecutive healthy samples before clearing"
            );
        }
        guard.observe_ingestion_snapshot(
            &store,
            now + chrono::Duration::seconds(400 + 20 * RISK_INFRA_CLEAR_HEALTHY_SAMPLES as i64),
            Some(infra_snapshot(
                now + chrono::Duration::seconds(400 + 20 * RISK_INFRA_CLEAR_HEALTHY_SAMPLES as i64),
                16_502_000 + (RISK_INFRA_CLEAR_HEALTHY_SAMPLES as u64 * 200),
                14_400_900 + (RISK_INFRA_CLEAR_HEALTHY_SAMPLES as u64 * 5),
            )),
        )?;
        assert!(
            guard.infra_block_reason.is_none(),
            "infra gate should clear after consecutive healthy samples"
        );

        let infra_stops = store.list_risk_events_by_type_desc("shadow_risk_infra_stop")?;
        let infra_clears = store.list_risk_events_by_type_desc("shadow_risk_infra_cleared")?;
        assert_eq!(
            infra_stops.len(),
            1,
            "expected one infra stop activation event"
        );
        assert_eq!(infra_clears.len(), 1, "expected one infra clear event");

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_infra_replaced_ratio_does_not_gate_yellowstone_source() -> Result<()> {
        let (store, db_path) = make_test_store("infra-ratio-yellowstone")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_infra_replaced_ratio_threshold = 0.80;
        cfg.shadow_infra_window_minutes = 20;
        let mut guard = ShadowRiskGuard::new_with_ingestion_source(cfg, "yellowstone_grpc");
        let now = Utc::now();
        guard.infra_samples = VecDeque::from([
            infra_snapshot(now - chrono::Duration::minutes(30), 10_000, 9_000),
            infra_snapshot(now - chrono::Duration::minutes(10), 16_500_000, 14_400_000),
            infra_snapshot(now, 16_500_300, 14_400_270),
        ]);
        assert!(
            guard.compute_infra_block_reason(now).is_none(),
            "yellowstone replaced_ratio should remain telemetry-only and not gate buys"
        );

        for offset in 1..=RISK_INFRA_ACTIVATE_CONSECUTIVE_SAMPLES {
            guard.observe_ingestion_snapshot(
                &store,
                now + chrono::Duration::seconds(20 * offset as i64),
                Some(infra_snapshot(
                    now + chrono::Duration::seconds(20 * offset as i64),
                    16_500_300 + (offset as u64 * 40),
                    14_400_270 + (offset as u64 * 36),
                )),
            )?;
        }
        assert!(
            guard.infra_block_reason.is_none(),
            "yellowstone replaced_ratio should not activate infra gate even after sustained breaches"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_observe_ingestion_snapshot_returns_error_on_fatal_infra_stop_risk_event_write(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("infra-stop-risk-event-fatal")?;
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

        for offset in 1..RISK_INFRA_ACTIVATE_CONSECUTIVE_SAMPLES {
            guard.observe_ingestion_snapshot(
                &store,
                now + chrono::Duration::seconds(20 * offset as i64),
                Some(infra_snapshot(
                    now + chrono::Duration::seconds(20 * offset as i64),
                    16_500_300 + (offset as u64 * 40),
                    14_400_270 + (offset as u64 * 36),
                )),
            )?;
        }
        assert!(guard.infra_block_key.is_none());

        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute_batch(
            "CREATE TRIGGER fail_shadow_risk_infra_stop_event_insert
             BEFORE INSERT ON risk_events
             WHEN NEW.type = 'shadow_risk_infra_stop'
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let error = guard
            .observe_ingestion_snapshot(
                &store,
                now + chrono::Duration::seconds(
                    20 * RISK_INFRA_ACTIVATE_CONSECUTIVE_SAMPLES as i64,
                ),
                Some(infra_snapshot(
                    now + chrono::Duration::seconds(
                        20 * RISK_INFRA_ACTIVATE_CONSECUTIVE_SAMPLES as i64,
                    ),
                    16_500_300 + (RISK_INFRA_ACTIVATE_CONSECUTIVE_SAMPLES as u64 * 40),
                    14_400_270 + (RISK_INFRA_ACTIVATE_CONSECUTIVE_SAMPLES as u64 * 36),
                )),
            )
            .expect_err(
                "fatal infra-stop risk event write must abort ingestion-snapshot observation",
            );
        let error_text = format!("{error:#}");
        assert!(
            error_text
                .contains("failed to persist shadow risk infra stop event with fatal sqlite I/O"),
            "expected infra-stop fatal context, got: {error_text}"
        );
        assert!(
            error_text.contains("failed to insert risk event"),
            "expected storage insert_risk_event context, got: {error_text}"
        );
        assert!(
            error_text.contains("xShmMap"),
            "expected fatal sqlite marker to survive error chain, got: {error_text}"
        );
        assert!(
            guard.infra_block_key.is_none(),
            "fatal infra-stop write must not flip runtime infra block state"
        );
        assert_eq!(store.risk_event_count_by_type("shadow_risk_infra_stop")?, 0);

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_observe_ingestion_snapshot_returns_error_on_fatal_infra_clear_risk_event_write(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("infra-clear-risk-event-fatal")?;
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
                    16_500_300 + (offset as u64 * 40),
                    14_400_270 + (offset as u64 * 36),
                )),
            )?;
        }
        assert!(guard.infra_block_key.is_some());
        assert_eq!(store.risk_event_count_by_type("shadow_risk_infra_stop")?, 1);

        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute_batch(
            "CREATE TRIGGER fail_shadow_risk_infra_clear_event_insert
             BEFORE INSERT ON risk_events
             WHEN NEW.type = 'shadow_risk_infra_cleared'
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        for offset in 1..RISK_INFRA_CLEAR_HEALTHY_SAMPLES {
            guard.observe_ingestion_snapshot(
                &store,
                now + chrono::Duration::seconds(400 + 20 * offset as i64),
                Some(infra_snapshot(
                    now + chrono::Duration::seconds(400 + 20 * offset as i64),
                    16_502_000 + (offset as u64 * 200),
                    14_400_900 + (offset as u64 * 5),
                )),
            )?;
        }
        let error = guard
            .observe_ingestion_snapshot(
                &store,
                now + chrono::Duration::seconds(400 + 20 * RISK_INFRA_CLEAR_HEALTHY_SAMPLES as i64),
                Some(infra_snapshot(
                    now + chrono::Duration::seconds(
                        400 + 20 * RISK_INFRA_CLEAR_HEALTHY_SAMPLES as i64,
                    ),
                    16_502_000 + (RISK_INFRA_CLEAR_HEALTHY_SAMPLES as u64 * 200),
                    14_400_900 + (RISK_INFRA_CLEAR_HEALTHY_SAMPLES as u64 * 5),
                )),
            )
            .expect_err(
                "fatal infra-clear risk event write must abort ingestion-snapshot observation",
            );
        let error_text = format!("{error:#}");
        assert!(
            error_text
                .contains("failed to persist shadow risk infra clear event with fatal sqlite I/O"),
            "expected infra-clear fatal context, got: {error_text}"
        );
        assert!(
            error_text.contains("failed to insert risk event"),
            "expected storage insert_risk_event context, got: {error_text}"
        );
        assert!(
            error_text.contains("xShmMap"),
            "expected fatal sqlite marker to survive error chain, got: {error_text}"
        );
        assert!(
            guard.infra_block_key.is_some(),
            "fatal infra-clear write must preserve runtime infra block state"
        );
        assert_eq!(
            store.risk_event_count_by_type("shadow_risk_infra_cleared")?,
            0
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }
