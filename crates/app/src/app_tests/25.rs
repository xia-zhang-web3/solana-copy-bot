    #[test]
    fn parse_app_log_env_filter_rejects_invalid_syntax() {
        with_app_log_filter_env(Some(OsString::from("[")), None, || {
            assert_eq!(
                env::var(APP_LOG_FILTER_ENV).as_deref(),
                Ok("["),
                "test helper failed to set app log filter env"
            );
            let error = parse_app_log_env_filter("info").expect_err("invalid filter must reject");
            assert!(
                error.to_string().contains(APP_LOG_FILTER_ENV),
                "unexpected error: {}",
                error
            );
        });
    }

    #[cfg(unix)]
    #[test]
    fn parse_app_log_env_filter_rejects_non_utf8() {
        use std::os::unix::ffi::OsStringExt;

        with_app_log_filter_env(Some(OsString::from_vec(vec![0xff])), None, || {
            let error = parse_app_log_env_filter("info").expect_err("non-UTF8 filter must reject");
            assert!(
                error.to_string().contains(APP_LOG_FILTER_ENV),
                "unexpected error: {}",
                error
            );
            assert!(
                error.to_string().contains("UTF-8"),
                "unexpected error: {}",
                error
            );
        });
    }

    #[test]
    fn parse_app_log_env_filter_ignores_rust_log() {
        with_app_log_filter_env(None, Some(OsString::from("error")), || {
            parse_app_log_env_filter("info")
                .expect("ambient RUST_LOG should not affect app log filter");
        });
    }

    #[test]
    fn ingestion_error_backoff_ms_uses_progressive_schedule() {
        assert_eq!(ingestion_error_backoff_ms(0), 100);
        assert_eq!(ingestion_error_backoff_ms(1), 100);
        assert_eq!(ingestion_error_backoff_ms(2), 250);
        assert_eq!(ingestion_error_backoff_ms(3), 500);
        assert_eq!(ingestion_error_backoff_ms(4), 1_000);
        assert_eq!(ingestion_error_backoff_ms(5), 2_000);
        assert_eq!(ingestion_error_backoff_ms(6), 5_000);
    }

    #[test]
    fn ingestion_error_backoff_ms_saturates_at_max_step() {
        assert_eq!(ingestion_error_backoff_ms(7), 5_000);
        assert_eq!(ingestion_error_backoff_ms(100), 5_000);
        assert_eq!(ingestion_error_backoff_ms(u32::MAX), 5_000);
    }

    #[test]
    fn apply_ingestion_source_override_has_priority_over_existing_source() {
        let mut source = "mock".to_string();
        let applied =
            apply_ingestion_source_override(&mut source, Some("yellowstone_grpc".to_string()));
        assert_eq!(applied.as_deref(), Some("yellowstone_grpc"));
        assert_eq!(source, "yellowstone_grpc");
    }

    #[test]
    fn parse_operator_emergency_stop_reason_ignores_comments() {
        let content = r#"
# stop trading immediately

manual override by operator
"#;
        assert_eq!(
            parse_operator_emergency_stop_reason(content).as_deref(),
            Some("manual override by operator")
        );
    }

    #[test]
    fn operator_emergency_stop_refresh_returns_error_on_fatal_activation_risk_event_write(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("operator-stop-risk-event-fatal")?;
        let now = DateTime::parse_from_rfc3339("2026-03-14T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let stop_path = std::env::temp_dir().join(format!(
            "copybot-operator-stop-{}-{}.flag",
            std::process::id(),
            now.timestamp_nanos_opt()
                .unwrap_or(now.timestamp_micros() * 1000)
        ));
        std::fs::write(&stop_path, "manual override by operator\n")?;

        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute_batch(
            "CREATE TRIGGER fail_operator_emergency_stop_risk_event_insert
             BEFORE INSERT ON risk_events
             WHEN NEW.type = 'operator_emergency_stop_activated'
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let mut stop = OperatorEmergencyStop {
            path: stop_path.clone(),
            poll_interval: Duration::from_millis(100),
            next_refresh_at: StdInstant::now()
                .checked_sub(Duration::from_secs(1))
                .unwrap_or_else(StdInstant::now),
            active: false,
            detail: String::new(),
        };
        let error = stop
            .refresh(&store, now)
            .expect_err("fatal operator stop risk-event write must abort refresh");
        let error_text = format!("{error:#}");
        assert!(
            error_text.contains(
                "failed to persist operator emergency stop activation event with fatal sqlite I/O"
            ),
            "expected operator-stop fatal context, got: {error_text}"
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
            stop.is_active(),
            "activation state should already be fail-closed"
        );
        assert_eq!(
            store.risk_event_count_by_type("operator_emergency_stop_activated")?,
            0
        );

        let _ = std::fs::remove_file(stop_path);
        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn operator_emergency_stop_refresh_returns_error_on_fatal_clear_risk_event_write() -> Result<()>
    {
        let (store, db_path) = make_test_store("operator-stop-clear-risk-event-fatal")?;
        let now = DateTime::parse_from_rfc3339("2026-03-14T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let stop_path = std::env::temp_dir().join(format!(
            "copybot-operator-stop-clear-{}-{}.flag",
            std::process::id(),
            now.timestamp_nanos_opt()
                .unwrap_or(now.timestamp_micros() * 1000)
        ));
        let _ = std::fs::remove_file(&stop_path);

        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute_batch(
            "CREATE TRIGGER fail_operator_emergency_stop_clear_risk_event_insert
             BEFORE INSERT ON risk_events
             WHEN NEW.type = 'operator_emergency_stop_cleared'
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let mut stop = OperatorEmergencyStop {
            path: stop_path.clone(),
            poll_interval: Duration::from_millis(100),
            next_refresh_at: StdInstant::now()
                .checked_sub(Duration::from_secs(1))
                .unwrap_or_else(StdInstant::now),
            active: true,
            detail: "manual override by operator".to_string(),
        };
        let error = stop
            .refresh(&store, now)
            .expect_err("fatal operator stop clear risk-event write must abort refresh");
        let error_text = format!("{error:#}");
        assert!(
            error_text.contains(
                "failed to persist operator emergency stop clear event with fatal sqlite I/O"
            ),
            "expected operator-stop clear fatal context, got: {error_text}"
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
            stop.is_active(),
            "fatal operator stop clear write must preserve active fail-closed state"
        );
        assert_eq!(stop.detail(), "manual override by operator");
        assert_eq!(
            store.risk_event_count_by_type("operator_emergency_stop_cleared")?,
            0
        );

        let _ = std::fs::remove_file(stop_path);
        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn record_shadow_queue_backpressure_risk_event_returns_error_on_fatal_write() -> Result<()> {
        let (store, db_path) = make_test_store("shadow-queue-risk-event-fatal")?;
        let now = DateTime::parse_from_rfc3339("2026-03-14T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute_batch(
            "CREATE TRIGGER fail_shadow_queue_risk_event_insert
             BEFORE INSERT ON risk_events
             WHEN NEW.type = 'shadow_queue_saturated'
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let error = record_shadow_queue_backpressure_risk_event(&store, 7, 2, 256, 256, now)
            .expect_err("fatal queue-backpressure risk-event write must abort");
        let error_text = format!("{error:#}");
        assert!(
            error_text.contains(
                "failed to persist shadow queue backpressure risk event with fatal sqlite I/O"
            ),
            "expected queue-backpressure fatal context, got: {error_text}"
        );
        assert!(
            error_text.contains("failed to insert risk event"),
            "expected storage insert_risk_event context, got: {error_text}"
        );
        assert!(
            error_text.contains("xShmMap"),
            "expected fatal sqlite marker to survive error chain, got: {error_text}"
        );
        assert_eq!(store.risk_event_count_by_type("shadow_queue_saturated")?, 0);

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_infra_block_respects_pause_new_trades_on_outage_flag() -> Result<()> {
        let (store, db_path) = make_test_store("infra-outage-flag")?;
        let mut guard = ShadowRiskGuard::new(RiskConfig::default());
        guard.infra_block_reason = Some("ingestion_degraded".to_string());
        let now = Utc::now();

        match guard.can_open_buy(&store, now, true) {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::Infra,
                detail,
            } => assert!(detail.contains("ingestion_degraded")),
            other => panic!("expected infra block when outage pausing is enabled, got {other:?}"),
        }
        match guard.can_open_buy(&store, now, false) {
            BuyRiskDecision::Allow => {}
            other => panic!(
                "expected outage infra block bypass when pause_new_trades_on_outage=false, got {other:?}"
            ),
        }

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn causal_holdback_holds_sell_without_open_lot_or_pending_key() {
        let key = ShadowTaskKey {
            wallet: "wallet-a".to_string(),
            token: "token-x".to_string(),
        };
        let shadow_scheduler = ShadowScheduler::new();
        let open_lots: HashSet<(String, String)> = HashSet::new();

        assert!(shadow_scheduler.should_hold_sell_for_causality(
            true,
            2500,
            ShadowSwapSide::Sell,
            &key,
            &open_lots,
        ));
    }

    #[test]
    fn causal_holdback_skips_when_open_lot_or_pending_exists() {
        let key = ShadowTaskKey {
            wallet: "wallet-a".to_string(),
            token: "token-x".to_string(),
        };
        let pending_task = ShadowTaskInput {
            swap: SwapEvent {
                wallet: "wallet-a".to_string(),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: "token-x".to_string(),
                amount_in: 1.0,
                amount_out: 1000.0,
                signature: "sig-pending".to_string(),
                slot: 1,
                ts_utc: Utc::now(),
                exact_amounts: None,
            },
            follow_snapshot: Arc::new(FollowSnapshot::default()),
            key: key.clone(),
        };
        let mut shadow_scheduler = ShadowScheduler::new();
        shadow_scheduler
            .pending_shadow_tasks
            .insert(key.clone(), VecDeque::from([pending_task]));
        let open_lots: HashSet<(String, String)> = HashSet::new();

        assert!(!shadow_scheduler.should_hold_sell_for_causality(
            true,
            2500,
            ShadowSwapSide::Sell,
            &key,
            &open_lots,
        ));

        shadow_scheduler.pending_shadow_tasks.clear();
        let open_lots_yes = HashSet::from([(key.wallet.clone(), key.token.clone())]);
        assert!(!shadow_scheduler.should_hold_sell_for_causality(
            true,
            2500,
            ShadowSwapSide::Sell,
            &key,
            &open_lots_yes,
        ));
    }

    fn with_app_log_filter_env<T>(
        app_value: Option<OsString>,
        rust_log_value: Option<OsString>,
        run: impl FnOnce() -> T,
    ) -> T {
        let _guard = APP_ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let saved_app = env::var_os(APP_LOG_FILTER_ENV);
        let saved_rust_log = env::var_os(LEGACY_RUST_LOG_ENV);
        env::remove_var(APP_LOG_FILTER_ENV);
        env::remove_var(LEGACY_RUST_LOG_ENV);
        if let Some(value) = app_value {
            env::set_var(APP_LOG_FILTER_ENV, value);
        }
        if let Some(value) = rust_log_value {
            env::set_var(LEGACY_RUST_LOG_ENV, value);
        }
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(run));
        env::remove_var(APP_LOG_FILTER_ENV);
        env::remove_var(LEGACY_RUST_LOG_ENV);
        if let Some(value) = saved_app {
            env::set_var(APP_LOG_FILTER_ENV, value);
        }
        if let Some(value) = saved_rust_log {
            env::set_var(LEGACY_RUST_LOG_ENV, value);
        }
        match outcome {
            Ok(value) => value,
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }
