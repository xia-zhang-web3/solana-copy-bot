    #[test]
    fn risk_guard_refresh_returns_error_on_fatal_drawdown_timed_pause_risk_event_write(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("drawdown-timed-pause-risk-event-fatal")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_drawdown_1h_stop_sol = -0.3;
        cfg.shadow_drawdown_1h_pause_minutes = 30;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_24h_stop_sol = -999.0;
        cfg.shadow_rug_loss_count_threshold = u64::MAX;
        cfg.shadow_rug_loss_rate_threshold = 1.0;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();
        store.insert_shadow_closed_trade(
            "sig-dd1-fatal",
            "wallet-a",
            "token-a",
            1.0,
            1.0,
            0.5,
            -0.5,
            now - chrono::Duration::minutes(10),
            now - chrono::Duration::minutes(5),
        )?;

        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute_batch(
            "CREATE TRIGGER fail_shadow_risk_timed_pause_event_insert
             BEFORE INSERT ON risk_events
             WHEN NEW.type = 'shadow_risk_pause'
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let error = guard
            .maybe_refresh_db_state(&store, now)
            .expect_err("fatal timed-pause risk event write must abort refresh");
        let error_text = format!("{error:#}");
        assert!(
            error_text
                .contains("failed to persist shadow risk timed pause event with fatal sqlite I/O"),
            "expected timed-pause fatal context, got: {error_text}"
        );
        assert!(
            error_text.contains("failed to insert risk event"),
            "expected storage insert_risk_event context, got: {error_text}"
        );
        assert!(
            error_text.contains("xShmMap"),
            "expected fatal sqlite marker to survive error chain, got: {error_text}"
        );
        assert!(guard.pause_until.is_some());
        assert_eq!(store.risk_event_count_by_type("shadow_risk_pause")?, 0);

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_refresh_returns_error_on_fatal_soft_exposure_pause_clear_risk_event_write(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("soft-pause-clear-risk-event-fatal")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_soft_exposure_cap_sol = 0.5;
        cfg.shadow_soft_exposure_resume_below_sol = 0.4;
        cfg.shadow_hard_exposure_cap_sol = 2.0;
        cfg.shadow_soft_pause_minutes = 1;
        cfg.shadow_drawdown_1h_stop_sol = -999.0;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_24h_stop_sol = -999.0;
        cfg.shadow_rug_loss_count_threshold = u64::MAX;
        cfg.shadow_rug_loss_rate_threshold = 1.0;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();
        let opened_ts = now - chrono::Duration::minutes(5);
        let lot_id = store.insert_shadow_lot("wallet-a", "token-a", 10.0, 0.6, opened_ts)?;

        let _ = guard.can_open_buy(&store, now, true);
        let initial_until = guard
            .soft_exposure_pause_until
            .expect("soft exposure pause should set until");
        let initial_reason = guard
            .soft_exposure_pause_reason
            .clone()
            .expect("soft exposure pause should set reason");
        assert!(guard.soft_exposure_pause_latched);
        assert_eq!(store.risk_event_count_by_type("shadow_risk_pause")?, 1);

        store.update_shadow_lot(lot_id, 10.0, 0.35)?;

        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute_batch(
            "CREATE TRIGGER fail_shadow_risk_soft_pause_clear_event_insert
             BEFORE INSERT ON risk_events
             WHEN NEW.type = 'shadow_risk_pause_cleared'
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let recovered_at =
            now + chrono::Duration::seconds((RISK_DB_REFRESH_MIN_SECONDS + 61).max(61));
        let error = guard
            .maybe_refresh_db_state(&store, recovered_at)
            .expect_err("fatal soft-exposure pause clear write must abort refresh");
        let error_text = format!("{error:#}");
        assert!(
            error_text.contains(
                "failed to persist shadow risk soft exposure pause clear event with fatal sqlite I/O"
            ),
            "expected soft-pause clear fatal context, got: {error_text}"
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
            guard.soft_exposure_pause_latched,
            "fatal soft-exposure pause clear write must preserve runtime soft pause latch"
        );
        assert_eq!(guard.soft_exposure_pause_until, Some(initial_until));
        assert_eq!(
            guard.soft_exposure_pause_reason.as_deref(),
            Some(initial_reason.as_str())
        );
        assert_eq!(
            store.risk_event_count_by_type("shadow_risk_pause_cleared")?,
            0
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_expired_timed_pause_blocks_buy_on_fatal_clear_risk_event_write() -> Result<()> {
        let (store, db_path) = make_test_store("timed-pause-clear-risk-event-fatal")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_drawdown_1h_stop_sol = -999.0;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_24h_stop_sol = -999.0;
        cfg.shadow_rug_loss_count_threshold = u64::MAX;
        cfg.shadow_rug_loss_rate_threshold = 1.0;
        let pause_started_at = Utc::now() - chrono::Duration::minutes(10);
        let mut guard = ShadowRiskGuard::new(cfg);
        guard.activate_pause(
            &store,
            pause_started_at,
            chrono::Duration::minutes(5),
            "drawdown_1h",
            "expired pause should fail closed on fatal clear write".to_string(),
        )?;
        assert!(guard.pause_until.is_some());
        assert_eq!(store.risk_event_count_by_type("shadow_risk_pause")?, 1);

        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute_batch(
            "CREATE TRIGGER fail_shadow_risk_timed_pause_clear_event_insert
             BEFORE INSERT ON risk_events
             WHEN NEW.type = 'shadow_risk_pause_cleared'
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let decision = guard.can_open_buy(&store, Utc::now(), true);
        match decision {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::FailClosed,
                detail,
            } => assert!(detail.contains("timed_pause_clear_error")),
            other => panic!(
                "expected fail-closed block when fatal timed-pause clear write fails, got {other:?}"
            ),
        }
        assert!(
            guard.pause_until.is_some(),
            "fatal timed-pause clear write must preserve runtime pause state"
        );
        assert_eq!(
            store.risk_event_count_by_type("shadow_risk_pause_cleared")?,
            0
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_cached_refresh_error_blocks_buy_on_fatal_fail_closed_risk_event_write(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("cached-refresh-fail-closed-risk-event-fatal")?;
        let mut guard = ShadowRiskGuard::new(RiskConfig::default());
        let now = Utc::now();
        guard.last_db_refresh_at = Some(now);
        guard.last_db_refresh_error = Some("simulated refresh failure".to_string());

        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute_batch(
            "CREATE TRIGGER fail_shadow_risk_fail_closed_event_insert
             BEFORE INSERT ON risk_events
             WHEN NEW.type = 'shadow_risk_fail_closed'
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let decision = guard.can_open_buy(&store, now + chrono::Duration::seconds(1), true);
        match decision {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::FailClosed,
                detail,
            } => {
                assert!(detail.contains("cached error"));
                assert!(detail.contains("fail_closed_event_error"));
                assert!(detail.contains(
                    "failed to persist shadow risk fail-closed event with fatal sqlite I/O"
                ));
                assert!(detail.contains("xShmMap"));
            }
            other => panic!(
                "expected fail-closed block when fatal fail-closed event write fails, got {other:?}"
            ),
        }
        assert_eq!(
            store.risk_event_count_by_type("shadow_risk_fail_closed")?,
            0
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_timed_pause_clear_blocks_buy_on_fatal_fail_closed_risk_event_write() -> Result<()>
    {
        let (store, db_path) = make_test_store("timed-pause-clear-fail-closed-risk-event-fatal")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_drawdown_1h_stop_sol = -999.0;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_24h_stop_sol = -999.0;
        cfg.shadow_rug_loss_count_threshold = u64::MAX;
        cfg.shadow_rug_loss_rate_threshold = 1.0;
        let pause_started_at = Utc::now() - chrono::Duration::minutes(10);
        let mut guard = ShadowRiskGuard::new(cfg);
        guard.activate_pause(
            &store,
            pause_started_at,
            chrono::Duration::minutes(5),
            "drawdown_1h",
            "expired pause should fail closed on fatal fail-closed write".to_string(),
        )?;

        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute_batch(
            "CREATE TRIGGER fail_shadow_risk_timed_pause_clear_event_insert
             BEFORE INSERT ON risk_events
             WHEN NEW.type = 'shadow_risk_pause_cleared'
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;
             CREATE TRIGGER fail_shadow_risk_fail_closed_event_insert
             BEFORE INSERT ON risk_events
             WHEN NEW.type = 'shadow_risk_fail_closed'
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let decision = guard.can_open_buy(&store, Utc::now(), true);
        match decision {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::FailClosed,
                detail,
            } => {
                assert!(detail.contains("timed_pause_clear_error"));
                assert!(detail.contains("fail_closed_event_error"));
                assert!(
                    detail.contains("failed to persist shadow risk fail-closed event with fatal sqlite I/O")
                );
                assert!(detail.contains("xShmMap"));
            }
            other => panic!(
                "expected fail-closed block when fatal fail-closed event write fails after timed-pause clear error, got {other:?}"
            ),
        }
        assert!(guard.pause_until.is_some());
        assert_eq!(
            store.risk_event_count_by_type("shadow_risk_pause_cleared")?,
            0
        );
        assert_eq!(
            store.risk_event_count_by_type("shadow_risk_fail_closed")?,
            0
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_refresh_error_resets_hard_stop_clear_streak() {
        let mut guard = ShadowRiskGuard::new(RiskConfig::default());
        guard.hard_stop_clear_healthy_streak = 4;
        let now = Utc::now();

        assert!(guard.on_risk_refresh_error(now));
        assert_eq!(
            guard.hard_stop_clear_healthy_streak, 0,
            "refresh error must reset healthy streak to enforce consecutive healthy refreshes"
        );

        // Subsequent errors inside throttle window should be suppressed.
        assert!(!guard.on_risk_refresh_error(now + chrono::Duration::seconds(1)));
    }

    #[test]
    fn risk_guard_cached_refresh_error_blocks_buy_within_throttle_window() -> Result<()> {
        let (store, db_path) = make_test_store("cached-refresh-error")?;
        let mut guard = ShadowRiskGuard::new(RiskConfig::default());
        let now = Utc::now();
        guard.last_db_refresh_at = Some(now);
        guard.last_db_refresh_error = Some("simulated refresh failure".to_string());

        match guard.can_open_buy(&store, now + chrono::Duration::seconds(1), true) {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::FailClosed,
                detail,
            } => assert!(
                detail.contains("cached error"),
                "expected cached refresh error in fail-closed detail, got: {detail}"
            ),
            other => panic!("expected fail-closed block from cached refresh error, got {other:?}"),
        }

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_refresh_is_noop_when_killswitch_disabled() -> Result<()> {
        let unique = format!(
            "risk-noop-disabled-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let store = SqliteStore::open(Path::new(&db_path))?;

        let mut cfg = RiskConfig::default();
        cfg.shadow_killswitch_enabled = false;
        let mut guard = ShadowRiskGuard::new(cfg);
        guard.last_db_refresh_error = Some("stale error".to_string());

        guard.maybe_refresh_db_state(&store, Utc::now())?;
        assert!(
            guard.last_db_refresh_error.is_none(),
            "killswitch-disabled refresh should clear cached refresh errors"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    fn make_shadow_buy_task(signature: &str, wallet: &str, token: &str) -> ShadowTaskInput {
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

    #[test]
    fn scheduler_keeps_single_inflight_per_wallet_token_key() {
        let mut shadow_scheduler = ShadowScheduler::new();

        assert!(shadow_scheduler
            .enqueue_shadow_task(
                SHADOW_PENDING_TASK_CAPACITY,
                make_shadow_buy_task("A1", "wallet-a", "token-x"),
            )
            .is_ok());
        assert!(shadow_scheduler
            .enqueue_shadow_task(
                SHADOW_PENDING_TASK_CAPACITY,
                make_shadow_buy_task("A2", "wallet-a", "token-x"),
            )
            .is_ok());
        assert!(shadow_scheduler
            .enqueue_shadow_task(
                SHADOW_PENDING_TASK_CAPACITY,
                make_shadow_buy_task("B1", "wallet-b", "token-y"),
            )
            .is_ok());
        assert_eq!(shadow_scheduler.pending_shadow_task_count, 3);

        let first = shadow_scheduler
            .dequeue_next_shadow_task()
            .expect("first task");
        let second = shadow_scheduler
            .dequeue_next_shadow_task()
            .expect("second task");

        assert_eq!(first.swap.signature, "A1");
        assert_eq!(second.swap.signature, "B1");

        shadow_scheduler.mark_task_complete(&first.key);

        let third = shadow_scheduler
            .dequeue_next_shadow_task()
            .expect("third task");

        assert_eq!(third.swap.signature, "A2");
        assert_eq!(shadow_scheduler.pending_shadow_task_count, 0);
    }

    #[test]
    fn scheduler_reports_pending_or_inflight_buy_by_token() {
        let mut shadow_scheduler = ShadowScheduler::new();

        assert!(shadow_scheduler
            .enqueue_shadow_task(
                SHADOW_PENDING_TASK_CAPACITY,
                make_shadow_buy_task("A1", "wallet-a", "token-x"),
            )
            .is_ok());
        assert!(shadow_scheduler.token_has_pending_or_inflight_buy("token-x"));
        assert!(!shadow_scheduler.token_has_pending_or_inflight_buy("token-y"));

        let first = shadow_scheduler
            .dequeue_next_shadow_task()
            .expect("first task");
        assert!(shadow_scheduler.token_has_pending_or_inflight_buy("token-x"));

        shadow_scheduler.mark_task_complete(&first.key);
        assert!(!shadow_scheduler.token_has_pending_or_inflight_buy("token-x"));
    }

    #[test]
    fn enqueue_shadow_task_enforces_capacity() {
        let mut shadow_scheduler = ShadowScheduler::new();
        let cap = 2usize;

        assert!(shadow_scheduler
            .enqueue_shadow_task(cap, make_shadow_buy_task("A1", "wallet-a", "token-x"),)
            .is_ok());
        assert!(shadow_scheduler
            .enqueue_shadow_task(cap, make_shadow_buy_task("A2", "wallet-a", "token-x"),)
            .is_ok());
        assert!(shadow_scheduler
            .enqueue_shadow_task(cap, make_shadow_buy_task("B1", "wallet-b", "token-y"),)
            .is_err());
        assert_eq!(shadow_scheduler.pending_shadow_task_count, cap);
    }
