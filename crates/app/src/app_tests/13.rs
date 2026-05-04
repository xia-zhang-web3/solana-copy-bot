    #[test]
    fn risk_guard_does_not_restore_cleared_timed_pause_after_restart() -> Result<()> {
        let (store, db_path) = make_test_store("timed-pause-restore-cleared")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_drawdown_1h_stop_sol = -999.0;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_24h_stop_sol = -999.0;
        cfg.shadow_rug_loss_count_threshold = u64::MAX;
        cfg.shadow_rug_loss_rate_threshold = 1.0;
        let now = Utc::now();
        let mut original_guard = ShadowRiskGuard::new(cfg.clone());
        original_guard.activate_pause(
            &store,
            now,
            chrono::Duration::minutes(5),
            "restart_test",
            "cleared pause should stay cleared".to_string(),
        )?;
        original_guard.clear_pause(&store, now + chrono::Duration::seconds(10))?;

        let mut restarted_guard = ShadowRiskGuard::new(cfg);
        restarted_guard.restore_pause_from_store(&store, now + chrono::Duration::seconds(20))?;

        match restarted_guard.can_open_buy(&store, now + chrono::Duration::seconds(20), true) {
            BuyRiskDecision::Allow => {}
            other => {
                panic!("expected cleared timed pause to stay cleared after restart, got {other:?}")
            }
        }
        assert!(
            restarted_guard.pause_until.is_none(),
            "cleared durable pause should not be restored after restart"
        );
        assert!(
            restarted_guard.pause_reason.is_none(),
            "cleared durable pause reason should stay empty after restart"
        );
        assert_eq!(store.risk_event_count_by_type("shadow_risk_pause")?, 1);
        assert_eq!(
            store.risk_event_count_by_type("shadow_risk_pause_cleared")?,
            1
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_rug_loss_triggers_hard_stop() -> Result<()> {
        let (store, db_path) = make_test_store("rug-stop")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_rug_loss_window_minutes = 120;
        cfg.shadow_rug_loss_count_threshold = 2;
        cfg.shadow_rug_loss_rate_sample_size = 10;
        cfg.shadow_rug_loss_rate_threshold = 0.99;
        cfg.shadow_rug_loss_return_threshold = -0.70;
        cfg.shadow_drawdown_1h_stop_sol = -999.0;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_24h_stop_sol = -999.0;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();

        store.insert_shadow_closed_trade(
            "sig-rug-1",
            "wallet-a",
            "token-a",
            1.0,
            1.0,
            0.2,
            -0.8,
            now - chrono::Duration::minutes(30),
            now - chrono::Duration::minutes(20),
        )?;
        store.insert_shadow_closed_trade(
            "sig-rug-2",
            "wallet-b",
            "token-b",
            1.0,
            2.0,
            0.3,
            -1.7,
            now - chrono::Duration::minutes(25),
            now - chrono::Duration::minutes(10),
        )?;

        let decision = guard.can_open_buy(&store, now, true);
        match decision {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::HardStop,
                detail,
            } => assert!(detail.contains("rug_loss")),
            other => panic!("expected hard stop block, got {other:?}"),
        }

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_rug_rate_hard_stop_auto_clears_after_window_without_new_trades() -> Result<()> {
        let (store, db_path) = make_test_store("rug-rate-autoclear")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_rug_loss_window_minutes = 1;
        cfg.shadow_rug_loss_count_threshold = u64::MAX;
        cfg.shadow_rug_loss_rate_sample_size = 200;
        cfg.shadow_rug_loss_rate_threshold = 0.5;
        cfg.shadow_rug_loss_return_threshold = -0.70;
        cfg.shadow_drawdown_1h_stop_sol = -999.0;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_24h_stop_sol = -999.0;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();

        store.insert_shadow_closed_trade(
            "sig-rug-rate-lock",
            "wallet-a",
            "token-a",
            1.0,
            1.0,
            0.2,
            -0.8,
            now - chrono::Duration::seconds(55),
            now - chrono::Duration::seconds(50),
        )?;

        match guard.can_open_buy(&store, now, true) {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::HardStop,
                ..
            } => {}
            other => panic!("expected hard-stop block from rug-rate breach, got {other:?}"),
        }
        assert!(
            guard.hard_stop_reason.is_some(),
            "hard stop should be active after rug-rate breach"
        );

        let refresh_step_seconds = (RISK_DB_REFRESH_MIN_SECONDS + 1).max(6);
        let mut cleared = false;
        for cycle in 1..=30 {
            let cycle_ts = now + chrono::Duration::seconds(refresh_step_seconds * cycle as i64);
            if matches!(
                guard.can_open_buy(&store, cycle_ts, true),
                BuyRiskDecision::Allow
            ) {
                cleared = true;
                break;
            }
        }
        assert!(
            cleared,
            "hard stop should auto-clear after rug window expires even without new trades"
        );
        assert!(
            guard.hard_stop_reason.is_none(),
            "hard stop reason should clear after healthy refresh streak"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_hard_stop_auto_clears_without_restart() -> Result<()> {
        let (store, db_path) = make_test_store("hard-stop-autoclear")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_drawdown_24h_stop_sol = -0.5;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_1h_stop_sol = -999.0;
        cfg.shadow_rug_loss_count_threshold = u64::MAX;
        cfg.shadow_rug_loss_rate_threshold = 1.0;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();

        store.insert_shadow_closed_trade(
            "sig-hard-stop-loss",
            "wallet-a",
            "token-a",
            1.0,
            1.0,
            0.2,
            -0.8,
            now - chrono::Duration::minutes(30),
            now - chrono::Duration::minutes(20),
        )?;

        match guard.can_open_buy(&store, now, true) {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::HardStop,
                ..
            } => {}
            other => panic!("expected hard-stop block after drawdown breach, got {other:?}"),
        }
        assert!(
            guard.hard_stop_reason.is_some(),
            "hard stop should be set after drawdown breach"
        );

        store.insert_shadow_closed_trade(
            "sig-hard-stop-recover",
            "wallet-b",
            "token-b",
            1.0,
            1.0,
            3.0,
            2.0,
            now - chrono::Duration::minutes(10),
            now - chrono::Duration::minutes(5),
        )?;

        let refresh_step_seconds = (RISK_DB_REFRESH_MIN_SECONDS + 1).max(6);
        for cycle in 1..HARD_STOP_CLEAR_HEALTHY_REFRESHES {
            let cycle_ts = now + chrono::Duration::seconds(refresh_step_seconds * cycle as i64);
            match guard.can_open_buy(&store, cycle_ts, true) {
                BuyRiskDecision::Blocked {
                    reason: BuyRiskBlockReason::HardStop,
                    ..
                } => {}
                other => panic!(
                    "expected hard-stop to remain active during healthy cooldown (cycle {cycle}), got {other:?}"
                ),
            }
            assert!(
                guard.hard_stop_reason.is_some(),
                "hard stop should stay active until healthy cooldown is satisfied"
            );
        }

        let decision_after_recovery = guard.can_open_buy(
            &store,
            now + chrono::Duration::seconds(
                refresh_step_seconds * HARD_STOP_CLEAR_HEALTHY_REFRESHES as i64,
            ),
            true,
        );
        match decision_after_recovery {
            BuyRiskDecision::Allow => {}
            other => panic!("expected hard-stop auto-clear after recovery, got {other:?}"),
        }
        assert!(
            guard.hard_stop_reason.is_none(),
            "hard stop should clear after metrics normalize"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_refresh_returns_error_on_fatal_hard_stop_risk_event_write() -> Result<()> {
        let (store, db_path) = make_test_store("hard-stop-risk-event-fatal")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_drawdown_24h_stop_sol = -0.5;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_1h_stop_sol = -999.0;
        cfg.shadow_rug_loss_count_threshold = u64::MAX;
        cfg.shadow_rug_loss_rate_threshold = 1.0;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();

        store.insert_shadow_closed_trade(
            "sig-hard-stop-fatal",
            "wallet-a",
            "token-a",
            1.0,
            1.0,
            0.2,
            -0.8,
            now - chrono::Duration::minutes(30),
            now - chrono::Duration::minutes(20),
        )?;

        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute_batch(
            "CREATE TRIGGER fail_shadow_risk_hard_stop_event_insert
             BEFORE INSERT ON risk_events
             WHEN NEW.type = 'shadow_risk_hard_stop'
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let error = guard
            .maybe_refresh_db_state(&store, now)
            .expect_err("fatal hard-stop risk event write must abort refresh");
        let error_text = format!("{error:#}");
        assert!(
            error_text
                .contains("failed to persist shadow risk hard stop event with fatal sqlite I/O"),
            "expected hard-stop fatal context, got: {error_text}"
        );
        assert!(
            error_text.contains("failed to insert risk event"),
            "expected storage insert_risk_event context, got: {error_text}"
        );
        assert!(
            error_text.contains("xShmMap"),
            "expected fatal sqlite marker to survive error chain, got: {error_text}"
        );
        assert!(guard.hard_stop_reason.is_some());
        assert_eq!(store.risk_event_count_by_type("shadow_risk_hard_stop")?, 0);

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_refresh_returns_error_on_fatal_exposure_hard_cap_risk_event_write() -> Result<()>
    {
        let (store, db_path) = make_test_store("exposure-hard-cap-risk-event-fatal")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_soft_exposure_cap_sol = 0.5;
        cfg.shadow_soft_exposure_resume_below_sol = 0.4;
        cfg.shadow_hard_exposure_cap_sol = 1.0;
        cfg.shadow_drawdown_24h_stop_sol = -999.0;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_1h_stop_sol = -999.0;
        cfg.shadow_rug_loss_count_threshold = u64::MAX;
        cfg.shadow_rug_loss_rate_threshold = 1.0;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();
        let opened_ts = now - chrono::Duration::minutes(5);
        store.insert_shadow_lot("wallet-a", "token-a", 10.0, 1.2, opened_ts)?;

        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute_batch(
            "CREATE TRIGGER fail_shadow_risk_exposure_hard_cap_event_insert
             BEFORE INSERT ON risk_events
             WHEN NEW.type = 'shadow_risk_exposure_hard_cap'
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let error = guard
            .maybe_refresh_db_state(&store, now)
            .expect_err("fatal exposure hard-cap risk event write must abort refresh");
        let error_text = format!("{error:#}");
        assert!(
            error_text.contains(
                "failed to persist shadow risk exposure hard cap event with fatal sqlite I/O"
            ),
            "expected hard-cap fatal context, got: {error_text}"
        );
        assert!(
            error_text.contains("failed to insert risk event"),
            "expected storage insert_risk_event context, got: {error_text}"
        );
        assert!(
            error_text.contains("xShmMap"),
            "expected fatal sqlite marker to survive error chain, got: {error_text}"
        );
        assert!(guard.exposure_hard_blocked);
        assert_eq!(
            store.risk_event_count_by_type("shadow_risk_exposure_hard_cap")?,
            0
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_refresh_returns_error_on_fatal_hard_stop_clear_risk_event_write() -> Result<()> {
        let (store, db_path) = make_test_store("hard-stop-clear-risk-event-fatal")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_drawdown_24h_stop_sol = -999.0;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_1h_stop_sol = -999.0;
        cfg.shadow_rug_loss_count_threshold = u64::MAX;
        cfg.shadow_rug_loss_rate_threshold = 1.0;
        let mut guard = ShadowRiskGuard::new(cfg);
        guard.hard_stop_reason = Some("drawdown_24h: synthetic prior breach".to_string());
        guard.hard_stop_clear_healthy_streak = HARD_STOP_CLEAR_HEALTHY_REFRESHES - 1;
        let now = Utc::now();

        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute_batch(
            "CREATE TRIGGER fail_shadow_risk_hard_stop_clear_event_insert
             BEFORE INSERT ON risk_events
             WHEN NEW.type = 'shadow_risk_hard_stop_cleared'
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let error = guard
            .maybe_refresh_db_state(&store, now)
            .expect_err("fatal hard-stop clear risk event write must abort refresh");
        let error_text = format!("{error:#}");
        assert!(
            error_text.contains(
                "failed to persist shadow risk hard stop clear event with fatal sqlite I/O"
            ),
            "expected hard-stop clear fatal context, got: {error_text}"
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
            guard.hard_stop_reason.is_some(),
            "fatal hard-stop clear write must preserve runtime hard-stop state"
        );
        assert_eq!(
            store.risk_event_count_by_type("shadow_risk_hard_stop_cleared")?,
            0
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_refresh_returns_error_on_fatal_exposure_hard_cap_clear_risk_event_write(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("exposure-hard-cap-clear-risk-event-fatal")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_soft_exposure_cap_sol = 0.5;
        cfg.shadow_soft_exposure_resume_below_sol = 0.4;
        cfg.shadow_hard_exposure_cap_sol = 1.0;
        cfg.shadow_drawdown_24h_stop_sol = -999.0;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_1h_stop_sol = -999.0;
        cfg.shadow_rug_loss_count_threshold = u64::MAX;
        cfg.shadow_rug_loss_rate_threshold = 1.0;
        let mut guard = ShadowRiskGuard::new(cfg);
        guard.exposure_hard_blocked = true;
        guard.exposure_hard_detail =
            Some("risk_open_notional_sol=1.200000 hard_cap=1.000000".to_string());
        let now = Utc::now();

        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute_batch(
            "CREATE TRIGGER fail_shadow_risk_exposure_hard_cap_clear_event_insert
             BEFORE INSERT ON risk_events
             WHEN NEW.type = 'shadow_risk_exposure_hard_cap_cleared'
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let error = guard
            .maybe_refresh_db_state(&store, now)
            .expect_err("fatal exposure hard-cap clear risk event write must abort refresh");
        let error_text = format!("{error:#}");
        assert!(
            error_text.contains(
                "failed to persist shadow risk exposure hard cap clear event with fatal sqlite I/O"
            ),
            "expected hard-cap clear fatal context, got: {error_text}"
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
            guard.exposure_hard_blocked,
            "fatal hard-cap clear write must preserve runtime hard-cap block"
        );
        assert!(guard.exposure_hard_detail.is_some());
        assert_eq!(
            store.risk_event_count_by_type("shadow_risk_exposure_hard_cap_cleared")?,
            0
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_refresh_returns_error_on_fatal_soft_exposure_pause_risk_event_write() -> Result<()>
    {
        let (store, db_path) = make_test_store("soft-exposure-pause-risk-event-fatal")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_soft_exposure_cap_sol = 0.5;
        cfg.shadow_soft_exposure_resume_below_sol = 0.4;
        cfg.shadow_hard_exposure_cap_sol = 2.0;
        cfg.shadow_soft_pause_minutes = 1;
        cfg.shadow_drawdown_24h_stop_sol = -999.0;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_1h_stop_sol = -999.0;
        cfg.shadow_rug_loss_count_threshold = u64::MAX;
        cfg.shadow_rug_loss_rate_threshold = 1.0;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();
        let opened_ts = now - chrono::Duration::minutes(5);
        store.insert_shadow_lot("wallet-a", "token-a", 10.0, 0.6, opened_ts)?;

        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute_batch(
            "CREATE TRIGGER fail_shadow_risk_soft_pause_event_insert
             BEFORE INSERT ON risk_events
             WHEN NEW.type = 'shadow_risk_pause'
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let error = guard
            .maybe_refresh_db_state(&store, now)
            .expect_err("fatal soft-exposure pause risk event write must abort refresh");
        let error_text = format!("{error:#}");
        assert!(
            error_text.contains(
                "failed to persist shadow risk soft exposure pause event with fatal sqlite I/O"
            ),
            "expected soft-pause fatal context, got: {error_text}"
        );
        assert!(
            error_text.contains("failed to insert risk event"),
            "expected storage insert_risk_event context, got: {error_text}"
        );
        assert!(
            error_text.contains("xShmMap"),
            "expected fatal sqlite marker to survive error chain, got: {error_text}"
        );
        assert!(guard.soft_exposure_pause_latched);
        assert_eq!(store.risk_event_count_by_type("shadow_risk_pause")?, 0);

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }
