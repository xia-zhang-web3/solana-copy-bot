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
    fn risk_guard_token_loss_cooldown_blocks_new_buys_for_toxic_token() -> Result<()> {
        let (store, db_path) = make_test_store("token-loss-cooldown")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_drawdown_1h_stop_sol = -999.0;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_24h_stop_sol = -999.0;
        cfg.shadow_rug_loss_count_threshold = u64::MAX;
        cfg.shadow_rug_loss_rate_threshold = 1.0;
        cfg.shadow_token_loss_cooldown_enabled = true;
        cfg.shadow_token_loss_cooldown_window_minutes = 120;
        cfg.shadow_token_loss_cooldown_count_threshold = 2;
        cfg.shadow_token_loss_cooldown_return_threshold = -0.60;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();

        for index in 0..2 {
            store.insert_shadow_closed_trade(
                &format!("sig-token-loss-{index}"),
                &format!("wallet-{index}"),
                "toxic-token",
                1.0,
                0.2,
                0.06,
                -0.14,
                now - chrono::Duration::minutes(20 - index),
                now - chrono::Duration::minutes(10 - index),
            )?;
        }

        match guard.can_open_buy_for_token(&store, now, true, Some("toxic-token")) {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::TokenCooldown,
                detail,
            } => {
                assert!(detail.contains("toxic-token"));
                assert!(detail.contains("loss_count=2"));
            }
            other => panic!("expected token cooldown block, got {other:?}"),
        }
        match guard.can_open_buy_for_token(&store, now, true, Some("clean-token")) {
            BuyRiskDecision::Allow => {}
            other => panic!("clean token must not be blocked by another token cooldown: {other:?}"),
        }

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_rug_rate_ignores_tiny_sample_below_floor() -> Result<()> {
        let (store, db_path) = make_test_store("rug-rate-tiny-sample")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_rug_loss_window_minutes = 120;
        cfg.shadow_rug_loss_count_threshold = 3;
        cfg.shadow_rug_loss_rate_sample_size = 200;
        cfg.shadow_rug_loss_rate_threshold = 0.04;
        cfg.shadow_rug_loss_return_threshold = -0.70;
        cfg.shadow_drawdown_1h_stop_sol = -999.0;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_24h_stop_sol = -999.0;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();

        for index in 0..2 {
            store.insert_shadow_closed_trade(
                &format!("sig-rug-rate-small-{index}"),
                &format!("wallet-{index}"),
                &format!("token-{index}"),
                1.0,
                1.0,
                0.2,
                -0.8,
                now - chrono::Duration::minutes(30 - index),
                now - chrono::Duration::minutes(20 - index),
            )?;
        }

        match guard.can_open_buy(&store, now, true) {
            BuyRiskDecision::Allow => {}
            other => panic!("tiny rug-rate sample must not hard-stop shadow, got {other:?}"),
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
        cfg.shadow_rug_loss_rate_sample_size = 3;
        cfg.shadow_rug_loss_rate_threshold = 0.5;
        cfg.shadow_rug_loss_return_threshold = -0.70;
        cfg.shadow_drawdown_1h_stop_sol = -999.0;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_24h_stop_sol = -999.0;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();

        for index in 0..3 {
            store.insert_shadow_closed_trade(
                &format!("sig-rug-rate-lock-{index}"),
                &format!("wallet-{index}"),
                &format!("token-{index}"),
                1.0,
                1.0,
                0.2,
                -0.8,
                now - chrono::Duration::seconds(55 - index),
                now - chrono::Duration::seconds(50 - index),
            )?;
        }

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
