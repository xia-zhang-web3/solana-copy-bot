    #[test]
    fn risk_guard_drawdown_1h_triggers_timed_pause() -> Result<()> {
        let (store, db_path) = make_test_store("drawdown-1h")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_drawdown_1h_stop_sol = -0.3;
        cfg.shadow_drawdown_1h_pause_minutes = 30;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_24h_stop_sol = -999.0;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();
        store.insert_shadow_closed_trade(
            "sig-dd1",
            "wallet-a",
            "token-a",
            1.0,
            1.0,
            0.5,
            -0.5,
            now - chrono::Duration::minutes(10),
            now - chrono::Duration::minutes(5),
        )?;

        let decision = guard.can_open_buy(&store, now, true);
        match decision {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::TimedPause,
                detail,
            } => assert!(detail.contains("drawdown_1h")),
            other => panic!("expected timed pause block, got {other:?}"),
        }

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_timed_pause_auto_clears_and_records_event() -> Result<()> {
        let (store, db_path) = make_test_store("timed-pause-autoclear")?;
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

        match guard.can_open_buy(&store, now, true) {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::TimedPause,
                detail,
            } => assert!(detail.contains("exposure_soft_cap")),
            other => panic!("expected timed pause block from soft exposure cap, got {other:?}"),
        }
        assert_eq!(store.risk_event_count_by_type("shadow_risk_pause")?, 1);
        assert!(
            guard.soft_exposure_pause_latched,
            "soft exposure pause should latch after breach"
        );
        assert!(
            guard.soft_exposure_pause_until.is_some(),
            "soft exposure pause should capture its initial cooldown window"
        );

        store.delete_shadow_lot(lot_id)?;
        let decision_after_clear = guard.can_open_buy(
            &store,
            now + chrono::Duration::seconds((RISK_DB_REFRESH_MIN_SECONDS + 61).max(61)),
            true,
        );
        match decision_after_clear {
            BuyRiskDecision::Allow => {}
            other => {
                panic!("expected timed pause auto-clear after exposure normalized, got {other:?}")
            }
        }
        assert!(
            !guard.soft_exposure_pause_latched,
            "soft exposure pause latch should clear after exposure normalizes below resume threshold"
        );
        assert!(
            guard.soft_exposure_pause_until.is_none(),
            "soft exposure pause window should clear after recovery below resume threshold"
        );
        assert!(
            guard.soft_exposure_pause_reason.is_none(),
            "soft exposure pause reason should clear after recovery below resume threshold"
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
    fn risk_guard_ignores_quarantined_legacy_shadow_open_notional_for_exposure_gates() -> Result<()>
    {
        let (store, db_path) = make_test_store("risk-guard-ignores-quarantined-open-notional")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_soft_exposure_cap_sol = 0.5;
        cfg.shadow_soft_exposure_resume_below_sol = 0.4;
        cfg.shadow_hard_exposure_cap_sol = 1.0;
        cfg.shadow_drawdown_1h_stop_sol = -999.0;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_24h_stop_sol = -999.0;
        cfg.shadow_rug_loss_count_threshold = u64::MAX;
        cfg.shadow_rug_loss_rate_threshold = 1.0;
        let now = Utc::now();
        let opened_ts = now - chrono::Duration::minutes(5);

        store.insert_shadow_lot("wallet-a", "token-risk", 10.0, 0.45, opened_ts)?;
        let quarantined_lot_id =
            store.insert_shadow_lot("wallet-a", "token-quarantine", 10.0, 0.30, opened_ts)?;
        store.update_shadow_lot_risk_context(
            quarantined_lot_id,
            copybot_storage_core::SHADOW_RISK_CONTEXT_QUARANTINED_LEGACY,
        )?;

        assert!((store.shadow_open_notional_sol()? - 0.75).abs() < 1e-12);
        assert!((store.shadow_risk_open_notional_sol()? - 0.45).abs() < 1e-12);

        let mut guard = ShadowRiskGuard::new(cfg);
        match guard.can_open_buy(&store, now, true) {
            BuyRiskDecision::Allow => {}
            other => panic!(
                "expected quarantined legacy exposure to stay out of live risk gating, got {other:?}"
            ),
        }

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_soft_exposure_pause_does_not_rearm_or_extend_while_still_above_resume_threshold(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("soft-pause-no-rearm")?;
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

        match guard.can_open_buy(&store, now, true) {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::TimedPause,
                detail,
            } => assert!(detail.contains("exposure_soft_cap")),
            other => panic!("expected initial soft pause block, got {other:?}"),
        }
        let initial_until = guard
            .soft_exposure_pause_until
            .expect("soft pause should set initial until");
        assert_eq!(store.risk_event_count_by_type("shadow_risk_pause")?, 1);

        let still_breached_at =
            now + chrono::Duration::seconds((RISK_DB_REFRESH_MIN_SECONDS + 10).max(10));
        match guard.can_open_buy(&store, still_breached_at, true) {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::TimedPause,
                detail,
            } => assert!(detail.contains("exposure_soft_cap")),
            other => panic!("expected soft pause to stay active without rearm, got {other:?}"),
        }
        assert_eq!(guard.soft_exposure_pause_until, Some(initial_until));
        assert_eq!(store.risk_event_count_by_type("shadow_risk_pause")?, 1);

        let after_initial_until =
            now + chrono::Duration::seconds((RISK_DB_REFRESH_MIN_SECONDS + 61).max(61));
        match guard.can_open_buy(&store, after_initial_until, true) {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::TimedPause,
                detail,
            } => assert!(detail.contains("exposure_soft_cap")),
            other => panic!(
                "expected soft exposure latch to remain blocked above resume threshold, got {other:?}"
            ),
        }
        assert_eq!(guard.soft_exposure_pause_until, Some(initial_until));
        assert_eq!(store.risk_event_count_by_type("shadow_risk_pause")?, 1);
        assert_eq!(
            store.risk_event_count_by_type("shadow_risk_pause_cleared")?,
            0
        );

        store.delete_shadow_lot(lot_id)?;
        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_soft_exposure_pause_requires_recovery_below_resume_threshold_to_clear(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("soft-pause-hysteresis")?;
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
        store.update_shadow_lot(lot_id, 10.0, 0.45)?;
        let between_thresholds_at =
            now + chrono::Duration::seconds((RISK_DB_REFRESH_MIN_SECONDS + 61).max(61));
        match guard.can_open_buy(&store, between_thresholds_at, true) {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::TimedPause,
                detail,
            } => assert!(detail.contains("exposure_soft_cap")),
            other => panic!(
                "expected soft exposure latch to stay blocked between resume and soft thresholds, got {other:?}"
            ),
        }
        assert!(guard.soft_exposure_pause_latched);
        assert_eq!(
            store.risk_event_count_by_type("shadow_risk_pause_cleared")?,
            0
        );

        store.update_shadow_lot(lot_id, 10.0, 0.35)?;
        let recovered_at = between_thresholds_at
            + chrono::Duration::seconds((RISK_DB_REFRESH_MIN_SECONDS + 1).max(6));
        match guard.can_open_buy(&store, recovered_at, true) {
            BuyRiskDecision::Allow => {}
            other => panic!(
                "expected recovery below resume threshold to clear soft pause, got {other:?}"
            ),
        }
        assert!(!guard.soft_exposure_pause_latched);
        assert_eq!(store.risk_event_count_by_type("shadow_risk_pause")?, 1);
        assert_eq!(
            store.risk_event_count_by_type("shadow_risk_pause_cleared")?,
            1
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }
