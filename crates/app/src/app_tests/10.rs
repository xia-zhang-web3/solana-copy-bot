    #[test]
    fn risk_guard_hard_exposure_blocks_then_auto_clears() -> Result<()> {
        let (store, db_path) = make_test_store("hard-exposure-stop")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_hard_exposure_cap_sol = 1.0;
        cfg.shadow_soft_exposure_cap_sol = 0.5;
        let mut guard = ShadowRiskGuard::new(cfg);
        let opened_ts = DateTime::parse_from_rfc3339("2026-02-17T00:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let lot_id = store.insert_shadow_lot("wallet-a", "token-a", 10.0, 1.2, opened_ts)?;
        let now = Utc::now();

        let decision = guard.can_open_buy(&store, now, true);
        match decision {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::ExposureCap,
                ..
            } => {}
            other => panic!("expected exposure-cap block, got {other:?}"),
        }

        store.delete_shadow_lot(lot_id)?;
        let decision_after_clear = guard.can_open_buy(
            &store,
            now + chrono::Duration::seconds((RISK_DB_REFRESH_MIN_SECONDS + 1).max(6)),
            true,
        );
        match decision_after_clear {
            BuyRiskDecision::Allow => {}
            other => panic!("expected auto-clear after exposure normalizes, got {other:?}"),
        }

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_hard_exposure_ignores_dust_shadow_lots() -> Result<()> {
        let (store, db_path) = make_test_store("hard-exposure-dust")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_hard_exposure_cap_sol = 1.0;
        cfg.shadow_soft_exposure_cap_sol = 0.5;
        let mut guard = ShadowRiskGuard::new(cfg);
        let opened_ts = DateTime::parse_from_rfc3339("2026-02-17T00:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let lot_id = store.insert_shadow_lot("wallet-a", "token-a", 10.0, 1.2, opened_ts)?;
        store.update_shadow_lot(lot_id, 1e-13, 1.2)?;
        let now = Utc::now();

        let decision = guard.can_open_buy(&store, now, true);
        match decision {
            BuyRiskDecision::Allow => {}
            other => panic!("expected dust lot to be ignored by hard exposure cap, got {other:?}"),
        }

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_hard_exposure_uses_lamport_normalized_cap() -> Result<()> {
        let (store, db_path) = make_test_store("hard-exposure-lamport-cap")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_hard_exposure_cap_sol = 0.1000000004;
        cfg.shadow_soft_exposure_cap_sol = 99.0;
        cfg.shadow_drawdown_1h_stop_sol = -999.0;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_24h_stop_sol = -999.0;
        let mut guard = ShadowRiskGuard::new(cfg);
        let opened_ts = DateTime::parse_from_rfc3339("2026-02-17T00:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        store.insert_shadow_lot("wallet-a", "token-a", 10.0, 0.1, opened_ts)?;
        let now = Utc::now();

        let decision = guard.can_open_buy(&store, now, true);
        match decision {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::ExposureCap,
                detail,
            } => assert!(
                detail.contains("hard_cap"),
                "expected hard cap detail, got {detail}"
            ),
            other => panic!("expected lamport-normalized exposure block, got {other:?}"),
        }

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }
