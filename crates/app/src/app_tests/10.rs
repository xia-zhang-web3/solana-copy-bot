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

    #[test]
    fn risk_guard_token_loss_cooldown_blocks_single_catastrophe() -> Result<()> {
        let (store, db_path) = make_test_store("token-catastrophe-cooldown")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_drawdown_1h_stop_sol = -999.0;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_24h_stop_sol = -999.0;
        cfg.shadow_rug_loss_count_threshold = u64::MAX;
        cfg.shadow_rug_loss_rate_threshold = 1.0;
        cfg.shadow_token_loss_cooldown_enabled = true;
        cfg.shadow_token_loss_cooldown_count_threshold = 3;
        cfg.shadow_token_loss_cooldown_return_threshold = -0.60;
        cfg.shadow_token_loss_cooldown_catastrophe_min_entry_sol = 0.15;
        cfg.shadow_token_loss_cooldown_catastrophe_max_roi = -0.60;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();

        store.insert_shadow_closed_trade(
            "sig-token-catastrophe",
            "wallet-a",
            "toxic-token",
            1.0,
            0.2,
            0.04,
            -0.16,
            now - chrono::Duration::minutes(20),
            now - chrono::Duration::minutes(10),
        )?;

        match guard.can_open_buy_for_token(&store, now, true, Some("toxic-token")) {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::TokenCooldown,
                detail,
            } => {
                assert!(detail.contains("toxic-token"));
                assert!(detail.contains("reason=catastrophic"));
                assert!(detail.contains("catastrophe_count=1"));
            }
            other => panic!("expected catastrophic token cooldown block, got {other:?}"),
        }

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_token_loss_cooldown_blocks_single_terminal_zero() -> Result<()> {
        let (store, db_path) = make_test_store("token-terminal-zero-catastrophe")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_drawdown_1h_stop_sol = -999.0;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_24h_stop_sol = -999.0;
        cfg.shadow_rug_loss_count_threshold = u64::MAX;
        cfg.shadow_rug_loss_rate_threshold = 1.0;
        cfg.shadow_token_loss_cooldown_enabled = true;
        cfg.shadow_token_loss_cooldown_count_threshold = 3;
        cfg.shadow_token_loss_cooldown_catastrophe_min_entry_sol = 0.15;
        cfg.shadow_token_loss_cooldown_catastrophe_max_roi = -0.60;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();

        store.insert_shadow_closed_trade_exact_with_context(
            "sig-token-terminal-zero",
            "wallet-a",
            "toxic-terminal-token",
            1.0,
            None,
            0.2,
            0.0,
            -0.2,
            copybot_storage_core::SHADOW_CLOSE_CONTEXT_STALE_TERMINAL_ZERO_PRICE,
            now - chrono::Duration::minutes(20),
            now - chrono::Duration::minutes(10),
        )?;

        match guard.can_open_buy_for_token(&store, now, true, Some("toxic-terminal-token")) {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::TokenCooldown,
                detail,
            } => {
                assert!(detail.contains("toxic-terminal-token"));
                assert!(detail.contains("reason=catastrophic"));
                assert!(detail.contains("catastrophe_count=1"));
            }
            other => panic!("expected terminal-zero token cooldown block, got {other:?}"),
        }

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_wallet_token_fast_loss_blocks_rapid_reentry() -> Result<()> {
        let (store, db_path) = make_test_store("wallet-token-fast-loss")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_drawdown_1h_stop_sol = -999.0;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_24h_stop_sol = -999.0;
        cfg.shadow_rug_loss_count_threshold = u64::MAX;
        cfg.shadow_rug_loss_rate_threshold = 1.0;
        cfg.shadow_wallet_loss_cooldown_enabled = false;
        cfg.shadow_token_loss_cooldown_enabled = false;
        cfg.shadow_wallet_token_fast_loss_cooldown_enabled = true;
        cfg.shadow_wallet_token_fast_loss_cooldown_window_minutes = 60;
        cfg.shadow_wallet_token_fast_loss_cooldown_max_hold_seconds = 60;
        cfg.shadow_wallet_token_fast_loss_cooldown_min_entry_sol = 0.15;
        cfg.shadow_wallet_token_fast_loss_cooldown_max_roi = -0.08;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();

        store.insert_shadow_closed_trade(
            "sig-fast-loss",
            "wallet-a",
            "token-a",
            1.0,
            0.2,
            0.18,
            -0.02,
            now - chrono::Duration::seconds(30),
            now - chrono::Duration::seconds(1),
        )?;

        match guard.can_open_buy_for_signal(&store, now, true, Some("wallet-a"), Some("token-a")) {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::WalletTokenCooldown,
                detail,
            } => {
                assert!(detail.contains("wallet-a"));
                assert!(detail.contains("token-a"));
                assert!(detail.contains("hold_seconds"));
            }
            other => panic!("expected wallet/token fast-loss cooldown block, got {other:?}"),
        }

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_wallet_token_fast_loss_ignores_slow_loss() -> Result<()> {
        let (store, db_path) = make_test_store("wallet-token-slow-loss")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_drawdown_1h_stop_sol = -999.0;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_24h_stop_sol = -999.0;
        cfg.shadow_rug_loss_count_threshold = u64::MAX;
        cfg.shadow_rug_loss_rate_threshold = 1.0;
        cfg.shadow_wallet_loss_cooldown_enabled = false;
        cfg.shadow_token_loss_cooldown_enabled = false;
        cfg.shadow_wallet_token_fast_loss_cooldown_enabled = true;
        cfg.shadow_wallet_token_fast_loss_cooldown_max_hold_seconds = 60;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();

        store.insert_shadow_closed_trade(
            "sig-slow-loss",
            "wallet-a",
            "token-a",
            1.0,
            0.2,
            0.18,
            -0.02,
            now - chrono::Duration::minutes(10),
            now - chrono::Duration::seconds(1),
        )?;

        match guard.can_open_buy_for_signal(&store, now, true, Some("wallet-a"), Some("token-a")) {
            BuyRiskDecision::Allow => {}
            other => panic!("expected slow pair loss not to block reentry, got {other:?}"),
        }

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_token_open_lot_count_blocks_second_open_lot() -> Result<()> {
        let (store, db_path) = make_test_store("token-open-lot-count")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_drawdown_1h_stop_sol = -999.0;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_24h_stop_sol = -999.0;
        cfg.shadow_rug_loss_count_threshold = u64::MAX;
        cfg.shadow_rug_loss_rate_threshold = 1.0;
        cfg.shadow_wallet_loss_cooldown_enabled = false;
        cfg.shadow_token_loss_cooldown_enabled = false;
        cfg.shadow_wallet_token_fast_loss_cooldown_enabled = false;
        cfg.shadow_max_open_lots_per_token = 1;
        cfg.shadow_max_open_notional_per_token_sol = 10.0;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();

        store.insert_shadow_lot("wallet-a", "crowded-token", 100.0, 0.2, now)?;

        match guard.can_open_buy_for_signal(
            &store,
            now,
            true,
            Some("wallet-b"),
            Some("crowded-token"),
        ) {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::ExposureCap,
                detail,
            } => {
                assert!(detail.contains("crowded-token"));
                assert!(detail.contains("risk_open_lots=1"));
                assert!(detail.contains("per_token_lot_cap=1"));
            }
            other => panic!("expected token open lot count block, got {other:?}"),
        }

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }
