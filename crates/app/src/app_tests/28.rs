#[test]
    fn stale_lot_cleanup_recovery_zero_price_does_not_override_terminal_close_after_threshold(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("stale-lot-recovery-terminal-boundary")?;
        let now = DateTime::parse_from_rfc3339("2026-03-10T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let opened_ts = now - chrono::Duration::hours(14);

        store.insert_observed_swap(&SwapEvent {
            wallet: "leader-wallet".to_string(),
            dex: "pumpswap".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-a".to_string(),
            amount_in: 1.0,
            amount_out: 1000.0,
            signature: "sig-only-one-sample-recovery-terminal".to_string(),
            slot: 1,
            ts_utc: now - chrono::Duration::minutes(5),
            exact_amounts: None,
        })?;
        let lot_id = store.insert_shadow_lot("wallet-a", "token-a", 500.0, 0.25, opened_ts)?;

        let mut open_pairs = store.list_shadow_open_pairs()?;
        let stats = close_stale_shadow_lots(&store, &mut open_pairs, 6, 12, true, now)?;

        assert_eq!(stats.closed_priced, 0);
        assert_eq!(stats.recovery_zero_closed, 0);
        assert_eq!(stats.terminal_zero_closed, 1);
        assert_eq!(stats.skipped_unpriced, 0);
        let signal_id = format!("stale-close-{}-{}", lot_id, now.timestamp_millis());
        assert_eq!(
            store.shadow_closed_trade_close_context(&signal_id)?,
            Some(copybot_storage_core::SHADOW_CLOSE_CONTEXT_STALE_TERMINAL_ZERO_PRICE.to_string())
        );
        assert_eq!(
            store.risk_event_count_by_type("shadow_stale_close_recovery_zero_price")?,
            0
        );
        assert_eq!(
            store.risk_event_count_by_type("shadow_stale_close_terminal_zero_price")?,
            1
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn stale_lot_cleanup_terminal_zero_price_preserves_exact_qty_sidecars() -> Result<()> {
        let (store, db_path) = make_test_store("stale-lot-unpriced-exact")?;
        let now = DateTime::parse_from_rfc3339("2026-03-10T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let opened_ts = now - chrono::Duration::hours(14);

        store.insert_observed_swap(&SwapEvent {
            wallet: "leader-wallet".to_string(),
            dex: "pumpswap".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-a".to_string(),
            amount_in: 1.0,
            amount_out: 1.0,
            signature: "sig-exact-only-one-sample".to_string(),
            slot: 1,
            ts_utc: now - chrono::Duration::minutes(5),
            exact_amounts: None,
        })?;
        let lot_id = store.insert_shadow_lot_exact(
            "wallet-a",
            "token-a",
            0.5,
            Some(TokenQuantity::new(500_000, 6)),
            0.25,
            opened_ts,
        )?;

        let mut open_pairs = store.list_shadow_open_pairs()?;
        let stats = close_stale_shadow_lots(&store, &mut open_pairs, 6, 12, false, now)?;

        assert_eq!(stats.closed_priced, 0);
        assert_eq!(stats.terminal_zero_closed, 1);
        assert_eq!(stats.skipped_unpriced, 0);
        assert!(!store.has_shadow_lots("wallet-a", "token-a")?);
        assert!(!open_pairs.contains(&("wallet-a".to_string(), "token-a".to_string())));

        let signal_id = format!("stale-close-{}-{}", lot_id, now.timestamp_millis());
        assert_eq!(
            store.shadow_closed_trade_qty_exact(&signal_id)?,
            Some(TokenQuantity::new(500_000, 6))
        );
        assert_eq!(
            store.shadow_closed_trade_accounting_bucket(&signal_id)?,
            Some("exact_post_cutover".to_string())
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_ignores_terminal_zero_price_stale_close_losses_for_hard_stop() -> Result<()> {
        let (store, db_path) = make_test_store("stale-lot-terminal-risk-ignore")?;
        let now = DateTime::parse_from_rfc3339("2026-03-10T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let opened_ts = now - chrono::Duration::hours(14);

        store.insert_observed_swap(&SwapEvent {
            wallet: "leader-wallet".to_string(),
            dex: "pumpswap".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-a".to_string(),
            amount_in: 1.0,
            amount_out: 1000.0,
            signature: "sig-only-one-sample-risk-ignore".to_string(),
            slot: 1,
            ts_utc: now - chrono::Duration::minutes(5),
            exact_amounts: None,
        })?;
        store.insert_shadow_lot("wallet-a", "token-a", 500.0, 0.25, opened_ts)?;

        let mut open_pairs = store.list_shadow_open_pairs()?;
        let stats = close_stale_shadow_lots(&store, &mut open_pairs, 6, 12, false, now)?;
        assert_eq!(stats.terminal_zero_closed, 1);

        let (all_trades, all_pnl) =
            store.shadow_realized_pnl_since(now - chrono::Duration::days(1))?;
        assert_eq!(all_trades, 1);
        assert!(
            all_pnl < 0.0,
            "accounting pnl must keep terminal-zero loss visible"
        );

        let (risk_trades, risk_pnl) =
            store.shadow_risk_realized_pnl_lamports_since(now - chrono::Duration::days(1))?;
        assert_eq!(risk_trades, 0);
        assert_eq!(risk_pnl, SignedLamports::ZERO);

        let mut cfg = RiskConfig::default();
        cfg.shadow_drawdown_24h_stop_sol = -0.5;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_1h_stop_sol = -999.0;
        cfg.shadow_rug_loss_count_threshold = u64::MAX;
        cfg.shadow_rug_loss_rate_threshold = 1.0;
        let mut guard = ShadowRiskGuard::new(cfg);
        match guard.can_open_buy(&store, now + chrono::Duration::seconds(1), true) {
            BuyRiskDecision::Allow => {}
            other => panic!("terminal-zero stale close should not trip hard stop, got {other:?}"),
        }

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_ignores_recovery_zero_price_stale_close_losses_for_hard_stop() -> Result<()> {
        let (store, db_path) = make_test_store("stale-lot-recovery-risk-ignore")?;
        let now = DateTime::parse_from_rfc3339("2026-03-10T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let opened_ts = now - chrono::Duration::hours(10);

        store.insert_observed_swap(&SwapEvent {
            wallet: "leader-wallet".to_string(),
            dex: "pumpswap".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-a".to_string(),
            amount_in: 1.0,
            amount_out: 1000.0,
            signature: "sig-only-one-sample-recovery-risk-ignore".to_string(),
            slot: 1,
            ts_utc: now - chrono::Duration::minutes(5),
            exact_amounts: None,
        })?;
        store.insert_shadow_lot("wallet-a", "token-a", 500.0, 0.25, opened_ts)?;

        let mut open_pairs = store.list_shadow_open_pairs()?;
        let stats = close_stale_shadow_lots(&store, &mut open_pairs, 6, 12, true, now)?;
        assert_eq!(stats.recovery_zero_closed, 1);

        let (all_trades, all_pnl) =
            store.shadow_realized_pnl_since(now - chrono::Duration::days(1))?;
        assert_eq!(all_trades, 1);
        assert!(all_pnl < 0.0);

        let (risk_trades, risk_pnl) =
            store.shadow_risk_realized_pnl_lamports_since(now - chrono::Duration::days(1))?;
        assert_eq!(risk_trades, 0);
        assert_eq!(risk_pnl, SignedLamports::ZERO);

        let mut cfg = RiskConfig::default();
        cfg.shadow_drawdown_24h_stop_sol = -0.5;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_1h_stop_sol = -999.0;
        cfg.shadow_rug_loss_count_threshold = u64::MAX;
        cfg.shadow_rug_loss_rate_threshold = 1.0;
        let mut guard = ShadowRiskGuard::new(cfg);
        match guard.can_open_buy(&store, now + chrono::Duration::seconds(1), true) {
            BuyRiskDecision::Allow => {}
            other => {
                panic!("recovery zero-price stale close should not trip hard stop, got {other:?}")
            }
        }

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn validate_execution_risk_contract_rejects_invalid_stale_close_terminal_zero_price_hours() {
        let mut risk = RiskConfig::default();
        risk.max_hold_hours = 6;
        risk.shadow_stale_close_terminal_zero_price_hours = 11;

        let error = validate_execution_risk_contract(&risk)
            .expect_err("expected stale close terminal zero threshold validation to fail");

        assert!(
            error
                .to_string()
                .contains("risk.shadow_stale_close_terminal_zero_price_hours"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn validate_execution_risk_contract_rejects_stale_close_recovery_without_max_hold_hours() {
        let mut risk = RiskConfig::default();
        risk.max_hold_hours = 0;
        risk.shadow_stale_close_recovery_zero_price_enabled = true;

        let error = validate_execution_risk_contract(&risk)
            .expect_err("expected stale close recovery validation to fail");

        assert!(
            error
                .to_string()
                .contains("risk.shadow_stale_close_recovery_zero_price_enabled"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn validate_execution_risk_contract_rejects_stale_close_recovery_without_dead_zone() {
        let mut risk = RiskConfig::default();
        risk.max_hold_hours = 6;
        risk.shadow_stale_close_terminal_zero_price_hours = 0;
        risk.shadow_stale_close_recovery_zero_price_enabled = true;

        let error = validate_execution_risk_contract(&risk)
            .expect_err("expected stale close recovery dead-zone validation to fail");

        assert!(
            error
                .to_string()
                .contains("risk.shadow_stale_close_terminal_zero_price_hours"),
            "unexpected error: {error}"
        );
    }
