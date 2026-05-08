    #[test]
    fn stale_lot_cleanup_ignores_micro_swap_outlier_price() -> Result<()> {
        let (store, db_path) = make_test_store("stale-lot-cleanup")?;
        let now = Utc::now();
        let opened_ts = now - chrono::Duration::hours(10);

        store.insert_observed_swap(&SwapEvent {
            wallet: "leader-wallet".to_string(),
            dex: "pumpswap".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-a".to_string(),
            amount_in: 0.9,
            amount_out: 900.0,
            signature: "sig-price-1".to_string(),
            slot: 1,
            ts_utc: now - chrono::Duration::minutes(20),
            exact_amounts: None,
        })?;
        store.insert_observed_swap(&SwapEvent {
            wallet: "leader-wallet".to_string(),
            dex: "pumpswap".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-a".to_string(),
            amount_in: 1.0,
            amount_out: 1000.0,
            signature: "sig-price-2".to_string(),
            slot: 2,
            ts_utc: now - chrono::Duration::minutes(12),
            exact_amounts: None,
        })?;
        store.insert_observed_swap(&SwapEvent {
            wallet: "leader-wallet".to_string(),
            dex: "pumpswap".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-a".to_string(),
            amount_in: 1.1,
            amount_out: 1100.0,
            signature: "sig-price-3".to_string(),
            slot: 3,
            ts_utc: now - chrono::Duration::minutes(7),
            exact_amounts: None,
        })?;
        store.insert_observed_swap(&SwapEvent {
            wallet: "leader-wallet".to_string(),
            dex: "pumpswap".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-a".to_string(),
            amount_in: 0.001,
            amount_out: 0.000001,
            signature: "sig-price-outlier".to_string(),
            slot: 4,
            ts_utc: now - chrono::Duration::minutes(1),
            exact_amounts: None,
        })?;
        store.insert_shadow_lot("wallet-a", "token-a", 500.0, 0.25, opened_ts)?;

        let mut open_pairs = store.list_shadow_open_pairs()?;
        let stats = close_stale_shadow_lots(&store, &mut open_pairs, 8, 0, false, now)?;

        assert_eq!(stats.closed_priced, 1);
        assert_eq!(stats.terminal_zero_closed, 0);
        assert_eq!(stats.skipped_unpriced, 0);
        assert!(!store.has_shadow_lots("wallet-a", "token-a")?);
        assert!(!open_pairs.contains(&("wallet-a".to_string(), "token-a".to_string())));

        let (trades, pnl) = store.shadow_realized_pnl_since(now - chrono::Duration::days(1))?;
        assert_eq!(trades, 1);
        assert!(pnl > 0.0, "expected positive pnl after stale cleanup close");
        assert!(
            pnl < 2.0,
            "stale-close pnl must stay in realistic band and ignore micro-swap outlier (got {})",
            pnl
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn stale_lot_cleanup_skips_and_records_risk_event_when_reliable_price_missing() -> Result<()> {
        let (store, db_path) = make_test_store("stale-lot-unpriced")?;
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
            signature: "sig-only-one-sample".to_string(),
            slot: 1,
            ts_utc: now - chrono::Duration::minutes(5),
            exact_amounts: None,
        })?;
        store.insert_shadow_lot("wallet-a", "token-a", 500.0, 0.25, opened_ts)?;

        let mut open_pairs = store.list_shadow_open_pairs()?;
        let stats = close_stale_shadow_lots(&store, &mut open_pairs, 8, 0, false, now)?;

        assert_eq!(stats.closed_priced, 0);
        assert_eq!(stats.terminal_zero_closed, 0);
        assert_eq!(stats.skipped_unpriced, 1);
        assert!(store.has_shadow_lots("wallet-a", "token-a")?);
        assert!(open_pairs.contains(&("wallet-a".to_string(), "token-a".to_string())));
        assert_eq!(
            store.risk_event_count_by_type("shadow_stale_close_price_unavailable")?,
            1
        );
        assert_eq!(
            store.risk_event_count_by_type("shadow_stale_close_terminal_zero_price")?,
            0
        );
        let (trades, _) = store.shadow_realized_pnl_since(now - chrono::Duration::days(1))?;
        assert_eq!(trades, 0);

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn stale_lot_cleanup_returns_error_on_fatal_price_unavailable_risk_event_write() -> Result<()> {
        let (store, db_path) = make_test_store("stale-lot-unpriced-risk-event-fatal")?;
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
            signature: "sig-only-one-sample-risk-event-fatal".to_string(),
            slot: 1,
            ts_utc: now - chrono::Duration::minutes(5),
            exact_amounts: None,
        })?;
        store.insert_shadow_lot("wallet-a", "token-a", 500.0, 0.25, opened_ts)?;

        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute_batch(
            "CREATE TRIGGER fail_stale_close_risk_event_insert
             BEFORE INSERT ON risk_events
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let mut open_pairs = store.list_shadow_open_pairs()?;
        let error = close_stale_shadow_lots(&store, &mut open_pairs, 8, 0, false, now)
            .expect_err("fatal stale-close risk event write must abort cleanup");
        let error_text = format!("{error:#}");
        assert!(
            error_text.contains(
                "failed to record stale-close price unavailable risk event with fatal sqlite I/O"
            ),
            "expected stale-close fatal risk-event context, got: {error_text}"
        );
        assert!(
            error_text.contains("failed to insert risk event"),
            "expected storage insert_risk_event context, got: {error_text}"
        );
        assert!(
            error_text.contains("xShmMap"),
            "expected fatal sqlite marker to survive error chain, got: {error_text}"
        );
        assert!(store.has_shadow_lots("wallet-a", "token-a")?);
        assert!(open_pairs.contains(&("wallet-a".to_string(), "token-a".to_string())));

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn stale_lot_cleanup_recovery_zero_price_closes_unpriced_lot_only_when_enabled() -> Result<()> {
        let (store, db_path) = make_test_store("stale-lot-recovery-unpriced")?;
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
            signature: "sig-only-one-sample-recovery".to_string(),
            slot: 1,
            ts_utc: now - chrono::Duration::minutes(5),
            exact_amounts: None,
        })?;
        let lot_id = store.insert_shadow_lot("wallet-a", "token-a", 500.0, 0.25, opened_ts)?;

        let mut open_pairs = store.list_shadow_open_pairs()?;
        let stats = close_stale_shadow_lots(&store, &mut open_pairs, 6, 12, true, now)?;

        assert_eq!(stats.closed_priced, 0);
        assert_eq!(stats.recovery_zero_closed, 1);
        assert_eq!(stats.terminal_zero_closed, 0);
        assert_eq!(stats.skipped_unpriced, 0);
        assert!(!store.has_shadow_lots("wallet-a", "token-a")?);
        assert!(!open_pairs.contains(&("wallet-a".to_string(), "token-a".to_string())));
        assert_eq!(
            store.risk_event_count_by_type("shadow_stale_close_price_unavailable")?,
            1
        );
        assert_eq!(
            store.risk_event_count_by_type("shadow_stale_close_recovery_zero_price")?,
            1
        );
        assert_eq!(
            store.risk_event_count_by_type("shadow_stale_close_terminal_zero_price")?,
            0
        );
        let signal_id = format!("stale-close-{}-{}", lot_id, now.timestamp_millis());
        assert_eq!(
            store.shadow_closed_trade_close_context(&signal_id)?,
            Some(
                copybot_storage_core::SHADOW_CLOSE_CONTEXT_RECOVERY_TERMINAL_ZERO_PRICE.to_string()
            )
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn stale_lot_cleanup_terminal_closes_and_records_risk_events_when_reliable_price_missing_after_terminal_threshold(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("stale-lot-terminal-unpriced")?;
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
            signature: "sig-only-one-sample-terminal".to_string(),
            slot: 1,
            ts_utc: now - chrono::Duration::minutes(5),
            exact_amounts: None,
        })?;
        store.insert_shadow_lot("wallet-a", "token-a", 500.0, 0.25, opened_ts)?;

        let mut open_pairs = store.list_shadow_open_pairs()?;
        let stats = close_stale_shadow_lots(&store, &mut open_pairs, 6, 12, false, now)?;

        assert_eq!(stats.closed_priced, 0);
        assert_eq!(stats.terminal_zero_closed, 1);
        assert_eq!(stats.skipped_unpriced, 0);
        assert!(!store.has_shadow_lots("wallet-a", "token-a")?);
        assert!(!open_pairs.contains(&("wallet-a".to_string(), "token-a".to_string())));
        assert_eq!(
            store.risk_event_count_by_type("shadow_stale_close_price_unavailable")?,
            1
        );
        assert_eq!(
            store.risk_event_count_by_type("shadow_stale_close_terminal_zero_price")?,
            1
        );
        let (trades, pnl) = store.shadow_realized_pnl_since(now - chrono::Duration::days(1))?;
        assert_eq!(trades, 1);
        assert!(
            pnl < 0.0,
            "terminal zero-price stale close must realize a loss"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }
