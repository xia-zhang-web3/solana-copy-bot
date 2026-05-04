    fn follow_snapshot(active_wallets: &[&str]) -> FollowSnapshot {
        FollowSnapshot::from_active_wallets(
            active_wallets
                .iter()
                .map(|wallet| wallet.to_string())
                .collect(),
        )
    }

    #[test]
    fn creates_shadow_signal_and_realized_pnl() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("shadow-test.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let follow = follow_snapshot(&["leader-wallet"]);

        let mut cfg = ShadowConfig::default();
        cfg.copy_notional_sol = 0.5;
        cfg.min_leader_notional_sol = 0.25;
        cfg.quality_gates_enabled = false;
        let service = ShadowService::new(cfg);

        let buy_ts = DateTime::parse_from_rfc3339("2026-02-12T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let sell_ts = DateTime::parse_from_rfc3339("2026-02-12T12:05:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        store.activate_follow_wallet(
            "leader-wallet",
            buy_ts - Duration::seconds(30),
            "test-seed-follow",
        )?;

        let buy = SwapEvent {
            wallet: "leader-wallet".to_string(),
            dex: "pumpswap".to_string(),
            token_in: SOL_MINT.to_string(),
            token_out: "TokenMint".to_string(),
            amount_in: 1.0,
            amount_out: 1000.0,
            signature: "sig-buy".to_string(),
            slot: 1,
            ts_utc: buy_ts,
            exact_amounts: None,
        };
        let buy_signal = service
            .process_swap(&store, &buy, &follow, buy_ts + Duration::seconds(1))?
            .expect_recorded("buy signal expected");
        assert_eq!(buy_signal.side, "buy");
        let signals = store.list_copy_signals_by_status("shadow_recorded", 10)?;
        assert_eq!(signals.len(), 1);
        assert_eq!(
            signals[0].notional_lamports,
            Some(Lamports::new(500_000_000))
        );
        assert_eq!(
            signals[0].notional_origin,
            COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE
        );
        assert!(store.shadow_open_lots_count()? > 0);

        let sell = SwapEvent {
            wallet: "leader-wallet".to_string(),
            dex: "pumpswap".to_string(),
            token_in: "TokenMint".to_string(),
            token_out: SOL_MINT.to_string(),
            amount_in: 1000.0,
            amount_out: 1.2,
            signature: "sig-sell".to_string(),
            slot: 2,
            ts_utc: sell_ts,
            exact_amounts: None,
        };
        let sell_signal = service
            .process_swap(&store, &sell, &follow, sell_ts + Duration::seconds(1))?
            .expect_recorded("sell signal expected");
        assert_eq!(sell_signal.side, "sell");
        assert!(sell_signal.realized_pnl_sol > 0.0);

        let snapshot = service.snapshot_24h(&store, sell_ts + Duration::seconds(2))?;
        assert!(snapshot.closed_trades_24h >= 1);
        assert!(snapshot.realized_pnl_sol_24h > 0.0);
        Ok(())
    }

    #[test]
    fn sell_closes_existing_lot_even_if_wallet_demoted() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("shadow-unfollowed-exit.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let mut follow = follow_snapshot(&["leader-wallet"]);

        let mut cfg = ShadowConfig::default();
        cfg.copy_notional_sol = 0.5;
        cfg.min_leader_notional_sol = 0.25;
        cfg.quality_gates_enabled = false;
        let service = ShadowService::new(cfg);

        let buy_ts = DateTime::parse_from_rfc3339("2026-02-12T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let sell_ts = DateTime::parse_from_rfc3339("2026-02-12T12:05:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        store.activate_follow_wallet(
            "leader-wallet",
            buy_ts - Duration::seconds(30),
            "test-seed-follow",
        )?;

        let buy = SwapEvent {
            wallet: "leader-wallet".to_string(),
            dex: "pumpswap".to_string(),
            token_in: SOL_MINT.to_string(),
            token_out: "TokenMint".to_string(),
            amount_in: 1.0,
            amount_out: 1000.0,
            signature: "sig-buy-demote".to_string(),
            slot: 10,
            ts_utc: buy_ts,
            exact_amounts: None,
        };
        service
            .process_swap(&store, &buy, &follow, buy_ts + Duration::seconds(1))?
            .expect_recorded("buy signal expected");
        assert_eq!(store.shadow_open_lots_count()?, 1);

        // Simulate a discovery demotion: wallet is no longer in active followlist.
        follow.active.clear();
        follow
            .demoted_at
            .insert("leader-wallet".to_string(), sell_ts - Duration::seconds(30));
        store.deactivate_follow_wallet(
            "leader-wallet",
            sell_ts - Duration::seconds(30),
            "test-demote",
        )?;

        let sell = SwapEvent {
            wallet: "leader-wallet".to_string(),
            dex: "pumpswap".to_string(),
            token_in: "TokenMint".to_string(),
            token_out: SOL_MINT.to_string(),
            amount_in: 1000.0,
            amount_out: 1.0,
            signature: "sig-sell-demote".to_string(),
            slot: 11,
            ts_utc: sell_ts,
            exact_amounts: None,
        };
        let sell_signal = service
            .process_swap(&store, &sell, &follow, sell_ts + Duration::seconds(1))?
            .expect_recorded("sell signal should close orphaned lot");
        assert_eq!(sell_signal.side, "sell");
        assert_eq!(store.shadow_open_lots_count()?, 0);

        let snapshot = service.snapshot_24h(&store, sell_ts + Duration::seconds(2))?;
        assert!(snapshot.closed_trades_24h >= 1);
        Ok(())
    }

    #[test]
    fn sell_does_not_treat_dust_lot_as_unfollowed_exit() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("shadow-unfollowed-dust-exit.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let mut follow = follow_snapshot(&["leader-wallet"]);

        let mut cfg = ShadowConfig::default();
        cfg.copy_notional_sol = 0.5;
        cfg.min_leader_notional_sol = 0.25;
        cfg.quality_gates_enabled = false;
        let service = ShadowService::new(cfg);

        let buy_ts = DateTime::parse_from_rfc3339("2026-02-12T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let sell_ts = DateTime::parse_from_rfc3339("2026-02-12T12:05:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        store.activate_follow_wallet(
            "leader-wallet",
            buy_ts - Duration::seconds(30),
            "test-seed-follow",
        )?;

        let buy = SwapEvent {
            wallet: "leader-wallet".to_string(),
            dex: "pumpswap".to_string(),
            token_in: SOL_MINT.to_string(),
            token_out: "TokenMint".to_string(),
            amount_in: 1.0,
            amount_out: 1000.0,
            signature: "sig-buy-dust-demote".to_string(),
            slot: 20,
            ts_utc: buy_ts,
            exact_amounts: None,
        };
        service
            .process_swap(&store, &buy, &follow, buy_ts + Duration::seconds(1))?
            .expect_recorded("buy signal expected");
        let signals = store.list_copy_signals_by_status("shadow_recorded", 10)?;
        assert_eq!(signals.len(), 1);
        assert_eq!(
            signals[0].notional_lamports,
            Some(Lamports::new(500_000_000))
        );
        assert_eq!(
            signals[0].notional_origin,
            COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE
        );

        let lots = store.list_shadow_lots("leader-wallet", "TokenMint")?;
        assert_eq!(lots.len(), 1, "expected single open lot before dusting");
        store.update_shadow_lot(lots[0].id, 1e-13, 1e-15)?;
        assert!(
            !store.has_shadow_lots("leader-wallet", "TokenMint")?,
            "dust lot should not count as open inventory"
        );
        let dust_snapshot = service.snapshot_24h(&store, sell_ts - Duration::seconds(1))?;
        assert_eq!(
            dust_snapshot.open_lots, 0,
            "dust lot should not appear in shadow snapshot open lots"
        );

        follow.active.clear();
        follow
            .demoted_at
            .insert("leader-wallet".to_string(), sell_ts - Duration::seconds(30));
        store.deactivate_follow_wallet(
            "leader-wallet",
            sell_ts - Duration::seconds(30),
            "test-demote",
        )?;

        let sell = SwapEvent {
            wallet: "leader-wallet".to_string(),
            dex: "pumpswap".to_string(),
            token_in: "TokenMint".to_string(),
            token_out: SOL_MINT.to_string(),
            amount_in: 1000.0,
            amount_out: 1.0,
            signature: "sig-sell-dust-demote".to_string(),
            slot: 21,
            ts_utc: sell_ts,
            exact_amounts: None,
        };
        let outcome =
            service.process_swap(&store, &sell, &follow, sell_ts + Duration::seconds(1))?;
        outcome.expect_dropped(
            ShadowDropReason::NotFollowed,
            "dust lot should not unlock unfollowed sell exit",
        );
        Ok(())
    }
