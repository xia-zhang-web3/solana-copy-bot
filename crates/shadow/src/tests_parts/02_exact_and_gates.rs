use super::*;

#[test]
fn buy_uses_temporal_follow_membership_when_runtime_set_is_stale() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("shadow-temporal-follow.db");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    store.run_migrations(&migration_dir)?;

    let mut follow = follow_snapshot(&[]);

    let mut cfg = ShadowConfig::default();
    cfg.copy_notional_sol = 0.5;
    cfg.min_leader_notional_sol = 0.25;
    cfg.quality_gates_enabled = false;
    let service = ShadowService::new(cfg);

    let buy_ts = DateTime::parse_from_rfc3339("2026-02-12T12:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    follow
        .promoted_at
        .insert("leader-wallet".to_string(), buy_ts - Duration::seconds(30));

    let buy = SwapEvent {
        wallet: "leader-wallet".to_string(),
        dex: "pumpswap".to_string(),
        token_in: SOL_MINT.to_string(),
        token_out: "TokenMint".to_string(),
        amount_in: 1.0,
        amount_out: 1000.0,
        signature: "sig-buy-temporal-follow".to_string(),
        slot: 22,
        ts_utc: buy_ts,
        exact_amounts: None,
    };

    let outcome = service.process_swap(&store, &buy, &follow, buy_ts + Duration::seconds(1))?;
    outcome.expect_recorded("buy should pass with temporal follow membership");
    Ok(())
}

#[test]
fn process_swap_preserves_exact_shadow_quantities() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("shadow-exact-qty.db");
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
        signature: "sig-buy-exact".to_string(),
        slot: 90,
        ts_utc: buy_ts,
        exact_amounts: Some(ExactSwapAmounts {
            amount_in_raw: "1000000000".to_string(),
            amount_in_decimals: 9,
            amount_out_raw: "1000000000".to_string(),
            amount_out_decimals: 6,
        }),
    };
    service
        .process_swap(&store, &buy, &follow, buy_ts + Duration::seconds(1))?
        .expect_recorded("buy signal expected");

    let lots = store.list_shadow_lots("leader-wallet", "TokenMint")?;
    assert_eq!(lots.len(), 1);
    assert_eq!(lots[0].accounting_bucket, "exact_post_cutover");
    assert_eq!(lots[0].qty_exact, Some(TokenQuantity::new(500_000_000, 6)));

    let sell = SwapEvent {
        wallet: "leader-wallet".to_string(),
        dex: "pumpswap".to_string(),
        token_in: "TokenMint".to_string(),
        token_out: SOL_MINT.to_string(),
        amount_in: 1000.0,
        amount_out: 1.2,
        signature: "sig-sell-exact".to_string(),
        slot: 91,
        ts_utc: sell_ts,
        exact_amounts: Some(ExactSwapAmounts {
            amount_in_raw: "500000000".to_string(),
            amount_in_decimals: 6,
            amount_out_raw: "600000000".to_string(),
            amount_out_decimals: 9,
        }),
    };
    service
        .process_swap(&store, &sell, &follow, sell_ts + Duration::seconds(1))?
        .expect_recorded("sell signal expected");

    let lots = store.list_shadow_lots("leader-wallet", "TokenMint")?;
    assert_eq!(lots.len(), 1);
    assert_eq!(lots[0].accounting_bucket, "exact_post_cutover");
    assert_eq!(lots[0].qty_exact, Some(TokenQuantity::new(83_333_334, 6)));

    let closed_bucket = store.shadow_closed_trade_accounting_bucket(
        "shadow:sig-sell-exact:leader-wallet:sell:TokenMint",
    )?;
    assert_eq!(closed_bucket.as_deref(), Some("exact_post_cutover"));
    let closed_qty_exact = store
        .shadow_closed_trade_qty_exact("shadow:sig-sell-exact:leader-wallet:sell:TokenMint")?;
    assert_eq!(closed_qty_exact, Some(TokenQuantity::new(416_666_666, 6)));
    Ok(())
}

#[test]
fn drops_buy_when_exact_shadow_qty_truncates_to_zero_raw() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("shadow-buy-zero-raw-exact.db");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    store.run_migrations(&migration_dir)?;

    let follow = follow_snapshot(&["leader-wallet"]);

    let mut cfg = ShadowConfig::default();
    cfg.copy_notional_sol = 0.5;
    cfg.min_leader_notional_sol = 0.25;
    cfg.quality_gates_enabled = false;
    let service = ShadowService::new(cfg);

    let buy_ts = DateTime::parse_from_rfc3339("2026-03-10T09:00:00Z")
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
        amount_out: 0.000001,
        signature: "sig-buy-zero-raw-exact".to_string(),
        slot: 190,
        ts_utc: buy_ts,
        exact_amounts: Some(ExactSwapAmounts {
            amount_in_raw: "1000000000".to_string(),
            amount_in_decimals: 9,
            amount_out_raw: "1".to_string(),
            amount_out_decimals: 6,
        }),
    };

    let outcome = service.process_swap(&store, &buy, &follow, buy_ts + Duration::seconds(1))?;
    outcome.expect_dropped(
        ShadowDropReason::InvalidSizing,
        "zero-raw exact buy should fail closed",
    );
    assert!(
        store
            .list_shadow_lots("leader-wallet", "TokenMint")?
            .is_empty(),
        "zero-raw exact buy must not persist a shadow lot"
    );
    assert!(
        store
            .list_copy_signals_by_status("shadow_recorded", 10)?
            .is_empty(),
        "zero-raw exact buy must not persist a copy signal"
    );
    Ok(())
}

#[test]
fn drops_buy_when_leader_exact_qty_zero_raw_source_side() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("shadow-buy-leader-zero-raw.db");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    store.run_migrations(&migration_dir)?;

    let follow = follow_snapshot(&["leader-wallet"]);

    let mut cfg = ShadowConfig::default();
    cfg.copy_notional_sol = 0.5;
    cfg.min_leader_notional_sol = 0.25;
    cfg.quality_gates_enabled = false;
    let service = ShadowService::new(cfg);

    let buy_ts = DateTime::parse_from_rfc3339("2026-03-10T09:10:00Z")
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
        signature: "sig-buy-leader-zero-raw".to_string(),
        slot: 190,
        ts_utc: buy_ts,
        exact_amounts: Some(ExactSwapAmounts {
            amount_in_raw: "1000000000".to_string(),
            amount_in_decimals: 9,
            amount_out_raw: "0".to_string(),
            amount_out_decimals: 6,
        }),
    };

    let outcome = service.process_swap(&store, &buy, &follow, buy_ts + Duration::seconds(1))?;
    outcome.expect_dropped(
        ShadowDropReason::InvalidSizing,
        "leader zero-raw exact buy should fail closed",
    );
    assert!(
        store
            .list_shadow_lots("leader-wallet", "TokenMint")?
            .is_empty(),
        "leader zero-raw exact buy must not persist a shadow lot"
    );
    assert!(
        store
            .list_copy_signals_by_status("shadow_recorded", 10)?
            .is_empty(),
        "leader zero-raw exact buy must not persist a copy signal"
    );
    Ok(())
}

#[test]
fn drops_sell_when_exact_shadow_qty_truncates_to_zero_raw() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("shadow-sell-zero-raw-exact.db");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    store.run_migrations(&migration_dir)?;

    let follow = follow_snapshot(&["leader-wallet"]);

    let mut cfg = ShadowConfig::default();
    cfg.copy_notional_sol = 0.5;
    cfg.min_leader_notional_sol = 0.25;
    cfg.quality_gates_enabled = false;
    let service = ShadowService::new(cfg);

    let opened_ts = DateTime::parse_from_rfc3339("2026-03-10T09:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    let sell_ts = opened_ts + Duration::minutes(5);
    store.activate_follow_wallet(
        "leader-wallet",
        opened_ts - Duration::seconds(30),
        "test-seed-follow",
    )?;
    store.insert_shadow_lot_exact(
        "leader-wallet",
        "TokenMint",
        1.0,
        Some(TokenQuantity::new(1_000_000, 6)),
        0.50,
        opened_ts,
    )?;

    let sell = SwapEvent {
        wallet: "leader-wallet".to_string(),
        dex: "pumpswap".to_string(),
        token_in: "TokenMint".to_string(),
        token_out: SOL_MINT.to_string(),
        amount_in: 0.000001,
        amount_out: 1.2,
        signature: "sig-sell-zero-raw-exact".to_string(),
        slot: 191,
        ts_utc: sell_ts,
        exact_amounts: Some(ExactSwapAmounts {
            amount_in_raw: "1".to_string(),
            amount_in_decimals: 6,
            amount_out_raw: "1200000000".to_string(),
            amount_out_decimals: 9,
        }),
    };

    let outcome = service.process_swap(&store, &sell, &follow, sell_ts + Duration::seconds(1))?;
    outcome.expect_dropped(
        ShadowDropReason::InvalidSizing,
        "zero-raw exact sell should fail closed",
    );

    let lots = store.list_shadow_lots("leader-wallet", "TokenMint")?;
    assert_eq!(lots.len(), 1);
    assert_eq!(lots[0].qty_exact, Some(TokenQuantity::new(1_000_000, 6)));
    assert!(
        store
            .shadow_closed_trade_qty_exact(
                "shadow:sig-sell-zero-raw-exact:leader-wallet:sell:TokenMint"
            )?
            .is_none(),
        "zero-raw exact sell must not persist a closed trade"
    );
    assert!(
        store
            .list_copy_signals_by_status("shadow_recorded", 10)?
            .is_empty(),
        "zero-raw exact sell must not persist a copy signal"
    );
    Ok(())
}

#[test]
fn drops_buy_when_runtime_follow_set_is_stale_after_demotion() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("shadow-runtime-stale-demotion.db");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    store.run_migrations(&migration_dir)?;

    let mut follow = follow_snapshot(&["leader-wallet"]);

    let mut cfg = ShadowConfig::default();
    cfg.copy_notional_sol = 0.5;
    cfg.min_leader_notional_sol = 0.25;
    cfg.quality_gates_enabled = false;
    let service = ShadowService::new(cfg);

    let demoted_at = DateTime::parse_from_rfc3339("2026-02-12T11:59:30Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    let buy_ts = DateTime::parse_from_rfc3339("2026-02-12T12:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);

    follow
        .demoted_at
        .insert("leader-wallet".to_string(), demoted_at);

    let buy = SwapEvent {
        wallet: "leader-wallet".to_string(),
        dex: "pumpswap".to_string(),
        token_in: SOL_MINT.to_string(),
        token_out: "TokenMint".to_string(),
        amount_in: 1.0,
        amount_out: 1000.0,
        signature: "sig-buy-runtime-stale".to_string(),
        slot: 55,
        ts_utc: buy_ts,
        exact_amounts: None,
    };

    let outcome = service.process_swap(&store, &buy, &follow, buy_ts + Duration::seconds(1))?;
    outcome.expect_dropped(
        ShadowDropReason::NotFollowed,
        "buy should be dropped when runtime follow set is stale after demotion",
    );
    Ok(())
}

#[test]
fn drops_buy_when_token_is_too_new() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("shadow-quality-too-new.db");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    store.run_migrations(&migration_dir)?;

    let follow = follow_snapshot(&["leader-wallet"]);

    let mut cfg = ShadowConfig::default();
    cfg.copy_notional_sol = 0.5;
    cfg.min_leader_notional_sol = 0.25;
    cfg.min_token_age_seconds = 600;
    let service = ShadowService::new(cfg);

    let buy_ts = DateTime::parse_from_rfc3339("2026-02-12T12:00:00Z")
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
        signature: "sig-buy-too-new".to_string(),
        slot: 101,
        ts_utc: buy_ts,
        exact_amounts: None,
    };

    let outcome = service.process_swap(&store, &buy, &follow, buy_ts + Duration::seconds(1))?;
    outcome.expect_dropped(ShadowDropReason::TooNew, "buy should be dropped by age");
    Ok(())
}
