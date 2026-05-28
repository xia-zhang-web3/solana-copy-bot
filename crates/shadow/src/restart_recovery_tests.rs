use super::outcome_ext::*;
use super::*;

fn follow_snapshot(active_wallets: &[&str]) -> FollowSnapshot {
    FollowSnapshot::from_active_wallets(
        active_wallets
            .iter()
            .map(|wallet| wallet.to_string())
            .collect(),
    )
}

#[test]
fn restart_recovery_sell_closes_open_lot_even_after_lag_window() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("shadow-restart-recovery-sell.db");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    store.run_migrations(&migration_dir)?;

    let follow = follow_snapshot(&["leader-wallet"]);

    let mut cfg = ShadowConfig::default();
    cfg.copy_notional_sol = 0.5;
    cfg.min_leader_notional_sol = 0.25;
    cfg.max_signal_lag_seconds = 30;
    cfg.quality_gates_enabled = false;
    let service = ShadowService::new(cfg);

    let buy_ts = DateTime::parse_from_rfc3339("2026-02-12T12:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    let sell_ts = DateTime::parse_from_rfc3339("2026-02-12T12:01:00Z")
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
        signature: "sig-recovery-buy".to_string(),
        slot: 100,
        ts_utc: buy_ts,
        exact_amounts: None,
    };
    service
        .process_swap(&store, &buy, &follow, buy_ts + Duration::seconds(1))?
        .expect_recorded("buy signal expected");
    assert_eq!(store.shadow_open_lots_count()?, 1);

    let sell = SwapEvent {
        wallet: "leader-wallet".to_string(),
        dex: "pumpswap".to_string(),
        token_in: "TokenMint".to_string(),
        token_out: SOL_MINT.to_string(),
        amount_in: 1000.0,
        amount_out: 1.0,
        signature: "sig-recovery-sell".to_string(),
        slot: 101,
        ts_utc: sell_ts,
        exact_amounts: None,
    };
    service
        .process_swap(&store, &sell, &follow, sell_ts + Duration::minutes(10))?
        .expect_dropped(
            ShadowDropReason::LagExceeded,
            "normal live path should still enforce lag",
        );

    let recovered = service
        .process_restart_recovery_sell(&store, &sell, sell_ts + Duration::minutes(10))?
        .expect_recorded("restart recovery should bypass lag for open-lot sell exits");
    assert_eq!(recovered.side, "sell");
    assert!(recovered.closed_qty > 0.0);
    assert_eq!(store.shadow_open_lots_count()?, 0);
    Ok(())
}

#[test]
fn restart_recovery_sell_closes_when_signal_was_inserted_before_prior_crash() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp
        .path()
        .join("shadow-restart-recovery-duplicate-signal.db");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    store.run_migrations(&migration_dir)?;

    let mut cfg = ShadowConfig::default();
    cfg.copy_notional_sol = 0.5;
    cfg.min_leader_notional_sol = 0.25;
    cfg.quality_gates_enabled = false;
    let service = ShadowService::new(cfg);

    let buy_ts = DateTime::parse_from_rfc3339("2026-02-12T12:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    let sell_ts = DateTime::parse_from_rfc3339("2026-02-12T12:01:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    store.insert_shadow_lot("leader-wallet", "TokenMint", 500.0, 0.5, buy_ts)?;

    let sell = SwapEvent {
        wallet: "leader-wallet".to_string(),
        dex: "pumpswap".to_string(),
        token_in: "TokenMint".to_string(),
        token_out: SOL_MINT.to_string(),
        amount_in: 1000.0,
        amount_out: 1.0,
        signature: "sig-recovery-crash-sell".to_string(),
        slot: 201,
        ts_utc: sell_ts,
        exact_amounts: None,
    };
    let signal_id = format!(
        "shadow:{}:{}:{}:{}",
        sell.signature, sell.wallet, "sell", "TokenMint"
    );
    store.insert_copy_signal(&CopySignalRow {
        signal_id,
        wallet_id: sell.wallet.clone(),
        side: "sell".to_string(),
        token: "TokenMint".to_string(),
        notional_sol: 0.5,
        notional_lamports: Some(Lamports::new(500_000_000)),
        notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
        ts: sell.ts_utc,
        status: "shadow_recorded".to_string(),
    })?;

    service
        .process_restart_recovery_sell(&store, &sell, sell_ts + Duration::seconds(1))?
        .expect_recorded("recovery should close even when copy signal already exists");
    assert_eq!(store.shadow_open_lots_count()?, 0);
    Ok(())
}
