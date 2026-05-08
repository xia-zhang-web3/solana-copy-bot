use super::*;

#[test]
fn shadow_risk_metrics_prefer_closed_trade_lamport_sidecars() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("shadow-closed-trade-lamports.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    let opened_ts = DateTime::parse_from_rfc3339("2026-03-01T10:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    let closed_ts = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);

    store.conn.execute(
        "INSERT INTO shadow_closed_trades(
                signal_id, wallet_id, token, qty,
                entry_cost_sol, entry_cost_lamports,
                exit_value_sol, exit_value_lamports,
                pnl_sol, pnl_lamports,
                opened_ts, closed_ts
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
        params![
            "sig-shadow",
            "wallet",
            "token",
            10.0_f64,
            0.10_f64,
            200_000_000_i64,
            0.05_f64,
            50_000_000_i64,
            -0.05_f64,
            -150_000_000_i64,
            opened_ts.to_rfc3339(),
            closed_ts.to_rfc3339()
        ],
    )?;

    let (trades, pnl_lamports) =
        store.shadow_realized_pnl_lamports_since(opened_ts - Duration::minutes(1))?;
    assert_eq!(trades, 1);
    assert_eq!(pnl_lamports, SignedLamports::new(-150_000_000));

    let (trades, pnl) = store.shadow_realized_pnl_since(opened_ts - Duration::minutes(1))?;
    assert_eq!(trades, 1);
    assert!(
        (pnl + 0.15).abs() < 1e-12,
        "expected realized pnl to prefer lamport sidecar, got {pnl}"
    );

    let rug_count = store.shadow_rug_loss_count_since(opened_ts - Duration::minutes(1), -0.70)?;
    assert_eq!(
        rug_count, 1,
        "expected rug-loss count to prefer exact lamport sidecars"
    );

    let (recent_rug_count, total_count, rug_rate) =
        store.shadow_rug_loss_rate_recent(opened_ts - Duration::minutes(1), 10, -0.70)?;
    assert_eq!(recent_rug_count, 1);
    assert_eq!(total_count, 1);
    assert!((rug_rate - 1.0).abs() < 1e-12);
    Ok(())
}

#[test]
fn shadow_risk_metrics_ignore_stale_terminal_zero_close_context() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("shadow-risk-ignore-terminal-zero.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    let opened_ts = DateTime::parse_from_rfc3339("2026-03-01T10:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    let closed_ts = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);

    store.insert_shadow_closed_trade_exact_with_context(
        "sig-terminal-zero",
        "wallet",
        "token",
        10.0,
        None,
        0.10,
        0.0,
        -0.10,
        SHADOW_CLOSE_CONTEXT_STALE_TERMINAL_ZERO_PRICE,
        opened_ts,
        closed_ts,
    )?;

    let (all_trades, all_pnl) =
        store.shadow_realized_pnl_lamports_since(opened_ts - Duration::minutes(1))?;
    assert_eq!(all_trades, 1);
    assert_eq!(all_pnl, SignedLamports::new(-100_000_000));

    let (risk_trades, risk_pnl) =
        store.shadow_risk_realized_pnl_lamports_since(opened_ts - Duration::minutes(1))?;
    assert_eq!(risk_trades, 0);
    assert_eq!(risk_pnl, SignedLamports::ZERO);

    assert_eq!(
        store.shadow_rug_loss_count_since(opened_ts - Duration::minutes(1), -0.70)?,
        0
    );
    let (recent_rug_count, total_count, rug_rate) =
        store.shadow_rug_loss_rate_recent(opened_ts - Duration::minutes(1), 10, -0.70)?;
    assert_eq!(recent_rug_count, 0);
    assert_eq!(total_count, 0);
    assert_eq!(rug_rate, 0.0);
    Ok(())
}

#[test]
fn shadow_risk_metrics_ignore_recovery_terminal_zero_close_context() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("shadow-risk-ignore-recovery-zero.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    let opened_ts = DateTime::parse_from_rfc3339("2026-03-01T10:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    let closed_ts = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);

    store.insert_shadow_closed_trade_exact_with_context(
        "sig-recovery-zero",
        "wallet",
        "token",
        10.0,
        None,
        0.10,
        0.0,
        -0.10,
        SHADOW_CLOSE_CONTEXT_RECOVERY_TERMINAL_ZERO_PRICE,
        opened_ts,
        closed_ts,
    )?;

    let (all_trades, all_pnl) =
        store.shadow_realized_pnl_lamports_since(opened_ts - Duration::minutes(1))?;
    assert_eq!(all_trades, 1);
    assert_eq!(all_pnl, SignedLamports::new(-100_000_000));

    let (risk_trades, risk_pnl) =
        store.shadow_risk_realized_pnl_lamports_since(opened_ts - Duration::minutes(1))?;
    assert_eq!(risk_trades, 0);
    assert_eq!(risk_pnl, SignedLamports::ZERO);

    assert_eq!(
        store.shadow_rug_loss_count_since(opened_ts - Duration::minutes(1), -0.70)?,
        0
    );
    let (recent_rug_count, total_count, rug_rate) =
        store.shadow_rug_loss_rate_recent(opened_ts - Duration::minutes(1), 10, -0.70)?;
    assert_eq!(recent_rug_count, 0);
    assert_eq!(total_count, 0);
    assert_eq!(rug_rate, 0.0);
    Ok(())
}
