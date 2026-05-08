use super::*;

#[test]
fn shadow_open_notional_sol_prefers_cost_lamports_sidecar() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("shadow-cost-lamports-preference.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    let opened_ts = DateTime::parse_from_rfc3339("2026-02-15T10:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    store.conn.execute(
        "INSERT INTO shadow_lots(wallet_id, token, qty, cost_sol, cost_lamports, opened_ts)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            "wallet",
            "token",
            10.0_f64,
            0.100000001_f64,
            100_000_123_i64,
            opened_ts.to_rfc3339()
        ],
    )?;

    let lots = store.list_shadow_lots("wallet", "token")?;
    assert_eq!(lots.len(), 1);
    assert_eq!(lots[0].risk_context, SHADOW_RISK_CONTEXT_MARKET);
    assert_eq!(lots[0].cost_lamports, Some(Lamports::new(100_000_123)));

    let open_notional_lamports = store.shadow_open_notional_lamports()?;
    assert_eq!(open_notional_lamports, Lamports::new(100_000_123));

    let open_notional = store.shadow_open_notional_sol()?;
    assert!(
        (open_notional - 0.100000123).abs() < 1e-12,
        "expected shadow open notional to prefer lamport sidecar, got {open_notional}"
    );
    Ok(())
}

#[test]
fn shadow_risk_open_notional_sol_excludes_quarantined_legacy_context() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("shadow-risk-open-notional-context.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    let opened_ts = DateTime::parse_from_rfc3339("2026-03-01T10:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    let market_lot_id = store.insert_shadow_lot("wallet", "token-market", 10.0, 0.20, opened_ts)?;
    let quarantined_lot_id =
        store.insert_shadow_lot("wallet", "token-quarantine", 10.0, 0.30, opened_ts)?;
    store.update_shadow_lot_risk_context(
        quarantined_lot_id,
        SHADOW_RISK_CONTEXT_QUARANTINED_LEGACY,
    )?;

    let lots = store.list_shadow_lots("wallet", "token-quarantine")?;
    assert_eq!(lots.len(), 1);
    assert_eq!(lots[0].risk_context, SHADOW_RISK_CONTEXT_QUARANTINED_LEGACY);

    assert_eq!(
        store.shadow_open_notional_lamports()?,
        Lamports::new(500_000_000)
    );
    assert_eq!(
        store.shadow_risk_open_notional_lamports()?,
        Lamports::new(200_000_000)
    );
    assert!((store.shadow_open_notional_sol()? - 0.50).abs() < 1e-12);
    assert!((store.shadow_risk_open_notional_sol()? - 0.20).abs() < 1e-12);

    store.update_shadow_lot_risk_context(market_lot_id, SHADOW_RISK_CONTEXT_QUARANTINED_LEGACY)?;
    assert_eq!(store.shadow_risk_open_notional_lamports()?, Lamports::ZERO);
    Ok(())
}

#[test]
fn shadow_lot_risk_context_rejects_unknown_value() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("shadow-risk-context-validation.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    let opened_ts = DateTime::parse_from_rfc3339("2026-03-01T10:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);

    let error = store
        .insert_shadow_lot_exact_with_risk_context(
            "wallet",
            "token",
            1.0,
            None,
            0.10,
            "typo_quarantine",
            opened_ts,
        )
        .expect_err("unknown risk_context must reject");
    assert!(error
        .to_string()
        .contains("unsupported shadow risk_context"));

    let lot_id = store.insert_shadow_lot("wallet", "token", 1.0, 0.10, opened_ts)?;
    let error = store
        .update_shadow_lot_risk_context(lot_id, "typo_quarantine")
        .expect_err("updating to unknown risk_context must reject");
    assert!(error
        .to_string()
        .contains("unsupported shadow risk_context"));
    assert_eq!(
        store.list_shadow_lots("wallet", "token")?[0].risk_context,
        SHADOW_RISK_CONTEXT_MARKET
    );
    Ok(())
}
