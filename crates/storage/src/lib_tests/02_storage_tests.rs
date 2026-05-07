use super::*;

#[test]
fn shadow_risk_metrics_ignore_quarantined_legacy_close_context_after_market_close() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("shadow-risk-ignore-quarantined-close.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    let opened_ts = DateTime::parse_from_rfc3339("2026-03-01T10:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    let closed_ts = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);

    let lot_id = store.insert_shadow_lot("wallet", "token", 10.0, 0.10, opened_ts)?;
    store.update_shadow_lot_risk_context(lot_id, SHADOW_RISK_CONTEXT_QUARANTINED_LEGACY)?;

    let close = store.close_shadow_lots_fifo_atomic(
        "sig-quarantined-close",
        "wallet",
        "token",
        10.0,
        0.0,
        closed_ts,
    )?;
    assert!((close.closed_qty - 10.0).abs() < 1e-12);
    assert_eq!(
        store.shadow_closed_trade_close_context("sig-quarantined-close")?,
        Some(SHADOW_CLOSE_CONTEXT_QUARANTINED_LEGACY.to_string())
    );

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
fn shadow_rug_loss_rate_recent_keeps_zero_entry_rows_in_sample_size() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("shadow-rug-rate-denominator.db");
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
            "sig-rug",
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
    store.conn.execute(
        "INSERT INTO shadow_closed_trades(
                signal_id, wallet_id, token, qty,
                entry_cost_sol, entry_cost_lamports,
                exit_value_sol, exit_value_lamports,
                pnl_sol, pnl_lamports,
                opened_ts, closed_ts
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
        params![
            "sig-zero-entry",
            "wallet",
            "token",
            1.0_f64,
            0.0_f64,
            0_i64,
            0.01_f64,
            10_000_000_i64,
            0.01_f64,
            10_000_000_i64,
            opened_ts.to_rfc3339(),
            (closed_ts + Duration::minutes(1)).to_rfc3339()
        ],
    )?;

    let (rug_count, total_count, rug_rate) =
        store.shadow_rug_loss_rate_recent(opened_ts - Duration::minutes(1), 10, -0.70)?;
    assert_eq!(
        rug_count, 1,
        "only the positive-entry trade should count as rug"
    );
    assert_eq!(
        total_count, 2,
        "zero-entry trades should still remain in the recent sample size"
    );
    assert!(
        (rug_rate - 0.5).abs() < 1e-12,
        "expected denominator to include both sampled rows, got {rug_rate}"
    );
    Ok(())
}

#[test]
fn copy_signal_roundtrip_preserves_exact_notional_lamports() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("copy-signal-exact-notional.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;
    let now = DateTime::parse_from_rfc3339("2026-03-08T12:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);

    let signal = CopySignalRow {
        signal_id: "shadow:sig-exact:wallet:buy:token-a".to_string(),
        wallet_id: "wallet-1".to_string(),
        side: "buy".to_string(),
        token: "token-a".to_string(),
        notional_sol: 0.25,
        notional_lamports: Some(Lamports::new(250_000_000)),
        notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
        ts: now,
        status: "shadow_recorded".to_string(),
    };
    assert!(store.insert_copy_signal(&signal)?);

    let signals = store.list_copy_signals_by_status("shadow_recorded", 10)?;
    assert_eq!(signals.len(), 1);
    assert_eq!(
        signals[0].notional_lamports,
        Some(Lamports::new(250_000_000))
    );
    let origin: String = store.conn.query_row(
        "SELECT notional_origin FROM copy_signals WHERE signal_id = ?1",
        params![signal.signal_id],
        |row| row.get(0),
    )?;
    assert_eq!(origin, "leader_exact_lamports");
    Ok(())
}

#[test]
fn copy_signal_approximate_origin_preserves_approximate_notional_sidecar_on_read() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("copy-signal-approx-origin.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;
    let now = DateTime::parse_from_rfc3339("2026-03-08T12:30:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);

    store.conn.execute(
            "INSERT INTO copy_signals(
                signal_id, wallet_id, side, token, notional_sol, notional_lamports, notional_origin, ts, status
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                "shadow:sig-approx:wallet:buy:token-a",
                "wallet-1",
                "buy",
                "token-a",
                0.25_f64,
                250_000_000_i64,
                "leader_approximate",
                now.to_rfc3339(),
                "shadow_recorded",
            ],
        )?;

    let signals = store.list_copy_signals_by_status("shadow_recorded", 10)?;
    assert_eq!(signals.len(), 1);
    assert_eq!(
        signals[0].notional_lamports,
        Some(Lamports::new(250_000_000))
    );
    assert_eq!(
        signals[0].notional_origin,
        COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE
    );
    Ok(())
}

#[test]
fn insert_copy_signal_rejects_exact_origin_without_notional_lamports() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("copy-signal-missing-exact-notional.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;
    let now = DateTime::parse_from_rfc3339("2026-03-08T12:45:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);

    let err = store
        .insert_copy_signal(&CopySignalRow {
            signal_id: "shadow:sig-missing-exact:wallet:buy:token-a".to_string(),
            wallet_id: "wallet-1".to_string(),
            side: "buy".to_string(),
            token: "token-a".to_string(),
            notional_sol: 0.25,
            notional_lamports: None,
            notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
            ts: now,
            status: "shadow_recorded".to_string(),
        })
        .expect_err("exact origin without lamport mirror must fail closed");
    assert!(
        err.to_string().contains("missing notional_lamports"),
        "unexpected error: {err}"
    );
    Ok(())
}

#[test]
fn insert_copy_signal_rejects_zero_notional_lamports() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("copy-signal-zero-notional.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;
    let now = DateTime::parse_from_rfc3339("2026-03-08T12:50:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);

    let err = store
        .insert_copy_signal(&CopySignalRow {
            signal_id: "shadow:sig-zero-notional:wallet:buy:token-a".to_string(),
            wallet_id: "wallet-1".to_string(),
            side: "buy".to_string(),
            token: "token-a".to_string(),
            notional_sol: 0.25,
            notional_lamports: Some(Lamports::ZERO),
            notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
            ts: now,
            status: "shadow_recorded".to_string(),
        })
        .expect_err("zero lamport mirror must fail closed");
    assert!(
        err.to_string().contains("zero notional_lamports"),
        "unexpected error: {err}"
    );
    Ok(())
}

#[test]
fn parse_non_negative_i64_rejects_negative_values() {
    let error = parse_non_negative_i64("orders.ata_create_rent_lamports", "ord-1", Some(-7))
        .expect_err("negative sqlite value must be rejected");
    assert!(
        error.to_string().contains("must be >= 0"),
        "unexpected error: {error}"
    );
}

#[test]
fn persist_discovery_cycle_keeps_only_latest_wallet_metric_windows() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("discovery-wallet-metrics-retention.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    let wallet_id = "wallet-retention".to_string();
    let base = DateTime::parse_from_rfc3339("2026-02-20T00:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);

    for offset_minutes in 0..4 {
        let window_start = base + Duration::minutes(offset_minutes);
        let wallets = vec![WalletUpsertRow {
            wallet_id: wallet_id.clone(),
            first_seen: base,
            last_seen: window_start,
            status: "active".to_string(),
        }];
        let metrics = vec![WalletMetricRow {
            wallet_id: wallet_id.clone(),
            window_start,
            pnl: 0.0,
            win_rate: 0.0,
            trades: 1,
            closed_trades: 1,
            hold_median_seconds: 0,
            score: 1.0,
            buy_total: 1,
            tradable_ratio: 1.0,
            rug_ratio: 0.0,
        }];
        let desired = vec![wallet_id.clone()];
        store.persist_discovery_cycle(
            &wallets,
            &metrics,
            &desired,
            true,
            true,
            window_start,
            "retention-test",
        )?;
    }

    let mut stmt = store
        .conn
        .prepare("SELECT DISTINCT window_start FROM wallet_metrics ORDER BY window_start ASC")?;
    let windows: Vec<String> = stmt
        .query_map([], |row| row.get(0))?
        .collect::<rusqlite::Result<Vec<String>>>()?;

    assert_eq!(
        windows.len(),
        3,
        "expected retention to keep 3 latest windows"
    );
    assert_eq!(windows[0], (base + Duration::minutes(1)).to_rfc3339());
    assert_eq!(windows[1], (base + Duration::minutes(2)).to_rfc3339());
    assert_eq!(windows[2], (base + Duration::minutes(3)).to_rfc3339());
    Ok(())
}

#[test]
fn persist_discovery_cycle_retention_keeps_cold_start_windows() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp
        .path()
        .join("discovery-wallet-metrics-cold-start-retention.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    let wallet_id = "wallet-cold-start".to_string();
    let base = DateTime::parse_from_rfc3339("2026-02-20T00:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);

    for offset_minutes in 0..2 {
        let window_start = base + Duration::minutes(offset_minutes);
        let wallets = vec![WalletUpsertRow {
            wallet_id: wallet_id.clone(),
            first_seen: base,
            last_seen: window_start,
            status: "active".to_string(),
        }];
        let metrics = vec![WalletMetricRow {
            wallet_id: wallet_id.clone(),
            window_start,
            pnl: 0.0,
            win_rate: 0.0,
            trades: 1,
            closed_trades: 1,
            hold_median_seconds: 0,
            score: 1.0,
            buy_total: 1,
            tradable_ratio: 1.0,
            rug_ratio: 0.0,
        }];
        let desired = vec![wallet_id.clone()];
        store.persist_discovery_cycle(
            &wallets,
            &metrics,
            &desired,
            true,
            true,
            window_start,
            "cold-start-retention-test",
        )?;
    }

    let mut stmt = store
        .conn
        .prepare("SELECT DISTINCT window_start FROM wallet_metrics ORDER BY window_start ASC")?;
    let windows: Vec<String> = stmt
        .query_map([], |row| row.get(0))?
        .collect::<rusqlite::Result<Vec<String>>>()?;

    assert_eq!(
        windows,
        vec![
            base.to_rfc3339(),
            (base + Duration::minutes(1)).to_rfc3339(),
        ],
        "retention must not delete cold-start metric windows before the threshold is reached"
    );
    Ok(())
}

#[test]
fn persist_discovery_cycle_skips_metric_retention_when_metric_batch_is_empty() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("discovery-wallet-metrics-empty-batch.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    let wallet_id = "wallet-empty-batch".to_string();
    let window_start = DateTime::parse_from_rfc3339("2026-02-20T00:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    let wallets = vec![WalletUpsertRow {
        wallet_id: wallet_id.clone(),
        first_seen: window_start,
        last_seen: window_start,
        status: "active".to_string(),
    }];
    let metrics = vec![WalletMetricRow {
        wallet_id: wallet_id.clone(),
        window_start,
        pnl: 0.0,
        win_rate: 0.0,
        trades: 1,
        closed_trades: 1,
        hold_median_seconds: 0,
        score: 1.0,
        buy_total: 1,
        tradable_ratio: 1.0,
        rug_ratio: 0.0,
    }];
    let desired = vec![wallet_id.clone()];
    store.persist_discovery_cycle(
        &wallets,
        &metrics,
        &desired,
        true,
        true,
        window_start,
        "seed-metrics",
    )?;
    let latest_before = store
        .latest_wallet_metrics_window_start()?
        .expect("expected wallet_metrics window after initial persist");

    let empty_follow_delta = store.persist_discovery_cycle(
        &wallets,
        &[],
        &desired,
        true,
        true,
        window_start + Duration::minutes(10),
        "skip-metrics",
    )?;
    assert_eq!(empty_follow_delta.activated, 0);
    assert_eq!(empty_follow_delta.deactivated, 0);

    let latest_after = store
        .latest_wallet_metrics_window_start()?
        .expect("wallet_metrics window should survive empty batch");
    assert_eq!(latest_after, latest_before);
    Ok(())
}
