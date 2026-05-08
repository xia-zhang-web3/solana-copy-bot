use super::*;

#[test]
fn startup_trusted_selection_gate_status_degrades_stale_trusted_current_snapshot_from_metadata(
) -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp
        .path()
        .join("startup-trusted-selection-gate-status-stale-current-metadata.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    let effective_window_start = DateTime::parse_from_rfc3339("2026-03-10T21:00:00+00:00")
        .expect("timestamp")
        .with_timezone(&Utc);
    let now = DateTime::parse_from_rfc3339("2026-03-15T22:05:00+00:00")
        .expect("timestamp")
        .with_timezone(&Utc);
    let snapshot_write = TrustedWalletMetricsSnapshotWrite {
        snapshot_id: "wallet_metrics:discovery_refresh:2026-03-10T21:00:00+00:00".to_string(),
        source_snapshot_id: None,
        source_window_start: Some(effective_window_start),
        effective_window_start,
        created_at: effective_window_start + Duration::minutes(1),
        source_kind: TrustedSnapshotSourceKind::DiscoveryRefresh,
        row_count: 1,
        trust_state: TrustedSelectionState::TrustedCurrent,
    };
    store.persist_discovery_cycle_with_snapshot_metadata(
        &[WalletUpsertRow {
            wallet_id: "wallet-stale-current".to_string(),
            first_seen: effective_window_start - Duration::days(1),
            last_seen: effective_window_start,
            status: "candidate".to_string(),
        }],
        &[WalletMetricRow {
            wallet_id: "wallet-stale-current".to_string(),
            window_start: effective_window_start,
            pnl: 1.0,
            win_rate: 0.8,
            trades: 4,
            closed_trades: 4,
            hold_median_seconds: 90,
            score: 0.8,
            buy_total: 4,
            tradable_ratio: 1.0,
            rug_ratio: 0.0,
        }],
        &[],
        false,
        false,
        effective_window_start + Duration::minutes(1),
        "stale-current-metadata-test",
        Some(&snapshot_write),
    )?;

    let status = store.startup_trusted_selection_gate_status()?;
    assert_eq!(
        status.selection_state,
        Some(TrustedSelectionState::TrustedCurrent)
    );
    assert!(!status.legacy_bool_fallback_used);
    assert_eq!(
        status.effective_selection_state(now, 5, 30 * 60, 60 * 60),
        Some(TrustedSelectionState::Invalid)
    );
    assert!(status.effective_startup_fail_closed(now, 5, 30 * 60, 60 * 60));
    Ok(())
}

#[test]
fn persist_discovery_cycle_retention_keeps_latest_logical_windows_with_legacy_z_rows() -> Result<()>
{
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp
        .path()
        .join("discovery-wallet-metrics-retention-legacy-z.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    let base = DateTime::parse_from_rfc3339("2026-02-20T00:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    let window0 = base;
    let window1 = base + Duration::minutes(1);
    let window2 = base + Duration::minutes(2);
    let window3 = base + Duration::minutes(3);

    for (wallet_id, last_seen) in [
        ("wallet-old-z", window0),
        ("wallet-mid-plus", window1),
        ("wallet-dup-z", window2),
        ("wallet-dup-plus", window2),
        ("wallet-new", window3),
    ] {
        store.upsert_wallet(wallet_id, base, last_seen, "candidate")?;
    }

    for (wallet_id, raw_window_start) in [
        ("wallet-old-z", "2026-02-20T00:00:00Z"),
        ("wallet-mid-plus", "2026-02-20T00:01:00+00:00"),
        ("wallet-dup-z", "2026-02-20T00:02:00Z"),
        ("wallet-dup-plus", "2026-02-20T00:02:00+00:00"),
    ] {
        store.conn.execute(
            "INSERT INTO wallet_metrics(
                    wallet_id,
                    window_start,
                    pnl,
                    win_rate,
                    trades,
                    closed_trades,
                    hold_median_seconds,
                    score,
                    buy_total,
                    tradable_ratio,
                    rug_ratio
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                wallet_id,
                raw_window_start,
                1.0_f64,
                1.0_f64,
                1_i64,
                1_i64,
                60_i64,
                1.0_f64,
                1_i64,
                1.0_f64,
                0.0_f64,
            ],
        )?;
    }

    store.persist_discovery_cycle(
        &[WalletUpsertRow {
            wallet_id: "wallet-new".to_string(),
            first_seen: base,
            last_seen: window3,
            status: "candidate".to_string(),
        }],
        &[WalletMetricRow {
            wallet_id: "wallet-new".to_string(),
            window_start: window3,
            pnl: 1.0,
            win_rate: 1.0,
            trades: 1,
            closed_trades: 1,
            hold_median_seconds: 60,
            score: 1.0,
            buy_total: 1,
            tradable_ratio: 1.0,
            rug_ratio: 0.0,
        }],
        &["wallet-new".to_string()],
        true,
        true,
        window3,
        "legacy-z-retention-test",
    )?;

    let mut raw_stmt = store
        .conn
        .prepare("SELECT DISTINCT window_start FROM wallet_metrics ORDER BY window_start ASC")?;
    let raw_windows: Vec<String> = raw_stmt
        .query_map([], |row| row.get(0))?
        .collect::<rusqlite::Result<Vec<String>>>()?;
    assert!(
            !raw_windows.contains(&"2026-02-20T00:00:00Z".to_string()),
            "oldest logical window must be deleted even when newer windows have mixed Z/+00:00 encodings"
        );
    assert!(raw_windows.contains(&window1.to_rfc3339()));
    assert!(raw_windows.contains(&"2026-02-20T00:02:00Z".to_string()));
    assert!(raw_windows.contains(&window2.to_rfc3339()));
    assert!(raw_windows.contains(&window3.to_rfc3339()));

    let mut logical_stmt = store.conn.prepare(
        "SELECT DISTINCT unixepoch(window_start)
             FROM wallet_metrics
             ORDER BY 1 ASC",
    )?;
    let logical_windows: Vec<i64> = logical_stmt
        .query_map([], |row| row.get(0))?
        .collect::<rusqlite::Result<Vec<i64>>>()?;
    assert_eq!(
            logical_windows,
            vec![window1.timestamp(), window2.timestamp(), window3.timestamp()],
            "retention must keep the latest three logical wallet_metrics windows even with mixed UTC encodings"
        );
    Ok(())
}

#[test]
fn persist_discovery_cycle_can_suppress_followlist_deactivations() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp
        .path()
        .join("discovery-followlist-deactivation-suppression.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    let now = DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    let wallet_id = "wallet-keep-active".to_string();
    store.activate_follow_wallet(&wallet_id, now, "seed-follow")?;
    assert!(store.list_active_follow_wallets()?.contains(&wallet_id));

    let wallets = vec![WalletUpsertRow {
        wallet_id: wallet_id.clone(),
        first_seen: now,
        last_seen: now,
        status: "observed".to_string(),
    }];

    let suppressed = store.persist_discovery_cycle(
        &wallets,
        &[],
        &[],
        true,
        false,
        now + Duration::minutes(1),
        "suppressed-demotions",
    )?;
    assert_eq!(suppressed.activated, 0);
    assert_eq!(suppressed.deactivated, 0);
    assert!(
        store.list_active_follow_wallets()?.contains(&wallet_id),
        "active wallet must remain followed when deactivations are suppressed"
    );

    let unsuppressed = store.persist_discovery_cycle(
        &wallets,
        &[],
        &[],
        true,
        true,
        now + Duration::minutes(2),
        "allow-demotions",
    )?;
    assert_eq!(unsuppressed.activated, 0);
    assert_eq!(unsuppressed.deactivated, 1);
    assert!(
        !store.list_active_follow_wallets()?.contains(&wallet_id),
        "active wallet should deactivate again once suppression is lifted"
    );
    Ok(())
}

#[test]
fn persist_discovery_cycle_can_suppress_followlist_activations() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp
        .path()
        .join("discovery-followlist-activation-suppression.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    let now = DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    let wallet_id = "wallet-dont-activate".to_string();
    let wallets = vec![WalletUpsertRow {
        wallet_id: wallet_id.clone(),
        first_seen: now,
        last_seen: now,
        status: "candidate".to_string(),
    }];

    let suppressed = store.persist_discovery_cycle(
        &wallets,
        &[],
        std::slice::from_ref(&wallet_id),
        false,
        true,
        now + Duration::minutes(1),
        "suppressed-promotions",
    )?;
    assert_eq!(suppressed.activated, 0);
    assert_eq!(suppressed.deactivated, 0);
    assert!(
        !store.list_active_follow_wallets()?.contains(&wallet_id),
        "candidate wallet must stay inactive when followlist activations are suppressed"
    );

    let unsuppressed = store.persist_discovery_cycle(
        &wallets,
        &[],
        std::slice::from_ref(&wallet_id),
        true,
        true,
        now + Duration::minutes(2),
        "allow-promotions",
    )?;
    assert_eq!(unsuppressed.activated, 1);
    assert_eq!(unsuppressed.deactivated, 0);
    assert!(
        store.list_active_follow_wallets()?.contains(&wallet_id),
        "candidate wallet should activate once suppression is lifted"
    );
    Ok(())
}
