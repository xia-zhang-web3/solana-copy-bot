use super::*;

#[test]
fn wallet_metrics_lookup_treats_z_and_plus00_as_same_window() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("wallet-metrics-z-plus00-lookup.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    let first_seen = DateTime::parse_from_rfc3339("2026-03-01T00:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    let window_start = DateTime::parse_from_rfc3339("2026-03-10T21:00:00+00:00")
        .expect("timestamp")
        .with_timezone(&Utc);
    store.upsert_wallet("wallet-z", first_seen, window_start, "candidate")?;
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
            "wallet-z",
            "2026-03-10T21:00:00Z",
            1.0_f64,
            0.8_f64,
            4_i64,
            4_i64,
            60_i64,
            0.9_f64,
            4_i64,
            1.0_f64,
            0.0_f64,
        ],
    )?;
    store.insert_wallet_metric(&WalletMetricRow {
        wallet_id: "wallet-z".to_string(),
        window_start,
        pnl: 2.0,
        win_rate: 0.9,
        trades: 7,
        closed_trades: 7,
        hold_median_seconds: 120,
        score: 1.3,
        buy_total: 7,
        tradable_ratio: 1.0,
        rug_ratio: 0.0,
    })?;

    assert!(
        store.wallet_metrics_window_exists(window_start)?,
        "logical UTC equality should treat Z and +00:00 as the same wallet_metrics window"
    );
    assert_eq!(
        store.latest_wallet_metrics_window_start()?,
        Some(window_start)
    );
    assert_eq!(
        store.wallet_metrics_row_count_for_window(window_start)?,
        1,
        "logical row count must dedupe mixed Z/+00:00 rows for the same wallet"
    );
    let snapshots = store.load_latest_wallet_metric_snapshots()?;
    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0].wallet_id, "wallet-z");
    assert_eq!(snapshots[0].window_start, window_start);
    assert_eq!(
            snapshots[0].score, 1.3,
            "loader must prefer the canonical row when both legacy Z and canonical +00:00 variants exist"
        );
    assert_eq!(snapshots[0].buy_total, 7);
    Ok(())
}

#[test]
fn persist_discovery_cycle_with_snapshot_metadata_writes_trusted_snapshot_row() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("wallet-metrics-snapshot-metadata.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    let window_start = DateTime::parse_from_rfc3339("2026-03-10T21:00:00+00:00")
        .expect("timestamp")
        .with_timezone(&Utc);
    let created_at = DateTime::parse_from_rfc3339("2026-03-15T12:00:00+00:00")
        .expect("timestamp")
        .with_timezone(&Utc);
    let snapshot_write = TrustedWalletMetricsSnapshotWrite {
        snapshot_id: "wallet_metrics:discovery_refresh:2026-03-10T21:00:00+00:00".to_string(),
        source_snapshot_id: None,
        source_window_start: Some(window_start),
        effective_window_start: window_start,
        created_at,
        source_kind: TrustedSnapshotSourceKind::DiscoveryRefresh,
        row_count: 1,
        trust_state: TrustedSelectionState::TrustedCurrent,
    };

    store.persist_discovery_cycle_with_snapshot_metadata(
        &[WalletUpsertRow {
            wallet_id: "wallet-meta".to_string(),
            first_seen: window_start - Duration::days(1),
            last_seen: window_start,
            status: "candidate".to_string(),
        }],
        &[WalletMetricRow {
            wallet_id: "wallet-meta".to_string(),
            window_start,
            pnl: 1.0,
            win_rate: 0.6,
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
        created_at,
        "snapshot-metadata-test",
        Some(&snapshot_write),
    )?;

    let metadata = store
        .trusted_wallet_metrics_snapshot_metadata_for_window(window_start)?
        .expect("snapshot metadata should be written for persisted window");
    assert_eq!(metadata.snapshot_id, snapshot_write.snapshot_id);
    assert_eq!(
        metadata.source_snapshot_id,
        snapshot_write.source_snapshot_id
    );
    assert_eq!(
        metadata.source_window_start,
        snapshot_write.source_window_start
    );
    assert_eq!(
        metadata.effective_window_start,
        snapshot_write.effective_window_start
    );
    assert_eq!(metadata.created_at, snapshot_write.created_at);
    assert_eq!(metadata.source_kind, snapshot_write.source_kind);
    assert_eq!(metadata.row_count, snapshot_write.row_count);
    assert_eq!(metadata.trust_state, snapshot_write.trust_state);
    Ok(())
}

#[test]
fn discovery_trusted_selection_state_rejects_legacy_naive_timestamp() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp
        .path()
        .join("discovery-trusted-selection-state-upgrade.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    store.conn.execute_batch(
        "DROP TABLE IF EXISTS discovery_strategy_state;
             CREATE TABLE discovery_strategy_state (
                 id INTEGER PRIMARY KEY CHECK (id = 1),
                 trusted_selection_bootstrap_required INTEGER NOT NULL DEFAULT 0,
                 trusted_selection_reason TEXT NOT NULL DEFAULT '',
                 updated_at TEXT NOT NULL DEFAULT (datetime('now'))
             );
             INSERT INTO discovery_strategy_state(
                 id,
                 trusted_selection_bootstrap_required,
                 trusted_selection_reason,
                 updated_at
             ) VALUES (1, 1, 'legacy_bootstrap_pending', '2026-03-15 12:00:00');",
    )?;

    let err = store
        .discovery_trusted_selection_state()
        .expect_err("legacy naive discovery_strategy_state timestamp must fail closed");
    assert!(err
        .to_string()
        .contains("must use canonical UTC offset +00:00"));
    Ok(())
}

#[test]
fn discovery_trusted_selection_state_reader_accepts_bool_setter_rows() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp
        .path()
        .join("discovery-trusted-selection-state-bool-setter.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    store.set_discovery_trusted_selection_bootstrap_required(true, "bool_setter_pending")?;

    let raw_updated_at: String = store.conn.query_row(
        "SELECT updated_at
             FROM discovery_strategy_state
             WHERE id = 1",
        [],
        |row| row.get(0),
    )?;
    assert!(
        raw_updated_at.contains('T') && raw_updated_at.contains("+00:00"),
        "bool setter should now persist RFC3339 updated_at for typed reader compatibility"
    );

    let state = store
        .discovery_trusted_selection_state()?
        .expect("typed reader should load bool-setter row");
    assert!(state.bootstrap_required);
    assert_eq!(state.reason, "bool_setter_pending");
    assert_eq!(state.selection_state, TrustedSelectionState::Invalid);
    assert_eq!(state.active_snapshot_id, None);
    Ok(())
}

#[test]
fn startup_trusted_selection_gate_status_falls_back_to_legacy_bool_when_row_missing() -> Result<()>
{
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp
        .path()
        .join("startup-trusted-selection-gate-status-missing-row.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    let status = store.startup_trusted_selection_gate_status()?;
    assert!(!status.bootstrap_required);
    assert_eq!(status.selection_state, None);
    assert!(!status.startup_fail_closed);
    assert!(status.legacy_bool_fallback_used);
    Ok(())
}

#[test]
fn startup_trusted_selection_gate_status_treats_legacy_bool_only_row_as_fallback() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp
        .path()
        .join("startup-trusted-selection-gate-status-legacy-bool-row.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    store.set_discovery_trusted_selection_bootstrap_required(false, "legacy_bool_false")?;
    let status = store.startup_trusted_selection_gate_status()?;
    assert!(!status.bootstrap_required);
    assert_eq!(status.selection_state, None);
    assert!(!status.startup_fail_closed);
    assert_eq!(status.reason.as_deref(), Some("legacy_bool_false"));
    assert!(status.legacy_bool_fallback_used);

    store.set_discovery_trusted_selection_bootstrap_required(true, "legacy_bool_true")?;
    let status = store.startup_trusted_selection_gate_status()?;
    assert!(status.bootstrap_required);
    assert_eq!(status.selection_state, None);
    assert!(status.startup_fail_closed);
    assert_eq!(status.reason.as_deref(), Some("legacy_bool_true"));
    assert!(status.legacy_bool_fallback_used);
    Ok(())
}
