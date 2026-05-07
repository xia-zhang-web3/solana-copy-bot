use super::*;

#[test]
fn followlist_single_active_guard_migration_dedupes_existing_duplicates() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("followlist-single-active-guard.db");
    let legacy_migrations = temp.path().join("legacy-migrations");
    copy_migrations_through(&legacy_migrations, "0017_positions_closed_state_index.sql")?;

    let mut legacy_store = SqliteStore::open(Path::new(&db_path))?;
    legacy_store.run_migrations(&legacy_migrations)?;
    legacy_store.conn.execute(
        "INSERT INTO followlist(wallet_id, added_at, reason, active)
             VALUES (?1, ?2, NULL, 1)",
        params!["wallet-dup", "2026-02-20T00:00:00Z"],
    )?;
    legacy_store.conn.execute(
        "INSERT INTO followlist(wallet_id, added_at, reason, active)
             VALUES (?1, ?2, NULL, 1)",
        params!["wallet-dup", "2026-02-20T00:01:00Z"],
    )?;
    drop(legacy_store);

    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut migrated_store = SqliteStore::open(Path::new(&db_path))?;
    migrated_store.run_migrations(&migration_dir)?;

    let rows: Vec<(i64, String, i64, Option<String>, Option<String>)> = {
        let mut stmt = migrated_store.conn.prepare(
            "SELECT id, added_at, active, removed_at, reason
                 FROM followlist
                 WHERE wallet_id = ?1
                 ORDER BY id ASC",
        )?;
        let mapped_rows = stmt.query_map(params!["wallet-dup"], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        })?;
        mapped_rows.collect::<rusqlite::Result<Vec<_>>>()?
    };
    assert_eq!(rows.len(), 2, "expected both historical rows to remain");
    assert_eq!(
        rows[0].2, 0,
        "older duplicate must be deactivated by migration"
    );
    assert_eq!(
        rows[0].3.as_deref(),
        Some("2026-02-20T00:01:00Z"),
        "migration should close duplicate row at the surviving row start time",
    );
    assert_eq!(
        rows[0].4.as_deref(),
        Some("migration_dedup_active_followlist"),
        "migration should annotate deduplicated row",
    );
    assert_eq!(rows[1].2, 1, "latest active row must stay active");
    assert!(
        migrated_store.was_wallet_followed_at(
            "wallet-dup",
            DateTime::parse_from_rfc3339("2026-02-20T00:00:30Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        )?,
        "dedup must preserve historical follow membership before the surviving active row"
    );

    let duplicate_insert = migrated_store.conn.execute(
        "INSERT INTO followlist(wallet_id, added_at, reason, active)
             VALUES (?1, ?2, ?3, 1)",
        params!["wallet-dup", "2026-02-20T00:02:00Z", "duplicate-test"],
    );
    assert!(
        duplicate_insert.is_err(),
        "unique partial index must reject a second active followlist row"
    );
    Ok(())
}

#[test]
fn positions_single_open_guard_migration_merges_duplicate_open_rows() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("positions-single-open-guard.db");
    let legacy_migrations = temp.path().join("legacy-migrations");
    copy_migrations_through(
        &legacy_migrations,
        "0018_followlist_single_active_guard.sql",
    )?;

    let mut legacy_store = SqliteStore::open(Path::new(&db_path))?;
    legacy_store.run_migrations(&legacy_migrations)?;
    legacy_store.conn.execute(
            "INSERT INTO positions(position_id, token, qty, cost_sol, opened_ts, closed_ts, pnl_sol, state)
             VALUES (?1, ?2, ?3, ?4, ?5, NULL, ?6, 'open')",
            params![
                "pos-open-a",
                "token-dup",
                1.5_f64,
                0.30_f64,
                "2026-03-01T00:00:00+00:00",
                0.05_f64,
            ],
        )?;
    legacy_store.conn.execute(
            "INSERT INTO positions(position_id, token, qty, cost_sol, opened_ts, closed_ts, pnl_sol, state)
             VALUES (?1, ?2, ?3, ?4, ?5, NULL, ?6, 'open')",
            params![
                "pos-open-b",
                "token-dup",
                2.0_f64,
                0.45_f64,
                "2026-03-01T00:05:00+00:00",
                0.07_f64,
            ],
        )?;
    drop(legacy_store);

    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut migrated_store = SqliteStore::open(Path::new(&db_path))?;
    migrated_store.run_migrations(&migration_dir)?;

    let open_rows: Vec<(String, f64, f64, String, Option<f64>)> = {
        let mut stmt = migrated_store.conn.prepare(
            "SELECT position_id, qty, cost_sol, opened_ts, pnl_sol
                 FROM positions
                 WHERE token = ?1
                   AND state = 'open'
                 ORDER BY opened_ts ASC",
        )?;
        let mapped_rows = stmt.query_map(params!["token-dup"], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        })?;
        mapped_rows.collect::<rusqlite::Result<Vec<_>>>()?
    };
    assert_eq!(
        open_rows.len(),
        1,
        "migration must leave exactly one open row"
    );
    assert!(
        (open_rows[0].1 - 3.5).abs() < 1e-9,
        "qty should be merged into surviving open row"
    );
    assert!(
        (open_rows[0].2 - 0.75).abs() < 1e-9,
        "cost_sol should be merged into surviving open row"
    );
    assert_eq!(
        open_rows[0].3, "2026-03-01T00:00:00+00:00",
        "merged row should preserve earliest opened_ts"
    );
    assert!(
        (open_rows[0].4.unwrap_or_default() - 0.12).abs() < 1e-9,
        "pnl_sol should be preserved across merged open rows"
    );
    let open_token_count: i64 = migrated_store.conn.query_row(
        "SELECT COUNT(DISTINCT token) FROM positions WHERE state = 'open' AND qty > 0",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(
        open_token_count, 1,
        "open position count should observe deduplicated schema invariant"
    );

    let duplicate_insert = migrated_store.conn.execute(
        "INSERT INTO positions(position_id, token, qty, cost_sol, opened_ts, state, pnl_sol)
             VALUES (?1, ?2, ?3, ?4, ?5, 'open', 0.0)",
        params![
            "pos-open-c",
            "token-dup",
            0.5_f64,
            0.10_f64,
            "2026-03-01T00:10:00+00:00",
        ],
    );
    assert!(
        duplicate_insert.is_err(),
        "unique partial index must reject a second open position row for the same token"
    );
    Ok(())
}

#[test]
fn positions_accounting_bucket_migration_allows_exact_bucket_beside_legacy_row() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("positions-accounting-bucket.db");
    let legacy_migrations = temp.path().join("legacy-migrations");
    copy_migrations_through(&legacy_migrations, "0032_copy_signals_notional_origin.sql")?;

    let mut legacy_store = SqliteStore::open(Path::new(&db_path))?;
    legacy_store.run_migrations(&legacy_migrations)?;
    legacy_store.conn.execute(
        "INSERT INTO positions(position_id, token, qty, cost_sol, opened_ts, state, pnl_sol)
             VALUES (?1, ?2, ?3, ?4, ?5, 'open', 0.0)",
        params![
            "pos-legacy",
            "token-bucket",
            1.0_f64,
            0.10_f64,
            "2026-03-08T12:00:00+00:00",
        ],
    )?;
    drop(legacy_store);

    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut migrated_store = SqliteStore::open(Path::new(&db_path))?;
    migrated_store.run_migrations(&migration_dir)?;

    migrated_store.conn.execute(
        "INSERT INTO positions(
                position_id, token, qty, qty_raw, qty_decimals, cost_sol, cost_lamports,
                accounting_bucket, opened_ts, state, pnl_sol, pnl_lamports
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 'open', 0.0, 0)",
        params![
            "pos-exact",
            "token-bucket",
            2.0_f64,
            "2000000",
            6_i64,
            0.20_f64,
            200_000_000_i64,
            POSITION_ACCOUNTING_BUCKET_EXACT_POST_CUTOVER,
            "2026-03-08T12:01:00+00:00",
        ],
    )?;

    let duplicate_legacy_insert = migrated_store.conn.execute(
        "INSERT INTO positions(
                position_id, token, qty, cost_sol, accounting_bucket, opened_ts, state, pnl_sol
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'open', 0.0)",
        params![
            "pos-legacy-dup",
            "token-bucket",
            0.5_f64,
            0.05_f64,
            POSITION_ACCOUNTING_BUCKET_LEGACY_PRE_CUTOVER,
            "2026-03-08T12:02:00+00:00",
        ],
    );
    assert!(
        duplicate_legacy_insert.is_err(),
        "unique partial index must still reject a second legacy open row"
    );
    let aggregate: (i64, f64, f64) = migrated_store.conn.query_row(
        "SELECT COUNT(DISTINCT token), SUM(qty), SUM(cost_sol)
             FROM positions
             WHERE token = ?1
               AND state = 'open'
               AND qty > 0",
        params!["token-bucket"],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    assert_eq!(
        aggregate.0, 1,
        "distinct-token open count should ignore bucket multiplicity"
    );
    assert!((aggregate.1 - 3.0).abs() < 1e-9);
    assert!((aggregate.2 - 0.30).abs() < 1e-9);
    Ok(())
}

#[test]
fn shadow_accounting_bucket_migration_backfills_legacy_rows() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("shadow-accounting-bucket.db");
    let legacy_migrations = temp.path().join("legacy-migrations");
    copy_migrations_through(&legacy_migrations, "0033_positions_accounting_bucket.sql")?;

    let mut legacy_store = SqliteStore::open(Path::new(&db_path))?;
    legacy_store.run_migrations(&legacy_migrations)?;
    legacy_store.conn.execute(
        "INSERT INTO shadow_lots(
                wallet_id, token, qty, qty_raw, qty_decimals, cost_sol, cost_lamports, opened_ts
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            "wallet",
            "token",
            1.0_f64,
            "1000000",
            6_i64,
            0.10_f64,
            100_000_000_i64,
            "2026-03-08T00:00:00+00:00",
        ],
    )?;
    legacy_store.conn.execute(
        "INSERT INTO shadow_closed_trades(
                signal_id, wallet_id, token, qty, qty_raw, qty_decimals,
                entry_cost_sol, entry_cost_lamports,
                exit_value_sol, exit_value_lamports,
                pnl_sol, pnl_lamports,
                opened_ts, closed_ts
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
        params![
            "signal",
            "wallet",
            "token",
            0.5_f64,
            "500000",
            6_i64,
            0.05_f64,
            50_000_000_i64,
            0.08_f64,
            80_000_000_i64,
            0.03_f64,
            30_000_000_i64,
            "2026-03-08T00:00:00+00:00",
            "2026-03-08T00:05:00+00:00",
        ],
    )?;
    drop(legacy_store);

    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut migrated_store = SqliteStore::open(Path::new(&db_path))?;
    migrated_store.run_migrations(&migration_dir)?;

    let lot_bucket: String = migrated_store.conn.query_row(
        "SELECT accounting_bucket FROM shadow_lots LIMIT 1",
        [],
        |row| row.get(0),
    )?;
    let trade_bucket: String = migrated_store.conn.query_row(
        "SELECT accounting_bucket FROM shadow_closed_trades LIMIT 1",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(lot_bucket, POSITION_ACCOUNTING_BUCKET_LEGACY_PRE_CUTOVER);
    assert_eq!(trade_bucket, POSITION_ACCOUNTING_BUCKET_LEGACY_PRE_CUTOVER);
    Ok(())
}

#[test]
fn execution_foreign_keys_migration_cleans_orphans_and_enforces_chain() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("execution-foreign-keys.db");
    let legacy_migrations = temp.path().join("legacy-migrations");
    copy_migrations_through(&legacy_migrations, "0019_positions_single_open_guard.sql")?;

    let mut legacy_store = SqliteStore::open(Path::new(&db_path))?;
    legacy_store.run_migrations(&legacy_migrations)?;
    let now = DateTime::parse_from_rfc3339("2026-03-04T12:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);

    legacy_store.conn.execute(
        "INSERT INTO copy_signals(signal_id, wallet_id, side, token, notional_sol, ts, status)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            "sig-valid",
            "wallet-1",
            "buy",
            "token-a",
            0.25_f64,
            now.to_rfc3339(),
            "execution_submitted",
        ],
    )?;
    legacy_store.conn.execute(
        "INSERT INTO orders(
                order_id, signal_id, route, submit_ts, status, client_order_id, attempt
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            "ord-valid",
            "sig-valid",
            "paper",
            now.to_rfc3339(),
            "execution_submitted",
            "cb_valid_order_a1",
            1_i64,
        ],
    )?;
    legacy_store.conn.execute(
        "INSERT INTO orders(
                order_id, signal_id, route, submit_ts, status, client_order_id, attempt
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            "ord-orphan",
            "sig-missing",
            "paper",
            now.to_rfc3339(),
            "execution_failed",
            "cb_orphan_order_a1",
            1_i64,
        ],
    )?;
    legacy_store.conn.execute(
        "INSERT INTO fills(order_id, token, qty, avg_price, fee, slippage_bps)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params!["ord-valid", "token-a", 1.0_f64, 0.25_f64, 0.0_f64, 0.0_f64],
    )?;
    legacy_store.conn.execute(
        "INSERT INTO fills(order_id, token, qty, avg_price, fee, slippage_bps)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            "ord-orphan",
            "token-orphan-chain",
            0.5_f64,
            0.20_f64,
            0.0_f64,
            0.0_f64,
        ],
    )?;
    legacy_store.conn.execute(
        "INSERT INTO fills(order_id, token, qty, avg_price, fee, slippage_bps)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            "ord-missing",
            "token-b",
            2.0_f64,
            0.15_f64,
            0.0_f64,
            0.0_f64,
        ],
    )?;
    drop(legacy_store);

    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut migrated_store = SqliteStore::open(Path::new(&db_path))?;
    migrated_store.run_migrations(&migration_dir)?;

    let order_count: i64 =
        migrated_store
            .conn
            .query_row("SELECT COUNT(*) FROM orders", [], |row| row.get(0))?;
    assert_eq!(
        order_count, 1,
        "orphan orders should be removed by migration"
    );

    let fill_count: i64 =
        migrated_store
            .conn
            .query_row("SELECT COUNT(*) FROM fills", [], |row| row.get(0))?;
    assert_eq!(fill_count, 1, "orphan fills should be removed by migration");

    let preserved: String = migrated_store.conn.query_row(
        "SELECT order_id FROM orders WHERE client_order_id = ?1",
        params!["cb_valid_order_a1"],
        |row| row.get(0),
    )?;
    assert_eq!(preserved, "ord-valid");

    let orphan_order_insert = migrated_store.conn.execute(
        "INSERT INTO orders(
                order_id, signal_id, route, submit_ts, status, client_order_id, attempt
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            "ord-after-migration",
            "sig-missing",
            "paper",
            now.to_rfc3339(),
            "execution_pending",
            "cb_after_migration_a1",
            1_i64,
        ],
    );
    assert!(
        orphan_order_insert.is_err(),
        "orders.signal_id foreign key must reject missing copy signal"
    );

    let orphan_fill_insert = migrated_store.conn.execute(
        "INSERT INTO fills(order_id, token, qty, avg_price, fee, slippage_bps)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            "ord-missing-after-migration",
            "token-c",
            1.0_f64,
            0.10_f64,
            0.0_f64,
            0.0_f64,
        ],
    );
    assert!(
        orphan_fill_insert.is_err(),
        "fills.order_id foreign key must reject missing order"
    );

    Ok(())
}
