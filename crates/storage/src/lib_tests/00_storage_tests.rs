use super::*;

pub(super) fn copy_migrations_through(dest: &Path, max_version: &str) -> Result<()> {
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    fs::create_dir_all(dest)
        .with_context(|| format!("failed to create temp migration dir {}", dest.display()))?;
    for entry in fs::read_dir(&source)
        .with_context(|| format!("failed to read migrations dir {}", source.display()))?
    {
        let entry =
            entry.with_context(|| format!("failed to read entry in {}", source.display()))?;
        let path = entry.path();
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if file_name <= max_version {
            fs::copy(&path, dest.join(file_name)).with_context(|| {
                format!(
                    "failed to copy migration {} into {}",
                    path.display(),
                    dest.display()
                )
            })?;
        }
    }
    Ok(())
}

#[test]
fn immutable_read_only_helper_is_proof_specific_and_general_read_only_keeps_default_semantics(
) -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("immutable-read-only-proof.db");

    {
        let conn = Connection::open(&db_path)
            .with_context(|| format!("failed creating fixture {}", db_path.display()))?;
        conn.execute_batch(
            "
                PRAGMA journal_mode=DELETE;
                CREATE TABLE proof_fixture(id INTEGER PRIMARY KEY, value TEXT NOT NULL);
                INSERT INTO proof_fixture(value) VALUES ('seed');
                ",
        )
        .context("failed seeding immutable read-only fixture")?;
    }

    let general_store = SqliteStore::open_read_only(&db_path)?;
    let immutable_store = SqliteStore::open_read_only_immutable(&db_path)?;
    let general_facts = general_store.sqlite_read_only_driver_compare_facts()?;
    let immutable_facts = immutable_store.sqlite_read_only_driver_compare_facts()?;

    assert!(
        !general_facts.query_only,
        "general read-only helper must not hardcode proof-only query_only semantics"
    );
    assert!(
        immutable_facts.query_only,
        "immutable proof helper must force query_only for frozen artifacts"
    );
    let row_count: i64 =
        immutable_store
            .conn
            .query_row("SELECT COUNT(*) FROM proof_fixture", [], |row| row.get(0))?;
    assert_eq!(
        row_count, 1,
        "immutable helper must read the frozen fixture"
    );
    assert!(
        immutable_store
            .conn
            .execute("CREATE TABLE proof_mutation(id INTEGER)", [])
            .is_err(),
        "immutable helper must remain read-only"
    );
    Ok(())
}

pub(super) fn fmt_f64(value: f64) -> String {
    format!("{value:.12}")
}

pub(super) fn make_swap(
    signature: impl Into<String>,
    wallet: impl Into<String>,
    token_in: impl Into<String>,
    token_out: impl Into<String>,
    amount_in: f64,
    amount_out: f64,
    slot: u64,
    ts_utc: DateTime<Utc>,
) -> SwapEvent {
    SwapEvent {
        signature: signature.into(),
        wallet: wallet.into(),
        dex: "raydium".to_string(),
        token_in: token_in.into(),
        token_out: token_out.into(),
        amount_in,
        amount_out,
        exact_amounts: None,
        slot,
        ts_utc,
    }
}

pub(super) fn comparable_wallet_scoring_days(
    store: &SqliteStore,
) -> Result<Vec<(String, String, String, String, u32, String, String)>> {
    let mut stmt = store.conn.prepare(
            "SELECT wallet_id, activity_day, first_seen, last_seen, trades, spent_sol, max_buy_notional_sol
             FROM wallet_scoring_days
             ORDER BY wallet_id ASC, activity_day ASC",
        )?;
    let mut rows = stmt.query([])?;
    let mut out = Vec::new();
    while let Some(row) = rows.next()? {
        out.push((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, i64>(4)?.max(0) as u32,
            fmt_f64(row.get::<_, f64>(5)?),
            fmt_f64(row.get::<_, f64>(6)?),
        ));
    }
    Ok(out)
}

pub(super) fn comparable_wallet_scoring_tx_minutes(
    store: &SqliteStore,
) -> Result<Vec<(String, i64, i64)>> {
    let mut stmt = store.conn.prepare(
        "SELECT wallet_id, minute_bucket, tx_count
             FROM wallet_scoring_tx_minutes
             ORDER BY wallet_id ASC, minute_bucket ASC",
    )?;
    let mut rows = stmt.query([])?;
    let mut out = Vec::new();
    while let Some(row) = rows.next()? {
        out.push((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, i64>(2)?,
        ));
    }
    Ok(out)
}

pub(super) fn comparable_wallet_scoring_buy_facts(store: &SqliteStore) -> Result<Vec<String>> {
    let mut stmt = store.conn.prepare(
        "SELECT buy_signature, wallet_id, token, ts, notional_sol, market_volume_5m_sol,
                    market_unique_traders_5m, market_liquidity_proxy_sol, quality_source,
                    quality_token_age_seconds, quality_holders, quality_liquidity_sol,
                     rug_check_after_ts, rug_volume_lookahead_sol, rug_unique_traders_lookahead
             FROM wallet_scoring_buy_facts
             ORDER BY buy_signature ASC",
    )?;
    let mut rows = stmt.query([])?;
    let mut out = Vec::new();
    while let Some(row) = rows.next()? {
        out.push(format!(
            "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            fmt_f64(row.get::<_, f64>(4)?),
            fmt_f64(row.get::<_, f64>(5)?),
            row.get::<_, i64>(6)?,
            fmt_f64(row.get::<_, f64>(7)?),
            row.get::<_, String>(8)?,
            row.get::<_, Option<i64>>(9)?
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string()),
            row.get::<_, Option<i64>>(10)?
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string()),
            row.get::<_, Option<f64>>(11)?
                .map(fmt_f64)
                .unwrap_or_else(|| "null".to_string()),
            row.get::<_, String>(12)?,
            row.get::<_, Option<f64>>(13)?
                .map(fmt_f64)
                .unwrap_or_else(|| "null".to_string()),
            row.get::<_, Option<i64>>(14)?
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string()),
        ));
    }
    Ok(out)
}

pub(super) fn comparable_wallet_scoring_close_facts(
    store: &SqliteStore,
) -> Result<Vec<(String, i64, String, String, String, String, i64, i64)>> {
    let mut stmt = store.conn.prepare(
            "SELECT sell_signature, segment_index, wallet_id, token, closed_ts, pnl_sol, hold_seconds, win
             FROM wallet_scoring_close_facts
             ORDER BY sell_signature ASC, segment_index ASC",
        )?;
    let mut rows = stmt.query([])?;
    let mut out = Vec::new();
    while let Some(row) = rows.next()? {
        out.push((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
            fmt_f64(row.get::<_, f64>(5)?),
            row.get::<_, i64>(6)?,
            row.get::<_, i64>(7)?,
        ));
    }
    Ok(out)
}

pub(super) fn comparable_wallet_scoring_open_lots(
    store: &SqliteStore,
) -> Result<Vec<(String, String, String, String, String, String)>> {
    let mut stmt = store.conn.prepare(
        "SELECT buy_signature, wallet_id, token, qty, cost_sol, opened_ts
             FROM wallet_scoring_open_lots
             ORDER BY wallet_id ASC, token ASC, opened_ts ASC, buy_signature ASC",
    )?;
    let mut rows = stmt.query([])?;
    let mut out = Vec::new();
    while let Some(row) = rows.next()? {
        out.push((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            fmt_f64(row.get::<_, f64>(3)?),
            fmt_f64(row.get::<_, f64>(4)?),
            row.get::<_, String>(5)?,
        ));
    }
    Ok(out)
}

#[test]
fn close_shadow_lots_fifo_atomic_handles_parallel_sells_without_double_close() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("shadow-close-race.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

    let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
    seed_store.run_migrations(&migration_dir)?;
    let opened_ts = DateTime::parse_from_rfc3339("2026-02-15T10:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    seed_store.insert_shadow_lot("wallet", "token", 100.0, 1.0, opened_ts)?;
    drop(seed_store);

    let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
    let db_path_a = db_path.clone();
    let barrier_a = barrier.clone();
    let opened_ts_a = opened_ts;
    let worker_a = std::thread::spawn(move || -> Result<ShadowCloseOutcome> {
        let store = SqliteStore::open(Path::new(&db_path_a))?;
        barrier_a.wait();
        store.close_shadow_lots_fifo_atomic(
            "signal-a",
            "wallet",
            "token",
            80.0,
            0.02,
            opened_ts_a + Duration::minutes(1),
        )
    });

    let db_path_b = db_path.clone();
    let barrier_b = barrier.clone();
    let opened_ts_b = opened_ts;
    let worker_b = std::thread::spawn(move || -> Result<ShadowCloseOutcome> {
        let store = SqliteStore::open(Path::new(&db_path_b))?;
        barrier_b.wait();
        store.close_shadow_lots_fifo_atomic(
            "signal-b",
            "wallet",
            "token",
            80.0,
            0.02,
            opened_ts_b + Duration::minutes(2),
        )
    });

    barrier.wait();
    let close_a = worker_a
        .join()
        .expect("worker A thread panicked")
        .context("worker A close failed")?;
    let close_b = worker_b
        .join()
        .expect("worker B thread panicked")
        .context("worker B close failed")?;

    let total_closed = close_a.closed_qty + close_b.closed_qty;
    assert!(
        (total_closed - 100.0).abs() < 1e-9,
        "expected total closed qty to equal available inventory, got {total_closed}"
    );

    let verify_store = SqliteStore::open(Path::new(&db_path))?;
    assert!(
        !verify_store.has_shadow_lots("wallet", "token")?,
        "all lots should be closed exactly once"
    );

    Ok(())
}

#[test]
fn insert_shadow_lot_returns_inserted_row_id_after_retryable_lock() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("shadow-insert-rowid-retry.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

    let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
    seed_store.run_migrations(&migration_dir)?;
    drop(seed_store);

    let blocker_store = SqliteStore::open(Path::new(&db_path))?;
    blocker_store
        .conn
        .busy_timeout(StdDuration::from_millis(1))
        .context("failed to shorten blocker busy timeout")?;
    blocker_store
        .conn
        .execute_batch("BEGIN IMMEDIATE TRANSACTION")?;

    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let worker_path = db_path.clone();
    let worker_barrier = barrier.clone();
    let worker = std::thread::spawn(move || -> Result<i64> {
        let store = SqliteStore::open(Path::new(&worker_path))?;
        store
            .conn
            .busy_timeout(StdDuration::from_millis(1))
            .context("failed to shorten worker busy timeout")?;
        worker_barrier.wait();
        let opened_ts = DateTime::parse_from_rfc3339("2026-02-15T10:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        store.insert_shadow_lot("wallet", "token", 100.0, 1.0, opened_ts)
    });

    barrier.wait();
    std::thread::sleep(StdDuration::from_millis(250));
    blocker_store.conn.execute_batch("COMMIT")?;

    let lot_id = worker
        .join()
        .expect("worker thread panicked")
        .context("worker insert failed")?;
    let verify_store = SqliteStore::open(Path::new(&db_path))?;
    let lots = verify_store.list_shadow_lots("wallet", "token")?;
    assert_eq!(lots.len(), 1, "expected exactly one inserted shadow lot");
    assert_eq!(
        lots[0].id, lot_id,
        "returned row id must match persisted shadow lot"
    );
    Ok(())
}

#[test]
fn has_shadow_lots_ignores_zero_and_dust_qty_rows() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("shadow-dust-open-check.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    let opened_ts = DateTime::parse_from_rfc3339("2026-02-15T10:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    let lot_id = store.insert_shadow_lot("wallet", "token", 10.0, 1.0, opened_ts)?;
    assert!(store.has_shadow_lots("wallet", "token")?);

    store.update_shadow_lot(lot_id, 1e-13, 1e-15)?;

    assert!(
        !store.has_shadow_lots("wallet", "token")?,
        "dust lots should not count as open inventory"
    );
    assert_eq!(
        store.shadow_open_lots_count()?,
        0,
        "dust lots should not count toward open lot metrics"
    );
    assert!(
        store.shadow_open_notional_sol()?.abs() < 1e-12,
        "dust lots should not contribute to open notional metrics"
    );
    assert!(
        !store
            .list_shadow_open_pairs()?
            .contains(&("wallet".to_string(), "token".to_string())),
        "dust lots should not appear in open pair queries"
    );
    assert!(
        store
            .list_open_shadow_lots_older_than(opened_ts + chrono::Duration::minutes(1), 10)?
            .is_empty(),
        "dust lots should not be returned as stale open lots"
    );

    Ok(())
}

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
