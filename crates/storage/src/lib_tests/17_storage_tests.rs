use super::*;

#[test]
fn observed_buy_mint_count_page_query_respects_exclusive_time_bounds() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp
        .path()
        .join("observed-buy-mint-count-page-time-bounds.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    let base = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
        .expect("valid timestamp")
        .with_timezone(&Utc);
    for (signature, token, ts) in [
        ("buy-a", "token-a", base + Duration::seconds(1)),
        ("buy-b", "token-b", base + Duration::seconds(2)),
        ("buy-c", "token-c", base + Duration::seconds(3)),
    ] {
        assert!(store.insert_observed_swap(&SwapEvent {
            signature: signature.to_string(),
            wallet: "wallet-buy-count-bounds".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: token.to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            slot: 10,
            ts_utc: ts,
            exact_amounts: None,
        })?);
    }

    let lower_exclusive = store
        .load_observed_buy_mint_counts_in_time_bounds_after_token_with_budget(
            base + Duration::seconds(1),
            false,
            base + Duration::seconds(3),
            true,
            None,
            None,
            10,
            std::time::Instant::now() + StdDuration::from_secs(1),
        )?;
    assert_eq!(
        lower_exclusive
            .rows
            .iter()
            .map(|row| row.mint.as_str())
            .collect::<Vec<_>>(),
        vec!["token-b", "token-c"]
    );

    let upper_exclusive = store
        .load_observed_buy_mint_counts_in_time_bounds_after_token_with_budget(
            base + Duration::seconds(1),
            true,
            base + Duration::seconds(3),
            false,
            None,
            None,
            10,
            std::time::Instant::now() + StdDuration::from_secs(1),
        )?;
    assert_eq!(
        upper_exclusive
            .rows
            .iter()
            .map(|row| row.mint.as_str())
            .collect::<Vec<_>>(),
        vec!["token-a", "token-b"]
    );
    Ok(())
}

#[test]
fn persist_discovery_cycle_retries_after_immediate_write_lock() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("discovery-write-retry.db");
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
    let worker_db_path = db_path.clone();
    let worker_barrier = barrier.clone();
    let worker = std::thread::spawn(move || -> Result<FollowlistUpdateResult> {
        let store = SqliteStore::open(Path::new(&worker_db_path))?;
        store
            .conn
            .busy_timeout(StdDuration::from_millis(1))
            .context("failed to shorten worker busy timeout")?;
        let window_start = DateTime::parse_from_rfc3339("2026-02-20T00:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let wallets = vec![WalletUpsertRow {
            wallet_id: "wallet-retry".to_string(),
            first_seen: window_start,
            last_seen: window_start,
            status: "candidate".to_string(),
        }];
        let metrics = vec![WalletMetricRow {
            wallet_id: "wallet-retry".to_string(),
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
        let desired_wallets = vec!["wallet-retry".to_string()];
        worker_barrier.wait();
        store.persist_discovery_cycle(
            &wallets,
            &metrics,
            &desired_wallets,
            true,
            true,
            window_start,
            "retry-test",
        )
    });

    barrier.wait();
    std::thread::sleep(StdDuration::from_millis(250));
    blocker_store.conn.execute_batch("COMMIT")?;

    let follow_delta = worker
        .join()
        .expect("worker thread panicked")
        .context("worker discovery cycle failed")?;
    assert_eq!(follow_delta.activated, 1);
    assert_eq!(follow_delta.deactivated, 0);

    let verify_store = SqliteStore::open(Path::new(&db_path))?;
    assert!(
        verify_store
            .list_active_follow_wallets()?
            .contains("wallet-retry"),
        "followlist activation should commit after retry"
    );
    let windows: i64 = verify_store.conn.query_row(
        "SELECT COUNT(*) FROM wallet_metrics WHERE wallet_id = ?1",
        params!["wallet-retry"],
        |row| row.get(0),
    )?;
    assert_eq!(windows, 1, "wallet metric insert should commit after retry");
    Ok(())
}
