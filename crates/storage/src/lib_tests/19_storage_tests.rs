use super::*;

#[test]
fn observed_swap_cursor_is_strictly_lexicographic() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("observed-swap-cursor-lexicographic.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    let base = DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    let swaps = [
        SwapEvent {
            signature: "sig-a".to_string(),
            wallet: "wallet-1".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-a".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            slot: 100,
            ts_utc: base,
            exact_amounts: None,
        },
        SwapEvent {
            signature: "sig-b".to_string(),
            wallet: "wallet-1".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-b".to_string(),
            amount_in: 1.1,
            amount_out: 11.0,
            slot: 100,
            ts_utc: base,
            exact_amounts: None,
        },
        SwapEvent {
            signature: "sig-c".to_string(),
            wallet: "wallet-1".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-c".to_string(),
            amount_in: 1.2,
            amount_out: 12.0,
            slot: 101,
            ts_utc: base,
            exact_amounts: None,
        },
        SwapEvent {
            signature: "sig-d".to_string(),
            wallet: "wallet-1".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-d".to_string(),
            amount_in: 1.3,
            amount_out: 13.0,
            slot: 1,
            ts_utc: base + Duration::seconds(1),
            exact_amounts: None,
        },
    ];
    for swap in &swaps {
        assert!(store.insert_observed_swap(swap)?);
    }

    let mut seen = Vec::new();
    let count = store.for_each_observed_swap_after_cursor(base, 100, "sig-a", 10, |swap| {
        seen.push((swap.signature, swap.slot, swap.ts_utc));
        Ok(())
    })?;

    assert_eq!(count, 3);
    assert_eq!(
        seen,
        vec![
            ("sig-b".to_string(), 100, base),
            ("sig-c".to_string(), 101, base),
            ("sig-d".to_string(), 1, base + Duration::seconds(1)),
        ]
    );
    Ok(())
}

#[test]
fn observed_swap_cursor_query_respects_expired_deadline() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("observed-swap-expired-deadline.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    let base = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
        .expect("valid timestamp")
        .with_timezone(&Utc);
    assert!(store.insert_observed_swap(&SwapEvent {
        signature: "sig-deadline".to_string(),
        wallet: "wallet-deadline".to_string(),
        dex: "raydium".to_string(),
        token_in: "So11111111111111111111111111111111111111112".to_string(),
        token_out: "token-deadline".to_string(),
        amount_in: 1.0,
        amount_out: 10.0,
        slot: 10,
        ts_utc: base,
        exact_amounts: None,
    })?);

    let page = store.for_each_observed_swap_after_cursor_with_budget(
        base - Duration::seconds(1),
        0,
        "",
        10,
        std::time::Instant::now(),
        |_swap| Ok(()),
    )?;
    assert_eq!(page.rows_seen, 0);
    assert!(page.time_budget_exhausted);
    Ok(())
}

#[test]
fn observed_swap_since_with_budget_streams_oldest_rows_in_order() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("observed-swap-since-budget.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    let base = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
        .expect("valid timestamp")
        .with_timezone(&Utc);
    for (idx, signature) in ["sig-a", "sig-b", "sig-c"].into_iter().enumerate() {
        assert!(store.insert_observed_swap(&SwapEvent {
            signature: signature.to_string(),
            wallet: "wallet-since-budget".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: format!("token-{signature}"),
            amount_in: 1.0,
            amount_out: 10.0,
            slot: 10 + idx as u64,
            ts_utc: base + Duration::seconds(idx as i64),
            exact_amounts: None,
        })?);
    }

    let mut seen = Vec::new();
    let page = store.for_each_observed_swap_since_with_budget(
        base,
        2,
        std::time::Instant::now() + std::time::Duration::from_secs(1),
        |swap| {
            seen.push(swap.signature);
            Ok(())
        },
    )?;
    assert_eq!(page.rows_seen, 2);
    assert!(!page.time_budget_exhausted);
    assert_eq!(seen, vec!["sig-a".to_string(), "sig-b".to_string()]);
    Ok(())
}

#[test]
fn observed_buy_mint_page_query_returns_distinct_mints_and_resumes_by_token() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("observed-buy-mint-page-query.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    let base = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
        .expect("valid timestamp")
        .with_timezone(&Utc);
    for swap in [
        SwapEvent {
            signature: "buy-b-1".to_string(),
            wallet: "wallet-buy-page".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-b".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            slot: 10,
            ts_utc: base,
            exact_amounts: None,
        },
        SwapEvent {
            signature: "buy-a-1".to_string(),
            wallet: "wallet-buy-page".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-a".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            slot: 11,
            ts_utc: base + Duration::seconds(1),
            exact_amounts: None,
        },
        SwapEvent {
            signature: "buy-b-2".to_string(),
            wallet: "wallet-buy-page".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-b".to_string(),
            amount_in: 1.0,
            amount_out: 11.0,
            slot: 12,
            ts_utc: base + Duration::seconds(2),
            exact_amounts: None,
        },
        SwapEvent {
            signature: "sell-noise".to_string(),
            wallet: "wallet-sell-noise".to_string(),
            dex: "raydium".to_string(),
            token_in: "token-z".to_string(),
            token_out: "So11111111111111111111111111111111111111112".to_string(),
            amount_in: 10.0,
            amount_out: 0.8,
            slot: 13,
            ts_utc: base + Duration::seconds(3),
            exact_amounts: None,
        },
        SwapEvent {
            signature: "buy-c-1".to_string(),
            wallet: "wallet-buy-page".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-c".to_string(),
            amount_in: 1.0,
            amount_out: 12.0,
            slot: 14,
            ts_utc: base + Duration::seconds(4),
            exact_amounts: None,
        },
    ] {
        assert!(store.insert_observed_swap(&swap)?);
    }

    let first_page = store.load_observed_buy_mints_in_window_after_token_with_budget(
        base - Duration::seconds(1),
        base + Duration::seconds(10),
        None,
        2,
        std::time::Instant::now() + StdDuration::from_secs(1),
    )?;
    assert!(!first_page.time_budget_exhausted);
    assert_eq!(
        first_page.mints,
        vec!["token-a".to_string(), "token-b".to_string()]
    );

    let second_page = store.load_observed_buy_mints_in_window_after_token_with_budget(
        base - Duration::seconds(1),
        base + Duration::seconds(10),
        Some("token-b"),
        2,
        std::time::Instant::now() + StdDuration::from_secs(1),
    )?;
    assert!(!second_page.time_budget_exhausted);
    assert_eq!(second_page.mints, vec!["token-c".to_string()]);
    Ok(())
}
