use super::*;

#[test]
fn observed_buy_mint_page_query_respects_exclusive_time_bounds() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("observed-buy-mint-page-time-bounds.db");
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
            wallet: "wallet-buy-page-bounds".to_string(),
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

    let lower_exclusive = store.load_observed_buy_mints_in_time_bounds_after_token_with_budget(
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
        lower_exclusive.mints,
        vec!["token-b".to_string(), "token-c".to_string()]
    );

    let upper_exclusive = store.load_observed_buy_mints_in_time_bounds_after_token_with_budget(
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
        upper_exclusive.mints,
        vec!["token-a".to_string(), "token-b".to_string()]
    );
    Ok(())
}

#[test]
fn observed_buy_mint_occurrence_count_query_respects_exclusive_time_bounds() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp
        .path()
        .join("observed-buy-mint-occurrence-count-time-bounds.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    let base = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
        .expect("valid timestamp")
        .with_timezone(&Utc);
    for (signature, token, ts) in [
        ("buy-a-1", "token-a", base + Duration::seconds(1)),
        ("buy-a-2", "token-a", base + Duration::seconds(2)),
        ("buy-b-1", "token-b", base + Duration::seconds(3)),
    ] {
        assert!(store.insert_observed_swap(&SwapEvent {
            signature: signature.to_string(),
            wallet: "wallet-buy-page-bounds".to_string(),
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

    let lower_exclusive = store.count_observed_buy_mint_occurrences_in_time_bounds_with_budget(
        base + Duration::seconds(1),
        false,
        base + Duration::seconds(3),
        true,
        "token-a",
        std::time::Instant::now() + StdDuration::from_secs(1),
    )?;
    assert!(!lower_exclusive.time_budget_exhausted);
    assert_eq!(lower_exclusive.buy_count, 1);

    let upper_exclusive = store.count_observed_buy_mint_occurrences_in_time_bounds_with_budget(
        base + Duration::seconds(1),
        true,
        base + Duration::seconds(2),
        false,
        "token-a",
        std::time::Instant::now() + StdDuration::from_secs(1),
    )?;
    assert!(!upper_exclusive.time_budget_exhausted);
    assert_eq!(upper_exclusive.buy_count, 1);
    Ok(())
}

#[test]
fn observed_buy_mint_exact_batch_count_query_counts_only_requested_tokens() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp
        .path()
        .join("observed-buy-mint-exact-batch-count-query.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    let base = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
        .expect("valid timestamp")
        .with_timezone(&Utc);
    for (signature, token, ts) in [
        ("buy-a-1", "token-a", base + Duration::seconds(1)),
        ("buy-a-2", "token-a", base + Duration::seconds(2)),
        ("buy-b-1", "token-b", base + Duration::seconds(3)),
        ("buy-c-1", "token-c", base + Duration::seconds(4)),
    ] {
        assert!(store.insert_observed_swap(&SwapEvent {
            signature: signature.to_string(),
            wallet: "wallet-buy-batch-count".to_string(),
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

    let page = store.load_observed_buy_mint_counts_for_exact_tokens_in_time_bounds_with_budget(
        base + Duration::seconds(1),
        true,
        base + Duration::seconds(4),
        true,
        &[
            "token-a".to_string(),
            "token-c".to_string(),
            "token-z".to_string(),
        ],
        std::time::Instant::now() + StdDuration::from_secs(1),
    )?;
    assert!(!page.time_budget_exhausted);
    assert_eq!(
        page.rows
            .iter()
            .map(|row| (row.mint.as_str(), row.buy_count))
            .collect::<Vec<_>>(),
        vec![("token-a", 2usize), ("token-c", 1usize)]
    );
    Ok(())
}

#[test]
fn observed_buy_mint_count_query_counts_safe_sorted_prefix_for_cursor_migration() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("observed-buy-mint-count-query.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    let base = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
        .expect("valid timestamp")
        .with_timezone(&Utc);
    for swap in [
        SwapEvent {
            signature: "buy-d-1".to_string(),
            wallet: "wallet-buy-count".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-d".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            slot: 10,
            ts_utc: base,
            exact_amounts: None,
        },
        SwapEvent {
            signature: "buy-a-1".to_string(),
            wallet: "wallet-buy-count".to_string(),
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
            signature: "buy-b-1".to_string(),
            wallet: "wallet-buy-count".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-b".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            slot: 12,
            ts_utc: base + Duration::seconds(2),
            exact_amounts: None,
        },
        SwapEvent {
            signature: "buy-c-1".to_string(),
            wallet: "wallet-buy-count".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-c".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            slot: 13,
            ts_utc: base + Duration::seconds(3),
            exact_amounts: None,
        },
    ] {
        assert!(store.insert_observed_swap(&swap)?);
    }

    let count_through_b = store.count_observed_buy_mints_in_window_up_to_token_with_budget(
        base - Duration::seconds(1),
        base + Duration::seconds(10),
        "token-b",
        std::time::Instant::now() + StdDuration::from_secs(1),
    )?;
    assert!(!count_through_b.time_budget_exhausted);
    assert_eq!(count_through_b.count, 2);

    let count_through_d = store.count_observed_buy_mints_in_window_up_to_token_with_budget(
        base - Duration::seconds(1),
        base + Duration::seconds(10),
        "token-d",
        std::time::Instant::now() + StdDuration::from_secs(1),
    )?;
    assert!(!count_through_d.time_budget_exhausted);
    assert_eq!(count_through_d.count, 4);
    Ok(())
}

#[test]
fn observed_buy_mint_count_page_query_returns_grouped_counts_and_resumes() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("observed-buy-mint-count-page-query.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    let base = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
        .expect("valid timestamp")
        .with_timezone(&Utc);
    for swap in [
        SwapEvent {
            signature: "buy-a-1".to_string(),
            wallet: "wallet-buy-count-page".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-a".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            slot: 10,
            ts_utc: base,
            exact_amounts: None,
        },
        SwapEvent {
            signature: "buy-a-2".to_string(),
            wallet: "wallet-buy-count-page".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-a".to_string(),
            amount_in: 1.0,
            amount_out: 11.0,
            slot: 11,
            ts_utc: base + Duration::seconds(1),
            exact_amounts: None,
        },
        SwapEvent {
            signature: "buy-b-1".to_string(),
            wallet: "wallet-buy-count-page".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-b".to_string(),
            amount_in: 1.0,
            amount_out: 12.0,
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
            signature: "buy-c-outside".to_string(),
            wallet: "wallet-buy-count-page".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-c".to_string(),
            amount_in: 1.0,
            amount_out: 13.0,
            slot: 14,
            ts_utc: base + Duration::seconds(20),
            exact_amounts: None,
        },
    ] {
        assert!(store.insert_observed_swap(&swap)?);
    }

    let first_page = store.load_observed_buy_mint_counts_in_window_after_token_with_budget(
        base - Duration::seconds(1),
        base + Duration::seconds(10),
        None,
        None,
        2,
        std::time::Instant::now() + StdDuration::from_secs(1),
    )?;
    assert!(!first_page.time_budget_exhausted);
    assert_eq!(first_page.rows.len(), 2);
    assert_eq!(first_page.rows[0].mint, "token-a");
    assert_eq!(first_page.rows[0].buy_count, 2);
    assert_eq!(first_page.rows[1].mint, "token-b");
    assert_eq!(first_page.rows[1].buy_count, 1);

    let bounded_page = store.load_observed_buy_mint_counts_in_window_after_token_with_budget(
        base - Duration::seconds(1),
        base + Duration::seconds(10),
        Some("token-a"),
        Some("token-b"),
        2,
        std::time::Instant::now() + StdDuration::from_secs(1),
    )?;
    assert!(!bounded_page.time_budget_exhausted);
    assert_eq!(bounded_page.rows.len(), 1);
    assert_eq!(bounded_page.rows[0].mint, "token-b");
    assert_eq!(bounded_page.rows[0].buy_count, 1);
    Ok(())
}
