use super::*;

#[test]
fn observed_wallet_activity_page_for_exact_wallets_exhausted_after_wallet_id_page_surfaces_pre_row_progress_stage1(
) -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp
        .path()
        .join("wallet-activity-page-exact-wallet-pre-row-exhausted.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    store.run_migrations(&migration_dir)?;

    let since = DateTime::parse_from_rfc3339("2026-04-11T11:00:00Z")
        .expect("ts")
        .with_timezone(&Utc);
    let until = since + Duration::hours(6);
    let exact_wallet_ids = seed_wallet_activity_interrupt_fixture(
        &mut store,
        since,
        25_000,
        "wallet-activity-exact-wallet-pre-row-exhausted",
    )?;

    let _guard = force_exact_wallet_activity_pre_row_budget_exhaustion_for_test();
    let page = store.observed_wallet_activity_page_for_exact_wallets_in_window_with_budget(
        &exact_wallet_ids,
        since,
        until,
        None,
        3,
        50,
        Instant::now() + StdDuration::from_secs(5),
    )?;

    assert!(page.time_budget_exhausted);
    assert!(page.rows.is_empty());
    assert_eq!(page.rows_seen, 0);
    assert!(
            page.wallet_id_page_wallets_seen > 0,
            "the exact-wallet helper must surface wallet-id prefilter progress even when budget exhaustion happens before the first materialized activity row"
        );
    assert!(
            !page.wallet_id_query_exhausted_before_first_page,
            "the zero-progress marker must stay reserved for the earlier wallet-id query seam; once a wallet-id page is known this path should stage/resume that page instead"
        );
    assert!(
            page.wallet_id_page_cursor_after.is_some(),
            "the exact-wallet helper must surface the last wallet-id cursor reached before pre-row exhaustion"
        );
    Ok(())
}
