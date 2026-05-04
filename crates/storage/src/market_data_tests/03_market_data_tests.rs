    #[test]
    fn observed_wallet_activity_page_does_not_count_future_until_day_from_fast_path() -> Result<()>
    {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("wallet-activity-page-future-until-day.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let window_start = DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let window_end = DateTime::parse_from_rfc3339("2026-03-07T12:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);

        store.insert_observed_swap(&swap(
            "sig-wallet-until-future-in-window",
            "wallet-until-future",
            DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
            SOL_MINT,
            "TokenUntilFuture111111111111111111111111111",
            1,
        ))?;
        store.insert_observed_swap(&swap(
            "sig-wallet-until-future-after-window",
            "wallet-until-future",
            DateTime::parse_from_rfc3339("2026-03-07T20:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
            SOL_MINT,
            "TokenUntilFuture111111111111111111111111111",
            2,
        ))?;
        store.insert_observed_swap(&swap(
            "sig-wallet-until-present-day-one",
            "wallet-until-present",
            DateTime::parse_from_rfc3339("2026-03-06T13:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
            SOL_MINT,
            "TokenUntilPresent11111111111111111111111111",
            3,
        ))?;
        store.insert_observed_swap(&swap(
            "sig-wallet-until-present-day-two",
            "wallet-until-present",
            DateTime::parse_from_rfc3339("2026-03-07T11:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
            SOL_MINT,
            "TokenUntilPresent11111111111111111111111111",
            4,
        ))?;
        store.upsert_wallet_activity_days(&[
            WalletActivityDayRow {
                wallet_id: "wallet-until-future".to_string(),
                activity_day: DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
                    .expect("ts")
                    .with_timezone(&Utc)
                    .date_naive(),
                last_seen: DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
                    .expect("ts")
                    .with_timezone(&Utc),
            },
            WalletActivityDayRow {
                wallet_id: "wallet-until-future".to_string(),
                activity_day: DateTime::parse_from_rfc3339("2026-03-07T20:00:00Z")
                    .expect("ts")
                    .with_timezone(&Utc)
                    .date_naive(),
                last_seen: DateTime::parse_from_rfc3339("2026-03-07T20:00:00Z")
                    .expect("ts")
                    .with_timezone(&Utc),
            },
            WalletActivityDayRow {
                wallet_id: "wallet-until-present".to_string(),
                activity_day: DateTime::parse_from_rfc3339("2026-03-06T13:00:00Z")
                    .expect("ts")
                    .with_timezone(&Utc)
                    .date_naive(),
                last_seen: DateTime::parse_from_rfc3339("2026-03-06T13:00:00Z")
                    .expect("ts")
                    .with_timezone(&Utc),
            },
            WalletActivityDayRow {
                wallet_id: "wallet-until-present".to_string(),
                activity_day: DateTime::parse_from_rfc3339("2026-03-07T11:00:00Z")
                    .expect("ts")
                    .with_timezone(&Utc)
                    .date_naive(),
                last_seen: DateTime::parse_from_rfc3339("2026-03-07T11:00:00Z")
                    .expect("ts")
                    .with_timezone(&Utc),
            },
        ])?;

        let page = store.observed_wallet_activity_page_in_window_with_budget(
            window_start,
            window_end,
            None,
            10,
            50,
            Instant::now() + StdDuration::from_secs(5),
        )?;
        assert!(!page.time_budget_exhausted);
        assert_eq!(
            page.active_day_count_source,
            Some(ObservedWalletActivityDayCountSource::WalletActivityDays)
        );
        let by_wallet: HashMap<String, ObservedWalletActivityRow> = page
            .rows
            .into_iter()
            .map(|row| (row.wallet_id.clone(), row))
            .collect();
        assert_eq!(
            by_wallet["wallet-until-future"].active_day_count,
            1,
            "future activity after until on the same day must not inflate fast-path active_day_count"
        );
        assert_eq!(by_wallet["wallet-until-present"].active_day_count, 2);
        Ok(())
    }

    #[test]
    fn observed_wallet_activity_page_for_exact_wallets_filters_and_resumes_in_order_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("wallet-activity-page-exact-wallet-filter.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let since = DateTime::parse_from_rfc3339("2026-04-11T09:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let until = since + Duration::hours(4);
        store.insert_observed_swap(&swap(
            "sig-exact-wallet-a-1",
            "wallet-exact-a",
            since + Duration::minutes(10),
            SOL_MINT,
            "TokenExactWalletA11111111111111111111111111",
            1,
        ))?;
        store.insert_observed_swap(&swap(
            "sig-exact-wallet-a-2",
            "wallet-exact-a",
            since + Duration::minutes(20),
            "TokenExactWalletA11111111111111111111111111",
            SOL_MINT,
            2,
        ))?;
        store.insert_observed_swap(&swap(
            "sig-exact-wallet-b-1",
            "wallet-exact-b",
            since + Duration::minutes(30),
            SOL_MINT,
            "TokenExactWalletB11111111111111111111111111",
            3,
        ))?;
        store.insert_observed_swap(&swap(
            "sig-exact-wallet-irrelevant",
            "wallet-exact-irrelevant",
            since + Duration::minutes(40),
            SOL_MINT,
            "TokenExactWalletIrrelevant1111111111111111",
            4,
        ))?;

        let exact_wallet_ids = vec!["wallet-exact-b".to_string(), "wallet-exact-a".to_string()];
        let first_page = store
            .observed_wallet_activity_page_for_exact_wallets_in_window_with_budget(
                &exact_wallet_ids,
                since,
                until,
                None,
                1,
                50,
                Instant::now() + StdDuration::from_secs(5),
            )?;
        assert!(!first_page.time_budget_exhausted);
        assert_eq!(first_page.rows_seen, 2);
        assert_eq!(first_page.rows.len(), 1);
        assert_eq!(first_page.rows[0].wallet_id, "wallet-exact-a");
        assert_eq!(first_page.rows[0].trades, 2);

        let second_page = store
            .observed_wallet_activity_page_for_exact_wallets_in_window_with_budget(
                &exact_wallet_ids,
                since,
                until,
                Some("wallet-exact-a"),
                1,
                50,
                Instant::now() + StdDuration::from_secs(5),
            )?;
        assert!(!second_page.time_budget_exhausted);
        assert_eq!(second_page.rows_seen, 1);
        assert_eq!(second_page.rows.len(), 1);
        assert_eq!(second_page.rows[0].wallet_id, "wallet-exact-b");
        assert_eq!(second_page.rows[0].trades, 1);
        Ok(())
    }

    #[test]
    fn observed_wallet_activity_target_wallet_filter_reuses_identical_wallet_set_across_pages_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("wallet-activity-exact-wallet-filter-cache-reuse.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let since = DateTime::parse_from_rfc3339("2026-04-11T09:30:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let until = since + Duration::hours(2);
        store.insert_observed_swap(&swap(
            "sig-wallet-filter-cache-a",
            "wallet-filter-cache-a",
            since + Duration::minutes(5),
            SOL_MINT,
            "TokenWalletFilterCacheA11111111111111111111",
            1,
        ))?;
        store.insert_observed_swap(&swap(
            "sig-wallet-filter-cache-b",
            "wallet-filter-cache-b",
            since + Duration::minutes(10),
            SOL_MINT,
            "TokenWalletFilterCacheB11111111111111111111",
            2,
        ))?;

        let exact_wallet_ids = vec![
            "wallet-filter-cache-b".to_string(),
            "wallet-filter-cache-a".to_string(),
        ];
        let before_first = store.conn.total_changes();
        let first_page = store
            .observed_wallet_activity_page_for_exact_wallets_in_window_with_budget(
                &exact_wallet_ids,
                since,
                until,
                None,
                1,
                50,
                Instant::now() + StdDuration::from_secs(5),
            )?;
        let after_first = store.conn.total_changes();
        let second_page = store
            .observed_wallet_activity_page_for_exact_wallets_in_window_with_budget(
                &exact_wallet_ids,
                since,
                until,
                Some("wallet-filter-cache-a"),
                1,
                50,
                Instant::now() + StdDuration::from_secs(5),
            )?;
        let after_second = store.conn.total_changes();

        assert_eq!(first_page.rows.len(), 1);
        assert_eq!(second_page.rows.len(), 1);
        assert!(
            after_first > before_first,
            "the first exact-wallet backfill page must populate the temporary wallet filter"
        );
        assert_eq!(
            after_second, after_first,
            "reusing the same exact candidate-wallet set on the same SQLite connection must not rewrite the temporary wallet filter rows on the next page"
        );
        Ok(())
    }

    #[test]
    fn observed_wallet_activity_target_wallet_filter_reloads_when_wallet_set_changes_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("wallet-activity-exact-wallet-filter-cache-invalidate.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let since = DateTime::parse_from_rfc3339("2026-04-11T09:45:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let until = since + Duration::hours(2);
        store.insert_observed_swap(&swap(
            "sig-wallet-filter-invalidate-a",
            "wallet-filter-invalidate-a",
            since + Duration::minutes(5),
            SOL_MINT,
            "TokenWalletFilterInvalidateA1111111111111111",
            1,
        ))?;
        store.insert_observed_swap(&swap(
            "sig-wallet-filter-invalidate-b",
            "wallet-filter-invalidate-b",
            since + Duration::minutes(10),
            SOL_MINT,
            "TokenWalletFilterInvalidateB1111111111111111",
            2,
        ))?;

        let first_wallet_ids = vec!["wallet-filter-invalidate-a".to_string()];
        let second_wallet_ids = vec![
            "wallet-filter-invalidate-a".to_string(),
            "wallet-filter-invalidate-b".to_string(),
        ];
        let before_first = store.conn.total_changes();
        store.observed_wallet_activity_page_for_exact_wallets_in_window_with_budget(
            &first_wallet_ids,
            since,
            until,
            None,
            1,
            50,
            Instant::now() + StdDuration::from_secs(5),
        )?;
        let after_first = store.conn.total_changes();
        store.observed_wallet_activity_page_for_exact_wallets_in_window_with_budget(
            &second_wallet_ids,
            since,
            until,
            None,
            2,
            50,
            Instant::now() + StdDuration::from_secs(5),
        )?;
        let after_second = store.conn.total_changes();

        assert!(
            after_first > before_first,
            "the first exact wallet-set load must populate the temporary filter"
        );
        assert!(
            after_second > after_first,
            "changing the exact candidate-wallet set must invalidate and rewrite the temporary wallet filter"
        );
        Ok(())
    }

    #[test]
    fn observed_wallet_activity_page_interrupt_on_wallet_id_query_preserves_time_budget_exhausted_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("wallet-activity-page-wallet-id-interrupt.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let since = DateTime::parse_from_rfc3339("2026-04-11T10:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let until = since + Duration::hours(6);
        seed_wallet_activity_interrupt_fixture(
            &mut store,
            since,
            25_000,
            "wallet-activity-wallet-id-interrupt",
        )?;

        let (stop_interrupts, interrupter) = spawn_interrupt_loop(
            store.conn.get_interrupt_handle(),
            StdDuration::from_millis(0),
        );
        let page = store.observed_wallet_activity_page_in_window_with_budget(
            since,
            until,
            None,
            900,
            50,
            Instant::now() + StdDuration::from_secs(5),
        )?;
        stop_interrupts.store(true, Ordering::Relaxed);
        interrupter.join().expect("interrupt loop thread panicked");

        assert!(
            page.time_budget_exhausted,
            "an interrupted wallet-id page query on the broad wallet-activity path must surface as time-budget exhaustion instead of a successful empty page"
        );
        assert!(
            page.rows.is_empty(),
            "the interrupted broad wallet-id page must not return a misleading successful wallet row set"
        );
        assert_eq!(page.rows_seen, 0);
        assert_eq!(page.active_day_count_source, None);
        Ok(())
    }

    #[test]
    fn observed_wallet_activity_page_for_exact_wallets_interrupt_on_wallet_id_query_preserves_time_budget_exhausted_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("wallet-activity-page-exact-wallet-id-interrupt.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let since = DateTime::parse_from_rfc3339("2026-04-11T10:30:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let until = since + Duration::hours(6);
        let exact_wallet_ids = seed_wallet_activity_interrupt_fixture(
            &mut store,
            since,
            25_000,
            "wallet-activity-exact-wallet-id-interrupt",
        )?;

        let preload = store.observed_wallet_activity_page_for_exact_wallets_in_window_with_budget(
            &exact_wallet_ids,
            since,
            until,
            None,
            1,
            50,
            Instant::now() + StdDuration::from_secs(5),
        )?;
        assert!(
            !preload.time_budget_exhausted && preload.rows.len() == 1,
            "the exact-wallet interrupt repro must preload the candidate-wallet temp filter successfully before targeting the wallet-id page seam"
        );

        let page = store.observed_wallet_activity_page_for_exact_wallets_in_window_with_budget(
            &exact_wallet_ids,
            since,
            until,
            None,
            900,
            50,
            Instant::now() + StdDuration::from_millis(1),
        )?;

        assert!(
            page.time_budget_exhausted,
            "an interrupted exact-wallet wallet-id page query must remain a time-budget outcome instead of collapsing into an empty successful page"
        );
        assert!(
            page.rows.is_empty(),
            "the interrupted exact-wallet wallet-id page must not return a misleading successful wallet row set"
        );
        assert_eq!(page.rows_seen, 0);
        assert_eq!(page.active_day_count_source, None);
        assert!(
            page.wallet_id_query_exhausted_before_first_page,
            "the exact-wallet helper must surface an explicit zero-progress seam marker when budget exhaustion happens inside the wallet-id page query before any truthful page boundary exists"
        );
        assert_eq!(page.wallet_id_page_wallets_seen, 0);
        assert_eq!(page.wallet_id_page_cursor_after, None);
        Ok(())
    }

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
