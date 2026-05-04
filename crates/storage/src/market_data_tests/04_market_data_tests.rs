    #[test]
    fn observed_sol_leg_target_buy_mint_filter_old_like_reload_rewrites_identical_target_set_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("observed-sol-leg-target-filter-old-like-reload.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-11T08:10:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        store.insert_observed_swap(&swap(
            "sig-target-filter-old-like",
            "wallet-target-filter",
            now,
            SOL_MINT,
            "TokenTargetFilterPrimary1111111111111111111",
            1,
        ))?;

        let target_buy_mints: Vec<String> = (0..256)
            .map(|idx| format!("TokenTargetFilterSet{idx:04}11111111111111111111111"))
            .collect();

        let before_first = store.conn.total_changes();
        store.replace_observed_sol_leg_target_buy_mint_filter(&target_buy_mints)?;
        let after_first = store.conn.total_changes();
        store.replace_observed_sol_leg_target_buy_mint_filter(&target_buy_mints)?;
        let after_second = store.conn.total_changes();

        assert!(
            after_first > before_first,
            "old-like setup must write the temporary exact-target filter rows on first load"
        );
        assert!(
            after_second > after_first,
            "old-like behavior must rewrite the same exact target set again instead of reusing the already-loaded temporary filter"
        );
        Ok(())
    }

    #[test]
    fn observed_sol_leg_target_buy_mint_filter_reuses_identical_target_set_across_pages_stage1(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("observed-sol-leg-target-filter-cache-reuse.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-11T08:15:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        store.insert_observed_swap(&swap(
            "sig-target-filter-cache-a",
            "wallet-target-filter",
            now,
            SOL_MINT,
            "TokenTargetFilterPrimary1111111111111111111",
            1,
        ))?;
        store.insert_observed_swap(&swap(
            "sig-target-filter-cache-b",
            "wallet-target-filter",
            now + Duration::seconds(1),
            "TokenTargetFilterPrimary1111111111111111111",
            SOL_MINT,
            2,
        ))?;

        let mut target_buy_mints: Vec<String> = (0..512)
            .map(|idx| format!("TokenTargetFilterSet{idx:04}11111111111111111111111"))
            .collect();
        target_buy_mints.push("TokenTargetFilterPrimary1111111111111111111".to_string());

        let before_first = store.conn.total_changes();
        let first_page = store
            .for_each_observed_sol_leg_swap_in_window_after_cursor_for_target_buy_mints_with_budget(
                now - Duration::seconds(1),
                now + Duration::seconds(2),
                None,
                &target_buy_mints,
                1,
                Instant::now() + StdDuration::from_secs(5),
                |_swap| Ok(()),
            )?;
        let after_first = store.conn.total_changes();
        let second_page = store
            .for_each_observed_sol_leg_swap_in_window_after_cursor_for_target_buy_mints_with_budget(
                now - Duration::seconds(1),
                now + Duration::seconds(2),
                Some(&DiscoveryRuntimeCursor {
                    ts_utc: now,
                    slot: 1,
                    signature: "sig-target-filter-cache-a".to_string(),
                }),
                &target_buy_mints,
                1,
                Instant::now() + StdDuration::from_secs(5),
                |_swap| Ok(()),
            )?;
        let after_second = store.conn.total_changes();

        assert_eq!(first_page.rows_seen, 1);
        assert_eq!(second_page.rows_seen, 1);
        assert!(
            after_first > before_first,
            "the first exact-target page must populate the temporary filter cache"
        );
        assert_eq!(
            after_second, after_first,
            "reusing the same exact target set on the same SQLite connection must not rewrite the temporary filter rows on the next replay page"
        );
        Ok(())
    }

    #[test]
    fn observed_sol_leg_target_buy_mint_filter_reloads_when_target_set_changes_stage1() -> Result<()>
    {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("observed-sol-leg-target-filter-cache-invalidate.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-04-11T08:20:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        store.insert_observed_swap(&swap(
            "sig-target-filter-cache-invalidate",
            "wallet-target-filter",
            now,
            SOL_MINT,
            "TokenTargetFilterReload111111111111111111111",
            1,
        ))?;

        let first_target_buy_mints =
            vec!["TokenTargetFilterReload111111111111111111111".to_string()];
        let second_target_buy_mints = vec![
            "TokenTargetFilterReload111111111111111111111".to_string(),
            "TokenTargetFilterReloadSecond1111111111111111".to_string(),
        ];

        let before_first = store.conn.total_changes();
        let _ = store
            .for_each_observed_sol_leg_swap_in_window_after_cursor_for_target_buy_mints_with_budget(
                now - Duration::seconds(1),
                now + Duration::seconds(1),
                None,
                &first_target_buy_mints,
                1,
                Instant::now() + StdDuration::from_secs(5),
                |_swap| Ok(()),
            )?;
        let after_first = store.conn.total_changes();
        let _ = store
            .for_each_observed_sol_leg_swap_in_window_after_cursor_for_target_buy_mints_with_budget(
                now - Duration::seconds(1),
                now + Duration::seconds(1),
                None,
                &second_target_buy_mints,
                1,
                Instant::now() + StdDuration::from_secs(5),
                |_swap| Ok(()),
            )?;
        let after_second = store.conn.total_changes();

        assert!(
            after_first > before_first,
            "the first target set load must write the temporary filter rows"
        );
        assert!(
            after_second > after_first,
            "changing the exact target set must invalidate and reload the temporary filter cache instead of silently reusing stale membership"
        );
        Ok(())
    }
