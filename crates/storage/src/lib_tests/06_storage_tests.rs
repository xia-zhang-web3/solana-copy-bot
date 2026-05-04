    #[test]
    fn prune_discovery_scoring_keeps_old_open_lots_for_late_sell_accounting() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("discovery-scoring-open-lot-retention.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let buy = SwapEvent {
            signature: "carryover-buy".to_string(),
            wallet: "wallet-carryover".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenCarryover111111111111111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 100.0,
            exact_amounts: None,
            slot: 1,
            ts_utc: DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        store.insert_observed_swaps_batch(&[buy.clone()])?;
        store.apply_discovery_scoring_batch(&[buy], &DiscoveryAggregateWriteConfig::default())?;

        let cutoff = DateTime::parse_from_rfc3339("2026-02-10T00:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        store.prune_discovery_scoring_before(cutoff)?;

        let open_lot_count: i64 =
            store
                .conn
                .query_row("SELECT COUNT(*) FROM wallet_scoring_open_lots", [], |row| {
                    row.get(0)
                })?;
        assert_eq!(
            open_lot_count, 1,
            "still-open scoring inventory must survive retention prune until consumed"
        );

        let carryover_count: i64 = store.conn.query_row(
            "SELECT COUNT(*) FROM wallet_scoring_carryover_lots",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(carryover_count, 0);

        let sell = SwapEvent {
            signature: "carryover-sell".to_string(),
            wallet: "wallet-carryover".to_string(),
            dex: "raydium".to_string(),
            token_in: "TokenCarryover111111111111111111111111111".to_string(),
            token_out: "So11111111111111111111111111111111111111112".to_string(),
            amount_in: 100.0,
            amount_out: 2.0,
            exact_amounts: None,
            slot: 2,
            ts_utc: DateTime::parse_from_rfc3339("2026-02-15T00:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        };
        store.insert_observed_swaps_batch(&[sell.clone()])?;
        store.apply_discovery_scoring_batch(&[sell], &DiscoveryAggregateWriteConfig::default())?;

        let close_fact_count: i64 = store.conn.query_row(
            "SELECT COUNT(*) FROM wallet_scoring_close_facts
             WHERE sell_signature = 'carryover-sell'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(close_fact_count, 1);
        let (pnl_sol, hold_seconds): (f64, i64) = store.conn.query_row(
            "SELECT pnl_sol, hold_seconds
             FROM wallet_scoring_close_facts
             WHERE sell_signature = 'carryover-sell'
               AND segment_index = 0",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert!((pnl_sol - 1.0).abs() < 1e-9);
        assert!(hold_seconds > 0);

        let remaining_open_lots: i64 = store.conn.query_row(
            "SELECT COUNT(*) FROM wallet_scoring_open_lots
             WHERE wallet_id = 'wallet-carryover'
               AND token = 'TokenCarryover111111111111111111111111111'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(remaining_open_lots, 0);
        Ok(())
    }

    #[test]
    fn prune_discovery_scoring_before_batched_chunks_retention_work() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("discovery-scoring-retention-batched.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let buy_one = SwapEvent {
            signature: "scoring-retention-buy-1".to_string(),
            wallet: "wallet-scoring-retention".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenRetention111111111111111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 100.0,
            exact_amounts: None,
            slot: 1,
            ts_utc: DateTime::parse_from_rfc3339("2026-02-01T00:00:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        };
        let buy_two = SwapEvent {
            signature: "scoring-retention-buy-2".to_string(),
            wallet: "wallet-scoring-retention".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenRetention111111111111111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 50.0,
            exact_amounts: None,
            slot: 2,
            ts_utc: DateTime::parse_from_rfc3339("2026-02-02T00:01:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        };
        let swaps = vec![buy_one, buy_two];
        store.insert_observed_swaps_batch(&swaps)?;
        store.apply_discovery_scoring_batch(&swaps, &DiscoveryAggregateWriteConfig::default())?;

        let cutoff = DateTime::parse_from_rfc3339("2026-02-10T00:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let summary = store.prune_discovery_scoring_before_batched(cutoff, 3)?;
        assert_eq!(summary.batches, 2);
        assert_eq!(summary.deleted_rows, 6);

        for table in [
            "wallet_scoring_buy_facts",
            "wallet_scoring_tx_minutes",
            "wallet_scoring_days",
        ] {
            let count: i64 =
                store
                    .conn
                    .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                        row.get(0)
                    })?;
            assert_eq!(
                count, 0,
                "expected {table} to be pruned by batched retention"
            );
        }
        Ok(())
    }

    #[test]
    fn observed_swap_batch_with_activity_days_is_atomic_on_activity_upsert_failure() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("observed-swap-activity-atomic.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        store.conn.execute_batch(
            "CREATE TRIGGER fail_wallet_activity_days_insert
             BEFORE INSERT ON wallet_activity_days
             BEGIN
                 SELECT RAISE(FAIL, 'forced wallet activity day failure');
             END;",
        )?;

        let swap = SwapEvent {
            signature: "atomic-activity-fail".to_string(),
            wallet: "wallet-atomic".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenAtomic11111111111111111111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 100.0,
            exact_amounts: None,
            slot: 1,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-08T12:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        };

        let error = store
            .insert_observed_swaps_batch_with_activity_days(&[swap.clone()])
            .expect_err("wallet_activity_days failure should abort the whole batch");
        let error_chain = format!("{error:#}");
        assert!(
            error_chain.contains("forced wallet activity day failure"),
            "unexpected atomic batch error: {error_chain}"
        );

        let swaps = store.load_observed_swaps_since(
            DateTime::parse_from_rfc3339("2026-03-08T11:59:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        )?;
        assert!(
            swaps.is_empty(),
            "observed_swaps insert must roll back when wallet_activity_days upsert fails"
        );
        let counts = store.wallet_active_day_counts_since(
            &["wallet-atomic".to_string()],
            DateTime::parse_from_rfc3339("2026-03-08T00:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        )?;
        assert!(
            counts.is_empty(),
            "wallet_activity_days must also remain empty"
        );
        Ok(())
    }

    #[test]
    fn wallet_metrics_window_start_index_migration_is_present() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("wallet-metrics-window-start-index.db");
        let legacy_migrations = temp.path().join("legacy-migrations");
        copy_migrations_through(&legacy_migrations, "0020_execution_foreign_keys.sql")?;

        let mut legacy_store = SqliteStore::open(Path::new(&db_path))?;
        legacy_store.run_migrations(&legacy_migrations)?;
        drop(legacy_store);

        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut migrated_store = SqliteStore::open(Path::new(&db_path))?;
        migrated_store.run_migrations(&migration_dir)?;

        let index_sql: Option<String> = migrated_store
            .conn
            .query_row(
                "SELECT sql
                 FROM sqlite_master
                 WHERE type = 'index' AND name = 'idx_wallet_metrics_window_start'",
                [],
                |row| row.get(0),
            )
            .optional()?;
        assert!(
            index_sql.is_some(),
            "wallet_metrics(window_start) hotfix index must exist after migration"
        );
        Ok(())
    }

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
