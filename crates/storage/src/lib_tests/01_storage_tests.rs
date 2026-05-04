    #[test]
    fn shadow_lot_and_closed_trade_persist_exact_qty_sidecars() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("shadow-exact-qty-sidecars.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let opened_ts = DateTime::parse_from_rfc3339("2026-03-01T10:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let closed_ts = DateTime::parse_from_rfc3339("2026-03-01T11:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        store.insert_shadow_lot_exact(
            "wallet",
            "token",
            2.0,
            Some(TokenQuantity::new(2_000_000, 6)),
            0.20,
            opened_ts,
        )?;

        let lots = store.list_shadow_lots("wallet", "token")?;
        assert_eq!(lots.len(), 1);
        assert_eq!(
            lots[0].accounting_bucket,
            POSITION_ACCOUNTING_BUCKET_EXACT_POST_CUTOVER
        );
        assert_eq!(lots[0].qty_exact, Some(TokenQuantity::new(2_000_000, 6)));

        let close = store.close_shadow_lots_fifo_atomic_exact(
            "signal",
            "wallet",
            "token",
            0.5,
            Some(TokenQuantity::new(500_000, 6)),
            0.12,
            closed_ts,
        )?;
        assert!((close.closed_qty - 0.5).abs() < 1e-12);

        let lots = store.list_shadow_lots("wallet", "token")?;
        assert_eq!(lots.len(), 1);
        assert_eq!(
            lots[0].accounting_bucket,
            POSITION_ACCOUNTING_BUCKET_EXACT_POST_CUTOVER
        );
        assert_eq!(lots[0].qty_exact, Some(TokenQuantity::new(1_500_000, 6)));

        let closed_row: (String, Option<String>, Option<i64>) = store.conn.query_row(
            "SELECT accounting_bucket, qty_raw, qty_decimals
             FROM shadow_closed_trades
             WHERE signal_id = ?1",
            params!["signal"],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        assert_eq!(
            closed_row.0,
            POSITION_ACCOUNTING_BUCKET_EXACT_POST_CUTOVER.to_string()
        );
        assert_eq!(closed_row.1.as_deref(), Some("500000"));
        assert_eq!(closed_row.2, Some(6));
        Ok(())
    }

    #[test]
    fn shadow_fifo_close_preserves_bucket_provenance_across_legacy_and_exact_lots() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("shadow-bucket-fifo.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let opened_ts = DateTime::parse_from_rfc3339("2026-03-01T10:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let closed_ts = DateTime::parse_from_rfc3339("2026-03-01T11:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        store.insert_shadow_lot("wallet", "token", 1.0, 0.10, opened_ts)?;
        store.insert_shadow_lot_exact(
            "wallet",
            "token",
            1.0,
            Some(TokenQuantity::new(1_000_000, 6)),
            0.20,
            opened_ts + Duration::seconds(1),
        )?;

        let lots = store.list_shadow_lots("wallet", "token")?;
        assert_eq!(lots.len(), 2);
        assert_eq!(
            lots[0].accounting_bucket,
            POSITION_ACCOUNTING_BUCKET_LEGACY_PRE_CUTOVER
        );
        assert_eq!(
            lots[1].accounting_bucket,
            POSITION_ACCOUNTING_BUCKET_EXACT_POST_CUTOVER
        );

        let close = store.close_shadow_lots_fifo_atomic_exact(
            "signal-mixed",
            "wallet",
            "token",
            1.5,
            Some(TokenQuantity::new(1_500_000, 6)),
            0.30,
            closed_ts,
        )?;
        assert!((close.closed_qty - 1.5).abs() < 1e-12);
        assert!(close.has_open_lots_after);

        let lots = store.list_shadow_lots("wallet", "token")?;
        assert_eq!(lots.len(), 1);
        assert_eq!(
            lots[0].accounting_bucket,
            POSITION_ACCOUNTING_BUCKET_EXACT_POST_CUTOVER
        );
        assert!((lots[0].qty - 0.5).abs() < 1e-12);
        assert_eq!(lots[0].qty_exact, Some(TokenQuantity::new(500_000, 6)));

        let closed_rows: Vec<(String, f64, Option<String>, Option<i64>)> = {
            let mut stmt = store.conn.prepare(
                "SELECT accounting_bucket, qty, qty_raw, qty_decimals
                 FROM shadow_closed_trades
                 WHERE signal_id = ?1
                 ORDER BY opened_ts ASC, id ASC",
            )?;
            let mapped = stmt.query_map(params!["signal-mixed"], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })?;
            mapped.collect::<rusqlite::Result<Vec<_>>>()?
        };
        assert_eq!(closed_rows.len(), 2);
        assert_eq!(
            closed_rows[0],
            (
                POSITION_ACCOUNTING_BUCKET_LEGACY_PRE_CUTOVER.to_string(),
                1.0_f64,
                None,
                None
            )
        );
        assert_eq!(
            closed_rows[1],
            (
                POSITION_ACCOUNTING_BUCKET_EXACT_POST_CUTOVER.to_string(),
                0.5_f64,
                Some("500000".to_string()),
                Some(6_i64)
            )
        );
        Ok(())
    }

    #[test]
    fn shadow_zero_raw_insert_shadow_lot_exact_rejects_zero_raw_exact_qty() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("shadow-zero-raw-lot-reject.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let opened_ts = DateTime::parse_from_rfc3339("2026-03-01T10:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let error = store
            .insert_shadow_lot_exact(
                "wallet",
                "token",
                0.5,
                Some(TokenQuantity::new(0, 6)),
                0.20,
                opened_ts,
            )
            .expect_err("zero-raw exact shadow lot must fail closed");
        let error_chain = format!("{error:#}");
        assert!(error_chain.contains("zero-raw exact quantity"));
        assert!(store.list_shadow_lots("wallet", "token")?.is_empty());
        Ok(())
    }

    #[test]
    fn shadow_zero_raw_insert_shadow_closed_trade_exact_rejects_zero_raw_exact_qty() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("shadow-zero-raw-closed-trade-reject.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let opened_ts = DateTime::parse_from_rfc3339("2026-03-01T10:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let closed_ts = opened_ts + Duration::hours(1);
        let error = store
            .insert_shadow_closed_trade_exact(
                "signal",
                "wallet",
                "token",
                0.5,
                Some(TokenQuantity::new(0, 6)),
                0.10,
                0.12,
                0.02,
                opened_ts,
                closed_ts,
            )
            .expect_err("zero-raw exact shadow closed trade must fail closed");
        let error_chain = format!("{error:#}");
        assert!(error_chain.contains("zero-raw exact quantity"));
        let count: i64 = store.conn.query_row(
            "SELECT COUNT(*) FROM shadow_closed_trades WHERE signal_id = ?1",
            params!["signal"],
            |row| row.get(0),
        )?;
        assert_eq!(count, 0);
        Ok(())
    }

    #[test]
    fn shadow_zero_raw_fifo_close_rejects_zero_raw_exact_segment_and_rolls_back() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("shadow-zero-raw-fifo-close-reject.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let opened_ts = DateTime::parse_from_rfc3339("2026-03-01T10:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let closed_ts = opened_ts + Duration::hours(1);

        store.insert_shadow_lot_exact(
            "wallet",
            "token",
            0.5,
            Some(TokenQuantity::new(500_000, 6)),
            0.10,
            opened_ts,
        )?;
        store.insert_shadow_lot_exact(
            "wallet",
            "token",
            0.5,
            Some(TokenQuantity::new(500_000, 6)),
            0.10,
            opened_ts + Duration::seconds(1),
        )?;

        let error = store
            .close_shadow_lots_fifo_atomic_exact(
                "signal-zero-raw-segment",
                "wallet",
                "token",
                1.0,
                Some(TokenQuantity::new(1, 6)),
                0.25,
                closed_ts,
            )
            .expect_err("zero-raw exact fifo segment must fail closed");
        let error_chain = format!("{error:#}");
        assert!(error_chain.contains("zero-raw exact quantity"));

        let lots = store.list_shadow_lots("wallet", "token")?;
        assert_eq!(lots.len(), 2, "failed close must roll back lot mutation");
        assert_eq!(lots[0].qty_exact, Some(TokenQuantity::new(500_000, 6)));
        assert_eq!(lots[1].qty_exact, Some(TokenQuantity::new(500_000, 6)));
        let closed_count: i64 = store.conn.query_row(
            "SELECT COUNT(*) FROM shadow_closed_trades WHERE signal_id = ?1",
            params!["signal-zero-raw-segment"],
            |row| row.get(0),
        )?;
        assert_eq!(
            closed_count, 0,
            "failed close must not persist closed trades"
        );
        Ok(())
    }

    #[test]
    fn shadow_zero_raw_fifo_close_allows_legitimate_exact_full_close() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("shadow-exact-full-close-ok.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let opened_ts = DateTime::parse_from_rfc3339("2026-03-01T10:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let closed_ts = opened_ts + Duration::hours(1);

        store.insert_shadow_lot_exact(
            "wallet",
            "token",
            1.0,
            Some(TokenQuantity::new(1_000_000, 6)),
            0.10,
            opened_ts,
        )?;

        let close = store.close_shadow_lots_fifo_atomic_exact(
            "signal-full-close",
            "wallet",
            "token",
            1.0,
            Some(TokenQuantity::new(1_000_000, 6)),
            0.25,
            closed_ts,
        )?;
        assert!((close.closed_qty - 1.0).abs() < 1e-12);
        assert!(!close.has_open_lots_after);

        let lots = store.list_shadow_lots("wallet", "token")?;
        assert!(
            lots.is_empty(),
            "exact full close should delete the open lot instead of erroring"
        );
        let closed_row: (String, Option<String>, Option<i64>) = store.conn.query_row(
            "SELECT accounting_bucket, qty_raw, qty_decimals
             FROM shadow_closed_trades
             WHERE signal_id = ?1",
            params!["signal-full-close"],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        assert_eq!(
            closed_row.0,
            POSITION_ACCOUNTING_BUCKET_EXACT_POST_CUTOVER.to_string()
        );
        assert_eq!(closed_row.1.as_deref(), Some("1000000"));
        assert_eq!(closed_row.2, Some(6));
        Ok(())
    }

    #[test]
    fn shadow_risk_metrics_prefer_closed_trade_lamport_sidecars() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("shadow-closed-trade-lamports.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let opened_ts = DateTime::parse_from_rfc3339("2026-03-01T10:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let closed_ts = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        store.conn.execute(
            "INSERT INTO shadow_closed_trades(
                signal_id, wallet_id, token, qty,
                entry_cost_sol, entry_cost_lamports,
                exit_value_sol, exit_value_lamports,
                pnl_sol, pnl_lamports,
                opened_ts, closed_ts
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                "sig-shadow",
                "wallet",
                "token",
                10.0_f64,
                0.10_f64,
                200_000_000_i64,
                0.05_f64,
                50_000_000_i64,
                -0.05_f64,
                -150_000_000_i64,
                opened_ts.to_rfc3339(),
                closed_ts.to_rfc3339()
            ],
        )?;

        let (trades, pnl_lamports) =
            store.shadow_realized_pnl_lamports_since(opened_ts - Duration::minutes(1))?;
        assert_eq!(trades, 1);
        assert_eq!(pnl_lamports, SignedLamports::new(-150_000_000));

        let (trades, pnl) = store.shadow_realized_pnl_since(opened_ts - Duration::minutes(1))?;
        assert_eq!(trades, 1);
        assert!(
            (pnl + 0.15).abs() < 1e-12,
            "expected realized pnl to prefer lamport sidecar, got {pnl}"
        );

        let rug_count =
            store.shadow_rug_loss_count_since(opened_ts - Duration::minutes(1), -0.70)?;
        assert_eq!(
            rug_count, 1,
            "expected rug-loss count to prefer exact lamport sidecars"
        );

        let (recent_rug_count, total_count, rug_rate) =
            store.shadow_rug_loss_rate_recent(opened_ts - Duration::minutes(1), 10, -0.70)?;
        assert_eq!(recent_rug_count, 1);
        assert_eq!(total_count, 1);
        assert!((rug_rate - 1.0).abs() < 1e-12);
        Ok(())
    }

    #[test]
    fn shadow_risk_metrics_ignore_stale_terminal_zero_close_context() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("shadow-risk-ignore-terminal-zero.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let opened_ts = DateTime::parse_from_rfc3339("2026-03-01T10:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let closed_ts = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        store.insert_shadow_closed_trade_exact_with_context(
            "sig-terminal-zero",
            "wallet",
            "token",
            10.0,
            None,
            0.10,
            0.0,
            -0.10,
            SHADOW_CLOSE_CONTEXT_STALE_TERMINAL_ZERO_PRICE,
            opened_ts,
            closed_ts,
        )?;

        let (all_trades, all_pnl) =
            store.shadow_realized_pnl_lamports_since(opened_ts - Duration::minutes(1))?;
        assert_eq!(all_trades, 1);
        assert_eq!(all_pnl, SignedLamports::new(-100_000_000));

        let (risk_trades, risk_pnl) =
            store.shadow_risk_realized_pnl_lamports_since(opened_ts - Duration::minutes(1))?;
        assert_eq!(risk_trades, 0);
        assert_eq!(risk_pnl, SignedLamports::ZERO);

        assert_eq!(
            store.shadow_rug_loss_count_since(opened_ts - Duration::minutes(1), -0.70)?,
            0
        );
        let (recent_rug_count, total_count, rug_rate) =
            store.shadow_rug_loss_rate_recent(opened_ts - Duration::minutes(1), 10, -0.70)?;
        assert_eq!(recent_rug_count, 0);
        assert_eq!(total_count, 0);
        assert_eq!(rug_rate, 0.0);
        Ok(())
    }

    #[test]
    fn shadow_risk_metrics_ignore_recovery_terminal_zero_close_context() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("shadow-risk-ignore-recovery-zero.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let opened_ts = DateTime::parse_from_rfc3339("2026-03-01T10:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let closed_ts = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        store.insert_shadow_closed_trade_exact_with_context(
            "sig-recovery-zero",
            "wallet",
            "token",
            10.0,
            None,
            0.10,
            0.0,
            -0.10,
            SHADOW_CLOSE_CONTEXT_RECOVERY_TERMINAL_ZERO_PRICE,
            opened_ts,
            closed_ts,
        )?;

        let (all_trades, all_pnl) =
            store.shadow_realized_pnl_lamports_since(opened_ts - Duration::minutes(1))?;
        assert_eq!(all_trades, 1);
        assert_eq!(all_pnl, SignedLamports::new(-100_000_000));

        let (risk_trades, risk_pnl) =
            store.shadow_risk_realized_pnl_lamports_since(opened_ts - Duration::minutes(1))?;
        assert_eq!(risk_trades, 0);
        assert_eq!(risk_pnl, SignedLamports::ZERO);

        assert_eq!(
            store.shadow_rug_loss_count_since(opened_ts - Duration::minutes(1), -0.70)?,
            0
        );
        let (recent_rug_count, total_count, rug_rate) =
            store.shadow_rug_loss_rate_recent(opened_ts - Duration::minutes(1), 10, -0.70)?;
        assert_eq!(recent_rug_count, 0);
        assert_eq!(total_count, 0);
        assert_eq!(rug_rate, 0.0);
        Ok(())
    }
