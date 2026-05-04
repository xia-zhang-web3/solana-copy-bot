    #[test]
    fn apply_history_retention_deletes_terminal_execution_history_child_first() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("execution-history-retention.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let stale_ts = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let fresh_ts = DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        for signal in [
            ("sig-old-confirmed", "execution_confirmed", stale_ts),
            ("sig-old-pending", "execution_submitted", stale_ts),
            (
                "sig-old-submit-recent-confirmed",
                "execution_confirmed",
                stale_ts,
            ),
            ("sig-fresh-confirmed", "execution_confirmed", fresh_ts),
        ] {
            store.conn.execute(
                "INSERT INTO copy_signals(signal_id, wallet_id, side, token, notional_sol, ts, status)
                 VALUES (?1, 'wallet-1', 'buy', 'token-1', 0.5, ?2, ?3)",
                params![signal.0, signal.2.to_rfc3339(), signal.1],
            )?;
        }

        for order in [
            (
                "ord-old-confirmed",
                "sig-old-confirmed",
                stale_ts,
                Some(stale_ts + Duration::minutes(1)),
                "execution_confirmed",
                "cli-old-confirmed",
            ),
            (
                "ord-old-pending",
                "sig-old-pending",
                stale_ts,
                None,
                "execution_submitted",
                "cli-old-pending",
            ),
            (
                "ord-old-submit-recent-confirmed",
                "sig-old-submit-recent-confirmed",
                stale_ts,
                Some(fresh_ts),
                "execution_confirmed",
                "cli-old-submit-recent-confirmed",
            ),
            (
                "ord-fresh-confirmed",
                "sig-fresh-confirmed",
                fresh_ts,
                Some(fresh_ts + Duration::minutes(1)),
                "execution_confirmed",
                "cli-fresh-confirmed",
            ),
        ] {
            store.conn.execute(
                "INSERT INTO orders(
                    order_id, signal_id, route, submit_ts, confirm_ts, status, err_code,
                    client_order_id, tx_signature, simulation_status, simulation_error, attempt
                 ) VALUES (?1, ?2, 'rpc', ?3, ?4, ?5, NULL, ?6, 'sig', NULL, NULL, 1)",
                params![
                    order.0,
                    order.1,
                    order.2.to_rfc3339(),
                    order.3.map(|ts| ts.to_rfc3339()),
                    order.4,
                    order.5,
                ],
            )?;
        }

        for fill in [
            ("ord-old-confirmed", "token-1", 10.0, 0.05, 0.001, 10.0),
            (
                "ord-old-submit-recent-confirmed",
                "token-1",
                10.5,
                0.05,
                0.001,
                10.0,
            ),
            ("ord-fresh-confirmed", "token-1", 11.0, 0.05, 0.001, 10.0),
        ] {
            store.conn.execute(
                "INSERT INTO fills(order_id, token, qty, avg_price, fee, slippage_bps)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![fill.0, fill.1, fill.2, fill.3, fill.4, fill.5],
            )?;
        }

        let summary = store.apply_history_retention(
            HistoryRetentionCutoffs {
                risk_events_before: fresh_ts - Duration::days(1),
                copy_signals_before: fresh_ts - Duration::days(1),
                orders_before: fresh_ts - Duration::days(1),
                shadow_closed_trades_before: fresh_ts - Duration::days(1),
            },
            false,
        )?;

        assert_eq!(summary.fills_deleted, 1);
        assert_eq!(summary.orders_deleted, 1);
        assert_eq!(summary.copy_signals_deleted, 1);
        assert_eq!(summary.execution_order_batches, 1);
        assert_eq!(summary.copy_signals_batches, 1);

        let remaining_orders: i64 =
            store
                .conn
                .query_row("SELECT COUNT(*) FROM orders", [], |row| row.get(0))?;
        let remaining_fills: i64 =
            store
                .conn
                .query_row("SELECT COUNT(*) FROM fills", [], |row| row.get(0))?;
        let remaining_signals: i64 =
            store
                .conn
                .query_row("SELECT COUNT(*) FROM copy_signals", [], |row| row.get(0))?;
        assert_eq!(remaining_orders, 3);
        assert_eq!(remaining_fills, 2);
        assert_eq!(remaining_signals, 3);

        let old_pending_status: String = store.conn.query_row(
            "SELECT status FROM orders WHERE order_id = 'ord-old-pending'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(old_pending_status, "execution_submitted");

        let recent_confirm_status: String = store.conn.query_row(
            "SELECT status FROM orders WHERE order_id = 'ord-old-submit-recent-confirmed'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(recent_confirm_status, "execution_confirmed");
        Ok(())
    }

    #[test]
    fn apply_history_retention_deletes_old_shadow_closed_trades() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("shadow-closed-trades-retention.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let stale_opened = DateTime::parse_from_rfc3339("2026-03-01T10:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let stale_closed = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let fresh_opened = DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let fresh_closed = DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        store.conn.execute(
            "INSERT INTO shadow_closed_trades(
                signal_id, wallet_id, token, qty, entry_cost_sol, exit_value_sol, pnl_sol, opened_ts, closed_ts
             ) VALUES ('sig-old', 'wallet-1', 'token-1', 10.0, 0.10, 0.12, 0.02, ?1, ?2)",
            params![stale_opened.to_rfc3339(), stale_closed.to_rfc3339()],
        )?;
        store.conn.execute(
            "INSERT INTO shadow_closed_trades(
                signal_id, wallet_id, token, qty, entry_cost_sol, exit_value_sol, pnl_sol, opened_ts, closed_ts
             ) VALUES ('sig-fresh', 'wallet-1', 'token-1', 10.0, 0.10, 0.12, 0.02, ?1, ?2)",
            params![fresh_opened.to_rfc3339(), fresh_closed.to_rfc3339()],
        )?;

        let summary = store.apply_history_retention(
            HistoryRetentionCutoffs {
                risk_events_before: fresh_closed - Duration::days(1),
                copy_signals_before: fresh_closed - Duration::days(1),
                orders_before: fresh_closed - Duration::days(1),
                shadow_closed_trades_before: fresh_closed - Duration::days(1),
            },
            false,
        )?;

        assert_eq!(summary.shadow_closed_trades_deleted, 1);
        assert_eq!(summary.shadow_closed_trades_batches, 1);
        let remaining: i64 =
            store
                .conn
                .query_row("SELECT COUNT(*) FROM shadow_closed_trades", [], |row| {
                    row.get(0)
                })?;
        assert_eq!(remaining, 1);
        Ok(())
    }

    #[test]
    fn apply_history_retention_batches_risk_events() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("risk-events-retention-batched.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let stale_ts = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let fresh_ts = DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        for idx in 0..(history_retention::HISTORY_RETENTION_RISK_EVENTS_BATCH_SIZE + 1) {
            store.conn.execute(
                "INSERT INTO risk_events(event_id, type, severity, ts, details_json)
                 VALUES (?1, 'risk_event', 'info', ?2, NULL)",
                params![format!("info-batched-{idx}"), stale_ts.to_rfc3339()],
            )?;
        }
        store.conn.execute(
            "INSERT INTO risk_events(event_id, type, severity, ts, details_json)
             VALUES ('info-fresh', 'risk_event', 'info', ?1, NULL)",
            params![fresh_ts.to_rfc3339()],
        )?;

        let summary = store.apply_history_retention(
            HistoryRetentionCutoffs {
                risk_events_before: fresh_ts - Duration::days(1),
                copy_signals_before: fresh_ts - Duration::days(1),
                orders_before: fresh_ts - Duration::days(1),
                shadow_closed_trades_before: fresh_ts - Duration::days(1),
            },
            false,
        )?;

        assert_eq!(
            summary.risk_events_deleted,
            (history_retention::HISTORY_RETENTION_RISK_EVENTS_BATCH_SIZE + 1) as u64
        );
        assert_eq!(summary.risk_events_batches, 2);
        let remaining: i64 =
            store
                .conn
                .query_row("SELECT COUNT(*) FROM risk_events", [], |row| row.get(0))?;
        assert_eq!(remaining, 1);
        Ok(())
    }

    #[test]
    fn apply_history_retention_batches_execution_history() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("execution-history-retention-batched.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let stale_ts = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let fresh_ts = DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        for idx in 0..(history_retention::HISTORY_RETENTION_EXECUTION_ORDER_BATCH_SIZE + 1) {
            let signal_id = format!("sig-old-batched-{idx}");
            let order_id = format!("ord-old-batched-{idx}");
            let client_order_id = format!("cli-old-batched-{idx}");
            store.conn.execute(
                "INSERT INTO copy_signals(signal_id, wallet_id, side, token, notional_sol, ts, status)
                 VALUES (?1, 'wallet-1', 'buy', 'token-1', 0.5, ?2, 'execution_confirmed')",
                params![signal_id, stale_ts.to_rfc3339()],
            )?;
            store.conn.execute(
                "INSERT INTO orders(
                    order_id, signal_id, route, submit_ts, confirm_ts, status, err_code,
                    client_order_id, tx_signature, simulation_status, simulation_error, attempt
                 ) VALUES (?1, ?2, 'rpc', ?3, ?4, 'execution_confirmed', NULL, ?5, 'sig', NULL, NULL, 1)",
                params![
                    order_id,
                    signal_id,
                    stale_ts.to_rfc3339(),
                    (stale_ts + Duration::minutes(1)).to_rfc3339(),
                    client_order_id,
                ],
            )?;
            store.conn.execute(
                "INSERT INTO fills(order_id, token, qty, avg_price, fee, slippage_bps)
                 VALUES (?1, 'token-1', 1.0, 0.05, 0.001, 10.0)",
                params![order_id],
            )?;
        }
        store.conn.execute(
            "INSERT INTO copy_signals(signal_id, wallet_id, side, token, notional_sol, ts, status)
             VALUES ('sig-fresh-batched', 'wallet-1', 'buy', 'token-1', 0.5, ?1, 'execution_confirmed')",
            params![fresh_ts.to_rfc3339()],
        )?;

        let summary = store.apply_history_retention(
            HistoryRetentionCutoffs {
                risk_events_before: fresh_ts - Duration::days(1),
                copy_signals_before: fresh_ts - Duration::days(1),
                orders_before: fresh_ts - Duration::days(1),
                shadow_closed_trades_before: fresh_ts - Duration::days(1),
            },
            false,
        )?;

        assert_eq!(
            summary.orders_deleted,
            (history_retention::HISTORY_RETENTION_EXECUTION_ORDER_BATCH_SIZE + 1) as u64
        );
        assert_eq!(summary.fills_deleted, summary.orders_deleted);
        assert_eq!(summary.copy_signals_deleted, summary.orders_deleted);
        assert_eq!(summary.execution_order_batches, 2);
        assert_eq!(summary.copy_signals_batches, 2);

        let remaining_orders: i64 =
            store
                .conn
                .query_row("SELECT COUNT(*) FROM orders", [], |row| row.get(0))?;
        let remaining_fills: i64 =
            store
                .conn
                .query_row("SELECT COUNT(*) FROM fills", [], |row| row.get(0))?;
        let remaining_signals: i64 =
            store
                .conn
                .query_row("SELECT COUNT(*) FROM copy_signals", [], |row| row.get(0))?;
        assert_eq!(remaining_orders, 0);
        assert_eq!(remaining_fills, 0);
        assert_eq!(remaining_signals, 1);
        Ok(())
    }

    #[test]
    fn apply_history_retention_bounded_stops_after_batch_budget() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("history-retention-bounded.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let stale_ts = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let fresh_ts = DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        for idx in 0..(history_retention::HISTORY_RETENTION_RISK_EVENTS_BATCH_SIZE + 1) {
            store.conn.execute(
                "INSERT INTO risk_events(event_id, type, severity, ts, details_json)
                 VALUES (?1, 'risk_event', 'info', ?2, NULL)",
                params![format!("bounded-info-{idx}"), stale_ts.to_rfc3339()],
            )?;
        }
        store.conn.execute(
            "INSERT INTO risk_events(event_id, type, severity, ts, details_json)
             VALUES ('bounded-info-fresh', 'risk_event', 'info', ?1, NULL)",
            params![fresh_ts.to_rfc3339()],
        )?;

        let summary = store.apply_history_retention_bounded(
            HistoryRetentionCutoffs {
                risk_events_before: fresh_ts - Duration::days(1),
                copy_signals_before: fresh_ts - Duration::days(1),
                orders_before: fresh_ts - Duration::days(1),
                shadow_closed_trades_before: fresh_ts - Duration::days(1),
            },
            false,
            1,
            1,
            1,
            1,
        )?;

        assert_eq!(
            summary.risk_events_deleted,
            history_retention::HISTORY_RETENTION_RISK_EVENTS_BATCH_SIZE as u64
        );
        assert_eq!(summary.risk_events_batches, 1);
        assert!(!summary.completed_full_sweep);
        let remaining: i64 =
            store
                .conn
                .query_row("SELECT COUNT(*) FROM risk_events", [], |row| row.get(0))?;
        assert_eq!(remaining, 2);
        Ok(())
    }

    #[test]
    fn apply_history_retention_batches_shadow_closed_trades() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("shadow-closed-trades-retention-batched.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let stale_opened = DateTime::parse_from_rfc3339("2026-03-01T10:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let stale_closed = DateTime::parse_from_rfc3339("2026-03-01T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let fresh_opened = DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let fresh_closed = DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        for idx in 0..(history_retention::HISTORY_RETENTION_SHADOW_CLOSED_TRADES_BATCH_SIZE + 1) {
            store.conn.execute(
                "INSERT INTO shadow_closed_trades(
                    signal_id, wallet_id, token, qty, entry_cost_sol, exit_value_sol, pnl_sol, opened_ts, closed_ts
                 ) VALUES (?1, 'wallet-1', 'token-1', 10.0, 0.10, 0.12, 0.02, ?2, ?3)",
                params![
                    format!("sig-old-batched-{idx}"),
                    stale_opened.to_rfc3339(),
                    stale_closed.to_rfc3339(),
                ],
            )?;
        }
        store.conn.execute(
            "INSERT INTO shadow_closed_trades(
                signal_id, wallet_id, token, qty, entry_cost_sol, exit_value_sol, pnl_sol, opened_ts, closed_ts
             ) VALUES ('sig-fresh-batched', 'wallet-1', 'token-1', 10.0, 0.10, 0.12, 0.02, ?1, ?2)",
            params![fresh_opened.to_rfc3339(), fresh_closed.to_rfc3339()],
        )?;

        let summary = store.apply_history_retention(
            HistoryRetentionCutoffs {
                risk_events_before: fresh_closed - Duration::days(1),
                copy_signals_before: fresh_closed - Duration::days(1),
                orders_before: fresh_closed - Duration::days(1),
                shadow_closed_trades_before: fresh_closed - Duration::days(1),
            },
            false,
        )?;

        assert_eq!(
            summary.shadow_closed_trades_deleted,
            (history_retention::HISTORY_RETENTION_SHADOW_CLOSED_TRADES_BATCH_SIZE + 1) as u64
        );
        assert_eq!(summary.shadow_closed_trades_batches, 2);
        let remaining: i64 =
            store
                .conn
                .query_row("SELECT COUNT(*) FROM shadow_closed_trades", [], |row| {
                    row.get(0)
                })?;
        assert_eq!(remaining, 1);
        Ok(())
    }

    #[test]
    fn record_heartbeat_retries_after_write_lock() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("heartbeat-retry.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

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
        let handle = std::thread::spawn(move || -> Result<()> {
            let worker_store = SqliteStore::open(Path::new(&worker_db_path))?;
            worker_store
                .conn
                .busy_timeout(StdDuration::from_millis(1))
                .context("failed to shorten worker busy timeout")?;
            worker_barrier.wait();
            worker_store.record_heartbeat("copybot-app", "alive")?;
            Ok(())
        });

        barrier.wait();
        std::thread::sleep(StdDuration::from_millis(250));
        blocker_store.conn.execute_batch("COMMIT")?;
        handle
            .join()
            .expect("worker thread panicked")
            .context("worker heartbeat should succeed after retry")?;

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        let count: i64 = verify_store.conn.query_row(
            "SELECT COUNT(*) FROM system_heartbeat WHERE component = ?1 AND status = ?2",
            params!["copybot-app", "alive"],
            |row| row.get(0),
        )?;
        assert_eq!(count, 1);
        Ok(())
    }

    #[test]
    fn observed_startup_step_reports_started_and_completed() -> Result<()> {
        let events = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let reporter_events = events.clone();
        let reporter: StartupStepProgressReporter = std::sync::Arc::new(move |event| {
            reporter_events
                .lock()
                .expect("startup reporter mutex poisoned")
                .push(event);
        });

        let value = run_observed_startup_step(
            "test_startup_step_complete",
            StartupStepRuntimePolicy::new(
                StdDuration::from_millis(10),
                Some(StdDuration::from_millis(250)),
            ),
            Some(&reporter),
            || Ok::<usize, anyhow::Error>(7),
        )?;
        assert_eq!(value, 7);

        let recorded = events.lock().expect("startup reporter mutex poisoned");
        assert!(
            recorded.iter().any(|event| {
                event.stage == "test_startup_step_complete"
                    && event.outcome == StartupStepOutcome::Started
            }),
            "startup step must emit a started event"
        );
        assert!(
            recorded.iter().any(|event| {
                event.stage == "test_startup_step_complete"
                    && event.outcome == StartupStepOutcome::Completed
            }),
            "startup step must emit a completed event"
        );
        Ok(())
    }
