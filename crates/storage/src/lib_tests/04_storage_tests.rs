    #[test]
    fn startup_trusted_selection_gate_status_degrades_stale_trusted_current_snapshot_from_metadata(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("startup-trusted-selection-gate-status-stale-current-metadata.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let effective_window_start = DateTime::parse_from_rfc3339("2026-03-10T21:00:00+00:00")
            .expect("timestamp")
            .with_timezone(&Utc);
        let now = DateTime::parse_from_rfc3339("2026-03-15T22:05:00+00:00")
            .expect("timestamp")
            .with_timezone(&Utc);
        let snapshot_write = TrustedWalletMetricsSnapshotWrite {
            snapshot_id: "wallet_metrics:discovery_refresh:2026-03-10T21:00:00+00:00".to_string(),
            source_snapshot_id: None,
            source_window_start: Some(effective_window_start),
            effective_window_start,
            created_at: effective_window_start + Duration::minutes(1),
            source_kind: TrustedSnapshotSourceKind::DiscoveryRefresh,
            row_count: 1,
            trust_state: TrustedSelectionState::TrustedCurrent,
        };
        store.persist_discovery_cycle_with_snapshot_metadata(
            &[WalletUpsertRow {
                wallet_id: "wallet-stale-current".to_string(),
                first_seen: effective_window_start - Duration::days(1),
                last_seen: effective_window_start,
                status: "candidate".to_string(),
            }],
            &[WalletMetricRow {
                wallet_id: "wallet-stale-current".to_string(),
                window_start: effective_window_start,
                pnl: 1.0,
                win_rate: 0.8,
                trades: 4,
                closed_trades: 4,
                hold_median_seconds: 90,
                score: 0.8,
                buy_total: 4,
                tradable_ratio: 1.0,
                rug_ratio: 0.0,
            }],
            &[],
            false,
            false,
            effective_window_start + Duration::minutes(1),
            "stale-current-metadata-test",
            Some(&snapshot_write),
        )?;

        let status = store.startup_trusted_selection_gate_status()?;
        assert_eq!(
            status.selection_state,
            Some(TrustedSelectionState::TrustedCurrent)
        );
        assert!(!status.legacy_bool_fallback_used);
        assert_eq!(
            status.effective_selection_state(now, 5, 30 * 60, 60 * 60),
            Some(TrustedSelectionState::Invalid)
        );
        assert!(status.effective_startup_fail_closed(now, 5, 30 * 60, 60 * 60));
        Ok(())
    }

    #[test]
    fn persist_discovery_cycle_retention_keeps_latest_logical_windows_with_legacy_z_rows(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("discovery-wallet-metrics-retention-legacy-z.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let base = DateTime::parse_from_rfc3339("2026-02-20T00:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let window0 = base;
        let window1 = base + Duration::minutes(1);
        let window2 = base + Duration::minutes(2);
        let window3 = base + Duration::minutes(3);

        for (wallet_id, last_seen) in [
            ("wallet-old-z", window0),
            ("wallet-mid-plus", window1),
            ("wallet-dup-z", window2),
            ("wallet-dup-plus", window2),
            ("wallet-new", window3),
        ] {
            store.upsert_wallet(wallet_id, base, last_seen, "candidate")?;
        }

        for (wallet_id, raw_window_start) in [
            ("wallet-old-z", "2026-02-20T00:00:00Z"),
            ("wallet-mid-plus", "2026-02-20T00:01:00+00:00"),
            ("wallet-dup-z", "2026-02-20T00:02:00Z"),
            ("wallet-dup-plus", "2026-02-20T00:02:00+00:00"),
        ] {
            store.conn.execute(
                "INSERT INTO wallet_metrics(
                    wallet_id,
                    window_start,
                    pnl,
                    win_rate,
                    trades,
                    closed_trades,
                    hold_median_seconds,
                    score,
                    buy_total,
                    tradable_ratio,
                    rug_ratio
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                params![
                    wallet_id,
                    raw_window_start,
                    1.0_f64,
                    1.0_f64,
                    1_i64,
                    1_i64,
                    60_i64,
                    1.0_f64,
                    1_i64,
                    1.0_f64,
                    0.0_f64,
                ],
            )?;
        }

        store.persist_discovery_cycle(
            &[WalletUpsertRow {
                wallet_id: "wallet-new".to_string(),
                first_seen: base,
                last_seen: window3,
                status: "candidate".to_string(),
            }],
            &[WalletMetricRow {
                wallet_id: "wallet-new".to_string(),
                window_start: window3,
                pnl: 1.0,
                win_rate: 1.0,
                trades: 1,
                closed_trades: 1,
                hold_median_seconds: 60,
                score: 1.0,
                buy_total: 1,
                tradable_ratio: 1.0,
                rug_ratio: 0.0,
            }],
            &["wallet-new".to_string()],
            true,
            true,
            window3,
            "legacy-z-retention-test",
        )?;

        let mut raw_stmt = store.conn.prepare(
            "SELECT DISTINCT window_start FROM wallet_metrics ORDER BY window_start ASC",
        )?;
        let raw_windows: Vec<String> = raw_stmt
            .query_map([], |row| row.get(0))?
            .collect::<rusqlite::Result<Vec<String>>>()?;
        assert!(
            !raw_windows.contains(&"2026-02-20T00:00:00Z".to_string()),
            "oldest logical window must be deleted even when newer windows have mixed Z/+00:00 encodings"
        );
        assert!(raw_windows.contains(&window1.to_rfc3339()));
        assert!(raw_windows.contains(&"2026-02-20T00:02:00Z".to_string()));
        assert!(raw_windows.contains(&window2.to_rfc3339()));
        assert!(raw_windows.contains(&window3.to_rfc3339()));

        let mut logical_stmt = store.conn.prepare(
            "SELECT DISTINCT unixepoch(window_start)
             FROM wallet_metrics
             ORDER BY 1 ASC",
        )?;
        let logical_windows: Vec<i64> = logical_stmt
            .query_map([], |row| row.get(0))?
            .collect::<rusqlite::Result<Vec<i64>>>()?;
        assert_eq!(
            logical_windows,
            vec![window1.timestamp(), window2.timestamp(), window3.timestamp()],
            "retention must keep the latest three logical wallet_metrics windows even with mixed UTC encodings"
        );
        Ok(())
    }

    #[test]
    fn persist_discovery_cycle_can_suppress_followlist_deactivations() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("discovery-followlist-deactivation-suppression.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let wallet_id = "wallet-keep-active".to_string();
        store.activate_follow_wallet(&wallet_id, now, "seed-follow")?;
        assert!(store.list_active_follow_wallets()?.contains(&wallet_id));

        let wallets = vec![WalletUpsertRow {
            wallet_id: wallet_id.clone(),
            first_seen: now,
            last_seen: now,
            status: "observed".to_string(),
        }];

        let suppressed = store.persist_discovery_cycle(
            &wallets,
            &[],
            &[],
            true,
            false,
            now + Duration::minutes(1),
            "suppressed-demotions",
        )?;
        assert_eq!(suppressed.activated, 0);
        assert_eq!(suppressed.deactivated, 0);
        assert!(
            store.list_active_follow_wallets()?.contains(&wallet_id),
            "active wallet must remain followed when deactivations are suppressed"
        );

        let unsuppressed = store.persist_discovery_cycle(
            &wallets,
            &[],
            &[],
            true,
            true,
            now + Duration::minutes(2),
            "allow-demotions",
        )?;
        assert_eq!(unsuppressed.activated, 0);
        assert_eq!(unsuppressed.deactivated, 1);
        assert!(
            !store.list_active_follow_wallets()?.contains(&wallet_id),
            "active wallet should deactivate again once suppression is lifted"
        );
        Ok(())
    }

    #[test]
    fn persist_discovery_cycle_can_suppress_followlist_activations() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp
            .path()
            .join("discovery-followlist-activation-suppression.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let now = DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let wallet_id = "wallet-dont-activate".to_string();
        let wallets = vec![WalletUpsertRow {
            wallet_id: wallet_id.clone(),
            first_seen: now,
            last_seen: now,
            status: "candidate".to_string(),
        }];

        let suppressed = store.persist_discovery_cycle(
            &wallets,
            &[],
            std::slice::from_ref(&wallet_id),
            false,
            true,
            now + Duration::minutes(1),
            "suppressed-promotions",
        )?;
        assert_eq!(suppressed.activated, 0);
        assert_eq!(suppressed.deactivated, 0);
        assert!(
            !store.list_active_follow_wallets()?.contains(&wallet_id),
            "candidate wallet must stay inactive when followlist activations are suppressed"
        );

        let unsuppressed = store.persist_discovery_cycle(
            &wallets,
            &[],
            std::slice::from_ref(&wallet_id),
            true,
            true,
            now + Duration::minutes(2),
            "allow-promotions",
        )?;
        assert_eq!(unsuppressed.activated, 1);
        assert_eq!(unsuppressed.deactivated, 0);
        assert!(
            store.list_active_follow_wallets()?.contains(&wallet_id),
            "candidate wallet should activate once suppression is lifted"
        );
        Ok(())
    }

    #[test]
    fn wallet_activity_day_counts_since_returns_day_level_counts() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("wallet-activity-days.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let rows = vec![
            WalletActivityDayRow {
                wallet_id: "wallet-a".to_string(),
                activity_day: NaiveDate::from_ymd_opt(2026, 3, 5).expect("date"),
                last_seen: DateTime::parse_from_rfc3339("2026-03-05T12:00:00Z")
                    .expect("ts")
                    .with_timezone(&Utc),
            },
            WalletActivityDayRow {
                wallet_id: "wallet-a".to_string(),
                activity_day: NaiveDate::from_ymd_opt(2026, 3, 6).expect("date"),
                last_seen: DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
                    .expect("ts")
                    .with_timezone(&Utc),
            },
            WalletActivityDayRow {
                wallet_id: "wallet-b".to_string(),
                activity_day: NaiveDate::from_ymd_opt(2026, 3, 6).expect("date"),
                last_seen: DateTime::parse_from_rfc3339("2026-03-06T08:00:00Z")
                    .expect("ts")
                    .with_timezone(&Utc),
            },
            WalletActivityDayRow {
                wallet_id: "wallet-a".to_string(),
                activity_day: NaiveDate::from_ymd_opt(2026, 3, 6).expect("date"),
                last_seen: DateTime::parse_from_rfc3339("2026-03-06T18:00:00Z")
                    .expect("ts")
                    .with_timezone(&Utc),
            },
        ];
        store.upsert_wallet_activity_days(&rows)?;

        let counts = store.wallet_active_day_counts_since(
            &["wallet-a".to_string(), "wallet-b".to_string()],
            DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        )?;
        assert_eq!(counts.get("wallet-a"), Some(&1));
        assert!(
            !counts.contains_key("wallet-b"),
            "same-day activity before exact window_start must not be counted"
        );
        Ok(())
    }

    #[test]
    fn backfill_wallet_activity_days_since_uses_existing_observed_swaps() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("wallet-activity-backfill.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let window_start = DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        store.insert_observed_swap(&SwapEvent {
            signature: "backfill-pre-window".to_string(),
            wallet: "wallet-a".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenBackfill111111111111111111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 100.0,
            exact_amounts: None,
            slot: 1,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T08:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        })?;
        store.insert_observed_swap(&SwapEvent {
            signature: "backfill-boundary-window".to_string(),
            wallet: "wallet-a".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenBackfill111111111111111111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 100.0,
            exact_amounts: None,
            slot: 2,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        })?;
        store.insert_observed_swap(&SwapEvent {
            signature: "backfill-later-day".to_string(),
            wallet: "wallet-a".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenBackfill111111111111111111111111111111".to_string(),
            amount_in: 1.0,
            amount_out: 100.0,
            exact_amounts: None,
            slot: 3,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-07T09:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
        })?;

        store.backfill_wallet_activity_days_since(window_start)?;

        let counts =
            store.wallet_active_day_counts_since(&["wallet-a".to_string()], window_start)?;
        assert_eq!(
            counts.get("wallet-a"),
            Some(&2),
            "backfill should use existing observed_swaps at or after the exact window_start"
        );
        Ok(())
    }

    #[test]
    fn discovery_scoring_coverage_marker_gates_window_readiness() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("discovery-scoring-coverage.db");
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        store.run_migrations(&migration_dir)?;

        let window_start = DateTime::parse_from_rfc3339("2026-03-01T00:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let now = DateTime::parse_from_rfc3339("2026-03-09T12:00:00Z")
            .expect("ts")
            .with_timezone(&Utc);
        let max_lag = Duration::minutes(10);
        assert!(!store.discovery_scoring_ready_for_window(window_start, now, max_lag)?);

        store.set_discovery_scoring_covered_since(window_start - Duration::hours(1))?;
        assert!(
            !store.discovery_scoring_ready_for_window(window_start, now, max_lag)?,
            "covered_since alone must not activate aggregate reads without a near-head watermark"
        );
        store.conn.execute(
            "INSERT INTO discovery_scoring_state(state_key, state_value, updated_at)
             VALUES ('covered_through_ts', ?1, ?2)
             ON CONFLICT(state_key) DO UPDATE SET
                state_value = excluded.state_value,
                updated_at = excluded.updated_at",
            params![
                (now - Duration::minutes(5)).to_rfc3339(),
                Utc::now().to_rfc3339()
            ],
        )?;
        assert!(
            !store.discovery_scoring_ready_for_window(window_start, now, max_lag)?,
            "timestamp-only covered_through state must not enable aggregate reads without the exact cursor"
        );

        store.set_discovery_scoring_covered_through_cursor(&DiscoveryRuntimeCursor {
            ts_utc: now - Duration::minutes(5),
            slot: 42,
            signature: "covered-through-ready".to_string(),
        })?;
        assert!(store.discovery_scoring_ready_for_window(window_start, now, max_lag)?);

        store.set_discovery_scoring_materialization_gap_cursor(&DiscoveryRuntimeCursor {
            ts_utc: now - Duration::minutes(15),
            slot: 100,
            signature: "gap-row".to_string(),
        })?;
        assert!(
            !store.discovery_scoring_ready_for_window(window_start, now, max_lag)?,
            "latched materialization gaps must block aggregate readiness even with near-head watermarks"
        );
        store.clear_discovery_scoring_materialization_gap_if_cursor_observed(
            &DiscoveryRuntimeCursor {
                ts_utc: now - Duration::minutes(30),
                slot: 0,
                signature: String::new(),
            },
        )?;
        assert!(
            !store.discovery_scoring_ready_for_window(window_start, now, max_lag)?,
            "observing an earlier cursor must not clear the exact continuity blocker"
        );
        store.clear_discovery_scoring_materialization_gap_if_cursor_observed(
            &DiscoveryRuntimeCursor {
                ts_utc: now - Duration::minutes(15),
                slot: 100,
                signature: "gap-row".to_string(),
            },
        )?;
        assert!(store.discovery_scoring_ready_for_window(window_start, now, max_lag)?);

        store.set_discovery_scoring_materialization_gap_cursor(&DiscoveryRuntimeCursor {
            ts_utc: now - Duration::minutes(15),
            slot: 100,
            signature: "gap-row-b".to_string(),
        })?;
        store.clear_discovery_scoring_materialization_gap_if_cursor_observed(
            &DiscoveryRuntimeCursor {
                ts_utc: now - Duration::minutes(15),
                slot: 100,
                signature: "zzz-after-gap".to_string(),
            },
        )?;
        assert!(
            !store.discovery_scoring_ready_for_window(window_start, now, max_lag)?,
            "observing a different row at the same timestamp must not clear the exact continuity blocker"
        );
        store.clear_discovery_scoring_materialization_gap_if_cursor_observed(
            &DiscoveryRuntimeCursor {
                ts_utc: now - Duration::minutes(15),
                slot: 100,
                signature: "gap-row-b".to_string(),
            },
        )?;
        assert!(store.discovery_scoring_ready_for_window(window_start, now, max_lag)?);

        store.set_discovery_scoring_covered_since(window_start + Duration::hours(1))?;
        assert!(
            !store.discovery_scoring_ready_for_window(window_start, now, max_lag)?,
            "coverage marker later than window_start must not enable aggregate reads yet"
        );

        store.set_discovery_scoring_covered_since(window_start - Duration::hours(1))?;
        let later_now = now + Duration::hours(3);
        assert!(
            !store.discovery_scoring_ready_for_window(window_start, later_now, max_lag)?,
            "stale covered_through watermark must keep aggregate reads disabled"
        );
        Ok(())
    }
