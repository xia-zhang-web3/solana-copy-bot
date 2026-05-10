    #[test]
    fn startup_wal_checkpoint_is_deferred_with_explicit_outcome() -> Result<()> {
        let (store, db_path) = make_test_store("startup-wal-checkpoint-deferred")?;
        let events = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let reporter_events = events.clone();
        let reporter: StartupStepProgressReporter = std::sync::Arc::new(move |event| {
            reporter_events
                .lock()
                .expect("startup reporter mutex poisoned")
                .push(event);
        });

        let outcome = perform_startup_wal_checkpoint(&reporter);
        assert_eq!(outcome, StartupWalCheckpointOutcome::Deferred);
        store
            .record_heartbeat("copybot-app", "startup-deferred-checkpoint")
            .context("store should remain writable after deferred checkpoint")?;

        let recorded = events.lock().expect("startup reporter mutex poisoned");
        assert!(
            recorded.iter().any(|event| {
                event.stage == "startup_sqlite_wal_checkpoint"
                    && event.outcome == StartupStepOutcome::Started
            }),
            "deferred checkpoint must emit a started outcome"
        );
        assert!(
            recorded.iter().any(|event| {
                event.stage == "startup_sqlite_wal_checkpoint"
                    && event.outcome == StartupStepOutcome::Skipped
                    && event.detail.as_deref() == Some(STARTUP_WAL_CHECKPOINT_DEFER_REASON)
            }),
            "deferred checkpoint must emit an explicit skipped outcome with reason"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn startup_sqlite_heartbeat_old_like_implicit_wal_autocheckpoint_surfaces_timeout_if_large_wal_stalls_stage1(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("startup-heartbeat-autocheckpoint-timeout")?;
        seed_startup_heartbeat_wal_backlog(&store, 8_192)?;

        let events = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let reporter_events = events.clone();
        let reporter: StartupStepProgressReporter = std::sync::Arc::new(move |event| {
            reporter_events
                .lock()
                .expect("startup reporter mutex poisoned")
                .push(event);
        });

        let conn = open_slow_startup_heartbeat_test_connection(&db_path, 1)?;
        let result =
            run_startup_heartbeat_insert_step(conn, Some(&reporter), StdDuration::from_millis(40));
        std::thread::sleep(StdDuration::from_millis(150));

        let recorded = events.lock().expect("startup reporter mutex poisoned");
        match result {
            Ok(()) => {
                let verify = Connection::open(&db_path)
                    .with_context(|| format!("failed opening verify db {}", db_path.display()))?;
                let heartbeat_rows: i64 = verify.query_row(
                    "SELECT COUNT(*) FROM system_heartbeat WHERE component = ?1 AND status = ?2",
                    params!["copybot-app", "startup"],
                    |row| row.get(0),
                )?;
                assert!(
                    heartbeat_rows >= 1,
                    "fast environments may complete the old-like heartbeat, but it must still write durably"
                );
            }
            Err(error) => {
                assert!(
                    error
                        .downcast_ref::<copybot_storage_core::StartupStepTimeout>()
                        .is_some(),
                    "unexpected error: {error:#}"
                );
                assert!(
                    recorded.iter().any(|event| {
                        event.stage == "test_startup_sqlite_heartbeat"
                            && event.outcome == StartupStepOutcome::Waiting
                    }),
                    "the reduced startup repro must emit waiting progress before timing out"
                );
                assert!(
                    recorded.iter().any(|event| {
                        event.stage == "test_startup_sqlite_heartbeat"
                            && event.outcome == StartupStepOutcome::TimedOut
                    }),
                    "the reduced startup repro must emit an explicit timed_out outcome"
                );
            }
        }

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn startup_sqlite_heartbeat_deferred_implicit_wal_autocheckpoint_completes_on_same_large_wal_stage1(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("startup-heartbeat-autocheckpoint-deferred")?;
        seed_startup_heartbeat_wal_backlog(&store, 8_192)?;

        let conn = open_slow_startup_heartbeat_test_connection(&db_path, 1)?;
        conn.pragma_update(None, "wal_autocheckpoint", 0_i64)
            .context(
            "failed to defer implicit wal_autocheckpoint on the startup heartbeat test connection",
        )?;
        run_startup_heartbeat_insert_step(conn, None, StdDuration::from_millis(250))
            .context("deferred implicit wal_autocheckpoint should let the same startup heartbeat write complete under the same timeout budget")?;

        let verify = Connection::open(&db_path)
            .with_context(|| format!("failed opening verify db {}", db_path.display()))?;
        let heartbeat_rows: i64 = verify.query_row(
            "SELECT COUNT(*) FROM system_heartbeat WHERE component = ?1 AND status = ?2",
            params!["copybot-app", "startup"],
            |row| row.get(0),
        )?;
        assert!(
            heartbeat_rows >= 1,
            "the deferred startup heartbeat path must still durably record the startup heartbeat instead of suppressing it"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn startup_sqlite_wal_autocheckpoint_defer_and_restore_preserve_store_writability_stage1(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("startup-heartbeat-autocheckpoint-helper")?;
        let original_pages = store.wal_autocheckpoint_pages()?;
        let (store, restore_pages) = defer_implicit_startup_sqlite_wal_autocheckpoint(store)?;
        assert_eq!(
            store.wal_autocheckpoint_pages()?,
            0,
            "startup helper must explicitly defer implicit wal checkpoint work before startup-critical sqlite writes"
        );
        let store = restore_implicit_startup_sqlite_wal_autocheckpoint(store, restore_pages)?;
        assert_eq!(
            store.wal_autocheckpoint_pages()?,
            original_pages,
            "startup helper must restore the prior implicit wal checkpoint contract before the runtime loop begins"
        );
        store
            .record_heartbeat("copybot-app", "startup-helper-restored")
            .context("store should remain writable after defer/restore")?;

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn runtime_sqlite_write_error_requires_restart_on_fatal_io() {
        let error = anyhow!(
            "failed to commit heartbeat transaction: disk I/O error: Error code 4874: I/O error within the xShmMap method"
        );
        assert!(runtime_sqlite_write_error_requires_restart(&error));
    }

    #[test]
    fn runtime_sqlite_write_error_does_not_require_restart_on_busy_lock() {
        let error = anyhow!("database is locked");
        assert!(!runtime_sqlite_write_error_requires_restart(&error));
    }

    #[test]
    fn history_retention_error_requires_restart_on_fatal_io() {
        let error = anyhow!(
            "failed deleting retained risk events: disk I/O error: Error code 4874: I/O error within the xShmMap method"
        );
        assert!(history_retention_error_requires_restart(&error));
    }

    #[test]
    fn history_retention_error_does_not_require_restart_on_busy_lock() {
        let error = anyhow!("database is busy");
        assert!(!history_retention_error_requires_restart(&error));
    }

    #[test]
    fn observed_swap_retention_error_requires_restart_on_fatal_io() {
        let error = anyhow!(
            "failed to delete observed swap retention slice: disk I/O error: Error code 4874: I/O error within the xShmMap method"
        );
        assert!(observed_swap_retention_error_requires_restart(&error));
    }

    #[test]
    fn observed_swap_retention_error_does_not_require_restart_on_busy_lock() {
        let error = anyhow!("database is locked");
        assert!(!observed_swap_retention_error_requires_restart(&error));
    }

    #[test]
    fn sqlite_maintenance_block_reason_blocks_during_startup_grace() {
        let now = StdInstant::now();
        let reason = sqlite_maintenance_block_reason(
            SqliteMaintenanceTask::ObservedSwapRetention,
            now,
            now,
            &maintenance_test_writer_snapshot(),
            SqliteContentionSnapshot::default(),
            SqliteContentionSnapshot::default(),
            None,
        )
        .expect("startup grace should block sqlite maintenance");
        assert!(reason.contains("startup_grace_remaining_ms="));
    }

    #[test]
    fn sqlite_maintenance_block_reason_blocks_on_writer_pending_requests() {
        let mut writer_snapshot = maintenance_test_writer_snapshot();
        writer_snapshot.pending_requests = 3;
        let reason = sqlite_maintenance_block_reason(
            SqliteMaintenanceTask::ObservedSwapRetention,
            StdInstant::now()
                - OBSERVED_SWAP_RETENTION_STARTUP_GRACE_INTERVAL
                - std::time::Duration::from_secs(1),
            StdInstant::now(),
            &writer_snapshot,
            SqliteContentionSnapshot::default(),
            SqliteContentionSnapshot::default(),
            None,
        )
        .expect("writer backlog should block sqlite maintenance");
        assert_eq!(reason, "writer_pending_requests=3");
    }

    #[test]
    fn sqlite_maintenance_block_reason_blocks_on_recent_raw_journal_backlog() {
        let mut writer_snapshot = maintenance_test_writer_snapshot();
        writer_snapshot.journal_queue_depth_batches = 2;
        let reason = sqlite_maintenance_block_reason(
            SqliteMaintenanceTask::ObservedSwapRetention,
            StdInstant::now()
                - OBSERVED_SWAP_RETENTION_STARTUP_GRACE_INTERVAL
                - std::time::Duration::from_secs(1),
            StdInstant::now(),
            &writer_snapshot,
            SqliteContentionSnapshot::default(),
            SqliteContentionSnapshot::default(),
            None,
        )
        .expect("recent raw journal backlog should block sqlite maintenance");
        assert_eq!(reason, "journal_queue_depth_batches=2");
    }

    #[test]
    fn sqlite_maintenance_block_reason_blocks_on_recent_raw_journal_overflow_backlog() {
        let mut writer_snapshot = maintenance_test_writer_snapshot();
        writer_snapshot.journal_overflow_depth_batches = 3;
        let reason = sqlite_maintenance_block_reason(
            SqliteMaintenanceTask::ObservedSwapRetention,
            StdInstant::now()
                - OBSERVED_SWAP_RETENTION_STARTUP_GRACE_INTERVAL
                - std::time::Duration::from_secs(1),
            StdInstant::now(),
            &writer_snapshot,
            SqliteContentionSnapshot::default(),
            SqliteContentionSnapshot::default(),
            None,
        )
        .expect("recent raw journal overflow backlog should block sqlite maintenance");
        assert_eq!(reason, "journal_overflow_depth_batches=3");
    }

    #[test]
    fn sqlite_maintenance_block_reason_blocks_on_sqlite_contention_delta() {
        let reason = sqlite_maintenance_block_reason(
            SqliteMaintenanceTask::ObservedSwapRetention,
            StdInstant::now()
                - OBSERVED_SWAP_RETENTION_STARTUP_GRACE_INTERVAL
                - std::time::Duration::from_secs(1),
            StdInstant::now(),
            &maintenance_test_writer_snapshot(),
            SqliteContentionSnapshot {
                write_retry_total: 10,
                busy_error_total: 20,
            },
            SqliteContentionSnapshot {
                write_retry_total: 12,
                busy_error_total: 21,
            },
            None,
        )
        .expect("sqlite contention delta should block sqlite maintenance");
        assert_eq!(
            reason,
            "sqlite_contention_delta write_retry_delta=2 busy_error_delta=1"
        );
    }

    #[test]
    fn sqlite_maintenance_block_reason_blocks_on_yellowstone_queue_pressure() {
        let reason = sqlite_maintenance_block_reason(
            SqliteMaintenanceTask::ObservedSwapRetention,
            StdInstant::now()
                - OBSERVED_SWAP_RETENTION_STARTUP_GRACE_INTERVAL
                - std::time::Duration::from_secs(1),
            StdInstant::now(),
            &maintenance_test_writer_snapshot(),
            SqliteContentionSnapshot::default(),
            SqliteContentionSnapshot::default(),
            Some(maintenance_test_ingestion_snapshot(
                SQLITE_MAINTENANCE_MAX_YELLOWSTONE_OUTPUT_QUEUE_FILL_RATIO,
            )),
        )
        .expect("yellowstone queue pressure should block sqlite maintenance");
        assert!(reason.contains("yellowstone_output_queue_fill_ratio="));
        assert!(reason.contains("depth=25"));
        assert!(reason.contains("capacity=100"));
    }

    #[test]
    fn sqlite_maintenance_block_reason_allows_healthy_runtime() {
        let reason = sqlite_maintenance_block_reason(
            SqliteMaintenanceTask::ObservedSwapRetention,
            StdInstant::now()
                - OBSERVED_SWAP_RETENTION_STARTUP_GRACE_INTERVAL
                - std::time::Duration::from_secs(1),
            StdInstant::now(),
            &maintenance_test_writer_snapshot(),
            SqliteContentionSnapshot::default(),
            SqliteContentionSnapshot::default(),
            Some(maintenance_test_ingestion_snapshot(0.10)),
        );
        assert!(reason.is_none());
    }

    #[test]
    fn sqlite_maintenance_block_reason_key_dedupes_dynamic_startup_grace_reason() {
        let key_a = sqlite_maintenance_block_reason_key("startup_grace_remaining_ms=1799000");
        let key_b = sqlite_maintenance_block_reason_key("startup_grace_remaining_ms=1200000");
        assert_eq!(key_a, "startup_grace_remaining_ms");
        assert_eq!(key_a, key_b);
    }

    #[test]
    fn sqlite_maintenance_block_reason_key_dedupes_dynamic_yellowstone_pressure_reason() {
        let key_a = sqlite_maintenance_block_reason_key(
            "yellowstone_output_queue_fill_ratio=0.2500 depth=25 capacity=100 oldest_age_ms=500",
        );
        let key_b = sqlite_maintenance_block_reason_key(
            "yellowstone_output_queue_fill_ratio=0.7400 depth=74 capacity=100 oldest_age_ms=2500",
        );
        assert_eq!(key_a, "yellowstone_output_queue_fill_ratio");
        assert_eq!(key_a, key_b);
    }

    #[test]
    fn stale_lot_cleanup_error_requires_restart_on_fatal_io() {
        let error = anyhow!(
            "failed stale close for wallet=wallet-a token=token-a lot_id=1: disk I/O error: Error code 4874: I/O error within the xShmMap method"
        );
        assert!(stale_lot_cleanup_error_requires_restart(&error));
    }

    #[test]
    fn stale_lot_cleanup_error_does_not_require_restart_on_busy_lock() {
        let error = anyhow!("database is busy");
        assert!(!stale_lot_cleanup_error_requires_restart(&error));
    }

    #[test]
    fn alert_delivery_error_requires_restart_on_fatal_io() {
        let error = anyhow!(
            "failed to advance alert delivery cursor: disk I/O error: Error code 4874: I/O error within the xShmMap method"
        );
        assert!(alert_delivery_error_requires_restart(&error));
    }

    #[test]
    fn alert_delivery_error_does_not_require_restart_on_webhook_failure() {
        let error = anyhow!("alert webhook request failed");
        assert!(!alert_delivery_error_requires_restart(&error));
    }

    #[test]
    fn discovery_task_error_requires_restart_on_fatal_io() {
        let error = anyhow!(
            "failed persisting discovery runtime cursor with fatal sqlite I/O: disk I/O error: Error code 4874: I/O error within the xShmMap method"
        );
        assert!(discovery_task_error_requires_restart(&error));
    }

    #[test]
    fn discovery_task_error_does_not_require_restart_on_busy_lock() {
        let error = anyhow!("failed persisting discovery runtime cursor: database is locked");
        assert!(!discovery_task_error_requires_restart(&error));
    }

    #[test]
    fn risk_event_write_error_requires_restart_on_fatal_io() {
        let error = anyhow!(
            "failed to insert risk event: disk I/O error: Error code 4874: I/O error within the xShmMap method"
        );
        assert!(risk_event_write_error_requires_restart(&error));
    }

    #[test]
    fn risk_event_write_error_does_not_require_restart_on_busy_lock() {
        let error = anyhow!("database is locked");
        assert!(!risk_event_write_error_requires_restart(&error));
    }

    #[test]
    fn shadow_risk_pause_restore_error_requires_restart_on_fatal_io() {
        let error = anyhow!(
            "failed to list pause events: disk I/O error: Error code 4874: I/O error within the xShmMap method"
        );
        assert!(shadow_risk_pause_restore_error_requires_restart(&error));
    }

    #[test]
    fn shadow_risk_pause_restore_error_does_not_require_restart_on_busy_lock() {
        let error = anyhow!("database is locked");
        assert!(!shadow_risk_pause_restore_error_requires_restart(&error));
    }

    #[test]
    fn shadow_risk_background_refresh_error_requires_restart_on_fatal_io() {
        let error = anyhow!(
            "failed refreshing shadow risk state: disk I/O error: Error code 4874: I/O error within the xShmMap method"
        );
        assert!(shadow_risk_background_refresh_error_requires_restart(
            &error
        ));
    }

    #[test]
    fn shadow_risk_background_refresh_error_does_not_require_restart_on_busy_lock() {
        let error = anyhow!("database is locked");
        assert!(!shadow_risk_background_refresh_error_requires_restart(
            &error
        ));
    }

    #[test]
    fn shadow_open_lot_refresh_error_requires_restart_on_fatal_io() {
        let error = anyhow!(
            "failed listing shadow open pairs: disk I/O error: Error code 4874: I/O error within the xShmMap method"
        );
        assert!(shadow_open_lot_refresh_error_requires_restart(&error));
    }

    #[test]
    fn shadow_open_lot_refresh_error_does_not_require_restart_on_busy_lock() {
        let error = anyhow!("database is busy");
        assert!(!shadow_open_lot_refresh_error_requires_restart(&error));
    }

    #[test]
    fn shadow_snapshot_error_requires_restart_on_fatal_io() {
        let error = anyhow!(
            "failed building shadow snapshot: disk I/O error: Error code 4874: I/O error within the xShmMap method"
        );
        assert!(shadow_snapshot_error_requires_restart(&error));
    }

    #[test]
    fn shadow_snapshot_error_does_not_require_restart_on_busy_lock() {
        let error = anyhow!("database is locked");
        assert!(!shadow_snapshot_error_requires_restart(&error));
    }

    #[test]
    fn shadow_risk_state_event_error_requires_abort_on_xshmmap_io_failure() {
        let error = anyhow!(
            "failed to insert risk event: disk I/O error: Error code 4874: I/O error within the xShmMap method"
        );
        assert!(shadow_risk_state_event_error_requires_abort(&error));
    }

    #[test]
    fn shadow_risk_state_event_error_does_not_require_abort_on_busy_lock() {
        let error = anyhow!("database is locked");
        assert!(!shadow_risk_state_event_error_requires_abort(&error));
    }

    #[test]
    fn transient_observed_swap_write_errors_do_not_require_restart() {
        let error = anyhow!("database is locked");
        assert!(!observed_swap_writer_error_requires_restart(&error));
    }
