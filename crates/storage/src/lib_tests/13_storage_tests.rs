use super::*;

#[test]
fn sqlite_startup_bootstrap_defers_optional_sol_leg_index_migration() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("sqlite-startup-defers-sol-leg-index.db");
    let migration_dir = temp.path().join("startup-deferred-migrations");
    copy_migrations_through(&migration_dir, "0038_alert_delivery_cursor.sql")?;
    fs::write(
        migration_dir.join("0039_observed_swaps_sol_leg_ts_index.sql"),
        "SELECT definitely_missing_function();\n",
    )
    .context("failed writing fake deferred migration")?;

    let policy = SqliteStartupPolicy {
        open_step: StartupStepRuntimePolicy::new(
            StdDuration::from_millis(10),
            Some(StdDuration::from_secs(1)),
        ),
        pragma_step: StartupStepRuntimePolicy::new(
            StdDuration::from_millis(10),
            Some(StdDuration::from_secs(1)),
        ),
        large_wal_checkpoint_step: StartupStepRuntimePolicy::new(
            StdDuration::from_millis(10),
            Some(StdDuration::from_secs(1)),
        ),
        schema_bootstrap_step: StartupStepRuntimePolicy::new(
            StdDuration::from_millis(10),
            Some(StdDuration::from_secs(1)),
        ),
        migrations_scan_step: StartupStepRuntimePolicy::new(
            StdDuration::from_millis(10),
            Some(StdDuration::from_secs(1)),
        ),
        migrations_apply_step: StartupStepRuntimePolicy::new(
            StdDuration::from_millis(10),
            Some(StdDuration::from_secs(1)),
        ),
        large_wal_checkpoint_threshold_bytes: SQLITE_STARTUP_LARGE_WAL_CHECKPOINT_THRESHOLD_BYTES,
    };

    let bootstrap = SqliteStore::open_and_migrate_for_startup(
        Path::new(&db_path),
        &migration_dir,
        &policy,
        None,
    )?;
    assert!(
        bootstrap
            .deferred_migrations
            .contains(&"0039_observed_swaps_sol_leg_ts_index.sql".to_string()),
        "startup bootstrap must explicitly defer the heavy sol-leg partial index migration"
    );

    let applied: Option<String> = bootstrap
        .store
        .conn
        .query_row(
            "SELECT version
                 FROM schema_migrations
                 WHERE version = '0039_observed_swaps_sol_leg_ts_index.sql'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    assert!(
        applied.is_none(),
        "startup bootstrap must not apply the deferred sol-leg index migration"
    );
    Ok(())
}

#[test]
fn sqlite_startup_bootstrap_does_not_report_deferred_sol_leg_index_after_offline_apply(
) -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp
        .path()
        .join("sqlite-startup-no-false-deferred-sol-leg-index.db");
    let legacy_migrations = temp.path().join("legacy-migrations");
    copy_migrations_through(&legacy_migrations, "0038_alert_delivery_cursor.sql")?;

    let mut legacy_store = SqliteStore::open(Path::new(&db_path))?;
    legacy_store.run_migrations(&legacy_migrations)?;
    legacy_store.conn.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_observed_swaps_sol_leg_ts_slot_signature
                 ON observed_swaps(ts, slot, signature)
                 WHERE token_in = 'So11111111111111111111111111111111111111112'
                    OR token_out = 'So11111111111111111111111111111111111111112';",
    )?;
    drop(legacy_store);

    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let events = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let reporter_events = events.clone();
    let reporter: StartupStepProgressReporter = std::sync::Arc::new(move |event| {
        reporter_events
            .lock()
            .expect("startup reporter mutex poisoned")
            .push(event);
    });
    let policy = SqliteStartupPolicy {
        open_step: StartupStepRuntimePolicy::new(
            StdDuration::from_millis(10),
            Some(StdDuration::from_secs(1)),
        ),
        pragma_step: StartupStepRuntimePolicy::new(
            StdDuration::from_millis(10),
            Some(StdDuration::from_secs(1)),
        ),
        large_wal_checkpoint_step: StartupStepRuntimePolicy::new(
            StdDuration::from_millis(10),
            Some(StdDuration::from_secs(1)),
        ),
        schema_bootstrap_step: StartupStepRuntimePolicy::new(
            StdDuration::from_millis(10),
            Some(StdDuration::from_secs(1)),
        ),
        migrations_scan_step: StartupStepRuntimePolicy::new(
            StdDuration::from_millis(10),
            Some(StdDuration::from_secs(1)),
        ),
        migrations_apply_step: StartupStepRuntimePolicy::new(
            StdDuration::from_millis(10),
            Some(StdDuration::from_secs(1)),
        ),
        large_wal_checkpoint_threshold_bytes: SQLITE_STARTUP_LARGE_WAL_CHECKPOINT_THRESHOLD_BYTES,
    };

    let bootstrap = SqliteStore::open_and_migrate_for_startup(
        Path::new(&db_path),
        &migration_dir,
        &policy,
        Some(&reporter),
    )?;
    assert!(
        bootstrap.deferred_migrations.is_empty(),
        "startup bootstrap must not report 0039 as deferred once the index already exists offline"
    );
    let recorded = events.lock().expect("startup reporter mutex poisoned");
    assert!(
            recorded.iter().any(|event| {
                event.stage == "sqlite_migrations_deferred"
                    && event.outcome == StartupStepOutcome::Completed
                    && event.detail.as_deref() == Some("deferred_count=0")
            }),
            "startup bootstrap must report no deferred migrations after the offline index is already present"
        );

    let applied: Option<String> = bootstrap
        .store
        .conn
        .query_row(
            "SELECT version
                 FROM schema_migrations
                 WHERE version = '0039_observed_swaps_sol_leg_ts_index.sql'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    assert_eq!(
            applied.as_deref(),
            Some("0039_observed_swaps_sol_leg_ts_index.sql"),
            "startup bootstrap should quickly record 0039 once the offline-created index already exists"
        );
    Ok(())
}

#[test]
fn discovery_runtime_artifact_roundtrip_restores_consistent_snapshot() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let source_db_path = temp.path().join("runtime-artifact-source.db");
    let restored_db_path = temp.path().join("runtime-artifact-restored.db");
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

    let now = DateTime::parse_from_rfc3339("2026-03-23T12:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    let metrics_window_start = now - Duration::days(7);
    let mut source_store = SqliteStore::open(Path::new(&source_db_path))?;
    source_store.run_migrations(&migration_dir)?;
    for (idx, wallet_id) in ["wallet_roundtrip_a", "wallet_roundtrip_b"]
        .iter()
        .enumerate()
    {
        source_store.upsert_wallet(
            wallet_id,
            now - Duration::days(8),
            now - Duration::minutes(idx as i64),
            "candidate",
        )?;
        source_store.insert_wallet_metric(&WalletMetricRow {
            wallet_id: (*wallet_id).to_string(),
            window_start: metrics_window_start,
            pnl: 2.0 + idx as f64,
            win_rate: 0.8,
            trades: 6,
            closed_trades: 6,
            hold_median_seconds: 120,
            score: 1.0 - idx as f64 * 0.1,
            buy_total: 6,
            tradable_ratio: 1.0,
            rug_ratio: 0.0,
        })?;
    }
    let runtime_cursor = DiscoveryRuntimeCursor {
        ts_utc: now - Duration::minutes(1),
        slot: 4242,
        signature: "runtime-artifact-roundtrip-cursor".to_string(),
    };
    source_store.upsert_discovery_runtime_cursor(&runtime_cursor)?;
    source_store.set_discovery_publication_state_with_identity(
        &DiscoveryPublicationStateUpdate {
            runtime_mode: DiscoveryRuntimeMode::Healthy,
            reason: "runtime_artifact_roundtrip".to_string(),
            last_published_at: Some(now - Duration::minutes(10)),
            last_published_window_start: Some(metrics_window_start),
            published_scoring_source: Some("discovery_v2_operational_window".to_string()),
            published_wallet_ids: Some(vec![
                "wallet_roundtrip_a".to_string(),
                "wallet_roundtrip_b".to_string(),
            ]),
        },
        false,
        Some("test-policy-fingerprint"),
        Some(&runtime_cursor),
    )?;
    let export_gate = DiscoveryPublicationFreshnessGate {
        scoring_window_days: 7,
        metric_snapshot_interval_seconds: 1800,
        refresh_seconds: 600,
        expected_scoring_source: Some("discovery_v2_operational_window".to_string()),
        expected_policy_fingerprint: Some("test-policy-fingerprint".to_string()),
    };
    let artifact = source_store.export_discovery_runtime_artifact(now, export_gate)?;

    let mut restored_store = SqliteStore::open(Path::new(&restored_db_path))?;
    restored_store.run_migrations(&migration_dir)?;
    let error = restored_store
        .restore_discovery_runtime_artifact(&artifact, now, false)
        .expect_err("legacy storage restore lane must stay quarantined");
    assert!(error
        .to_string()
        .contains("legacy copybot-storage runtime artifact restore is quarantined"));
    Ok(())
}
