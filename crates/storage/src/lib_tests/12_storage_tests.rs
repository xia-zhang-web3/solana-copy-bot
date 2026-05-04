    #[test]
    fn observed_startup_step_times_out_with_explicit_progress() {
        let events = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let reporter_events = events.clone();
        let reporter: StartupStepProgressReporter = std::sync::Arc::new(move |event| {
            reporter_events
                .lock()
                .expect("startup reporter mutex poisoned")
                .push(event);
        });

        let error = run_observed_startup_step(
            "test_startup_step_timeout",
            StartupStepRuntimePolicy::new(
                StdDuration::from_millis(10),
                Some(StdDuration::from_millis(30)),
            ),
            Some(&reporter),
            || {
                std::thread::sleep(StdDuration::from_millis(80));
                Ok::<(), anyhow::Error>(())
            },
        )
        .expect_err("slow startup step must fail explicitly on timeout");
        assert!(
            error.downcast_ref::<StartupStepTimeout>().is_some(),
            "timeout must surface as StartupStepTimeout for explicit diagnosis"
        );

        let recorded = events.lock().expect("startup reporter mutex poisoned");
        assert!(
            recorded.iter().any(|event| {
                event.stage == "test_startup_step_timeout"
                    && event.outcome == StartupStepOutcome::Waiting
            }),
            "slow startup step must emit waiting progress before timeout"
        );
        assert!(
            recorded.iter().any(|event| {
                event.stage == "test_startup_step_timeout"
                    && event.outcome == StartupStepOutcome::TimedOut
            }),
            "slow startup step must emit an explicit timed_out outcome"
        );
    }

    #[test]
    fn observed_startup_step_fatal_timeout_panics_instead_of_returning() {
        let events = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let reporter_events = events.clone();
        let reporter: StartupStepProgressReporter = std::sync::Arc::new(move |event| {
            reporter_events
                .lock()
                .expect("startup reporter mutex poisoned")
                .push(event);
        });

        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = run_observed_startup_step(
                "test_startup_step_fatal_timeout",
                StartupStepRuntimePolicy::new(
                    StdDuration::from_millis(10),
                    Some(StdDuration::from_millis(30)),
                )
                .with_timeout_behavior(StartupStepTimeoutBehavior::Panic),
                Some(&reporter),
                || {
                    std::thread::sleep(StdDuration::from_millis(80));
                    Ok::<(), anyhow::Error>(())
                },
            );
        }));
        assert!(
            panic.is_err(),
            "fatal timeout policy must not return normally"
        );
        std::thread::sleep(StdDuration::from_millis(100));

        let recorded = events.lock().expect("startup reporter mutex poisoned");
        assert!(
            recorded.iter().any(|event| {
                event.stage == "test_startup_step_fatal_timeout"
                    && event.outcome == StartupStepOutcome::TimedOut
            }),
            "fatal timeout path must still emit a timed_out outcome"
        );
    }

    fn sqlite_startup_test_policy(threshold_bytes: u64) -> SqliteStartupPolicy {
        SqliteStartupPolicy {
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
            large_wal_checkpoint_threshold_bytes: threshold_bytes,
        }
    }

    fn collect_startup_events() -> (
        std::sync::Arc<std::sync::Mutex<Vec<StartupStepProgress>>>,
        StartupStepProgressReporter,
    ) {
        let events = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let reporter_events = events.clone();
        let reporter: StartupStepProgressReporter = std::sync::Arc::new(move |event| {
            reporter_events
                .lock()
                .expect("startup reporter mutex poisoned")
                .push(event);
        });
        (events, reporter)
    }

    fn seed_startup_wal_file(db_path: &Path, rows: usize) -> Result<Connection> {
        let conn = Connection::open(db_path)
            .with_context(|| format!("failed opening startup wal seed db {}", db_path.display()))?;
        conn.busy_timeout(StdDuration::from_millis(250))
            .context("failed setting startup wal seed busy timeout")?;
        conn.pragma_update(None, "journal_mode", "WAL")
            .context("failed enabling WAL for startup wal seed")?;
        conn.pragma_update(None, "wal_autocheckpoint", 0_i64)
            .context("failed disabling wal_autocheckpoint for startup wal seed")?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS startup_wal_seed(
                id INTEGER PRIMARY KEY,
                payload TEXT NOT NULL
            );",
        )?;
        let payload = "startup-large-wal-checkpoint-ballast".repeat(16);
        for idx in 0..rows.max(1) {
            conn.execute(
                "INSERT INTO startup_wal_seed(id, payload) VALUES (?1, ?2)",
                params![idx as i64, payload],
            )
            .with_context(|| format!("failed inserting startup wal seed row {idx}"))?;
        }
        Ok(conn)
    }

    fn startup_event_index(
        events: &[StartupStepProgress],
        stage: &'static str,
        outcome: StartupStepOutcome,
    ) -> Option<usize> {
        events
            .iter()
            .position(|event| event.stage == stage && event.outcome == outcome)
    }

    #[test]
    fn sqlite_startup_large_wal_checkpoint_skips_missing_wal_before_journal_mode_stage(
    ) -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("startup-large-wal-missing.db");
        let (events, reporter) = collect_startup_events();
        let policy = sqlite_startup_test_policy(1);

        let store = SqliteStore::open_for_startup(&db_path, &policy, Some(&reporter))?;
        drop(store);

        let recorded = events.lock().expect("startup reporter mutex poisoned");
        let skip_idx = startup_event_index(
            &recorded,
            SQLITE_STARTUP_LARGE_WAL_CHECKPOINT_TRUNCATE_STAGE,
            StartupStepOutcome::Skipped,
        )
        .expect("missing WAL must emit a skipped checkpoint event");
        let journal_idx = startup_event_index(
            &recorded,
            "sqlite_pragma_journal_mode_wal",
            StartupStepOutcome::Started,
        )
        .expect("startup must continue to journal_mode WAL stage after skipped checkpoint");
        assert!(
            skip_idx < journal_idx,
            "large-WAL guard must run before sqlite_pragma_journal_mode_wal"
        );
        assert!(
            recorded.iter().any(|event| {
                event.stage == SQLITE_STARTUP_LARGE_WAL_CHECKPOINT_TRUNCATE_STAGE
                    && event.outcome == StartupStepOutcome::Skipped
                    && event.detail.as_deref().is_some_and(|detail| {
                        detail.contains("reason=wal_missing")
                            && detail.contains("before_wal_bytes=0")
                    })
            }),
            "missing-WAL skip must report the inspected WAL size"
        );
        Ok(())
    }

    #[test]
    fn sqlite_startup_large_wal_checkpoint_skips_small_wal() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("startup-large-wal-small.db");
        let _seed_conn = seed_startup_wal_file(&db_path, 1)?;
        let before_wal_bytes = sqlite_wal_size_bytes(&db_path)?.unwrap_or(0);
        assert!(
            before_wal_bytes > 0,
            "test setup must leave a WAL file to prove the small-WAL skip path"
        );
        let (events, reporter) = collect_startup_events();
        let policy = sqlite_startup_test_policy(before_wal_bytes + 1);

        let store = SqliteStore::open_for_startup(&db_path, &policy, Some(&reporter))?;
        drop(store);

        let recorded = events.lock().expect("startup reporter mutex poisoned");
        assert!(
            recorded.iter().any(|event| {
                event.stage == SQLITE_STARTUP_LARGE_WAL_CHECKPOINT_TRUNCATE_STAGE
                    && event.outcome == StartupStepOutcome::Skipped
                    && event.detail.as_deref().is_some_and(|detail| {
                        detail.contains("reason=wal_below_threshold")
                            && detail.contains(&format!("before_wal_bytes={before_wal_bytes}"))
                    })
            }),
            "small-WAL skip must include the observed WAL size"
        );
        Ok(())
    }

    #[test]
    fn sqlite_startup_large_wal_checkpoint_truncates_before_journal_mode_stage() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("startup-large-wal-truncate.db");
        let _seed_conn = seed_startup_wal_file(&db_path, 16)?;
        let before_wal_bytes = sqlite_wal_size_bytes(&db_path)?.unwrap_or(0);
        assert!(before_wal_bytes > 1, "test setup must create a WAL file");
        let (events, reporter) = collect_startup_events();
        let policy = sqlite_startup_test_policy(1);

        let store = SqliteStore::open_for_startup(&db_path, &policy, Some(&reporter))?;
        let journal_mode: String = store
            .conn
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
        assert_eq!(journal_mode.to_ascii_lowercase(), "wal");
        drop(store);

        let after_wal_bytes = sqlite_wal_size_bytes(&db_path)?.unwrap_or(0);
        assert!(
            after_wal_bytes <= before_wal_bytes,
            "SQLite-managed checkpoint should not grow the WAL: before={before_wal_bytes} after={after_wal_bytes}"
        );

        let recorded = events.lock().expect("startup reporter mutex poisoned");
        let checkpoint_started_idx = startup_event_index(
            &recorded,
            SQLITE_STARTUP_LARGE_WAL_CHECKPOINT_TRUNCATE_STAGE,
            StartupStepOutcome::Started,
        )
        .expect("large WAL checkpoint must emit started progress");
        let journal_started_idx = startup_event_index(
            &recorded,
            "sqlite_pragma_journal_mode_wal",
            StartupStepOutcome::Started,
        )
        .expect("startup must continue to journal_mode WAL after successful checkpoint");
        assert!(
            checkpoint_started_idx < journal_started_idx,
            "large-WAL checkpoint must run before sqlite_pragma_journal_mode_wal"
        );
        assert!(
            recorded.iter().any(|event| {
                event.stage == SQLITE_STARTUP_LARGE_WAL_CHECKPOINT_TRUNCATE_STAGE
                    && event.outcome == StartupStepOutcome::Completed
                    && event.detail.as_deref().is_some_and(|detail| {
                        detail.contains(&format!("before_wal_bytes={before_wal_bytes}"))
                            && detail.contains("after_wal_bytes=")
                            && detail.contains("busy=0")
                    })
            }),
            "successful checkpoint must report before/after WAL size and checkpoint result"
        );
        let completed_count = recorded
            .iter()
            .filter(|event| {
                event.stage == SQLITE_STARTUP_LARGE_WAL_CHECKPOINT_TRUNCATE_STAGE
                    && event.outcome == StartupStepOutcome::Completed
            })
            .count();
        assert_eq!(
            completed_count, 1,
            "checkpoint stage must emit exactly one terminal Completed event"
        );
        Ok(())
    }

    #[test]
    fn sqlite_startup_large_wal_checkpoint_uses_dedicated_policy_not_pragma_step() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("startup-large-wal-dedicated-policy.db");
        let _seed_conn = seed_startup_wal_file(&db_path, 16)?;
        let (events, reporter) = collect_startup_events();
        let mut policy = sqlite_startup_test_policy(1);
        policy.pragma_step = StartupStepRuntimePolicy::new(
            StdDuration::from_millis(10),
            Some(StdDuration::from_millis(123)),
        );
        policy.large_wal_checkpoint_step = StartupStepRuntimePolicy::new(
            StdDuration::from_millis(10),
            Some(StdDuration::from_millis(4_567)),
        );

        let store = SqliteStore::open_for_startup(&db_path, &policy, Some(&reporter))?;
        drop(store);

        let recorded = events.lock().expect("startup reporter mutex poisoned");
        let checkpoint_started = recorded
            .iter()
            .find(|event| {
                event.stage == SQLITE_STARTUP_LARGE_WAL_CHECKPOINT_TRUNCATE_STAGE
                    && event.outcome == StartupStepOutcome::Started
            })
            .expect("large WAL checkpoint must emit started progress");
        assert_eq!(
            checkpoint_started.budget_ms,
            Some(4_567),
            "large WAL checkpoint must use its dedicated timeout budget"
        );
        let journal_started = recorded
            .iter()
            .find(|event| {
                event.stage == "sqlite_pragma_journal_mode_wal"
                    && event.outcome == StartupStepOutcome::Started
            })
            .expect("startup must continue to journal pragma after checkpoint");
        assert_eq!(
            journal_started.budget_ms,
            Some(123),
            "journal pragma must keep the shorter pragma budget"
        );
        Ok(())
    }

    #[test]
    fn sqlite_startup_large_wal_checkpoint_busy_fails_before_journal_mode_stage() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("startup-large-wal-busy.db");
        let seed_conn = seed_startup_wal_file(&db_path, 8)?;
        let reader = Connection::open(&db_path)
            .with_context(|| format!("failed opening busy reader {}", db_path.display()))?;
        reader.pragma_update(None, "journal_mode", "WAL")?;
        reader.execute_batch("BEGIN")?;
        let _: i64 = reader.query_row("SELECT COUNT(*) FROM startup_wal_seed", [], |row| {
            row.get(0)
        })?;
        seed_conn.execute(
            "INSERT INTO startup_wal_seed(payload) VALUES ('post-reader-frame')",
            [],
        )?;

        let (events, reporter) = collect_startup_events();
        let mut policy = sqlite_startup_test_policy(1);
        policy.large_wal_checkpoint_step = StartupStepRuntimePolicy::new(
            StdDuration::from_millis(10),
            Some(StdDuration::from_secs(7)),
        );
        let error = match SqliteStore::open_for_startup(&db_path, &policy, Some(&reporter)) {
            Ok(_) => {
                anyhow::bail!("busy large-WAL checkpoint must fail closed before startup handoff")
            }
            Err(error) => error,
        };
        assert!(
            format!("{error:#}")
                .contains("sqlite startup large WAL checkpoint truncate remained busy"),
            "unexpected startup checkpoint error: {error:#}"
        );

        let recorded = events.lock().expect("startup reporter mutex poisoned");
        assert!(
            startup_event_index(
                &recorded,
                SQLITE_STARTUP_LARGE_WAL_CHECKPOINT_TRUNCATE_STAGE,
                StartupStepOutcome::Failed,
            )
            .is_some(),
            "busy checkpoint must emit failed startup progress"
        );
        assert!(
            startup_event_index(
                &recorded,
                "sqlite_pragma_journal_mode_wal",
                StartupStepOutcome::Started,
            )
            .is_none(),
            "startup must fail closed before sqlite_pragma_journal_mode_wal when checkpoint is busy"
        );
        reader.execute_batch("ROLLBACK")?;
        Ok(())
    }

    #[test]
    fn sqlite_startup_bootstrap_reports_stage_progress() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("sqlite-startup-bootstrap-progress.db");
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
                Some(StdDuration::from_secs(3)),
            ),
            large_wal_checkpoint_threshold_bytes:
                SQLITE_STARTUP_LARGE_WAL_CHECKPOINT_THRESHOLD_BYTES,
        };

        let bootstrap = SqliteStore::open_and_migrate_for_startup(
            Path::new(&db_path),
            &migration_dir,
            &policy,
            Some(&reporter),
        )?;
        assert!(bootstrap.applied_migrations > 0);

        let recorded = events.lock().expect("startup reporter mutex poisoned");
        for stage in [
            "sqlite_open_connection",
            "sqlite_pragma_journal_mode_wal",
            "sqlite_schema_migrations_bootstrap",
            "sqlite_migrations_scan",
            "sqlite_migrations_apply",
        ] {
            assert!(
                recorded.iter().any(|event| {
                    event.stage == stage && event.outcome == StartupStepOutcome::Completed
                }),
                "startup bootstrap must emit completed progress for stage {stage}"
            );
        }
        assert!(
            recorded.iter().any(|event| {
                event.stage == "sqlite_migrations_deferred"
                    && event.outcome == StartupStepOutcome::Skipped
                    && event
                        .detail
                        .as_deref()
                        .map(|detail| detail.contains("0039_observed_swaps_sol_leg_ts_index.sql"))
                        .unwrap_or(false)
            }),
            "startup bootstrap must emit an explicit deferred migration outcome for startup-deferred performance indexes"
        );
        Ok(())
    }
