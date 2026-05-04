    #[test]
    fn recent_raw_journal_prune_due_stays_paused_while_overflow_backlog_exists_stage1() -> Result<()>
    {
        let unique = format!(
            "copybot-app-recent-raw-journal-prune-overflow-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let journal_db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let journal_store = SqliteStore::open(Path::new(&journal_db_path))?;
        journal_store.ensure_recent_raw_journal_tables()?;
        let now = DateTime::parse_from_rfc3339("2026-04-08T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let telemetry = ObservedSwapWriterTelemetry::default();
        telemetry
            .journal_overflow_depth_batches
            .store(3, Ordering::Relaxed);
        let should_prune = recent_raw_journal_prune_due(
            &journal_store,
            &ObservedSwapRecentRawJournalConfig {
                sqlite_path: journal_db_path
                    .to_str()
                    .context("journal sqlite path must be valid utf-8")?
                    .to_string(),
                retention_days: 8,
                writer_queue_capacity_batches: 16,
                write_coalesce_max_batches: 1,
                overflow_capacity_batches: 64,
                skip_prune_while_backlogged: true,
                skip_startup_prune: true,
            },
            &telemetry,
            now,
        )?;
        assert!(
            !should_prune,
            "recent_raw journal prune must stay paused while overflow backlog still represents unflushed hot-path work"
        );
        let inflight_telemetry = ObservedSwapWriterTelemetry::default();
        inflight_telemetry.note_journal_queue_enqueued(1);
        inflight_telemetry.note_journal_writer_inflight_started(4);
        let should_prune_while_inflight = recent_raw_journal_prune_due(
            &journal_store,
            &ObservedSwapRecentRawJournalConfig {
                sqlite_path: journal_db_path
                    .to_str()
                    .context("journal sqlite path must be valid utf-8")?
                    .to_string(),
                retention_days: 8,
                writer_queue_capacity_batches: 16,
                write_coalesce_max_batches: 1,
                overflow_capacity_batches: 64,
                skip_prune_while_backlogged: true,
                skip_startup_prune: true,
            },
            &inflight_telemetry,
            now,
        )?;
        assert!(
            !should_prune_while_inflight,
            "recent_raw journal prune/check must stay paused while a hot-path journal write is still inflight"
        );
        let _ = std::fs::remove_file(journal_db_path);
        Ok(())
    }
