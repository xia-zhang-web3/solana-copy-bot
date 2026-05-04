    #[test]
    fn observed_swap_retention_maintenance_respects_backfill_source_protection() -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-retention-protect-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;

        let stale_swap = SwapEvent {
            wallet: "wallet-protected-old".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-protected-old".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-protected-old".to_string(),
            slot: 100,
            ts_utc: Utc::now() - ChronoDuration::days(3),
            exact_amounts: None,
        };
        seed_store.insert_observed_swaps_batch(&[stale_swap.clone()])?;
        seed_store.set_discovery_scoring_backfill_source_protection(
            Utc::now() - ChronoDuration::days(4),
            Utc::now() + ChronoDuration::hours(1),
        )?;

        let summary = super::run_observed_swap_retention_maintenance_once(
            db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?,
            super::ObservedSwapRetentionConfig::production(1, 7, false),
            None,
        )?;
        assert_eq!(summary.raw_deleted_rows, 0);
        assert_eq!(summary.raw_delete_batches, 0);
        assert_eq!(summary.checkpoint.mode, "passive_runtime");

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        let stale_rows = verify_store
            .load_observed_swaps_since(Utc::now() - ChronoDuration::days(7))?
            .into_iter()
            .filter(|swap| swap.signature == "sig-protected-old")
            .count();
        assert_eq!(
            stale_rows, 1,
            "source protection must defer raw retention pruning"
        );
        let _ = std::fs::remove_file(db_path);

        Ok(())
    }

    #[test]
    fn observed_swap_retention_maintenance_stops_after_raw_batch_budget_and_skips_checkpoint(
    ) -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-retention-bounded-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;

        let stale_ts = Utc::now() - ChronoDuration::days(3);
        let stale_rows = super::OBSERVED_SWAP_RETENTION_DELETE_BATCH_SIZE
            * super::OBSERVED_SWAP_RETENTION_MAX_RAW_DELETE_BATCHES_PER_RUN
            + 1;
        for idx in 0..stale_rows {
            seed_store.insert_observed_swap(&SwapEvent {
                wallet: "wallet-bounded-retention".to_string(),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: format!("token-bounded-{idx}"),
                amount_in: 1.0,
                amount_out: 10.0,
                signature: format!("sig-bounded-retention-{idx}"),
                slot: idx as u64 + 1,
                ts_utc: stale_ts + ChronoDuration::seconds(idx as i64),
                exact_amounts: None,
            })?;
        }

        let summary = super::run_observed_swap_retention_maintenance_once(
            db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?,
            super::ObservedSwapRetentionConfig::production(1, 7, false),
            None,
        )?;
        assert_eq!(
            summary.raw_deleted_rows,
            super::OBSERVED_SWAP_RETENTION_DELETE_BATCH_SIZE
                * super::OBSERVED_SWAP_RETENTION_MAX_RAW_DELETE_BATCHES_PER_RUN
        );
        assert_eq!(
            summary.raw_delete_batches,
            super::OBSERVED_SWAP_RETENTION_MAX_RAW_DELETE_BATCHES_PER_RUN
        );
        assert_eq!(summary.scoring_deleted_rows, 0);
        assert!(!summary.completed_full_sweep);
        assert_eq!(summary.stop_reason, Some("raw_batch_budget"));
        assert_eq!(summary.checkpoint.mode, "skipped_bounded_run");

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        let remaining = verify_store
            .load_observed_swaps_since(Utc::now() - ChronoDuration::days(7))?
            .len();
        assert_eq!(remaining, 1);

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_retention_should_stop_on_runtime_pressure() {
        let telemetry = Arc::new(ObservedSwapWriterTelemetry::default());
        telemetry.pending_requests.store(1, Ordering::Relaxed);
        let runtime_health = super::ObservedSwapRetentionRuntimeHealthHandle::new(
            super::ObservedSwapWriterHealthHandle { telemetry },
            Arc::new(Mutex::new(None)),
        );
        let mut last_sqlite_contention = SqliteContentionSnapshot::default();

        let stop_reason = super::observed_swap_retention_should_stop(
            Some(&runtime_health),
            &mut last_sqlite_contention,
            Instant::now(),
        );
        assert_eq!(stop_reason, Some("runtime_pressure"));
    }

    #[test]
    fn observed_swap_retention_checkpoint_error_requires_abort_on_xshmmap_io_failure() {
        let primary = anyhow!("database is locked");
        let fallback =
            anyhow!("disk I/O error: Error code 4874: I/O error within the xShmMap method");
        assert!(
            !super::observed_swap_retention_checkpoint_error_requires_abort(Some(&primary), None)
        );
        assert!(
            super::observed_swap_retention_checkpoint_error_requires_abort(
                Some(&primary),
                Some(&fallback)
            )
        );
    }

    #[test]
    fn observed_swap_retention_checkpoint_warn_failure_mode_is_distinct() {
        let summary = super::ObservedSwapRetentionCheckpointSummary {
            mode: "passive_runtime_failed",
            busy: 0,
            log_frames: 0,
            checkpointed_frames: 0,
        };
        assert_eq!(summary.mode, "passive_runtime_failed");
    }

    #[test]
    fn observed_swap_writer_discovery_scoring_error_requires_abort_on_xshmmap_io_failure() {
        let error = anyhow!("disk I/O error: Error code 4874: I/O error within the xShmMap method");
        assert!(super::observed_swap_writer_discovery_scoring_error_requires_abort(&error));
    }

    #[test]
    fn observed_swap_writer_discovery_scoring_error_does_not_require_abort_on_busy_lock() {
        let error = anyhow!("database is locked");
        assert!(!super::observed_swap_writer_discovery_scoring_error_requires_abort(&error));
    }

    #[test]
    fn observed_swap_writer_discovery_scoring_rug_finalize_retryable_classifier_matches_lock_only()
    {
        let replay_apply_retryable = anyhow!(
            "failed replaying discovery scoring rows during aggregate-writer startup catch-up: failed to run discovery scoring batch: failed to open discovery scoring batch transaction: database is locked"
        );
        assert!(
            super::observed_swap_writer_discovery_scoring_replay_apply_error_is_retryable(
                &replay_apply_retryable
            )
        );

        let replay_apply_fatal = anyhow!(
            "failed replaying discovery scoring rows during aggregate-writer startup catch-up: disk I/O error: Error code 4874: I/O error within the xShmMap method"
        );
        assert!(
            !super::observed_swap_writer_discovery_scoring_replay_apply_error_is_retryable(
                &replay_apply_fatal
            )
        );

        let replay_apply_unknown =
            anyhow!("failed replaying discovery scoring rows: forced replay apply failure");
        assert!(
            !super::observed_swap_writer_discovery_scoring_replay_apply_error_is_retryable(
                &replay_apply_unknown
            )
        );

        let covered_update_retryable = anyhow!(
            "failed to run discovery scoring covered_through cursor update: failed to open discovery scoring covered_through cursor update transaction: database is locked"
        );
        assert!(
            super::observed_swap_writer_discovery_scoring_covered_through_update_error_is_retryable(
                &covered_update_retryable
            )
        );

        let covered_update_fatal = anyhow!(
            "failed to run discovery scoring covered_through cursor update: disk I/O error: Error code 4874: I/O error within the xShmMap method"
        );
        assert!(
            !super::observed_swap_writer_discovery_scoring_covered_through_update_error_is_retryable(
                &covered_update_fatal
            )
        );

        let covered_update_unknown = anyhow!(
            "failed to run discovery scoring covered_through cursor update: malformed cursor"
        );
        assert!(
            !super::observed_swap_writer_discovery_scoring_covered_through_update_error_is_retryable(
                &covered_update_unknown
            )
        );

        let retryable = anyhow!(
            "failed to run discovery scoring rug finalize: failed to open discovery scoring rug finalize transaction: database is locked"
        );
        assert!(
            super::observed_swap_writer_discovery_scoring_rug_finalize_error_is_retryable(
                &retryable
            )
        );

        let fatal = anyhow!(
            "failed to run discovery scoring rug finalize: disk I/O error: Error code 4874: I/O error within the xShmMap method"
        );
        assert!(
            !super::observed_swap_writer_discovery_scoring_rug_finalize_error_is_retryable(&fatal)
        );

        let unknown = anyhow!("failed to run discovery scoring rug finalize: malformed rug fact");
        assert!(
            !super::observed_swap_writer_discovery_scoring_rug_finalize_error_is_retryable(
                &unknown
            )
        );
    }

    #[test]
    fn observed_swap_writer_aggregate_queue_capacity_tracks_raw_queue_in_batches() {
        assert_eq!(
            super::observed_swap_writer_aggregate_queue_capacity(
                &ObservedSwapWriterConfig::for_test(16, 8, true, aggregate_write_config(), None,),
            ),
            2
        );
        assert_eq!(
            super::observed_swap_writer_aggregate_queue_capacity(
                &ObservedSwapWriterConfig::for_test(16, 8, false, aggregate_write_config(), None,),
            ),
            0
        );
    }

    #[test]
    fn observed_swap_writer_normal_try_enqueue_soft_limit_stays_one_batch_when_aggregates_disabled_stage1(
    ) {
        let config = ObservedSwapWriterConfig::for_test(
            super::OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY,
            super::OBSERVED_SWAP_BATCH_MAX_SIZE,
            false,
            aggregate_write_config(),
            None,
        );

        assert_eq!(
            super::observed_swap_writer_normal_try_enqueue_soft_limit(&config),
            super::OBSERVED_SWAP_BATCH_MAX_SIZE,
            "aggregate-disabled non-critical irrelevant swaps must keep the old one-batch try_enqueue budget"
        );
    }

    #[test]
    fn observed_swap_writer_normal_try_enqueue_soft_limit_uses_normal_capacity_when_aggregates_enabled_stage1(
    ) {
        let config = ObservedSwapWriterConfig::for_test(
            super::OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY,
            super::OBSERVED_SWAP_BATCH_MAX_SIZE,
            true,
            aggregate_write_config(),
            None,
        );
        let discovery_critical_reserve =
            super::observed_swap_writer_discovery_critical_reserve_requests(&config);
        let expected_normal_capacity =
            super::OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY.saturating_sub(discovery_critical_reserve);

        assert_eq!(
            super::observed_swap_writer_normal_try_enqueue_soft_limit(&config),
            expected_normal_capacity,
            "aggregate-enabled non-critical irrelevant swaps need the full normal writer budget so observed_swaps can continue feeding aggregate coverage"
        );
        assert!(
            expected_normal_capacity > super::OBSERVED_SWAP_BATCH_MAX_SIZE,
            "the aggregate-enabled budget must be above the old one-batch plateau"
        );
    }

    #[test]
    fn observed_swap_retention_effective_cutoff_requires_abort_on_fatal_protection_load_failure() {
        let now = Utc::now();
        let config = super::ObservedSwapRetentionConfig::production(1, 7, false);
        let error = super::resolve_observed_swap_retention_effective_cutoff(config, now, |_| {
            Err(anyhow!(
                "failed querying discovery_scoring_state.backfill_protect_since_ts: disk I/O error: Error code 4874: I/O error within the xShmMap method"
            ))
        })
        .expect_err("fatal protection load failure must not fall back to nominal cutoff");
        let error_text = format!("{error:#}");
        assert!(
            error_text.contains("source protection lookup failed with fatal sqlite I/O"),
            "expected fatal protection lookup context, got: {error_text}"
        );
        assert!(
            error_text.contains("xShmMap"),
            "expected fatal sqlite I/O marker to survive error chain, got: {error_text}"
        );
    }

    #[test]
    fn observed_swap_retention_effective_cutoff_falls_back_on_busy_protection_load_failure() {
        let now = Utc::now();
        let config = super::ObservedSwapRetentionConfig::production(1, 7, false);
        let effective_cutoff =
            super::resolve_observed_swap_retention_effective_cutoff(config, now, |_| {
                Err(anyhow!("database is locked"))
            })
            .expect("busy protection load failure should keep nominal fallback behavior");
        assert_eq!(
            effective_cutoff,
            super::observed_swap_retention_nominal_cutoff(now, config)
        );
    }

    #[test]
    fn observed_swap_retention_maintenance_returns_error_on_fatal_raw_delete_failure() -> Result<()>
    {
        let unique = format!(
            "copybot-app-observed-swap-retention-fatal-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;
        let stale_swap = SwapEvent {
            wallet: "wallet-fatal-delete".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-fatal-delete".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-fatal-delete".to_string(),
            slot: 100,
            ts_utc: Utc::now() - ChronoDuration::days(3),
            exact_amounts: None,
        };
        seed_store.insert_observed_swaps_batch(&[stale_swap])?;

        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute_batch(
            "CREATE TRIGGER fail_observed_swap_retention_delete
             BEFORE DELETE ON observed_swaps
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let error = super::run_observed_swap_retention_maintenance_once(
            db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?,
            super::ObservedSwapRetentionConfig::production(1, 7, false),
            None,
        )
        .expect_err("fatal raw delete failure must propagate out of retention maintenance");
        let error_text = format!("{error:#}");
        assert!(
            error_text.contains("failed to delete observed swap retention slice"),
            "expected retention delete failure context, got: {error_text}"
        );
        assert!(
            error_text.contains("xShmMap"),
            "expected fatal sqlite I/O marker to survive error chain, got: {error_text}"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }
