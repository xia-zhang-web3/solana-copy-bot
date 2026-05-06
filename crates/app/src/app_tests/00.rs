    const TEST_OBSERVED_SWAP_WRITER_BATCH_MAX_SIZE: usize = 128;

    fn make_test_store(name: &str) -> Result<(SqliteStore, PathBuf)> {
        let unique = format!(
            "{}-{}-{}",
            name,
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("copybot-app-{unique}.db"));
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;
        Ok((store, db_path))
    }

    fn seed_startup_heartbeat_wal_backlog(store: &SqliteStore, rows: usize) -> Result<()> {
        store.set_wal_autocheckpoint_pages(0).context(
            "failed to defer wal_autocheckpoint while seeding startup heartbeat backlog",
        )?;
        let details_payload = "startup-heartbeat-wal-ballast".repeat(128);
        for idx in 0..rows {
            store
                .insert_risk_event(
                    &format!("startup-heartbeat-seed-{idx:05}"),
                    "warn",
                    Utc::now() + chrono::Duration::milliseconds(idx as i64),
                    Some(&details_payload),
                )
                .with_context(|| format!("failed seeding startup heartbeat ballast row {idx}"))?;
        }
        Ok(())
    }

    fn open_slow_startup_heartbeat_test_connection(
        db_path: &Path,
        wal_autocheckpoint_pages: i64,
    ) -> Result<Connection> {
        let conn = Connection::open(db_path).with_context(|| {
            format!(
                "failed opening startup heartbeat test db {}",
                db_path.display()
            )
        })?;
        conn.busy_timeout(StdDuration::from_secs(5))
            .context("failed to set startup heartbeat test busy timeout")?;
        conn.pragma_update(None, "journal_mode", "WAL")
            .context("failed to force WAL mode for startup heartbeat test connection")?;
        conn.pragma_update(None, "wal_autocheckpoint", wal_autocheckpoint_pages)
            .with_context(|| {
                format!(
                    "failed to set startup heartbeat test wal_autocheckpoint={wal_autocheckpoint_pages}"
                )
            })?;
        conn.progress_handler(
            25,
            Some(|| {
                std::thread::sleep(StdDuration::from_millis(1));
                false
            }),
        );
        Ok(conn)
    }

    fn run_startup_heartbeat_insert_step(
        conn: Connection,
        reporter: Option<&StartupStepProgressReporter>,
        timeout: StdDuration,
    ) -> Result<()> {
        run_observed_startup_step(
            "test_startup_sqlite_heartbeat",
            StartupStepRuntimePolicy::new(StdDuration::from_millis(5), Some(timeout)),
            reporter,
            move || {
                conn.execute(
                    "INSERT INTO system_heartbeat(component, ts, status) VALUES (?1, datetime('now'), ?2)",
                    params!["copybot-app", "startup"],
                )
                .context("failed to write startup heartbeat via raw sqlite connection")?;
                Ok(())
            },
        )
    }

    fn seed_test_discovery_critical_target_buy_mints(
        store: &SqliteStore,
        target_buy_mints: &[&str],
        unique_buy_mints: &[&str],
    ) -> Result<()> {
        let now = DateTime::parse_from_rfc3339("2026-03-14T16:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        store.upsert_discovery_persisted_rebuild_state(&DiscoveryPersistedRebuildStateRow {
            phase: DiscoveryPersistedRebuildPhase::CollectBuyMints,
            window_start: now - chrono::Duration::days(2),
            horizon_end: now,
            metrics_window_start: now - chrono::Duration::hours(1),
            phase_cursor: None,
            prepass_rows_processed: 0,
            prepass_pages_processed: 0,
            replay_rows_processed: 0,
            replay_pages_processed: 0,
            chunks_completed: 0,
            state_json: serde_json::json!({
                "discovery_critical_target_buy_mints": target_buy_mints,
                "unique_buy_mints": unique_buy_mints,
            })
            .to_string(),
            started_at: now,
            updated_at: now,
        })?;
        Ok(())
    }

    fn test_publication_policy_fingerprint(config: &copybot_config::DiscoveryConfig) -> String {
        format!(
            concat!(
                "follow_top_n={};",
                "scoring_window_days={};",
                "decay_window_days={};",
                "min_leader_notional_sol={:.6};",
                "min_trades={};",
                "min_active_days={};",
                "min_score={:.6};",
                "max_tx_per_minute={};",
                "min_buy_count={};",
                "min_tradable_ratio={:.6};",
                "require_open_positions_for_publication={};",
                "max_rug_ratio={:.6};",
                "rug_lookahead_seconds={};",
                "thin_market_min_volume_sol={:.6};",
                "thin_market_min_unique_traders={}"
            ),
            config.follow_top_n,
            config.scoring_window_days,
            config.decay_window_days,
            config.min_leader_notional_sol,
            config.min_trades,
            config.min_active_days,
            config.min_score,
            config.max_tx_per_minute,
            config.min_buy_count,
            config.min_tradable_ratio,
            config.require_open_positions_for_publication,
            config.max_rug_ratio,
            config.rug_lookahead_seconds,
            config.thin_market_min_volume_sol,
            config.thin_market_min_unique_traders,
        )
    }

    fn permissive_shadow_quality() -> ShadowConfig {
        let mut config = ShadowConfig::default();
        config.min_token_age_seconds = 0;
        config.min_holders = 0;
        config.min_liquidity_sol = 0.0;
        config.min_volume_5m_sol = 0.0;
        config.min_unique_traders_5m = 0;
        config
    }

    fn error_chain_contains(error: &anyhow::Error, needle: &str) -> bool {
        error
            .chain()
            .any(|cause| cause.to_string().contains(needle))
    }

    fn maintenance_test_writer_snapshot() -> ObservedSwapWriterSnapshot {
        ObservedSwapWriterSnapshot {
            pending_requests: 0,
            write_latency_ms_p95: 0,
            raw_batch_write_ms_p95: 0,
            observed_swaps_insert_ms_p95: 0,
            wallet_activity_days_ms_p95: 0,
            journal_enqueue_wait_ms_p95: 0,
            journal_batch_write_ms_p95: 0,
            worker_busy_ms_p95: 0,
            journal_queue_depth_batches: 0,
            journal_queue_row_debt: 0,
            journal_queue_capacity_batches: 32,
            journal_overflow_depth_batches: 0,
            journal_overflow_row_debt: 0,
            journal_overflow_capacity_batches: 64,
            journal_overflow_row_debt_capacity: 0,
            journal_writer_inflight_rows: 0,
            journal_sqlite_write_retry_total: 0,
            journal_sqlite_busy_error_total: 0,
        }
    }

    fn maintenance_test_ingestion_snapshot(fill_ratio: f64) -> IngestionRuntimeSnapshot {
        IngestionRuntimeSnapshot {
            ts_utc: Utc::now(),
            ws_notifications_enqueued: 0,
            ws_notifications_replaced_oldest: 0,
            grpc_message_total: 0,
            grpc_transaction_updates_total: 0,
            parse_rejected_total: 0,
            grpc_decode_errors: 0,
            rpc_429: 0,
            rpc_5xx: 0,
            ingestion_lag_ms_p95: 0,
            yellowstone_output_queue_depth: 25,
            yellowstone_output_queue_capacity: 100,
            yellowstone_output_queue_fill_ratio: fill_ratio,
            yellowstone_output_oldest_age_ms: 500,
        }
    }

    fn test_swap(signature: &str) -> SwapEvent {
        SwapEvent {
            wallet: "wallet-test".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-test".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: signature.to_string(),
            slot: 1,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-14T16:00:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        }
    }

    fn irrelevant_backpressure_swap(
        signature: &str,
        idx: usize,
        base_ts: DateTime<Utc>,
    ) -> SwapEvent {
        let mut swap = test_swap(signature);
        swap.wallet = format!("wallet-irrelevant-{idx:05}");
        swap.token_out = format!("token-irrelevant-{idx:05}");
        swap.slot = 50_000 + idx as u64;
        swap.ts_utc = base_ts + chrono::Duration::milliseconds(idx as i64);
        swap
    }

    fn seed_runtime_raw_insert_backpressure(db_path: &Path) -> Result<()> {
        let conn = rusqlite::Connection::open(db_path)?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS raw_writer_backpressure_pad(
                id INTEGER PRIMARY KEY,
                payload BLOB NOT NULL
             );
             CREATE TRIGGER slow_observed_swaps_insert
             AFTER INSERT ON observed_swaps
             BEGIN
                 INSERT INTO raw_writer_backpressure_pad(payload)
                 WITH RECURSIVE cnt(x) AS (
                     SELECT 1
                     UNION ALL
                     SELECT x + 1 FROM cnt WHERE x < 8
                 )
                 SELECT randomblob(4096) FROM cnt;
             END;",
        )?;
        Ok(())
    }

    fn should_buffer_backpressured_irrelevant_observed_swap_old(
        discovery_critical: bool,
        follow_snapshot: &FollowSnapshot,
        open_shadow_lots: &HashSet<(String, String)>,
        shadow_strategy_fail_closed: bool,
    ) -> bool {
        !zero_universe_fail_closed_discovery_market_context_mode(
            follow_snapshot,
            open_shadow_lots,
            shadow_strategy_fail_closed,
        ) || discovery_critical
    }

    fn irrelevant_observed_swap_requires_discovery_critical_persistence_old(
        swap: &SwapEvent,
        follow_snapshot: &FollowSnapshot,
        open_shadow_lots: &HashSet<(String, String)>,
        shadow_strategy_fail_closed: bool,
        discovery_critical_target_buy_mints: &HashSet<String>,
    ) -> bool {
        if !zero_universe_fail_closed_discovery_market_context_mode(
            follow_snapshot,
            open_shadow_lots,
            shadow_strategy_fail_closed,
        ) {
            return false;
        }

        match classify_swap_side(swap) {
            Some(ShadowSwapSide::Buy) => {
                discovery_critical_target_buy_mints.is_empty()
                    || discovery_critical_target_buy_mints.contains(swap.token_out.as_str())
            }
            Some(ShadowSwapSide::Sell) => {
                !discovery_critical_target_buy_mints.is_empty()
                    && discovery_critical_target_buy_mints.contains(swap.token_in.as_str())
            }
            None => false,
        }
    }

    fn load_discovery_critical_target_buy_mints_old(
        store: &SqliteStore,
    ) -> Result<HashSet<String>> {
        let Some(state_row) = store.load_discovery_persisted_rebuild_state_read_only()? else {
            return Ok(HashSet::new());
        };
        let payload: DiscoveryCriticalPersistedRebuildPayloadTargetMints =
            serde_json::from_str(&state_row.state_json).context(
                "failed parsing discovery persisted rebuild payload while loading target buy mints for critical market-context persistence",
            )?;
        let target_buy_mints = if payload.discovery_critical_target_buy_mints.is_empty() {
            payload.unique_buy_mints
        } else {
            payload.discovery_critical_target_buy_mints
        };
        Ok(target_buy_mints.into_iter().collect())
    }

    fn refresh_discovery_critical_target_buy_mints_or_warn_old(
        store: &SqliteStore,
        target_buy_mints: &mut HashSet<String>,
    ) -> Result<()> {
        match load_discovery_critical_target_buy_mints_old(store) {
            Ok(loaded) => {
                *target_buy_mints = loaded;
                Ok(())
            }
            Err(error) => {
                if is_fatal_sqlite_anyhow_error(&error) {
                    return Err(error).context(
                        "failed refreshing discovery-critical target buy mints with fatal sqlite I/O",
                    );
                }
                warn!(
                    error = %error,
                    "failed refreshing discovery-critical target buy mints; keeping the previous target set"
                );
                Ok(())
            }
        }
    }

    #[derive(Debug, Clone, Copy)]
    struct NoncriticalIrrelevantBackpressureSummary {
        baseline_rows_persisted: usize,
        first_backpressure_pending_requests: usize,
        first_backpressure_discovery_critical: bool,
        upstream_queue_depth_at_first_backpressure: usize,
        max_pending_requests_before_pause: usize,
        pending_irrelevant_queue_depth_at_pause: usize,
        upstream_queue_depth_shortly_after_pause: usize,
        upstream_queue_depth_after_escalation: usize,
        runtime_wal_bytes_at_pause: u64,
        sqlite_write_retry_delta: u64,
        sqlite_busy_error_delta: u64,
        journal_queue_depth_at_pause: usize,
        dropped_noncritical_irrelevant_swaps: usize,
        ingestion_paused_by_pending_irrelevant_queue: bool,
    }

    #[derive(Debug, Clone, Copy)]
    struct NoncriticalIrrelevantOutputPressureWaveSummary {
        baseline_rows_persisted: usize,
        writer_pending_requests_at_wave_peak: usize,
        journal_queue_depth_at_wave_peak: usize,
        upstream_queue_depth_before_loop: usize,
        upstream_queue_depth_after_loop: usize,
        accepted_noncritical_irrelevant_swaps: usize,
        dropped_noncritical_irrelevant_swaps: usize,
        completed_waves: usize,
    }

    #[derive(Debug, Clone, Copy)]
    struct IrrelevantNotFollowedNoOwnershipSurfaceSummary {
        writer_pending_requests_peak: usize,
        first_backpressure_pending_requests: usize,
        journal_queue_depth_at_peak: usize,
        dropped_swaps: usize,
        accepted_swaps: usize,
        completed_waves: usize,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum IrrelevantObservedSwapScenarioBranch {
        NotFollowed,
        Unclassified,
    }

    #[derive(Debug, Clone, Copy)]
    struct ZeroUniverseNoncriticalIrrelevantZeroOutputPressureSummary {
        baseline_rows_persisted: usize,
        first_backpressure_pending_requests: usize,
        writer_pending_requests_peak: usize,
        journal_queue_depth_at_peak: usize,
        yellowstone_output_queue_depth: u64,
        yellowstone_output_queue_fill_ratio: f64,
        accepted_noncritical_irrelevant_swaps: usize,
        dropped_noncritical_irrelevant_swaps: usize,
        first_backpressure_discovery_critical: bool,
        completed_waves: usize,
        best_effort_budget_exhausted: bool,
    }
