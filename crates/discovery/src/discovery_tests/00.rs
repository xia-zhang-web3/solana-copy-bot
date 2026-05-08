    // Reduced inline fixture copied from the extracted prod morning fallback row.
    // It preserves the exact timestamps/counters/cursor/runtime mode that matter for
    // the replay state-machine regression, without depending on machine-local `.tmp/`.
    const EXTRACTED_PROD_MORNING_REBUILD_ROW_JSON: &str = r#"[{
        "phase":"replay",
        "window_start":"2026-03-30T07:37:54.180093001+00:00",
        "horizon_end":"2026-04-04T07:37:54.180093001+00:00",
        "metrics_window_start":"2026-03-30T07:00:00+00:00",
        "phase_cursor_ts":null,
        "phase_cursor_slot":null,
        "phase_cursor_signature":null,
        "prepass_rows_processed":77011,
        "prepass_pages_processed":1538,
        "replay_rows_processed":0,
        "replay_pages_processed":0,
        "chunks_completed":166,
        "started_at":"2026-04-02T20:35:16.408276183+00:00",
        "updated_at":"2026-04-04T07:39:54.175668054+00:00"
    }]"#;

    const EXTRACTED_PROD_MORNING_PUBLICATION_ROW_JSON: &str = r#"[{
        "publication_runtime_mode":"fail_closed",
        "publication_reason":"raw_window_incomplete_no_recent_published_universe",
        "publication_wallet_ids_json":"[]",
        "updated_at":"2026-04-04T07:40:56.665448258+00:00"
    }]"#;

    #[derive(Debug, Deserialize)]
    struct ExtractedPersistedRebuildRow {
        phase: String,
        window_start: DateTime<Utc>,
        horizon_end: DateTime<Utc>,
        metrics_window_start: DateTime<Utc>,
        phase_cursor_ts: Option<DateTime<Utc>>,
        phase_cursor_slot: Option<u64>,
        phase_cursor_signature: Option<String>,
        prepass_rows_processed: usize,
        prepass_pages_processed: usize,
        replay_rows_processed: usize,
        replay_pages_processed: usize,
        chunks_completed: usize,
        started_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
    }

    #[derive(Debug, Deserialize)]
    struct ExtractedPublicationRow {
        publication_runtime_mode: String,
        publication_reason: String,
        publication_wallet_ids_json: String,
        updated_at: DateTime<Utc>,
    }

    fn extracted_prod_morning_unique_buy_mints() -> Vec<String> {
        (0..22_089)
            .map(|idx| format!("extracted_prod_mint_{idx:05}"))
            .collect()
    }

    fn extracted_prod_morning_by_wallet() -> HashMap<String, WalletAccumulator> {
        (0..24_300)
            .map(|idx| {
                (
                    format!("extracted_prod_wallet_{idx:05}"),
                    WalletAccumulator::default(),
                )
            })
            .collect()
    }

    fn extracted_prod_morning_fallback_payload_json() -> Result<String> {
        let unique_buy_mints = extracted_prod_morning_unique_buy_mints();
        let buy_mint_counts: BTreeMap<String, u32> = unique_buy_mints
            .iter()
            .cloned()
            .map(|mint| (mint, 1))
            .collect();
        let token_quality_cache: HashMap<String, quality_cache::TokenQualityResolution> =
            unique_buy_mints
                .iter()
                .cloned()
                .map(|mint| (mint, quality_cache::TokenQualityResolution::Missing))
                .collect();
        let payload = PersistedStreamRebuildPayload {
            unique_buy_mints,
            discovery_critical_target_buy_mints: Vec::new(),
            buy_mint_counts,
            collect_buy_mints_cursor_token: None,
            collect_buy_mints_prepass_complete: true,
            collect_buy_mints_mode: CollectBuyMintsMode::FreshScan,
            collect_buy_mints_reconcile_source_window_start: None,
            collect_buy_mints_reconcile_source_horizon_end: None,
            collect_buy_mints_reconcile_expired_head_cursor: None,
            collect_buy_mints_reconcile_new_tail_cursor: None,
            collect_buy_mints_reconcile_expired_head_cursor_token: None,
            collect_buy_mints_reconcile_new_tail_cursor_token: None,
            collect_buy_mints_reconcile_expired_head_pending_mints: Vec::new(),
            collect_buy_mints_reconcile_new_tail_slice_end_token: None,
            collect_buy_mints_reconcile_new_tail_pending_mints: Vec::new(),
            replay_mode: ReplayMode::WalletStatsThenSolLeg,
            replay_wallet_stats_complete: false,
            replay_wallet_stats_rows_processed: 378_138,
            replay_wallet_stats_pages_processed: 28,
            replay_wallet_stats_wallet_cursor: Some(
                "2eviCimfYrYCeAFueuswzN61TJyT2HsbYWVx6pcCLhsy".to_string(),
            ),
            replay_wallet_stats_day_count_source_progress:
                ReplayWalletStatsDayCountSourceProgress {
                    fast_path_pages_processed: 27,
                    fallback_pages_processed: 0,
                    fast_path_wallets_processed: 24_300,
                    fallback_wallets_processed: 0,
                },
            replay_wallet_stats_budget_floor_wallets: 662_481,
            replay_wallet_stats_last_partial_cycle_pages_processed: 27,
            replay_wallet_stats_last_partial_cycle_wallets_processed: 24_300,
            replay_wallet_stats_last_partial_cycle_elapsed_ms: 60_002,
            replay_wallet_stats_milestone_reached: false,
            replay_sol_leg_reentry_pending: false,
            replay_sol_leg_last_partial_cycle_pages_processed: 0,
            replay_sol_leg_last_partial_cycle_rows_processed: 0,
            replay_sol_leg_last_partial_cycle_elapsed_ms: 0,
            replay_sol_leg_budget_floor_pages: 0,
            replay_sol_leg_retained_contract_floor_pages: 0,
            replay_candidate_activity_backfill_required: false,
            replay_candidate_activity_backfill_pending: false,
            replay_candidate_activity_backfill_wallet_cursor: None,
            replay_exact_target_surface_wallet_cursor: None,
            replay_exact_target_surface_pre_row_blocked: false,
            replay_exact_target_surface_staged_wallet_ids: Vec::new(),
            replay_exact_target_surface_staged_wallet_cursor_after: None,
            token_quality_cache,
            token_quality_progress: quality_cache::TokenQualityResolutionProgress {
                next_mint_index: 22_089,
                rpc_attempted: 0,
                rpc_spent_ms: 0,
            },
            by_wallet: extracted_prod_morning_by_wallet(),
            token_states: HashMap::new(),
            token_recent_sol_trades: HashMap::new(),
            pending_rug_checks: VecDeque::new(),
            token_pending_buy_starts: HashMap::new(),
            completed_snapshots: Vec::new(),
            publish_pending_requested_wallet_ids: None,
            publish_pending_quality_retry_mints: None,
        };
        serde_json::to_string(&payload).context(
            "failed serializing reduced inline payload for extracted prod morning fallback fixture",
        )
    }

    fn phase_from_extracted_row(phase: &str) -> DiscoveryPersistedRebuildPhase {
        match phase {
            "collect_buy_mints" => DiscoveryPersistedRebuildPhase::CollectBuyMints,
            "resolve_token_quality" => DiscoveryPersistedRebuildPhase::ResolveTokenQuality,
            "replay" => DiscoveryPersistedRebuildPhase::Replay,
            "publish_pending" => DiscoveryPersistedRebuildPhase::PublishPending,
            other => panic!("unexpected extracted rebuild phase: {other}"),
        }
    }

    fn load_extracted_prod_morning_fallback_state() -> Result<(
        PersistedStreamRebuildState,
        DiscoveryRuntimeMode,
        String,
        Vec<String>,
        DateTime<Utc>,
    )> {
        let payload_json = extracted_prod_morning_fallback_payload_json()?;
        let payload: PersistedStreamRebuildPayload = serde_json::from_str(&payload_json)
            .context("failed deserializing reduced inline extracted prod rebuild payload")?;
        let rebuild_rows: Vec<ExtractedPersistedRebuildRow> =
            serde_json::from_str(EXTRACTED_PROD_MORNING_REBUILD_ROW_JSON)
                .context("failed deserializing inline extracted persisted rebuild row")?;
        let publication_rows: Vec<ExtractedPublicationRow> =
            serde_json::from_str(EXTRACTED_PROD_MORNING_PUBLICATION_ROW_JSON)
                .context("failed deserializing inline extracted publication row")?;

        let rebuild_row = rebuild_rows
            .into_iter()
            .next()
            .context("missing extracted persisted rebuild row")?;
        let publication_row = publication_rows
            .into_iter()
            .next()
            .context("missing extracted publication row")?;
        let published_wallet_ids: Vec<String> =
            serde_json::from_str(&publication_row.publication_wallet_ids_json)
                .context("failed deserializing extracted publication wallet ids")?;
        let runtime_mode = DiscoveryRuntimeMode::parse(&publication_row.publication_runtime_mode)
            .context("failed parsing extracted publication runtime mode")?;

        Ok((
            PersistedStreamRebuildState {
                phase: phase_from_extracted_row(&rebuild_row.phase),
                window_start: rebuild_row.window_start,
                horizon_end: rebuild_row.horizon_end,
                metrics_window_start: rebuild_row.metrics_window_start,
                phase_cursor: match (
                    rebuild_row.phase_cursor_ts,
                    rebuild_row.phase_cursor_slot,
                    rebuild_row.phase_cursor_signature,
                ) {
                    (Some(ts_utc), Some(slot), Some(signature)) => Some(DiscoveryRuntimeCursor {
                        ts_utc,
                        slot,
                        signature,
                    }),
                    (None, None, None) => None,
                    partial => panic!("unexpected partial extracted replay cursor: {partial:?}"),
                },
                prepass_rows_processed: rebuild_row.prepass_rows_processed,
                prepass_pages_processed: rebuild_row.prepass_pages_processed,
                replay_rows_processed: rebuild_row.replay_rows_processed,
                replay_pages_processed: rebuild_row.replay_pages_processed,
                chunks_completed: rebuild_row.chunks_completed,
                started_at: rebuild_row.started_at,
                updated_at: rebuild_row.updated_at,
                payload,
            },
            runtime_mode,
            publication_row.publication_reason,
            published_wallet_ids,
            publication_row.updated_at,
        ))
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

    fn seed_recent_published_universe(
        discovery: &DiscoveryService,
        store: &SqliteStore,
        now: DateTime<Utc>,
        metrics_window_start: DateTime<Utc>,
        scoring_source: &str,
        published_wallet_ids: &HashSet<String>,
    ) -> Result<()> {
        store.set_discovery_publication_state_with_options(
            &DiscoveryPublicationStateUpdate {
                runtime_mode: DiscoveryRuntimeMode::Healthy,
                reason: "test_recent_published_universe".to_string(),
                last_published_at: Some(now),
                last_published_window_start: Some(metrics_window_start),
                published_scoring_source: Some(scoring_source.to_string()),
                published_wallet_ids: Some(published_wallet_ids.iter().cloned().collect()),
            },
            false,
            Some(
                discovery
                    .publication_selection_policy_fingerprint()
                    .as_str(),
            ),
        )
    }
