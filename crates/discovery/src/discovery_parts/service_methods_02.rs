impl DiscoveryService {
    fn published_universe_telemetry(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        active_wallets: &HashSet<String>,
    ) -> Result<(usize, usize)> {
        let Some(publication_state) = store.discovery_publication_state()? else {
            return Ok((active_wallets.len(), active_wallets.len()));
        };
        let Some(last_published_window_start) = publication_state.last_published_window_start
        else {
            return Ok((active_wallets.len(), active_wallets.len()));
        };
        let persisted_rows =
            store.load_wallet_metric_snapshots_for_window(last_published_window_start)?;
        if persisted_rows.is_empty() {
            return Ok((active_wallets.len(), active_wallets.len()));
        }
        let snapshots = self.wallet_snapshots_from_persisted_metric_rows(now, persisted_rows);
        let eligible_wallets = rank_follow_candidates(&snapshots, self.config.min_score).len();
        Ok((snapshots.len(), eligible_wallets))
    }

    fn fail_close_without_recent_universe(
        &self,
        store: &SqliteStore,
        window_start: DateTime<Utc>,
        metrics_window_start: DateTime<Utc>,
        publish_due: bool,
        force_followlist_deactivation: bool,
        cap_truncation_telemetry: &CapTruncationTelemetrySnapshot,
        scoring_source: &'static str,
        reason: &str,
        now: DateTime<Utc>,
    ) -> Result<DiscoverySummary> {
        let follow_delta = store.persist_discovery_cycle(
            &[],
            &[],
            &[],
            false,
            publish_due || force_followlist_deactivation,
            now,
            reason,
        )?;
        self.persist_trusted_selection_state(
            store,
            TrustedSelectionState::Invalid,
            None,
            None,
            None,
            true,
            reason,
            now,
        )?;
        let _ = self.persist_publication_state(
            store,
            DiscoveryRuntimeMode::FailClosed,
            publish_due,
            metrics_window_start,
            None,
            scoring_source,
            reason,
            now,
        )?;
        if publish_due {
            self.record_live_publish(now);
        }
        let active_follow_wallets = store.list_active_follow_wallets()?.len();
        Ok(DiscoverySummary {
            window_start,
            wallets_seen: 0,
            eligible_wallets: 0,
            metrics_written: 0,
            follow_promoted: 0,
            follow_demoted: follow_delta.deactivated,
            active_follow_wallets,
            top_wallets: Vec::new(),
            published: publish_due,
            ..DiscoverySummary::default()
        }
        .with_runtime_mode(DiscoveryRuntimeMode::FailClosed)
        .with_scoring_source(scoring_source)
        .with_cap_truncation_telemetry(cap_truncation_telemetry))
    }

    fn runtime_cursor_from_swap(swap: &SwapEvent) -> DiscoveryRuntimeCursor {
        DiscoveryRuntimeCursor {
            ts_utc: swap.ts_utc,
            slot: swap.slot,
            signature: swap.signature.clone(),
        }
    }

    fn runtime_cursor_cmp(
        left: &DiscoveryRuntimeCursor,
        right: &DiscoveryRuntimeCursor,
    ) -> Ordering {
        left.ts_utc
            .cmp(&right.ts_utc)
            .then_with(|| left.slot.cmp(&right.slot))
            .then_with(|| left.signature.cmp(&right.signature))
    }

    fn first_observed_swap_cursor_in_window(
        &self,
        store: &SqliteStore,
        window_start: DateTime<Utc>,
        until: &DiscoveryRuntimeCursor,
        deadline: Instant,
    ) -> Result<(Option<DiscoveryRuntimeCursor>, bool)> {
        let mut first_cursor: Option<DiscoveryRuntimeCursor> = None;
        let page = store.for_each_observed_swap_in_window_after_cursor_with_budget(
            window_start,
            until.ts_utc,
            None,
            1,
            deadline,
            |swap| {
                first_cursor = Some(Self::runtime_cursor_from_swap(&swap));
                Ok(())
            },
        )?;
        Ok((first_cursor, page.time_budget_exhausted))
    }

    pub fn repair_runtime_store_publication_truth_from_recent_raw_journal_if_needed(
        &self,
        runtime_store: &SqliteStore,
        journal_store: Option<&SqliteStore>,
        now: DateTime<Utc>,
        replay_batch_size: usize,
        deadline: Instant,
    ) -> Result<DiscoveryPublicationTruthRepairTelemetry> {
        self.repair_runtime_store_publication_truth_from_recent_raw_journal_if_needed_with_options(
            runtime_store,
            journal_store,
            now,
            replay_batch_size,
            deadline,
            None,
            None,
            true,
        )
    }

}
