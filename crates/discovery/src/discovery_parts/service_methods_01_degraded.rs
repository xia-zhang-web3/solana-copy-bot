impl DiscoveryService {
    fn degraded_summary_from_published_universe(
        &self,
        store: &SqliteStore,
        window_start: DateTime<Utc>,
        metrics_window_start: DateTime<Utc>,
        publish_due: bool,
        active_wallets: HashSet<String>,
        cap_truncation_telemetry: &CapTruncationTelemetrySnapshot,
        scoring_source: &'static str,
        reason: &str,
        now: DateTime<Utc>,
    ) -> Result<DiscoverySummary> {
        let mut desired_wallets: Vec<String> = active_wallets.iter().cloned().collect();
        desired_wallets.sort();
        let follow_delta =
            store.persist_discovery_cycle(&[], &[], &desired_wallets, true, true, now, reason)?;
        let (wallets_seen, eligible_wallets) =
            self.published_universe_telemetry(store, now, &active_wallets)?;
        let mut top_wallets = desired_wallets.clone();
        top_wallets.truncate(5);
        let active_follow_wallets = store.list_active_follow_wallets()?.len();
        let summary = DiscoverySummary {
            window_start,
            wallets_seen,
            eligible_wallets,
            metrics_written: 0,
            follow_promoted: follow_delta.activated,
            follow_demoted: follow_delta.deactivated,
            active_follow_wallets,
            top_wallets,
            published: publish_due,
            ..DiscoverySummary::default()
        }
        .with_runtime_mode(DiscoveryRuntimeMode::Degraded)
        .with_scoring_source(scoring_source)
        .with_cap_truncation_telemetry(cap_truncation_telemetry);
        if publish_due {
            self.record_live_publish(now);
        }
        let _ = self.persist_publication_state(
            store,
            DiscoveryRuntimeMode::Degraded,
            publish_due,
            metrics_window_start,
            None,
            scoring_source,
            reason,
            now,
        )?;
        Ok(summary)
    }

    fn bootstrap_degraded_summary_from_published_universe(
        &self,
        store: &SqliteStore,
        window_start: DateTime<Utc>,
        active_wallets: HashSet<String>,
        cap_truncation_telemetry: &CapTruncationTelemetrySnapshot,
        scoring_source: &'static str,
        reason: &str,
        now: DateTime<Utc>,
    ) -> Result<DiscoverySummary> {
        let bootstrap_state = store.discovery_bootstrap_degraded_state_read_only()?;
        let mut desired_wallets: Vec<String> = active_wallets.iter().cloned().collect();
        desired_wallets.sort();
        let follow_delta =
            store.persist_discovery_cycle(&[], &[], &desired_wallets, true, true, now, reason)?;
        store.set_discovery_bootstrap_degraded_state(
            true,
            Some(reason),
            bootstrap_state.armed_at.or(Some(now)),
        )?;
        let (wallets_seen, eligible_wallets) =
            self.published_universe_telemetry(store, now, &active_wallets)?;
        let mut top_wallets = desired_wallets.clone();
        top_wallets.truncate(5);
        let active_follow_wallets = store.list_active_follow_wallets()?.len();
        Ok(DiscoverySummary {
            window_start,
            wallets_seen,
            eligible_wallets,
            metrics_written: 0,
            follow_promoted: follow_delta.activated,
            follow_demoted: follow_delta.deactivated,
            active_follow_wallets,
            top_wallets,
            published: false,
            ..DiscoverySummary::default()
        }
        .with_runtime_mode(DiscoveryRuntimeMode::BootstrapDegraded)
        .with_scoring_source(scoring_source)
        .with_cap_truncation_telemetry(cap_truncation_telemetry))
    }
}
