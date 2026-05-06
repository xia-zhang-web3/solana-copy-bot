impl DiscoveryService {
    pub fn new(config: DiscoveryConfig, shadow_quality: ShadowConfig) -> Self {
        Self::new_with_helius(config, shadow_quality, None)
    }

    pub fn new_with_helius(
        config: DiscoveryConfig,
        shadow_quality: ShadowConfig,
        helius_http_url: Option<String>,
    ) -> Self {
        let helius_http_url = helius_http_url
            .map(|url| url.trim().to_string())
            .filter(|url| !url.is_empty() && !url.contains("REPLACE_ME"));
        Self {
            config,
            shadow_quality,
            helius_http_url,
            window_state: Arc::new(Mutex::new(DiscoveryWindowState::default())),
        }
    }

    pub fn published_universe_max_age(&self) -> Duration {
        self.publication_freshness_gate()
            .published_universe_max_age()
    }

    pub fn runtime_published_universe_max_age(&self) -> Duration {
        self.published_universe_max_age()
    }

    pub fn publication_freshness_gate(&self) -> DiscoveryPublicationFreshnessGate {
        DiscoveryPublicationFreshnessGate {
            scoring_window_days: self.runtime_scoring_window_days(),
            metric_snapshot_interval_seconds: self.runtime_metric_snapshot_interval_seconds(),
            refresh_seconds: self.config.refresh_seconds,
            expected_scoring_source: Some("discovery_v2_operational_window".to_string()),
            expected_policy_fingerprint: Some(self.discovery_v2_publication_policy_fingerprint(false)),
        }
    }

    pub fn runtime_scoring_window_days(&self) -> i64 {
        self.config.scoring_window_days as i64
    }

    pub fn runtime_metric_snapshot_interval_seconds(&self) -> u64 {
        self.config.metric_snapshot_interval_seconds
    }

    fn publication_selection_policy_fingerprint(&self) -> String {
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
            self.config.follow_top_n,
            self.config.scoring_window_days,
            self.config.decay_window_days,
            self.config.min_leader_notional_sol,
            self.config.min_trades,
            self.config.min_active_days,
            self.config.min_score,
            self.config.max_tx_per_minute,
            self.config.min_buy_count,
            self.config.min_tradable_ratio,
            self.config.require_open_positions_for_publication,
            self.config.max_rug_ratio,
            self.config.rug_lookahead_seconds,
            self.config.thin_market_min_volume_sol,
            self.config.thin_market_min_unique_traders,
        )
    }

    pub fn discovery_v2_publication_policy_fingerprint(&self, execution_enabled: bool) -> String {
        let window_minutes =
            u64::from(self.config.scoring_window_days.max(1)).saturating_mul(24 * 60);
        let max_rows = self
            .config
            .max_window_swaps_in_memory
            .max(self.config.max_fetch_swaps_per_cycle)
            .max(1);
        format!(
            concat!(
                "scoring_source={};window_minutes={};max_tail_lag_seconds={};",
                "max_rows={};time_budget_ms={};follow_top_n={};",
                "decay_window_days={};rug_lookahead_seconds={};",
                "metric_snapshot_interval_seconds={};",
                "min_leader_notional_sol={:.6};min_trades={};min_active_days={};",
                "min_score={:.6};max_tx_per_minute={};min_buy_count={};",
                "min_tradable_ratio={:.6};require_open_positions_for_publication={};",
                "max_rug_ratio={:.6};thin_market_min_volume_sol={:.6};",
                "thin_market_min_unique_traders={};quality_gates_enabled={};",
                "min_token_age_seconds={};min_holders={};min_liquidity_sol={:.6};",
                "min_volume_5m_sol={:.6};min_unique_traders_5m={};",
                "execution_enabled={};token_quality_ttl_seconds={};",
                "token_rolling_market_window_seconds={}"
            ),
            "discovery_v2_operational_window",
            window_minutes,
            self.config.refresh_seconds.max(1),
            max_rows,
            self.config.fetch_time_budget_ms.max(1),
            self.config.follow_top_n,
            self.config.decay_window_days,
            self.config.rug_lookahead_seconds,
            self.config.metric_snapshot_interval_seconds,
            self.config.min_leader_notional_sol,
            self.config.min_trades,
            self.config.min_active_days,
            self.config.min_score,
            self.config.max_tx_per_minute,
            self.config.min_buy_count,
            self.config.min_tradable_ratio,
            self.config.require_open_positions_for_publication,
            self.config.max_rug_ratio,
            self.config.thin_market_min_volume_sol,
            self.config.thin_market_min_unique_traders,
            self.shadow_quality.quality_gates_enabled,
            self.shadow_quality.min_token_age_seconds,
            self.shadow_quality.min_holders,
            self.shadow_quality.min_liquidity_sol,
            self.shadow_quality.min_volume_5m_sol,
            self.shadow_quality.min_unique_traders_5m,
            execution_enabled,
            600,
            300,
        )
    }
}
