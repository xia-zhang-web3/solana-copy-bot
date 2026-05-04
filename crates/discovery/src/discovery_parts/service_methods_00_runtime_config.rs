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
}
