#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PersistedWalletMetricSnapshotRow {
    pub wallet_id: String,
    pub window_start: DateTime<Utc>,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    pub pnl: f64,
    pub win_rate: f64,
    pub trades: u32,
    pub closed_trades: u32,
    pub hold_median_seconds: i64,
    pub score: f64,
    pub buy_total: u32,
    pub tradable_ratio: f64,
    pub rug_ratio: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoveryPublicationFreshnessGate {
    pub scoring_window_days: i64,
    pub metric_snapshot_interval_seconds: u64,
    pub refresh_seconds: u64,
}

impl DiscoveryPublicationFreshnessGate {
    pub fn published_universe_max_age(self) -> Duration {
        Duration::seconds(
            self.metric_snapshot_interval_seconds
                .max(self.refresh_seconds.max(1))
                .saturating_mul(2) as i64,
        )
    }

    fn expected_metrics_window_start(self, now: DateTime<Utc>) -> DateTime<Utc> {
        let interval_seconds = self.metric_snapshot_interval_seconds.max(1) as i64;
        let bucketed_ts = now.timestamp().div_euclid(interval_seconds) * interval_seconds;
        let bucketed_now = DateTime::<Utc>::from_timestamp(bucketed_ts, 0).unwrap_or(now);
        bucketed_now - Duration::days(self.scoring_window_days.max(1))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrustedSelectionState {
    TrustedCurrent,
    Invalid,
}

impl TrustedSelectionState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TrustedCurrent => "trusted_current",
            Self::Invalid => "invalid",
        }
    }

    pub fn parse(raw: &str) -> Result<Self> {
        match raw {
            "trusted_current" => Ok(Self::TrustedCurrent),
            "trusted_bridged" | "trusted_bridged_stale" => Ok(Self::Invalid),
            "invalid" => Ok(Self::Invalid),
            _ => Err(anyhow!("invalid trusted selection state: {raw}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrustedSnapshotSourceKind {
    DiscoveryRefresh,
    Legacy,
}

impl TrustedSnapshotSourceKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DiscoveryRefresh => "discovery_refresh",
            Self::Legacy => "legacy",
        }
    }

    pub fn parse(raw: &str) -> Result<Self> {
        match raw {
            "discovery_refresh" => Ok(Self::DiscoveryRefresh),
            "clone_latest_bridge" | "admin_materialization" => Ok(Self::Legacy),
            "legacy" => Ok(Self::Legacy),
            _ => Err(anyhow!("invalid trusted snapshot source kind: {raw}")),
        }
    }
}
