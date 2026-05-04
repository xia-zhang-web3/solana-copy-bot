#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum DiscoveryRuntimeMode {
    Healthy,
    Degraded,
    BootstrapDegraded,
    #[default]
    FailClosed,
}

impl DiscoveryRuntimeMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
            Self::BootstrapDegraded => "bootstrap_degraded",
            Self::FailClosed => "fail_closed",
        }
    }

    pub fn parse(raw: &str) -> Result<Self> {
        match raw {
            "healthy" => Ok(Self::Healthy),
            "degraded" => Ok(Self::Degraded),
            "bootstrap_degraded" => Ok(Self::BootstrapDegraded),
            "fail_closed" => Ok(Self::FailClosed),
            _ => Err(anyhow!("invalid discovery runtime mode: {raw}")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoveryRuntimeCursor {
    pub ts_utc: DateTime<Utc>,
    pub slot: u64,
    pub signature: String,
}

#[derive(Debug, Clone)]
pub struct DiscoveryPublicationStateUpdate {
    pub runtime_mode: DiscoveryRuntimeMode,
    pub reason: String,
    pub last_published_at: Option<DateTime<Utc>>,
    pub last_published_window_start: Option<DateTime<Utc>>,
    pub published_scoring_source: Option<String>,
    pub published_wallet_ids: Option<Vec<String>>,
}
