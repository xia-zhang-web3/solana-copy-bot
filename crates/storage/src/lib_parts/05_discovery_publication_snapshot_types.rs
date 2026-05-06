pub use copybot_storage_core::{
    DiscoveryPublicationFreshnessGate, PersistedWalletMetricSnapshotRow,
};

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
