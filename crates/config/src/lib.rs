#[cfg(test)]
use std::fs;
#[cfg(test)]
use std::path::{Path, PathBuf};

mod discovery_v2_identity;
mod env_parsing;
mod loader;
mod risk_validation;
mod schema;

pub use self::discovery_v2_identity::{
    discovery_v2_policy_fingerprint, DiscoveryV2PolicyFingerprintInput,
    DISCOVERY_V2_SCORING_SOURCE, DISCOVERY_V2_SHADOW_FEEDBACK_CATASTROPHE_MAX_ROI,
    DISCOVERY_V2_SHADOW_FEEDBACK_CATASTROPHE_MIN_CLOSED_TRADES,
    DISCOVERY_V2_SHADOW_FEEDBACK_CATASTROPHE_MIN_ENTRY_SOL,
    DISCOVERY_V2_SHADOW_FEEDBACK_MAX_PNL_SOL, DISCOVERY_V2_SHADOW_FEEDBACK_MAX_ROI,
    DISCOVERY_V2_SHADOW_FEEDBACK_MIN_CLOSED_TRADES, DISCOVERY_V2_SHADOW_FEEDBACK_MIN_ENTRY_SOL,
    DISCOVERY_V2_SHADOW_FEEDBACK_SINGLE_LOSS_MAX_ROI,
    DISCOVERY_V2_SHADOW_FEEDBACK_SINGLE_LOSS_MIN_ENTRY_SOL,
    DISCOVERY_V2_SHADOW_FEEDBACK_WINDOW_HOURS, DISCOVERY_V2_TOKEN_QUALITY_TTL_SECONDS,
    DISCOVERY_V2_TOKEN_ROLLING_MARKET_WINDOW_SECONDS,
};
pub use self::env_parsing::normalize_ingestion_source;
pub use self::loader::{load_from_env_or_default, load_from_path};
pub use self::schema::{
    AppConfig, DiscoveryConfig, ExecutionConfig, HistoryRetentionConfig, IngestionConfig,
    RecentRawJournalConfig, RiskConfig, RuntimeRestoreOpsConfig, ShadowConfig, SqliteConfig,
    SystemConfig,
};

#[cfg(test)]
mod tests;
