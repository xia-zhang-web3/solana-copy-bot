#[cfg(test)]
use std::fs;
#[cfg(test)]
use std::path::{Path, PathBuf};

mod env_parsing;
mod loader;
mod schema;

pub use self::env_parsing::normalize_ingestion_source;
pub use self::loader::{load_from_env_or_default, load_from_path};
pub use self::schema::{
    AppConfig, DiscoveryConfig, ExecutionConfig, HistoryRetentionConfig, IngestionConfig,
    RecentRawJournalConfig, RiskConfig, RuntimeRestoreOpsConfig, ShadowConfig, SqliteConfig,
    SystemConfig,
};

#[cfg(test)]
mod tests;
