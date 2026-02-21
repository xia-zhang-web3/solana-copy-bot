#[cfg(test)]
use std::fs;
#[cfg(test)]
use std::path::{Path, PathBuf};

mod env_parsing;
mod loader;
mod schema;

pub use self::loader::{load_from_env_or_default, load_from_path};
pub use self::schema::{
    AppConfig, DiscoveryConfig, ExecutionConfig, IngestionConfig, RiskConfig, ShadowConfig,
    SqliteConfig, SystemConfig,
};

pub const EXECUTION_ROUTE_TIP_LAMPORTS_MAX: u64 = 100_000_000;
pub const EXECUTION_ROUTE_COMPUTE_UNIT_LIMIT_MIN: u32 = 1;
pub const EXECUTION_ROUTE_COMPUTE_UNIT_LIMIT_MAX: u32 = 1_400_000;
pub const EXECUTION_ROUTE_COMPUTE_UNIT_PRICE_MICRO_LAMPORTS_MIN: u64 = 1;
pub const EXECUTION_ROUTE_COMPUTE_UNIT_PRICE_MICRO_LAMPORTS_MAX: u64 = 10_000_000;
pub const EXECUTION_ROUTE_DEFAULT_COMPUTE_UNIT_LIMIT_MIN: u32 = 100_000;
#[cfg(test)]
mod tests;
