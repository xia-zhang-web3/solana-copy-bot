#[path = "loader_discovery_shadow_env.rs"]
mod loader_discovery_shadow_env;
#[path = "loader_ingestion_env.rs"]
mod loader_ingestion_env;
#[path = "loader_risk_env.rs"]
mod loader_risk_env;

use anyhow::Result;

use super::super::AppConfig;

pub(super) fn apply_env_overrides(config: &mut AppConfig) -> Result<()> {
    loader_ingestion_env::apply_ingestion_and_history_env_overrides(config)?;
    loader_discovery_shadow_env::apply_discovery_shadow_execution_env_overrides(config)?;
    loader_risk_env::apply_risk_env_overrides(config)?;
    Ok(())
}
