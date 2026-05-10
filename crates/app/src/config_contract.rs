use anyhow::{anyhow, Result};
use copybot_config::ExecutionConfig;

pub(crate) fn contains_placeholder_value(value: &str) -> bool {
    value.to_ascii_uppercase().contains("REPLACE_ME")
}

pub(crate) fn validate_execution_runtime_contract(
    config: &ExecutionConfig,
    _env: &str,
) -> Result<()> {
    if config.enabled {
        return Err(anyhow!(
            "execution.enabled=true is not supported by copybot-app after execution runtime quarantine; keep execution.enabled=false"
        ));
    }
    Ok(())
}

pub(crate) fn validate_live_ingestion_source_contract(source: &str, env: &str) -> Result<()> {
    let env_norm = env.trim().to_ascii_lowercase();
    let prod_like = matches!(
        env_norm.as_str(),
        "prod" | "production" | "live" | "prod-live"
    ) || env_norm.starts_with("prod-");
    if prod_like && source == "mock" {
        return Err(anyhow!(
            "ingestion.source=mock is not supported in production env {}; use yellowstone_grpc",
            env
        ));
    }
    Ok(())
}
