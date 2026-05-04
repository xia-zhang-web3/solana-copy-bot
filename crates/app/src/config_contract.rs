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
