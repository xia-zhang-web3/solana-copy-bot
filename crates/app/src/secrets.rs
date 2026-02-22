use anyhow::{anyhow, Context, Result};
use copybot_config::ExecutionConfig;
use std::fs;
use std::path::{Path, PathBuf};

pub(crate) fn resolve_execution_adapter_secrets(
    config: &mut ExecutionConfig,
    loaded_config_path: &Path,
) -> Result<()> {
    if !config.enabled || config.mode.trim().to_ascii_lowercase() != "adapter_submit_confirm" {
        return Ok(());
    }

    let auth_token_file = config.submit_adapter_auth_token_file.trim();
    if !auth_token_file.is_empty() {
        if !config.submit_adapter_auth_token.trim().is_empty() {
            return Err(anyhow!(
                "execution.submit_adapter_auth_token and execution.submit_adapter_auth_token_file cannot be set at the same time"
            ));
        }
        let resolved = resolve_secret_file_path(auth_token_file, loaded_config_path);
        config.submit_adapter_auth_token =
            read_trimmed_secret_file(resolved.as_path()).with_context(|| {
                format!(
                    "failed loading execution.submit_adapter_auth_token_file from {} (resolved path: {})",
                    auth_token_file,
                    resolved.display()
                )
            })?;
    }

    let hmac_secret_file = config.submit_adapter_hmac_secret_file.trim();
    if !hmac_secret_file.is_empty() {
        if !config.submit_adapter_hmac_secret.trim().is_empty() {
            return Err(anyhow!(
                "execution.submit_adapter_hmac_secret and execution.submit_adapter_hmac_secret_file cannot be set at the same time"
            ));
        }
        let resolved = resolve_secret_file_path(hmac_secret_file, loaded_config_path);
        config.submit_adapter_hmac_secret = read_trimmed_secret_file(resolved.as_path())
            .with_context(|| {
                format!(
                    "failed loading execution.submit_adapter_hmac_secret_file from {} (resolved path: {})",
                    hmac_secret_file,
                    resolved.display()
                )
            })?;
    }

    let dynamic_cu_price_api_auth_token_file =
        config.submit_dynamic_cu_price_api_auth_token_file.trim();
    if !dynamic_cu_price_api_auth_token_file.is_empty() {
        if !config
            .submit_dynamic_cu_price_api_auth_token
            .trim()
            .is_empty()
        {
            return Err(anyhow!(
                "execution.submit_dynamic_cu_price_api_auth_token and execution.submit_dynamic_cu_price_api_auth_token_file cannot be set at the same time"
            ));
        }
        let resolved =
            resolve_secret_file_path(dynamic_cu_price_api_auth_token_file, loaded_config_path);
        config.submit_dynamic_cu_price_api_auth_token =
            read_trimmed_secret_file(resolved.as_path()).with_context(|| {
                format!(
                    "failed loading execution.submit_dynamic_cu_price_api_auth_token_file from {} (resolved path: {})",
                    dynamic_cu_price_api_auth_token_file,
                    resolved.display()
                )
            })?;
    }

    Ok(())
}

fn resolve_secret_file_path(path: &str, loaded_config_path: &Path) -> PathBuf {
    let value = Path::new(path.trim());
    if value.is_absolute() {
        return value.to_path_buf();
    }
    match loaded_config_path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.join(value),
        _ => value.to_path_buf(),
    }
}

fn read_trimmed_secret_file(path: &Path) -> Result<String> {
    let value = fs::read_to_string(path)
        .with_context(|| format!("failed reading secret file {}", path.display()))?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("secret file {} is empty", path.display()));
    }
    Ok(trimmed.to_string())
}
