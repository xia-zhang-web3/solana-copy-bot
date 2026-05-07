use anyhow::{anyhow, Result};
use copybot_config::{ExecutionConfig, RiskConfig};
use std::env;
use std::path::{Path, PathBuf};
use tracing_subscriber::EnvFilter;

use crate::constants::{APP_LOG_FILTER_ENV, LEGACY_RUST_LOG_ENV};

pub(crate) fn validate_live_execution_policy_contract(
    execution: &ExecutionConfig,
    risk: &RiskConfig,
    env: &str,
) -> Result<()> {
    if !execution.enabled {
        return Ok(());
    }
    Err(anyhow!(
        "execution.enabled=true is quarantined in copybot-app; env={} risk_execution_buy_cooldown_seconds={}",
        env,
        risk.execution_buy_cooldown_seconds
    ))
}

pub(crate) fn resolve_migrations_dir(
    config_path: &Path,
    configured_migrations_dir: &str,
) -> PathBuf {
    let configured = PathBuf::from(configured_migrations_dir);
    if configured.is_absolute() || configured.exists() {
        return configured;
    }

    if let Some(config_parent) = config_path.parent() {
        let sibling_candidate = config_parent.join(&configured);
        if sibling_candidate.exists() {
            return sibling_candidate;
        }

        if let Some(project_root) = config_parent.parent() {
            let root_candidate = project_root.join(&configured);
            if root_candidate.exists() {
                return root_candidate;
            }
        }
    }

    configured
}

pub(crate) fn init_tracing(log_level: &str, json: bool) -> Result<()> {
    if env::var_os(LEGACY_RUST_LOG_ENV).is_some() && env::var_os(APP_LOG_FILTER_ENV).is_none() {
        eprintln!(
            "ignoring RUST_LOG for copybot-app; use {APP_LOG_FILTER_ENV} or system.log_level instead"
        );
    }
    let filter = parse_app_log_env_filter(log_level)?;
    if json {
        tracing_subscriber::fmt()
            .with_target(false)
            .with_env_filter(filter)
            .json()
            .compact()
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_target(false)
            .with_env_filter(filter)
            .compact()
            .init();
    }
    Ok(())
}

pub(crate) fn parse_app_log_env_filter(default_log_level: &str) -> Result<EnvFilter> {
    let raw = match env::var(APP_LOG_FILTER_ENV) {
        Ok(value) => value,
        Err(env::VarError::NotPresent) => return Ok(EnvFilter::new(default_log_level)),
        Err(env::VarError::NotUnicode(_)) => {
            return Err(anyhow!("{APP_LOG_FILTER_ENV} must be valid UTF-8"));
        }
    };
    EnvFilter::try_new(raw.as_str())
        .map_err(|error| anyhow!("{APP_LOG_FILTER_ENV} is invalid: {}", error))
}
