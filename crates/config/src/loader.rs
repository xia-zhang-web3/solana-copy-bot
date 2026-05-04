#[path = "loader_env.rs"]
mod loader_env;
mod validation;

use anyhow::{Context, Result};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use super::env_parsing::{normalize_ingestion_queue_overflow_policy, normalize_ingestion_source};
use super::AppConfig;
use validation::validate_loaded_config;

pub fn load_from_path(path: impl AsRef<Path>) -> Result<AppConfig> {
    let path = path.as_ref();
    let mut cfg = parse_from_path(path)?;
    normalize_loaded_config(&mut cfg)?;
    validate_loaded_config(&cfg)?;
    Ok(cfg)
}

fn parse_from_path(path: &Path) -> Result<AppConfig> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read config: {}", path.display()))?;
    toml::from_str(&raw).with_context(|| format!("failed to parse TOML: {}", path.display()))
}

pub fn load_from_env_or_default(default_path: &Path) -> Result<(AppConfig, PathBuf)> {
    let configured = env::var("SOLANA_COPY_BOT_CONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|_| default_path.to_path_buf());
    let mut config = parse_from_path(&configured)?;

    loader_env::apply_env_overrides(&mut config)?;

    normalize_loaded_config(&mut config)?;
    validate_loaded_config(&config)?;

    Ok((config, configured))
}

fn normalize_loaded_config(config: &mut AppConfig) -> Result<()> {
    config.ingestion.source = normalize_ingestion_source(&config.ingestion.source)?;
    config.ingestion.queue_overflow_policy =
        normalize_ingestion_queue_overflow_policy(&config.ingestion.queue_overflow_policy)?;
    Ok(())
}
