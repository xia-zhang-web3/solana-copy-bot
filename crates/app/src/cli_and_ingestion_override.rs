use anyhow::{Context, Result};
use copybot_config::normalize_ingestion_source;
use std::env;
use std::fs;
use std::path::PathBuf;

use crate::constants::DEFAULT_INGESTION_OVERRIDE_PATH;

pub(crate) fn parse_config_arg() -> Option<PathBuf> {
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--config" {
            return args.next().map(PathBuf::from);
        }
        if let Some(inline) = arg.strip_prefix("--config=") {
            return Some(PathBuf::from(inline));
        }
    }
    None
}

pub(crate) fn load_ingestion_source_override() -> Result<Option<String>> {
    let override_path = env::var("SOLANA_COPY_BOT_INGESTION_OVERRIDE_FILE")
        .unwrap_or_else(|_| DEFAULT_INGESTION_OVERRIDE_PATH.to_string());
    let content = match fs::read_to_string(&override_path) {
        Ok(content) => content,
        Err(_) => return Ok(None),
    };
    parse_ingestion_source_override(&content)
        .with_context(|| format!("invalid ingestion source override in {override_path}"))
}

pub(crate) fn apply_ingestion_source_override(
    ingestion_source: &mut String,
    source_override: Option<String>,
) -> Option<String> {
    if let Some(source_override) = source_override {
        *ingestion_source = source_override.clone();
        return Some(source_override);
    }
    None
}

pub(crate) fn parse_ingestion_source_override(content: &str) -> Result<Option<String>> {
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let Some((key, value)) = trimmed.split_once('=') else {
            continue;
        };
        if key.trim() != "SOLANA_COPY_BOT_INGESTION_SOURCE" {
            continue;
        }
        let source = value
            .trim()
            .trim_matches('"')
            .trim_matches('\'')
            .to_string();
        if !source.is_empty() {
            return normalize_ingestion_source(&source).map(Some);
        }
    }
    Ok(None)
}
