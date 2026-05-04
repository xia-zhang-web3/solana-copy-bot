use anyhow::{anyhow, Result};
use std::collections::HashSet;
use std::fmt;
use std::str::FromStr;

pub(crate) fn parse_bool_value(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

pub(crate) fn parse_env_bool(env_name: &str) -> Result<Option<bool>> {
    let Some(raw) = std::env::var(env_name).ok() else {
        return Ok(None);
    };
    let trimmed = raw.trim();
    parse_bool_value(trimmed).map(Some).ok_or_else(|| {
        anyhow!("{env_name} must be a valid bool (1/0/true/false/yes/no/on/off), got {trimmed:?}")
    })
}

pub(crate) fn parse_env_number<T>(env_name: &str, type_name: &str) -> Result<Option<T>>
where
    T: FromStr,
    T::Err: fmt::Display,
{
    let Some(raw) = std::env::var(env_name).ok() else {
        return Ok(None);
    };
    let trimmed = raw.trim();
    trimmed.parse::<T>().map(Some).map_err(|error| {
        anyhow!("{env_name} must be a valid {type_name}, got {trimmed:?}: {error}")
    })
}

pub(crate) fn normalize_ingestion_queue_overflow_policy(value: &str) -> Result<String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "block" => Ok(String::from("block")),
        "drop_oldest" | "drop-oldest" => Ok(String::from("drop_oldest")),
        other => Err(anyhow!(
            "ingestion.queue_overflow_policy must be one of: block, drop_oldest; got {other:?}"
        )),
    }
}

pub fn normalize_ingestion_source(value: &str) -> Result<String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "mock" => Ok(String::from("mock")),
        "yellowstone" | "yellowstone_grpc" => Ok(String::from("yellowstone_grpc")),
        other => Err(anyhow!(
            "ingestion.source must be one of: mock, yellowstone_grpc; got {other:?}"
        )),
    }
}

pub(crate) fn parse_env_string_list(csv: &str, env_name: &str) -> Result<Vec<String>> {
    let mut values = Vec::new();
    let mut seen = HashSet::new();
    for token in csv.trim().trim_matches('"').trim_matches('\'').split(',') {
        let value = token.trim().trim_matches('"').trim_matches('\'');
        if value.is_empty() {
            continue;
        }
        let normalized = value.to_string();
        if !seen.insert(normalized.clone()) {
            return Err(anyhow!(
                "{env_name} contains duplicate value after normalization: {}",
                normalized
            ));
        }
        values.push(normalized);
    }
    if values.is_empty() {
        return Err(anyhow!(
            "{env_name} must contain at least one non-empty value"
        ));
    }
    Ok(values)
}
