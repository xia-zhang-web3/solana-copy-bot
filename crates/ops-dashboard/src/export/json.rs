use chrono::{DateTime, Utc};
use serde_json::Value;

pub(super) fn parse_time(raw: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .ok()
        .map(|timestamp| timestamp.with_timezone(&Utc))
}

pub(super) fn str_path<'a>(value: &'a Option<Value>, path: &[&str]) -> Option<&'a str> {
    path.iter()
        .try_fold(value.as_ref()?, |node, key| node.get(*key))?
        .as_str()
}

pub(super) fn bool_path(value: Option<&Value>, path: &[&str]) -> Option<bool> {
    path.iter()
        .try_fold(value?, |node, key| node.get(*key))?
        .as_bool()
}

pub(super) fn u64_path(value: &Option<Value>, path: &[&str]) -> Option<u64> {
    u64_path_value(value.as_ref()?, path)
}

pub(super) fn u64_path_ref(value: Option<&Value>, path: &[&str]) -> Option<u64> {
    u64_path_value(value?, path)
}

pub(super) fn f64_path(value: Option<&Value>, path: &[&str]) -> Option<f64> {
    path.iter()
        .try_fold(value?, |node, key| node.get(*key))?
        .as_f64()
}

pub(super) fn array_len(value: &Option<Value>, path: &[&str]) -> Option<u64> {
    let node = path
        .iter()
        .try_fold(value.as_ref()?, |node, key| node.get(*key))?;
    Some(node.as_array()?.len() as u64)
}

pub(super) fn display_u64(value: Option<u64>) -> Value {
    value
        .map(Value::from)
        .unwrap_or_else(|| Value::String("unknown".to_string()))
}

pub(super) fn row(label: impl ToString, value: Option<u64>, detail: &str) -> Vec<String> {
    vec![
        label.to_string(),
        value
            .map(|n| n.to_string())
            .unwrap_or_else(|| "unknown".to_string()),
        detail.to_string(),
        if value.is_some() { "safe" } else { "warning" }.to_string(),
    ]
}

pub(super) fn row_float(label: &str, value: Option<f64>, detail: &str) -> Vec<String> {
    vec![
        label.to_string(),
        value
            .map(|n| format!("{n:.6} SOL"))
            .unwrap_or_else(|| "unknown".to_string()),
        detail.to_string(),
        if value.unwrap_or(0.0) >= 0.0 {
            "safe"
        } else {
            "warning"
        }
        .to_string(),
    ]
}

pub(super) fn row_bytes(label: &str, value: Option<u64>, detail: &str) -> Vec<String> {
    vec![
        label.to_string(),
        value
            .map(format_bytes)
            .unwrap_or_else(|| "unknown".to_string()),
        detail.to_string(),
        if value.is_some() { "safe" } else { "warning" }.to_string(),
    ]
}

pub(super) fn storage_level(level: &str) -> &str {
    match level {
        "critical" => "dangerous",
        "large" | "unproven" | "unknown" => "warning",
        _ => "safe",
    }
}

fn u64_path_value(value: &Value, path: &[&str]) -> Option<u64> {
    path.iter()
        .try_fold(value, |node, key| node.get(*key))?
        .as_u64()
}

fn format_bytes(bytes: u64) -> String {
    const GIB: f64 = 1024.0 * 1024.0 * 1024.0;
    const MIB: f64 = 1024.0 * 1024.0;
    if bytes >= 1024 * 1024 * 1024 {
        format!("{:.1} GiB", bytes as f64 / GIB)
    } else {
        format!("{:.1} MiB", bytes as f64 / MIB)
    }
}
