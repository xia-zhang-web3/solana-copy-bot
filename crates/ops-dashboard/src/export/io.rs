use std::{fs, path::Path, time::Duration};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::{json, Value};

use super::json::parse_time;

#[derive(Debug, Clone, Serialize)]
pub struct InputReportStatus {
    pub name: String,
    pub path: String,
    pub loaded: bool,
    pub stale: bool,
    pub age_ms: Option<u64>,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub(super) struct InputReport {
    pub(super) status: InputReportStatus,
    pub(super) value: Option<Value>,
}

pub(super) fn load_report(input_dir: &Path, name: &str, max_age: Duration) -> InputReport {
    let path = input_dir.join(name);
    match load_report_value(&path, max_age) {
        Ok((value, age_ms, stale)) => InputReport {
            status: InputReportStatus {
                name: name.to_string(),
                path: path.display().to_string(),
                loaded: true,
                stale,
                age_ms: Some(age_ms),
                error: None,
            },
            value: Some(value),
        },
        Err(error) => InputReport {
            status: InputReportStatus {
                name: name.to_string(),
                path: path.display().to_string(),
                loaded: false,
                stale: true,
                age_ms: None,
                error: Some(error.to_string()),
            },
            value: None,
        },
    }
}

pub(super) fn write_snapshot(
    output_dir: &Path,
    name: &str,
    generated_at: DateTime<Utc>,
    stale: bool,
    data: Value,
) -> Result<()> {
    let path = output_dir.join(format!("{name}.json"));
    let tmp = output_dir.join(format!("{name}.json.tmp"));
    let envelope = json!({
        "source": "snapshot",
        "generated_at": generated_at.to_rfc3339(),
        "freshness_age_ms": 0,
        "stale": stale,
        "data": data
    });
    fs::write(&tmp, serde_json::to_vec_pretty(&envelope)?)
        .with_context(|| format!("write {}", tmp.display()))?;
    fs::rename(&tmp, &path)
        .with_context(|| format!("rename {} -> {}", tmp.display(), path.display()))?;
    Ok(())
}

fn load_report_value(path: &Path, max_age: Duration) -> Result<(Value, u64, bool)> {
    let metadata = fs::metadata(path)?;
    let raw = fs::read_to_string(path)?;
    let value: Value = serde_json::from_str(&raw)?;
    let generated_at = report_generated_at(&value)
        .or_else(|| metadata.modified().ok().map(DateTime::<Utc>::from))
        .unwrap_or_else(Utc::now);
    let age_ms = Utc::now()
        .signed_duration_since(generated_at)
        .to_std()
        .unwrap_or(Duration::ZERO)
        .as_millis() as u64;
    Ok((value, age_ms, age_ms > max_age.as_millis() as u64))
}

fn report_generated_at(value: &Value) -> Option<DateTime<Utc>> {
    if value.get("source").and_then(Value::as_str) == Some("discovery_v2_operational_window") {
        return None;
    }
    ["generated_at", "as_of", "now"]
        .iter()
        .find_map(|key| parse_time(value.get(*key)?.as_str()?))
        .or_else(|| parse_time(value.get("summary")?.get("as_of")?.as_str()?))
}
