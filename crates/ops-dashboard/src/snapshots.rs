use std::{
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;

#[derive(Clone)]
pub struct SnapshotStore {
    dir: PathBuf,
    max_age: Duration,
}

#[derive(Serialize)]
pub struct ApiEnvelope {
    pub source: String,
    pub generated_at: String,
    pub freshness_age_ms: u64,
    pub stale: bool,
    pub data: Value,
}

impl SnapshotStore {
    pub fn new(dir: PathBuf, max_age: Duration) -> Self {
        Self { dir, max_age }
    }

    pub fn load(&self, name: &str, fallback: Value) -> ApiEnvelope {
        let path = self.dir.join(format!("{name}.json"));
        match self.load_file(name, &path) {
            Ok(envelope) => envelope,
            Err(err) => fallback_envelope(name, fallback, err.to_string()),
        }
    }

    fn load_file(&self, name: &str, path: &Path) -> anyhow::Result<ApiEnvelope> {
        let metadata = fs::metadata(path)?;
        let raw = fs::read_to_string(path)?;
        let value: Value = serde_json::from_str(&raw)?;
        let generated_at = generated_at(&value)
            .or_else(|| metadata.modified().ok().map(DateTime::<Utc>::from))
            .unwrap_or_else(Utc::now);
        let age_ms = freshness_age_ms(generated_at);
        let source = value
            .get("source")
            .and_then(Value::as_str)
            .unwrap_or(name)
            .to_string();
        let stale = value.get("stale").and_then(Value::as_bool).unwrap_or(false)
            || age_ms > self.max_age.as_millis() as u64;
        let data = value.get("data").cloned().unwrap_or(value);

        Ok(ApiEnvelope {
            source,
            generated_at: generated_at.to_rfc3339(),
            freshness_age_ms: age_ms,
            stale,
            data,
        })
    }
}

fn generated_at(value: &Value) -> Option<DateTime<Utc>> {
    let raw = value.get("generated_at")?.as_str()?;
    DateTime::parse_from_rfc3339(raw)
        .ok()
        .map(|timestamp| timestamp.with_timezone(&Utc))
}

fn fallback_envelope(name: &str, data: Value, reason: String) -> ApiEnvelope {
    let now = Utc::now();
    let mut data = data;
    if let Some(object) = data.as_object_mut() {
        object.insert("snapshot_error".to_string(), Value::String(reason));
    }
    ApiEnvelope {
        source: format!("{name}_fallback"),
        generated_at: now.to_rfc3339(),
        freshness_age_ms: 0,
        stale: true,
        data,
    }
}

fn freshness_age_ms(generated_at: DateTime<Utc>) -> u64 {
    let now = Utc::now();
    now.signed_duration_since(generated_at)
        .to_std()
        .unwrap_or(Duration::ZERO)
        .as_millis() as u64
}
