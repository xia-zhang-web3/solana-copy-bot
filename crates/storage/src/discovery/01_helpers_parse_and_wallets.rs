use anyhow::{Context, Result};
use chrono::{DateTime, NaiveDateTime, Utc};

pub(crate) fn canonical_wallet_metrics_window_start(window_start: DateTime<Utc>) -> String {
    window_start.to_rfc3339()
}

pub(crate) fn wallet_metrics_window_start_query_variants(
    window_start: DateTime<Utc>,
) -> (String, String) {
    let canonical = canonical_wallet_metrics_window_start(window_start);
    let legacy_z = canonical
        .strip_suffix("+00:00")
        .map(|prefix| format!("{prefix}Z"))
        .unwrap_or_else(|| canonical.clone());
    (canonical, legacy_z)
}

pub(crate) fn parse_rfc3339_utc(raw: &str, field_name: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .or_else(|_| {
            NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S")
                .map(|naive| DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc))
        })
        .with_context(|| format!("invalid {field_name} timestamp value: {raw}"))
}

pub(crate) fn parse_optional_rfc3339_utc(
    raw: Option<String>,
    field_name: &str,
) -> Result<Option<DateTime<Utc>>> {
    raw.map(|raw| parse_rfc3339_utc(&raw, field_name))
        .transpose()
}

pub(crate) fn canonicalize_wallet_ids(wallet_ids: &[String]) -> Vec<String> {
    let mut canonical = wallet_ids.to_vec();
    canonical.sort();
    canonical.dedup();
    canonical
}

pub(crate) fn parse_optional_wallet_ids_json(
    raw: Option<String>,
    field_name: &str,
) -> Result<Option<Vec<String>>> {
    raw.map(|raw| {
        let wallet_ids = serde_json::from_str::<Vec<String>>(&raw)
            .with_context(|| format!("invalid {field_name} JSON payload: {raw}"))?;
        Ok(canonicalize_wallet_ids(&wallet_ids))
    })
    .transpose()
}

pub(crate) fn parse_wallet_ids_json(raw: String, field_name: &str) -> Result<Vec<String>> {
    serde_json::from_str::<Vec<String>>(&raw)
        .with_context(|| format!("invalid {field_name} JSON payload: {raw}"))
}
