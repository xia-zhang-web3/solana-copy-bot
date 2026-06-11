use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use copybot_storage_core::{RiskEventRow, SqliteStore};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

use crate::execution_canary_quote_pnl_gate::TinyExecutionGateCheck;

const EVENT_SHADOW_RISK_HARD_STOP: &str = "shadow_risk_hard_stop";
const EVENT_SHADOW_RISK_HARD_STOP_CLEARED: &str = "shadow_risk_hard_stop_cleared";
const EVENT_SHADOW_RISK_PAUSE: &str = "shadow_risk_pause";
const EVENT_SHADOW_RISK_PAUSE_CLEARED: &str = "shadow_risk_pause_cleared";
const PAUSE_TYPE_EXPOSURE_SOFT_CAP: &str = "exposure_soft_cap";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TinyExecutionUpstreamState {
    pub(crate) status: String,
    pub(crate) can_open_new_tiny_entries: bool,
    pub(crate) blocker_count: u64,
    pub(crate) entry_blocker_count: u64,
    pub(crate) blockers: Vec<TinyExecutionGateCheck>,
    pub(crate) why_not_trading_now: Vec<String>,
}

impl TinyExecutionUpstreamState {
    pub(crate) fn ready() -> Self {
        Self {
            status: "ready".to_string(),
            can_open_new_tiny_entries: true,
            blocker_count: 0,
            entry_blocker_count: 0,
            blockers: Vec::new(),
            why_not_trading_now: Vec::new(),
        }
    }
}

pub(crate) fn load_tiny_execution_upstream_state(
    store: &SqliteStore,
    as_of: DateTime<Utc>,
) -> Result<TinyExecutionUpstreamState> {
    let mut blockers = Vec::new();
    if let Some(check) = active_shadow_hard_stop_check(store)? {
        blockers.push(check);
    }
    blockers.extend(active_shadow_pause_checks(store, as_of)?);
    if blockers.is_empty() {
        return Ok(TinyExecutionUpstreamState::ready());
    }
    let why_not_trading_now = blockers
        .iter()
        .map(|check| {
            format!(
                "{}={} blocks upstream entry flow: {}",
                check.name, check.value, check.reason
            )
        })
        .collect();
    Ok(TinyExecutionUpstreamState {
        status: "entries_paused".to_string(),
        can_open_new_tiny_entries: false,
        blocker_count: blockers.len() as u64,
        entry_blocker_count: blockers.len() as u64,
        blockers,
        why_not_trading_now,
    })
}

fn active_shadow_hard_stop_check(store: &SqliteStore) -> Result<Option<TinyExecutionGateCheck>> {
    let Some(stop) = store.latest_risk_event_by_type(EVENT_SHADOW_RISK_HARD_STOP)? else {
        return Ok(None);
    };
    let cleared = store.latest_risk_event_by_type(EVENT_SHADOW_RISK_HARD_STOP_CLEARED)?;
    if cleared
        .as_ref()
        .is_some_and(|cleared| cleared.rowid > stop.rowid)
    {
        return Ok(None);
    }
    let details = event_details(&stop)?;
    let stop_type = json_string(&details, "stop_type").unwrap_or("hard_stop");
    let detail = json_string(&details, "detail").unwrap_or("active");
    Ok(Some(TinyExecutionGateCheck {
        name: "upstream_shadow_risk_hard_stop".to_string(),
        status: "block".to_string(),
        value: stop_type.to_string(),
        threshold: "inactive".to_string(),
        reason: detail.to_string(),
    }))
}

fn active_shadow_pause_checks(
    store: &SqliteStore,
    as_of: DateTime<Utc>,
) -> Result<Vec<TinyExecutionGateCheck>> {
    let pause_events = store.list_risk_events_by_type_desc(EVENT_SHADOW_RISK_PAUSE)?;
    if pause_events.is_empty() {
        return Ok(Vec::new());
    }
    let cleared_events = store.list_risk_events_by_type_desc(EVENT_SHADOW_RISK_PAUSE_CLEARED)?;
    let latest_clear_by_type = latest_clear_by_pause_type(&cleared_events)?;
    let wildcard_clear_rowid = latest_wildcard_clear_rowid(&cleared_events)?;
    let mut seen_pause_types = HashSet::new();
    let mut checks = Vec::new();
    for event in pause_events {
        let details = event_details(&event)?;
        let pause_type = json_string(&details, "pause_type").unwrap_or("timed_pause");
        if !seen_pause_types.insert(pause_type.to_string()) {
            continue;
        }
        if wildcard_clear_rowid.is_some_and(|rowid| rowid > event.rowid) {
            continue;
        }
        if latest_clear_by_type
            .get(pause_type)
            .copied()
            .is_some_and(|rowid| rowid > event.rowid)
        {
            continue;
        }
        let Some(until) = json_string(&details, "until").and_then(parse_rfc3339) else {
            continue;
        };
        if pause_type != PAUSE_TYPE_EXPOSURE_SOFT_CAP && until <= as_of {
            continue;
        }
        let detail = json_string(&details, "detail")
            .map(str::to_string)
            .unwrap_or_else(|| format!("risk_event_rowid={}", event.rowid));
        checks.push(TinyExecutionGateCheck {
            name: "upstream_shadow_risk_pause".to_string(),
            status: "block".to_string(),
            value: format!("{pause_type}; until={}", until.to_rfc3339()),
            threshold: "inactive".to_string(),
            reason: detail,
        });
    }
    Ok(checks)
}

fn latest_clear_by_pause_type(events: &[RiskEventRow]) -> Result<HashMap<String, i64>> {
    let mut latest = HashMap::new();
    for event in events {
        let details = event_details(event)?;
        let Some(pause_type) = json_string(&details, "pause_type") else {
            continue;
        };
        latest.entry(pause_type.to_string()).or_insert(event.rowid);
    }
    Ok(latest)
}

fn latest_wildcard_clear_rowid(events: &[RiskEventRow]) -> Result<Option<i64>> {
    for event in events {
        let details = event_details(event)?;
        if json_string(&details, "pause_type").is_none() {
            return Ok(Some(event.rowid));
        }
    }
    Ok(None)
}

fn event_details(event: &RiskEventRow) -> Result<Value> {
    let Some(raw) = event.details_json.as_deref() else {
        return Ok(Value::Null);
    };
    serde_json::from_str(raw).with_context(|| {
        format!(
            "failed parsing risk event details_json rowid={}",
            event.rowid
        )
    })
}

fn json_string<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value.get(key).and_then(Value::as_str)
}

fn parse_rfc3339(raw: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .ok()
        .map(|value| value.with_timezone(&Utc))
}
