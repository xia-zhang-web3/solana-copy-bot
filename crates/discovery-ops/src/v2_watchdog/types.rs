use chrono::{DateTime, Utc};
use serde::Serialize;
use std::path::PathBuf;

pub(super) const USAGE: &str = "usage:
  discovery_v2_watchdog --config <path> [--db-path <path>] [--min-active-wallets <n>] [--json] [--fail-on-warn]";

#[derive(Debug, Clone)]
pub struct WatchdogConfig {
    pub config_path: PathBuf,
    pub db_path: Option<PathBuf>,
    pub min_active_wallets: Option<usize>,
    pub json: bool,
    pub fail_on_warn: bool,
    pub now: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WatchdogState {
    Ok,
    Warn,
    Critical,
}

impl WatchdogState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Warn => "warn",
            Self::Critical => "critical",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct WatchdogFinding {
    pub severity: WatchdogState,
    pub code: String,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct WatchdogOutput {
    pub event: String,
    pub state: WatchdogState,
    pub config_path: String,
    pub db_path: String,
    pub checked_at: DateTime<Utc>,
    pub publication_runtime_mode: Option<String>,
    pub publication_reason: Option<String>,
    pub publication_last_published_at: Option<DateTime<Utc>>,
    pub publication_age_seconds: Option<i64>,
    pub publication_warn_age_seconds: i64,
    pub publication_max_age_seconds: i64,
    pub publication_fresh: bool,
    pub publication_identity_matches: bool,
    pub publication_cursor_fresh: bool,
    pub published_wallet_count: usize,
    pub active_follow_wallet_count: usize,
    pub min_active_wallets: usize,
    pub findings: Vec<WatchdogFinding>,
}
