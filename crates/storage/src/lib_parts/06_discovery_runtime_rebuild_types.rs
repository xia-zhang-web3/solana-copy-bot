use super::*;

pub use copybot_storage_core::DiscoveryRuntimeCursor;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscoveryPersistedRebuildPhase {
    CollectBuyMints,
    ResolveTokenQuality,
    Replay,
    PublishPending,
}

impl DiscoveryPersistedRebuildPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CollectBuyMints => "collect_buy_mints",
            Self::ResolveTokenQuality => "resolve_token_quality",
            Self::Replay => "replay",
            Self::PublishPending => "publish_pending",
        }
    }

    pub fn parse(raw: &str) -> Result<Self> {
        match raw {
            "collect_buy_mints" => Ok(Self::CollectBuyMints),
            "resolve_token_quality" => Ok(Self::ResolveTokenQuality),
            "replay" => Ok(Self::Replay),
            "publish_pending" => Ok(Self::PublishPending),
            _ => Err(anyhow!("invalid discovery persisted rebuild phase: {raw}")),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DiscoveryPersistedRebuildStateRow {
    pub phase: DiscoveryPersistedRebuildPhase,
    pub window_start: DateTime<Utc>,
    pub horizon_end: DateTime<Utc>,
    pub metrics_window_start: DateTime<Utc>,
    pub phase_cursor: Option<DiscoveryRuntimeCursor>,
    pub prepass_rows_processed: usize,
    pub prepass_pages_processed: usize,
    pub replay_rows_processed: usize,
    pub replay_pages_processed: usize,
    pub chunks_completed: usize,
    pub state_json: String,
    pub started_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct DiscoveryPersistedRebuildStateMetaRow {
    pub phase: DiscoveryPersistedRebuildPhase,
    pub state_json_bytes: usize,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct DiscoveryPersistedRebuildStateMetaLiteRawRow {
    pub phase_raw: String,
    pub updated_at_raw: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqliteReadOnlyProbeFacts {
    pub page_size: usize,
    pub page_count: usize,
    pub freelist_count: usize,
    pub journal_mode: String,
    pub locking_mode: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SqliteReadOnlyDriverCompareFacts {
    pub busy_timeout_ms: u64,
    pub cache_size: i64,
    pub mmap_size: i64,
    pub query_only: bool,
    pub journal_mode: String,
    pub locking_mode: String,
}
