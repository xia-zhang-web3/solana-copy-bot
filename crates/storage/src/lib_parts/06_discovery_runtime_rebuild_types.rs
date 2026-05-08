use super::*;

pub use copybot_storage_core::{
    DiscoveryPersistedRebuildPhase, DiscoveryPersistedRebuildStateRow, DiscoveryRuntimeCursor,
};

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
