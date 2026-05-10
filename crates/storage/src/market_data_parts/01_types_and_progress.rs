use super::*;

pub(super) const TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
pub(super) const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
pub(super) const OBSERVED_SWAP_CURSOR_PROGRESS_OPS: i32 = 2_000;
pub(super) const OBSERVED_SWAP_CURSOR_QUERY_PAGE_LIMIT: usize = 2_048;
pub(super) const OBSERVED_SOL_LEG_CURSOR_QUERY_PAGE_LIMIT: usize = 2_048;
pub(super) const OBSERVED_SOL_LEG_TARGET_BUY_MINT_TEMP_TABLE: &str =
    "temp_discovery_replay_target_buy_mints";
pub(super) const OBSERVED_SOL_LEG_TARGET_BUY_MINT_TEMP_META_TABLE: &str =
    "temp_discovery_replay_target_buy_mints_meta";
pub(super) const OBSERVED_WALLET_ACTIVITY_TARGET_WALLET_TEMP_TABLE: &str =
    "temp_discovery_replay_candidate_wallets";
pub(super) const OBSERVED_WALLET_ACTIVITY_TARGET_WALLET_TEMP_META_TABLE: &str =
    "temp_discovery_replay_candidate_wallets_meta";
pub(super) const OBSERVED_SWAPS_TAIL_CURSOR_QUERY: &str = "SELECT ts, slot, signature
             FROM observed_swaps INDEXED BY idx_observed_swaps_ts_slot_signature
             ORDER BY ts DESC, slot DESC, signature DESC
             LIMIT 1";
pub(crate) const OBSERVED_SWAPS_AFTER_CURSOR_PAGE_QUERY: &str =
    "SELECT signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts,
                        qty_in_raw, qty_in_decimals, qty_out_raw, qty_out_decimals
                 FROM observed_swaps INDEXED BY idx_observed_swaps_ts_slot_signature
                 WHERE (ts, slot, signature) > (?1, ?2, ?3)
                 ORDER BY ts ASC, slot ASC, signature ASC
                 LIMIT ?4";
pub(super) const RECENT_RAW_JOURNAL_BULK_INSERT_PARAMS_PER_ROW: usize = 13;
pub(super) const RECENT_RAW_JOURNAL_BULK_INSERT_HARD_CAP_ROWS: usize = 512;

pub use copybot_storage_core::ObservedSwapCursorPage;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObservedSolLegCursorAccessPath {
    TsCursorFallback,
    SolLegPartialIndex,
}

impl ObservedSolLegCursorAccessPath {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TsCursorFallback => "ts_cursor_fallback",
            Self::SolLegPartialIndex => "sol_leg_partial_index",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObservedSolLegCursorPage {
    pub rows_seen: usize,
    pub time_budget_exhausted: bool,
    pub access_path: ObservedSolLegCursorAccessPath,
}

#[derive(Debug, Clone, Default)]
pub struct ObservedBuyMintPage {
    pub mints: Vec<String>,
    pub time_budget_exhausted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedWalletActivityRow {
    pub wallet_id: String,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    pub trades: usize,
    pub active_day_count: u32,
    pub suspicious: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObservedWalletActivityDayCountSource {
    WalletActivityDays,
    ObservedSwapsFallback,
}

impl ObservedWalletActivityDayCountSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::WalletActivityDays => "wallet_activity_days",
            Self::ObservedSwapsFallback => "observed_swaps_fallback",
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ObservedWalletActivityPage {
    pub rows: Vec<ObservedWalletActivityRow>,
    pub rows_seen: usize,
    pub time_budget_exhausted: bool,
    pub active_day_count_source: Option<ObservedWalletActivityDayCountSource>,
    pub wallet_id_query_exhausted_before_first_page: bool,
    pub wallet_id_page_wallets_seen: usize,
    pub wallet_id_page_cursor_after: Option<String>,
    pub wallet_id_page_wallet_ids: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub(super) struct ObservedWalletActivityWalletIdPage {
    pub(super) wallet_ids: Vec<String>,
    pub(super) time_budget_exhausted: bool,
}

#[derive(Debug, Clone, Default)]
pub(super) struct ObservedWalletActiveDayCountPage {
    pub(super) counts: HashMap<String, u32>,
    pub(super) time_budget_exhausted: bool,
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct ObservedWalletActivityDaySummaryRow {
    pub(super) inclusive_day_count: u32,
    pub(super) has_start_day: bool,
    pub(super) has_end_day: bool,
}

#[derive(Debug, Clone, Default)]
pub(super) struct ObservedWalletActivityDaySummaryPage {
    pub(super) rows: HashMap<String, ObservedWalletActivityDaySummaryRow>,
    pub(super) time_budget_exhausted: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ObservedBuyMintCountRow {
    pub mint: String,
    pub buy_count: usize,
}

#[derive(Debug, Clone, Default)]
pub struct ObservedBuyMintCountPage {
    pub rows: Vec<ObservedBuyMintCountRow>,
    pub time_budget_exhausted: bool,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ObservedBuyMintCount {
    pub count: usize,
    pub time_budget_exhausted: bool,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ObservedBuyMintOccurrenceCount {
    pub buy_count: usize,
    pub time_budget_exhausted: bool,
}

pub(super) struct ProgressHandlerGuard<'a> {
    pub(super) conn: &'a Connection,
}

impl<'a> ProgressHandlerGuard<'a> {
    pub(super) fn install(conn: &'a Connection, deadline: Instant) -> Self {
        conn.progress_handler(
            OBSERVED_SWAP_CURSOR_PROGRESS_OPS,
            Some(move || Instant::now() >= deadline),
        );
        Self { conn }
    }
}

impl Drop for ProgressHandlerGuard<'_> {
    fn drop(&mut self) {
        self.conn.progress_handler(0, None::<fn() -> bool>);
    }
}

pub(super) fn sqlite_interrupted_after_deadline(
    error: &rusqlite::Error,
    deadline: Instant,
) -> bool {
    error.sqlite_error_code() == Some(ErrorCode::OperationInterrupted) && Instant::now() >= deadline
}

pub(super) fn anyhow_error_is_sqlite_interrupted(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<rusqlite::Error>()
            .is_some_and(|error| error.sqlite_error_code() == Some(ErrorCode::OperationInterrupted))
            || cause
                .to_string()
                .to_ascii_lowercase()
                .contains("interrupted")
    })
}
