const TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
const OBSERVED_SWAP_CURSOR_PROGRESS_OPS: i32 = 2_000;
const OBSERVED_SWAP_CURSOR_QUERY_PAGE_LIMIT: usize = 2_048;
const OBSERVED_SOL_LEG_CURSOR_QUERY_PAGE_LIMIT: usize = 2_048;
const OBSERVED_SOL_LEG_TARGET_BUY_MINT_TEMP_TABLE: &str = "temp_discovery_replay_target_buy_mints";
const OBSERVED_SOL_LEG_TARGET_BUY_MINT_TEMP_META_TABLE: &str =
    "temp_discovery_replay_target_buy_mints_meta";
const OBSERVED_WALLET_ACTIVITY_TARGET_WALLET_TEMP_TABLE: &str =
    "temp_discovery_replay_candidate_wallets";
const OBSERVED_WALLET_ACTIVITY_TARGET_WALLET_TEMP_META_TABLE: &str =
    "temp_discovery_replay_candidate_wallets_meta";
const OBSERVED_SWAPS_TAIL_CURSOR_QUERY: &str = "SELECT ts, slot, signature
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
const RECENT_RAW_JOURNAL_BULK_INSERT_PARAMS_PER_ROW: usize = 13;
const RECENT_RAW_JOURNAL_BULK_INSERT_HARD_CAP_ROWS: usize = 512;

#[derive(Debug, Clone, Copy, Default)]
pub struct ObservedSwapCursorPage {
    pub rows_seen: usize,
    pub time_budget_exhausted: bool,
}

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
struct ObservedWalletActivityWalletIdPage {
    wallet_ids: Vec<String>,
    time_budget_exhausted: bool,
}

#[derive(Debug, Clone, Default)]
struct ObservedWalletActiveDayCountPage {
    counts: HashMap<String, u32>,
    time_budget_exhausted: bool,
}

#[derive(Debug, Clone, Copy, Default)]
struct ObservedWalletActivityDaySummaryRow {
    inclusive_day_count: u32,
    has_start_day: bool,
    has_end_day: bool,
}

#[derive(Debug, Clone, Default)]
struct ObservedWalletActivityDaySummaryPage {
    rows: HashMap<String, ObservedWalletActivityDaySummaryRow>,
    time_budget_exhausted: bool,
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

struct ProgressHandlerGuard<'a> {
    conn: &'a Connection,
}

impl<'a> ProgressHandlerGuard<'a> {
    fn install(conn: &'a Connection, deadline: Instant) -> Self {
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
