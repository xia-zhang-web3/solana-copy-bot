#![allow(dead_code)]
#[path = "../../../storage-core/tests/common/source_write_off_fixture.rs"]
mod shared;
use super::*;
use copybot_core_types::{CopySignalRow, TokenQuantity};
use copybot_storage_core::{ExecutionSourceSellIntent, ExecutionSourceSellPromotionOutcome};
pub(super) use shared::*;

pub(super) const ROUTE: &str =
    crate::execution_canary_route::CANARY_ROUTE_METIS_SWAP_INSTRUCTIONS_DRY_RUN;
pub(super) const EVENT: &str = "quote:source-write-off";
pub(super) struct Fixture {
    pub store: SqliteStore,
    pub path: PathBuf,
    pub now: DateTime<Utc>,
    pub staged: ExecutionSourceSellIntent,
    pub signal: CopySignalRow,
}
impl Fixture {
    pub fn new(qty: u64) -> Result<Self> {
        static NEXT_DB: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let sequence = NEXT_DB.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let (store, path) = make_test_store(&format!("source-write-off-{sequence}"))?;
        let now = Utc::now() - chrono::Duration::seconds(10);
        proven_buy(&store, "buy-a", "source-a", now, TokenQuantity::new(qty, 3))?;
        let staged = staged(&store, "sell-a", now)?;
        let ExecutionSourceSellPromotionOutcome::Inserted(binding) =
            store.promote_execution_source_sell_intent(&staged.intent_id)?
        else {
            anyhow::bail!("fixture promotion refused")
        };
        let signal = store
            .load_copy_signal_by_signal_id(&binding.signal_id)?
            .unwrap();
        store.record_execution_quote_canary_event(&quote(&signal, now))?;
        Ok(Self {
            store,
            path,
            now,
            staged,
            signal,
        })
    }
    pub fn conn(&self) -> Result<Connection> {
        Ok(Connection::open(&self.path)?)
    }
    pub fn replace(&self, qty: u64) -> Result<()> {
        replace(&self.store, self.now, qty)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", self.path.display()));
        }
    }
}
pub(super) fn replace(store: &SqliteStore, now: DateTime<Utc>, qty: u64) -> Result<()> {
    store.record_execution_canary_manual_terminal_write_off(
        "mint",
        "tiny",
        "fixture_replace",
        now,
    )?;
    proven_buy(store, "buy-b", "source-a", now, TokenQuantity::new(qty, 3))?;
    Ok(())
}
pub(super) fn config(url: &str) -> ExecutionConfig {
    let mut c = ExecutionConfig::default();
    c.canary_enabled = true;
    c.canary_dry_run = true;
    c.canary_tiny_submit_enabled = true;
    c.canary_route = ROUTE.into();
    c.max_submit_attempts = 1;
    c.canary_wallet_pubkey = "execution-wallet".into();
    c.execution_signer_pubkey = "execution-wallet".into();
    c.execution_signer_keypair_path = "/tmp/not-read-by-write-off-tests.json".into();
    c.submit_adapter_http_url = url.into();
    c.quote_canary_base_url = url.into();
    c.quote_canary_timeout_ms = 3000;
    c.swap_transaction_dry_run_enabled = true;
    c.quote_canary_pump_fun_parallel_enabled = false;
    c
}
pub(super) fn quote(
    s: &CopySignalRow,
    now: DateTime<Utc>,
) -> copybot_storage_core::ExecutionQuoteCanaryEventInsert {
    copybot_storage_core::ExecutionQuoteCanaryEventInsert {
        http_request_started_ts: None,
        quote_response_available_ts: None,
        event_id: EVENT.into(),
        signal_id: Some(s.signal_id.clone()),
        shadow_closed_trade_id: None,
        wallet_id: s.wallet_id.clone(),
        token: s.token.clone(),
        side: "sell".into(),
        quote_status: "ok".into(),
        request_ts: now,
        signal_ts: Some(s.ts),
        decision_delay_ms: Some(1000),
        quote_latency_ms: Some(50),
        leader_notional_sol: Some(s.notional_sol),
        quote_in_amount_raw: Some("1".into()),
        quote_out_amount_raw: Some("1".into()),
        quote_response_json: Some("{\"loadedLongtailToken\":true}".into()),
        quote_price_sol: Some(0.001),
        shadow_price_sol: Some(0.001),
        slippage_bps: Some(50.0),
        price_impact_pct: Some(0.01),
        route_plan_json: Some("[{\"swapInfo\":{\"label\":\"Metis\"}}]".into()),
        priority_fee_status: Some("ok".into()),
        priority_fee_lamports: Some(12345),
        priority_fee_json: Some(super::priority_fee_fixture::total_json(12345)),
        decision_status: Some("would_execute".into()),
        decision_reason: Some("within_slippage_limit".into()),
        error: None,
    }
}
