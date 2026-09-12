pub(super) use super::receipt_rpc_fixture::Rpc;
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_core_types::{CopySignalRow, Lamports};
use copybot_storage_core::{ExecutionCanaryBuildPlanMetadata, SqliteStore};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

pub(super) const WALLET: &str = "ReceiptWallet";
pub(super) const TOKEN: &str = "ReceiptMint";
pub(super) const SIGNATURE: &str = "receipt-signature";
pub(super) const ROUTE: &str = "metis-swap-instructions-dry-run";

pub(super) struct Fixture {
    pub path: PathBuf,
    pub store: SqliteStore,
    pub now: DateTime<Utc>,
    pub order_id: String,
}

impl Fixture {
    pub fn new(side: &str) -> Result<Self> {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let seq = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("receipt-{}-{stamp}-{seq}.db", std::process::id()));
        let mut store = SqliteStore::open(&path)?;
        store.run_migrations(Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../migrations"
        )))?;
        let now = Utc::now();
        let order_id = add_order(&store, "receipt", side, TOKEN, now, true)?;
        let order = store.load_execution_canary_order(&order_id)?.unwrap();
        store.record_execution_canary_build_plan_metadata(&ExecutionCanaryBuildPlanMetadata {
            http_request_started_ts: None,
            quote_response_available_ts: None,
            order_id: order_id.clone(),
            signal_id: order.signal_id,
            client_order_id: order.client_order_id,
            recorded_ts: now,
            quote_source: None,
            quote_event_id: None,
            quote_request_ts: None,
            quote_status: Some("ok".into()),
            quote_in_amount_raw: Some("10000".into()),
            quote_out_amount_raw: Some("10000".into()),
            quote_response_json: Some(r#"{"outDecimals":3}"#.into()),
            quote_price_sol: Some(0.1),
            price_impact_pct: None,
            route_plan_json: None,
            priority_fee_source: None,
            priority_fee_status: None,
            priority_fee_lamports: None,
            priority_fee_json: None,
            slippage_bps: None,
            decision_status: None,
            decision_reason: None,
        })?;
        if side == "sell" {
            store.record_execution_canary_open_position(
                "owned",
                TOKEN,
                10.0,
                Some(copybot_core_types::TokenQuantity::new(10_000, 3)),
                0.8,
                now,
            )?;
            // Prepared exact inventory with a proven initial zero for receipt regressions.
            // Historical/import NULL is tested separately; the generic writer retains NULL.
            rusqlite::Connection::open(&path)?.execute(
                "UPDATE positions SET pnl_lamports=0 WHERE position_id='exec-canary-pos:owned'",
                [],
            )?;
        }
        Ok(Self {
            path,
            store,
            now,
            order_id,
        })
    }

    pub fn conn(&self) -> Result<rusqlite::Connection> {
        Ok(rusqlite::Connection::open(&self.path)?)
    }
    pub fn fills(&self) -> Result<u64> {
        Ok(self.conn()?.query_row(
            "SELECT count(*) FROM fills WHERE order_id = ?1",
            [&self.order_id],
            |r| r.get(0),
        )?)
    }
    pub fn reopen(&mut self) -> Result<()> {
        drop(std::mem::replace(
            &mut self.store,
            SqliteStore::open(Path::new(":memory:"))?,
        ));
        self.store = SqliteStore::open(&self.path)?;
        Ok(())
    }
    pub async fn reconcile(
        &self,
        rpc: &Rpc,
        seconds: i64,
    ) -> Result<crate::execution_submit_adapter::ExecutionTinySubmitConfirmPathOutcome> {
        self.reconcile_with_timeout(
            rpc,
            seconds,
            super::receipt_rpc_fixture::RECEIPT_TEST_BUDGET_MS,
        )
        .await
    }

    pub async fn reconcile_with_timeout(
        &self,
        rpc: &Rpc,
        seconds: i64,
        timeout_ms: u64,
    ) -> Result<crate::execution_submit_adapter::ExecutionTinySubmitConfirmPathOutcome> {
        anyhow::ensure!(
            timeout_ms > 0 && timeout_ms <= 2000,
            "invalid receipt test budget"
        );
        let started = std::time::Instant::now();
        let result = crate::execution_submit_adapter::reconcile_execution_tiny_submit_confirmation(
            &self.store,
            &config(&rpc.url),
            &self.order_id,
            &reqwest::Client::new(),
            &rpc.url,
            self.now + chrono::Duration::seconds(seconds),
            timeout_ms,
        )
        .await;
        let side = self
            .store
            .load_execution_canary_order(&self.order_id)
            .and_then(|o| {
                o.map(|o| self.store.load_copy_signal_by_signal_id(&o.signal_id))
                    .transpose()
            })
            .map(|signal| signal.flatten().map(|signal| signal.side));
        eprintln!("receipt reconcile side={side:?} seconds={seconds} budget_ms={timeout_ms} elapsed={:?} outcome={result:?} {}", started.elapsed(), rpc.diagnostics());
        result
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
        let _ = std::fs::remove_file(format!("{}-wal", self.path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", self.path.display()));
    }
}

pub(super) fn config(url: &str) -> ExecutionConfig {
    let mut config = ExecutionConfig::default();
    config.canary_wallet_pubkey = WALLET.into();
    config.canary_buy_size_sol = 1.0;
    config.canary_route = ROUTE.into();
    config.submit_adapter_http_url = url.into();
    config.canary_entry_submit_enabled = true;
    config.canary_max_open_positions = 20;
    config.canary_max_daily_loss_sol = 10.0;
    config.canary_kill_switch_path = "/nonexistent/receipt-test-kill-switch".into();
    config
}

pub(super) fn add_order(
    store: &SqliteStore,
    name: &str,
    side: &str,
    token: &str,
    now: DateTime<Utc>,
    submitted: bool,
) -> Result<String> {
    store.insert_copy_signal(&CopySignalRow {
        signal_id: name.into(),
        wallet_id: "leader".into(),
        side: side.into(),
        token: token.into(),
        notional_sol: 1.0,
        notional_lamports: Some(Lamports::new(1_000_000_000)),
        notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.into(),
        ts: now,
        status: "shadow_recorded".into(),
    })?;
    let order = store
        .reserve_execution_canary_order(name, ROUTE, now)?
        .order;
    store.mark_execution_canary_built(&order.order_id, now)?;
    store.mark_execution_canary_simulated(
        &order.order_id,
        now,
        copybot_storage_core::EXECUTION_SIMULATION_STATUS_PASSED,
        None,
    )?;
    if submitted {
        store.mark_execution_canary_submitted(&order.order_id, now, SIGNATURE)?;
    }
    Ok(order.order_id)
}

pub(super) fn receipt(side: &str, net_lamports: i64) -> Value {
    let balance = |raw: &str| {
        json!({"accountIndex":1, "owner":WALLET, "mint":TOKEN,
        "uiTokenAmount":{"amount":raw,"decimals":3}})
    };
    json!({"jsonrpc":"2.0", "result":{
        "slot":42, "blockTime":1_780_000_000,
        "transaction":{"signatures":[SIGNATURE],"message":{"accountKeys":[
            {"pubkey":WALLET,"signer":true,"writable":true},
            {"pubkey":"token-account","signer":false,"writable":true}]}},
        "meta":{"err":null,"preBalances":[2_000_000_000_i64,2_039_280],
            "postBalances":[2_000_000_000_i64 + net_lamports,2_039_280],
            "preTokenBalances": if side == "buy" {vec![balance("0")]} else {vec![balance("10000")]},
            "postTokenBalances": if side == "buy" {vec![balance("7000")]} else {vec![balance("3000")]}}
    }})
}
