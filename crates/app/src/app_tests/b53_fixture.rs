use super::{
    b53_http::Rpc,
    priority_fee_route_fixture::{Fixture as Backend, Route, TOKEN},
};
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_core_types::{CopySignalRow, Lamports};
use copybot_storage_core::{ExecutionCanaryOrder, SqliteStore};
use serde_json::{json, Value};

pub(super) struct Fixture {
    pub store: SqliteStore,
    pub config: ExecutionConfig,
    pub now: DateTime<Utc>,
    pub backend: Backend,
    pub rpc: Rpc,
    path: std::path::PathBuf,
}
impl Fixture {
    pub async fn new() -> Result<Self> {
        let backend = Backend::new(Route::Direct, 10_000, 1_400_000).await?;
        // The canonical route event encodes 22001 priority lamports after CU rounding.
        // Keep the mock full fee coherent and within variant A's transaction cap.
        backend.funding.lock().unwrap().fee = Some(100_000);
        let rpc = Rpc::new(
            backend.config.submit_adapter_http_url.clone(),
            backend.config.canary_wallet_pubkey.clone(),
        )
        .await?;
        let mut config = backend.config.clone();
        config.quote_canary_base_url = rpc.url.clone();
        config.submit_adapter_http_url = rpc.url.clone();
        config.canary_max_open_positions = 10;
        config.canary_max_daily_loss_sol = 1.0;
        config.canary_batch_limit = 10;
        config.quote_canary_enabled = false;
        config.max_confirm_seconds = 30;
        config = super::b126_config_fixture::activated(&config)?;
        let (store, path) = super::make_test_store("b50-actual-runtime")?;
        Ok(Self {
            now: backend.now,
            store,
            config,
            backend,
            rpc,
            path,
        })
    }
    pub fn seed(&self, id: &str, seconds: i64) -> Result<()> {
        let now = self.now + chrono::Duration::seconds(seconds);
        let signal = CopySignalRow {
            signal_id: id.into(),
            wallet_id: "leader".into(),
            token: TOKEN.into(),
            side: "buy".into(),
            notional_sol: 0.01,
            notional_lamports: Some(Lamports::new(10_000_000)),
            notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.into(),
            ts: now,
            status: "shadow_recorded".into(),
        };
        self.store.insert_copy_signal(&signal)?;
        super::execution_state_machine_tiny_submit_route::record_tiny_route_quote(
            &self.store,
            &signal,
            now,
        )?;
        let m = &self.backend.request.metadata;
        self.conn()?.execute("UPDATE execution_quote_canary_events SET quote_price_sol=?1,quote_response_json=?2,quote_in_amount_raw=?3,quote_out_amount_raw=?4,route_plan_json=?5 WHERE signal_id=?6",
            rusqlite::params![m.quote_price_sol,m.quote_response_json,m.quote_in_amount_raw,m.quote_out_amount_raw,m.route_plan_json,id])?;
        Ok(())
    }
    pub fn conn(&self) -> Result<rusqlite::Connection> {
        Ok(rusqlite::Connection::open(&self.path)?)
    }
    pub fn reopen(&mut self) -> Result<()> {
        drop(std::mem::replace(
            &mut self.store,
            SqliteStore::open(":memory:")?,
        ));
        self.store = SqliteStore::open(&self.path)?;
        Ok(())
    }
    pub fn order(&self, id: &str) -> Result<ExecutionCanaryOrder> {
        Ok(self
            .store
            .load_execution_canary_order_by_signal(id)?
            .unwrap())
    }
    pub fn sends(&self) -> Vec<Value> {
        self.rpc.state.lock().unwrap().sends.clone()
    }
    pub async fn tick(
        &self,
        seconds: i64,
    ) -> Result<crate::execution_canary::ExecutionCanaryTickSummary> {
        let now = self.now + chrono::Duration::seconds(seconds);
        super::entry_risk_clock_fixture::at(
            now + chrono::Duration::seconds(1),
            crate::execution_canary::ExecutionCanaryRunner::new(self.config.clone())
                .process_tick(&self.store, now),
        )
        .await
    }
    pub fn emit(&self, stage: &str, id: &str) -> Result<Value> {
        let order = self.order(id)?;
        let sends = self.sends();
        let first = &sends[0];
        let identity = super::b53_storage_trace::locations(
            &self.conn()?,
            first["signature"].as_str().unwrap(),
        )?;
        let hash = super::b53_storage_trace::locations(
            &self.conn()?,
            first["transaction_sha256"].as_str().unwrap(),
        )?;
        let selected = self
            .store
            .list_reconcilable_execution_canary_orders_for_retry_reasons(
                &self.config.canary_route,
                "retry_after_unknown_submit_timeout",
                Some("retry_after_rpc_submit_not_sent"),
                100,
                None,
            )?;
        let safety = crate::execution_canary_safety::pre_submit_safety_snapshot(
            &self.config,
            &self.store,
            self.now,
        )?;
        let value = json!({"stage":stage,"signal":id,"status":order.status,"signature":order.tx_signature,"attempt":order.attempt,
            "err_code":order.err_code,"simulation_error":order.simulation_error,
            "selected":selected.iter().any(|o| o.order_id == order.order_id),"accounting_pending":self.store.execution_canary_accounting_pending()?,
            "buy_blocker":safety.blocked_reason,"fills":self.conn()?.query_row("SELECT COUNT(*) FROM fills", [], |r| r.get::<_,i64>(0))?,
            "failed_expenses":self.conn()?.query_row("SELECT COUNT(*) FROM execution_failed_expense_ledger", [], |r| r.get::<_,i64>(0))?,
            "rpc_balance":self.backend.funding.lock().unwrap().balance,"sends":sends,
            "first_signature_locations":identity,"first_transaction_hash_locations":hash});
        println!("B53_OBSERVATION {value}");
        Ok(value)
    }
    pub async fn finish(&mut self) -> Result<()> {
        self.rpc.finish().await?;
        self.backend.finish().await
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}
