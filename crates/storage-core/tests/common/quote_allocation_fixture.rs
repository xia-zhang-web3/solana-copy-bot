#![allow(dead_code)]
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_storage_core::{
    ExecutionCanaryQuotePnlSummary, ExecutionQuoteCanaryEventInsert, SqliteStore,
};
use tempfile::{tempdir, TempDir};

pub struct Fixture {
    pub dir: TempDir,
    pub store: SqliteStore,
    pub opened: DateTime<Utc>,
}
impl Fixture {
    pub fn new() -> Result<Self> {
        let dir = tempdir()?;
        let mut store = SqliteStore::open(dir.path().join("allocation.db"))?;
        store.run_migrations(std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../migrations"
        )))?;
        Ok(Self {
            dir,
            store,
            opened: "2026-09-05T10:00:00.123456789Z".parse()?,
        })
    }
    pub fn reopen(&mut self) -> Result<()> {
        self.store = SqliteStore::open(self.dir.path().join("allocation.db"))?;
        Ok(())
    }
    pub fn buy(&self, quantity: &str, fee: Option<u64>) -> Result<()> {
        self.store
            .record_execution_quote_canary_event(&self.event(true, 0, quantity, fee))?;
        Ok(())
    }
    pub fn exit(&self, n: i64, quantity: &str, fee: Option<u64>) -> Result<i64> {
        self.exit_with_output(n, quantity, fee, "110000000")
    }
    pub fn exit_with_output(
        &self,
        n: i64,
        quantity: &str,
        fee: Option<u64>,
        output: &str,
    ) -> Result<i64> {
        let signal = format!("sell-{n}");
        self.store.insert_shadow_closed_trade(
            &signal,
            "wallet",
            "Token",
            1.0,
            0.1,
            0.11,
            0.01,
            self.opened,
            self.opened + Duration::seconds(n),
        )?;
        let id = self
            .store
            .list_execution_quote_canary_close_candidates_for_signal(&signal, 1)?[0]
            .id;
        let mut event = self.event(false, n, quantity, fee);
        event.quote_out_amount_raw = Some(output.into());
        event.event_id = format!("quote:close:{id}");
        event.shadow_closed_trade_id = Some(id);
        self.store.record_execution_quote_canary_event(&event)?;
        Ok(id)
    }
    pub fn report(&self, since: i64, limit: u32) -> Result<ExecutionCanaryQuotePnlSummary> {
        self.store.execution_canary_quote_pnl_summary(
            self.opened + Duration::hours(1),
            self.opened + Duration::seconds(since),
            limit,
        )
    }
    pub fn event(
        &self,
        buy: bool,
        n: i64,
        quantity: &str,
        fee: Option<u64>,
    ) -> ExecutionQuoteCanaryEventInsert {
        let time = self.opened + Duration::seconds(n);
        ExecutionQuoteCanaryEventInsert {
            http_request_started_ts: None,
            quote_response_available_ts: None,
            event_id: if buy {
                "quote:entry:buy".into()
            } else {
                format!("quote:close:{n}")
            },
            signal_id: Some(if buy {
                "buy".into()
            } else {
                format!("sell-{n}")
            }),
            shadow_closed_trade_id: (!buy).then_some(n),
            wallet_id: "wallet".into(),
            token: "Token".into(),
            side: if buy { "buy" } else { "sell" }.into(),
            quote_status: "ok".into(),
            request_ts: time + Duration::milliseconds(10),
            signal_ts: Some(time),
            decision_delay_ms: Some(10),
            quote_latency_ms: Some(20),
            leader_notional_sol: Some(0.1),
            quote_in_amount_raw: Some(if buy {
                "100000000".into()
            } else {
                quantity.into()
            }),
            quote_out_amount_raw: Some(if buy {
                quantity.into()
            } else {
                "110000000".into()
            }),
            quote_response_json: None,
            quote_price_sol: None,
            shadow_price_sol: None,
            slippage_bps: Some(10.0),
            price_impact_pct: None,
            route_plan_json: None,
            priority_fee_status: Some("ok".into()),
            priority_fee_lamports: fee,
            priority_fee_json: None,
            decision_status: Some("would_execute".into()),
            decision_reason: None,
            error: None,
        }
    }
}
