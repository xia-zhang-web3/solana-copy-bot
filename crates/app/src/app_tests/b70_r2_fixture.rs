use super::{b64_http_fixture as http, b70_fixture::Fixture, *};
use crate::execution_quote_canary::job::HotQuoteOrigin;
use crate::shadow_scheduler::{ShadowSwapSide, ShadowTaskKey, ShadowTaskOutput};
use anyhow::{ensure, Result};
use copybot_shadow::ShadowProcessOutcome;
use copybot_storage_core::ExecutionQuoteCanaryEventInsert;

pub(super) struct Ready {
    pub f: Fixture,
    pub runner: ExecutionCanaryRunner,
    pub origin: Option<HotQuoteOrigin>,
    pub signal: copybot_shadow::ShadowSignalResult,
    pub swap: SwapEvent,
    pub risk: ShadowRiskGuard,
    pub quote: ExecutionQuoteCanaryEventInsert,
    pub samples: Vec<Vec<rusqlite::types::Value>>,
}
impl Ready {
    pub async fn new(size: f64, http_first: bool) -> Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let mut f = Fixture::new(&format!("http://{}", listener.local_addr()?), true).await?;
        f.execution.priority_fee_canary_enabled = true;
        f.execution.priority_fee_canary_rpc_url = format!("http://{}", listener.local_addr()?);
        f.execution.priority_fee_canary_timeout_ms = 2_000;
        f.execution.canary_buy_size_sol = f.execution.quote_canary_buy_size_sol;
        f.execution.canary_batch_limit = 1;
        let swap = f.buy();
        f.f.store.insert_observed_swap(&swap)?;
        let runner = ExecutionCanaryRunner::new(f.execution.clone());
        let mut scheduler = ShadowScheduler::new();
        let key = ShadowTaskKey {
            wallet: swap.wallet.clone(),
            token: swap.token_out.clone(),
        };
        scheduler.inflight_shadow_keys.insert(key.clone());
        runner.admit_hot_observed_buy_quote(&f.f.store, &swap, Utc::now(), &mut scheduler)?;
        let requests = http::pair(&listener).await?;
        let mut quality = permissive_shadow_quality();
        quality.copy_notional_sol = size;
        let shadow = ShadowService::new(quality);
        let insert =
            || shadow.process_swap_with_buy_receipt(&f.f.store, &swap, &f.f.follow, Utc::now());
        let mut recorded = if http_first { None } else { Some(insert()?) };
        for request in requests {
            let body = request.quote();
            request.reply(200, body).await?;
        }
        let fee = http::accept(&listener).await?;
        ensure!(fee.body["method"] == "qn_estimatePriorityFees");
        fee.reply(
            200,
            serde_json::json!({"jsonrpc":"2.0","result":{"recommended":0}}),
        )
        .await?;
        scheduler.hot_quotes.collect_next().await;
        ensure!(!scheduler.hot_completion_ready());
        if http_first {
            recorded = Some(insert()?);
        }
        let (outcome, receipt) = recorded.unwrap();
        let ShadowProcessOutcome::Recorded(signal) = &outcome else {
            anyhow::bail!("not recorded")
        };
        let signal = signal.clone();
        ensure!(receipt.is_some());
        let output = ShadowTaskOutput {
            signature: swap.signature.clone(),
            key: key.clone(),
            signal_id: Some(signal.signal_id.clone()),
            side: Some(ShadowSwapSide::Buy),
            buy_receipt: receipt,
            owned_sell_reject: None,
            outcome: Ok(outcome),
        };
        scheduler.hot_quotes.note_shadow_output(&output);
        scheduler.mark_task_complete(&key);
        let mut risk = ShadowRiskGuard::new(RiskConfig::default());
        let stop = OperatorEmergencyStop::from_env();
        let origin = runner.complete_hot_observed_buy_quote(
            &f.f.store,
            scheduler.hot_quotes.take_completion().unwrap(),
            &f.f.follow,
            false,
            &mut risk,
            &stop,
            true,
            Utc::now(),
            0,
            0,
        );
        ensure!(origin.is_some(), "healthy completion");
        let quote =
            f.f.store
                .load_latest_execution_quote_canary_entry_event(&signal.signal_id)?
                .unwrap();
        ensure!(quote.quote_status == "ok");
        ensure!(quote.decision_status.as_deref() == Some("owner_pending"));
        ensure!(
            crate::execution_canary_route::list_swap_blueprint_state_machine_candidates(
                &f.f.store,
                &f.execution,
                "shadow_recorded",
                swap.ts_utc
            )?
            .len()
                == 0
        );
        http::no_more(&listener).await?;
        scheduler.hot_quotes.shutdown().await;
        ensure!(scheduler.active_task_count() == 0 && scheduler.buffered_shadow_task_count() == 0);
        let samples = Self::samples(&f)?;
        Ok(Self {
            f,
            runner,
            origin,
            signal,
            swap,
            risk,
            quote,
            samples,
        })
    }
    pub fn samples(f: &Fixture) -> Result<Vec<Vec<rusqlite::types::Value>>> {
        let conn = f.f.conn()?;
        let mut stmt = conn.prepare(
            "SELECT * FROM execution_quote_canary_provider_samples ORDER BY event_id, provider",
        )?;
        let columns = stmt.column_count();
        let rows = stmt.query_map([], |r| (0..columns).map(|i| r.get(i)).collect())?;
        Ok(rows.collect::<std::result::Result<_, _>>()?)
    }
    pub fn orders(&self) -> Result<bool> {
        Ok(self.f.f.conn()?.query_row(
            "SELECT EXISTS(SELECT 1 FROM orders WHERE signal_id=?1)",
            [&self.signal.signal_id],
            |r| r.get(0),
        )?)
    }
    pub fn unchanged_quote_facts(&self) -> Result<()> {
        let mut current = self
            .f
            .f
            .store
            .load_latest_execution_quote_canary_entry_event(&self.signal.signal_id)?
            .unwrap();
        current.decision_status = self.quote.decision_status.clone();
        current.decision_reason = self.quote.decision_reason.clone();
        ensure!(current == self.quote, "provider event facts changed");
        ensure!(
            Self::samples(&self.f)? == self.samples,
            "provider samples changed"
        );
        Ok(())
    }
}
