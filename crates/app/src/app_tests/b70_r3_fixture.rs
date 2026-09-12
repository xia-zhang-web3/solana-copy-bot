use super::{b64_http_fixture as http, b70_fixture::Fixture, *};
use crate::execution_quote_canary::ExecutionQuoteCanaryRunner;
use crate::shadow_scheduler::{
    hot_quotes::Completion, ShadowSwapSide, ShadowTaskKey, ShadowTaskOutput,
};
use anyhow::{ensure, Result};
use copybot_shadow::{ShadowProcessOutcome, ShadowSignalResult};
use tokio::net::TcpListener;

pub(super) struct Boundary {
    pub f: Fixture,
    pub runner: ExecutionCanaryRunner,
    pub listener: TcpListener,
}
impl Boundary {
    pub async fn new() -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let mut f = Fixture::new(&url, true).await?;
        f.execution.priority_fee_canary_enabled = true;
        f.execution.priority_fee_canary_rpc_url = url;
        f.execution.priority_fee_canary_timeout_ms = 2_000;
        f.execution.priority_fee_canary_cache_ttl_ms = 60_000;
        f.execution.canary_buy_size_sol = f.execution.quote_canary_buy_size_sol;
        f.execution.canary_batch_limit = 1;
        let runner = ExecutionCanaryRunner::new(f.execution.clone());
        Ok(Self {
            f,
            runner,
            listener,
        })
    }
    pub async fn respond(&self, fresh: bool, fee: bool) -> Result<()> {
        if fresh {
            for request in http::pair(&self.listener).await? {
                let body = request.quote();
                request.reply(200, body).await?;
            }
        }
        if fee {
            let request = http::accept(&self.listener).await?;
            ensure!(request.body["method"] == "qn_estimatePriorityFees");
            request
                .reply(200, serde_json::json!({"result":{"recommended":0}}))
                .await?;
        }
        http::no_more(&self.listener).await
    }
    pub fn shadow(&self, swap: &SwapEvent) -> Result<ShadowTaskOutput> {
        let mut quality = permissive_shadow_quality();
        quality.copy_notional_sol = 0.067;
        let (outcome, receipt) = ShadowService::new(quality).process_swap_with_buy_receipt(
            &self.f.f.store,
            swap,
            &self.f.f.follow,
            Utc::now(),
        )?;
        let ShadowProcessOutcome::Recorded(signal) = &outcome else {
            anyhow::bail!("Shadow was not recorded")
        };
        ensure!(receipt.is_some());
        Ok(ShadowTaskOutput {
            signature: swap.signature.clone(),
            key: ShadowTaskKey {
                wallet: swap.wallet.clone(),
                token: swap.token_out.clone(),
            },
            signal_id: Some(signal.signal_id.clone()),
            side: Some(ShadowSwapSide::Buy),
            buy_receipt: receipt,
            owned_sell_reject: None,
            outcome: Ok(outcome),
        })
    }
    pub async fn completed(
        &self,
        swap: &SwapEvent,
        existing: bool,
        shadow: bool,
        fee: bool,
    ) -> Result<(Completion, Option<ShadowSignalResult>)> {
        self.f.f.store.insert_observed_swap(swap)?;
        if existing {
            // Real legacy quote, with unknown fee, before runtime priority admission.
            let mut config = self.f.execution.clone();
            config.priority_fee_canary_enabled = false;
            let quote = ExecutionQuoteCanaryRunner::new(config);
            let job = quote
                .prepare_hot_quote(&self.f.f.store, swap, Utc::now())?
                .unwrap();
            let (output, server) = tokio::join!(job.job.run(), self.respond(true, false));
            server?;
            ensure!(
                quote.finish_hot_quote(
                    &self.f.f.store,
                    &job.origin,
                    Ok(output),
                    Utc::now(),
                    None
                )? == "hot_quote_recorded"
            );
        }
        let mut scheduler = ShadowScheduler::new();
        let result = async {
            self.runner.admit_hot_observed_buy_quote(
                &self.f.f.store,
                swap,
                Utc::now(),
                &mut scheduler,
            )?;
            if existing {
                let event = self
                    .f
                    .f
                    .store
                    .load_latest_execution_quote_canary_entry_event(&format!(
                        "shadow:{}:{}:buy:{}",
                        swap.signature, swap.wallet, swap.token_out
                    ))?
                    .unwrap();
                ensure!(event.decision_status.as_deref() == Some("owner_pending"));
            }
            let recorded = if shadow {
                let output = self.shadow(swap)?;
                let signal = match &output.outcome {
                    Ok(ShadowProcessOutcome::Recorded(s)) => s.clone(),
                    _ => unreachable!(),
                };
                scheduler.hot_quotes.note_shadow_output(&output);
                Some(signal)
            } else {
                None
            };
            self.respond(!existing, fee).await?;
            scheduler.hot_quotes.collect_next().await;
            ensure!(scheduler.hot_completion_ready());
            let completion = scheduler.hot_quotes.take_completion().unwrap();
            Ok::<_, anyhow::Error>((completion, recorded))
        }
        .await;
        scheduler.hot_quotes.shutdown().await;
        ensure!(scheduler.active_task_count() == 0 && scheduler.buffered_shadow_task_count() == 0);
        result
    }
    pub async fn sell_after_closed_buy(&self) -> Result<()> {
        let (installed, tx) = super::b70_hooks::Installed::new();
        let h = &installed.0;
        let b = self.f.sell();
        let control = async {
            let result = async {
                tx.try_send(b.clone())?;
                h.wait("ack_inserted", &b.signature, 1).await?;
                h.wait("staging_inserted", &b.signature, 1).await?;
                Ok::<_, anyhow::Error>(())
            }
            .await;
            h.stop();
            result
        };
        let (daemon, control) = tokio::join!(self.f.run(), control);
        daemon?;
        control?;
        ensure!(h.count("checked_shutdown", "") == 1);
        let state = self.f.snapshot()?;
        ensure!(state["staged"]["position"] == self.f.position);
        self.f.save(
            "r3-sell-after-closed-a",
            serde_json::json!({"state":state,"events":h.events.lock().unwrap().clone()}),
        )?;
        Ok(())
    }
    pub fn count(&self, signal: &str) -> Result<i64> {
        Ok(self.f.f.conn()?.query_row(
            "SELECT count(*) FROM orders WHERE signal_id=?1",
            [signal],
            |r| r.get(0),
        )?)
    }
}
