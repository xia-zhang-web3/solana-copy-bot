//! Strict-mode native BUY handoff from durable admission to the existing tiny route.
use super::{apply_quote_summary, apply_state_machine_summary, ExecutionCanaryRunner, ExecutionCanaryTickSummary};
use crate::execution_canary_route::{process_native_buy_candidate_for_route, NativeBuyGuard};
use crate::execution_native_buy_rpc;
use crate::execution_owned_sell_rpc::fractional::transport::Http;
use crate::execution_owned_sell_rpc::fractional::transport::Transport;
#[cfg(test)]
use crate::execution_owned_sell_rpc::fractional::transport::Parsed;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use copybot_storage_core::{native_buy::SPL_TOKEN_PROGRAM, SqliteStore};
use std::path::Path;

impl ExecutionCanaryRunner {
    pub(super) async fn process_native_buy_tick(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        summary: &mut ExecutionCanaryTickSummary,
    ) -> Result<()> {
        // Submitted decisions remain obligations even after their admission age
        // expires or new BUY authority is disabled. This sweep only reconciles
        // the native dispatch subset and never rearms an order.
        #[cfg(test)]
        let recovered = if let Some(mock) = self.native_buy_mock.as_deref() {
            crate::execution_canary_route::process_native_buy_receipt_recovery_for_route_with_mock(
                &self.config, store, now, mock,
            ).await?
        } else {
            crate::execution_canary_route::process_native_buy_receipt_recovery_for_route(
                &self.config, store, now,
            ).await?
        };
        #[cfg(not(test))]
        let recovered = crate::execution_canary_route::process_native_buy_receipt_recovery_for_route(
            &self.config, store, now,
        ).await?;
        apply_state_machine_summary(summary, recovered);
        if !execution_native_buy_rpc::enabled(&self.config) {
            return Ok(());
        }
        if Path::new(&self.config.canary_kill_switch_path).exists() {
            summary.skipped_reason = Some("kill_switch_active");
            return Ok(());
        }
        let limit = self.config.canary_batch_limit.clamp(1, 64);
        let pending = store.list_native_buy_pending(limit)?;
        let http = reqwest::Client::new();
        for source in pending {
            #[cfg(test)]
            let mut rpc: Box<dyn Transport + '_> = if let Some(io) = self.native_buy_mock.as_ref() {
                let io = io.clone();
                Box::new(Parsed(move |request: serde_json::Value| {
                    let io = io.clone();
                    async move {
                        let runner = io.runner.as_ref().context("native_runner_mock_missing")?;
                        let value = match request["method"].as_str() {
                            Some("getGenesisHash") => serde_json::json!(runner.finalized_genesis),
                            Some("getTransaction") => {
                                io.count(|c| c.source_finality += 1);
                                runner.finalized_transaction.clone()
                            }
                            Some("getAccountInfo") => runner.mint_account.clone(),
                            _ => anyhow::bail!("native_runner_unexpected_rpc"),
                        };
                        Ok(serde_json::json!({"jsonrpc":"2.0","id":request["id"],"result":value}))
                    }
                }))
            } else {
                Box::new(Http { http: &http, config: &self.config,
                    url: crate::execution_owned_sell_rpc::endpoint(&self.config)?,
                    budget: Default::default() })
            };
            #[cfg(not(test))]
            let mut rpc: Box<dyn Transport + '_> = Box::new(Http {
                http: &http, config: &self.config,
                url: crate::execution_owned_sell_rpc::endpoint(&self.config)?,
                budget: Default::default(),
            });
            let mut check = || {
                anyhow::ensure!(
                    !Path::new(&self.config.canary_kill_switch_path).exists(),
                    "native_buy_kill_switch"
                );
                Ok(())
            };
            if execution_native_buy_rpc::finalized_source(
                rpc.as_mut(),
                &self.config,
                &source.signature,
                source.slot,
                &source.wallet,
                &source.mint,
                &mut check,
            ).await.is_err() {
                summary.last_error = Some("native_buy_finalized_source_denied".into());
                continue;
            }
            if !store.native_buy_record_finalized(
                &source.signature,
                source.slot,
                SPL_TOKEN_PROGRAM,
                Utc::now(),
            )? {
                summary.last_error = Some("native_buy_finality_binding_denied".into());
            }
        }
        for source in store.list_native_buy_finalized(
            limit,
            Utc::now(),
            self.config.canary_max_signal_age_seconds,
        )? {
            let Some(candidate) = store.native_buy_ready(
                &source.signature,
                Utc::now(),
                self.config.canary_max_signal_age_seconds,
            )? else {
                continue;
            };
            let signal = store.load_copy_signal_by_signal_id(&candidate.signal_id)?
                .context("native_buy_promoted_signal_missing")?;
            let guard = NativeBuyGuard::new(
                &self.config,
                &candidate.signal_id,
                &candidate.decision_id,
                now,
            )?;
            #[cfg(test)]
            let guard = if let Some(mock) = &self.native_buy_mock {
                guard.with_mock_io(mock.clone())
            } else { guard };
            if !guard.check(store)? {
                continue;
            }
            let quote = self.quote_canary.process_native_buy_signal(store, &signal, &guard, now).await?;
            apply_quote_summary(summary, quote);
            if !guard.check(store)? {
                continue;
            }
            let state = process_native_buy_candidate_for_route(
                &self.config,
                store,
                &signal,
                now,
                &guard,
            ).await?;
            apply_state_machine_summary(summary, state);
        }
        Ok(())
    }
}
