use super::source_guard_rpc_fixture::{Server, Snapshot};
use super::source_write_off_fixture::{self as old, Fixture as Source};
use crate::execution_submit_adapter::*;
use anyhow::Result;
use copybot_config::ExecutionConfig;
use copybot_storage_core::*;
use ed25519_dalek::SigningKey;
use std::path::PathBuf;

pub(super) struct Fixture {
    pub f: Source,
    pub rpc: Server,
    pub config: ExecutionConfig,
    pub event_id: String,
    key: PathBuf,
}
impl Fixture {
    pub async fn new() -> Result<Self> {
        Self::with_parent(true).await
    }
    pub async fn legacy_parent() -> Result<Self> {
        Self::with_parent(false).await
    }
    async fn with_parent(pinned: bool) -> Result<Self> {
        let signing = SigningKey::from_bytes(&[29; 32]);
        let payer = signing.verifying_key().to_bytes();
        let mut f = if pinned {
            Source::new_with_parent(|store, path, now| {
                super::tiny_parent_fixture::seed(
                    store,
                    &rusqlite::Connection::open(path)?,
                    "buy-a",
                    "source-a",
                    "mint",
                    &bs58::encode(payer).into_string(),
                    copybot_core_types::TokenQuantity::new(4000, 3),
                    1000,
                    now,
                )
                .map(|_| ())
            })?
        } else {
            Source::new_with_parent(|store, _, now| {
                old::proven_buy(
                    store,
                    "buy-a",
                    "source-a",
                    now,
                    copybot_core_types::TokenQuantity::new(4000, 3),
                )
                .map(|_| ())
            })?
        };
        if !pinned {
            f.replacement_wallet = Some(bs58::encode(payer).into_string());
        }
        let key = f.path.with_extension("synthetic-signing.json");
        std::fs::write(
            &key,
            serde_json::to_vec(&[signing.to_bytes().to_vec(), payer.to_vec()].concat())?,
        )?;
        let rpc = Server::new(f.path.clone(), f.now, payer).await?;
        let mut config = old::config(&rpc.url);
        config.canary_wallet_pubkey = bs58::encode(payer).into_string();
        config.execution_signer_pubkey = config.canary_wallet_pubkey.clone();
        config.execution_signer_keypair_path = key.to_string_lossy().into();
        config.quote_canary_enabled = true;
        config.priority_fee_canary_enabled = true;
        config.priority_fee_canary_rpc_url = rpc.url.clone();
        config.swap_instructions_dry_run_enabled = true;
        config.max_submit_attempts = 3;
        config.pretrade_max_priority_fee_lamports = 500_000;
        config.max_confirm_seconds = 1;
        config.quote_canary_sell_slippage_bps = 500;
        config.tiny_experiment = super::b126_config_fixture::activated(&config)?.tiny_experiment;
        Ok(Self {
            f,
            rpc,
            config,
            key,
            event_id: old::EVENT.into(),
        })
    }
    pub fn state(&self) -> Result<Snapshot> {
        old::snapshot(&self.f.conn()?, &[])
    }
    pub async fn finish(&mut self) -> Result<()> {
        self.rpc.finish().await
    }
    pub async fn owned_quote(
        &self,
    ) -> Result<crate::execution_quote_canary::ExecutionQuoteCanaryTickSummary> {
        let s = &self.f.signal;
        let shadow = copybot_shadow::ShadowSignalResult {
            signal_id: s.signal_id.clone(),
            wallet_id: s.wallet_id.clone(),
            side: s.side.clone(),
            token: s.token.clone(),
            notional_sol: s.notional_sol,
            latency_ms: 0,
            closed_qty: 0.0,
            realized_pnl_sol: 0.0,
            has_open_lots_after_signal: Some(false),
        };
        crate::execution_quote_canary::ExecutionQuoteCanaryRunner::new(self.config.clone())
            .process_recorded_shadow_signal(&self.f.store, &shadow, self.f.now)
            .await
    }
    pub async fn tiny(
        &self,
    ) -> Result<crate::execution_canary_state_machine::ExecutionCanaryStateMachineSummary> {
        Ok(
            crate::execution_canary_route::process_tiny_submit_sell_quote_event_for_route(
                &self.config,
                &self.f.store,
                &self.event_id,
                self.f.now,
            )
            .await?
            .unwrap(),
        )
    }
    pub fn request(&self) -> Result<ExecutionSubmitRequest> {
        let s = &self.f.signal;
        let order = self
            .f
            .store
            .reserve_execution_canary_order(&s.signal_id, &self.config.canary_route, self.f.now)?
            .order;
        let event = self
            .f
            .store
            .load_execution_quote_canary_event_by_id(&self.event_id)?
            .unwrap();
        let mut metadata =
            crate::execution_quote_provider_selection::selected_execution_build_plan_metadata(
                &self.f.store,
                event,
            )?;
        metadata.quote_response_json = Some(serde_json::json!({"inputMint":"mint","outputMint":"So11111111111111111111111111111111111111112","inAmount":"4000","outAmount":"100000000","otherAmountThreshold":"90000000","swapMode":"ExactIn","slippageBps":500,"routePlan":[{"swapInfo":{"label":"Metis"}}]}).to_string());
        metadata.quote_in_amount_raw = Some("4000".into());
        metadata.quote_out_amount_raw = Some("100000000".into());
        let source = crate::execution_source_sell_guard::order(
            &self.f.store,
            &order.order_id,
            &[EXECUTION_STATUS_CANARY_CANDIDATE],
        )?;
        let position = self
            .f
            .store
            .load_execution_canary_open_position(&s.token)?
            .unwrap();
        let selection = crate::execution_source_sell_guard::amount::Selection::new(&position)?;
        metadata = selection.finish(
            metadata,
            source.as_ref(),
            &self.config.canary_wallet_pubkey,
            4000,
            4000,
        )?;
        Ok(ExecutionSubmitRequest {
            order_id: order.order_id,
            signal_id: s.signal_id.clone(),
            client_order_id: order.client_order_id,
            attempt: order.attempt,
            route: order.route,
            wallet_id: s.wallet_id.clone(),
            token: s.token.clone(),
            side: s.side.clone(),
            buy_size_sol: 0.01,
            slippage_tolerance_bps: 500,
            wallet_pubkey: self.config.canary_wallet_pubkey.clone(),
            entry_route_plan_json: None,
            metadata,
        })
    }
    pub fn retry(&self) -> Result<String> {
        let r = self.request()?;
        let adapter = JupiterMetisDryRunExecutionAdapter::new(self.config.clone());
        let plan = adapter.build_transaction_plan(&r)?;
        crate::execution_build_plan_metadata::record_execution_build_plan_metadata(
            &self.f.store,
            &plan,
            self.f.now,
        )?;
        self.f
            .store
            .mark_execution_canary_built(&r.order_id, self.f.now)?;
        self.f.store.mark_execution_canary_simulated(&r.order_id,self.f.now,EXECUTION_SIMULATION_STATUS_PASSED,
            Some(crate::execution_canary_submit_contract::TINY_SUBMIT_RETRY_AFTER_RPC_NOT_SENT_REASON))?;
        Ok(r.order_id)
    }
    pub async fn retry_sweep(
        &self,
    ) -> Result<crate::execution_canary_state_machine::ExecutionCanaryStateMachineSummary> {
        crate::execution_canary_route::process_tiny_submit_reconciliation_sweep(
            &self.config,
            &self.f.store,
            self.f.now,
        )
        .await?
        .ok_or_else(|| anyhow::anyhow!("missing retry route"))
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.key);
    }
}
