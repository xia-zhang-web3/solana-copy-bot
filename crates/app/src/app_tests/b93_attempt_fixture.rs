use super::{association_fixture::Db, b93_fixture as f, b93_http_fixture::Server};
use crate::execution_source_sell_guard as guard;
use crate::execution_submit_adapter::*;
use anyhow::Result;
use copybot_config::ExecutionConfig;
use copybot_core_types::CopySignalRow;
use copybot_storage_core::*;
use serde_json::Value;

pub struct Attempt {
    pub db: Db,
    pub m: Value,
    pub rpc: Server,
    pub config: ExecutionConfig,
    pub signal: CopySignalRow,
    pub event_id: String,
    pub request: ExecutionSubmitRequest,
    pub plan: ExecutionTransactionPlan,
}
impl Attempt {
    pub async fn new(name: &str) -> Result<Self> {
        let (db, m) = f::seeded(name).await?;
        let signal = f::legacy(&db, &m)?;
        let quote = super::source_write_off_fixture::quote(&signal, f::at());
        db.store.record_execution_quote_canary_event(&quote)?;
        let order = db
            .store
            .reserve_execution_canary_sell_order_unless_token_in_flight(
                &signal.signal_id,
                super::source_write_off_fixture::ROUTE,
                f::at(),
            )?
            .order;
        let rpc = Server::new(db.path.clone(), f::at(), [94; 32]).await?;
        let config = f::config(&rpc.url);
        let source = guard::order(
            &db.store,
            &order.order_id,
            &[EXECUTION_STATUS_CANARY_CANDIDATE],
        )?;
        let metadata =
            crate::execution_quote_provider_selection::selected_execution_build_plan_metadata(
                &db.store,
                quote.clone(),
            )?;
        let metadata = crate::execution_canary_route::guarded_owned_position_sell_metadata(
            &config,
            &db.store,
            &signal.token,
            metadata,
            source.as_ref(),
        )
        .await?;
        let request = ExecutionSubmitRequest {
            order_id: order.order_id,
            signal_id: signal.signal_id.clone(),
            client_order_id: order.client_order_id,
            attempt: order.attempt,
            route: order.route,
            wallet_id: signal.wallet_id.clone(),
            token: signal.token.clone(),
            side: signal.side.clone(),
            wallet_pubkey: config.canary_wallet_pubkey.clone(),
            buy_size_sol: config.canary_buy_size_sol,
            slippage_tolerance_bps:
                crate::execution_quote_canary_helpers::quote_canary_slippage_limit_bps(
                    &config, "sell",
                ),
            entry_route_plan_json: None,
            metadata,
        };
        guard::request(&db.store, &request, &[EXECUTION_STATUS_CANARY_CANDIDATE])?;
        let adapter = JupiterMetisDryRunExecutionAdapter::new(config.clone());
        let plan = adapter.build_transaction_plan(&request)?;
        crate::execution_build_plan_metadata::record_execution_build_plan_metadata(
            &db.store,
            &plan,
            f::at(),
        )?;
        db.store
            .mark_execution_canary_built(&request.order_id, f::at())?;
        let simulation = adapter.simulate_transaction_plan(&plan).await?;
        assert_eq!(simulation.status, EXECUTION_SIMULATION_STATUS_PASSED);
        db.store.mark_execution_canary_simulated(&request.order_id, f::at(), &simulation.status,
            Some(crate::execution_canary_submit_contract::TINY_SUBMIT_RETRY_AFTER_RPC_NOT_SENT_REASON))?;
        Ok(Self {
            db,
            m,
            rpc,
            config,
            signal,
            event_id: quote.event_id,
            request,
            plan,
        })
    }
    pub fn partial(&self, name: &str) -> Result<ExecutionCanaryReceiptFacts> {
        let facts = f::receipt(&self.db, &self.m, name, 3000)?;
        f::settle(&self.db, &facts)?;
        assert_eq!(f::raw(&self.db, &self.m)?, 4000);
        Ok(facts)
    }
    pub fn reopen(&mut self) -> Result<()> {
        self.db = f::open(&self.db.path)?;
        Ok(())
    }
    pub async fn retry(
        &self,
    ) -> Result<crate::execution_canary_state_machine::ExecutionCanaryStateMachineSummary> {
        crate::execution_canary_route::process_tiny_submit_reconciliation_sweep(
            &self.config,
            &self.db.store,
            f::at(),
        )
        .await?
        .ok_or_else(|| anyhow::anyhow!("retry route missing"))
    }
    pub fn sign(
        &self,
        r: &ExecutionSubmitRequest,
        p: &ExecutionTransactionPlan,
    ) -> Result<crate::execution_canary_signing_contract::ExecutionSigningEnvelopeOutcome> {
        crate::execution_canary_signing_contract::record_execution_signing_envelope(
            &self.db.store,
            &JupiterMetisDryRunExecutionAdapter::new(self.config.clone()),
            r,
            p,
            f::at(),
        )
    }
}
