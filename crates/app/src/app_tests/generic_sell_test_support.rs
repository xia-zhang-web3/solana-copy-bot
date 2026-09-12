use super::generic_sell_fixture::*;
use super::generic_sell_loopback::*;
use crate::execution_submit_adapter::*;
use anyhow::Result;
use copybot_config::ExecutionConfig;

pub(super) struct Run {
    pub server: Server,
    pub config: ExecutionConfig,
    pub request: ExecutionSubmitRequest,
    pub plan: ExecutionTransactionPlan,
    pub result: Result<ExecutionSimulationResult>,
}
pub(super) async fn run(
    replies: Replies,
    configure: impl FnOnce(&mut ExecutionConfig),
    modify: impl FnOnce(&mut ExecutionTransactionPlan),
) -> Result<Run> {
    let server = Server::start(replies);
    let request = request()?;
    let mut config = config(&server.base);
    configure(&mut config);
    let adapter = JupiterMetisDryRunExecutionAdapter::new(config.clone());
    let mut plan = adapter.build_transaction_plan(&request)?;
    modify(&mut plan);
    let result = adapter.simulate_transaction_plan(&plan).await;
    Ok(Run {
        server,
        config,
        request,
        plan,
        result,
    })
}
impl Run {
    pub fn payload(&self) -> Option<String> {
        self.plan
            .serialized_transaction_payload_slot
            .as_ref()
            .unwrap()
            .load()
            .unwrap()
            .map(|p| p.serialized_transaction_base64)
    }
    pub fn assert_rejected(&self, case: &str, simulations: usize) {
        assert!(self.result.is_err(), "{case}: {:?}", self.result);
        assert!(
            self.payload().is_none(),
            "{case}: payload stored on refusal"
        );
        assert_eq!(
            self.server.count("/swap"),
            0,
            "{case}: hidden swap fallback"
        );
        assert_eq!(
            self.server.simulations().len(),
            simulations,
            "{case}: wrong simulation boundary"
        );
        assert_eq!(
            self.server.count_kind("unexpected"),
            0,
            "{case}: unexpected request"
        );
        super::generic_sell_fixture::save(&format!("negative-{case}.json"), &serde_json::json!({"case":case,"error":format!("{:?}",self.result),"calls":self.server.calls(),"slot":self.payload()})).unwrap();
        eprintln!("{case}: {}", self.result.as_ref().unwrap_err());
    }
}
