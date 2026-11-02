//! Only a validated negative simulation on the unsigned runner yields this proof.
use anyhow::Result;
use serde_json::Value;
#[derive(Debug)]
pub(crate) struct Rejection {
    payload_sha256: String,
    slot: u64,
}
impl Rejection {
    pub(crate) fn proof(&self) -> (&str, u64) {
        (&self.payload_sha256, self.slot)
    }
}
impl std::fmt::Display for Rejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("owned_sell_unsigned_simulation_rejected")
    }
}
impl std::error::Error for Rejection {}
pub(crate) fn verify(value: &Value, source: &str, unsigned: &str) -> Result<()> {
    match crate::execution_transaction_rpc_simulation::parse_rpc_simulation_response(value, source)
    {
        Ok(_) => Ok(()),
        Err(error) => match error
            .downcast_ref::<crate::execution_transaction_rpc_simulation::RpcSimulationRejected>()
        {
            Some(rejected) => Err(Rejection {
                payload_sha256: crate::execution_owned_sell_rpc::digest(unsigned),
                slot: rejected.slot,
            }
            .into()),
            None => Err(error),
        },
    }
}
