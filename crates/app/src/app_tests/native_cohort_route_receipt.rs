//! Test-only bridge to the unchanged production receipt parser.
pub(crate) fn parse(order: &str, proof: &copybot_storage_core::ExecutionCanaryReceiptProof,
    value: &serde_json::Value) -> anyhow::Result<copybot_storage_core::ReceiptObservationBundle> {
    super::facts_from_transaction_json(order, proof, value)
}
