use crate::{AppConfig, IngestionConfig};
use anyhow::{ensure, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryBudget {
    pub count: usize,
    pub bytes: usize,
}
/// No default budgets or TTLs. Operators must explicitly choose every limit.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssociationDeliveryConfig {
    pub pending: DeliveryBudget,
    pub blocks: DeliveryBudget,
    pub history: DeliveryBudget,
    pub outputs: DeliveryBudget,
    pub queue: DeliveryBudget,
    pub inbox: DeliveryBudget,
    pub input_bytes: usize,
    pub metadata_bytes: usize,
    pub pending_ttl_ms: u64,
    pub block_ttl_ms: u64,
    pub history_ttl_ms: u64,
    pub tick_ms: u64,
    pub sqlite_busy_ms: u64,
}
impl AssociationDeliveryConfig {
    pub fn validate(&self) -> Result<()> {
        let mut total = 0usize;
        for b in [
            &self.pending,
            &self.blocks,
            &self.history,
            &self.outputs,
            &self.queue,
            &self.inbox,
        ] {
            ensure!(
                b.count > 0 && b.bytes > 0,
                "association delivery zero budget"
            );
            total = total
                .checked_add(b.bytes)
                .and_then(|n| b.count.checked_mul(1024).and_then(|c| n.checked_add(c)))
                .ok_or_else(|| anyhow::anyhow!("association delivery budget overflow"))?;
        }
        ensure!(
            self.input_bytes > 0
                && self.input_bytes <= u32::MAX as usize
                && self.metadata_bytes > 0,
            "association delivery input/metadata budget"
        );
        ensure!(
            self.queue.bytes <= u32::MAX as usize && self.queue.count <= u32::MAX as usize,
            "association delivery queue semaphore bound"
        );
        ensure!(
            [
                self.pending_ttl_ms,
                self.block_ttl_ms,
                self.history_ttl_ms,
                self.tick_ms,
                self.sqlite_busy_ms
            ]
            .iter()
            .all(|n| *n > 0),
            "association delivery explicit positive durations required"
        );
        ensure!(
            self.sqlite_busy_ms <= 60_000,
            "association delivery busy timeout bound"
        );
        ensure!(
            total
                .checked_add(self.input_bytes)
                .and_then(|n| n.checked_add(self.metadata_bytes))
                .is_some_and(|n| n <= isize::MAX as usize),
            "association delivery overflow"
        );
        Ok(())
    }
}
pub fn validate_delivery_source(c: &IngestionConfig) -> Result<()> {
    if let Some(path) = &c.capture_scope_db {
        ensure!(
            !path.trim().is_empty()
                && c.source == "yellowstone_grpc"
                && c.yellowstone_delivery_mode == "legacy",
            "scoped capture requires legacy Yellowstone and an explicit database path"
        );
    }
    match c.yellowstone_delivery_mode.as_str() {
        "legacy" => Ok(()),
        "durable_association_v1" => {
            ensure!(
                c.source == "yellowstone_grpc",
                "durable association requires Yellowstone"
            );
            c.yellowstone_association
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("explicit association limits required"))?
                .validate()
        }
        _ => anyhow::bail!("unknown yellowstone delivery mode"),
    }
}
pub fn validate_association_delivery(c: &AppConfig) -> Result<()> {
    validate_delivery_source(&c.ingestion)?;
    if c.ingestion.capture_scope_db.is_some() {
        ensure!(
            !c.execution.enabled
                && !c.execution.canary_tiny_submit_enabled
                && !c.execution.tiny_experiment.activate,
            "observation-only capture requires execution, tiny and activation flags=false"
        );
    }
    crate::validate_owned_sell_preparation(&c.execution, &c.ingestion)?;
    if let Some(native) = &c.execution.native_fresh_buy {
        ensure!(
            native.policy == crate::PROCESSED_SLOT_FENCE_AVAILABILITY_V1,
            "native_fresh_buy_policy_unsupported"
        );
        ensure!(
            c.ingestion.yellowstone_delivery_mode == "durable_association_v1"
                && c.ingestion.source == "yellowstone_grpc"
                && c.ingestion.capture_scope_db.is_none(),
            "native_fresh_buy_delivery_mode"
        );
        ensure!(
            c.execution.canary_tiny_submit_enabled
                && crate::owned_sell_dispatch(&c.execution),
            "native_fresh_buy_owned_sell_dispatch_required"
        );
        ensure!(
            c.execution.tiny_experiment.id.is_some()
                && !c.execution.tiny_experiment.activate,
            "native_fresh_buy_existing_experiment_required"
        );
        ensure!(
            c.execution.canary_max_signal_age_seconds > 0,
            "native_fresh_buy_signal_age_limit_required"
        );
    }
    if c.ingestion.yellowstone_delivery_mode == "durable_association_v1" {
        ensure!(
            crate::owned_sell_flags(&c.execution),
            "durable association requires both execution flags=false"
        );
    }
    Ok(())
}
