//! Committed provider observations used for replay, never trade authority.
use crate::association_delivery::{AdmissionFacts, InfoIdentity};
use crate::association_parent::ParentObservation;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayScope {
    pub policy: String,
    pub wallets: Vec<String>,
    pub programs: Vec<String>,
    pub raydium_programs: Vec<String>,
    pub pumpswap_programs: Vec<String>,
}
impl ReplayScope {
    pub fn valid(&self) -> bool {
        self.policy == "durable_checkpoint_replay_v1"
            && sorted_nonempty(&self.wallets, 16)
            && sorted_nonempty(&self.programs, 256)
            && sorted_nonempty(&self.raydium_programs, 256)
            && sorted_nonempty(&self.pumpswap_programs, 256)
    }
}
fn sorted_nonempty(values: &[String], max: usize) -> bool {
    !values.is_empty()
        && values.len() <= max
        && values.iter().all(|v| !v.is_empty() && v.len() <= 128)
        && values.windows(2).all(|pair| pair[0] < pair[1])
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointClaim {
    pub signature: String,
    pub transaction_index: u64,
    pub info: InfoIdentity,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockCheckpoint {
    pub scope: ReplayScope,
    pub observation: ParentObservation,
    pub executed_transaction_count: u64,
    pub supplied_transaction_count: u64,
    pub claims: Vec<CheckpointClaim>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableCheckpoint {
    pub session: String,
    pub sequence: u64,
    pub block: BlockCheckpoint,
    /// Inclusive overlap, possibly lowered by a durable pending identity without a terminal.
    pub from_slot: u64,
    /// Snapshot of immutable first admissions in the inclusive replay overlap.
    /// Populated by the SQLite reader; never used as trade authority.
    #[serde(default)]
    pub overlap: Vec<AdmissionFacts>,
}
