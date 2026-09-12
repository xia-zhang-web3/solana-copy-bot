use crate::ExecutionCanaryReceiptFacts;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const MAX_NATIVE_ACCOUNTS: usize = 64;
pub const MAX_NATIVE_INSTRUCTIONS: usize = 64;
pub const MAX_NATIVE_OBSERVATION_BYTES: usize = 262_144;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservationCoverage {
    Known,
    Missing,
    Invalid,
    Unsupported,
    Truncated,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservationSource {
    RpcAccountKey,
    RpcNativeBalance,
    RpcTokenBalance,
    ParsedInstruction,
    ProvenLifecycle,
    Unavailable,
}
/// Exact quantities and identities remain strings through SQLite, JSON and React.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeObservation {
    pub value: Option<String>,
    pub coverage: ObservationCoverage,
    pub source: ObservationSource,
}
impl NativeObservation {
    pub fn known(value: impl ToString, source: ObservationSource) -> Self {
        Self {
            value: Some(value.to_string()),
            coverage: ObservationCoverage::Known,
            source,
        }
    }
    pub fn unknown(coverage: ObservationCoverage) -> Self {
        Self {
            value: None,
            coverage,
            source: ObservationSource::Unavailable,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeTokenEndpoint {
    pub mint: NativeObservation,
    pub token_owner: NativeObservation,
    pub token_program: NativeObservation,
    pub decimals: NativeObservation,
    pub raw: NativeObservation,
}
impl NativeTokenEndpoint {
    pub fn unknown(coverage: ObservationCoverage) -> Self {
        let v = NativeObservation::unknown(coverage);
        Self {
            mint: v.clone(),
            token_owner: v.clone(),
            token_program: v.clone(),
            decimals: v.clone(),
            raw: v,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeAccountObservation {
    pub account_index: u32,
    pub pubkey: String,
    pub native_pre: NativeObservation,
    pub native_post: NativeObservation,
    pub native_delta: NativeObservation,
    pub pre_token: NativeTokenEndpoint,
    pub post_token: NativeTokenEndpoint,
    /// Selection evidence only. Instruction authority never proves token ownership.
    pub relevance: Vec<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeInstructionObservation {
    pub outer_index: u32,
    pub inner_index: Option<u32>,
    pub stack_height: NativeObservation,
    pub program_id: NativeObservation,
    pub instruction_type: NativeObservation,
    /// Allowlisted parsed fields; owner here is an instruction role, not account state.
    pub fields: BTreeMap<String, NativeObservation>,
    pub coverage: ObservationCoverage,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeAccountObservations {
    pub order_id: String,
    pub tx_signature: String,
    pub wallet_pubkey: String,
    pub token: String,
    pub side: String,
    pub slot: String,
    pub accounts: Vec<NativeAccountObservation>,
    pub instructions: Vec<NativeInstructionObservation>,
    pub accounts_coverage: ObservationCoverage,
    pub instructions_coverage: ObservationCoverage,
    pub reasons: Vec<String>,
}
impl NativeAccountObservations {
    pub fn empty(f: &ExecutionCanaryReceiptFacts) -> Self {
        Self {
            order_id: f.order_id.clone(),
            tx_signature: f.tx_signature.clone(),
            wallet_pubkey: f.wallet_pubkey.clone(),
            token: f.token.clone(),
            side: f.side.clone(),
            slot: f.slot.to_string(),
            accounts: vec![],
            instructions: vec![],
            accounts_coverage: ObservationCoverage::Known,
            instructions_coverage: ObservationCoverage::Known,
            reasons: vec![],
        }
    }
    pub fn note(&mut self, reason: &str) {
        if self.reasons.len() < 16 && !self.reasons.iter().any(|r| r == reason) {
            self.reasons.push(reason.into());
        }
    }
}
#[derive(Debug, Clone)]
pub struct ReceiptObservationBundle {
    pub facts: ExecutionCanaryReceiptFacts,
    pub native: NativeAccountObservations,
}
