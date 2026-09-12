//! Private invocation-bound scalar170 observation; no context slot/atomic-bank claim.
use super::types::ObservationTiming;
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Token2022Collection {
    pub(super) rent: Option<Token2022RentObservation>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Token2022RentObservation {
    pub(super) data_length: usize,
    pub(super) commitment: &'static str,
    pub(super) lamports: u64,
    pub(super) timing: ObservationTiming,
    pub(super) transaction_sha256: String,
}
impl Token2022RentObservation {
    pub(crate) fn data_length(&self) -> usize {
        self.data_length
    }
    pub(crate) fn commitment(&self) -> &'static str {
        self.commitment
    }
    pub(crate) fn lamports(&self) -> u64 {
        self.lamports
    }
    pub(crate) fn transaction_sha256(&self) -> &str {
        &self.transaction_sha256
    }
}
