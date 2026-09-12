use super::types::{NativeFundingRpcFacts, ObservationTiming};

pub(crate) const CLASSIC_TOKEN_ACCOUNT_LENGTH: usize = 165;

/// Scalar RPC observation: this method supplies NO context slot or snapshot proof.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ClassicAtaRentObservation {
    pub(super) data_length: usize,
    pub(super) commitment: &'static str,
    pub(super) lamports: u64,
    pub(super) timing: ObservationTiming,
}
impl ClassicAtaRentObservation {
    pub(crate) fn data_length(&self) -> usize {
        self.data_length
    }
    pub(crate) fn commitment(&self) -> &'static str {
        self.commitment
    }
    pub(crate) fn lamports(&self) -> u64 {
        self.lamports
    }
    pub(crate) fn timing(&self) -> &ObservationTiming {
        &self.timing
    }
}

/// Compatibility carrier: classic observations plus an opt-in Token2022 collection.
/// One collector invocation only. No public construction, mutation or parts assembly.
/// Independent observations, not an atomic snapshot or a full native reserve budget.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ClassicAtaFundingFacts {
    pub(super) native: NativeFundingRpcFacts,
    pub(super) rent: ClassicAtaRentObservation,
    pub(super) token2022: Option<super::token2022_rent::Token2022Collection>,
}
impl ClassicAtaFundingFacts {
    pub(crate) fn token2022_collected(&self) -> bool {
        self.token2022.is_some()
    }
    pub(crate) fn token2022_rent(
        &self,
    ) -> Option<&super::token2022_rent::Token2022RentObservation> {
        self.token2022.as_ref()?.rent.as_ref()
    }

    pub(crate) fn native(&self) -> &NativeFundingRpcFacts {
        &self.native
    }
    pub(crate) fn rent(&self) -> &ClassicAtaRentObservation {
        &self.rent
    }
}
