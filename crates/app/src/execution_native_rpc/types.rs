use crate::execution_native_funding::types::NativeFundingRequirements;
use crate::execution_solana_tx::PubkeyBytes;
use std::time::{Duration, Instant, SystemTime};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ObservationTiming {
    pub(crate) started_at: SystemTime,
    pub(crate) completed_at: SystemTime,
    pub(crate) elapsed: Duration,
}

impl ObservationTiming {
    pub(super) fn finish(started_at: SystemTime, started: Instant) -> Self {
        Self {
            started_at,
            completed_at: SystemTime::now(),
            elapsed: started.elapsed(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RpcObservation<T> {
    pub(crate) slot: u64,
    pub(crate) timing: ObservationTiming,
    pub(crate) value: T,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AccountObservation {
    Absent,
    Present {
        lamports: u64,
        /// Program owner, NOT the token owner/authority encoded in account data.
        owner_program: PubkeyBytes,
        executable: bool,
        data: Vec<u8>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KeyedAccountObservation {
    pub(crate) pubkey: PubkeyBytes,
    pub(crate) account: AccountObservation,
}

/// Two independent confirmed observations, NOT an atomic or reserve-ready snapshot.
/// Requirements retain all offline unknowns. RPC total fee is separate from encoded CU
/// fee: do not add them, or infer base fee, rent, refunds, route cost or pending reserve.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NativeFundingRpcFacts {
    pub(super) requirements: NativeFundingRequirements,
    pub(super) requested_keys: Vec<PubkeyBytes>,
    pub(super) commitment: &'static str,
    pub(super) min_context_slot: Option<u64>,
    pub(super) fee: RpcObservation<Option<u64>>,
    pub(super) accounts: RpcObservation<Vec<KeyedAccountObservation>>,
    pub(super) timing: ObservationTiming,
}

impl NativeFundingRpcFacts {
    // Only the collector can construct/mutate the full bundle. Consumers may clone
    // it or individual observations, but cannot insert observations into a bundle.
    pub(crate) fn requirements(&self) -> &NativeFundingRequirements {
        &self.requirements
    }

    pub(crate) fn requested_keys(&self) -> &[PubkeyBytes] {
        &self.requested_keys
    }

    pub(crate) fn commitment(&self) -> &'static str {
        self.commitment
    }

    pub(crate) fn min_context_slot(&self) -> Option<u64> {
        self.min_context_slot
    }

    pub(crate) fn fee(&self) -> &RpcObservation<Option<u64>> {
        &self.fee
    }

    pub(crate) fn accounts(&self) -> &RpcObservation<Vec<KeyedAccountObservation>> {
        &self.accounts
    }

    pub(crate) fn timing(&self) -> &ObservationTiming {
        &self.timing
    }

    /// Observed payer lamports only; pending exposure and spendable SOL are unknown.
    pub(crate) fn observed_payer_lamports(&self) -> Option<u64> {
        match &self.accounts.value.first()?.account {
            AccountObservation::Absent => None,
            AccountObservation::Present { lamports, .. } => Some(*lamports),
        }
    }
}
