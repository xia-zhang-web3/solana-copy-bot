//! In-process external account/fee observations for the native BUY route.
use super::rent_types::{ClassicAtaFundingFacts, ClassicAtaRentObservation};
use super::types::{
    AccountObservation, KeyedAccountObservation, NativeFundingRpcFacts, ObservationTiming,
    RpcObservation,
};
use crate::execution_native_funding::decode_native_funding_requirements;
use crate::execution_pumpswap_accounts::system_program_id;
use crate::execution_solana_tx::PubkeyBytes;
use anyhow::Result;
use std::time::{Duration, SystemTime};

pub(crate) fn synthetic_classic_funding(
    payload: &str,
    wallet: PubkeyBytes,
    balance: u64,
    fee: u64,
    slot: u64,
) -> Result<ClassicAtaFundingFacts> {
    let requirements = decode_native_funding_requirements(payload, wallet)?;
    let requested_keys: Vec<_> = requirements.binding.accounts.iter().map(|a| a.pubkey).collect();
    let timing = ObservationTiming {
        started_at: SystemTime::now(),
        completed_at: SystemTime::now(),
        elapsed: Duration::ZERO,
    };
    let accounts = requested_keys.iter().map(|pubkey| KeyedAccountObservation {
        pubkey: *pubkey,
        account: if *pubkey == wallet {
            AccountObservation::Present {
                lamports: balance,
                owner_program: system_program_id(),
                executable: false,
                data: Vec::new(),
            }
        } else {
            AccountObservation::Absent
        },
    }).collect();
    Ok(ClassicAtaFundingFacts {
        native: NativeFundingRpcFacts {
            requirements,
            requested_keys,
            commitment: "confirmed",
            min_context_slot: None,
            fee: RpcObservation { slot, timing: timing.clone(), value: Some(fee) },
            accounts: RpcObservation { slot, timing: timing.clone(), value: accounts },
            timing: timing.clone(),
        },
        rent: ClassicAtaRentObservation {
            data_length: 165,
            commitment: "confirmed",
            lamports: 2_039_280,
            timing,
        },
        token2022: None,
    })
}
