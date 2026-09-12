pub(super) use super::native_funding_fixture::{budget, direct, payload, PEER, TOKEN, WALLET};
pub(super) use super::native_setup_rpc_fixture::{responses, set};
pub(super) use crate::execution_native_rpc::types::*;
pub(super) use crate::execution_native_setup::{interpret_native_setup as interpret, types::*};
pub(super) use crate::execution_pumpswap_accounts::*;
use crate::execution_solana_tx::{PubkeyBytes, SolanaAccountMeta as Meta, SolanaInstruction};
use anyhow::Result;

pub(super) fn present(lamports: u64, program: PubkeyBytes, data: Vec<u8>) -> AccountObservation {
    AccountObservation::Present {
        lamports,
        owner_program: program,
        executable: false,
        data,
    }
}

pub(super) fn token_bytes(
    mint: PubkeyBytes,
    owner: PubkeyBytes,
    amount: u64,
    reserve: Option<u64>,
) -> Vec<u8> {
    let mut data = vec![0; 165];
    data[..32].copy_from_slice(&mint);
    data[32..64].copy_from_slice(&owner);
    data[64..72].copy_from_slice(&amount.to_le_bytes());
    data[108] = 1;
    if let Some(reserve) = reserve {
        data[109..113].copy_from_slice(&1_u32.to_le_bytes());
        data[113..121].copy_from_slice(&reserve.to_le_bytes());
    }
    data
}

pub(super) fn key_option(data: &mut [u8], offset: usize, key: PubkeyBytes) {
    data[offset..offset + 4].copy_from_slice(&1_u32.to_le_bytes());
    data[offset + 4..offset + 36].copy_from_slice(&key);
}

pub(super) fn ata(
    owner: PubkeyBytes,
    mint: PubkeyBytes,
    program: PubkeyBytes,
    address: PubkeyBytes,
) -> SolanaInstruction {
    SolanaInstruction {
        program_id: associated_token_program_id(),
        data: vec![1],
        accounts: vec![
            Meta::signer_writable(WALLET),
            Meta::writable(address),
            Meta::readonly(owner),
            Meta::readonly(mint),
            Meta::readonly(system_program_id()),
            Meta::readonly(program),
        ],
    }
}

pub(super) fn sync_close(source: PubkeyBytes, destination: PubkeyBytes) -> Vec<SolanaInstruction> {
    let mut result = budget();
    result.push(SolanaInstruction {
        program_id: token_program_id(),
        data: vec![17],
        accounts: vec![Meta::writable(source)],
    });
    result.push(SolanaInstruction {
        program_id: token_program_id(),
        data: vec![9],
        accounts: vec![
            Meta::writable(source),
            Meta::writable(destination),
            Meta::signer_writable(WALLET),
        ],
    });
    result
}

pub(super) fn setup_payload(
    owner: PubkeyBytes,
    mint: PubkeyBytes,
    program: PubkeyBytes,
    address: PubkeyBytes,
) -> Result<String> {
    let mut instructions = budget();
    instructions.push(ata(owner, mint, program, address));
    payload(&instructions)
}

pub(super) fn assert_unknown(result: &NativeSetupInterpretation<'_>) {
    use crate::execution_native_funding::types::{FundingCoverage, UnavailableNativeBudget};
    assert_eq!(
        result.facts.requirements().unavailable_budget,
        UnavailableNativeBudget::default()
    );
    assert_eq!(
        result.facts.requirements().coverage,
        FundingCoverage::PartialWithStateOrUnsupported
    );
}
