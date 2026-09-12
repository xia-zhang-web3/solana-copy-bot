use super::types::*;
use crate::execution_native_rpc::types::{AccountObservation, KeyedAccountObservation};
use crate::execution_pumpswap_accounts::{system_program_id, token_program_id, wsol_mint};

pub(super) fn initial_account(row: &KeyedAccountObservation, index: usize) -> InitialSetupAccount {
    let (observed_lamports, state) = match &row.account {
        AccountObservation::Absent => (None, InitialAccountState::Absent),
        AccountObservation::Present {
            lamports,
            owner_program,
            executable,
            data,
        } => {
            let state = if *executable {
                InitialAccountState::Unsupported(AccountIssue::Executable)
            } else if *owner_program == system_program_id() {
                if data.is_empty() {
                    InitialAccountState::SystemOwnedEmpty
                } else {
                    InitialAccountState::Unsupported(AccountIssue::SystemDataNotEmpty)
                }
            } else if *owner_program != token_program_id() {
                // Includes Token-2022, even for a 165-byte body. No extension decoding.
                InitialAccountState::Unsupported(AccountIssue::ProgramOwner)
            } else {
                match unpack(data) {
                    Ok(token) => InitialAccountState::Classic {
                        native: native_state(&token, *lamports),
                        token,
                    },
                    Err(reason) => InitialAccountState::Unsupported(reason),
                }
            };
            (Some(*lamports), state)
        }
    };
    InitialSetupAccount {
        pubkey: row.pubkey,
        observation_index: index,
        observed_lamports,
        state,
    }
}

// Layout and exact COption tags: spl-token 7.0.0 state.rs Account::unpack_from_slice.
// Decode uninitialized explicitly instead of treating it as an initialized account.
fn unpack(data: &[u8]) -> Result<ClassicTokenAccount, AccountIssue> {
    if data.len() != 165 {
        return Err(AccountIssue::ClassicLength);
    }
    let state = match data[108] {
        0 => ClassicAccountState::Uninitialized,
        1 => ClassicAccountState::Initialized,
        2 => ClassicAccountState::Frozen,
        _ => return Err(AccountIssue::ClassicState),
    };
    Ok(ClassicTokenAccount {
        mint: data[0..32].try_into().expect("fixed layout"),
        token_owner: data[32..64].try_into().expect("fixed layout"),
        amount: u64::from_le_bytes(data[64..72].try_into().expect("fixed layout")),
        delegate: option_bytes::<32>(&data[72..108], AccountIssue::DelegateTag)?,
        state,
        native_reserve: option_bytes::<8>(&data[109..121], AccountIssue::NativeTag)?
            .map(u64::from_le_bytes),
        delegated_amount: u64::from_le_bytes(data[121..129].try_into().expect("fixed layout")),
        close_authority: option_bytes::<32>(&data[129..165], AccountIssue::CloseAuthorityTag)?,
    })
}

fn option_bytes<const N: usize>(
    data: &[u8],
    reason: AccountIssue,
) -> Result<Option<[u8; N]>, AccountIssue> {
    match &data[..4] {
        [0, 0, 0, 0] => Ok(None), // SPL ignores unused bytes, including nonzero payloads.
        [1, 0, 0, 0] => Ok(Some(data[4..].try_into().expect("fixed layout"))),
        _ => Err(reason),
    }
}

fn native_state(token: &ClassicTokenAccount, lamports: u64) -> NativeInitialState {
    match (token.mint == wsol_mint(), token.native_reserve) {
        (false, None) => NativeInitialState::NonNative,
        (true, Some(reserve)) => match lamports.checked_sub(reserve) {
            None => NativeInitialState::ReserveExceedsLamports,
            Some(backing) if token.amount > backing => NativeInitialState::AmountExceedsBacking,
            Some(backing) => NativeInitialState::Wsol {
                lamports_minus_reserve: backing,
                synced: token.amount == backing,
            },
        },
        _ => NativeInitialState::MintReserveMismatch,
    }
}
