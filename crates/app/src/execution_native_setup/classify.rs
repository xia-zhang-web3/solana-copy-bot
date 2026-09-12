use super::types::*;
use crate::execution_pumpswap_accounts::{
    associated_token_address, system_program_id, token_program_id,
};
use crate::execution_solana_tx::PubkeyBytes;

pub(super) fn associated(
    account: &InitialSetupAccount,
    owner: &PubkeyBytes,
    mint: &PubkeyBytes,
    program: &PubkeyBytes,
) -> AssociatedInitialState {
    if account.pubkey != associated_token_address(owner, mint, program) {
        return AssociatedInitialState::WrongDerivedAddress;
    }
    if *program != token_program_id() {
        return AssociatedInitialState::UnsupportedTokenProgram;
    }
    match &account.state {
        InitialAccountState::Absent => AssociatedInitialState::CreationRequiredAbsent,
        InitialAccountState::SystemOwnedEmpty => {
            AssociatedInitialState::CreationCandidateSystemPrefunded
        }
        InitialAccountState::Unsupported(_) => AssociatedInitialState::UnsupportedInitialAccount,
        InitialAccountState::Classic { token, .. } => {
            if token.state == ClassicAccountState::Uninitialized {
                AssociatedInitialState::Uninitialized
            } else if token.token_owner != *owner {
                AssociatedInitialState::WrongTokenOwner
            } else if token.mint != *mint {
                AssociatedInitialState::WrongMint
            } else {
                AssociatedInitialState::ExistingIdentityMatch
            }
        }
    }
}

pub(super) fn sync(account: &InitialSetupAccount) -> SyncInitialState {
    match &account.state {
        InitialAccountState::Classic { token, native } => {
            if token.state == ClassicAccountState::Uninitialized {
                return SyncInitialState::Uninitialized;
            }
            match native {
                NativeInitialState::Wsol { .. } => SyncInitialState::ObservedWsol,
                NativeInitialState::NonNative => SyncInitialState::NonNative,
                _ => SyncInitialState::InconsistentNative,
            }
        }
        _ => SyncInitialState::NoClassicInitialState,
    }
}

pub(super) fn close(
    account: &InitialSetupAccount,
    destination: PubkeyBytes,
    authority: PubkeyBytes,
    wallet: PubkeyBytes,
) -> CloseInitialFacts {
    let mut result = CloseInitialFacts {
        effective_authority: None,
        authority_relation: CloseAuthorityRelation::NoClassicInitialState,
        authority_is_expected_wallet: authority == wallet,
        source_equals_destination: account.pubkey == destination,
        destination_is_expected_wallet: destination == wallet,
        balance: CloseInitialBalance::NoClassicInitialState,
    };
    if let InitialAccountState::Classic { token, native } = &account.state {
        let effective = token.close_authority.unwrap_or(token.token_owner);
        result.effective_authority = Some(effective);
        // SPL has a special system/incinerator token-owner branch. Do not pretend
        // ordinary key equality proves that branch, signatures or multisig validation.
        let incinerator = bs58::decode("1nc1nerator11111111111111111111111111111111")
            .into_vec()
            .expect("constant pubkey");
        result.authority_relation = if token.token_owner == system_program_id()
            || token.token_owner.as_slice() == incinerator
        {
            CloseAuthorityRelation::UnsupportedSpecialOwner
        } else if effective == authority {
            CloseAuthorityRelation::MatchesEffectiveKey
        } else {
            CloseAuthorityRelation::ForeignKey
        };
        result.balance = if token.state == ClassicAccountState::Uninitialized {
            CloseInitialBalance::Uninitialized
        } else {
            match native {
                NativeInitialState::Wsol { .. } => CloseInitialBalance::ObservedNative,
                NativeInitialState::NonNative if token.amount == 0 => {
                    CloseInitialBalance::NonNativeZero
                }
                NativeInitialState::NonNative => CloseInitialBalance::NonNativePositive,
                _ => CloseInitialBalance::InconsistentNative,
            }
        };
    }
    result
}
