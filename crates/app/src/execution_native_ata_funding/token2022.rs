//! Reparse only bound ordered observations; never accept caller-assembled mint/rent facts.
pub(crate) use super::profile::{program_id, ACCOUNT_LENGTH};
use super::{
    profile,
    types::{AtaFundingAmount, AtaFundingIssue},
};
use crate::execution_native_funding::types::FundingOperation as Op;
use crate::execution_native_rpc::{
    rent_types::ClassicAtaFundingFacts,
    types::{AccountObservation as Account, KeyedAccountObservation, NativeFundingRpcFacts},
};
use crate::execution_pumpswap_accounts::{associated_token_address, system_program_id};
use crate::execution_solana_tx::PubkeyBytes;

pub(super) enum Candidate {
    Existing,
    Creation { prefunded: u64 },
}

pub(super) fn candidate(facts: &NativeFundingRpcFacts, index: usize) -> Option<Candidate> {
    candidate_in(facts.requirements(), &facts.accounts().value, index)
}
fn candidate_in(
    requirements: &crate::execution_native_funding::types::NativeFundingRequirements,
    observations: &[KeyedAccountObservation],
    index: usize,
) -> Option<Candidate> {
    let keys = &requirements.binding.accounts;
    if keys.len() != observations.len()
        || keys
            .iter()
            .zip(observations)
            .any(|(key, row)| key.pubkey != row.pubkey)
    {
        return None;
    }
    let rows = &requirements.requirements;
    let Op::AssociatedTokenCreateIdempotent {
        associated_account,
        owner,
        mint,
        token_program,
        ..
    } = &rows.get(index)?.operation
    else {
        return None;
    };
    if *token_program != program_id()
        || *associated_account != associated_token_address(owner, mint, token_program)
        || rows[..index]
            .iter()
            .any(|r| invalidates(&r.operation, associated_account, mint))
    {
        return None;
    }
    let mint_row = observed(observations, mint)?;
    let Account::Present {
        owner_program,
        executable: false,
        data,
        ..
    } = mint_row
    else {
        return None;
    };
    if owner_program != token_program {
        return None;
    }
    profile::mint(data, mint)?;
    match observed(observations, associated_account)? {
        Account::Absent => Some(Candidate::Creation { prefunded: 0 }),
        Account::Present {
            lamports,
            owner_program,
            executable: false,
            data,
        } if *owner_program == system_program_id() && data.is_empty() => {
            Some(Candidate::Creation {
                prefunded: *lamports,
            })
        }
        Account::Present {
            owner_program,
            executable: false,
            data,
            ..
        } if owner_program == token_program && profile::account(data, mint, owner) => {
            Some(Candidate::Existing)
        }
        _ => None,
    }
}
fn observed<'a>(rows: &'a [KeyedAccountObservation], key: &PubkeyBytes) -> Option<&'a Account> {
    rows.iter()
        .find(|row| row.pubkey == *key)
        .map(|row| &row.account)
}
fn invalidates(op: &Op, ata: &PubkeyBytes, mint: &PubkeyBytes) -> bool {
    let touches = |k: &PubkeyBytes| k == ata || k == mint;
    match op {
        Op::Unresolved { .. } => true,
        Op::AssociatedTokenCreateIdempotent {
            payer,
            associated_account,
            ..
        } => touches(payer) || touches(associated_account),
        Op::SystemTransfer { from, to, .. } => touches(from) || touches(to),
        Op::SyncNative { account, .. } => touches(account),
        Op::CloseTokenAccount {
            account,
            destination,
            ..
        } => touches(account) || touches(destination),
        Op::ComputeBudget => false,
    }
}

pub(crate) fn needs_rent(
    requirements: &crate::execution_native_funding::types::NativeFundingRequirements,
    rows: &[KeyedAccountObservation],
) -> bool {
    (0..requirements.requirements.len()).any(|i| {
        matches!(
            candidate_in(requirements, rows, i),
            Some(Candidate::Creation { .. })
        )
    })
}

pub(super) fn funding(facts: &ClassicAtaFundingFacts, index: usize) -> AtaFundingAmount {
    use AtaFundingAmount::{Known, Unresolved};
    if !facts.token2022_collected() {
        return Unresolved(AtaFundingIssue::UnsupportedToken2022State);
    }
    match candidate(facts.native(), index) {
        Some(Candidate::Existing) => Known(0),
        Some(Candidate::Creation { prefunded }) => match facts.token2022_rent() {
            Some(rent)
                if rent.data_length() == ACCOUNT_LENGTH
                    && rent.commitment() == "confirmed"
                    && rent.transaction_sha256()
                        == facts.native().requirements().binding.transaction_sha256 =>
            {
                Known(rent.lamports().max(1).saturating_sub(prefunded))
            }
            _ => Unresolved(AtaFundingIssue::MissingToken2022Rent),
        },
        None => Unresolved(AtaFundingIssue::UnsupportedToken2022State),
    }
}
