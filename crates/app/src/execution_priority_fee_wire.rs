//! CU byte facts from the shared bounded wire parser; no native funding business gate.
use crate::execution_priority_fee::{PriorityFee, RequestedComputeUnitLimit};
use crate::execution_transaction_wire::{decode_message, DecodedMessage};
use anyhow::{bail, ensure, Context, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EncodedPriorityFee {
    pub(crate) message_sha256: String,
    pub(crate) transaction_sha256: String,
    pub(crate) limit: RequestedComputeUnitLimit,
    pub(crate) price: u64,
    pub(crate) total: u64,
}

pub(crate) fn decode_priority_fee(payload: &str) -> Result<EncodedPriorityFee> {
    Ok(decode_priority_fee_message(payload)?.1)
}

pub(crate) fn decode_priority_fee_message(
    payload: &str,
) -> Result<(DecodedMessage, EncodedPriorityFee)> {
    let budget: Vec<u8> = bs58::decode("ComputeBudget111111111111111111111111111111").into_vec()?;
    let mut limit = None;
    let mut price = None;
    let mut auxiliary = [false; 5];
    let message = decode_message(payload, |instruction| {
        if instruction.program.pubkey.as_slice() != budget {
            return Ok(());
        }
        let accounts = &instruction.accounts;
        let data = &instruction.data;
        ensure!(
            accounts.is_empty(),
            "priority_fee_budget_accounts_not_empty"
        );
        match data.first().copied() {
            Some(2) => {
                ensure!(
                    data.len() == 5 && limit.is_none(),
                    "priority_fee_duplicate_or_malformed_limit"
                );
                limit = Some(RequestedComputeUnitLimit::checked(u32::from_le_bytes(
                    data[1..].try_into()?,
                ))?);
            }
            Some(3) => {
                ensure!(
                    data.len() == 9 && price.is_none(),
                    "priority_fee_duplicate_or_malformed_price"
                );
                price = Some(u64::from_le_bytes(data[1..].try_into()?));
            }
            Some(tag @ (1 | 4)) => {
                ensure!(
                    data.len() == 5 && !auxiliary[tag as usize],
                    "priority_fee_invalid_auxiliary_budget"
                );
                auxiliary[tag as usize] = true;
                let amount = u32::from_le_bytes(data[1..].try_into()?);
                ensure!(
                    if tag == 1 {
                        (32_768..=262_144).contains(&amount) && amount % 1024 == 0
                    } else {
                        (1..=67_108_864).contains(&amount)
                    },
                    "priority_fee_invalid_auxiliary_budget"
                );
            }
            _ => bail!("priority_fee_unsupported_budget_instruction"),
        }
        Ok(())
    })?;
    let limit = limit.context("priority_fee_explicit_cu_limit_required")?;
    let price = price.context("priority_fee_explicit_cu_price_required")?;
    let total = PriorityFee::MicroLamportsPerComputeUnit(price).total(limit)?;
    let fee = EncodedPriorityFee {
        message_sha256: message.binding.message_sha256.clone(),
        transaction_sha256: message.binding.transaction_sha256.clone(),
        limit,
        price,
        total,
    };
    Ok((message, fee))
}
