use anyhow::{Context, Result};
use std::collections::BTreeMap;

pub(crate) type PubkeyBytes = [u8; 32];

const SIGNATURE_BYTES: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SolanaAccountMeta {
    pub(crate) pubkey: PubkeyBytes,
    pub(crate) is_signer: bool,
    pub(crate) is_writable: bool,
}

impl SolanaAccountMeta {
    pub(crate) fn readonly(pubkey: PubkeyBytes) -> Self {
        Self {
            pubkey,
            is_signer: false,
            is_writable: false,
        }
    }

    pub(crate) fn writable(pubkey: PubkeyBytes) -> Self {
        Self {
            pubkey,
            is_signer: false,
            is_writable: true,
        }
    }

    pub(crate) fn signer_writable(pubkey: PubkeyBytes) -> Self {
        Self {
            pubkey,
            is_signer: true,
            is_writable: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SolanaInstruction {
    pub(crate) program_id: PubkeyBytes,
    pub(crate) accounts: Vec<SolanaAccountMeta>,
    pub(crate) data: Vec<u8>,
}

pub(crate) fn serialize_unsigned_legacy_transaction(
    payer: PubkeyBytes,
    recent_blockhash: PubkeyBytes,
    instructions: &[SolanaInstruction],
) -> Result<Vec<u8>> {
    let message = compile_legacy_message(payer, recent_blockhash, instructions)?;
    let mut transaction = Vec::with_capacity(1 + SIGNATURE_BYTES + message.len());
    encode_shortvec(1, &mut transaction)?;
    transaction.extend_from_slice(&[0_u8; SIGNATURE_BYTES]);
    transaction.extend_from_slice(&message);
    Ok(transaction)
}

fn compile_legacy_message(
    payer: PubkeyBytes,
    recent_blockhash: PubkeyBytes,
    instructions: &[SolanaInstruction],
) -> Result<Vec<u8>> {
    let account_keys = compile_account_keys(payer, instructions)?;
    let required_signers = account_keys.iter().filter(|entry| entry.is_signer).count();
    let readonly_signed = account_keys
        .iter()
        .filter(|entry| entry.is_signer && !entry.is_writable)
        .count();
    let readonly_unsigned = account_keys
        .iter()
        .filter(|entry| !entry.is_signer && !entry.is_writable)
        .count();
    if required_signers == 0 || required_signers > u8::MAX as usize {
        anyhow::bail!("legacy transaction has invalid signer count");
    }
    if readonly_signed > u8::MAX as usize {
        anyhow::bail!("legacy transaction has too many readonly signed accounts");
    }
    if readonly_unsigned > u8::MAX as usize {
        anyhow::bail!("legacy transaction has too many readonly unsigned accounts");
    }
    let key_index = account_keys
        .iter()
        .enumerate()
        .map(|(index, entry)| (entry.pubkey, index))
        .collect::<BTreeMap<_, _>>();
    let mut message = Vec::new();
    message.push(required_signers as u8);
    message.push(readonly_signed as u8);
    message.push(readonly_unsigned as u8);
    encode_shortvec(account_keys.len(), &mut message)?;
    for entry in &account_keys {
        message.extend_from_slice(&entry.pubkey);
    }
    message.extend_from_slice(&recent_blockhash);
    encode_shortvec(instructions.len(), &mut message)?;
    for instruction in instructions {
        let program_index = *key_index
            .get(&instruction.program_id)
            .context("compiled message missing instruction program id")?;
        push_u8_index(program_index, &mut message)?;
        encode_shortvec(instruction.accounts.len(), &mut message)?;
        for account in &instruction.accounts {
            let account_index = *key_index
                .get(&account.pubkey)
                .context("compiled message missing instruction account")?;
            push_u8_index(account_index, &mut message)?;
        }
        encode_shortvec(instruction.data.len(), &mut message)?;
        message.extend_from_slice(&instruction.data);
    }
    Ok(message)
}

#[derive(Debug, Clone)]
struct CompiledAccountKey {
    pubkey: PubkeyBytes,
    is_signer: bool,
    is_writable: bool,
    first_seen: usize,
}

fn compile_account_keys(
    payer: PubkeyBytes,
    instructions: &[SolanaInstruction],
) -> Result<Vec<CompiledAccountKey>> {
    let mut keys = Vec::<CompiledAccountKey>::new();
    merge_account(&mut keys, SolanaAccountMeta::signer_writable(payer), 0);
    let mut seen = 1usize;
    for instruction in instructions {
        for account in &instruction.accounts {
            merge_account(&mut keys, *account, seen);
            seen += 1;
        }
        merge_account(
            &mut keys,
            SolanaAccountMeta::readonly(instruction.program_id),
            seen,
        );
        seen += 1;
    }
    keys.sort_by_key(|entry| {
        (
            !entry.is_signer,
            !entry.is_writable,
            entry.first_seen,
            entry.pubkey,
        )
    });
    if keys.len() > u8::MAX as usize {
        anyhow::bail!("legacy transaction has too many account keys");
    }
    Ok(keys)
}

fn merge_account(keys: &mut Vec<CompiledAccountKey>, meta: SolanaAccountMeta, first_seen: usize) {
    if let Some(entry) = keys.iter_mut().find(|entry| entry.pubkey == meta.pubkey) {
        entry.is_signer |= meta.is_signer;
        entry.is_writable |= meta.is_writable;
        return;
    }
    keys.push(CompiledAccountKey {
        pubkey: meta.pubkey,
        is_signer: meta.is_signer,
        is_writable: meta.is_writable,
        first_seen,
    });
}

fn push_u8_index(index: usize, output: &mut Vec<u8>) -> Result<()> {
    if index > u8::MAX as usize {
        anyhow::bail!("legacy transaction account index overflow");
    }
    output.push(index as u8);
    Ok(())
}

pub(crate) fn encode_shortvec(mut value: usize, output: &mut Vec<u8>) -> Result<()> {
    loop {
        let mut byte = u8::try_from(value & 0x7f).context("shortvec byte overflow")?;
        value >>= 7;
        if value == 0 {
            output.push(byte);
            return Ok(());
        }
        byte |= 0x80;
        output.push(byte);
    }
}
