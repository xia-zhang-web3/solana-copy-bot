//! Single bounded legacy/v0 (no ALT) wire parser. Prefixes retain the existing fee contract.
//! Signatures are skipped, not cryptographically verified. Unsigned placeholders are accepted.
use crate::execution_solana_tx::{PubkeyBytes, SolanaAccountMeta};
use anyhow::{bail, ensure, Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MessageBinding {
    pub(crate) message_bytes: Vec<u8>,
    pub(crate) message_sha256: String,
    pub(crate) transaction_sha256: String,
    pub(crate) signature_count: usize,
    pub(crate) required_signatures: usize,
    pub(crate) readonly_signed: usize,
    pub(crate) readonly_unsigned: usize,
    pub(crate) accounts: Vec<SolanaAccountMeta>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DecodedInstruction {
    pub(crate) index: usize,
    pub(crate) program_index: u8,
    pub(crate) program: SolanaAccountMeta,
    pub(crate) account_indices: Vec<u8>,
    pub(crate) accounts: Vec<SolanaAccountMeta>,
    pub(crate) data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DecodedMessage {
    pub(crate) recent_blockhash: PubkeyBytes,
    pub(crate) binding: MessageBinding,
    pub(crate) instructions: Vec<DecodedInstruction>,
}

// The visitor runs at the original instruction boundary. Fee validation must precede
// later wire/ALT/trailing errors, exactly as it did before extraction.
pub(crate) fn decode_message(
    payload: &str,
    mut visit: impl FnMut(&DecodedInstruction) -> Result<()>,
) -> Result<DecodedMessage> {
    let bytes = STANDARD
        .decode(payload)
        .context("priority_fee_invalid_base64")?;
    ensure!(bytes.len() <= 1232, "priority_fee_transaction_too_large");
    let mut wire = Wire {
        bytes: &bytes,
        offset: 0,
    };
    let signatures = wire.shortvec()?;
    ensure!(signatures > 0, "priority_fee_missing_signatures");
    wire.take(
        signatures
            .checked_mul(64)
            .context("priority_fee_length_overflow")?,
    )?;
    let message_start = wire.offset;
    let first = wire.byte()?;
    let versioned = first & 0x80 != 0;
    let required_signatures = if versioned {
        ensure!(first == 0x80, "priority_fee_unsupported_message_version");
        usize::from(wire.byte()?)
    } else {
        usize::from(first)
    };
    let readonly_signed = usize::from(wire.byte()?);
    let readonly_unsigned = usize::from(wire.byte()?);
    let key_count = wire.shortvec()?;
    ensure!(
        key_count <= 256
            && signatures == required_signatures
            && signatures <= key_count
            && readonly_signed < signatures
            && readonly_unsigned <= key_count - signatures,
        "priority_fee_invalid_header"
    );
    let mut keys = Vec::with_capacity(key_count);
    for _ in 0..key_count {
        let key: PubkeyBytes = wire.take(32)?.try_into()?;
        ensure!(!keys.contains(&key), "priority_fee_duplicate_account_key");
        keys.push(key);
    }
    let recent_blockhash = wire.take(32)?.try_into()?;
    let accounts: Vec<_> = keys
        .into_iter()
        .enumerate()
        .map(|(index, pubkey)| {
            let is_signer = index < required_signatures;
            SolanaAccountMeta {
                pubkey,
                is_signer,
                is_writable: if is_signer {
                    index < required_signatures - readonly_signed
                } else {
                    index < key_count - readonly_unsigned
                },
            }
        })
        .collect();

    let mut instructions = Vec::new();
    for index in 0..wire.shortvec()? {
        let program_index = wire.byte()?;
        let program = usize::from(program_index);
        ensure!(
            program > 0 && program < key_count,
            "priority_fee_unresolved_program_index"
        );
        let account_count = wire.shortvec()?;
        let indices = wire.take(account_count)?;
        ensure!(
            indices.iter().all(|i| usize::from(*i) < key_count),
            "priority_fee_unresolved_account_index"
        );
        let data_count = wire.shortvec()?;
        let data = wire.take(data_count)?;
        let instruction = DecodedInstruction {
            index,
            program_index,
            program: accounts[program],
            account_indices: indices.to_vec(),
            accounts: indices.iter().map(|i| accounts[usize::from(*i)]).collect(),
            data: data.to_vec(),
        };
        visit(&instruction)?;
        instructions.push(instruction);
    }
    if versioned {
        ensure!(
            wire.shortvec()? == 0,
            "priority_fee_unresolved_address_lookup_table"
        );
    }
    ensure!(
        wire.offset == bytes.len(),
        "priority_fee_trailing_wire_data"
    );
    Ok(DecodedMessage {
        recent_blockhash,
        binding: MessageBinding {
            message_bytes: bytes[message_start..].to_vec(),
            message_sha256: format!("{:x}", Sha256::digest(&bytes[message_start..])),
            transaction_sha256: format!("{:x}", Sha256::digest(&bytes)),
            signature_count: signatures,
            required_signatures,
            readonly_signed,
            readonly_unsigned,
            accounts,
        },
        instructions,
    })
}

struct Wire<'a> {
    bytes: &'a [u8],
    offset: usize,
}
impl<'a> Wire<'a> {
    fn take(&mut self, count: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(count)
            .context("priority_fee_length_overflow")?;
        let value = self
            .bytes
            .get(self.offset..end)
            .context("priority_fee_truncated_wire")?;
        self.offset = end;
        Ok(value)
    }
    fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn shortvec(&mut self) -> Result<usize> {
        let mut value = 0usize;
        for shift in [0, 7, 14] {
            let byte = self.byte()?;
            ensure!(shift != 14 || byte <= 3, "priority_fee_invalid_shortvec");
            value |= usize::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                ensure!(
                    shift == 0 || byte != 0,
                    "priority_fee_noncanonical_shortvec"
                );
                return Ok(value);
            }
        }
        bail!("priority_fee_invalid_shortvec")
    }
}
