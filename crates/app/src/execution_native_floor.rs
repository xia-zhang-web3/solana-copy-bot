//! Byte-bound final native floor preparation/verification. No runtime caller or reserve policy.
use crate::execution_solana_tx::{
    serialize_unsigned_legacy_transaction, PubkeyBytes, SolanaAccountMeta, SolanaInstruction,
};
use crate::execution_transaction_wire::{decode_message, MessageBinding};
use anyhow::{ensure, Result};
use base64::{engine::general_purpose::STANDARD, Engine};

const SYSTEM_PROGRAM: PubkeyBytes = [0; 32];
const MAX_ENCODED_PACKET: usize = 1644; // base64 size for the parser's 1232-byte packet cap

/// Structural evidence only: no signature, network ownership, solvency or execution proof.
/// The preparation API returns unsigned bytes; independent verification also accepts signed bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VerifiedNativeFloor {
    payload: String,
    wallet: PubkeyBytes,
    reserve_lamports: u64,
    final_instruction_index: usize,
    binding: MessageBinding,
}
impl VerifiedNativeFloor {
    pub(crate) fn payload(&self) -> &str {
        &self.payload
    }
    pub(crate) fn wallet(&self) -> PubkeyBytes {
        self.wallet
    }
    pub(crate) fn reserve_lamports(&self) -> u64 {
        self.reserve_lamports
    }
    pub(crate) fn final_instruction_index(&self) -> usize {
        self.final_instruction_index
    }
    pub(crate) fn binding(&self) -> &MessageBinding {
        &self.binding
    }
}

pub(crate) fn prepare_final_native_floor(
    wallet: PubkeyBytes,
    blockhash: PubkeyBytes,
    original: &[SolanaInstruction],
    reserve_lamports: u64,
) -> Result<VerifiedNativeFloor> {
    // Meta privileges can be promoted by the payer/other instructions during compilation.
    // Reject this exact ordinary transfer's data/operands regardless of those local flags.
    ensure!(
        !original.iter().any(|ix| {
            ix.program_id == SYSTEM_PROGRAM
                && ordinary_transfer(&ix.data)
                && ix.accounts.len() == 2
                && ix.accounts.iter().all(|a| a.pubkey == wallet)
        }),
        "native_floor_already_guarded_or_ambiguous"
    );
    let mut instructions = original.to_vec();
    instructions.push(SolanaInstruction {
        program_id: SYSTEM_PROGRAM,
        accounts: vec![
            SolanaAccountMeta::signer_writable(wallet),
            SolanaAccountMeta::writable(wallet),
        ],
        data: [
            2_u32.to_le_bytes().to_vec(),
            reserve_lamports.to_le_bytes().to_vec(),
        ]
        .concat(),
    });
    let bytes = serialize_unsigned_legacy_transaction(wallet, blockhash, &instructions)?;
    verify_final_native_floor(&STANDARD.encode(bytes), wallet, reserve_lamports)
}

/// Re-run on the actual final bytes before future signing/submission; old evidence is not authority.
pub(crate) fn verify_final_native_floor(
    payload: &str,
    expected_wallet: PubkeyBytes,
    expected_reserve_lamports: u64,
) -> Result<VerifiedNativeFloor> {
    // Reject oversized encoded input before the existing decoder allocates its base64 output.
    ensure!(
        payload.len() <= MAX_ENCODED_PACKET,
        "native_floor_payload_too_large"
    );
    let decoded = decode_message(payload, |_| Ok(()))?;
    let binding = &decoded.binding;
    ensure!(
        binding.message_bytes[0] & 0x80 == 0,
        "native_floor_unsupported_version"
    );
    ensure!(
        binding.required_signatures == 1 && binding.signature_count == 1,
        "native_floor_single_signer"
    );
    let payer = &binding.accounts[0];
    ensure!(
        payer.pubkey == expected_wallet && payer.is_signer && payer.is_writable,
        "native_floor_wallet_payer"
    );
    let last = decoded
        .instructions
        .last()
        .ok_or_else(|| anyhow::anyhow!("native_floor_missing_guard"))?;
    ensure!(
        last.program.pubkey == SYSTEM_PROGRAM,
        "native_floor_program"
    );
    ensure!(
        !last.program.is_signer && !last.program.is_writable,
        "native_floor_program_flags"
    );
    ensure!(last.accounts.len() == 2, "native_floor_operands");
    ensure!(
        last.accounts
            .iter()
            .all(|a| a.pubkey == expected_wallet && a.is_signer && a.is_writable),
        "native_floor_wallet_operands"
    );
    ensure!(
        ordinary_transfer(&last.data),
        "native_floor_transfer_encoding"
    );
    let reserve_lamports = u64::from_le_bytes(last.data[4..12].try_into()?);
    ensure!(
        reserve_lamports == expected_reserve_lamports,
        "native_floor_reserve_mismatch"
    );
    Ok(VerifiedNativeFloor {
        payload: payload.to_owned(),
        wallet: expected_wallet,
        reserve_lamports,
        final_instruction_index: last.index,
        binding: decoded.binding,
    })
}

fn ordinary_transfer(data: &[u8]) -> bool {
    data.len() == 12 && data[..4] == 2_u32.to_le_bytes()
}
