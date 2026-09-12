//! Solana PDA contract: seeds || canonical bump || program || marker, off-curve.
use super::{ATA, TOKEN};
use curve25519_dalek::edwards::CompressedEdwardsY;
use sha2::{Digest, Sha256};

pub(super) fn associated(owner: &str, mint: &str) -> Option<String> {
    let decode = |s: &str| -> Option<[u8; 32]> { bs58::decode(s).into_vec().ok()?.try_into().ok() };
    let (owner, token, mint, program) =
        (decode(owner)?, decode(TOKEN)?, decode(mint)?, decode(ATA)?);
    // Matches cached solana-pubkey 2.1.15 try_find_program_address (255..=1).
    for bump in (1..=u8::MAX).rev() {
        let mut h = Sha256::new();
        h.update(owner);
        h.update(token);
        h.update(mint);
        h.update([bump]);
        h.update(program);
        h.update(b"ProgramDerivedAddress");
        let bytes: [u8; 32] = h.finalize().into();
        if CompressedEdwardsY(bytes).decompress().is_none() {
            return Some(bs58::encode(bytes).into_string());
        }
    }
    None
}
