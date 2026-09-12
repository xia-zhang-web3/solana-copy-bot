//! Provider header observations are separate from full-Info transaction assertions.
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct BlockKey {
    pub slot: u64,
    pub hash: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ParentIssue {
    ZeroChildSlot,
    MissingChildHash,
    MalformedChildHash,
    MissingParentHash,
    MalformedParentHash,
    NondecreasingParentSlot,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParentObservation {
    pub child: BlockKey,
    pub parent: BlockKey,
    pub issue: Option<ParentIssue>,
}
impl ParentObservation {
    /// Rechecked at the storage boundary; a serialized validity tag is not proof.
    pub fn expected_issue(&self) -> Option<ParentIssue> {
        issue(&self.child, &self.parent, valid_hash)
    }
}
pub fn issue(
    child: &BlockKey,
    parent: &BlockKey,
    valid: impl Fn(&str) -> bool,
) -> Option<ParentIssue> {
    if child.slot == 0 {
        return Some(ParentIssue::ZeroChildSlot);
    }
    if child.hash.is_empty() {
        return Some(ParentIssue::MissingChildHash);
    }
    if !valid(&child.hash) {
        return Some(ParentIssue::MalformedChildHash);
    }
    if parent.hash.is_empty() {
        return Some(ParentIssue::MissingParentHash);
    }
    if !valid(&parent.hash) {
        return Some(ParentIssue::MalformedParentHash);
    }
    if parent.slot >= child.slot {
        return Some(ParentIssue::NondecreasingParentSlot);
    }
    None
}
/// Fixed 32-byte base58 boundary check without adding a codec dependency to storage.
/// Ingestion still uses its existing bs58 validator; parity is tested separately.
pub fn valid_hash(hash: &str) -> bool {
    const ALPHABET: &[u8] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
    if !(32..=44).contains(&hash.len()) {
        return false;
    }
    let mut bytes = [0u8; 32];
    for ch in hash.bytes() {
        let Some(digit) = ALPHABET.iter().position(|x| *x == ch) else {
            return false;
        };
        let mut carry = digit as u32;
        for byte in bytes.iter_mut().rev() {
            carry += u32::from(*byte) * 58;
            *byte = carry as u8;
            carry >>= 8;
        }
        if carry != 0 {
            return false;
        }
    }
    hash.bytes().take_while(|x| *x == b'1').count() == bytes.iter().take_while(|x| **x == 0).count()
}
