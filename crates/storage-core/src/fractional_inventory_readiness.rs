//! Acquisition state only. Missing parent evidence never supplies an amount or authority.
use super::Inventory;
use anyhow::Result;

pub(crate) enum Readiness {
    Proven(Inventory),
    MissingTargetParentAccount(Bank),
}
pub(crate) struct Bank {
    pub slot: u64,
    pub parent: u64,
    pub bank_hash: String,
    pub parent_hash: String,
    pub target_index: usize,
}
impl Readiness {
    pub(crate) fn bank(&self) -> Bank {
        match self {
            Self::Proven(p) => Bank {
                slot: p.slot,
                parent: p.parent,
                bank_hash: p.bank_hash.clone(),
                parent_hash: p.parent_hash.clone(),
                target_index: p.target_index,
            },
            Self::MissingTargetParentAccount(p) => Bank {
                slot: p.slot,
                parent: p.parent,
                bank_hash: p.bank_hash.clone(),
                parent_hash: p.parent_hash.clone(),
                target_index: p.target_index,
            },
        }
    }
    pub(crate) fn require_proven(self) -> Result<Inventory> {
        match self {
            Self::Proven(p) => Ok(p),
            Self::MissingTargetParentAccount(_) => anyhow::bail!("fraction_target_pre_conflict"),
        }
    }
}
