//! Select a validated explicit daemon mode without creating ingestion services.
use crate::{ExecutionConfig, OWNER_BOT_RECEIPT_EXIT_V1};
use anyhow::Result;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionOnlyMode {
    OwnerRoundtrip,
    BotReceiptExit,
}

/// Presence of a receipt-exit config never supplies activation or financial authority.
pub fn execution_only_mode(e: &ExecutionConfig) -> Result<Option<ExecutionOnlyMode>> {
    if let Some(exit) = &e.owner_exit {
        if exit.policy == OWNER_BOT_RECEIPT_EXIT_V1 {
            crate::validate_owner_exit(e)?;
            // validate_owner_exit also proves absent BUY parent, inactive old scopes,
            // exact receipt scope and the existing fee/loss/execution flag contract.
            return Ok(Some(ExecutionOnlyMode::BotReceiptExit));
        }
    }
    Ok(crate::owner_execution_roundtrip(e).then_some(ExecutionOnlyMode::OwnerRoundtrip))
}
