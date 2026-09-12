use crate::{
    execution_canary_fill_marker::fill_exists, ExecutionCanaryReceiptFacts,
    EXECUTION_STATUS_CANARY_CONFIRMED, EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED,
};
use anyhow::{ensure, Result};
use rusqlite::{Connection, OptionalExtension};

/// Shared durable identity rejection. SQL decoding/corruption errors remain errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiptFactsIdentityRejection {
    Missing,
    Status,
    Identity,
    Slot,
    Confirmation,
}

impl std::fmt::Display for ReceiptFactsIdentityRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Missing => "receipt facts durable order/signal/proof missing",
            Self::Status => "receipt facts require durable pending proof",
            Self::Identity => "receipt facts durable identity mismatch",
            Self::Slot => "receipt facts durable slot mismatch",
            Self::Confirmation => "receipt facts durable confirmation invalid",
        })
    }
}
impl std::error::Error for ReceiptFactsIdentityRejection {}

pub(crate) fn validate_identity(
    conn: &Connection,
    facts: &ExecutionCanaryReceiptFacts,
) -> Result<()> {
    let Some((
        status,
        order_sig,
        token,
        side,
        proof_sig,
        wallet,
        proof_token,
        proof_side,
        slot,
        confirmation,
    )) = conn
        .query_row(
            "SELECT o.status, o.tx_signature, s.token, s.side, p.tx_signature, p.wallet_pubkey,
         p.token, p.side, p.slot, p.confirmation_status FROM orders o
         JOIN copy_signals s ON s.signal_id = o.signal_id
         JOIN execution_canary_receipt_proofs p ON p.order_id = o.order_id WHERE o.order_id = ?1",
            [&facts.order_id],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, Option<String>>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, String>(4)?,
                    r.get::<_, String>(5)?,
                    r.get::<_, String>(6)?,
                    r.get::<_, String>(7)?,
                    r.get::<_, Option<String>>(8)?,
                    r.get::<_, String>(9)?,
                ))
            },
        )
        .optional()?
    else {
        return Err(ReceiptFactsIdentityRejection::Missing.into());
    };
    ensure!(
        status == EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED
            || (status == EXECUTION_STATUS_CANARY_CONFIRMED && fill_exists(conn, &facts.order_id)?),
        ReceiptFactsIdentityRejection::Status
    );
    ensure!(
        order_sig.as_deref() == Some(&facts.tx_signature)
            && proof_sig == facts.tx_signature
            && wallet == facts.wallet_pubkey
            && token == facts.token
            && proof_token == facts.token
            && side.eq_ignore_ascii_case(&facts.side)
            && proof_side == facts.side,
        ReceiptFactsIdentityRejection::Identity
    );
    ensure!(
        slot.map(|s| s.parse::<u64>())
            .transpose()?
            .is_none_or(|s| s == facts.slot),
        ReceiptFactsIdentityRejection::Slot
    );
    ensure!(
        matches!(
            confirmation.as_str(),
            "confirmed" | "finalized" | "legacy_confirmed"
        ),
        ReceiptFactsIdentityRejection::Confirmation
    );
    Ok(())
}
