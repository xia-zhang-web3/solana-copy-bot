use crate::ExecutionCanaryReceiptFacts;
use anyhow::Result;
use rusqlite::{params, Connection, OptionalExtension};

/// The signed compute-budget proof is bound to the dispatched message and
/// transaction hashes; the successful receipt supplies its fee and signature.
/// An incomplete or mismatched proof remains unknown (`None`). The caller must
/// supply the fee from the bound successful receipt, never a quote or hint.
pub fn confirmed_priority_fee(
    conn: &Connection,
    facts: &ExecutionCanaryReceiptFacts,
    total_fee: Option<&str>,
) -> Result<Option<u64>> {
    let Some(total_fee) = total_fee.and_then(|v| v.parse::<u64>().ok()) else {
        return Ok(None);
    };
    let row: Option<(Option<String>, String, String, String)> = conn
        .query_row(
            "SELECT m.priority_fee_json,d.message_sha256,d.transaction_sha256,d.tx_signature
         FROM execution_canary_build_plan_metadata m
         JOIN execution_canary_dispatch d ON d.order_id=m.order_id
         WHERE m.order_id=?1",
            [&facts.order_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .optional()?;
    let Some((Some(json), message_hash, transaction_hash, signature)) = row else {
        return Ok(None);
    };
    if signature != facts.tx_signature {
        return Ok(None);
    }
    let Ok(json) = serde_json::from_str::<serde_json::Value>(&json) else {
        return Ok(None);
    };
    let proof = &json["fee_proof"];
    let (Some(message), Some(transaction), Some(limit), Some(price), Some(priority)) = (
        proof["message_sha256"].as_str(),
        proof["transaction_sha256"].as_str(),
        proof["requested_compute_unit_limit"].as_u64(),
        proof["micro_lamports_per_compute_unit"].as_u64(),
        proof["total_priority_fee_lamports"].as_u64(),
    ) else {
        return Ok(None);
    };
    let version = proof["version"].as_u64();
    let reserved: Option<u64> = if facts.side == "buy" {
        conn.query_row(
            "SELECT priority_fee FROM execution_tiny_reservations
            WHERE order_id=?1 AND tx_signature=?2 AND wallet=?3 AND side='buy'",
            params![facts.order_id, facts.tx_signature, facts.wallet_pubkey],
            |r| r.get(0),
        )
        .optional()?
    } else {
        conn.query_row(
            "SELECT priority_fee FROM owner_exit_fee_reservations
            WHERE order_id=?1 AND tx_signature=?2 AND wallet=?3",
            params![facts.order_id, facts.tx_signature, facts.wallet_pubkey],
            |r| r.get(0),
        )
        .optional()?
    };
    let expected = u128::from(limit)
        .checked_mul(u128::from(price))
        .and_then(|value| value.checked_add(999_999))
        .map(|value| value / 1_000_000);
    Ok((message == message_hash
        && transaction == transaction_hash
        && version == Some(1)
        && (1..=1_400_000).contains(&limit)
        && expected == Some(u128::from(priority))
        && reserved == Some(priority)
        && priority <= total_fee)
        .then_some(priority))
}
