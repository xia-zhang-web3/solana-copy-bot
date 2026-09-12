//! Same-transaction staging and wake-only dependencies. No new selected witness.
use super::*;
use crate::ordered_source_sell::{self, OrderedSellStage};
use rusqlite::{params, OptionalExtension};

pub(super) fn after_refresh(
    c: &Connection,
    signature: &str,
    evaluation: &Evaluation,
    limits: InboxLimits,
    proof: &mut Readback,
) -> Result<()> {
    if !proof.automatic() {
        return Ok(());
    }
    // Only origins actually read under the bounded relevant Shadow scan can
    // provide a wake key. Missing/partial origins remain Unknown, never invented.
    if let Some(shadow) = &evaluation.shadow {
        for lot in &shadow.lots {
            if let Some(origin) = &lot.origin {
                wake(c, signature, &origin.signature, proof)?;
            }
        }
    }
    let result = ordered_source_sell::stage_on_connection(c, signature, limits)?;
    if let OrderedSellStage::Inserted(intent) | OrderedSellStage::Existing(intent) = result {
        // Positive history is immutable, but its exact blobs/ownership must be
        // included with the event/preparation/cursor in pre/postcommit readback.
        proof.expect(
            c,
            "SELECT json_array(intent_id,signature,version,policy,record) FROM ordered_source_sell_intents WHERE signature=?1",
            vec![signature.into()],
            Some(serde_json::to_string(&serde_json::json!([
                intent.intent_id, signature, intent.version, intent.policy,
                serde_json::to_string(&intent)?
            ]))?),
        )?;
        proof.expect(
            c,
            "SELECT json_array(signature,owner,intent_id) FROM source_sell_signature_claims WHERE signature=?1",
            vec![signature.into()],
            Some(serde_json::to_string(&serde_json::json!([
                signature, ordered_source_sell::PROVIDER_ORDER_STRICT_V1, intent.intent_id
            ]))?),
        )?;
    }
    // Unknown/Blocked completes this attempt. Only durable continuation, a new
    // relevant provider dependency or bootstrap can schedule another attempt.
    Ok(())
}

fn wake(c: &Connection, sell: &str, anchor: &str, proof: &mut Readback) -> Result<()> {
    let old: Option<Option<String>> = c.query_row(
        "SELECT first_identity FROM association_sell_dependencies WHERE sell_signature=?1 AND anchor_signature=?2",
        params![sell, anchor], |r| r.get(0),
    ).optional()?;
    if old.is_some() {
        return Ok(());
    }
    // This key is deliberately absent from evaluate::keys(first); NULL does not
    // pin a newly selected financial witness. It only joins the existing cursor.
    c.execute(
        "INSERT INTO association_sell_dependencies(sell_signature,anchor_signature,first_identity) VALUES(?1,?2,NULL)",
        params![sell, anchor],
    )?;
    proof.expect(
        c,
        "SELECT json_array(sell_signature,anchor_signature,first_identity) FROM association_sell_dependencies WHERE sell_signature=?1 AND anchor_signature=?2",
        vec![sell.into(), anchor.into()],
        Some(serde_json::to_string(&serde_json::json!([sell, anchor, null]))?),
    )
}
