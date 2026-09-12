//! Reciprocal ownership under the caller's IMMEDIATE transaction.
use super::*;
use anyhow::ensure;
use rusqlite::{params, Connection, OptionalExtension};

pub(super) fn owner(c: &Connection, signature: &str) -> Result<Option<String>> {
    let row: Option<(String, String)> = c
        .query_row(
            "SELECT owner,intent_id FROM source_sell_signature_claims WHERE signature=?1",
            [signature],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    row.map(|(owner, id)| {
        ensure!(
            id == canonical(signature) && (owner == "legacy" || owner == PROVIDER_ORDER_STRICT_V1),
            "corrupt SELL claim"
        );
        Ok(owner)
    })
    .transpose()
}
pub(crate) fn legacy_exists(c: &Connection, signature: &str) -> Result<bool> {
    let id = canonical(signature);
    // Literal signature prefix plus exactly the canonical SELL suffix grammar.
    // BUY/noncanonical signals must not reserve an unrelated SELL identity.
    let prefix = format!("shadow:{signature}:");
    for (sql, args) in [
        ("SELECT 1 FROM execution_source_sell_intents WHERE intent_id=?1 OR event_signature=?2 LIMIT 1", vec![id.as_str(), signature]),
        ("SELECT 1 FROM execution_source_sell_promotions WHERE intent_id=?1 OR (substr(signal_id,1,length(?2))=?2 AND signal_id GLOB 'shadow:?*:?*:sell:?*' AND length(signal_id)-length(replace(signal_id,':',''))=4) LIMIT 1", vec![id.as_str(), prefix.as_str()]),
        ("SELECT 1 FROM copy_signals WHERE substr(signal_id,1,length(?1))=?1 AND signal_id GLOB 'shadow:?*:?*:sell:?*' AND length(signal_id)-length(replace(signal_id,':',''))=4 LIMIT 1", vec![prefix.as_str()]),
        ("SELECT 1 FROM orders WHERE substr(signal_id,1,length(?1))=?1 AND signal_id GLOB 'shadow:?*:?*:sell:?*' AND length(signal_id)-length(replace(signal_id,':',''))=4 LIMIT 1", vec![prefix.as_str()]),
    ] {
        if c.prepare(sql)?.exists(rusqlite::params_from_iter(args))? { return Ok(true); }
    }
    Ok(false)
}
pub(super) fn claim(c: &Connection, signature: &str) -> Result<()> {
    ensure!(owner(c, signature)?.is_none(), "SELL already owned");
    let changed = c.execute(
        "INSERT INTO source_sell_signature_claims(signature,owner,intent_id) VALUES (?1,?2,?3)",
        params![signature, PROVIDER_ORDER_STRICT_V1, canonical(signature)],
    )?;
    ensure!(
        changed == 1 && owner(c, signature)?.as_deref() == Some(PROVIDER_ORDER_STRICT_V1),
        "SELL claim insert ignored/changed"
    );
    Ok(())
}
/// Existing legacy semantics on old schema. A new-mode claim is an immutable
/// staged-event conflict; this does not alter legacy time/amount predicates.
pub(crate) fn blocks_legacy(c: &Connection, signature: &str) -> Result<bool> {
    if !schema::check_if_applied(c)? {
        return Ok(false);
    }
    Ok(owner(c, signature)?.is_some_and(|o| o != "legacy")
        || c.prepare("SELECT 1 FROM ordered_source_sell_intents WHERE signature=?1")?
            .exists([signature])?)
}
