//! A checked anchor compares two exact source representations; neither raw is rewritten.
use anyhow::{ensure, Result};
use copybot_core_types::association_delivery::{ProtectedEvidence, ProtectedSourceKind};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{json, Value};

pub(super) fn record(db: &Connection, e: &ProtectedEvidence) -> Result<()> {
    let Some(anchor) = &e.anchor else {
        return Ok(());
    };
    ensure!(
        e.source_kind != ProtectedSourceKind::Stream
            && anchor.selected_source != ProtectedSourceKind::Stream,
        "protected anchor source invalid"
    );
    ensure!(
        anchor.other_evidence_sha256.len() == 64
            && anchor
                .other_evidence_sha256
                .bytes()
                .all(|b| b.is_ascii_hexdigit())
            && anchor.other_evidence_sha256 != e.sha256,
        "protected anchor evidence identity invalid"
    );
    ensure!(
        [
            "EXACT",
            "ESTIMATED_BLOCK_TIME_METADATA",
            "SUCCESSFUL_TERMINAL_UNWIND",
            "HTTP_COMPLETES_LIVE_PAYLOAD"
        ]
        .contains(&anchor.comparison.as_str()),
        "protected anchor comparison unsupported"
    );
    let wire = serde_json::to_string(anchor)?;
    let saved:Option<String>=db.query_row("SELECT metadata_json FROM capture_recovery_anchor_links WHERE evidence_sha256=? AND other_evidence_sha256=?",params![e.sha256,anchor.other_evidence_sha256],|r|r.get(0)).optional()?;
    if let Some(saved) = saved {
        ensure!(saved == wire, "protected anchor identity conflict");
        return Ok(());
    }
    super::recovery_write::capacity(db, wire.len() + 512, 1)?;
    db.execute(
        "INSERT INTO capture_recovery_anchor_links VALUES(?,?,?)",
        params![e.sha256, anchor.other_evidence_sha256, wire],
    )?;
    super::recovery_write::charge(db, wire.len() + 512)?;
    Ok(())
}
pub(super) fn verify(db: &Connection, e: &ProtectedEvidence) -> Result<()> {
    let Some(anchor) = &e.anchor else {
        return Ok(());
    };
    let saved:String=db.query_row("SELECT metadata_json FROM capture_recovery_anchor_links WHERE evidence_sha256=? AND other_evidence_sha256=?",params![e.sha256,anchor.other_evidence_sha256],|r|r.get(0))?;
    ensure!(
        saved == serde_json::to_string(anchor)?,
        "protected anchor commit readback mismatch"
    );
    Ok(())
}
pub(super) fn resolved(db: &Connection, hash: &str) -> Result<Vec<Value>> {
    let mut q=db.prepare("SELECT evidence_sha256,other_evidence_sha256,metadata_json FROM capture_recovery_anchor_links WHERE evidence_sha256=? OR other_evidence_sha256=? ORDER BY evidence_sha256,other_evidence_sha256")?;
    let rows = q
        .query_map([hash, hash], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    rows.into_iter().map(|(first,other,wire)| {
        let anchor:Value=serde_json::from_str(&wire)?;
        let (kind,slot):(String,String)=db.query_row("SELECT source_kind,slot FROM capture_recovery_evidence WHERE evidence_id=?",[&first],|r|Ok((r.get(0)?,r.get(1)?)))?;
        let (other_kind,other_slot):(String,String)=db.query_row("SELECT source_kind,slot FROM capture_recovery_evidence WHERE evidence_id=?",[&other],|r|Ok((r.get(0)?,r.get(1)?)))?;
        ensure!(slot==other_slot && ((kind=="complete_live_block" && other_kind=="confirmed_http") || (kind=="confirmed_http" && other_kind=="complete_live_block")),"protected anchor raw pair incomplete or conflicting");
        Ok(json!({"evidence_sha256":first,"other_evidence_sha256":other,"comparison":anchor["comparison"],"selected_source":anchor["selected_source"]}))
    }).collect()
}
pub(super) fn complete(db: &Connection) -> Result<()> {
    let missing:bool=db.query_row("SELECT EXISTS(SELECT 1 FROM capture_recovery_anchor_links a JOIN capture_recovery_evidence own ON own.evidence_id=a.evidence_sha256 LEFT JOIN capture_recovery_evidence e ON e.evidence_id=a.other_evidence_sha256 WHERE e.evidence_id IS NULL OR e.slot!=own.slot OR NOT ((own.source_kind='complete_live_block' AND e.source_kind='confirmed_http') OR (own.source_kind='confirmed_http' AND e.source_kind='complete_live_block')))",[],|r|r.get(0))?;
    ensure!(
        !missing,
        "protected anchor counterpart evidence not durable before checkpoint ACK"
    );
    Ok(())
}
