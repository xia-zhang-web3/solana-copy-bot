use anyhow::{ensure, Result};
use rusqlite::{params, Connection, OptionalExtension};

pub(super) fn read(db: &Connection, id: i64, hash: &str) -> Result<Option<(String, String)>> {
    Ok(db.query_row("SELECT source_kind,provenance_json FROM capture_recovery_links WHERE recovery_id=? AND evidence_id=?",params![id,hash],|r|Ok((r.get(0)?,r.get(1)?))).optional()?)
}
pub(super) fn verify(
    db: &Connection,
    id: i64,
    hash: &str,
    expected: &(String, String),
) -> Result<()> {
    ensure!(
        read(db, id, hash)?.as_ref() == Some(expected),
        "protected identity/evidence link commit readback mismatch"
    );
    Ok(())
}
pub(super) fn complete(db: &Connection) -> Result<()> {
    let missing:bool=db.query_row("SELECT EXISTS(SELECT 1 FROM capture_recovery r LEFT JOIN capture_recovery_links l ON l.recovery_id=r.id AND l.evidence_id=r.evidence_id WHERE l.recovery_id IS NULL)",[],|r|r.get(0))?;
    ensure!(
        !missing,
        "protected identity/evidence handoff unfinished before checkpoint ACK"
    );
    Ok(())
}
