use super::*;
use rusqlite::{params, OptionalExtension};
const JOB: &str = "SELECT after_signature FROM association_sell_work WHERE anchor_signature=?1";
const BOOT: &str =
    "SELECT json_array(after_signature,complete) FROM association_sell_bootstrap WHERE singleton=1";
pub(super) fn initialize(c: &Connection, p: &mut Readback) -> Result<()> {
    // Reuse the already charged bootstrap row. Recovery never allocates one
    // job per unresolved anchor. Each turn revisits at most one durable identity;
    // prepare() refreshes its original binding and cannot choose a new witness.
    c.execute("INSERT INTO association_sell_bootstrap VALUES(1,'',0) ON CONFLICT(singleton) DO UPDATE SET after_signature='',complete=0", [])?;
    p.expect(c, BOOT, vec![], Some("[\"\",0]".into()))?;
    Ok(())
}
pub(super) fn enqueue(c: &Connection, key: &str, p: &mut Readback) -> Result<()> {
    if !c
        .prepare("SELECT 1 FROM association_sell_dependencies WHERE anchor_signature=?1 LIMIT 1")?
        .exists([key])?
    {
        return Ok(());
    }
    c.execute("INSERT INTO association_sell_work(anchor_signature,after_signature) VALUES(?1,'') ON CONFLICT(anchor_signature) DO UPDATE SET after_signature=''",[key])?;
    p.expect(c, JOB, vec![key.into()], Some(String::new()))
}
pub(super) fn pending(c: &Connection) -> Result<bool> {
    if parent_graph::pending(c)? {
        return Ok(true);
    }
    Ok(c.query_row("SELECT EXISTS(SELECT 1 FROM association_sell_work) OR EXISTS(SELECT 1 FROM association_sell_bootstrap WHERE complete=0)",[],|r|r.get(0))?)
}
pub(super) fn step(c: &Connection, l: InboxLimits, p: &mut Readback) -> Result<()> {
    if super::shadow_recovery::step(c, l, p)? || parent_graph::step(c, l, p)? {
        return Ok(());
    }
    // One dependent row per writer turn. Durable lexical continuation survives
    // restart/commit-before-ACK; repeated anchor evidence resets only its own key.
    let job:Option<(String,String)>=c.query_row("SELECT anchor_signature,after_signature FROM association_sell_work ORDER BY anchor_signature LIMIT 1",[],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
    if let Some((anchor, after)) = job {
        let next:Option<String>=c.query_row("SELECT sell_signature FROM association_sell_dependencies WHERE anchor_signature=?1 AND sell_signature>?2 ORDER BY sell_signature LIMIT 1",params![anchor,after],|r|r.get(0)).optional()?;
        if let Some(next) = next {
            rows::refresh(c, &next, l, p)?;
            c.execute(
                "UPDATE association_sell_work SET after_signature=?2 WHERE anchor_signature=?1",
                params![anchor, next],
            )?;
            p.expect(c, JOB, vec![anchor], Some(next))?;
        } else {
            c.execute(
                "DELETE FROM association_sell_work WHERE anchor_signature=?1",
                [&anchor],
            )?;
            p.expect(c, JOB, vec![anchor], None)?;
        }
        return Ok(());
    }
    let (after, complete): (String, bool) = c.query_row(
        "SELECT after_signature,complete FROM association_sell_bootstrap WHERE singleton=1",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    if complete {
        return Ok(());
    }
    let next:Option<String>=c.query_row("SELECT signature FROM association_inbox_identities WHERE signature>?1 ORDER BY signature LIMIT 1",[&after],|r|r.get(0)).optional()?;
    let (cursor, done) = if let Some(next) = next {
        rows::prepare(c, &next, false, None, l, p)?;
        (next, 0)
    } else {
        (after, 1)
    };
    c.execute(
        "UPDATE association_sell_bootstrap SET after_signature=?1,complete=?2 WHERE singleton=1",
        params![cursor, done],
    )?;
    p.expect(
        c,
        BOOT,
        vec![],
        Some(serde_json::to_string(&serde_json::json!([cursor, done]))?),
    )
}
