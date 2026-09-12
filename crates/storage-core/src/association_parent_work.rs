use super::*;
const JOB: &str =
    "SELECT json_array(after_signature,pending) FROM association_parent_work WHERE block_hash=?1";
const DEP: &str="SELECT sell_signature FROM association_parent_dependencies WHERE sell_signature=?1 AND block_hash=?2";
/// Reserve a durable work row at first dependency, before ACK. Completed jobs
/// retain their reservation; restart and later headers cannot allocate new jobs.
pub(in crate::association_sell_preparation) fn bind(
    c: &Connection,
    sell: &str,
    hashes: &[String],
    p: &mut Readback,
) -> Result<()> {
    for hash in hashes {
        c.execute(
            "INSERT OR IGNORE INTO association_parent_dependencies VALUES(?1,?2)",
            params![sell, hash],
        )?;
        p.expect(c, DEP, vec![sell.into(), hash.clone()], Some(sell.into()))?;
        let old: Option<String> = c.query_row(JOB, [hash], |r| r.get(0)).optional()?;
        if old.is_none() {
            c.execute(
                "INSERT INTO association_parent_work VALUES(?1,'',0)",
                [hash],
            )?;
        }
        p.expect(
            c,
            JOB,
            vec![hash.clone()],
            Some(old.unwrap_or_else(|| "[\"\",0]".into())),
        )?;
    }
    Ok(())
}
pub(in crate::association_sell_preparation) fn enqueue(
    c: &Connection,
    hash: &str,
    p: &mut Readback,
) -> Result<()> {
    if !c
        .prepare("SELECT 1 FROM association_parent_work WHERE block_hash=?1")?
        .exists([hash])?
    {
        return Ok(());
    }
    c.execute(
        "UPDATE association_parent_work SET after_signature='',pending=1 WHERE block_hash=?1",
        [hash],
    )?;
    p.expect(c, JOB, vec![hash.into()], Some("[\"\",1]".into()))
}
pub(in crate::association_sell_preparation) fn pending(c: &Connection) -> Result<bool> {
    Ok(c.query_row(
        "SELECT EXISTS(SELECT 1 FROM association_parent_work WHERE pending=1)",
        [],
        |r| r.get(0),
    )?)
}
pub(in crate::association_sell_preparation) fn step(
    c: &Connection,
    l: InboxLimits,
    p: &mut Readback,
) -> Result<bool> {
    let job:Option<(String,String)>=c.query_row("SELECT block_hash,after_signature FROM association_parent_work WHERE pending=1 ORDER BY block_hash LIMIT 1",[],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
    let Some((hash, after)) = job else {
        return Ok(false);
    };
    let next:Option<String>=c.query_row("SELECT sell_signature FROM association_parent_dependencies WHERE block_hash=?1 AND sell_signature>?2 ORDER BY sell_signature LIMIT 1",params![hash,after],|r|r.get(0)).optional()?;
    let (after, pending) = if let Some(next) = next {
        rows::refresh(c, &next, l, p)?;
        (next, 1)
    } else {
        (after, 0)
    };
    c.execute(
        "UPDATE association_parent_work SET after_signature=?2,pending=?3 WHERE block_hash=?1",
        params![hash, after, pending],
    )?;
    p.expect(
        c,
        JOB,
        vec![hash],
        Some(serde_json::to_string(&serde_json::json!([after, pending]))?),
    )?;
    Ok(true)
}
