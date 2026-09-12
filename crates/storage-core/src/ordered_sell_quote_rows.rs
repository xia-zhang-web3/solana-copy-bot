use super::*;
use anyhow::ensure;
use rusqlite::{params, Connection, OptionalExtension};
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Row {
    pub attempt: i64,
    pub binding_attempt: i64,
    pub owner: String,
    pub lease: Option<String>,
    pub binding: Option<String>,
    pub record: Option<String>,
}
pub(super) fn load(c: &Connection, id: &str) -> Result<Option<Row>> {
    Ok(c.query_row("SELECT attempt,owner,lease_until,binding,record,binding_attempt FROM ordered_sell_quote_results WHERE intent_id=?1",[id],|r|Ok(Row{attempt:r.get(0)?,owner:r.get(1)?,lease:r.get(2)?,binding:r.get(3)?,record:r.get(4)?,binding_attempt:r.get(5)?})).optional()?)
}
pub(super) fn budget(c: &Connection, l: InboxLimits) -> Result<()> {
    capacity::required(c, capacity::caps(l, QuoteCapacity::PRODUCTION))?;
    Ok(())
}
pub(super) fn save(c: &Connection, id: &str, old: Option<&Row>, row: &Row) -> Result<()> {
    let n = if let Some(old) = old {
        c.execute("UPDATE ordered_sell_quote_results SET attempt=?2,owner=?3,lease_until=?4,binding=?5,record=?6,binding_attempt=?12 WHERE intent_id=?1 AND attempt=?7 AND owner=?8 AND lease_until IS ?9 AND binding IS ?10 AND record IS ?11 AND binding_attempt=?13",params![id,row.attempt,row.owner,row.lease,row.binding,row.record,old.attempt,old.owner,old.lease,old.binding,old.record,row.binding_attempt,old.binding_attempt])?
    } else {
        c.execute("INSERT INTO ordered_sell_quote_results(intent_id,attempt,owner,lease_until,binding,record,binding_attempt) VALUES(?1,?2,?3,?4,?5,?6,?7)",params![id,row.attempt,row.owner,row.lease,row.binding,row.record,row.binding_attempt])?
    };
    ensure!(
        n == 1 && load(c, id)?.as_ref() == Some(row),
        "strict quote CAS/insert/readback lost"
    );
    Ok(())
}
pub(super) fn cursor(c: &Connection, id: &str) -> Result<()> {
    let n=c.execute("INSERT INTO ordered_sell_quote_cursor(singleton,intent_id) VALUES(1,?1) ON CONFLICT(singleton) DO UPDATE SET intent_id=excluded.intent_id",[id])?;
    let actual: String = c.query_row(
        "SELECT intent_id FROM ordered_sell_quote_cursor WHERE singleton=1",
        [],
        |r| r.get(0),
    )?;
    ensure!(
        n == 1 && actual == id,
        "strict quote cursor ignored/changed"
    );
    Ok(())
}
