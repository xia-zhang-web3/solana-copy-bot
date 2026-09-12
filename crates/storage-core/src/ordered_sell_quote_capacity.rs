//! The same bounded projection is used before a claim and after every write.
//! Pending rows reserve the largest permitted completion; no HTTP without headroom.
use super::{rows::Row, *};
use rusqlite::Connection;
const RECORD_OVERHEAD: usize = 4096;
pub(super) fn record_limit(binding: Option<&str>) -> usize {
    binding
        .map_or(0, str::len)
        .saturating_add(RECORD_OVERHEAD)
        .min(131072)
}
pub(super) fn row_bytes(id: &str, r: &Row) -> usize {
    512 + id.len()
        + r.owner.len()
        + r.binding.as_ref().map_or(0, String::len)
        + r.record
            .as_ref()
            .map_or_else(|| record_limit(r.binding.as_deref()), String::len)
}
pub(super) fn usage(c: &Connection) -> Result<(usize, usize)> {
    Ok(c.query_row("SELECT count(*),coalesce(sum(512+length(CAST(intent_id AS BLOB))+length(CAST(owner AS BLOB))+coalesce(length(CAST(binding AS BLOB)),0)+coalesce(length(CAST(record AS BLOB)),min(coalesce(length(CAST(binding AS BLOB)),0)+4096,131072))),0) FROM ordered_sell_quote_results", [], |r| Ok((r.get(0)?,r.get(1)?)))?)
}
pub(super) fn caps(l: InboxLimits, requested: QuoteCapacity) -> QuoteCapacity {
    QuoteCapacity {
        count: l.count.min(requested.count).min(4096),
        bytes: l.bytes.min(requested.bytes).min(16 << 20),
    }
}
pub(super) fn required(c: &Connection, caps: QuoteCapacity) -> Result<(usize, usize)> {
    let used = usage(c)?;
    ensure!(
        used.0 <= caps.count && used.1 <= caps.bytes,
        "strict quote existing retained state exceeds budget"
    );
    Ok(used)
}
pub(super) fn refusal(
    c: &Connection,
    caps: QuoteCapacity,
    id: &str,
    old: Option<&Row>,
    row: &Row,
) -> Result<Option<QuoteCapacityRefusal>> {
    // SQL/schema/invalid existing accounting are errors, never quota-refusal success.
    let used = required(c, caps)?;
    let count = used.0 + usize::from(old.is_none());
    let bytes = used
        .1
        .checked_sub(old.map_or(0, |r| row_bytes(id, r)))
        .context("strict quote old row accounting conflict")?
        .checked_add(row_bytes(id, row))
        .context("strict quote projected bytes overflow")?;
    let dimension = if row.record.is_none()
        && row.binding.as_ref().map_or(0, String::len) + RECORD_OVERHEAD > 131072
    {
        Some("record_bytes")
    } else if count > caps.count {
        Some("count")
    } else if bytes > caps.bytes {
        Some("bytes")
    } else {
        None
    };
    Ok(dimension.map(|dimension| QuoteCapacityRefusal {
        intent_id: id.into(),
        new_row: old.is_none(),
        dimension: dimension.into(),
        projected_count: count,
        projected_bytes: bytes,
    }))
}
pub(super) fn cursor_readback(c: &Connection, id: &str) -> Result<()> {
    ensure!(
        c.query_row(
            "SELECT intent_id FROM ordered_sell_quote_cursor WHERE singleton=1",
            [],
            |r| r.get::<_, String>(0)
        )? == id,
        "strict quote capacity cursor readback lost"
    );
    Ok(())
}
