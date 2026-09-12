//! One pair cursor per successful stale-close transaction, never a SELL fan-out.
//! Mutable cursor readback stays inside IMMEDIATE: a later producer is allowed
//! to reset it immediately after commit, including before the consumer's ACK.
use super::{rows, Readback};
use crate::association_inbox::{ConsumerMode, InboxLimits};
use anyhow::{ensure, Result};
use rusqlite::{params, Connection, OptionalExtension};
#[path = "association_shadow_recovery_schema.rs"]
pub(crate) mod schema;
const NEXT: &str = "SELECT signature FROM association_sell_preparations INDEXED BY b102_shadow_sell_pair
 WHERE CASE WHEN json_valid(first_binding) THEN json_extract(first_binding,'$.sell.admission.facts.wallet') END=?1
 AND CASE WHEN json_valid(first_binding) THEN json_extract(first_binding,'$.sell.admission.facts.token_in') END=?2
 AND signature>?3 ORDER BY signature LIMIT 1";
const CURSOR: &str =
    "SELECT after_signature FROM association_shadow_sell_work WHERE wallet=?1 AND token=?2";

pub(crate) fn usage(c: &Connection) -> Result<(usize, usize)> {
    // Same 512-byte row overhead as 99; reserve the largest related signature so
    // cursor progress cannot consume unaccounted capacity. No positive intent cap
    // exemption: the existing full-domain check still owns every recovery commit.
    Ok(c.query_row("SELECT count(*),coalesce(sum(512+length(CAST(wallet AS BLOB))+length(CAST(token AS BLOB))+max(length(CAST(after_signature AS BLOB)),coalesce((SELECT length(CAST(signature AS BLOB)) FROM association_sell_preparations INDEXED BY b102_shadow_sell_pair_bytes
        WHERE CASE WHEN json_valid(first_binding) THEN json_extract(first_binding,'$.sell.admission.facts.wallet') END=w.wallet
        AND CASE WHEN json_valid(first_binding) THEN json_extract(first_binding,'$.sell.admission.facts.token_in') END=w.token
        ORDER BY length(CAST(signature AS BLOB)) DESC LIMIT 1),0))),0) FROM association_shadow_sell_work w",
        [], |r| Ok((r.get(0)?,r.get(1)?)))?)
}
pub(crate) fn pending(c: &Connection) -> Result<bool> {
    Ok(c.query_row(
        "SELECT EXISTS(SELECT 1 FROM association_shadow_sell_work)",
        [],
        |r| r.get(0),
    )?)
}
fn verify(c: &Connection, wallet: &str, token: &str, expected: Option<&str>) -> Result<()> {
    let actual: Option<String> = c
        .query_row(CURSOR, params![wallet, token], |r| r.get(0))
        .optional()?;
    ensure!(
        actual.as_deref() == expected,
        "Shadow SELL recovery cursor write ignored/changed"
    );
    Ok(())
}
pub(crate) fn enqueue(c: &Connection, wallet: &str, token: &str, l: InboxLimits) -> Result<()> {
    ensure!(
        !c.is_autocommit(),
        "Shadow recovery requires the close transaction"
    );
    schema::required(c)?;
    // Even old preparations with a missing lot origin are present in this index.
    if c.prepare(NEXT)?.exists(params![wallet, token, ""])? {
        c.execute("INSERT INTO association_shadow_sell_work(wallet,token,after_signature) VALUES(?1,?2,'')
          ON CONFLICT(wallet,token) DO UPDATE SET after_signature=''",params![wallet,token])?;
        verify(c, wallet, token, Some(""))?;
    }
    crate::association_inbox::check_budget(c, l, ConsumerMode::ProviderOrderStrictV1)
}
pub(super) fn step(c: &Connection, l: InboxLimits, proof: &mut Readback) -> Result<bool> {
    if !proof.automatic() {
        return Ok(false);
    }
    let job: Option<(String,String,String)> = c.query_row(
        "SELECT wallet,token,after_signature FROM association_shadow_sell_work ORDER BY wallet,token LIMIT 1",
        [], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
    let Some((wallet, token, after)) = job else {
        return Ok(false);
    };
    let next: Option<String> = c
        .query_row(NEXT, params![wallet, token, after], |r| r.get(0))
        .optional()?;
    if let Some(next) = next {
        // Re-read first/ownership/amount/generation/temporal risk. Unknown is an
        // evaluated attempt, so A advances and cannot hold B behind it forever.
        rows::refresh(c, &next, l, proof)?;
        c.execute("UPDATE association_shadow_sell_work SET after_signature=?3 WHERE wallet=?1 AND token=?2",
            params![wallet,token,next])?;
        verify(c, &wallet, &token, Some(&next))?;
    } else {
        c.execute(
            "DELETE FROM association_shadow_sell_work WHERE wallet=?1 AND token=?2",
            params![wallet, token],
        )?;
        verify(c, &wallet, &token, None)?;
    }
    Ok(true)
}

/// Scope FULL sync to this producer's transaction. Other main-connection writers
/// keep their previous mode. Even a restoration error cannot erase committed work.
pub(crate) fn durable_close<T>(
    c: &Connection,
    recovery: Option<InboxLimits>,
    run: impl FnOnce() -> Result<T>,
) -> Result<T> {
    let Some(l) = recovery else {
        return run();
    };
    ensure!(l.count > 0 && l.bytes > 0, "invalid Shadow recovery limits");
    schema::required(c)?;
    let previous: i64 = c.query_row("PRAGMA synchronous", [], |r| r.get(0))?;
    c.pragma_update(None, "synchronous", "FULL")?;
    let result = (|| {
        ensure!(
            c.query_row("PRAGMA synchronous", [], |r| r.get::<_, i64>(0))? == 2,
            "Shadow recovery FULL synchronous required"
        );
        run()
    })();
    let restored = c.pragma_update(None, "synchronous", previous);
    match (result, restored) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) => Err(error),
        (result, Err(error)) => Err(anyhow::anyhow!(
            "restore SQLite sync after stale close: {error}; close error: {:?}",
            result.err()
        )),
    }
}
