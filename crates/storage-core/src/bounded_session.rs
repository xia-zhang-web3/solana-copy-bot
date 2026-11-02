//! Explicit backend selection for the existing canonical session state/accounting.
//! LOCAL validation remains local-only. Normal grants are loaded independently.
use crate::{controlled_receipt_lot::Scope,native_buy::TechnicalCohortAuthority};
use anyhow::{ensure,Context,Result};
use chrono::{DateTime,Utc};
use rusqlite::{params,Connection,OptionalExtension};
pub(crate) use crate::local_bounded_session::{Authority,Model};
#[path="bounded_session_state.rs"]
pub(crate) mod state;
#[path="bounded_session_budget.rs"]
pub(crate) mod budget;
#[path="bounded_session_native.rs"]
pub(crate) mod native;
#[path="bounded_session_source.rs"]
pub(crate) mod source;
pub(crate) fn active(c:&Connection)->Result<bool>{
    let real=crate::real_session::active(c)?;
    let local=crate::local_bounded_session::active(c)?;
    ensure!(!(real && local),"session_mixed_authority");
    Ok(real || local)
}
pub(crate) fn load(c:&Connection)->Result<Option<Authority>>{
    active(c)?;
    if let Some(a)=crate::real_session::load(c)? {return Ok(Some(a.view()))}
    crate::local_bounded_session::load(c)
}
pub(crate) fn sql(c:&Connection,sql:&str)->Result<String>{
    active(c)?;
    Ok(if crate::real_session::active(c)? {sql.replace("local_bounded_","real_session_")} else {sql.into()})
}
pub(crate) fn bounds(c:&Connection)->Result<(u64,u64,u64)>{
    if let Some(a)=crate::real_session::load(c)? {return Ok((a.total_fee_cap,a.priority_fee_cap,a.loss_cap))}
    Ok((100_000,22_000,20_000_000))
}
pub(crate) fn native_limit(c:&Connection)->Result<u32>{
    Ok(load(c)?.map_or(1,|a|a.model.max_cycles))
}
