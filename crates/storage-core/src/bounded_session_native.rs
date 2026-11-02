//! Existing native decision engine, with mode-specific durable cycle bindings.
use super::*;
pub(crate) fn table(c:&Connection)->Result<&'static str>{
    if crate::real_session::active(c)? {return Ok("real_session_native_decisions")}
    Ok(if crate::local_bounded_session::active(c)? {"local_bounded_native_decisions"} else {"native_buy_cohort_decisions"})
}
pub(crate) fn count(c:&Connection,run:&str)->Result<u64>{
    Ok(c.query_row(&format!("SELECT count(*) FROM {} WHERE run_id=?1",table(c)?),[run],|r|r.get(0))?)
}
pub(crate) fn insert(c:&Connection,signature:&str,a:&TechnicalCohortAuthority,epoch:i64,wallet:&str,mint:&str,at:DateTime<Utc>)->Result<()>{
    if crate::real_session::active(c)? {
        source::admit(c,signature,a,epoch,at)?;
    }
    if active(c)? {
        let p=state::current(c)?;
        ensure!(p.source_buy==signature && p.state!="closed","session_native_cycle");
        c.execute(&format!("INSERT INTO {}(signature,run_id,cycle_id,fence_epoch_id,wallet,mint,admitted_at,policy_identity) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",table(c)?),
            params![signature,a.run_id,p.id,epoch,wallet,mint,at.to_rfc3339(),a.policy_identity])?;
    } else {
        c.execute("INSERT INTO native_buy_cohort_decisions(signature,run_id,fence_epoch_id,wallet,mint,admitted_at,policy_identity) VALUES(?1,?2,?3,?4,?5,?6,?7)",params![signature,a.run_id,epoch,wallet,mint,at.to_rfc3339(),a.policy_identity])?;
    }
    Ok(())
}
