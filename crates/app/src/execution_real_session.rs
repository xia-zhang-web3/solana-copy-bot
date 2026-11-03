//! Normal grant registration and dynamic scope. No LOCAL mode or future signatures.
#[path="execution_real_source_receipt.rs"]
pub(crate) mod receipt;
use anyhow::{ensure,Context,Result};
use copybot_config::ExecutionConfig;
use copybot_storage_core::{controlled_receipt_lot::Scope,real_session::Authority,SqliteStore};
pub(crate) fn authority(c:&ExecutionConfig)->Result<Option<Authority>>{
    copybot_config::validate_real_session(c)?;
    let Some(p)=&c.real_session else{return Ok(None)};
    let scope=c.controlled_receipt_lot.as_ref().context("real_session_scope")?;
    Ok(Some(Authority{policy:p.policy.clone(),cohort:crate::execution_technical_cohort::authority(c)?.context("real_session_cohort")?,
        scope:Scope{policy:scope.policy.clone(),source_run_id:scope.source_run_id.clone(),bot_run_id:scope.bot_run_id.clone(),source_wallet:scope.source_wallet.clone(),bot_wallet:scope.bot_wallet.clone(),source_buy_intent_id:scope.source_buy_intent_id.clone(),source_state_db_path:scope.source_state_db_path.clone()},
        genesis:p.genesis.clone(),mint:p.mint.clone(),route:p.route.clone(),max_cycles:p.max_cycles,buy_lamports:p.buy_lamports,
        gross_buy_cap:p.gross_buy_cap,fee_cap:p.fee_cap,loss_cap:p.loss_cap,total_fee_cap:p.total_fee_cap,
        priority_fee_cap:p.priority_fee_cap,native_floor:p.native_floor,personal_usdc_raw:p.personal_usdc_raw,slippage_bps:p.slippage_bps}))
}
pub(crate) fn register(c:&ExecutionConfig,s:&SqliteStore)->Result<()>{
    if let Some(a)=authority(c)? {s.register_real_session(&a,chrono::Utc::now())?}
    else if let Some(scope)=s.real_session_scope()? {
        // A receipt exit consumes an existing cycle; it never registers or
        // refreshes its expired normal-session BUY grant. Authority/receipt,
        // position and fee checks remain in the ordinary owner EXIT path.
        ensure!(copybot_config::execution_only_mode(c)?
            == Some(copybot_config::ExecutionOnlyMode::BotReceiptExit)
            && crate::execution_controlled_receipt_lot::scope(c)?.as_ref()==Some(&scope),
            "real_session_profile_required");
    }
    Ok(())
}
pub(crate) fn scope(c:&ExecutionConfig)->Result<Option<Scope>>{
    let Some(a)=authority(c)? else{return Ok(None)};
    let db=SqliteStore::open_read_only(&a.scope.source_state_db_path)?;
    Ok(Some(db.real_session_scope()?.unwrap_or(a.scope)))
}
pub(crate) fn source_buy(c:&ExecutionConfig,signature:&str,slot:u64,wallet:&str,mint:&str)->Result<bool>{
    let Some(scope)=scope(c)? else{return Ok(false)};
    let db=SqliteStore::open_read_only(&scope.source_state_db_path)?;
    let event=db.real_source_event(signature)?;
    ensure!(event.facts.slot==slot && event.facts.wallet==wallet && wallet==scope.source_wallet
        && event.facts.token_out==mint && mint==copybot_storage_core::OWNER_EXIT_MINT,"real_session_source_buy");
    Ok(true)
}
