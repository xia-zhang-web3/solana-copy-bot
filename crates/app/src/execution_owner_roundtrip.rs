//! Execution-only authority: one daemon BUY, then its exact receipt-bound owner EXIT.
use crate::execution_canary_state_machine::ExecutionCanaryStateMachineSummary;
use crate::execution_submit_adapter::JupiterMetisDryRunExecutionAdapter;
use anyhow::{ensure,Context,Result};
use chrono::{DateTime,Utc};
use copybot_config::{ExecutionConfig,OwnerExitConfig,OWNER_ROUNDTRIP_EXIT_V1};
use copybot_storage_core::{owner_technical_buy_order_id,SqliteStore};

pub(crate) async fn tick(c: &ExecutionConfig,s: &SqliteStore,now: DateTime<Utc>) -> Result<ExecutionCanaryStateMachineSummary> {
    let buy=JupiterMetisDryRunExecutionAdapter::new(c.clone());
    ensure!(copybot_config::owner_execution_roundtrip(c),"owner_roundtrip_policy");
    let summary=crate::execution_owner_buy::tick_with_adapter(c,s,now,&buy).await?;
    let Some(exit_config)=exit_config(c,s)? else {return Ok(summary)};
    let adapter=JupiterMetisDryRunExecutionAdapter::new(exit_config.clone());
    let exit=crate::execution_owner_exit::tick_with_adapter(&exit_config,s,now,&adapter).await?;
    if exit.candidates > 0 || exit.existing > 0 || exit.reserved > 0 || exit.skipped_reason.is_some() {Ok(exit)} else {Ok(summary)}
}
/// The config is generated only from the canonical accounted BUY, never a quote
/// estimate, leader observation, helper input, or preexisting wallet inventory.
pub(crate) fn exit_config(c: &ExecutionConfig,s: &SqliteStore) -> Result<Option<ExecutionConfig>> {
    let p=c.owner_technical_buy.as_ref().context("owner_roundtrip_config_missing")?;
    let id=format!("execution-only:{}",p.intent_id);
    let existing=s.load_owner_exit_intent(&id)?;
    let intent=if let Some(i)=existing {i} else {
        if !p.activate {return Ok(None)}
        let buy=owner_technical_buy_order_id(&p.intent_id);
        let Some(order)=s.load_execution_canary_order(&buy)? else {return Ok(None)};
        if order.status != copybot_storage_core::EXECUTION_STATUS_CANARY_CONFIRMED
            || !s.execution_canary_fill_exists(&buy)? {return Ok(None)}
        if !crate::execution_owner_source_gate::ready(c,s,Utc::now())? {return Ok(None)}
        let facts=s.load_execution_canary_receipt_facts(&buy)?.context("owner_roundtrip_receipt_missing")?;
        let raw=facts.token_delta.context("owner_roundtrip_quantity_missing")?;
        ensure!(facts.wallet_pubkey == p.wallet_pubkey && facts.token == p.mint && facts.side == "buy"
            && raw.raw > 0 && raw.decimals == 6 && order.tx_signature.as_deref() == Some(facts.tx_signature.as_str()),
            "owner_roundtrip_receipt_identity");
        let position=s.load_execution_canary_open_position(&p.mint)?.context("owner_roundtrip_position_missing")?;
        ensure!(position.position_id == format!("exec-canary-pos:{buy}")
            && position.qty_exact.is_some_and(|q|i128::from(q.raw()) == raw.raw && q.decimals() == raw.decimals),
            "owner_roundtrip_position_changed");
        let cfg=OwnerExitConfig { source_recovery_parent_intent_id: None,
            policy:OWNER_ROUNDTRIP_EXIT_V1.into(),activate:true,run_id:p.run_id.clone(),intent_id:id,
            buy_order_id:buy,buy_receipt_signature:facts.tx_signature,position_id:position.position_id,
            wallet_pubkey:p.wallet_pubkey.clone(),signer_pubkey:p.signer_pubkey.clone(),genesis_hash:p.genesis_hash.clone(),
            mint:p.mint.clone(),amount_raw:u64::try_from(raw.raw)?,decimals:raw.decimals,route:p.route.clone(),
            activated_at:p.activated_at.clone(),expires_at:p.expires_at.clone(),
            max_priority_fee_lamports:p.max_priority_fee_lamports.min(22_000),min_reserve_lamports:p.min_reserve_lamports,
            max_slippage_bps:p.max_slippage_bps,max_daily_loss_lamports:p.max_daily_loss_lamports,
        };
        let mut derived=c.clone();derived.owner_technical_buy.as_mut().unwrap().activate=false;
        derived.canary_entry_submit_enabled=false;derived.owner_exit=Some(cfg);
        crate::execution_owner_exit_authority::configured(&derived)?.context("owner_roundtrip_exit_missing")?
    };
    // Stored identity remains usable for read-only recovery even after position
    // closure or authority expiration. It cannot acquire a second dispatch slot.
    let mut derived=c.clone();derived.owner_technical_buy.as_mut().unwrap().activate=false;
    derived.canary_entry_submit_enabled=false;
    derived.owner_exit=Some(OwnerExitConfig { source_recovery_parent_intent_id: None,
        policy:OWNER_ROUNDTRIP_EXIT_V1.into(),activate:p.activate,run_id:intent.run_id,intent_id:intent.intent_id,
        buy_order_id:intent.buy_order_id,buy_receipt_signature:intent.buy_receipt_signature,position_id:intent.position_id,
        wallet_pubkey:intent.wallet,signer_pubkey:intent.signer,genesis_hash:intent.genesis_hash,mint:intent.mint,
        amount_raw:intent.amount_raw,decimals:intent.decimals,route:intent.route,
        activated_at:p.activated_at.clone(),expires_at:p.expires_at.clone(),
        max_priority_fee_lamports:intent.max_priority_fee_lamports,min_reserve_lamports:intent.min_reserve_lamports,
        max_slippage_bps:u64::from(intent.max_slippage_bps),max_daily_loss_lamports:intent.max_daily_loss_lamports,
    });
    Ok(Some(derived))
}
