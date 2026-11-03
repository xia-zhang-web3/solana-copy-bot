//! Durable claim ordering on disposable stopped operands; no signing or transport.
#[path = "common/owned_expiry_fixture.rs"]
mod fixture;
use anyhow::Result;
use chrono::Duration;
use copybot_storage_core::{
    ordered_sell_quote::QuoteClaimStep, rpc_owned_sell_handoff::dispatch::Prepared,
    ExecutionCanaryDispatch, ExecutionDispatchClaim, TinyBudgetClaim,
};
fn operands(p: &Prepared) -> (ExecutionCanaryDispatch, TinyBudgetClaim) {
    let signature = "LOCAL_NO_SEND_SIGNATURE".to_string();
    let d = ExecutionCanaryDispatch {
        order_id: p.order_id(),
        signal_id: p.handoff.intent_id.clone(),
        client_order_id: p.handoff.owner.clone(),
        route: "jupiter_swap_instructions".into(),
        attempt: 1,
        wallet: p.handoff.wallet.clone(),
        token: p.handoff.snapshot.quote.mint.clone(),
        side: "sell".into(),
        tx_signature: signature.clone(),
        transaction_sha256: "a".repeat(64),
        message_sha256: p.message_sha256.clone(),
    };
    let b = TinyBudgetClaim {
        experiment_id: p.handoff.experiment_id.clone(),
        wallet: p.handoff.wallet.clone(),
        tx_signature: signature,
        transaction_sha256: d.transaction_sha256.clone(),
        message_sha256: p.message_sha256.clone(),
        buy_lamports: Some(0),
        protected_capital: None,
        total_fee: p.total_fee,
        priority_fee: p.priority_fee,
        fee_slot: p.handoff.snapshot.sell.facts.slot + 1,
    };
    (d, b)
}
#[test]
#[ignore = "requires pinned stopped live snapshot; set COPYBOT_EXPIRED_SELL_DB"]
fn dispatch_winner_is_unknown_and_reconciliation_only_after_reopen() -> Result<()> {
    let f = fixture::open()?;
    let (d, b) = operands(&f.old);
    let now = f.old.handoff.deadline - Duration::milliseconds(1);
    assert_eq!(
        f.store
            .claim_owned_sell_dispatch(&f.old, &d, &b, || Ok(now))?,
        ExecutionDispatchClaim::New
    );
    assert_eq!(
        f.scalar("SELECT count(*) FROM execution_canary_unresolved_dispatch")?,
        1
    );
    let reopened = copybot_storage_core::SqliteStore::open(&f.path)?;
    assert_eq!(
        reopened.claim_owned_sell_dispatch(&f.old, &d, &b, || Ok(f.after()))?,
        ExecutionDispatchClaim::Existing
    );
    assert!(matches!(f.claim(f.after())?, QuoteClaimStep::Skipped));
    assert_eq!(
        f.scalar("SELECT count(*) FROM owned_sell_quote_expiry_recoveries")?,
        0
    );
    assert_eq!(
        f.scalar("SELECT count(*) FROM rpc_owned_sell_dispatches")?,
        1
    );
    assert_eq!(reopened.owned_sell_dispatch_ids(4)?, vec![d.order_id]);
    Ok(())
}
#[test]
#[ignore = "requires pinned stopped live snapshot; set COPYBOT_EXPIRED_SELL_DB"]
fn replacement_winner_invalidates_old_signed_claim_even_with_old_valid_clock() -> Result<()> {
    let f = fixture::open()?;
    let stale_writer = copybot_storage_core::SqliteStore::open(&f.path)?;
    let (d, b) = operands(&f.old);
    f.recover()?;
    let now = f.old.handoff.deadline - Duration::milliseconds(1);
    let error = stale_writer
        .claim_owned_sell_dispatch(&f.old, &d, &b, || Ok(now))
        .unwrap_err();
    assert!(format!("{error:#}").contains("owned_sell_prepared_changed"));
    assert_eq!(
        f.scalar("SELECT count(*) FROM rpc_owned_sell_dispatches")?,
        0
    );
    assert_eq!(f.scalar("SELECT count(*) FROM orders")?, 1);
    assert_eq!(
        f.scalar("SELECT sum(fee_reserve) FROM rpc_owned_sell_handoffs")?,
        100000
    );
    Ok(())
}
