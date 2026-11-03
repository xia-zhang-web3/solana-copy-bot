//! Causal transfer/rollback/UNKNOWN proofs; no provider, signer or sends.
#[path = "common/unsigned_exit_fixture.rs"]
mod f;
use anyhow::Result;
use chrono::Duration;
use copybot_storage_core::*;
use f::Case;

#[test]
#[ignore = "pinned private copied DB; COPYBOT_EXPIRED_SELL_DB"]
fn unsigned_hold_transfers_once_unknown_reopens_and_receipt_closes_exact_lot() -> Result<()> {
    let f = Case::new()?;
    let parents = f.parents()?;
    let proof = f.store.owner_bot_receipt_exit_proof(&f.intent)?;
    assert_eq!(
        (
            proof.personal_raw,
            proof.expected_wallet_total_raw,
            proof.native_remaining
        ),
        (100, 930283, 4945000)
    );
    assert_eq!(
        f.scalar("SELECT count(*) FROM rpc_owned_sell_pending_obligations")?,
        1
    );
    let (o, d, b) = f.dispatch()?;
    assert_eq!(
        f.store
            .claim_owner_exit_dispatch(&o, &d, &b, || Ok(f.intent.activated_at))?,
        ExecutionDispatchClaim::New
    );
    assert_eq!(f.parents()?, parents);
    assert_eq!(
        f.scalar("SELECT fee_bound FROM owner_exit_fee_reservations")?,
        100000
    );
    assert_eq!(
        f.scalar("SELECT count(*) FROM rpc_owned_sell_pending_obligations")?,
        0
    );
    assert_eq!(
        f.scalar("SELECT count(*) FROM execution_canary_unresolved_dispatch")?,
        1
    );
    let c = f.conn()?;
    let (h, m): (String, String) = c.query_row(
        "SELECT handoff_json,build_plan_json FROM unsigned_owner_exit_transfers",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    let h: serde_json::Value = serde_json::from_str(&h)?;
    assert_eq!(h["unsigned_payload"], f.old.payload);
    assert_eq!(h["fee_reserve"], 100000);
    assert_eq!(h["state"], "unsigned_prepared");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&m)?["client_order_id"],
        f.old.handoff.owner
    );
    assert!(c
        .execute("DELETE FROM unsigned_owner_exit_transfers", [])
        .is_err());
    let reopened = SqliteStore::open(&f.path)?;
    assert_eq!(
        reopened.list_owner_exit_recovery_intents()?,
        vec![f.intent.clone()]
    );
    assert_eq!(
        reopened.claim_owner_exit_dispatch(&o, &d, &b, || Ok(f.intent.expires_at))?,
        ExecutionDispatchClaim::Existing
    );
    assert!(!reopened.rearm_owner_exit_undispatched(&f.intent.intent_id)?);
    let metadata = reopened
        .load_execution_canary_build_plan_metadata(&f.old.order_id())?
        .unwrap();
    assert!(reopened
        .record_owned_sell_build_plan_metadata(&f.old, &metadata, None)
        .is_err());
    assert!(!reopened.expired_owned_sell_preparation(f.intent.expires_at)?);
    f.store
        .refresh_local_bounded_session(f.intent.activated_at)?;
    assert_eq!(
        c.query_row("SELECT state FROM real_session_cycles", [], |r| r
            .get::<_, String>(0))?,
        "active"
    );
    f.settle(&d)?;
    f.store
        .refresh_local_bounded_session(f.intent.activated_at + Duration::seconds(1))?;
    assert_eq!(
        c.query_row("SELECT state,qty_raw FROM positions", [], |r| Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?
        )))?,
        ("closed".into(), "0".into())
    );
    assert_eq!(
        c.query_row("SELECT state FROM real_session_cycles", [], |r| r
            .get::<_, String>(0))?,
        "closed"
    );
    assert_eq!(
        f.scalar("SELECT actual_fee FROM owner_exit_fee_reservations")?,
        27000
    );
    assert_eq!(
        f.scalar("SELECT count(*) FROM rpc_owned_sell_dispatches")?,
        0
    );
    assert_eq!(f.scalar("SELECT count(*) FROM fills")?, 2);
    assert_eq!(f.parents()?, parents);
    assert!(f.store.owner_bot_receipt_exit_proof(&f.intent).is_err());
    Ok(())
}
#[test]
#[ignore = "pinned private copied DB; atomic last-clock failure"]
fn expiry_rolls_back_archive_fee_dispatch_and_retains_old_hold() -> Result<()> {
    let f = Case::new()?;
    let parents = f.parents()?;
    let (o, d, b) = f.dispatch()?;
    let n = std::cell::Cell::new(0);
    let error = f
        .store
        .claim_owner_exit_dispatch(&o, &d, &b, || {
            let v = n.get();
            n.set(v + 1);
            Ok(if v == 0 {
                f.intent.activated_at
            } else {
                f.intent.expires_at
            })
        })
        .unwrap_err();
    assert_eq!(n.get(), 2, "must reach the final post-dispatch clock");
    assert_eq!(error.root_cause().to_string(), "owner_exit_intent_expired");
    for table in [
        "unsigned_owner_exit_transfers",
        "owner_exit_fee_reservations",
    ] {
        assert_eq!(f.scalar(&format!("SELECT count(*) FROM {table}"))?, 0);
    }
    assert!(f
        .store
        .load_execution_canary_dispatch(&o.order_id)?
        .is_none());
    assert_eq!(
        f.scalar("SELECT count(*) FROM rpc_owned_sell_pending_obligations")?,
        1
    );
    assert_eq!(
        f.scalar("SELECT sum(fee_reserve) FROM rpc_owned_sell_pending_obligations")?,
        100000
    );
    assert_eq!(f.parents()?, parents);
    Ok(())
}
#[test]
#[ignore = "pinned private clones; related partial/foreign evidence must refuse"]
fn related_or_foreign_evidence_cannot_be_relabelled_unsigned() -> Result<()> {
    for table in [
        "orders",
        "execution_order_sources",
        "execution_canary_dispatch",
        "execution_tiny_reservations",
        "execution_canary_receipt_facts",
        "execution_canary_receipt_proofs",
        "fills",
    ] {
        let f = Case::new()?;
        f.original.related_row(table)?;
        assert!(
            f.store.owner_bot_receipt_exit_proof(&f.intent).is_err(),
            "accepted {table}"
        );
        assert_eq!(
            f.scalar("SELECT count(*) FROM unsigned_owner_exit_transfers")?,
            0
        );
    }
    for sql in [
        "UPDATE rpc_owned_sell_handoffs SET state='preparing',unsigned_payload=NULL",
        "UPDATE rpc_owned_sell_handoffs SET deadline='2027-01-01T00:00:00Z'",
        "UPDATE rpc_owned_sell_handoffs SET snapshot=json_set(snapshot,'$.quote.raw',930184)",
        "UPDATE rpc_owned_sell_handoffs SET snapshot=json_set(snapshot,'$.receipts[0].contributor.tx_signature','foreign')",
        "UPDATE rpc_owned_sell_handoffs SET wallet='foreign'",
    ] {
        let f=Case::new()?; f.corrupt_clone(sql)?;
        assert!(f.store.owner_bot_receipt_exit_proof(&f.intent).is_err(),"accepted {sql}");
    }
    Ok(())
}
#[test]
#[ignore = "pinned private clone; corrupted transfer linkage cannot hide hold"]
fn incomplete_transfer_linkage_keeps_conservative_hold() -> Result<()> {
    let f = Case::new()?;
    let (o, d, b) = f.dispatch()?;
    f.store
        .claim_owner_exit_dispatch(&o, &d, &b, || Ok(f.intent.activated_at))?;
    f.corrupt_clone("DELETE FROM owner_exit_fee_reservations")?;
    assert_eq!(
        f.scalar("SELECT sum(fee_reserve) FROM rpc_owned_sell_pending_obligations")?,
        100000
    );
    assert!(f.store.owner_bot_receipt_exit_proof(&f.intent).is_err());
    assert!(f
        .store
        .claim_owner_exit_dispatch(&o, &d, &b, || Ok(f.intent.expires_at))
        .is_err());
    Ok(())
}

fn old_operands(f: &Case) -> (ExecutionCanaryDispatch, TinyBudgetClaim) {
    let p = &f.old;
    let d = ExecutionCanaryDispatch {
        order_id: p.order_id(),
        signal_id: p.handoff.intent_id.clone(),
        client_order_id: p.handoff.owner.clone(),
        route: "jupiter_swap_instructions".into(),
        attempt: 1,
        wallet: p.handoff.wallet.clone(),
        token: p.handoff.snapshot.quote.mint.clone(),
        side: "sell".into(),
        tx_signature: "LOCAL-OLD-NO-SEND".into(),
        transaction_sha256: "a".repeat(64),
        message_sha256: p.message_sha256.clone(),
    };
    let b = TinyBudgetClaim {
        experiment_id: p.handoff.experiment_id.clone(),
        wallet: d.wallet.clone(),
        tx_signature: d.tx_signature.clone(),
        message_sha256: d.message_sha256.clone(),
        transaction_sha256: d.transaction_sha256.clone(),
        buy_lamports: Some(0),
        protected_capital: None,
        total_fee: p.total_fee,
        priority_fee: p.priority_fee,
        fee_slot: p.handoff.snapshot.sell.facts.slot + 1,
    };
    (d, b)
}
#[test]
#[ignore = "pinned private clones; competing writer generations"]
fn old_dispatch_winner_blocks_transfer_and_transfer_winner_blocks_old_claim() -> Result<()> {
    let f = Case::new()?;
    let (d, b) = old_operands(&f);
    let before = f.old.handoff.deadline - Duration::milliseconds(1);
    assert_eq!(
        f.store
            .claim_owned_sell_dispatch(&f.old, &d, &b, || Ok(before))?,
        ExecutionDispatchClaim::New
    );
    assert!(f.store.owner_bot_receipt_exit_proof(&f.intent).is_err());
    assert_eq!(
        f.scalar("SELECT count(*) FROM unsigned_owner_exit_transfers")?,
        0
    );
    let f = Case::new()?;
    let stale = SqliteStore::open(&f.path)?;
    let (old_d, old_b) = old_operands(&f);
    let (o, d, b) = f.dispatch()?;
    f.store
        .claim_owner_exit_dispatch(&o, &d, &b, || Ok(f.intent.activated_at))?;
    let e = stale
        .claim_owned_sell_dispatch(&f.old, &old_d, &old_b, || {
            Ok(f.old.handoff.deadline - Duration::milliseconds(1))
        })
        .unwrap_err();
    assert!(
        format!("{e:#}").contains("owned_sell_prepared_changed"),
        "{e:#}"
    );
    assert_eq!(
        f.scalar("SELECT count(*) FROM rpc_owned_sell_dispatches")?,
        0
    );
    assert_eq!(
        f.scalar("SELECT count(*) FROM unsigned_owner_exit_transfers")?,
        1
    );
    assert_eq!(
        f.scalar("SELECT count(*) FROM execution_canary_dispatch WHERE side='sell'")?,
        1
    );
    Ok(())
}
