//! Causal replay against a pinned stopped snapshot; no network, signer or live writes.
#[path = "common/owned_expiry_fixture.rs"]
mod fixture;
use anyhow::Result;
use chrono::Duration;
use copybot_storage_core::{
    ordered_sell_quote::QuoteClaimStep, rpc_owned_sell_handoff::dispatch::Prepared,
};
use rusqlite::{params, Connection};

#[test]
#[ignore = "requires pinned stopped live snapshot; set COPYBOT_EXPIRED_SELL_DB"]
fn expired_pre_dispatch_refresh_preserves_old_proof_and_one_hold_across_reopen() -> Result<()> {
    let f = fixture::open()?;
    assert!(matches!(
        f.claim(f.old.handoff.deadline - Duration::milliseconds(1))?,
        QuoteClaimStep::Skipped
    ));
    let h = f.recover()?;
    assert_ne!(h.owner, f.old.handoff.owner);
    assert_eq!(h.order_id, f.old.handoff.order_id);
    assert_eq!(
        h.deadline,
        h.quote.http_started.unwrap() + Duration::seconds(5)
    );
    let c = Connection::open(&f.path)?;
    let (old, metadata): (String, String) = c.query_row(
        "SELECT prepared_json,build_plan_json FROM owned_sell_quote_expiry_recoveries",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&old)?,
        serde_json::to_value(&f.old)?
    );
    let saved: serde_json::Value = serde_json::from_str(&metadata)?;
    assert_eq!(saved["client_order_id"], f.old.handoff.owner);
    assert!(saved["priority_fee_json"]
        .as_str()
        .unwrap()
        .contains("22000"));
    let amount:Option<String> = c.query_row("SELECT owned_sell_amount_proof_json FROM execution_canary_build_plan_metadata WHERE order_id=?1",[f.old.order_id()],|r|r.get(0))?;
    assert_eq!(
        saved["owned_sell_amount_proof_json"],
        serde_json::to_value(amount)?
    );
    let old_metadata = f
        .store
        .load_execution_canary_build_plan_metadata(&f.old.order_id())?
        .unwrap();
    assert!(f
        .store
        .record_owned_sell_build_plan_metadata(&f.old, &old_metadata, None)
        .is_err());
    assert_eq!(
        f.scalar("SELECT sum(fee_reserve) FROM rpc_owned_sell_handoffs")?,
        100000
    );
    assert_eq!(f.scalar("SELECT count(*) FROM orders")?, 1);
    assert_eq!(
        f.scalar("SELECT count(*) FROM execution_tiny_reservations")?,
        1
    );
    assert_eq!(
        f.scalar("SELECT count(*) FROM rpc_owned_sell_dispatches")?,
        0
    );
    assert!(c
        .execute("DELETE FROM owned_sell_quote_expiry_recoveries", [])
        .is_err());
    assert!(c
        .execute(
            "UPDATE owned_sell_quote_expiry_recoveries SET new_owner='altered'",
            []
        )
        .is_err());
    drop(c);
    let restarted = copybot_storage_core::SqliteStore::open(&f.path)?;
    assert!(restarted
        .recheck_owned_sell_prepared(&f.old, f.after())
        .is_err());
    // Even with a clock before the old expiry, replacement invalidates signed old work.
    assert!(restarted
        .recheck_owned_sell_prepared(&f.old, f.old.handoff.deadline - Duration::milliseconds(1))
        .is_err());
    restarted.complete_owned_sell_handoff(
        &h,
        fixture::limits(),
        &f.old.payload,
        &f.old.message_sha256,
        f.old.total_fee,
        f.old.priority_fee,
        || f.after() + Duration::milliseconds(300),
    )?;
    let new = Prepared {
        handoff: h.clone(),
        experiment: restarted.owned_sell_experiment_snapshot()?,
        ..f.old.clone()
    };
    let mut metadata = old_metadata.clone();
    metadata.client_order_id = h.owner.clone();
    metadata.recorded_ts = f.after() + Duration::milliseconds(350);
    metadata.decision_reason = Some("NEW_GENERATION_PROOF".into());
    restarted.record_owned_sell_build_plan_metadata(&new, &metadata, None)?;
    assert_eq!(
        restarted
            .load_execution_canary_build_plan_metadata(&new.order_id())?
            .unwrap()
            .decision_reason,
        metadata.decision_reason
    );
    assert!(restarted
        .record_owned_sell_build_plan_metadata(&f.old, &old_metadata, None)
        .is_err());
    let c = Connection::open(&f.path)?;
    assert_eq!(
        c.query_row(
            "SELECT build_plan_json FROM owned_sell_quote_expiry_recoveries",
            [],
            |r| r.get::<_, String>(0)
        )?,
        serde_json::to_string(&saved)?
    );
    assert!(matches!(
        restarted.claim_strict_sell_quote_for_owned_preparation(
            fixture::limits(),
            &h.snapshot.quote.endpoint,
            || h.deadline + Duration::milliseconds(1)
        )?,
        QuoteClaimStep::Skipped
    ));
    assert_eq!(
        f.scalar("SELECT count(*) FROM owned_sell_quote_expiry_recoveries")?,
        1
    );
    assert_eq!(
        f.scalar("SELECT sum(fee_reserve) FROM rpc_owned_sell_handoffs")?,
        100000
    );
    Ok(())
}

#[test]
#[ignore = "requires pinned stopped live snapshot; set COPYBOT_EXPIRED_SELL_DB"]
fn partial_financial_evidence_and_expired_authority_forbid_refresh() -> Result<()> {
    for table in [
        "orders",
        "execution_canary_dispatch",
        "execution_order_sources",
        "execution_tiny_reservations",
        "execution_canary_receipt_facts",
        "execution_canary_receipt_proofs",
        "fills",
    ] {
        let f = fixture::open()?;
        f.related_row(table)?;
        assert!(
            matches!(f.claim(f.after())?, QuoteClaimStep::Skipped),
            "{table}"
        );
        assert_eq!(
            f.scalar("SELECT count(*) FROM owned_sell_quote_expiry_recoveries")?,
            0
        );
        assert_eq!(
            f.scalar("SELECT sum(fee_reserve) FROM rpc_owned_sell_handoffs")?,
            100000
        );
    }
    let f = fixture::open()?;
    let end = f.old.experiment.experiment.deadline;
    assert!(f.claim(end + Duration::milliseconds(1)).is_err());
    assert_eq!(
        f.scalar("SELECT count(*) FROM owned_sell_quote_expiry_recoveries")?,
        0
    );
    Ok(())
}

#[test]
#[ignore = "requires pinned stopped live snapshot; set COPYBOT_EXPIRED_SELL_DB"]
fn fresh_quote_change_and_expiry_inside_transaction_do_not_release_hold() -> Result<()> {
    for corrupt in [false, true] {
        let f = fixture::open()?;
        let (snapshot, mut q) = f.fresh()?;
        if corrupt {
            q.response_sha256 = Some("0".repeat(64));
        }
        let h = &f.old.handoff;
        let mut calls = 0;
        let result = f.store.recover_owned_sell_handoff(
            &snapshot,
            &q,
            fixture::limits(),
            &h.config_sha256,
            &h.authority,
            &h.experiment_id,
            &h.wallet,
            || {
                calls += 1;
                f.after() + Duration::milliseconds(if calls > 1 { 6000 } else { 200 })
            },
        );
        assert!(result.is_err());
        if !corrupt {
            assert_eq!(calls, 2, "must reach post-archive/CAS deadline check");
            assert!(format!("{:#}", result.unwrap_err()).contains("owned_sell_deadline"));
        }
        let c = Connection::open(&f.path)?;
        let (owner, state, hold): (String, String, u64) = c.query_row(
            "SELECT owner,state,fee_reserve FROM rpc_owned_sell_handoffs WHERE intent_id=?1",
            [&h.intent_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        assert_eq!(
            (owner.as_str(), state.as_str(), hold),
            (h.owner.as_str(), "unsigned_prepared", 100000)
        );
        assert_eq!(
            f.scalar("SELECT count(*) FROM owned_sell_quote_expiry_recoveries")?,
            0
        );
        assert!(c.execute("UPDATE rpc_owned_sell_handoffs SET state='preparing',unsigned_payload=NULL,message_sha256=NULL,total_fee=NULL,priority_fee=NULL WHERE intent_id=?1",params![h.intent_id]).is_ok());
        assert!(matches!(f.claim(f.after())?, QuoteClaimStep::Skipped));
    }
    Ok(())
}

#[test]
#[ignore = "requires pinned stopped live snapshot; set COPYBOT_EXPIRED_SELL_DB"]
fn expiry_then_simulation_refusal_cannot_create_a_second_refresh() -> Result<()> {
    let f = fixture::open()?;
    let h = f.recover()?;
    f.store.record_owned_sell_simulation_refusal(
        &h,
        fixture::limits(),
        &"a".repeat(64),
        h.snapshot.sell.facts.slot,
        f.after() + Duration::milliseconds(300),
    )?;
    assert!(matches!(
        f.claim(h.deadline + Duration::milliseconds(1))?,
        QuoteClaimStep::Skipped
    ));
    assert_eq!(
        f.scalar("SELECT count(*) FROM owned_sell_quote_expiry_recoveries")?,
        1
    );
    assert_eq!(
        f.scalar("SELECT count(*) FROM owned_sell_simulation_refusals")?,
        1
    );
    assert_eq!(
        f.scalar("SELECT count(*) FROM owned_sell_preparation_recoveries")?,
        0
    );
    assert_eq!(
        f.scalar("SELECT sum(fee_reserve) FROM rpc_owned_sell_handoffs")?,
        100000
    );
    Ok(())
}
