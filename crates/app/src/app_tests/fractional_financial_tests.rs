use super::{
    fractional_financial_fixture as money, fractional_fixture::Fixture, fractional_tests as f,
};
use anyhow::Result;
use chrono::Utc;
use copybot_storage_core::*;
#[tokio::test]
async fn fractional_native_unsigned_hold_dispatch_receipt_250_remaining_750() -> Result<()> {
    let mut f = Fixture::new().await?;
    money::budget(&f)?;
    let old = f.claim()?;
    let claim = f::bind(&mut f, old, f::evidence()?).await?;
    let before =
        f.db.store
            .load_execution_canary_open_position(&claim.binding.mint)?
            .unwrap();
    let p = money::prepare(&f, &claim)?;
    assert_eq!(p.handoff.snapshot.quote.raw, 250);
    f.db.store.recheck_owned_sell_prepared(&p, Utc::now())?;
    let d = money::dispatch(&f, &p)?;
    let facts = money::receipt(&d, 250);
    f.db.store.mark_execution_canary_confirmed_unreconciled(
        &d.order_id,
        &ExecutionCanaryReceiptProof {
            tx_signature: d.tx_signature.clone(),
            wallet_pubkey: d.wallet.clone(),
            token: d.token.clone(),
            side: "sell".into(),
            confirmation_status: "confirmed".into(),
            slot: Some(151),
            confirmed_at: Utc::now(),
            reason: "mocked canonical receipt".into(),
        },
        Utc::now(),
    )?;
    f.db.store
        .record_execution_canary_receipt_facts(&facts, Utc::now())?;
    f.db.store
        .apply_execution_canary_sell_settlement(&facts, Utc::now())?;
    let reopened = SqliteStore::open(&f.db.path)?;
    reopened.apply_execution_canary_sell_settlement(&facts, Utc::now())?;
    let after = reopened
        .load_execution_canary_open_position(&d.token)?
        .unwrap();
    assert_eq!(after.qty_exact.unwrap().raw(), 750);
    let basis = before.cost_lamports.unwrap().as_u64();
    let allocated = (basis * 250).div_ceil(1000);
    assert_eq!(after.cost_lamports.unwrap().as_u64(), basis - allocated);
    let cash = reopened
        .load_execution_canary_cash_settlement(&d.order_id)?
        .unwrap();
    assert_eq!(cash.sold_quantity.raw(), 250);
    assert_eq!(cash.remaining_quantity.raw(), 750);
    assert_eq!(cash.wallet_native_cash_delta.as_i128(), 981000);
    assert_eq!(cash.allocated_entry_basis.as_u64(), allocated);
    assert_eq!(cash.remaining_entry_basis.as_u64(), basis - allocated);
    let fills: i64 = f.db.sql.query_row(
        "SELECT count(*) FROM fills WHERE order_id=?1",
        [&d.order_id],
        |r| r.get(0),
    )?;
    assert_eq!(fills, 1);
    let fee: Option<String> = f.db.sql.query_row(
        "SELECT transaction_fee FROM execution_canary_receipt_facts WHERE order_id=?1",
        [&d.order_id],
        |r| r.get(0),
    )?;
    assert_eq!(fee.as_deref(), Some("19000"));
    assert!(reopened
        .recheck_owned_sell_prepared(&p, Utc::now())
        .is_err());
    Ok(())
}
#[tokio::test]
async fn fractional_unknown_dispatch_restart_holds_no_resend() -> Result<()> {
    let mut f = Fixture::new().await?;
    money::budget(&f)?;
    let old = f.claim()?;
    let claim = f::bind(&mut f, old, f::evidence()?).await?;
    let p = money::prepare(&f, &claim)?;
    let d = money::dispatch(&f, &p)?;
    let before = f.db.store.load_tiny_experiment(Utc::now())?;
    let reopened = SqliteStore::open(&f.db.path)?;
    assert_eq!(
        reopened.owned_sell_dispatch_ids(4)?,
        vec![d.order_id.clone()]
    );
    assert_eq!(
        serde_json::to_value(before)?,
        serde_json::to_value(reopened.load_tiny_experiment(Utc::now())?)?
    );
    assert!(reopened
        .recheck_owned_sell_prepared(&p, Utc::now())
        .is_err());
    assert_eq!(
        reopened
            .load_execution_canary_open_position(&d.token)?
            .unwrap()
            .qty_exact
            .unwrap()
            .raw(),
        1000
    );
    assert!(reopened.has_owned_sell_handoff(&claim.intent_id)?);
    let count: i64 =
        f.db.sql
            .query_row("SELECT count(*) FROM rpc_owned_sell_dispatches", [], |r| {
                r.get(0)
            })?;
    assert_eq!(count, 1);
    Ok(())
}
#[tokio::test]
async fn fractional_stale_decision_blocks_predispatch_without_release() -> Result<()> {
    let mut f = Fixture::new().await?;
    money::budget(&f)?;
    let old = f.claim()?;
    let claim = f::bind(&mut f, old, f::evidence()?).await?;
    let p = money::prepare(&f, &claim)?;
    f.db.sql.execute("UPDATE fractional_sell_decisions SET decision=json_set(decision,'$.decision_id','substituted')",[])?;
    assert!(f
        .db
        .store
        .recheck_owned_sell_prepared(&p, Utc::now())
        .is_err());
    let holds: i64 =
        f.db.sql
            .query_row("SELECT fee_reserve FROM rpc_owned_sell_handoffs", [], |r| {
                r.get(0)
            })?;
    assert_eq!(holds, 100000);
    assert_eq!(
        f.db.sql
            .query_row("SELECT count(*) FROM rpc_owned_sell_dispatches", [], |r| {
                r.get::<_, i64>(0)
            })?,
        0
    );
    Ok(())
}

#[tokio::test]
async fn prepared_fast_guard_falls_back_after_foreign_decision_change() -> Result<()> {
    let mut f = Fixture::new().await?;
    money::budget(&f)?;
    let old = f.claim()?;
    let claim = f::bind(&mut f, old, f::evidence()?).await?;
    let p = money::prepare(&f, &claim)?;
    let version = f.db.store.sqlite_data_version()?;
    assert!(f
        .db
        .store
        .recheck_owned_sell_prepared_at_version(&p, version, Utc::now())?);
    f.db.sql.execute("UPDATE fractional_sell_decisions SET decision=json_set(decision,'$.decision_id','substituted')",[])?;
    assert!(!f
        .db
        .store
        .recheck_owned_sell_prepared_at_version(&p, version, Utc::now())?);
    assert!(f
        .db
        .store
        .recheck_owned_sell_prepared(&p, Utc::now())
        .is_err());
    Ok(())
}

#[tokio::test]
async fn parent_commits_after_reserve_and_complete_keep_sell_owner() -> Result<()> {
    use std::sync::{Arc, Mutex};
    let mut f = Fixture::new().await?;
    money::budget(&f)?;
    let old = f.claim()?;
    let claim = f::bind(&mut f, old, f::evidence()?).await?;
    let c = f::config(&f)?;
    let observations = Arc::new(Mutex::new(Vec::new()));
    let reserve_observer = rusqlite::Connection::open(&f.db.path)?;
    let reserve_writer = rusqlite::Connection::open(&f.db.path)?;
    let complete_observer = rusqlite::Connection::open(&f.db.path)?;
    let complete_writer = rusqlite::Connection::open(&f.db.path)?;
    let reserve_seen = observations.clone();
    let complete_seen = observations.clone();
    let mut reserve_calls = 0;
    let mut complete_calls = 0;
    let p = money::prepare_with_commit_clocks(
        &f,
        &claim,
        &c,
        "fractional-test",
        move || {
            reserve_calls += 1;
            if reserve_calls == 1 {
                reserve_seen.lock().unwrap().push(reserve_observer.query_row(
                    "PRAGMA data_version", [], |r| r.get::<_, i64>(0),
                ).unwrap());
            } else if reserve_calls == 3 {
                reserve_writer.execute(
                    "INSERT INTO association_parent_blocks(block_key,first_observation,first_session,first_sequence) VALUES('unrelated-reserve-parent','late','other-stream',999999)",
                    [],
                ).unwrap();
                reserve_seen.lock().unwrap().push(reserve_observer.query_row(
                    "PRAGMA data_version", [], |r| r.get::<_, i64>(0),
                ).unwrap());
            }
            Utc::now()
        },
        move || {
            complete_calls += 1;
            if complete_calls == 1 {
                complete_seen.lock().unwrap().push(complete_observer.query_row(
                    "PRAGMA data_version", [], |r| r.get::<_, i64>(0),
                ).unwrap());
            } else if complete_calls == 3 {
                complete_writer.execute(
                    "INSERT INTO association_parent_blocks(block_key,first_observation,first_session,first_sequence) VALUES('unrelated-complete-parent','late','other-stream',1000000)",
                    [],
                ).unwrap();
                complete_seen.lock().unwrap().push(complete_observer.query_row(
                    "PRAGMA data_version", [], |r| r.get::<_, i64>(0),
                ).unwrap());
            }
            Utc::now()
        },
    )?;
    let versions = observations.lock().unwrap().clone();
    assert_eq!(versions.len(), 4);
    assert_ne!(versions[0], versions[1], "reserve parent commit changes global version");
    assert_ne!(versions[2], versions[3], "complete parent commit changes global version");
    f.db.store.recheck_owned_sell_prepared(&p, Utc::now())?;
    assert!(!f.db.store.owned_sell_handoff_dispatched(&p.handoff.intent_id)?);
    let reopened = SqliteStore::open(&f.db.path)?;
    reopened.recheck_owned_sell_prepared(&p, Utc::now())?;
    assert!(reopened.expired_owned_sell_preparation(p.handoff.deadline)?);
    let d = money::dispatch(&f, &p)?;
    assert!(f.db.store.owned_sell_handoff_dispatched(&p.handoff.intent_id)?);
    assert!(!f.db.store.expired_owned_sell_preparation(p.handoff.deadline)?);
    assert_eq!(reopened.owned_sell_dispatch_ids(4)?, vec![d.order_id]);
    Ok(())
}

#[tokio::test]
async fn relevant_parent_conflict_after_complete_blocks_final_dispatch() -> Result<()> {
    let mut f = Fixture::new().await?;
    money::budget(&f)?;
    let old = f.claim()?;
    let claim = f::bind(&mut f, old, f::evidence()?).await?;
    let p = money::prepare(&f, &claim)?;
    let before = f.db.store.sqlite_data_version()?;
    let changed = f.db.sql.execute(
        "UPDATE association_parent_blocks SET contradiction='test parent conflict' WHERE EXISTS(SELECT 1 FROM association_parent_dependencies d WHERE d.sell_signature=?1 AND block_key LIKE '%'||d.block_hash||'%')",
        [&p.handoff.snapshot.sell.facts.signature],
    )?;
    assert!(changed > 0, "source SELL parent dependency missing");
    assert_ne!(before, f.db.store.sqlite_data_version()?);
    let (dispatch, budget) = money::dispatch_candidate(&p);
    assert!(f.db.store.claim_owned_sell_dispatch(&p, &dispatch, &budget, || Ok(Utc::now())).is_err());
    assert!(!f.db.store.owned_sell_handoff_dispatched(&p.handoff.intent_id)?);
    let reopened = SqliteStore::open(&f.db.path)?;
    assert!(reopened.recheck_owned_sell_prepared(&p, Utc::now()).is_err());
    assert!(reopened.expired_owned_sell_preparation(p.handoff.deadline)?);
    assert!(reopened.owned_sell_dispatch_ids(4)?.is_empty());
    assert_eq!(reopened.load_execution_canary_open_position(&p.handoff.snapshot.quote.mint)?.unwrap().qty_exact.unwrap().raw(), 1000);
    Ok(())
}
#[tokio::test]
async fn relevant_parent_conflict_after_quote_blocks_handoff_before_send() -> Result<()> {
    use copybot_storage_core::ordered_sell_quote::{QuoteObservation, QuoteOutcome};
    let mut f = Fixture::new().await?;
    money::budget(&f)?;
    let old = f.claim()?;
    let claim = f::bind(&mut f, old, f::evidence()?).await?;
    let limits = super::association_parent_fixture::limits();
    let now = Utc::now();
    let quote = f.db.store.complete_strict_sell_quote(&claim, limits, QuoteObservation {
        version: 1, binding: Some(claim.binding.clone()), outcome: QuoteOutcome::Current,
        reason: None, http_started: Some(now), http_response: Some(now),
        quote_response_available_ts: Some(now), http_ended: now,
        response_in_raw: Some(claim.binding.raw.to_string()),
        response_out_raw: Some("1000000".into()), response_sha256: Some("a".repeat(64)),
        event_time: None, event_delay_ns: None,
    }, Utc::now)?;
    assert_eq!(quote.outcome, QuoteOutcome::Current);
    let snapshot = f.db.store.owned_sell_snapshot(&claim.binding, limits)?;
    let changed = f.db.sql.execute(
        "UPDATE association_parent_blocks SET contradiction='test parent conflict' WHERE EXISTS(SELECT 1 FROM association_parent_dependencies d WHERE d.sell_signature=?1 AND block_key LIKE '%'||d.block_hash||'%')",
        [&snapshot.sell.facts.signature],
    )?;
    assert!(changed > 0);
    let config = f::config(&f)?;
    assert!(f.db.store.reserve_owned_sell_handoff(&snapshot, &quote, limits,
        &crate::execution_owned_sell_rpc::identity(&config)?, "synthetic authority",
        "fractional-test", &config.canary_wallet_pubkey, Utc::now).is_err());
    assert_eq!(f.db.sql.query_row("SELECT count(*) FROM rpc_owned_sell_handoffs", [], |r| r.get::<_, i64>(0))?, 0);
    assert_eq!(f.db.sql.query_row("SELECT count(*) FROM rpc_owned_sell_dispatches", [], |r| r.get::<_, i64>(0))?, 0);
    Ok(())
}
#[tokio::test]
async fn relevant_parent_conflict_during_build_blocks_completion() -> Result<()> {
    let mut f = Fixture::new().await?;
    money::budget(&f)?;
    let old = f.claim()?;
    let claim = f::bind(&mut f, old, f::evidence()?).await?;
    let config = f::config(&f)?;
    let error = money::prepare_with_stage_hook(
        &f, &claim, &config, "fractional-test", Utc::now, Utc::now,
        |handoff| {
            let changed = f.db.sql.execute(
                "UPDATE association_parent_blocks SET contradiction='test build conflict' WHERE EXISTS(SELECT 1 FROM association_parent_dependencies d WHERE d.sell_signature=?1 AND block_key LIKE '%'||d.block_hash||'%')",
                [&handoff.snapshot.sell.facts.signature],
            )?;
            assert!(changed > 0);
            f.db.store.recheck_owned_sell_handoff_progress(
                handoff, super::association_parent_fixture::limits(), Utc::now(),
            )?;
            Ok(())
        },
    ).unwrap_err();
    assert!(error.to_string().contains("owned_sell_snapshot_changed"), "{error:#}");
    assert_eq!(f.db.sql.query_row("SELECT count(*) FROM rpc_owned_sell_handoffs WHERE state='preparing'", [], |r| r.get::<_, i64>(0))?, 1);
    assert_eq!(f.db.sql.query_row("SELECT count(*) FROM rpc_owned_sell_dispatches", [], |r| r.get::<_, i64>(0))?, 0);
    Ok(())
}
