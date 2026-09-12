use super::{
    association_parent_fixture as p, association_sell_fixture as s, b93_fixture as f,
    b93_fixture::config, b93_http_fixture::Server,
};
use anyhow::Result;
use copybot_storage_core::*;
use serde_json::json;

#[tokio::test]
async fn b93_receipt_missing_decimals_conflict_and_pending_never_debit() -> Result<()> {
    for arm in [
        "pending",
        "missing-token",
        "wrong-decimals",
        "conflicting-replay",
    ] {
        let (db, m) = f::seeded(&format!("b93-receipt-{arm}")).await?;
        let mut facts = f::receipt(&db, &m, arm, 3000)?;
        if arm == "missing-token" {
            facts.token_delta = None;
            facts.token_coverage = ReceiptTokenCoverage::Unresolved;
            facts.token_coverage_reason = Some("synthetic_missing_raw".into());
        }
        if arm == "wrong-decimals" {
            facts.token_delta.as_mut().unwrap().decimals = 4;
        }
        if arm == "pending" {
            let before = s::snapshot(&db)?;
            let plan = db
                .store
                .plan_execution_canary_sell_settlement(&facts.order_id)?;
            assert!(matches!(
                plan,
                ExecutionCanarySellSettlement::Unsupported(_)
            ));
            assert_eq!(s::snapshot(&db)?, before);
            f::write(
                arm,
                json!({"plan":format!("{plan:?}"),"state":f::state(&db,&m)?}),
            )?;
        } else if arm == "conflicting-replay" {
            f::settle(&db, &facts)?;
            let before = s::snapshot(&db)?;
            facts.token_delta.as_mut().unwrap().raw = -2000;
            let record = db
                .store
                .record_execution_canary_receipt_facts(&facts, f::at());
            let apply = db
                .store
                .apply_execution_canary_sell_settlement(&facts, f::at());
            assert!(record.is_err() && apply.is_err());
            assert_eq!(s::snapshot(&db)?, before);
            assert_eq!(f::raw(&db, &m)?, 4000);
            f::write(
                arm,
                json!({"record":format!("{record:?}"),"apply":format!("{apply:?}"),"state":f::state(&db,&m)?}),
            )?;
        } else {
            db.store
                .record_execution_canary_receipt_facts(&facts, f::at())?;
            let before = s::snapshot(&db)?;
            let plan = db
                .store
                .plan_execution_canary_sell_settlement(&facts.order_id)?;
            let result = db
                .store
                .apply_execution_canary_sell_settlement(&facts, f::at());
            assert!(result.is_err());
            assert_eq!(f::raw(&db, &m)?, 7000);
            assert_eq!(s::snapshot(&db)?, before);
            f::write(
                arm,
                json!({"plan":format!("{plan:?}"),"apply":format!("{result:?}"),"state":f::state(&db,&m)?}),
            )?;
        }
        assert_eq!(
            db.store
                .execution_canary_receipt_submit_block_reason("next", f::token(&m), "sell")?
                .is_some(),
            arm != "conflicting-replay"
        );
    }
    Ok(())
}

#[tokio::test]
async fn b93_generation_latest_buy_pending_and_decimals_guard_outcomes() -> Result<()> {
    for arm in [
        "generation",
        "latest-buy",
        "pending-before",
        "pending-await",
        "wallet-decimals",
    ] {
        let (db, m) = f::seeded(&format!("b93-negative-{arm}")).await?;
        let first = p::read(&db, &m)?.first;
        let signal = f::legacy(&db, &m)?;
        let quote = super::source_write_off_fixture::quote(&signal, f::at());
        db.store.record_execution_quote_canary_event(&quote)?;
        if arm == "pending-before" {
            let _ = f::receipt(&db, &m, arm, 3000)?;
        }
        let mut rpc = Server::new(db.path.clone(), f::at(), [94; 32]).await?;
        {
            let mut c = rpc.state.lock().unwrap();
            if arm == "wallet-decimals" {
                c.wallet_decimals = 4;
            }
            if matches!(arm, "generation" | "latest-buy" | "pending-await") {
                c.mutate_at = Some(if arm == "pending-await" {
                    "quote"
                } else {
                    "simulateTransaction"
                });
                let path = db.path.clone();
                let m = m.clone();
                c.mutation = Some(Box::new(move || {
                    let db = f::open(&path)?;
                    if arm == "pending-await" {
                        let _ = f::receipt(&db, &m, arm, 3000)?;
                    } else {
                        if arm == "generation" {
                            let facts = f::receipt(&db, &m, "full-close", 7000)?;
                            f::settle(&db, &facts)?;
                            assert!(db
                                .store
                                .load_execution_canary_open_position(f::token(&m))?
                                .is_none());
                        }
                        f::additional_buy(
                            &db,
                            &m,
                            arm,
                            m["source"]["signer"].as_str().unwrap(),
                            "2026-09-09T00:00:20Z",
                        )?;
                    }
                    Ok(())
                }));
            }
        }
        let out = crate::execution_canary_route::process_tiny_submit_sell_quote_event_for_route(
            &config(&rpc.url),
            &db.store,
            &quote.event_id,
            f::at(),
        )
        .await;
        rpc.finish().await?;
        let out = out?.unwrap();
        assert_eq!(out.signing_envelope_built, 0);
        assert_eq!(rpc.count("sendTransaction"), 0);
        assert_eq!(p::read(&db, &m)?.first, first);
        if matches!(arm, "generation" | "latest-buy" | "pending-await") {
            assert_eq!(out.source_sell_refusals.count(), 1, "{arm}: {out:?}");
        }
        if matches!(arm, "pending-before" | "wallet-decimals") {
            assert_eq!(rpc.count("quote"), 0, "{arm}: {out:?}");
        }
        f::write(
            &format!("guard-{arm}"),
            json!({"summary":format!("{out:?}"),"state":f::state(&db,&m)?,"calls":rpc.state.lock().unwrap().calls}),
        )?;
    }
    Ok(())
}

#[tokio::test]
async fn b93_two_contributors_use_current_full_position_not_source_allocation() -> Result<()> {
    let (db, m) = f::seeded("b93-two-contributors").await?;
    f::additional_buy(
        &db,
        &m,
        "second-leader",
        "independent-leader-B",
        "2026-09-09T00:00:02Z",
    )?;
    let signal = f::legacy(&db, &m)?;
    assert_eq!(f::raw(&db, &m)?, 9000);
    let mut rpc = Server::new(db.path.clone(), f::at(), [94; 32]).await?;
    rpc.state.lock().unwrap().wallet_raw = 12000;
    let quote = super::source_write_off_fixture::quote(&signal, f::at());
    db.store.record_execution_quote_canary_event(&quote)?;
    let out = crate::execution_canary_route::process_tiny_submit_sell_quote_event_for_route(
        &config(&rpc.url),
        &db.store,
        &quote.event_id,
        f::at(),
    )
    .await?;
    rpc.finish().await?;
    let amounts: Vec<String> = db
        .sql
        .prepare("SELECT quote_in_amount_raw FROM execution_canary_build_plan_metadata")?
        .query_map([], |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    assert_eq!(amounts, vec!["9000"]);
    f::write(
        "two-contributors",
        json!({"plan":amounts,"summary":format!("{out:?}"),"state":f::state(&db,&m)?,"calls":rpc.state.lock().unwrap().calls}),
    )?;
    assert_eq!(rpc.count("sendTransaction"), 0);
    Ok(())
}
