use super::{association_parent_fixture as p, association_sell_fixture as s, b93_fixture as f};
use anyhow::Result;
use copybot_storage_core::*;
use serde_json::json;

#[tokio::test]
async fn b93_ambiguous_partial_receipt_claim_never_applies_or_rebinds() -> Result<()> {
    let (db, m) = f::seeded("b93-ambiguity").await?;
    let first = p::read(&db, &m)?.first;
    let a = f::receipt_signature(&db, &m, "claim-a", 3000, "same-receipt")?;
    db.store
        .record_execution_canary_receipt_facts(&a, f::at())?;
    let b = f::receipt_signature(&db, &m, "claim-b", 3000, "same-receipt")?;
    let record = db.store.record_execution_canary_receipt_facts(&b, f::at());
    let before = s::snapshot(&db)?;
    let apply = db.store.apply_execution_canary_sell_settlement(&a, f::at());
    assert!(apply.is_err());
    assert_eq!(f::raw(&db, &m)?, 7000);
    assert_eq!(s::snapshot(&db)?, before);
    assert_eq!(p::read(&db, &m)?.first, first);
    assert!(db
        .store
        .execution_canary_token_accounting_pending(f::token(&m))?);
    f::write(
        "ambiguous-receipt",
        json!({"second_claim_record":format!("{record:?}"),"apply":format!("{apply:?}"),"state":f::state(&db,&m)?,"revalidation_financial_delta":0}),
    )
}

#[tokio::test]
async fn b93_unknown_time_preparation_and_explicit_legacy_before_position() -> Result<()> {
    let (db, m) = f::seeded("b93-time-boundary").await?;
    let current = p::read(&db, &m)?;
    let v = serde_json::to_value(&current.current)?;
    assert!(v["anchors"]
        .as_array()
        .unwrap()
        .iter()
        .all(|a| a["identity"]["admission"]["message_time"] == "Missing"));
    assert_eq!(current.current.trade_authority, "trade_authority_none");
    let generation = db
        .store
        .load_execution_canary_open_position(f::token(&m))?
        .unwrap()
        .position_id;
    let mut event = f::legacy_event(&m)?;
    event.ts_utc = "2026-09-08T23:59:59Z".parse()?;
    db.store.insert_observed_swap(&event)?;
    let before = s::snapshot(&db)?;
    let outcome = db
        .store
        .stage_execution_source_sell_intent(&event, &generation)?;
    assert!(
        matches!(
            outcome,
            ExecutionSourceSellOutcome::Rejected(ExecutionSourceSellReject::SellBeforePosition)
        ),
        "{outcome:?}"
    );
    assert_eq!(s::snapshot(&db)?, before);
    f::write(
        "time-boundary",
        json!({"outcome":format!("{outcome:?}"),"explicit_legacy_fixture_time":event.ts_utc,"durable_message_time":"Missing","state":f::state(&db,&m)?,"stage_financial_delta":0}),
    )
}
