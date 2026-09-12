#[path = "common/buy_attribution_review_fixture.rs"]
mod fixture;
#[path = "common/buy_receipt_history.rs"]
mod history;
use anyhow::Result;
use copybot_storage_core::*;
use fixture::Db;

fn ensure_ambiguous_signature_is_not_proven(db: &Db) -> Result<()> {
    let before = db.snapshot()?;
    let result = db.store.load_execution_canary_buy_attribution("mint");
    assert_eq!(db.snapshot()?, before);
    println!("ROOT_B30_ATTRIBUTION {result:?}");
    if let Ok(ExecutionCanaryBuyAttribution::Open(value)) = result {
        assert!(
            !value
                .proven_contributors
                .iter()
                .any(|c| c.tx_signature == "shared-signature"),
            "ambiguous receipt was treated as proven source: {value:?}"
        );
    }
    Ok(())
}

#[test]
fn root_duplicate_receipt_cannot_prove_two_sources() -> Result<()> {
    let db = Db::new()?;
    let a = db.seed("dup-a", "source-a", "buy")?;
    db.buy(&a)?;
    let b = db.seed("dup-b", "source-b", "buy")?;
    history::buy(&db, &b, "mint")?;
    ensure_ambiguous_signature_is_not_proven(&db)
}

#[test]
fn root_duplicate_receipt_from_closed_generation_is_not_new_source() -> Result<()> {
    let mut db = Db::new()?;
    let a = db.seed("dup-a", "source-a", "buy")?;
    db.buy(&a)?;
    db.close()?;
    let b = db.seed("dup-b", "source-b", "buy")?;
    history::buy(&db, &b, "mint")?;
    db.reopen()?;
    ensure_ambiguous_signature_is_not_proven(&db)
}

#[test]
fn root_distinct_receipts_still_prove_two_sources() -> Result<()> {
    let db = Db::new()?;
    let a = db.seed("a", "source-a", "buy")?;
    db.buy(&a)?;
    let b = db.seed("b", "source-b", "buy")?;
    db.buy(&b)?;
    let before = db.snapshot()?;
    let ExecutionCanaryBuyAttribution::Open(value) =
        db.store.load_execution_canary_buy_attribution("mint")?
    else {
        panic!("missing position");
    };
    assert_eq!(value.coverage, BuyAttributionCoverage::ProvenSubset);
    assert_eq!(value.proven_contributors.len(), 2);
    assert_eq!(db.snapshot()?, before);
    Ok(())
}
