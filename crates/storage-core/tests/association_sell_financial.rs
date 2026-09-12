#[path = "common/association_sell_fixture.rs"]
mod f;
use anyhow::Result;
use copybot_storage_core::association_sell_preparation::*;
use f::*;
#[test]
fn b90_malformed_signal_and_receipt_conflicts_do_not_choose_another_buy() -> Result<()> {
    for sql in [
        "UPDATE copy_signals SET signal_id='bad'; UPDATE orders SET signal_id='bad'",
        "UPDATE execution_canary_receipt_proofs SET tx_signature='leaderbuy'",
        "UPDATE execution_canary_receipt_facts SET token='other'",
        "UPDATE fills SET qty_raw='7001'",
        "UPDATE fills SET qty_decimals=4",
        "UPDATE fills SET position_id=NULL",
        "UPDATE orders SET status='canary_submitted'",
        "DELETE FROM execution_canary_receipt_facts",
    ] {
        let mut f = F::new()?;
        f.anchors()?;
        f.db.conn()?
            .execute_batch(&format!("PRAGMA foreign_keys=OFF; {sql}"))?;
        f.sell()?;
        let p = f.read()?;
        assert!(matches!(p.first.witness, FirstWitness::Unknown(_)), "{sql}");
        assert_ne!(
            p.current.selected_chain,
            Check::ProviderOrderedWithinBlock,
            "{sql}"
        );
    }
    Ok(())
}
#[test]
fn b90_retained_source_contradictions_block_but_absence_is_not_nearest_buy_search() -> Result<()> {
    for variant in 0..5 {
        let mut f = F::new()?;
        f.anchors()?;
        let c = f.db.conn()?;
        c.execute("INSERT INTO observed_swaps(signature,wallet_id,dex,token_in,token_out,qty_in,qty_out,slot,ts,qty_in_raw,qty_in_decimals,qty_out_raw,qty_out_decimals) VALUES('leaderbuy','leader','fixture',?1,'mint',0.00000095,7,42,'2026-09-07T12:00:00Z','950',9,'7000',3)",[SOL])?;
        match variant {
            0 => {}
            1 => {
                c.execute("UPDATE observed_swaps SET wallet_id='other'", [])?;
            }
            2 => {
                c.execute("UPDATE observed_swaps SET qty_out_raw='7001'", [])?;
            }
            3 => {
                c.execute("UPDATE observed_swaps SET qty_out_decimals=4", [])?;
            }
            _ => {
                c.execute("UPDATE observed_swaps SET qty_in=3,qty_in_raw=NULL,qty_in_decimals=NULL,qty_out_raw=NULL,qty_out_decimals=NULL", [])?;
            }
        }
        f.sell()?;
        assert_eq!(
            f.read()?.current.selected_chain,
            if variant == 0 {
                Check::ProviderOrderedWithinBlock
            } else {
                Check::Blocked(Reason::SourceFactsConflict)
            }
        );
    }
    Ok(())
}
#[test]
fn b90_unproven_subset_and_pending_initial_are_preserved_in_coverage() -> Result<()> {
    let mut f = F::new()?;
    f.anchors()?;
    let unproven =
        f.db.seed("shadow:unproven:other:buy:mint", "other", "buy")?;
    f.db.buy(&unproven)?;
    f.db.conn()?.execute(
        "UPDATE fills SET position_id=NULL WHERE order_id=?1",
        [unproven],
    )?;
    f.db.seed("shadow:pending:other:buy:mint", "other", "buy")?;
    f.sell()?;
    let p = f.read()?;
    assert_eq!(p.current.selected_chain, Check::ProviderOrderedWithinBlock);
    assert!(!p.current.pending_buys.is_empty());
    assert!(!p.current.unproven_links.is_empty());
    assert!(p.first.contributors_fingerprint.is_some());
    Ok(())
}
#[test]
fn b90_financial_lookup_exceeding_existing_count_budget_is_unknown_not_subset() -> Result<()> {
    let mut f = F::new()?;
    f.anchors()?;
    let c = f.db.conn()?;
    for n in 0..1001 {
        c.execute("INSERT INTO copy_signals(signal_id,wallet_id,token,side,notional_sol,ts,status) VALUES(?1,'other','other','buy',1,'2026-09-07T12:00:00Z','fixture')",[format!("unrelated-{n}")])?;
    }
    f.sell()?;
    let p = f.read()?;
    assert_eq!(p.first.witness, FirstWitness::Unknown(Reason::LookupBound));
    assert_eq!(
        p.current.selected_chain,
        Check::Unknown(Reason::LookupBound)
    );
    Ok(())
}
