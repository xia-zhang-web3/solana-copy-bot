#[path = "common/association_sell_fixture.rs"]
mod f;
use anyhow::Result;
use copybot_core_types::association_delivery::AdmissionFacts;
use copybot_storage_core::association_sell_preparation::*;
use f::*;
fn consistent(a: &mut AdmissionFacts) {
    let Some(e) = &a.facts.exact_amounts else {
        return;
    };
    a.facts.amount_in_bits = (e.amount_in_raw.parse::<u128>().unwrap() as f64
        / 10f64.powi(e.amount_in_decimals.into()))
    .to_bits();
    a.facts.amount_out_bits = (e.amount_out_raw.parse::<u128>().unwrap() as f64
        / 10f64.powi(e.amount_out_decimals.into()))
    .to_bits();
}
fn chain(
    change: impl FnOnce(&mut AdmissionFacts, &mut AdmissionFacts),
) -> Result<(F, ValidatedPreparation)> {
    let mut f = F::new()?;
    let mut source = facts("leaderbuy", "leader", true);
    let mut sell = facts("sell", "leader", false);
    change(&mut source, &mut sell);
    for a in [&mut source, &mut sell] {
        consistent(a);
    }
    let before = f.db.snapshot()?;
    for (a, index) in [
        (source, 1),
        (facts(&f.our, "execution-wallet", true), 2),
        (sell, 3),
    ] {
        f.admit(a.clone())?;
        f.terminal(&a, index, 42, "block")?;
    }
    f.drain()?;
    let p = f.read()?;
    assert_eq!(f.db.snapshot()?, before);
    assert_eq!(p.current.trade_authority, "trade_authority_none");
    Ok((f, p))
}
#[test]
fn b90r1_all_four_known_decimal_conflicts_block_initial_and_current_validation() -> Result<()> {
    for variant in 0..4 {
        let (f, p) = chain(|source, sell| match variant {
            0 => {
                source
                    .facts
                    .exact_amounts
                    .as_mut()
                    .unwrap()
                    .amount_out_decimals = 4
            }
            1 => {
                sell.facts
                    .exact_amounts
                    .as_mut()
                    .unwrap()
                    .amount_in_decimals = 4
            }
            2 => {
                source
                    .facts
                    .exact_amounts
                    .as_mut()
                    .unwrap()
                    .amount_in_decimals = 8
            }
            _ => {
                sell.facts
                    .exact_amounts
                    .as_mut()
                    .unwrap()
                    .amount_out_decimals = 8
            }
        })?;
        assert_eq!(
            p.current.selected_chain,
            Check::Blocked(Reason::DecimalsConflict)
        );
        assert_eq!(p.historical_latest.selected_chain, p.current.selected_chain);
        if variant == 1 || variant == 3 {
            assert_eq!(
                p.current.contributor_orders[0].relative_to_sell,
                Check::Blocked(Reason::DecimalsConflict)
            );
        }
        // A forged historical positive cannot substitute for current validation.
        let mut stale = p.historical_latest.clone();
        stale.selected_chain = Check::ProviderOrderedWithinBlock;
        f.db.conn()?.execute(
            "UPDATE association_sell_preparations SET latest_evaluation=?1 WHERE signature='sell'",
            [serde_json::to_string(&stale)?],
        )?;
        let read = f.read()?;
        assert_eq!(
            read.historical_latest.selected_chain,
            Check::ProviderOrderedWithinBlock
        );
        assert_eq!(
            read.current.selected_chain,
            Check::Blocked(Reason::DecimalsConflict)
        );
        assert_eq!(read.first, p.first);
    }
    Ok(())
}
#[test]
fn b90r1_missing_exact_stays_unknown_and_other_known_conflict_still_blocks() -> Result<()> {
    for source_missing in [true, false] {
        let (_, p) = chain(|source, sell| {
            if source_missing {
                source.facts.exact_amounts = None;
            } else {
                sell.facts.exact_amounts = None;
            }
        })?;
        assert_eq!(
            p.current.selected_chain,
            Check::Unknown(Reason::MissingExactAmounts)
        );
    }
    let (_, p) = chain(|source, sell| {
        source.facts.exact_amounts = None;
        sell.facts
            .exact_amounts
            .as_mut()
            .unwrap()
            .amount_in_decimals = 4;
    })?;
    assert_eq!(
        p.current.selected_chain,
        Check::Blocked(Reason::DecimalsConflict)
    );
    Ok(())
}
#[test]
fn b90r1_different_leader_raw_sizes_are_valid_but_noncanonical_or_overflow_are_not() -> Result<()> {
    let (_, p) = chain(|source, sell| {
        source.facts.exact_amounts.as_mut().unwrap().amount_out_raw = "12000".into();
        source.facts.exact_amounts.as_mut().unwrap().amount_in_raw = "1700".into();
        sell.facts.exact_amounts.as_mut().unwrap().amount_in_raw = "2000".into();
    })?;
    assert_eq!(p.current.selected_chain, Check::ProviderOrderedWithinBlock);
    for raw in ["07000", "0", "340282366920938463463374607431768211456"] {
        let mut f = F::new()?;
        let mut source = facts("leaderbuy", "leader", true);
        source.facts.exact_amounts.as_mut().unwrap().amount_out_raw = raw.into();
        f.admit(source.clone())?;
        f.terminal(&source, 1, 42, "block")?;
        let our = facts(&f.our, "execution-wallet", true);
        f.admit(our.clone())?;
        f.terminal(&our, 2, 42, "block")?;
        f.sell()?;
        assert_eq!(
            f.read()?.current.selected_chain,
            Check::Blocked(Reason::AmountConflict)
        );
    }
    Ok(())
}
