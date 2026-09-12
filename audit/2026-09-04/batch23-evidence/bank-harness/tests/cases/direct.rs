use crate::support::*;
use anyhow::Result;

#[test]
fn final_floor_below_absent_and_last_guard_causal_pair() -> Result<()> {
    let f = Fixture::new(WALLET);
    let debit = WALLET - FEE - RESERVE + 1;
    let absent = f.run("absent_below", &[f.debit(debit, false)], None)?;
    assert_eq!(absent.status, Ok(()));
    assert_eq!(
        (absent.wallet, absent.recipient),
        (RESERVE - 1, RECIPIENT + debit)
    );
    let guarded = f.run(
        "last_below",
        &[f.debit(debit, false), f.guard(RESERVE)],
        Some(1),
    )?;
    assert_eq!(guarded.status, insufficient(1));
    assert_eq!(
        (guarded.wallet, guarded.recipient),
        (WALLET - FEE, RECIPIENT)
    );
    assert!(guarded
        .logs
        .iter()
        .any(|l| l.contains("Transfer: insufficient lamports 1999999, need 2000000")));
    Ok(())
}

#[test]
fn final_floor_exact_above_and_zero_are_net_zero() -> Result<()> {
    let f = Fixture::new(WALLET);
    for remaining in [RESERVE, RESERVE + 1] {
        let debit = WALLET - FEE - remaining;
        let out = f.run(
            if remaining == RESERVE {
                "last_exact"
            } else {
                "last_above"
            },
            &[f.debit(debit, false), f.guard(RESERVE)],
            Some(1),
        )?;
        assert_eq!(out.status, Ok(()));
        assert_eq!((out.wallet, out.recipient), (remaining, RECIPIENT + debit));
    }
    let zero = f.run("zero_reserve", &[f.guard(0)], Some(0))?;
    assert_eq!(zero.status, Ok(()));
    assert_eq!((zero.wallet, zero.recipient), (WALLET - FEE, RECIPIENT));
    Ok(())
}

#[test]
fn final_floor_early_guard_negative_control() -> Result<()> {
    let f = Fixture::new(WALLET);
    let debit = WALLET - FEE - RESERVE + 1;
    let out = f.run(
        "early_below",
        &[f.guard(RESERVE), f.debit(debit, false)],
        Some(0),
    )?;
    assert_eq!(out.status, Ok(()));
    assert_eq!(
        (out.wallet, out.recipient),
        (RESERVE - 1, RECIPIENT + debit)
    );
    Ok(())
}

#[test]
fn final_floor_failed_fee_exception_is_explicit() -> Result<()> {
    let f = Fixture::new(RESERVE + FEE - 1);
    let out = f.run("failed_fee", &[f.guard(RESERVE)], Some(0))?;
    assert_eq!(out.status, insufficient(0));
    assert_eq!((out.wallet, out.recipient), (RESERVE - 1, RECIPIENT));
    assert!(out
        .logs
        .iter()
        .any(|l| l.contains("Transfer: insufficient lamports 1999999, need 2000000")));
    Ok(())
}
