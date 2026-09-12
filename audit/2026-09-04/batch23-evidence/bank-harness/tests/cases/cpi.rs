use crate::support::*;
use anyhow::Result;

#[test]
fn final_floor_real_system_cpi_success_and_rollback() -> Result<()> {
    let f = Fixture::new(WALLET);
    for remaining in [RESERVE - 1, RESERVE, RESERVE + 1] {
        let debit = WALLET - FEE - remaining;
        let without = f.run(
            &format!("cpi_absent_{remaining}"),
            &[f.debit(debit, true)],
            None,
        )?;
        assert_eq!(without.status, Ok(()));
        assert_eq!(
            (without.wallet, without.recipient),
            (remaining, RECIPIENT + debit)
        );
        let out = f.run(
            &format!("cpi_last_{remaining}"),
            &[f.debit(debit, true), f.guard(RESERVE)],
            Some(1),
        )?;
        assert!(out
            .logs
            .iter()
            .any(|l| l == "Program 11111111111111111111111111111111 invoke [2]"));
        assert!(out
            .logs
            .iter()
            .any(|l| l == "Program 11111111111111111111111111111111 success"));
        if remaining < RESERVE {
            assert_eq!(out.status, insufficient(1));
            assert_eq!((out.wallet, out.recipient), (WALLET - FEE, RECIPIENT));
        } else {
            assert_eq!(out.status, Ok(()));
            assert_eq!((out.wallet, out.recipient), (remaining, RECIPIENT + debit));
        }
    }
    Ok(())
}
