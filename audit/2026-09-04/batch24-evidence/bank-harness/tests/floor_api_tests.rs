#[path = "../../../batch23-evidence/bank-harness/tests/support/cpi.rs"]
mod cpi;
#[path = "../../../../../crates/app/src/execution_native_floor.rs"]
mod execution_native_floor;
#[path = "../../../../../crates/app/src/execution_solana_tx.rs"]
mod execution_solana_tx;
#[path = "../../../../../crates/app/src/execution_transaction_wire.rs"]
mod execution_transaction_wire;
mod support;
use anyhow::Result;
use support::*;

#[test]
fn native_floor_bank_constructor_direct_and_cpi_controls() -> Result<()> {
    let f = Fixture::new(WALLET);
    for cpi in [false, true] {
        for remaining in [RESERVE - 1, RESERVE, RESERVE + 1] {
            let debit = WALLET - FEE - remaining;
            let instructions = [f.debit(debit, cpi)];
            let absent = f.run(&format!("absent_cpi{cpi}_{remaining}"), &instructions, None)?;
            assert_eq!(absent.status, Ok(()));
            assert_eq!(
                (absent.wallet, absent.recipient),
                (remaining, RECIPIENT + debit)
            );
            let out = f.run(
                &format!("prepared_cpi{cpi}_{remaining}"),
                &instructions,
                Some(RESERVE),
            )?;
            if remaining < RESERVE {
                assert_eq!(out.status, insufficient(1));
                assert_eq!((out.wallet, out.recipient), (WALLET - FEE, RECIPIENT));
                assert!(out
                    .logs
                    .iter()
                    .any(|l| l == "Transfer: insufficient lamports 1999999, need 2000000"));
            } else {
                assert_eq!(out.status, Ok(()));
                assert_eq!((out.wallet, out.recipient), (remaining, RECIPIENT + debit));
            }
            if cpi {
                assert!(out
                    .logs
                    .iter()
                    .any(|l| l == "Program 11111111111111111111111111111111 invoke [2]"));
                assert!(out
                    .logs
                    .iter()
                    .any(|l| l == "Program 11111111111111111111111111111111 success"));
            }
        }
    }
    Ok(())
}

#[test]
fn native_floor_bank_constructor_failed_fee_exception() -> Result<()> {
    let f = Fixture::new(RESERVE + FEE - 1);
    let out = f.run("prepared_failed_fee", &[], Some(RESERVE))?;
    assert_eq!(out.status, insufficient(0));
    assert_eq!((out.wallet, out.recipient), (RESERVE - 1, RECIPIENT));
    Ok(())
}
