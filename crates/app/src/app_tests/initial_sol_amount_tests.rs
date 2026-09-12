use super::native_ata_fixture::*;
use super::native_funding_fixture::{foreign_signer_transfer, transfer};
use crate::execution_initial_sol::check;
use anyhow::Result;

#[tokio::test]
async fn initial_sol_exact_threshold_transfers_ata_and_refunds() -> Result<()> {
    let reserve = 50_000_001;
    let mint = [73; 32];
    let ata = associated_token_address(&WALLET, &mint, &token_program_id());
    for state in ["absent", "prefunded", "existing"] {
        let mut ix = budget();
        ix.push(create(mint));
        ix.extend([transfer(WALLET, PEER, 30), transfer(WALLET, PEER, 70)]);
        ix.extend(sync_close(ata, WALLET).into_iter().skip(2));
        ix.push(transfer(WALLET, WALLET, reserve));
        let p = payload(&ix)?;
        let ata_cost = match state {
            "absent" => 1000,
            "prefunded" => 600,
            _ => 0,
        };
        let expected = reserve + 19 + 100 + ata_cost;
        for short in [0, 1] {
            let mut input = Inputs::new(&p, 1000)?;
            input.fee = Some(19); // Encoded priority is deliberately larger: never add it again.
            input.set(
                WALLET,
                present(expected - short, system_program_id(), vec![]),
            );
            match state {
                "prefunded" => input.set(ata, present(400, system_program_id(), vec![])),
                "existing" => input.set(
                    ata,
                    present(
                        9_000_000,
                        token_program_id(),
                        token_bytes(mint, WALLET, 0, None),
                    ),
                ),
                _ => {}
            }
            let facts = input.collect(&p).await?;
            let out = check(&p, WALLET, reserve, &facts);
            if short == 1 {
                assert_eq!(
                    out.unwrap_err().to_string(),
                    format!(
                        "initial_sol_insufficient:observed={}:required={expected}:shortfall=1",
                        expected - 1
                    )
                );
            } else {
                let out = out?;
                assert_eq!(
                    (out.required, out.classic_ata, out.outgoing_transfers),
                    (u128::from(expected), u128::from(ata_cost), 100)
                );
                assert_eq!(out.total_fee, 19);
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn initial_sol_foreign_transfers_zero_fee_and_u64_excess_are_not_credits() -> Result<()> {
    for to in [WALLET, [53; 32]] {
        let p = foreign_signer_transfer(to, u64::MAX)?;
        let mut input = Inputs::new(&p, 1000)?;
        input.fee = Some(0);
        input.set(WALLET, present(1, system_program_id(), vec![]));
        let facts = input.collect(&p).await?;
        let out = check(&p, WALLET, 1, &facts)?;
        assert_eq!(
            (out.required, out.outgoing_transfers, out.total_fee),
            (1, 0, 0)
        );
    }
    let mut ix = budget();
    ix.extend([
        create([73; 32]),
        create([74; 32]),
        transfer(WALLET, PEER, u64::MAX),
    ]);
    let p = payload(&ix)?;
    let mut input = Inputs::new(&p, u64::MAX)?;
    input.fee = Some(u64::MAX);
    input.set(WALLET, present(u64::MAX, system_program_id(), vec![]));
    let facts = input.collect(&p).await?;
    let error = check(&p, WALLET, u64::MAX, &facts).unwrap_err().to_string();
    assert!(
        error.contains(&format!("required={}", 5 * u128::from(u64::MAX))),
        "{error}"
    );
    Ok(())
}

#[tokio::test]
async fn initial_sol_payer_fee_binding_and_partial_setup_fail_closed() -> Result<()> {
    for case in [
        "absent",
        "owner",
        "executable",
        "data",
        "fee",
        "binding",
        "extension",
        "token2022",
    ] {
        let mut ix = direct(true, case == "extension", 10_000_000)?;
        if case == "token2022" {
            // Explicit ATA token program is unsupported even with abundant lamports.
            ix[2].accounts[5].pubkey =
                parse_pubkey("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb", "test")?;
        }
        let p = payload(&ix)?;
        let mut input = Inputs::new(&p, 1000)?;
        input.fee = Some(19);
        let mut payer = present(u64::MAX, system_program_id(), vec![]);
        match &mut payer {
            AccountObservation::Present {
                owner_program,
                executable,
                data,
                ..
            } => match case {
                "owner" => *owner_program = PEER,
                "executable" => *executable = true,
                "data" => data.push(1),
                _ => {}
            },
            _ => unreachable!(),
        }
        if case != "absent" {
            input.set(WALLET, payer);
        }
        if case == "fee" {
            input.fee = None;
        }
        let facts = input.collect(&p).await?;
        let actual = if case == "binding" {
            payload(&budget())?
        } else {
            p
        };
        let error = check(&actual, WALLET, 50_000_001, &facts)
            .unwrap_err()
            .to_string();
        let expected = match case {
            "fee" => "initial_sol_fee_unavailable",
            "binding" => "initial_sol_observations_binding",
            "extension" | "token2022" => "initial_sol_unsupported_setup",
            _ => "initial_sol_payer_unavailable",
        };
        assert_eq!(error, expected, "{case}");
    }
    Ok(())
}
