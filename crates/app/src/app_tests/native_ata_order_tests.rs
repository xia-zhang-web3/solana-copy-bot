use super::native_ata_fixture::*;
use super::native_funding_fixture::transfer;
use anyhow::Result;

#[tokio::test]
async fn native_ata_prior_writers_and_opaque_cpi_do_not_reuse_initial_state() -> Result<()> {
    let mint = [73; 32];
    let create = create(mint);
    let account = create.accounts[1].pubkey;
    for case in ["duplicate", "close", "transfer", "opaque", "sync"] {
        let mut ix = budget();
        match case {
            "duplicate" => ix.push(create.clone()),
            "close" => {
                ix.push(create.clone());
                ix.push(sync_close(account, WALLET).pop().unwrap());
            }
            "transfer" => ix.push(transfer(WALLET, account, 7)),
            "opaque" => ix.push(direct(false, true, 7)?[2].clone()),
            "sync" => ix.push(sync_close(account, WALLET)[2].clone()),
            _ => unreachable!(),
        }
        ix.push(create.clone());
        let p = payload(&ix)?;
        for existing in [false, true] {
            let mut input = Inputs::new(&p, 2_039_280)?;
            if existing {
                input.set(
                    account,
                    present(1, token_program_id(), token_bytes(mint, WALLET, 0, None)),
                );
            }
            let facts = input.collect(&p).await?;
            let out = plan(&p, WALLET, &facts)?;
            assert_eq!(out.rows.last().unwrap().requirement_index, ix.len() - 1);
            assert_eq!(
                out.rows.last().unwrap().amount,
                AtaFundingAmount::Unresolved(if case == "opaque" {
                    AtaFundingIssue::PriorOpaqueInstruction
                } else {
                    AtaFundingIssue::PriorAccountWrite
                }),
                "{case} existing={existing}"
            );
            assert_eq!(out.explicit_ata_coverage, ExplicitAtaCoverage::Partial);
            assert_eq!(
                out.known_wallet_payer_lamports,
                if ["duplicate", "close"].contains(&case) && !existing {
                    2_039_280
                } else {
                    0
                }
            );
            assert_unknown(&out.setup);
        }
    }
    Ok(())
}

#[tokio::test]
async fn native_ata_actual_builder_buy_sell_and_extension_funding_matrix() -> Result<()> {
    let wsol = associated_token_address(&WALLET, &wsol_mint(), &token_program_id());
    let mint = parse_pubkey(TOKEN, "test")?;
    let token = associated_token_address(&WALLET, &mint, &token_program_id());
    for buy in [false, true] {
        for extension in [false, true] {
            for state in ["absent", "prefunded", "existing"] {
                let ix = direct(buy, extension, 10_000_000)?;
                let p = payload(&ix)?;
                let mut input = Inputs::new(&p, 2_039_280)?;
                for (key, mint) in [(wsol, wsol_mint()), (token, mint)] {
                    if state == "prefunded" && input.rows.iter().any(|r| r.pubkey == key) {
                        input.set(key, present(39_280, system_program_id(), vec![]));
                    }
                    if state == "existing" && input.rows.iter().any(|r| r.pubkey == key) {
                        input.set(
                            key,
                            present(
                                2_039_290,
                                token_program_id(),
                                token_bytes(
                                    mint,
                                    WALLET,
                                    10,
                                    (mint == wsol_mint()).then_some(2_039_280),
                                ),
                            ),
                        );
                    }
                }
                let facts = input.collect(&p).await?;
                let out = plan(&p, WALLET, &facts)?;
                let n = if buy { 2 } else { 1 };
                assert_eq!(out.rows.len(), n);
                let one = match state {
                    "absent" => 2_039_280,
                    "prefunded" => 2_000_000,
                    _ => 0,
                };
                if extension {
                    assert!(out.rows.iter().all(|r| r.amount
                        == AtaFundingAmount::Unresolved(AtaFundingIssue::PriorOpaqueInstruction)));
                    assert_eq!(out.explicit_ata_coverage, ExplicitAtaCoverage::Partial);
                    assert_eq!(out.known_wallet_payer_lamports, 0);
                } else {
                    assert!(out
                        .rows
                        .iter()
                        .all(|r| r.amount == AtaFundingAmount::Known(one)));
                    assert_eq!(out.known_wallet_payer_lamports, u128::from(one) * n as u128);
                    assert_eq!(out.explicit_ata_coverage, ExplicitAtaCoverage::Complete);
                    assert_eq!(
                        out.rows
                            .iter()
                            .map(|r| r.requirement_index)
                            .collect::<Vec<_>>(),
                        if buy { vec![2, 5] } else { vec![2] }
                    );
                }
                // BUY's transfer/sync only changes WSOL; the second ATA retains its initial proof.
                // Opaque swap/optional extension and all nine full reserve fields remain unknown.
                assert_unknown(&out.setup);
                assert_eq!(facts.native().fee().value, None);
            }
        }
    }
    Ok(())
}
