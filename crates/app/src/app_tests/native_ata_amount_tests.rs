use super::native_ata_fixture::*;
use anyhow::Result;

#[tokio::test]
async fn native_ata_rent_exact_absent_prefunding_and_zero_floor() -> Result<()> {
    let mint = [73; 32];
    let account = associated_token_address(&WALLET, &mint, &token_program_id());
    let mut ix = budget();
    ix.push(create(mint));
    let p = payload(&ix)?;
    for rent in [0, 1, 2_039_280, 9_007_199_254_740_993, u64::MAX] {
        for prefund in [
            None,
            Some(0),
            Some(1),
            Some(2_039_279),
            Some(2_039_280),
            Some(2_039_281),
            Some(u64::MAX),
        ] {
            let mut input = Inputs::new(&p, rent)?;
            if let Some(prefund) = prefund {
                input.set(account, present(prefund, system_program_id(), vec![]));
            }
            let facts = input.collect(&p).await?;
            let out = plan(&p, WALLET, &facts)?;
            let expected = rent.max(1).saturating_sub(prefund.unwrap_or(0));
            assert_eq!(out.rows.len(), 1);
            assert_eq!(out.rows[0].requirement_index, 2);
            assert_eq!((out.rows[0].payer, out.rows[0].account), (WALLET, account));
            assert_eq!(
                out.rows[0].amount,
                AtaFundingAmount::Known(expected),
                "rent={rent} prefund={prefund:?}"
            );
            assert_eq!(out.known_wallet_payer_lamports, u128::from(expected));
            assert_eq!(out.explicit_ata_coverage, ExplicitAtaCoverage::Complete);
            assert_eq!(facts.rent().lamports(), rent);
            assert_eq!(facts.native().fee().value, None);
            assert_unknown(&out.setup);
        }
    }
    Ok(())
}

#[tokio::test]
async fn native_ata_sum_exceeds_u64_and_foreign_payer_does_not_charge_wallet() -> Result<()> {
    let mut ix = budget();
    ix.extend([create([73; 32]), create([74; 32])]);
    let p = payload(&ix)?;
    let mut input = Inputs::new(&p, u64::MAX)?;
    input.fee = Some(u64::MAX);
    let facts = input.collect(&p).await?;
    let out = plan(&p, WALLET, &facts)?;
    assert_eq!(out.known_wallet_payer_lamports, 2 * u128::from(u64::MAX));
    assert_eq!(
        out.rows
            .iter()
            .map(|r| r.requirement_index)
            .collect::<Vec<_>>(),
        [2, 3]
    );
    assert_eq!(facts.native().fee().value, Some(u64::MAX));
    assert_unknown(&out.setup);
    let p = foreign_payer(&mut ix)?;
    let facts = Inputs::new(&p, 2_039_280)?.collect(&p).await?;
    let out = plan(&p, WALLET, &facts)?;
    assert_eq!((out.rows[0].payer, out.rows[1].payer), (PEER, WALLET));
    assert_eq!(out.rows[0].amount, AtaFundingAmount::Known(2_039_280));
    assert_eq!(out.known_wallet_payer_lamports, 2_039_280);
    assert_eq!(out.explicit_ata_coverage, ExplicitAtaCoverage::Complete);
    assert_eq!(
        facts.native().requirements().binding.accounts[0].pubkey,
        WALLET
    );
    Ok(())
}

#[tokio::test]
async fn native_ata_existing_initialized_and_frozen_zero_keep_initial_state() -> Result<()> {
    let mint = [73; 32];
    let key = associated_token_address(&WALLET, &mint, &token_program_id());
    let mut ix = budget();
    ix.push(create(mint));
    let p = payload(&ix)?;
    for state in [1, 2] {
        let mut bytes = token_bytes(mint, WALLET, u64::MAX, None);
        bytes[108] = state;
        let mut input = Inputs::new(&p, u64::MAX)?;
        input.set(key, present(0, token_program_id(), bytes));
        let facts = input.collect(&p).await?;
        let out = plan(&p, WALLET, &facts)?;
        assert_eq!(out.rows[0].amount, AtaFundingAmount::Known(0));
        assert_eq!(out.known_wallet_payer_lamports, 0);
        let InitialAccountState::Classic { token, .. } = &out.setup.initial_accounts[0].state
        else {
            panic!("classic")
        };
        assert_eq!(
            token.state,
            if state == 1 {
                ClassicAccountState::Initialized
            } else {
                ClassicAccountState::Frozen
            }
        );
        assert_unknown(&out.setup);
    }
    Ok(())
}

#[tokio::test]
async fn native_ata_invalid_identity_program_layout_remain_partial_even_with_known_zero(
) -> Result<()> {
    let mint = [73; 32];
    let key = associated_token_address(&WALLET, &mint, &token_program_id());
    for case in [
        "derive",
        "mint",
        "owner",
        "program",
        "executable",
        "length",
        "uninitialized",
        "token2022",
        "system_data",
    ] {
        let mut bad = create(mint);
        if case == "derive" {
            bad.accounts[1].pubkey = PEER;
        }
        if case == "token2022" {
            let program = parse_pubkey("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb", "test")?;
            bad.accounts[5].pubkey = program;
            bad.accounts[1].pubkey = associated_token_address(&WALLET, &mint, &program);
        }
        let mut ix = budget();
        ix.extend([create([74; 32]), bad]);
        let p = payload(&ix)?;
        let mut input = Inputs::new(&p, 2_039_280)?;
        let zero = associated_token_address(&WALLET, &[74; 32], &token_program_id());
        input.set(
            zero,
            present(
                100,
                token_program_id(),
                token_bytes([74; 32], WALLET, 0, None),
            ),
        );
        if !["derive", "token2022"].contains(&case) {
            let mut bytes = token_bytes(mint, WALLET, 0, None);
            match case {
                "mint" => bytes[..32].fill(1),
                "owner" => bytes[32..64].fill(1),
                "length" => {
                    bytes.pop();
                }
                "uninitialized" => bytes[108] = 0,
                _ => {}
            }
            let mut a = present(
                0,
                if case == "program" {
                    PEER
                } else if case == "system_data" {
                    system_program_id()
                } else {
                    token_program_id()
                },
                bytes,
            );
            if let AccountObservation::Present { executable, .. } = &mut a {
                *executable = case == "executable";
            }
            input.set(key, a);
        }
        let facts = input.collect(&p).await?;
        let out = plan(&p, WALLET, &facts)?;
        assert_eq!(out.rows[0].amount, AtaFundingAmount::Known(0));
        assert!(
            matches!(
                out.rows[1].amount,
                AtaFundingAmount::Unresolved(AtaFundingIssue::InitialState(_))
            ),
            "{case}: {:?}",
            out.rows
        );
        assert_eq!(out.explicit_ata_coverage, ExplicitAtaCoverage::Partial);
        assert_eq!(out.known_wallet_payer_lamports, 0);
    }
    Ok(())
}
