use super::native_setup_fixture::*;
use anyhow::Result;

#[tokio::test]
async fn native_setup_ata_absent_prefunded_exact_and_existing_identity_with_distinct_payer(
) -> Result<()> {
    let mint = wsol_mint();
    let address = associated_token_address(&PEER, &mint, &token_program_id());
    let payload = setup_payload(PEER, mint, token_program_id(), address)?;
    let mut raw = responses(&payload)?;
    let facts = raw.collect(&payload).await?;
    let value = interpret(&payload, WALLET, &facts)?;
    assert_eq!(
        value.instructions[2].interpretation,
        SetupOperation::Associated(AssociatedInitialState::CreationRequiredAbsent)
    );
    assert_eq!(value.initial_accounts[0].observed_lamports, None);
    for amount in [0, (1_u64 << 53) + 1, u64::MAX] {
        set(
            &mut raw,
            address,
            present(amount, system_program_id(), vec![]),
        );
        let facts = raw.collect(&payload).await?;
        let value = interpret(&payload, WALLET, &facts)?;
        assert_eq!(
            value.instructions[2].interpretation,
            SetupOperation::Associated(AssociatedInitialState::CreationCandidateSystemPrefunded)
        );
        assert_eq!(value.initial_accounts[0].observed_lamports, Some(amount));
        assert_unknown(&value);
    }
    for (state, expected) in [
        (0, AssociatedInitialState::Uninitialized),
        (1, AssociatedInitialState::ExistingIdentityMatch),
        (2, AssociatedInitialState::ExistingIdentityMatch),
    ] {
        let mut bytes = token_bytes(mint, PEER, 17, Some(2));
        bytes[108] = state;
        set(&mut raw, address, present(19, token_program_id(), bytes));
        let facts = raw.collect(&payload).await?;
        let value = interpret(&payload, WALLET, &facts)?;
        assert_eq!(
            value.instructions[2].interpretation,
            SetupOperation::Associated(expected)
        );
        let InitialAccountState::Classic { token, .. } = &value.initial_accounts[0].state else {
            panic!("classic");
        };
        assert_eq!(token.token_owner, PEER);
        assert_eq!(
            token.state,
            [
                ClassicAccountState::Uninitialized,
                ClassicAccountState::Initialized,
                ClassicAccountState::Frozen
            ][usize::from(state)]
        );
        assert_unknown(&value);
    }
    Ok(())
}

#[tokio::test]
async fn native_setup_ata_rejects_wrong_derived_address_owner_mint_and_token_program() -> Result<()>
{
    let mint = wsol_mint();
    let address = associated_token_address(&WALLET, &mint, &token_program_id());
    let wrong_payload = setup_payload(WALLET, mint, token_program_id(), PEER)?;
    let wrong = responses(&wrong_payload)?.collect(&wrong_payload).await?;
    assert_eq!(
        interpret(&wrong_payload, WALLET, &wrong)?.instructions[2].interpretation,
        SetupOperation::Associated(AssociatedInitialState::WrongDerivedAddress)
    );
    let payload = setup_payload(WALLET, mint, token_program_id(), address)?;
    let mut raw = responses(&payload)?;
    for (owner, token_mint, expected) in [
        (PEER, mint, AssociatedInitialState::WrongTokenOwner),
        (WALLET, PEER, AssociatedInitialState::WrongMint),
    ] {
        set(
            &mut raw,
            address,
            present(
                100,
                token_program_id(),
                token_bytes(token_mint, owner, 0, None),
            ),
        );
        let value = raw.collect(&payload).await?;
        assert_eq!(
            interpret(&payload, WALLET, &value)?.instructions[2].interpretation,
            SetupOperation::Associated(expected)
        );
    }
    let token2022 = parse_pubkey("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb", "test")?;
    let address2022 = associated_token_address(&WALLET, &mint, &token2022);
    let payload2022 = setup_payload(WALLET, mint, token2022, address2022)?;
    for observation in [
        AccountObservation::Absent,
        present(0, system_program_id(), vec![]),
        present(100, token2022, token_bytes(mint, WALLET, 0, None)),
    ] {
        let mut raw = responses(&payload2022)?;
        set(&mut raw, address2022, observation);
        let value = raw.collect(&payload2022).await?;
        assert_eq!(
            interpret(&payload2022, WALLET, &value)?.instructions[2].interpretation,
            SetupOperation::Associated(AssociatedInitialState::UnsupportedTokenProgram)
        );
    }
    // Even with a classic instruction operand, Token-2022 data is never classic.
    set(
        &mut raw,
        address,
        present(100, token2022, token_bytes(mint, WALLET, 0, None)),
    );
    let value = raw.collect(&payload).await?;
    let result = interpret(&payload, WALLET, &value)?;
    assert_eq!(
        result.initial_accounts[0].state,
        InitialAccountState::Unsupported(AccountIssue::ProgramOwner)
    );
    assert_eq!(
        result.instructions[2].interpretation,
        SetupOperation::Associated(AssociatedInitialState::UnsupportedInitialAccount)
    );
    Ok(())
}

#[tokio::test]
async fn native_setup_account_layout_failures_are_local_and_keep_other_observations() -> Result<()>
{
    let a = associated_token_address(&WALLET, &wsol_mint(), &token_program_id());
    let b = associated_token_address(&PEER, &wsol_mint(), &token_program_id());
    let mut instructions = budget();
    instructions.push(ata(WALLET, wsol_mint(), token_program_id(), a));
    instructions.push(ata(PEER, wsol_mint(), token_program_id(), b));
    let payload = payload(&instructions)?;
    let mut raw = responses(&payload)?;
    set(
        &mut raw,
        b,
        present(
            99,
            token_program_id(),
            token_bytes(wsol_mint(), PEER, 90, Some(9)),
        ),
    );
    let valid = token_bytes(wsol_mint(), WALLET, 0, Some(0));
    let mut cases = vec![
        (
            present(0, token_program_id(), vec![0; 164]),
            AccountIssue::ClassicLength,
        ),
        (
            present(0, token_program_id(), vec![0; 166]),
            AccountIssue::ClassicLength,
        ),
        (
            present(0, system_program_id(), vec![0]),
            AccountIssue::SystemDataNotEmpty,
        ),
        (present(0, PEER, valid.clone()), AccountIssue::ProgramOwner),
        (
            AccountObservation::Present {
                lamports: 0,
                owner_program: token_program_id(),
                executable: true,
                data: valid.clone(),
            },
            AccountIssue::Executable,
        ),
    ];
    for state in [3, 255] {
        let mut bytes = valid.clone();
        bytes[108] = state;
        cases.push((
            present(0, token_program_id(), bytes),
            AccountIssue::ClassicState,
        ));
    }
    for (offset, reason) in [
        (72, AccountIssue::DelegateTag),
        (109, AccountIssue::NativeTag),
        (129, AccountIssue::CloseAuthorityTag),
    ] {
        for tag in [[2, 0, 0, 0], [1, 1, 0, 0], [0, 0, 0, 1]] {
            let mut bytes = valid.clone();
            bytes[offset..offset + 4].copy_from_slice(&tag);
            cases.push((present(0, token_program_id(), bytes), reason));
        }
    }
    for (observation, reason) in cases {
        set(&mut raw, a, observation);
        let facts = raw.collect(&payload).await?;
        let before = facts.clone();
        let value = interpret(&payload, WALLET, &facts)?;
        assert_eq!(
            value.initial_accounts[0].state,
            InitialAccountState::Unsupported(reason)
        );
        assert_eq!(
            value.instructions[2].interpretation,
            SetupOperation::Associated(AssociatedInitialState::UnsupportedInitialAccount)
        );
        assert_eq!(
            value.instructions[3].interpretation,
            SetupOperation::Associated(AssociatedInitialState::ExistingIdentityMatch)
        );
        assert_eq!(*value.facts, before);
        assert_unknown(&value);
    }
    Ok(())
}

#[tokio::test]
async fn native_setup_classic_exact_options_and_ignored_none_payloads() -> Result<()> {
    let payload = payload(&sync_close(PEER, WALLET))?;
    for amount in [0, (1_u64 << 53) + 1, u64::MAX] {
        for some in [false, true] {
            let mut raw = responses(&payload)?;
            let mut bytes = token_bytes(wsol_mint(), WALLET, 0, Some(amount));
            if some {
                key_option(&mut bytes, 72, [33; 32]);
                key_option(&mut bytes, 129, [44; 32]);
            } else {
                bytes[76..108].fill(33);
                bytes[133..165].fill(44);
                bytes[109..113].fill(0); // None with nonzero reserve payload is legal.
            }
            bytes[64..72].copy_from_slice(&amount.to_le_bytes());
            bytes[121..129].copy_from_slice(&amount.to_le_bytes());
            set(&mut raw, PEER, present(amount, token_program_id(), bytes));
            let facts = raw.collect(&payload).await?;
            let result = interpret(&payload, WALLET, &facts)?;
            let InitialAccountState::Classic { token, .. } = &result.initial_accounts[0].state
            else {
                panic!("classic");
            };
            assert_eq!(
                (
                    token.mint,
                    token.token_owner,
                    token.amount,
                    token.delegated_amount
                ),
                (wsol_mint(), WALLET, amount, amount)
            );
            assert_eq!(token.native_reserve, some.then_some(amount));
            assert_eq!(token.delegate, some.then_some([33; 32]));
            assert_eq!(token.close_authority, some.then_some([44; 32]));
            assert_eq!(result.initial_accounts[0].observed_lamports, Some(amount));
            assert_unknown(&result);
        }
    }
    Ok(())
}
