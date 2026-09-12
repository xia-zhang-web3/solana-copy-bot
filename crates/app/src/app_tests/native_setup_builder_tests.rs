use super::native_setup_fixture::*;
use crate::execution_native_funding::types::{FundingOperation, UnsupportedFundingInstruction};
use anyhow::Result;

#[tokio::test]
async fn native_setup_actual_direct_buy_sell_extension_and_initial_account_matrix() -> Result<()> {
    let base = associated_token_address(&WALLET, &wsol_mint(), &token_program_id());
    let mint = parse_pubkey(TOKEN, "fixture")?;
    let quote = associated_token_address(&WALLET, &mint, &token_program_id());
    for buy in [false, true] {
        for extension in [false, true] {
            let instructions = direct(buy, extension, 10_000_000)?;
            let payload = payload(&instructions)?;
            for mode in 0..3 {
                let mut raw = responses(&payload)?;
                let expected_ata = match mode {
                    0 => AssociatedInitialState::CreationRequiredAbsent,
                    1 => AssociatedInitialState::CreationCandidateSystemPrefunded,
                    _ => AssociatedInitialState::ExistingIdentityMatch,
                };
                for (key, mint) in [(base, wsol_mint()), (quote, mint)] {
                    if mode == 1 {
                        set(&mut raw, key, present(0, system_program_id(), vec![]));
                    }
                    if mode == 2 {
                        set(
                            &mut raw,
                            key,
                            present(
                                100,
                                token_program_id(),
                                token_bytes(mint, WALLET, 10, (mint == wsol_mint()).then_some(2)),
                            ),
                        );
                    }
                }
                // Other static mint/program/CPI operands are not decoded as token accounts.
                set(
                    &mut raw,
                    mint,
                    present(12, token_program_id(), vec![255; 165]),
                );
                let facts = raw.collect(&payload).await?;
                let before = facts.clone();
                let result = interpret(&payload, WALLET, &facts)?;
                assert_eq!(result.initial_accounts.len(), if buy { 2 } else { 1 });
                assert_eq!(result.instructions.len(), instructions.len());
                let mut order = Vec::new();
                for (index, (row, original)) in
                    result.instructions.iter().zip(&instructions).enumerate()
                {
                    assert_eq!(row.requirement_index, index);
                    let requirement = &result.facts.requirements().requirements[index];
                    assert_eq!(requirement.instruction.index, index);
                    assert_eq!(requirement.instruction.data, original.data);
                    assert_eq!(
                        requirement
                            .instruction
                            .accounts
                            .iter()
                            .map(|a| a.pubkey)
                            .collect::<Vec<_>>(),
                        original
                            .accounts
                            .iter()
                            .map(|a| a.pubkey)
                            .collect::<Vec<_>>()
                    );
                    match &requirement.operation {
                        FundingOperation::AssociatedTokenCreateIdempotent {
                            payer,
                            owner,
                            associated_account,
                            ..
                        } => {
                            assert_eq!((*payer, *owner), (WALLET, WALLET));
                            assert_eq!(
                                row.interpretation,
                                SetupOperation::Associated(expected_ata)
                            );
                            assert_eq!(
                                result.initial_accounts[row.initial_account_index.unwrap()].pubkey,
                                *associated_account
                            );
                            order.push("ata");
                        }
                        FundingOperation::SyncNative { .. } => {
                            assert_eq!(row.initial_account_index, Some(0));
                            assert_eq!(
                                row.interpretation,
                                SetupOperation::Sync(if mode == 2 {
                                    SyncInitialState::ObservedWsol
                                } else {
                                    SyncInitialState::NoClassicInitialState
                                })
                            );
                            order.push("sync");
                        }
                        FundingOperation::CloseTokenAccount {
                            account,
                            destination,
                            authority,
                            ..
                        } => {
                            assert_eq!(
                                (*account, *destination, *authority),
                                (base, WALLET, WALLET)
                            );
                            assert_eq!(row.initial_account_index, Some(0));
                            let SetupOperation::Close(close) = &row.interpretation else {
                                panic!("close");
                            };
                            assert_eq!(
                                close.balance,
                                if mode == 2 {
                                    CloseInitialBalance::ObservedNative
                                } else {
                                    CloseInitialBalance::NoClassicInitialState
                                }
                            );
                            order.push("close");
                        }
                        FundingOperation::Unresolved { reason } => {
                            assert_eq!(
                                *reason,
                                UnsupportedFundingInstruction::ProgramSemanticsAndCpiExpenses
                            );
                            assert_eq!(row.interpretation, SetupOperation::NotSetup);
                            assert_eq!(row.initial_account_index, None);
                            order.push("unresolved");
                        }
                        FundingOperation::SystemTransfer { .. } => order.push("transfer"),
                        FundingOperation::ComputeBudget => order.push("cu"),
                    }
                }
                let mut expected_order = vec!["cu", "cu"];
                if extension {
                    expected_order.push("unresolved");
                }
                expected_order.extend(if buy {
                    vec!["ata", "transfer", "sync", "ata", "unresolved", "close"]
                } else {
                    vec!["ata", "unresolved", "close"]
                });
                assert_eq!(order, expected_order);
                for account in &result.initial_accounts {
                    assert_eq!(
                        account.pubkey,
                        facts.accounts().value[account.observation_index].pubkey
                    );
                }
                assert_eq!(*result.facts, before);
                assert_unknown(&result);
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn native_setup_repeated_create_and_incoming_sol_never_project_post_instruction_state(
) -> Result<()> {
    let mut instructions = direct(true, false, 999_999)?;
    let create = instructions[2].clone();
    instructions.push(create.clone()); // after CPI/close, still the same initial state.
    instructions.push(super::native_funding_fixture::transfer(
        WALLET,
        WALLET,
        u64::MAX,
    ));
    let payload = payload(&instructions)?;
    let original = responses(&payload)?.collect(&payload).await?;
    let result = interpret(&payload, WALLET, &original)?;
    let refs: Vec<_> = result
        .instructions
        .iter()
        .filter(|row| row.initial_account_index == Some(0))
        .collect();
    assert_eq!(refs.len(), 4); // create / sync / close / create
    assert!(matches!(
        result.initial_accounts[0].state,
        InitialAccountState::Absent
    ));
    assert_eq!(refs[0].interpretation, refs[3].interpretation);
    assert_eq!(
        refs[1].interpretation,
        SetupOperation::Sync(SyncInitialState::NoClassicInitialState)
    );
    let SetupOperation::Close(close) = &refs[2].interpretation else {
        panic!("close");
    };
    assert_eq!(close.balance, CloseInitialBalance::NoClassicInitialState);
    assert_unknown(&result);
    // True foreign-source incoming SOL has a second encoded signer; no signature proof.
    let incoming = super::native_funding_fixture::foreign_signer_transfer(WALLET, u64::MAX)?;
    let original = responses(&incoming)?.collect(&incoming).await?;
    let result = interpret(&incoming, WALLET, &original)?;
    assert!(result.initial_accounts.is_empty());
    assert_eq!(
        result.facts.requirements().unavailable_budget,
        crate::execution_native_funding::types::UnavailableNativeBudget::default()
    );
    assert_eq!(
        result
            .facts
            .requirements()
            .nominal_wallet_source_transfer_operands_lamports,
        0
    );
    Ok(())
}
