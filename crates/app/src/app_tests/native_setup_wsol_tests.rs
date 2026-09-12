use super::native_setup_fixture::*;
use anyhow::Result;
use base64::{engine::general_purpose::STANDARD, Engine};

#[tokio::test]
async fn native_setup_wsol_sync_distinguishes_initial_unsynced_and_inconsistent_backing(
) -> Result<()> {
    use NativeInitialState as N;
    use SyncInitialState as S;
    let payload = payload(&sync_close(PEER, WALLET))?;
    for (mint, reserve, amount, lamports, expected, sync) in [
        (
            wsol_mint(),
            Some(10),
            90,
            100,
            N::Wsol {
                lamports_minus_reserve: 90,
                synced: true,
            },
            S::ObservedWsol,
        ),
        (
            wsol_mint(),
            Some(10),
            20,
            100,
            N::Wsol {
                lamports_minus_reserve: 90,
                synced: false,
            },
            S::ObservedWsol,
        ),
        (
            wsol_mint(),
            Some(0),
            0,
            0,
            N::Wsol {
                lamports_minus_reserve: 0,
                synced: true,
            },
            S::ObservedWsol,
        ),
        (
            wsol_mint(),
            Some(0),
            u64::MAX,
            u64::MAX,
            N::Wsol {
                lamports_minus_reserve: u64::MAX,
                synced: true,
            },
            S::ObservedWsol,
        ),
        (
            wsol_mint(),
            Some(u64::MAX),
            0,
            u64::MAX,
            N::Wsol {
                lamports_minus_reserve: 0,
                synced: true,
            },
            S::ObservedWsol,
        ),
        (
            wsol_mint(),
            Some(101),
            0,
            100,
            N::ReserveExceedsLamports,
            S::InconsistentNative,
        ),
        (
            wsol_mint(),
            Some(10),
            91,
            100,
            N::AmountExceedsBacking,
            S::InconsistentNative,
        ),
        (
            wsol_mint(),
            None,
            0,
            100,
            N::MintReserveMismatch,
            S::InconsistentNative,
        ),
        (
            [33; 32],
            Some(10),
            0,
            100,
            N::MintReserveMismatch,
            S::InconsistentNative,
        ),
        ([33; 32], None, 0, 100, N::NonNative, S::NonNative),
        ([33; 32], None, u64::MAX, 0, N::NonNative, S::NonNative),
    ] {
        let mut raw = responses(&payload)?;
        set(
            &mut raw,
            PEER,
            present(
                lamports,
                token_program_id(),
                token_bytes(mint, WALLET, amount, reserve),
            ),
        );
        let facts = raw.collect(&payload).await?;
        let before = facts.clone();
        let result = interpret(&payload, WALLET, &facts)?;
        let InitialAccountState::Classic { token, native } = &result.initial_accounts[0].state
        else {
            panic!("classic");
        };
        assert_eq!(*native, expected);
        assert_eq!((token.amount, token.native_reserve), (amount, reserve));
        assert_eq!(
            result.instructions[2].interpretation,
            SetupOperation::Sync(sync)
        );
        let SetupOperation::Close(close) = &result.instructions[3].interpretation else {
            panic!("close");
        };
        assert_eq!(
            close.balance,
            match expected {
                N::NonNative if amount == 0 => CloseInitialBalance::NonNativeZero,
                N::NonNative => CloseInitialBalance::NonNativePositive,
                N::Wsol { .. } => CloseInitialBalance::ObservedNative,
                _ => CloseInitialBalance::InconsistentNative,
            }
        );
        assert_eq!(*result.facts, before);
        assert_unknown(&result);
    }
    Ok(())
}

#[tokio::test]
async fn native_setup_close_effective_authority_is_not_delegate_or_signature_proof() -> Result<()> {
    use CloseAuthorityRelation as A;
    let incinerator = parse_pubkey("1nc1nerator11111111111111111111111111111111", "fixture")?;
    for destination in [WALLET, PEER, [99; 32]] {
        let payload = payload(&sync_close(PEER, destination))?;
        for (owner, close_authority, delegate, expected_key, relation) in [
            (WALLET, None, Some([33; 32]), WALLET, A::MatchesEffectiveKey),
            (
                [44; 32],
                Some(WALLET),
                Some([33; 32]),
                WALLET,
                A::MatchesEffectiveKey,
            ),
            (
                WALLET,
                Some([44; 32]),
                Some(WALLET),
                [44; 32],
                A::ForeignKey,
            ),
            ([44; 32], None, Some(WALLET), [44; 32], A::ForeignKey),
            (
                system_program_id(),
                Some(WALLET),
                None,
                WALLET,
                A::UnsupportedSpecialOwner,
            ),
            (
                incinerator,
                None,
                None,
                incinerator,
                A::UnsupportedSpecialOwner,
            ),
        ] {
            let mut raw = responses(&payload)?;
            let mut bytes = token_bytes(wsol_mint(), owner, 1, Some(9));
            if let Some(key) = close_authority {
                key_option(&mut bytes, 129, key);
            }
            if let Some(key) = delegate {
                key_option(&mut bytes, 72, key);
            }
            set(&mut raw, PEER, present(10, token_program_id(), bytes));
            let facts = raw.collect(&payload).await?;
            let result = interpret(&payload, WALLET, &facts)?;
            let SetupOperation::Close(close) = &result.instructions[3].interpretation else {
                panic!("close");
            };
            assert_eq!(close.effective_authority, Some(expected_key));
            assert_eq!(close.authority_relation, relation);
            assert_eq!(close.source_equals_destination, destination == PEER);
            assert_eq!(close.destination_is_expected_wallet, destination == WALLET);
            assert!(close.authority_is_expected_wallet);
            assert_unknown(&result);
        }
    }
    Ok(())
}

#[tokio::test]
async fn native_setup_sync_close_keep_absent_prefunded_unsupported_and_uninitialized() -> Result<()>
{
    let payload = payload(&sync_close(PEER, WALLET))?;
    for observation in [
        AccountObservation::Absent,
        present(7, system_program_id(), vec![]),
        present(7, [99; 32], vec![0; 165]),
    ] {
        let mut raw = responses(&payload)?;
        set(&mut raw, PEER, observation);
        let facts = raw.collect(&payload).await?;
        let result = interpret(&payload, WALLET, &facts)?;
        assert_eq!(
            result.instructions[2].interpretation,
            SetupOperation::Sync(SyncInitialState::NoClassicInitialState)
        );
        let SetupOperation::Close(close) = &result.instructions[3].interpretation else {
            panic!("close");
        };
        assert_eq!(close.balance, CloseInitialBalance::NoClassicInitialState);
        assert_eq!(close.effective_authority, None);
        assert_unknown(&result);
    }
    for state in [0, 1, 2] {
        let mut raw = responses(&payload)?;
        let mut bytes = token_bytes(wsol_mint(), WALLET, 1, Some(9));
        bytes[108] = state;
        set(&mut raw, PEER, present(10, token_program_id(), bytes));
        let facts = raw.collect(&payload).await?;
        let result = interpret(&payload, WALLET, &facts)?;
        assert_eq!(
            result.instructions[2].interpretation,
            SetupOperation::Sync(if state == 0 {
                SyncInitialState::Uninitialized
            } else {
                SyncInitialState::ObservedWsol
            })
        );
        let SetupOperation::Close(close) = &result.instructions[3].interpretation else {
            panic!("close");
        };
        assert_eq!(
            close.balance,
            if state == 0 {
                CloseInitialBalance::Uninitialized
            } else {
                CloseInitialBalance::ObservedNative
            }
        );
        assert_unknown(&result);
    }
    Ok(())
}

#[tokio::test]
async fn native_setup_foreign_encoded_close_signer_remains_distinct_from_wallet() -> Result<()> {
    let authority = [66; 32];
    let mut instructions = sync_close(PEER, WALLET);
    instructions[3].accounts[2] =
        crate::execution_solana_tx::SolanaAccountMeta::writable(authority);
    // The fixture serializer only emits one signer. Promote the writable prefix up to
    // the foreign authority for this synthetic multi-signer message, with zero signatures.
    let mut wire = STANDARD.decode(payload(&instructions)?)?;
    let index = (0..usize::from(wire[68]))
        .find(|i| wire[69 + i * 32..101 + i * 32] == authority)
        .unwrap();
    let signers = index + 1;
    assert!(signers > 1 && signers < 128);
    wire[0] = signers as u8;
    wire[65] = signers as u8;
    wire.splice(1..1, vec![0; 64 * (signers - 1)]);
    let payload = STANDARD.encode(wire);
    let mut raw = responses(&payload)?;
    set(
        &mut raw,
        PEER,
        present(
            10,
            token_program_id(),
            token_bytes(wsol_mint(), authority, 1, Some(9)),
        ),
    );
    let facts = raw.collect(&payload).await?;
    let result = interpret(&payload, WALLET, &facts)?;
    let SetupOperation::Close(close) = &result.instructions[3].interpretation else {
        panic!("close");
    };
    assert_eq!(close.effective_authority, Some(authority));
    assert_eq!(
        close.authority_relation,
        CloseAuthorityRelation::MatchesEffectiveKey
    );
    assert!(!close.authority_is_expected_wallet);
    assert!(close.destination_is_expected_wallet);
    assert_unknown(&result);
    Ok(())
}
