use super::priority_fee_route_fixture::{Fixture, Route};
use anyhow::Result;

fn blockhash(payload: &str) -> Result<[u8; 32]> {
    let decoded = crate::execution_transaction_wire::decode_message(payload, |_| Ok(()))?;
    let binding = decoded.binding;
    assert!(binding.accounts.len() < 128);
    assert_eq!(binding.message_bytes[0], 1); // legacy single-signer fixture
    assert_eq!(
        usize::from(binding.message_bytes[3]),
        binding.accounts.len()
    );
    let offset = 4 + 32 * binding.accounts.len();
    Ok(binding.message_bytes[offset..offset + 32].try_into()?)
}

#[tokio::test]
async fn root_b25_hash_only_retry_uses_new_blockhash_with_same_reserve() -> Result<()> {
    for extension in [false, true] {
        let mut f = Fixture::new(Route::Direct, 200_000, 1_400_000).await?;
        f.wire.lock().unwrap().extension = extension;
        let old = f.build().await?.envelope.unwrap();
        assert_eq!(
            blockhash(old.signed_transaction_base64.as_deref().unwrap())?,
            [0; 32]
        );
        f.store.mark_execution_canary_retry_after_submit_not_sent(
            &f.request.order_id,
            f.now,
            crate::execution_canary_submit_contract::TINY_SUBMIT_RETRY_AFTER_RPC_NOT_SENT_REASON,
        )?;
        f.wire.lock().unwrap().blockhash = 17;
        let outcome = super::entry_risk_clock_fixture::at(
            f.now + chrono::Duration::seconds(2),
            crate::execution_canary_route::process_tiny_submit_reconciliation_sweep(
                &f.config, &f.store, f.now,
            ),
        )
        .await;
        f.finish().await?;
        let outcome = outcome?.unwrap();
        if extension {
            assert_eq!(outcome.failed, 1, "{outcome:?}");
            assert_eq!(
                outcome.last_error.as_deref(),
                Some("initial_sol_unsupported_setup")
            );
            assert_eq!(f.sends(), 0);
            let calls = f.calls.lock().unwrap();
            let simulated = calls
                .iter()
                .rev()
                .find(|(_, v)| v["method"] == "simulateTransaction")
                .unwrap()
                .1["params"][0]
                .as_str()
                .unwrap();
            assert_eq!(blockhash(simulated)?, [17; 32]);
            continue;
        }
        assert_eq!(outcome.failed, 0, "{outcome:?}");
        assert_eq!(outcome.signing_envelope_built, 1);
        assert_eq!(f.sends(), 1);
        let calls = f.calls.lock().unwrap();
        let simulated = calls
            .iter()
            .rev()
            .find(|(_, v)| v["method"] == "simulateTransaction")
            .unwrap()
            .1["params"][0]
            .as_str()
            .unwrap();
        let sent = calls
            .iter()
            .find(|(_, v)| v["method"] == "sendTransaction")
            .unwrap()
            .1["params"][0]
            .as_str()
            .unwrap();
        assert_eq!(blockhash(simulated)?, [17; 32]);
        assert_eq!(blockhash(sent)?, [17; 32]);
        let wallet = crate::execution_pumpswap_accounts::parse_pubkey(
            &f.config.canary_wallet_pubkey,
            "test",
        )?;
        let before = crate::execution_native_floor::verify_final_native_floor(
            simulated, wallet, 50_000_001,
        )?;
        let after =
            crate::execution_native_floor::verify_final_native_floor(sent, wallet, 50_000_001)?;
        assert_eq!(
            before.binding().message_bytes,
            after.binding().message_bytes
        );
    }
    Ok(())
}
