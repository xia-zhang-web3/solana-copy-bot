use super::{
    b127_causal_fixture::*, b127_runtime_tests::prepared, priority_fee_route_fixture::Route,
};
use crate::execution_submit_adapter::ExecutionSubmitAdapter;
use crate::{execution_native_floor as floor, execution_native_floor_policy as policy};
use anyhow::Result;
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::json;

#[tokio::test]
async fn b127_exact_guard_rejections_at_signing_after_signing_and_submit() -> Result<()> {
    for case in [
        "minus-one",
        "static",
        "missing",
        "wrong-wallet",
        "not-last",
        "blockhash",
    ] {
        let mut f = prepared(Route::Metis).await?;
        let plan = f.adapter.build_transaction_plan(&f.request)?;
        let env = f.build().await?.envelope.unwrap();
        let signed = env.signed_transaction_base64.as_ref().unwrap();
        let wallet =
            crate::execution_pumpswap_accounts::parse_pubkey(&f.request.wallet_pubkey, "wallet")?;
        let before = policy::verify_signing_payload(&f.config, &f.request, &plan, signed)?.unwrap();
        let mut ix = instructions(wallet);
        let r = match case {
            "minus-one" => F - 1,
            "static" => 50_000_001,
            _ => F,
        };
        let mut guard = super::native_funding_fixture::transfer(wallet, wallet, r);
        if case == "wrong-wallet" {
            guard.accounts[1].pubkey = [31; 32];
        }
        if case == "not-last" {
            ix.insert(0, guard);
        } else if case != "missing" {
            ix.push(guard);
        }
        let mutated = STANDARD.encode(
            crate::execution_solana_tx::serialize_unsigned_legacy_transaction(
                wallet,
                if case == "blockhash" {
                    [10; 32]
                } else {
                    [9; 32]
                },
                &ix,
            )?,
        );
        if case != "blockhash" {
            assert!(
                policy::verify_signing_payload(&f.config, &f.request, &plan, &mutated).is_err(),
                "{case}"
            );
            assert!(
                policy::verify_submit_payload(&f.request, &mutated, 0.05, &f.request.wallet_pubkey)
                    .is_err(),
                "{case}"
            );
        }
        assert!(
            policy::verify_after_signing(Some(&before), &mutated).is_err(),
            "{case}"
        );
        let mut broken = env.clone();
        broken.signed_transaction_base64 = Some(mutated);
        let out = f.submit(&broken).await?;
        assert_eq!(f.sends(), 0, "{case}: {out:?}");
        assert!(f
            .store
            .load_execution_canary_dispatch(&f.request.order_id)?
            .is_none());
        println!("B127_GUARD_REFUSAL {case}: {out:?}");
        f.finish().await?;
    }
    Ok(())
}
#[tokio::test]
async fn b127_numeric_floor_oracle_and_external_inflow_control() -> Result<()> {
    let mut f = prepared(Route::Metis).await?;
    let envelope = f.build().await?.envelope.unwrap();
    let wallet =
        crate::execution_pumpswap_accounts::parse_pubkey(&f.request.wallet_pubkey, "wallet")?;
    let proof = floor::verify_final_native_floor(
        envelope.signed_transaction_base64.as_ref().unwrap(),
        wallet,
        F,
    )?;
    let m = crate::execution_transaction_wire::decode_message(proof.payload(), |_| Ok(()))?;
    let operand = u64::from_le_bytes(m.instructions.last().unwrap().data[4..12].try_into()?);
    // Arithmetic model of unchanged accepted Bank23 self-transfer: not runtime execution.
    for (post, pass) in [(F - 1, false), (F, true), (F + 1, true)] {
        assert_eq!(post.checked_sub(operand).is_some(), pass);
        println!(
            "B127_NUMERIC {}",
            json!({"post":post,"floor":operand,"accepted":pass,"oracle":"arithmetic model; historical Bank23 semantics"})
        );
    }
    let inflow = 25_000_000_u64;
    let debit = 40_000_000_u64; // >15m can pass with an external inflow: no per-tx delta promise.
    let post = B.checked_add(inflow).unwrap().checked_sub(debit).unwrap();
    assert_eq!(post, F);
    assert!(debit > 15_000_000);
    assert!(post.checked_sub(operand).is_some());
    f.funding.lock().unwrap().balance = B + inflow;
    assert_eq!(f.submit(&envelope).await?.submitted, 1);
    let anchored = f.store.tiny_native_policy(
        "local-variant-a",
        &f.request.wallet_pubkey,
        chrono::Utc::now(),
    )?;
    assert_eq!((anchored.initial_lamports, anchored.floor_lamports), (B, F));
    let held: u64 = f.conn()?.query_row(
        "SELECT fee_bound FROM execution_tiny_reservations",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(held, 19000);
    println!(
        "B127_INFLOW {}",
        json!({"B":B,"inflow":inflow,"debit":debit,"post":post,"floor":F,"buy_slots":1,"fee_bound":held})
    );
    f.finish().await
}
#[tokio::test]
async fn b127_postawait_policy_identity_and_deadline_changes_never_send() -> Result<()> {
    for phase in [1, 2] {
        for case in ["mode", "wallet", "policy", "deadline", "signal"] {
            let mut f = prepared(Route::Metis).await?;
            let envelope = f.build().await?.envelope.unwrap();
            f.funding.lock().unwrap().delay_ms = 35;
            f.config.submit_timeout_ms = 1000;
            let mutate = async {
                loop {
                    let seen = f
                        .calls
                        .lock()
                        .unwrap()
                        .iter()
                        .filter(|(_, v)| v["method"] == "getFeeForMessage")
                        .count();
                    if seen >= phase {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
                let conn = f.conn().unwrap();
                match case {
                    "mode" => {
                        conn.execute(
                            "UPDATE execution_tiny_experiment SET policy_mode='decoded_amount'",
                            [],
                        )
                        .unwrap();
                    }
                    "wallet" => {
                        conn.execute("UPDATE execution_tiny_experiment SET wallet='other'", [])
                            .unwrap();
                    }
                    "policy" => {
                        conn.execute("DELETE FROM execution_tiny_native_policy", [])
                            .unwrap();
                    }
                    "deadline" => {
                        let old = chrono::Utc::now() - chrono::Duration::hours(2);
                        conn.execute(
                            "UPDATE execution_tiny_experiment SET activated_at=?1,deadline=?2",
                            rusqlite::params![
                                old.to_rfc3339(),
                                (old + chrono::Duration::hours(1)).to_rfc3339()
                            ],
                        )
                        .unwrap();
                    }
                    "signal" => {
                        conn.execute(
                            "UPDATE copy_signals SET token='other' WHERE signal_id=?1",
                            [&f.request.signal_id],
                        )
                        .unwrap();
                    }
                    _ => unreachable!(),
                }
            };
            let (out, ()) = tokio::time::timeout(std::time::Duration::from_secs(4), async {
                tokio::join!(f.submit(&envelope), mutate)
            })
            .await?;
            let out = out?;
            assert_eq!(f.sends(), 0, "phase={phase} case={case}: {out:?}");
            assert!(f
                .store
                .load_execution_canary_dispatch(&f.request.order_id)?
                .is_none());
            println!("B127_POSTAWAIT phase={phase} case={case} {out:?}");
            f.finish().await?;
        }
    }
    Ok(())
}
