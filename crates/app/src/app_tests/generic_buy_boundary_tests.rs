use super::generic_buy_fixture::*;
use super::generic_buy_loopback::*;
use super::generic_buy_test_support::*;
use anyhow::Result;
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{json, Value};

#[tokio::test]
async fn batch111_packet_boundary_preserves_every_instruction() -> Result<()> {
    for (extra, size) in [(35, 1232), (36, 1233)] {
        let mut value: Value = serde_json::from_str(INSTRUCTIONS)?;
        let mut data = STANDARD.decode(value["swapInstruction"]["data"].as_str().unwrap())?;
        data.extend(vec![0; extra]);
        value["swapInstruction"]["data"] = json!(STANDARD.encode(data));
        let r = run(
            Replies {
                instructions: value.to_string(),
                ..Default::default()
            },
            "buy",
            |_| {},
            |_| {},
        )
        .await?;
        if size == 1232 {
            assert!(r.result.is_ok(), "{:?}", r.result);
            let payload = r.payload().unwrap();
            assert_eq!(
                super::generic_buy_decode::verify(&payload, &value, PAYER, 50_000_001),
                size
            );
            assert_eq!(r.server.simulations(), vec![payload]);
            assert_eq!(r.server.count("/swap"), 0);
        } else {
            r.assert_rejected("packet1233", 0);
        }
    }
    Ok(())
}

#[tokio::test]
async fn batch111_existing_sell_and_all_flag_combinations() -> Result<()> {
    for side in ["buy", "sell"] {
        for tiny in [false, true] {
            for ix in [false, true] {
                for tx in [false, true] {
                    // The newly enabled tiny SELL bundle is covered by the exact R4 oracle.
                    if side == "sell" && tiny && ix && tx {
                        continue;
                    }
                    let guarded = side == "buy" && tiny && ix && tx;
                    let r = run(
                        Replies::default(),
                        side,
                        |c| {
                            c.canary_tiny_submit_enabled = tiny;
                            c.swap_instructions_dry_run_enabled = ix;
                            c.swap_transaction_dry_run_enabled = tx;
                        },
                        |_| {},
                    )
                    .await?;
                    assert!(
                        r.result.is_ok(),
                        "side={side} tiny={tiny} ix={ix} tx={tx}: {:?}",
                        r.result
                    );
                    assert_eq!(r.server.count("swap-instructions"), usize::from(ix));
                    assert_eq!(r.server.count("/swap"), usize::from(tx && !guarded));
                    assert_eq!(r.payload().is_some(), tx);
                    assert_eq!(r.server.simulations().len(), usize::from(tiny && tx));
                    if tx && !guarded {
                        let old: Value = serde_json::from_str(OLD_SWAP)?;
                        assert_eq!(r.payload().as_deref(), old["swapTransaction"].as_str());
                    }
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn batch111_existing_soft_errors_only() -> Result<()> {
    let cases = [
        (
            200,
            json!({"simulationError":"missing account required"}).to_string(),
            true,
            2,
        ),
        (400, "Missing token program".into(), true, 3),
        (400, "provider unavailable".into(), false, 1),
        (
            200,
            json!({"simulationError":"unrelated provider failure"}).to_string(),
            false,
            1,
        ),
    ];
    for (status, body, soft, calls) in cases {
        let r = run(
            Replies {
                instructions_status: status,
                instructions: body,
                ..Default::default()
            },
            "buy",
            |_| {},
            |_| {},
        )
        .await?;
        assert_eq!(r.server.count("swap-instructions"), calls);
        if soft {
            assert!(r.result.is_ok(), "{:?}", r.result);
            assert_eq!(r.server.count("/swap"), 1);
            assert_eq!(r.server.simulations(), vec![r.payload().unwrap()]);
            // Preserving a soft fallback does not bypass presign v0/floor refusal.
            assert!(crate::execution_priority_fee_proof::prove(
                &r.request,
                &r.payload().unwrap(),
                22000
            )
            .is_err());
        } else {
            r.assert_rejected("unapproved-soft-error", 0);
        }
    }
    Ok(())
}

#[tokio::test]
async fn batch111_nullable_cleanup_and_empty_setup_are_explicit() -> Result<()> {
    for cleanup in [false, true] {
        let mut value: Value = serde_json::from_str(INSTRUCTIONS)?;
        value["setupInstructions"] = json!([]);
        if !cleanup {
            value["cleanupInstruction"] = Value::Null;
        }
        let r = run(
            Replies {
                instructions: value.to_string(),
                ..Default::default()
            },
            "buy",
            |_| {},
            |_| {},
        )
        .await?;
        assert!(r.result.is_ok(), "{:?}", r.result);
        super::generic_buy_decode::verify(&r.payload().unwrap(), &value, PAYER, 50_000_001);
    }
    Ok(())
}
