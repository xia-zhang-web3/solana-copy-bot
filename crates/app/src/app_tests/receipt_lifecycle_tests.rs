use super::receipt_lifecycle_fixture::*;
use super::receipt_reconciliation_fixture::*;
use anyhow::Result;
use copybot_core_types::{Lamports, TokenQuantity};
use serde_json::json;

#[tokio::test]
async fn receipt_lifecycle_missing_pre_row_does_not_invent_buy_quantity() -> Result<()> {
    missing_existing_row("buy").await
}

#[tokio::test]
async fn receipt_lifecycle_missing_post_row_does_not_invent_sell_quantity() -> Result<()> {
    missing_existing_row("sell").await
}

async fn missing_existing_row(side: &str) -> Result<()> {
    for program in [SPL_TOKEN, TOKEN_2022] {
        let mut f = Fixture::new(side)?;
        let mut valid = program_receipt(side, program);
        if side == "buy" {
            valid["result"]["meta"]["preTokenBalances"][0]["uiTokenAmount"]["amount"] =
                json!("10000");
            valid["result"]["meta"]["postTokenBalances"][0]["uiTokenAmount"]["amount"] =
                json!("17000");
        }
        let mut incomplete = valid.clone();
        incomplete["result"]["meta"][if side == "buy" {
            "preTokenBalances"
        } else {
            "postTokenBalances"
        }]
        .as_array_mut()
        .unwrap()
        .remove(0);
        let before = f.store.load_execution_canary_open_position(TOKEN)?;
        let rpc = Rpc::new(incomplete).await?;
        rpc.context(format!(
            "missing_existing_row side={side:?} program={program:?}"
        ));
        f.reconcile(&rpc, 10).await?;
        assert_eq!(f.fills()?, 0, "ambiguous missing row must not become zero");
        assert_eq!(f.store.load_execution_canary_open_position(TOKEN)?, before);
        assert_eq!(pnl(&f)?, if side == "sell" { Some(0) } else { None });
        assert!(f.store.execution_canary_accounting_pending()?);
        let reason = if side == "buy" {
            "receipt_token_creation_unproven"
        } else {
            "receipt_sell_unsupported:UnresolvedTokenCoverage"
        };
        assert_eq!(
            f.store
                .load_execution_canary_receipt_proof(&f.order_id)?
                .unwrap()
                .reason,
            reason
        );
        f.reopen()?;
        f.reconcile(&rpc, 1_000).await?;
        assert_eq!(f.fills()?, 0);
        rpc.set(valid);
        f.reconcile(&rpc, 2_000).await?;
        let after = f.store.load_execution_canary_open_position(TOKEN)?.unwrap();
        assert_eq!(
            after.qty_exact,
            Some(TokenQuantity::new(
                if side == "buy" { 7_000 } else { 3_000 },
                3
            ))
        );
        assert_eq!(
            after.cost_lamports,
            Some(Lamports::new(if side == "buy" {
                900_000_000
            } else {
                240_000_000
            }))
        );
        assert_eq!(f.fills()?, 1);
        assert_eq!(
            pnl(&f)?,
            if side == "sell" {
                Some(640_000_000)
            } else {
                Some(0)
            }
        );
        assert!(!f.store.execution_canary_accounting_pending()?);
        f.reconcile(&rpc, 3_000).await?;
        assert_eq!(
            f.store.load_execution_canary_open_position(TOKEN)?,
            Some(after)
        );
        assert_eq!(
            pnl(&f)?,
            if side == "sell" {
                Some(640_000_000)
            } else {
                Some(0)
            }
        );
        assert_eq!(f.fills()?, 1);
        assert!(!rpc
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|m| m == "sendTransaction"));
        rpc.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn receipt_lifecycle_accepts_explicit_initialization_and_closure_for_both_programs(
) -> Result<()> {
    for program in [SPL_TOKEN, TOKEN_2022] {
        for inner in [false, true] {
            for (side, prefunded) in [("buy", false), ("buy", true), ("sell", false)] {
                let f = Fixture::new(side)?;
                let rpc = Rpc::new(lifecycle_receipt(side, program, inner, prefunded)).await?;
                rpc.context(format!("receipt_lifecycle_accepts_explicit_initialization_and_closure_for_both_programs program={program:?} inner={inner:?} side={side:?} prefunded={prefunded:?}"));
                assert_eq!(f.reconcile(&rpc, 10).await?.confirmation_confirmed, 1);
                assert_eq!(f.fills()?, 1);
                let pos = f.store.load_execution_canary_open_position(TOKEN)?;
                if side == "buy" {
                    let pos = pos.unwrap();
                    assert_eq!(pos.qty_exact, Some(TokenQuantity::new(7000, 3)));
                    assert_eq!(pos.cost_lamports, Some(Lamports::new(900_000_000)));
                } else {
                    assert!(pos.is_none());
                    assert_eq!(pnl(&f)?, Some(400_000_000));
                }
                f.reconcile(&rpc, 1_000).await?;
                assert_eq!(f.fills()?, 1);
                assert_eq!(
                    rpc.calls.lock().unwrap().as_slice(),
                    ["getSignatureStatuses", "getTransaction"]
                );
                rpc.finish().await?;
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn receipt_lifecycle_rejects_ambiguous_or_mismatched_evidence() -> Result<()> {
    for program in [SPL_TOKEN, TOKEN_2022] {
        for side in ["buy", "sell"] {
            let valid = lifecycle_receipt(side, program, false, true);
            let rpc = Rpc::new(json!(null)).await?;
            rpc.context(format!("receipt_lifecycle_rejects_ambiguous_or_mismatched_evidence program={program:?} side={side:?}"));
            let ix = if side == "buy" { 0 } else { 1 };
            let row = if side == "buy" {
                "postTokenBalances"
            } else {
                "preTokenBalances"
            };
            let base = format!("/result/transaction/message/instructions/{ix}");
            for (path, replacement) in [
                (format!("{base}/programId"), json!("NotTokenProgram")),
                (
                    format!("{base}/parsed/info/account"),
                    json!("other-account"),
                ),
                (format!("{base}/parsed/info/owner"), json!("other-owner")),
                (format!("{base}/parsed/type"), json!("createIdempotent")),
                (
                    format!("/result/meta/{row}/0/programId"),
                    json!("NotTokenProgram"),
                ),
                ("/result/meta/innerInstructions".into(), json!(null)),
                (format!("{base}/parsed/info"), json!(null)),
                (
                    "/result/transaction/message/accountKeys/1/writable".into(),
                    json!(false),
                ),
                ("/result/transaction/message/instructions".into(), json!([])),
                (
                    if side == "buy" {
                        format!("{base}/parsed/info/mint")
                    } else {
                        format!("{base}/parsed/info/destination")
                    },
                    json!("WrongMintOrDestination"),
                ),
                (
                    "/result/meta/postBalances/1".into(),
                    json!(if side == "buy" { 0 } else { 2_039_280 }),
                ),
            ] {
                // Independent malformed receipts must not contradict one saved signature.
                let f = Fixture::new(side)?;
                let pos = f.store.load_execution_canary_open_position(TOKEN)?;
                let mut invalid = valid.clone();
                *invalid.pointer_mut(&path).unwrap() = replacement;
                rpc.set(invalid);
                assert_eq!(
                    f.reconcile(&rpc, 1_000).await?.confirmation_pending,
                    1,
                    "{path}"
                );
                assert_eq!(f.fills()?, 0, "{path}");
                assert_eq!(f.store.load_execution_canary_open_position(TOKEN)?, pos);
                assert_eq!(pnl(&f)?, if side == "sell" { Some(0) } else { None });
            }
            for mutation in ["duplicate", "opposite", "unparsed"] {
                let f = Fixture::new(side)?;
                let mut invalid = valid.clone();
                let mut extra =
                    invalid["result"]["transaction"]["message"]["instructions"][ix].clone();
                if mutation == "opposite" {
                    extra = json!({"programId":program,"parsed":if side == "buy" {
                        json!({"type":"closeAccount","info":{"account":"token-account","destination":WALLET,"owner":WALLET}})
                    } else {
                        json!({"type":"initializeAccount3","info":{"account":"token-account","mint":TOKEN,"owner":WALLET}})
                    }});
                } else if mutation == "unparsed" {
                    extra = json!({"programId":program,"accounts":["token-account"],"data":"1"});
                }
                invalid["result"]["transaction"]["message"]["instructions"]
                    .as_array_mut()
                    .unwrap()
                    .push(extra);
                rpc.set(invalid);
                assert_eq!(
                    f.reconcile(&rpc, 1_000).await?.confirmation_pending,
                    1,
                    "{mutation}"
                );
                assert_eq!(f.fills()?, 0);
            }
            let f = Fixture::new(side)?;
            rpc.set(valid);
            assert_eq!(f.reconcile(&rpc, 2_000).await?.confirmation_confirmed, 1);
            assert_eq!(f.fills()?, 1);
            rpc.finish().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn receipt_lifecycle_sums_existing_and_created_or_closed_wallet_accounts() -> Result<()> {
    for program in [SPL_TOKEN, TOKEN_2022] {
        for side in ["buy", "sell"] {
            let f = Fixture::new(side)?;
            let mut value = lifecycle_receipt(side, program, true, false);
            // A second wallet account: creation adds 7, existing loses 2;
            // closure removes 10, existing gains 3. Aggregate wallet delta is +5 / -7.
            let extra_index = value["result"]["transaction"]["message"]["accountKeys"]
                .as_array()
                .unwrap()
                .len();
            value["result"]["transaction"]["message"]["accountKeys"]
                .as_array_mut()
                .unwrap()
                .push(json!({"pubkey":"wallet-extra","signer":false,"writable":true}));
            for name in ["preBalances", "postBalances"] {
                value["result"]["meta"][name]
                    .as_array_mut()
                    .unwrap()
                    .push(json!(2_039_280));
            }
            let mut row = json!({"accountIndex":extra_index,"owner":WALLET,"mint":TOKEN,"programId":program,
                "uiTokenAmount":{"amount":"5000","decimals":3}});
            let pool = value["result"]["meta"]["postTokenBalances"]
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .find(|r| r["accountIndex"] == 4)
                .unwrap();
            pool["uiTokenAmount"]["amount"] = json!(if side == "buy" { "5000" } else { "17000" });
            value["result"]["meta"]["innerInstructions"][0]["instructions"].as_array_mut().unwrap().push(json!({
                "programId":program,"parsed":{"type":"transferChecked","info":{
                    "source":if side == "buy" { "wallet-extra" } else { "pool-account" },
                    "destination":if side == "buy" { "pool-account" } else { "wallet-extra" },
                    "mint":TOKEN,"authority":if side == "buy" { WALLET } else { "pool-owner" },
                    "tokenAmount":{"amount":if side == "buy" { "2000" } else { "3000" },"decimals":3}}}}));
            value["result"]["meta"]["preTokenBalances"]
                .as_array_mut()
                .unwrap()
                .push(row.clone());
            row["uiTokenAmount"]["amount"] = json!(if side == "buy" { "3000" } else { "8000" });
            value["result"]["meta"]["postTokenBalances"]
                .as_array_mut()
                .unwrap()
                .push(row);
            let rpc = Rpc::new(value).await?;
            rpc.context(format!("receipt_lifecycle_sums_existing_and_created_or_closed_wallet_accounts program={program:?} side={side:?}"));
            assert_eq!(f.reconcile(&rpc, 10).await?.confirmation_confirmed, 1);
            let pos = f.store.load_execution_canary_open_position(TOKEN)?.unwrap();
            assert_eq!(
                pos.qty_exact,
                Some(TokenQuantity::new(
                    if side == "buy" { 5_000 } else { 3_000 },
                    3
                ))
            );
            assert_eq!(f.fills()?, 1);
            rpc.finish().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn receipt_lifecycle_supports_legacy_initializers_with_complete_info() -> Result<()> {
    for program in [SPL_TOKEN, TOKEN_2022] {
        for kind in ["initializeAccount", "initializeAccount2"] {
            let f = Fixture::new("buy")?;
            let mut valid = lifecycle_receipt("buy", program, false, true);
            valid["result"]["transaction"]["message"]["instructions"][0]["parsed"]["type"] =
                json!(kind);
            valid["result"]["transaction"]["message"]["instructions"][0]["parsed"]["info"]
                ["rentSysvar"] = json!("SysvarRent111111111111111111111111111111111");
            let rpc = Rpc::new(valid.clone()).await?;
            rpc.context(format!("receipt_lifecycle_supports_legacy_initializers_with_complete_info program={program:?} kind={kind:?}"));
            let mut incomplete = valid.clone();
            incomplete["result"]["transaction"]["message"]["instructions"][0]["parsed"]["info"]
                .as_object_mut()
                .unwrap()
                .remove("rentSysvar");
            rpc.set(incomplete);
            assert_eq!(f.reconcile(&rpc, 10).await?.confirmation_pending, 1);
            assert_eq!(f.fills()?, 0);
            rpc.set(valid);
            assert_eq!(f.reconcile(&rpc, 20).await?.confirmation_confirmed, 1);
            assert_eq!(f.fills()?, 1);
            rpc.finish().await?;
        }
    }
    Ok(())
}

fn pnl(f: &Fixture) -> Result<Option<i64>> {
    Ok(f.conn()?
        .query_row("SELECT SUM(pnl_lamports) FROM positions", [], |r| r.get(0))?)
}
