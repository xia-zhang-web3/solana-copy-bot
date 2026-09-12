use super::receipt_cash_facts_fixture::{facts, money_snapshot, transaction_calls};
use super::receipt_lifecycle_fixture::{lifecycle_receipt, SPL_TOKEN, TOKEN_2022};
use super::receipt_reconciliation_fixture::*;
use anyhow::Result;
use copybot_storage_core::{
    NativeAccountObservations, ObservationCoverage as Cov, ObservationSource as Src,
};
use serde_json::{json, Value};
const SYSTEM: &str = "11111111111111111111111111111111";
fn saved(f: &Fixture) -> Result<NativeAccountObservations> {
    Ok(f.store
        .load_receipt_native_observations(&f.order_id)?
        .unwrap())
}
fn payment(v: &mut Value) {
    if !v["result"]["transaction"]["message"]["accountKeys"]
        .as_array()
        .unwrap()
        .iter()
        .any(|k| k["pubkey"] == SYSTEM)
    {
        v["result"]["transaction"]["message"]["accountKeys"]
            .as_array_mut()
            .unwrap()
            .push(json!({"pubkey":SYSTEM,"signer":false,"writable":false}));
        for name in ["preBalances", "postBalances"] {
            v["result"]["meta"][name]
                .as_array_mut()
                .unwrap()
                .push(json!(1));
        }
    }
    let wallet = v["result"]["meta"]["postBalances"][0].as_u64().unwrap();
    let payee = v["result"]["meta"]["postBalances"][5].as_u64().unwrap();
    v["result"]["meta"]["postBalances"][0] = json!(wallet - 7);
    v["result"]["meta"]["postBalances"][5] = json!(payee + 7);
    v["result"]["transaction"]["message"]["instructions"].as_array_mut().unwrap().push(json!({"programId":SYSTEM,"parsed":{"type":"transfer","info":{"source":WALLET,"destination":"pool-owner","lamports":7}}}));
}
// A present RPC zero is an independent neighbor. It also prevents malformed
// target metadata from changing the old whole-receipt wallet identity check.
fn rpc_zero_neighbor(v: &mut Value, program: &str) {
    let index = v["result"]["transaction"]["message"]["accountKeys"]
        .as_array()
        .unwrap()
        .len();
    v["result"]["transaction"]["message"]["accountKeys"]
        .as_array_mut()
        .unwrap()
        .push(json!({"pubkey":"zero-neighbor","signer":false,"writable":true}));
    for name in ["preBalances", "postBalances"] {
        v["result"]["meta"][name]
            .as_array_mut()
            .unwrap()
            .push(json!(2039280));
    }
    for name in ["preTokenBalances", "postTokenBalances"] {
        v["result"]["meta"][name].as_array_mut().unwrap().push(json!({"accountIndex":index,"owner":WALLET,"mint":TOKEN,"programId":program,"uiTokenAmount":{"amount":"0","decimals":3}}));
    }
}
#[tokio::test]
async fn receipt_lifecycle_selection_keeps_proven_zero_rpc_zero_cash_and_completed_replay(
) -> Result<()> {
    for side in ["buy", "sell"] {
        for program in [SPL_TOKEN, TOKEN_2022] {
            for inner in [false, true] {
                for prefunded in [false, true] {
                    for paid in [false, true] {
                        let mut f = Fixture::new(side)?;
                        let mut v = lifecycle_receipt(side, program, inner, prefunded);
                        rpc_zero_neighbor(&mut v, program);
                        if paid {
                            payment(&mut v);
                        }
                        let rpc = Rpc::new(v).await?;
                        rpc.context(format!("receipt_lifecycle_selection_keeps_proven_zero_rpc_zero_cash_and_completed_replay side={side:?} program={program:?} inner={inner:?} prefunded={prefunded:?} paid={paid:?}"));
                        assert_eq!(f.reconcile(&rpc, 1).await?.confirmation_confirmed, 1);
                        f.reopen()?;
                        let o = saved(&f)?;
                        let account = o
                            .accounts
                            .iter()
                            .find(|a| a.pubkey == "token-account")
                            .unwrap();
                        let zero = if side == "buy" {
                            &account.pre_token.raw
                        } else {
                            &account.post_token.raw
                        };
                        assert_eq!(
                            zero.value.as_deref(),
                            Some("0"),
                            "{side} inner={inner} paid={paid}"
                        );
                        assert_eq!(zero.coverage, Cov::Known);
                        assert_eq!(zero.source, Src::ProvenLifecycle);
                        let neighbor = o
                            .accounts
                            .iter()
                            .find(|a| a.pubkey == "zero-neighbor")
                            .unwrap();
                        assert_eq!(neighbor.pre_token.raw.value.as_deref(), Some("0"));
                        assert_eq!(neighbor.post_token.raw.value.as_deref(), Some("0"));
                        assert_eq!(neighbor.pre_token.raw.source, Src::RpcTokenBalance);
                        assert_eq!(neighbor.post_token.raw.source, Src::RpcTokenBalance);
                        assert!(!o.accounts.iter().any(|a| a.pubkey == "pool-owner"));
                        if paid {
                            assert_ne!(o.accounts_coverage, Cov::Known);
                            assert!(o
                                .reasons
                                .iter()
                                .any(|r| r == "funding_token_identity_unproven"));
                        }
                        let cash = if side == "buy" {
                            -900_000_000_i64
                        } else {
                            1_200_000_000
                        } - if paid { 7 } else { 0 };
                        assert_eq!(facts(&f)?.wallet_native_delta.as_i128(), i128::from(cash));
                        if side == "sell" {
                            let settled = f
                                .store
                                .load_execution_canary_cash_settlement(&f.order_id)?
                                .unwrap();
                            assert_eq!(
                                settled.wallet_native_cash_delta.as_i128(),
                                i128::from(cash)
                            );
                            assert_eq!(
                                settled.cash_result_delta.as_i128(),
                                i128::from(cash) - 800_000_000
                            );
                        } else {
                            assert_eq!(
                                f.conn()?.query_row(
                                    "SELECT cost_lamports FROM positions",
                                    [],
                                    |r| r.get::<_, i64>(0)
                                )?,
                                -cash
                            );
                        }
                        let before = money_snapshot(&f)?;
                        assert_eq!(transaction_calls(&rpc), 1);
                        rpc.set(json!({"result":null}));
                        f.reopen()?;
                        assert_eq!(f.reconcile(&rpc, 2).await?.confirmation_confirmed, 1);
                        assert_eq!(f.fills()?, 1);
                        assert_eq!(transaction_calls(&rpc), 1);
                        assert_eq!(money_snapshot(&f)?, before);
                        assert_eq!(saved(&f)?, o);
                        rpc.finish().await?;
                    }
                }
            }
        }
    }
    Ok(())
}
#[tokio::test]
async fn receipt_lifecycle_selection_partial_rows_and_invalid_lifecycle_never_infer_zero(
) -> Result<()> {
    for side in ["buy", "sell"] {
        for program in [SPL_TOKEN, TOKEN_2022] {
            for case in [
                "missing_array",
                "invalid_array",
                "duplicate_row",
                "bad_index",
                "missing_owner",
                "missing_mint",
                "missing_program",
                "unsupported_program",
                "invalid_raw",
                "invalid_decimals",
                "missing_inner",
                "invalid_inner",
                "bad_inner_index",
                "duplicate_inner",
                "raw_target_cpi",
                "duplicate_lifecycle",
                "conflicting_lifecycle",
            ] {
                let mut f = Fixture::new(side)?;
                let mut v = lifecycle_receipt(side, program, true, false);
                rpc_zero_neighbor(&mut v, program);
                payment(&mut v);
                let missing = if side == "buy" {
                    "preTokenBalances"
                } else {
                    "postTokenBalances"
                };
                let observed = if side == "buy" {
                    "postTokenBalances"
                } else {
                    "preTokenBalances"
                };
                match case {
                    "missing_array" => v["result"]["meta"][missing]=json!(null),
                    "invalid_array" => v["result"]["meta"][missing]=json!({}),
                    "duplicate_row" => {let row=v["result"]["meta"][missing][0].clone();v["result"]["meta"][missing].as_array_mut().unwrap().push(row);},
                    "bad_index" => v["result"]["meta"][missing].as_array_mut().unwrap().push(json!({"accountIndex":999,"mint":TOKEN})),
                    "missing_owner" => v["result"]["meta"][observed][0]["owner"]=json!(null),
                    "missing_mint" => v["result"]["meta"][observed][0]["mint"]=json!(null),
                    "missing_program" => v["result"]["meta"][observed][0]["programId"]=json!(null),
                    "unsupported_program" => v["result"]["meta"][observed][0]["programId"]=json!("UnknownProgram"),
                    "invalid_raw" => v["result"]["meta"][observed][0]["uiTokenAmount"]["amount"]=json!(5),
                    "invalid_decimals" => v["result"]["meta"][observed][0]["uiTokenAmount"]["decimals"]=json!(256),
                    "missing_inner" => v["result"]["meta"]["innerInstructions"]=json!(null),
                    "invalid_inner" => v["result"]["meta"]["innerInstructions"]=json!({}),
                    "bad_inner_index" => v["result"]["meta"]["innerInstructions"][0]["index"]=json!(999),
                    "duplicate_inner" => {let group=v["result"]["meta"]["innerInstructions"][0].clone();v["result"]["meta"]["innerInstructions"].as_array_mut().unwrap().push(group);},
                    "raw_target_cpi" => v["result"]["meta"]["innerInstructions"][0]["instructions"][1]=json!({"programId":program,"accounts":["token-account"],"data":"1"}),
                    "duplicate_lifecycle" => {let ix=v["result"]["meta"]["innerInstructions"][0]["instructions"][1].clone();v["result"]["meta"]["innerInstructions"][0]["instructions"].as_array_mut().unwrap().push(ix);},
                    _ => v["result"]["meta"]["innerInstructions"][0]["instructions"].as_array_mut().unwrap().push(json!({"programId":program,"parsed":{"type":if side=="buy" {"closeAccount"}else{"initializeAccount3"},"info":{"account":"token-account","owner":WALLET,"mint":TOKEN,"destination":"pool-owner"}}})),
                }
                let before = money_snapshot(&f)?;
                let rpc = Rpc::new(v).await?;
                rpc.context(format!("receipt_lifecycle_selection_partial_rows_and_invalid_lifecycle_never_infer_zero side={side:?} program={program:?} case={case:?}"));
                assert_eq!(
                    f.reconcile(&rpc, 1).await?.confirmation_pending,
                    1,
                    "{side}/{case}"
                );
                f.reopen()?;
                let o = saved(&f)?;
                let a = o
                    .accounts
                    .iter()
                    .find(|a| a.pubkey == "token-account")
                    .unwrap();
                let missing = if side == "buy" {
                    &a.pre_token.raw
                } else {
                    &a.post_token.raw
                };
                assert_eq!(missing.value, None, "{side}/{case}");
                assert_eq!(missing.source, Src::Unavailable, "{side}/{case}");
                assert_ne!(o.accounts_coverage, Cov::Known);
                assert!(!o.accounts.iter().any(|a| a.pubkey == "pool-owner"));
                let neighbor = o
                    .accounts
                    .iter()
                    .find(|a| a.pubkey == "zero-neighbor")
                    .unwrap();
                let rpc_zero = if side == "buy" {
                    &neighbor.post_token.raw
                } else {
                    &neighbor.pre_token.raw
                };
                assert_eq!(rpc_zero.value.as_deref(), Some("0"));
                assert_eq!(rpc_zero.source, Src::RpcTokenBalance);
                assert_eq!(f.fills()?, 0);
                assert_eq!(money_snapshot(&f)?, before);
                assert!(f.store.execution_canary_accounting_pending()?);
                rpc.finish().await?;
            }
        }
    }
    Ok(())
}
