use super::receipt_cash_facts_fixture::{money_snapshot, transaction_calls};
use super::receipt_lifecycle_fixture::*;
use super::receipt_reconciliation_fixture::*;
use anyhow::Result;
use copybot_storage_core::{
    NativeAccountObservations, ObservationCoverage as Cov, ObservationSource as Src,
};
use serde_json::json;
const WSOL: &str = "So11111111111111111111111111111111111111112";
fn saved(f: &Fixture) -> Result<NativeAccountObservations> {
    Ok(f.store
        .load_receipt_native_observations(&f.order_id)?
        .unwrap())
}
#[tokio::test]
async fn receipt_native_lifecycle_endpoints_prefunded_foreign_close_and_completion() -> Result<()> {
    for side in ["buy", "sell"] {
        for prefunded in [false, true] {
            let mut f = Fixture::new(side)?;
            let mut value = lifecycle_receipt(side, SPL_TOKEN, true, prefunded);
            if side == "sell" {
                value["result"]["meta"]["innerInstructions"][0]["instructions"][1]["parsed"]
                    ["info"]["destination"] = json!("pool-account");
            }
            let rpc = Rpc::new(value).await?;
            rpc.context(format!("receipt_native_lifecycle_endpoints_prefunded_foreign_close_and_completion side={side:?} prefunded={prefunded:?}"));
            assert_eq!(f.reconcile(&rpc, 1).await?.confirmation_confirmed, 1);
            f.reopen()?;
            let obs = saved(&f)?;
            let a = obs.accounts.iter().find(|a| a.account_index == 1).unwrap();
            let zero = if side == "buy" {
                &a.pre_token.raw
            } else {
                &a.post_token.raw
            };
            assert_eq!(zero.value.as_deref(), Some("0"));
            assert_eq!(zero.source, Src::ProvenLifecycle);
            assert_eq!(
                a.native_pre.value.as_deref(),
                Some(if side == "buy" && !prefunded {
                    "0"
                } else {
                    "2039280"
                })
            );
            if side == "sell" {
                assert_eq!(a.native_post.value.as_deref(), Some("0"));
                assert!(obs.instructions.iter().any(|i| i
                    .fields
                    .get("destination")
                    .is_some_and(|v| v.value.as_deref() == Some("pool-account"))));
            }
            let before = money_snapshot(&f)?;
            let calls = transaction_calls(&rpc);
            f.reconcile(&rpc, 2).await?;
            assert_eq!(transaction_calls(&rpc), calls);
            assert_eq!(money_snapshot(&f)?, before);
            assert_eq!(saved(&f)?, obs);
            assert_eq!(f.fills()?, 1);
            rpc.finish().await?;
        }
    }
    Ok(())
}
#[tokio::test]
async fn receipt_native_wsol_unsynced_and_create_close_keep_intermediate_instructions() -> Result<()>
{
    for close_cycle in [false, true] {
        let mut f = Fixture::new("sell")?;
        let mut value = program_receipt("sell", SPL_TOKEN);
        let index = value["result"]["transaction"]["message"]["accountKeys"]
            .as_array()
            .unwrap()
            .len();
        value["result"]["transaction"]["message"]["accountKeys"]
            .as_array_mut()
            .unwrap()
            .push(json!({"pubkey":"wrapped-account","signer":false,"writable":true}));
        for name in ["preBalances", "postBalances"] {
            value["result"]["meta"][name]
                .as_array_mut()
                .unwrap()
                .push(json!(if close_cycle {
                    0
                } else if name == "preBalances" {
                    200
                } else {
                    900
                }));
        }
        if !close_cycle {
            for name in ["preTokenBalances", "postTokenBalances"] {
                value["result"]["meta"][name].as_array_mut().unwrap().push(json!({"accountIndex":index,"mint":WSOL,"owner":"foreign-owner","programId":SPL_TOKEN,"uiTokenAmount":{"amount":"100","decimals":9}}));
            }
        }
        value["result"]["transaction"]["message"]["instructions"].as_array_mut().unwrap().extend([
            json!({"programId":SPL_TOKEN,"parsed":{"type":"initializeAccount3","info":{"account":"wrapped-account","owner":WALLET,"mint":WSOL}}}),
            json!({"programId":"11111111111111111111111111111111","parsed":{"type":"transfer","info":{"source":WALLET,"destination":"wrapped-account","lamports":700}}}),
            json!({"programId":SPL_TOKEN,"parsed":{"type":if close_cycle{"closeAccount"}else{"syncNative"},"info":{"account":"wrapped-account","owner":WALLET,"destination":"foreign-owner"}}}),
        ]);
        let rpc = Rpc::new(value).await?;
        rpc.context(format!("receipt_native_wsol_unsynced_and_create_close_keep_intermediate_instructions close_cycle={close_cycle:?}"));
        f.reconcile(&rpc, 1).await?;
        f.reopen()?;
        let obs = saved(&f)?;
        let a = obs
            .accounts
            .iter()
            .find(|a| a.pubkey == "wrapped-account")
            .unwrap();
        if close_cycle {
            assert_eq!(a.native_delta.value.as_deref(), Some("0"));
            assert_eq!(a.pre_token.raw.coverage, Cov::Missing);
            assert_eq!(a.post_token.raw.coverage, Cov::Missing);
        } else {
            assert_eq!(a.native_delta.value.as_deref(), Some("700"));
            assert_eq!(a.pre_token.raw.value.as_deref(), Some("100"));
            assert_eq!(a.post_token.raw.value.as_deref(), Some("100"));
            assert_eq!(
                a.post_token.token_owner.value.as_deref(),
                Some("foreign-owner")
            );
        }
        assert!(obs.instructions.iter().any(|i| i
            .fields
            .get("lamports")
            .is_some_and(|v| v.value.as_deref() == Some("700"))));
        assert_eq!(f.fills()?, 1);
        rpc.finish().await?;
    }
    Ok(())
}
