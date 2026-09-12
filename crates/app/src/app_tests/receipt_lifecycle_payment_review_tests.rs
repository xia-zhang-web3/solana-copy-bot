use super::receipt_lifecycle_fixture::{lifecycle_receipt, SPL_TOKEN};
use super::receipt_reconciliation_fixture::{Fixture, Rpc, WALLET};
use anyhow::Result;
use copybot_storage_core::{ObservationCoverage as Cov, ObservationSource as Src};
use serde_json::json;

async fn verify_lifecycle(side: &str, payment: bool) -> Result<()> {
    let mut f = Fixture::new(side)?;
    let mut value = lifecycle_receipt(side, SPL_TOKEN, false, false);
    if payment {
        let keys = value["result"]["transaction"]["message"]["accountKeys"]
            .as_array_mut()
            .unwrap();
        let system = "11111111111111111111111111111111";
        if !keys.iter().any(|k| k["pubkey"] == system) {
            keys.push(json!({"pubkey":system,"signer":false,"writable":false}));
            for name in ["preBalances", "postBalances"] {
                value["result"]["meta"][name]
                    .as_array_mut()
                    .unwrap()
                    .push(json!(1));
            }
        }
        let wallet_after = value["result"]["meta"]["postBalances"][0].as_u64().unwrap();
        let payee_after = value["result"]["meta"]["postBalances"][5].as_u64().unwrap();
        value["result"]["meta"]["postBalances"][0] = json!(wallet_after - 7);
        value["result"]["meta"]["postBalances"][5] = json!(payee_after + 7);
        value["result"]["transaction"]["message"]["instructions"]
            .as_array_mut()
            .unwrap()
            .push(json!({
                "programId":system,"parsed":{"type":"transfer","info":{
                    "source":WALLET,"destination":"pool-owner","lamports":7}}
            }));
    }
    let rpc = Rpc::new(value).await?;
    rpc.context(format!(
        "verify_lifecycle side={side:?} payment={payment:?}"
    ));
    assert_eq!(f.reconcile(&rpc, 1).await?.confirmation_confirmed, 1);
    f.reopen()?;
    let obs = f
        .store
        .load_receipt_native_observations(&f.order_id)?
        .unwrap();
    let account = obs
        .accounts
        .iter()
        .find(|a| a.pubkey == "token-account")
        .unwrap();
    let zero = if side == "buy" {
        &account.pre_token.raw
    } else {
        &account.post_token.raw
    };
    eprintln!(
        "side={side}, payment={payment}, zero={zero:?}, coverage={:?}, fills={}",
        obs.accounts_coverage,
        f.fills()?
    );
    assert_eq!(zero.value.as_deref(), Some("0"), "{side}, payment={payment}: unrelated unproven SOL endpoint must not disable valid target lifecycle proof; coverage={:?}", obs.accounts_coverage);
    assert_eq!(zero.coverage, Cov::Known);
    assert_eq!(zero.source, Src::ProvenLifecycle);
    if payment {
        assert!(!obs.accounts.iter().any(|a| a.pubkey == "pool-owner"));
        assert_ne!(obs.accounts_coverage, Cov::Known);
    }
    assert_eq!(f.fills()?, 1);
    rpc.finish().await?;
    Ok(())
}

#[tokio::test]
async fn independent_buy_lifecycle_zero_survives_plain_sol_payment() -> Result<()> {
    verify_lifecycle("buy", false).await?;
    verify_lifecycle("buy", true).await
}
#[tokio::test]
async fn independent_sell_lifecycle_zero_survives_plain_sol_payment() -> Result<()> {
    verify_lifecycle("sell", false).await?;
    verify_lifecycle("sell", true).await
}
