// Independent B09 review regression. Wire as sibling module in app_tests.rs.
use super::receipt_lifecycle_fixture::{program_receipt, SPL_TOKEN};
use super::receipt_reconciliation_fixture::{Fixture, Rpc, WALLET};
use anyhow::Result;
use copybot_storage_core::ObservationCoverage as Cov;
use serde_json::json;

#[tokio::test]
async fn independent_native_system_funding_keeps_proven_token_account_endpoints() -> Result<()> {
    let mut f = Fixture::new("sell")?;
    let mut value = program_receipt("sell", SPL_TOKEN);
    let account = "funded-aux-token-account";
    let index = value["result"]["transaction"]["message"]["accountKeys"]
        .as_array()
        .unwrap()
        .len();
    value["result"]["transaction"]["message"]["accountKeys"]
        .as_array_mut()
        .unwrap()
        .extend([
            json!({"pubkey":account,"signer":false,"writable":true}),
            json!({"pubkey":"11111111111111111111111111111111","signer":false,"writable":false}),
        ]);
    value["result"]["meta"]["preBalances"]
        .as_array_mut()
        .unwrap()
        .extend([json!(2039280), json!(1)]);
    value["result"]["meta"]["postBalances"]
        .as_array_mut()
        .unwrap()
        .extend([json!(2039287), json!(1)]);
    let after = value["result"]["meta"]["postBalances"][0].as_u64().unwrap();
    value["result"]["meta"]["postBalances"][0] = json!(after - 7);
    for name in ["preTokenBalances", "postTokenBalances"] {
        value["result"]["meta"][name]
            .as_array_mut()
            .unwrap()
            .push(json!({
                "accountIndex":index,"mint":"AuxiliaryMint","owner":"ForeignTokenOwner",
                "programId":SPL_TOKEN,"uiTokenAmount":{"amount":"123","decimals":6}
            }));
    }
    value["result"]["transaction"]["message"]["instructions"]
        .as_array_mut()
        .unwrap()
        .push(json!({
            "programId":"11111111111111111111111111111111","parsed":{"type":"transfer",
            "info":{"source":WALLET,"destination":account,"lamports":7}}
        }));
    let rpc = Rpc::new(value).await?;
    rpc.context(format!(
        "independent_native_system_funding_keeps_proven_token_account_endpoints"
    ));
    assert_eq!(f.reconcile(&rpc, 1).await?.confirmation_confirmed, 1);
    f.reopen()?;
    let obs = f
        .store
        .load_receipt_native_observations(&f.order_id)?
        .unwrap();
    assert!(obs.instructions.iter().any(|i| i
        .fields
        .get("destination")
        .is_some_and(|v| v.value.as_deref() == Some(account))));
    // Both endpoint rows prove this is a token account. The supported transfer
    // explicitly links it to our wallet but does not make its tokens ours.
    let aux = obs
        .accounts
        .iter()
        .find(|a| a.pubkey == account)
        .unwrap_or_else(|| {
            panic!(
                "supported System funding lost linked token account; coverage={:?}, accounts={:?}",
                obs.accounts_coverage,
                obs.accounts
                    .iter()
                    .map(|a| a.pubkey.as_str())
                    .collect::<Vec<_>>()
            )
        });
    assert_eq!(aux.native_pre.value.as_deref(), Some("2039280"));
    assert_eq!(aux.native_post.value.as_deref(), Some("2039287"));
    assert_eq!(aux.native_delta.value.as_deref(), Some("7"));
    assert_eq!(
        aux.post_token.token_owner.value.as_deref(),
        Some("ForeignTokenOwner")
    );
    assert_eq!(aux.post_token.token_owner.coverage, Cov::Known);
    assert!(aux.relevance.iter().any(|v| v == "wallet_instruction_link"));
    assert_eq!(f.fills()?, 1);
    rpc.finish().await?;
    Ok(())
}
