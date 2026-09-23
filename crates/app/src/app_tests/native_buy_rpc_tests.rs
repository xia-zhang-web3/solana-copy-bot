use crate::execution_native_buy_rpc;
use crate::execution_owned_sell_rpc::fractional::transport::Parsed;
use anyhow::Result;
use copybot_config::{
    ExecutionConfig, NativeFreshBuyConfig, OwnedSellPreparationConfig,
    PROCESSED_SLOT_FENCE_AVAILABILITY_V1, RPC_FINALIZED_OWNED_SELL_V1,
};
use serde_json::{json, Value};

fn config() -> ExecutionConfig {
    let mut c = ExecutionConfig::default();
    c.native_fresh_buy = Some(NativeFreshBuyConfig {
        policy: PROCESSED_SLOT_FENCE_AVAILABILITY_V1.into(),
    });
    c.owned_sell_preparation = Some(OwnedSellPreparationConfig {
        policy: RPC_FINALIZED_OWNED_SELL_V1.into(),
        tiny_dispatch: true,
        fractional_inventory: Some("whole_wallet_parent_program_fraction_v1".into()),
        rpc_url: "http://127.0.0.1:1".into(),
        genesis_hash: "11111111111111111111111111111111".into(),
        identity: "native-buy-rpc-test".into(),
    });
    c
}

fn response(request: &Value, tx_slot: u64, mint_owner: &str) -> Value {
    let result = match request["method"].as_str().unwrap() {
        "getGenesisHash" => json!("11111111111111111111111111111111"),
        "getSlot" => json!(10),
        "getTransaction" => json!({
            "slot": tx_slot,
            "meta": {"err": null, "postTokenBalances": [
                {"mint":"mint-A","owner":"wallet-A"}
            ]},
            "transaction": {
                "signatures": ["signature-A"],
                "message": {"accountKeys": [
                    {"pubkey":"wallet-A","signer":true}
                ]}
            }
        }),
        "getAccountInfo" => json!({
            "context":{"slot":tx_slot},
            "value":{"owner":mint_owner}
        }),
        _ => panic!("unexpected RPC method"),
    };
    json!({"jsonrpc":"2.0","id":request["id"],"result":result})
}

#[tokio::test]
async fn native_buy_fence_and_finality_bind_slot_signer_mint_and_spl_owner() -> Result<()> {
    let c = config();
    let mut check = || Ok(());
    let mut rpc = Parsed(|request: Value| async move {
        Ok(response(&request, 11, "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"))
    });
    let fence = execution_native_buy_rpc::fence(&mut rpc, &c, "session-A", &mut check).await?;
    assert_eq!(fence.processed_slot, 10);
    assert_eq!(fence.policy_identity, execution_native_buy_rpc::policy_identity(&c)?);
    execution_native_buy_rpc::finalized_source(
        &mut rpc, &c, "signature-A", 11, "wallet-A", "mint-A", &mut check,
    ).await?;
    assert!(execution_native_buy_rpc::finalized_source(
        &mut rpc, &c, "signature-B", 11, "wallet-A", "mint-A", &mut check,
    ).await.is_err());
    assert!(execution_native_buy_rpc::finalized_source(
        &mut rpc, &c, "signature-A", 12, "wallet-A", "mint-A", &mut check,
    ).await.is_err());
    assert!(execution_native_buy_rpc::finalized_source(
        &mut rpc, &c, "signature-A", 11, "wallet-B", "mint-A", &mut check,
    ).await.is_err());
    assert!(execution_native_buy_rpc::finalized_source(
        &mut rpc, &c, "signature-A", 11, "wallet-A", "mint-B", &mut check,
    ).await.is_err());
    let mut wrong_program = Parsed(|request: Value| async move {
        Ok(response(&request, 11, "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"))
    });
    assert!(execution_native_buy_rpc::finalized_source(
        &mut wrong_program, &c, "signature-A", 11, "wallet-A", "mint-A", &mut check,
    ).await.is_err());
    Ok(())
}

#[tokio::test]
async fn native_buy_finality_requires_present_null_meta_err() -> Result<()> {
    for fault in ["missing_err", "missing_meta", "array_meta", "failed_err"] {
        let c = config();
        let mut rpc = Parsed(move |request: Value| async move {
            let mut reply = response(&request, 11,
                "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
            if request["method"] == "getTransaction" {
                let tx = &mut reply["result"];
                match fault {
                    "missing_err" => { tx["meta"].as_object_mut().unwrap().remove("err"); },
                    "missing_meta" => { tx.as_object_mut().unwrap().remove("meta"); },
                    "array_meta" => tx["meta"] = json!([]),
                    "failed_err" => tx["meta"]["err"] = json!({"InstructionError":[0,"GenericError"]}),
                    _ => unreachable!(),
                }
            }
            Ok(reply)
        });
        let result = execution_native_buy_rpc::finalized_source(
            &mut rpc, &c, "signature-A", 11, "wallet-A", "mint-A", &mut || Ok(()),
        ).await;
        assert!(result.is_err(), "{fault} cannot prove source success");
    }
    Ok(())
}
