//! Causal compiled-message mismatch controls on the ordinary source proof boundary.
use anyhow::Result;
use serde_json::json;
#[test]
fn real_source_compiled_message_must_equal_wire_despite_unchanged_signature_and_deltas()->Result<()>{
    let wire=crate::execution_transaction_wire::decode_message(&super::generic_sell_fixture::legacy(),|_|Ok(()))?;
    let keys=wire.binding.accounts.iter().map(|a|bs58::encode(a.pubkey).into_string()).collect::<Vec<_>>();
    let index=|key|keys.iter().position(|k|*k==bs58::encode(key).into_string()).unwrap();
    let original=json!({"version":"legacy","transaction":{"message":{
        "header":{"numRequiredSignatures":wire.binding.required_signatures,"numReadonlySignedAccounts":wire.binding.readonly_signed,"numReadonlyUnsignedAccounts":wire.binding.readonly_unsigned},
        "accountKeys":keys,"recentBlockhash":bs58::encode(wire.recent_blockhash).into_string(),
        "instructions":wire.instructions.iter().map(|i|json!({"programIdIndex":index(i.program.pubkey),"accounts":i.accounts.iter().map(|a|index(a.pubkey)).collect::<Vec<_>>(),"data":bs58::encode(&i.data).into_string()})).collect::<Vec<_>>()}}});
    let verify=crate::execution_controlled_receipt_lot::real::receipt::binding::verify;
    verify(&original,&wire)?;
    for field in ["blockhash","account","operand","data"] {
        let mut value=original.clone();let m=&mut value["transaction"]["message"];
        match field {
            "blockhash"=>m["recentBlockhash"]=json!("11111111111111111111111111111111"),
            "account"=>m["accountKeys"][0]=json!("11111111111111111111111111111111"),
            "operand"=>{let i=m["instructions"].as_array_mut().unwrap().iter_mut().find(|i|i["accounts"].as_array().is_some_and(|a|!a.is_empty())).unwrap();i["accounts"][0]=json!(255);},
            _=>m["instructions"][0]["data"]=json!("1")
        }
        assert!(verify(&value,&wire).is_err(),"{field}");
    }
    Ok(())
}
#[tokio::test]
async fn real_inventory_requires_source_context_and_preserves_old_owned_balance()->Result<()>{
    use crate::execution_owned_sell_rpc::fractional::transport::Parsed;
    let bot="7vJDrxN46rmZXKAuVyv2ZRCNRMyRn3d25B5P2pkFcvrn";
    let mut c=super::generic_buy_fixture::config("http://127.0.0.1:1");c.canary_wallet_pubkey=bot.into();c.execution_signer_pubkey=bot.into();
    c.real_session=Some(serde_json::from_value(json!({"policy":"real_bounded_usdc_session_v1","session_id":"test","bot_wallet":bot,
        "source_wallet":"7v54NWdBtkjuAFJrLGsS2SXnuk8nKam81mZJeeYxVFi9","mint":copybot_storage_core::OWNER_EXIT_MINT,
        "route":"jupiter_swap_instructions","genesis":copybot_config::MAINNET_GENESIS,"activated_at":"test","deadline":"test",
        "max_cycles":2,"max_open_positions":1,"buy_lamports":10000000,"gross_buy_cap":20000000,"fee_cap":400000,
        "loss_cap":20000000,"total_fee_cap":100000,"priority_fee_cap":50000,"native_floor":0,"personal_usdc_raw":100,"slippage_bps":50}))?);
    let ata=crate::execution_controlled_receipt_lot::canonical(&c)?;
    let inventory=json!({"context":{"slot":42},"value":[{"pubkey":ata,"account":{"owner":"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
        "data":{"parsed":{"info":{"owner":bot,"mint":copybot_storage_core::OWNER_EXIT_MINT,"tokenAmount":{"decimals":6,"amount":"100"}}}}}}]});
    for mode in ["valid","stale","missing","old-owned"] {
        let mut value=inventory.clone();match mode {
            "stale"=>value["context"]["slot"]=json!(41),"missing"=>{value.as_object_mut().unwrap().remove("context");},
            "old-owned"=>value["value"][0]["account"]["data"]["parsed"]["info"]["tokenAmount"]["amount"]=json!("941205"),_=>{}
        }
        let mut rpc=Parsed(move |r:serde_json::Value|{
            assert_eq!(r["params"][2]["minContextSlot"],42);let v=value.clone();
            std::future::ready(Ok(json!({"jsonrpc":"2.0","id":r["id"],"result":v})))
        });
        let result=crate::execution_controlled_receipt_lot::real::receipt::entry_inventory(&c,&mut rpc,42,&mut ||Ok(())).await;
        match mode {"valid"=>result?,"old-owned"=>assert_eq!(result.unwrap_err().to_string(),"real_session_prior_inventory_obligation"),_=>assert_eq!(result.unwrap_err().to_string(),"real_session_entry_context")}
    }
    Ok(())
}
#[test]
#[ignore="explicit saved SOURCE response replay on copied stopped DB; no networking"]
fn replay_saved_real_source_witness()->Result<()>{
    use std::path::PathBuf;
    let root=PathBuf::from(std::env::var("REAL_SOURCE_CASE")?);
    let config:copybot_config::AppConfig=toml::from_str(&std::fs::read_to_string(root.join("package/config/bot.RUNTIME.toml"))?)?;
    let inputs:serde_json::Value=serde_json::from_slice(&std::fs::read(root.join("SOURCE_RPC_REPLAY_INPUTS.json"))?)?;
    let rows=inputs.as_array().unwrap();
    let response=|encoding:&str|->Result<serde_json::Value>{
        let input=rows.iter().find(|r|r["request"]["params"][1]["encoding"]==encoding).unwrap();
        Ok(serde_json::from_slice::<serde_json::Value>(&std::fs::read(input["response"].as_str().unwrap())?)?["result"].clone())
    };
    let compiled=response("json")?;let wire=response("base64")?;
    let signature=rows[0]["request"]["params"][0].as_str().unwrap();let slot=compiled["slot"].as_u64().unwrap();
    let store=copybot_storage_core::SqliteStore::open(root.join("source-replay-db/live_runtime.db"))?;
    crate::execution_controlled_receipt_lot::real::receipt::record(&store,&config.execution,signature,slot,"buy",&compiled,&wire)?;
    Ok(())
}
#[test]
fn compiled_initialize_account3_proves_only_exact_token_account_mint_and_owner()->Result<()>{
    let account=bs58::encode([7;32]).into_string();let mint=bs58::encode([8;32]).into_string();let wallet=bs58::encode([9;32]).into_string();
    let token="TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
    let keys=vec![account.as_str(),mint.as_str(),token];
    let data=[vec![18],vec![9;32]].concat();
    let valid=json!({"transaction":{"message":{"instructions":[{"programIdIndex":2,"accounts":[0,1],"data":bs58::encode(&data).into_string()}]}},"meta":{"innerInstructions":[]}});
    let verify=crate::execution_owned_sell_rpc::decode::token_init::initialized;
    assert!(verify(&valid,&keys,&account,&mint,&wallet)?);
    for mode in ["owner","account","mint","program","contradictory-program","contradictory-parsed","length"]{
        let mut v=valid.clone();let i=&mut v["transaction"]["message"]["instructions"][0];
        match mode {
            "owner"=>i["data"]=json!(bs58::encode([vec![18],vec![10;32]].concat()).into_string()),
            "account"=>i["accounts"][0]=json!(1),"mint"=>i["accounts"][1]=json!(0),
            "program"=>i["programIdIndex"]=json!(0),
            "contradictory-parsed"=>{i["data"]=json!(bs58::encode([vec![18],vec![10;32]].concat()).into_string());
                i["parsed"]=json!({"type":"initializeAccount3","info":{"account":account,"mint":mint,"owner":wallet}});},
            "contradictory-program"=>{i["programIdIndex"]=json!(0);i["programId"]=json!(token);},_=>i["data"]=json!(bs58::encode([vec![18],vec![9;33]].concat()).into_string())
        }
        assert!(!matches!(verify(&v,&keys,&account,&mint,&wallet),Ok(true)),"{mode}");
    }
    Ok(())
}
