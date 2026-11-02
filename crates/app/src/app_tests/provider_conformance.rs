//! Explicit offline replay of saved provider bytes. Scratch authority is a test
//! fixture only: this does not prove native admission, grant validity or dispatch.
use crate::execution_instruction_bundle_binding::BundleRequest;
use crate::execution_submit_adapter::*;
use anyhow::{ensure, Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use chrono::Utc;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

fn read(root: &Path, name: &str) -> Result<Value> {
    Ok(serde_json::from_slice(&std::fs::read(root.join(name))?)?)
}
fn write(root: &Path, name: &str, value: Value) -> Result<()> {
    std::fs::write(root.join(name), serde_json::to_vec_pretty(&value)?)?;
    Ok(())
}
fn request(q: Value, side: &str, c: &copybot_config::ExecutionConfig, ids: &Value) -> Result<ExecutionSubmitRequest> {
    let sample = crate::execution_quote_http::quote_sample_from_json(q.clone())?;
    let buy = side == "buy";
    let (input, output, amount) = if buy {
        ("So11111111111111111111111111111111111111112",copybot_storage_core::OWNER_EXIT_MINT,"10000000")
    } else {
        (copybot_storage_core::OWNER_EXIT_MINT,"So11111111111111111111111111111111111111112","941105")
    };
    ensure!(q["inputMint"]==input && q["outputMint"]==output && q["inAmount"]==amount
        && q["swapMode"]=="ExactIn" && q["slippageBps"]==50,"live_quote_operands");
    Ok(ExecutionSubmitRequest {
        order_id: if buy { ids["order_id"].as_str().context("order")?.into() } else { "exec-canary:provider-conformance-owned-sell-01".into() },
        signal_id: if buy {ids["signal_id"].as_str().context("signal")?.into()} else {"source-sell:provider-conformance-01".into()},
        client_order_id: if buy {ids["client_order_id"].as_str().context("client")?.into()} else {"copybot:provider-conformance-owned-sell-01".into()},
        attempt:1, route:c.canary_route.clone(),wallet_id:c.technical_cohort.as_ref().context("cohort")?.wallet_ids[0].clone(),
        token:copybot_storage_core::OWNER_EXIT_MINT.into(),side:side.into(),buy_size_sol:0.01,
        slippage_tolerance_bps:50,wallet_pubkey:c.canary_wallet_pubkey.clone(),entry_route_plan_json:None,
        metadata:ExecutionBuildPlanMetadata {
            quote_source:Some(crate::execution_quote_provider_selection::QUOTE_SOURCE_GENERIC_METIS.into()),
            quote_event_id:Some(format!("provider-conformance-{side}")),quote_status:Some("ok".into()),
            quote_in_amount_raw:Some(sample.in_amount),quote_out_amount_raw:Some(sample.out_amount),
            quote_response_json:Some(sample.response_json),route_plan_json:sample.route_plan_json,
            price_impact_pct:sample.price_impact_pct,slippage_bps:Some(50.0),
            priority_fee_status:Some("ok".into()),priority_fee_lamports:Some(50_000),
            priority_fee_json:Some(json!({"version":1,"source":"provider-conformance-config-cap", "unit":"total_priority_fee_lamports","value":50000}).to_string()),
            ..Default::default()
        }
    })
}

fn simulation_semantics(root: &Path,side: &str,c: &copybot_config::ExecutionConfig,response: &Value,
    message: &crate::execution_transaction_wire::DecodedMessage,total_fee: u64,floor: u64) -> Result<Value> {
    let value=&response["result"]["value"];
    ensure!(value["err"].is_null() && response["result"]["context"]["slot"].as_u64().is_some_and(|s|s>=floor),"simulation_success_context");
    ensure!(value["fee"].as_u64()==Some(total_fee) && value["unitsConsumed"].as_u64().is_some_and(|n|n>0),"simulation_fee_units");
    let canonical=crate::execution_controlled_receipt_lot::canonical(c)?;
    let amount=|field: &str| -> Result<u64> {
        let rows=value[field].as_array().context("simulation_token_rows")?;
        let rows=rows.iter().filter(|r|r["owner"]==c.canary_wallet_pubkey && r["mint"]==copybot_storage_core::OWNER_EXIT_MINT).collect::<Vec<_>>();
        ensure!(rows.len()==1,"simulation_wallet_token_identity");
        let r=rows[0];let index=r["accountIndex"].as_u64().context("accountIndex")? as usize;
        ensure!(r["programId"]=="TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA" && r["uiTokenAmount"]["decimals"]==6
            && message.binding.accounts.get(index).is_some_and(|a|bs58::encode(a.pubkey).into_string()==canonical),"simulation_token_account_binding");
        Ok(r["uiTokenAmount"]["amount"].as_str().context("token amount")?.parse()?)
    };
    let pre=amount("preTokenBalances")?;let post=amount("postTokenBalances")?;
    ensure!(pre==941205,"simulation_actual_inventory");
    let quote=read(root,&format!("{side}-controlled-quote.response.json"))?;
    let threshold:u64=quote["otherAmountThreshold"].as_str().context("threshold")?.parse()?;
    let native_pre=value["preBalances"][0].as_u64().context("native pre")?;
    let native_post=value["postBalances"][0].as_u64().context("native post")?;
    let output=if side=="buy" {
        ensure!(native_pre.checked_sub(native_post)==Some(10_000_000+total_fee),"simulation_buy_native_delta");
        post.checked_sub(pre).context("simulation_buy_token_delta")?
    } else {
        ensure!(pre.checked_sub(post)==Some(941105) && post==100,"simulation_sell_owned_only");
        native_post.checked_add(total_fee).and_then(|n|n.checked_sub(native_pre)).context("simulation_sell_native_delta")?
    };
    ensure!(output>=threshold,"simulation_output_below_threshold");
    let accounts=value["accounts"].as_array().context("simulation_accounts")?;
    ensure!(accounts.len()==2 && accounts[0]["lamports"].as_u64()==Some(native_post),"simulation_post_native_account");
    let token=STANDARD.decode(accounts[1]["data"][0].as_str().context("post token data")?)?;
    ensure!(token.len()==165 && u64::from_le_bytes(token[64..72].try_into()?)==post,"simulation_post_token_account");
    let logs=value["logs"].as_array().context("simulation_logs")?;
    ensure!(!logs.is_empty() && logs.iter().any(|l|l.as_str().is_some_and(|l|l.contains("Program 675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8 success"))),"simulation_raydium_success_log");
    Ok(json!({"pre_usdc_raw":pre,"post_usdc_raw":post,"native_pre":native_pre,"native_post":native_post,
        "output_raw":output,"quote_minimum_output_raw":threshold,"simulation_fee_lamports":total_fee,
        "units_consumed":value["unitsConsumed"],"logs_checked":true,"post_account_bytes_checked":true,
        "only_simulation_no_persisted_trade":true}))
}

#[tokio::test]
#[ignore = "explicit saved-response replay; requires PROVIDER_CONFORMANCE_ROOT and PRIVATE"]
async fn provider_conformance_saved_responses() -> Result<()> {
    let root=PathBuf::from(std::env::var("PROVIDER_CONFORMANCE_ROOT")?);
    let private=PathBuf::from(std::env::var("PROVIDER_CONFORMANCE_PRIVATE")?);
    let phase=std::env::var("PROVIDER_CONFORMANCE_PHASE")?;
    let mut c:copybot_config::ExecutionConfig=serde_json::from_value(read(&private,"execution-keyless.json")?)?;
    ensure!(c.execution_signer_keypair_path.is_empty() && c.quote_canary_api_key.is_empty()
        && c.local_bounded_session.is_none() && !c.enabled,"keyless_config");
    if phase=="quote_request" {
        c.quote_canary_base_url="https://api.jup.ag/swap/v1".into();
        let http=reqwest::Client::new();
        for (side,input,output,raw) in [("buy","So11111111111111111111111111111111111111112",copybot_storage_core::OWNER_EXIT_MINT,"10000000"),("sell",copybot_storage_core::OWNER_EXIT_MINT,"So11111111111111111111111111111111111111112","941105")] {
            let request=crate::execution_quote_http::build_owner_direct_quote_request(&http,&c,input,output,raw,50)?;
            let url=request.url();
            write(&root,&format!("{side}-controlled-quote-target.json"),json!({"method":request.method().as_str(),"path":format!("{}?{}",url.path(),url.query().context("query")?)}))?;
        }
        return Ok(());
    }
    // Historical DB bytes were copied. Changes below affect only the private scratch fixture.
    let db_path=private.join("scratch-v2/live_runtime.db");
    let sql=rusqlite::Connection::open(&db_path)?;
    let ids:Value=sql.query_row("SELECT order_id,signal_id,client_order_id FROM orders WHERE signal_id LIKE 'native-buy-v1:%' LIMIT 1",[],|r|Ok(json!({"order_id":r.get::<_,String>(0)?,"signal_id":r.get::<_,String>(1)?,"client_order_id":r.get::<_,String>(2)?})))?;
    let now=Utc::now();
    drop(sql);
    let store=copybot_storage_core::SqliteStore::open(&db_path)?;
    if phase!="body" {
        let inventory=read(&root,"inventory.response.json")?;
        copybot_storage_core::ordered_sell_quote::fractional::controlled::available(
            &inventory["result"],&crate::execution_controlled_receipt_lot::canonical(&c)?,
            &c.canary_wallet_pubkey,941105,inventory["result"]["context"]["slot"].as_u64().context("slot")?)?;
    }
    for side in ["buy","sell"] {
        let mut r=request(read(&root,&format!("{side}-controlled-quote.response.json"))?,side,&c,&ids)?;
        if side=="buy" {
            // Existing constructor; no direct private proof factory or hidden provider call.
            // A policy already exists in this isolated fixture, so no collect_payer branch.
            crate::execution_native_floor_policy::protected::prepare_request(&store,&c,&mut r,now).await?;
        } else {
            let snapshot=serde_json::from_value(read(&private,"sell-snapshot-fixture.json")?)?;
            crate::execution_wsol_lifecycle::authorize_owned(&c,&snapshot,&mut r)?;
        }
        let plan=JupiterMetisDryRunExecutionAdapter::new(c.clone()).build_transaction_plan(&r)?;
        ensure!(!plan.submit_enabled,"replay_plan_must_be_dry_run");
        let binding=BundleRequest::capture(&plan)?;
        write(&root,&format!("{side}-instructions-body.json"),binding.body().clone())?;
        if phase=="body" { continue; }
        let bound=binding.bind(&read(&root,&format!("{side}-instructions.response.json"))?)?;
        let rent=read(&root,"rent.response.json")?["result"].as_u64().context("rent")?;
        let transaction=if side=="buy" {
            crate::execution_guarded_generic_buy::assemble_with_rent(&c,&plan,&bound,0,Some(rent))?
        } else {
            crate::execution_guarded_generic_sell::assemble_unsigned_with_rent(&c,&plan,&bound,Some(rent))?
        };
        let payload=&transaction.serialized_transaction_base64;
        if side=="sell" {
            crate::execution_protected_token_baseline::wire(&c,&r,payload,941105)?;
        }
        let fee=crate::execution_priority_fee_wire::decode_priority_fee(payload)?;
        ensure!(fee.total<=50_000,"priority_cap");
        let decoded=crate::execution_transaction_wire::decode_message(payload,|_|Ok(()))?;
        let bytes=STANDARD.decode(payload)?;
        ensure!(bytes[0]==1 && bytes[1..65].iter().all(|b|*b==0),"all_signatures_zero");
        let mut options=json!({"encoding":"base64","sigVerify":false,
            "replaceRecentBlockhash":side=="buy" || !copybot_config::owned_sell_dispatch(&c),
            "commitment":if side=="buy" {"confirmed"} else {"finalized"},
            "accounts":{"encoding":"base64","addresses":[c.canary_wallet_pubkey,crate::execution_controlled_receipt_lot::canonical(&c)?]}});
        let slot=read(&root,"inventory.response.json")?["result"]["context"]["slot"].as_u64().context("inventory slot")?;
        options["minContextSlot"]=json!(slot);
        write(&root,&format!("{side}-simulation-body.json"),json!({"jsonrpc":"2.0","id":"execution-swap-transaction-simulate","method":"simulateTransaction","params":[payload,options]}))?;
        write(&root,&format!("{side}-wire.json"),json!({"source":transaction.source,"summary":transaction.summary,
            "transaction_sha256":decoded.binding.transaction_sha256,"message_sha256":decoded.binding.message_sha256,
            "message_base64":STANDARD.encode(&decoded.binding.message_bytes),"wire_bytes":bytes.len(),
            "priority_fee_lamports":fee.total,"all_signatures_zero":true,"required_signatures":decoded.binding.required_signatures,
            "accounts":decoded.binding.accounts.iter().map(|a|bs58::encode(a.pubkey).into_string()).collect::<Vec<_>>(),
            "scratch_context_only":true,"native_admission_or_financial_authority_proven":false}))?;
        if phase=="simulation" {
            let fee_response=read(&root,&format!("{side}-fee.response.json"))?;
            let saved_fee_request=read(&root,&format!("{side}-fee.request.json"))?["body"].clone();
            let response_for_fixture=fee_response.clone();
            let fixture=crate::app_tests::native_rpc_fixture::Fixture::start(false,move |_|crate::app_tests::native_rpc_fixture::Reply::json(response_for_fixture.clone())).await?;
            let client=crate::execution_native_rpc::NativeFundingRpcClient::new()?;
            let observation=client.collect_fee_only(&fixture.endpoint,std::time::Duration::from_secs(2),payload).await?;
            let (total_fee,fee_slot)=observation.bound_fee(&decoded.binding)?;
            let trace=fixture.finish().await?;
            ensure!(trace.len()==1 && trace[0].request==saved_fee_request,"actual_fee_request_replay_binding");
            ensure!(total_fee==55000 && fee_slot>=slot,"actual_total_fee_context");
            write(&root,&format!("{side}-fee-adapter-result.json"),json!({"total_fee_lamports":total_fee,"priority_fee_lamports":fee.total,
                "fee_context_slot":fee_slot,"exact_final_message_binding":true,"saved_real_response_replayed_through_current_client":true,"loopback_requests":1,"external_requests":0}))?;
            let response=read(&root,&format!("{side}-simulation.response.json"))?;
            let result=crate::execution_transaction_rpc_simulation::parse_rpc_simulation_response(&response,&transaction.source);
            write(&root,&format!("{side}-simulation-semantics.json"),simulation_semantics(&root,side,&c,&response,&decoded,total_fee,slot)?)?;
            let owned=if side=="sell" {Some(crate::execution_guarded_generic_sell::owned_simulation::verify(&response,&transaction.source,payload).map_err(|e|e.to_string()))}else{None};
            write(&root,&format!("{side}-adapter-result.json"),json!({"generic_parser_passed":result.is_ok(),
                "generic_error":result.err().map(|e|e.to_string()),"owned_parser_passed":owned.as_ref().map(|v|v.is_ok()),
                "owned_error":owned.and_then(|v|v.err()),"response_context_slot":response["result"]["context"]["slot"],
                "value_err":response["result"]["value"]["err"]}))?;
        }
    }
    Ok(())
}
