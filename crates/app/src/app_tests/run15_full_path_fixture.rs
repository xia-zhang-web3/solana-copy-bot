//! Explicit model follower funding, receipt and complete historical inventory.
use super::run15_rpc_proof_fixture as f;
use crate::execution_canary_route::{
    NativeBuyMockAdapter, NativeBuyMockCounts, NativeBuyMockIo, NativeBuyRunnerOperands,
};
use crate::execution_quote_canary_helpers::{PriorityFeeSample, QuoteSample};
use anyhow::Result;
use copybot_config::AppConfig;
use copybot_storage_core::ordered_sell_quote::fractional::inventory::{
    Evidence, Page, TOKEN, TOKEN22,
};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};

// Model execution price follows the saved source BUY; risk filters stay intact.
pub(super) const OWNED: u64 = (163956354_u128 * 10_000_000 / 6120191) as u64;
pub(super) const SOLD: u64 = (OWNED as u128 * 264731434 / 6770149697) as u64;
pub(super) fn evidence(corpus: &std::path::Path, model: &Value) -> Result<Evidence> {
    let record: Value = serde_json::from_slice(&std::fs::read(corpus.join("response-725.json"))?)?;
    let block = record["result"].clone();
    let slot = record["params"][0].as_u64().unwrap();
    let owner = model["transaction"]["message"]["accountKeys"][0]
        .as_str()
        .unwrap();
    let target_index=model["meta"]["postTokenBalances"].as_array().unwrap().iter()
        .find(|r|r["owner"]==owner && r["mint"]==f::MINT).unwrap()["accountIndex"].as_u64().unwrap() as usize;
    let target=model["transaction"]["message"]["accountKeys"][target_index].as_str().unwrap();
    let row = |key: &str, wallet: &str, amount: u64| {
        json!({"pubkey":key,"account":{
        "owner":TOKEN,"data":{"parsed":{"type":"account","info":{"owner":wallet,"mint":f::MINT,
            "tokenAmount":{"amount":amount.to_string(),"decimals":9}}}}}})
    };
    let page = |program: &str, values: Vec<Value>| Page {
        program: program.into(),
        cursor: None,
        response: json!({"context":{"slot":slot-1},"value":values,"pageKey":null}),
    };
    Ok(Evidence {
        version: 1,
        slot,
        parent: json!({"blockhash":block["previousBlockhash"]}),
        block,
        pages: vec![
            page(TOKEN22, vec![]),
            page(
                TOKEN,
                vec![row(
                    "eNQee2A5Bsx56JJMs3LBezwkBNeqvt8P98fesn43yLD",
                    f::LEADER,
                    6770149697,
                )],
            ),
        ],
        execution_accounts: json!({"context":{"slot":slot+1},"value":[row(target,owner,OWNED)]}),
    })
}
pub(super) fn config(
    path: &std::path::Path,
    root: &std::path::Path,
    url: &str,
    model: &Value,
) -> Result<AppConfig> {
    let mut app = copybot_config::load_from_path(path)?;
    let c = &mut app.execution;
    let wallet = model["transaction"]["message"]["accountKeys"][0]
        .as_str()
        .unwrap();
    c.canary_wallet_pubkey = wallet.into();
    c.execution_signer_pubkey = wallet.into();
    let key = ed25519_dalek::SigningKey::from_bytes(&[11; 32]);
    let signer = root.join("synthetic-key.json");
    std::fs::write(
        &signer,
        serde_json::to_vec(&key.to_keypair_bytes().as_slice())?,
    )?;
    c.execution_signer_keypair_path = signer.to_string_lossy().into_owned();
    c.canary_kill_switch_path = root.join("STOP").to_string_lossy().into_owned();
    c.canary_tiny_submit_enabled = true;
    c.canary_entry_submit_enabled = true;
    c.tiny_experiment.activate = true;
    c.submit_adapter_http_url = url.into();
    c.quote_canary_base_url = format!("{url}/swap/v1");
    c.priority_fee_canary_rpc_url = url.into();
    c.quote_canary_api_key = "offline-model".into();
    c.quote_canary_timeout_ms = 4000;
    c.submit_timeout_ms = 200;
    c.max_confirm_seconds = 1;
    c.owned_sell_preparation.as_mut().unwrap().rpc_url = url.into();
    c.owned_sell_preparation.as_mut().unwrap().genesis_hash =
        "11111111111111111111111111111111".into();
    let cohort = c.technical_cohort.as_mut().unwrap();
    cohort.activate = true;
    cohort.wallet_ids = vec![f::LEADER.into()];
    let now = chrono::Utc::now() - chrono::Duration::seconds(180);
    cohort.activated_at = now.to_rfc3339();
    cohort.deadline = (now + chrono::Duration::seconds(14400)).to_rfc3339();
    app.ingestion.yellowstone_grpc_url = "http://127.0.0.1:1".into();
    app.ingestion.yellowstone_x_token = "offline-model".into();
    app.ingestion.helius_http_url = "http://127.0.0.1:1".into();
    app.ingestion.helius_http_urls = vec![];
    assert_eq!(
        (
            cohort.max_buy_count,
            cohort.max_source_sell_count,
            cohort.max_wait_seconds
        ),
        (1, 1, 14400)
    );
    assert_eq!(c.canary_buy_size_sol, 0.01);
    assert_eq!(c.pretrade_min_sol_reserve, 0.160200031);
    copybot_config::validate_association_delivery(&app)?;
    Ok(app)
}
pub(super) fn parsed(model: &Value, payload: &str) -> Result<Value> {
    super::run15_receipt_projection::parsed(model,payload)
}
pub(super) fn io(
    c: &copybot_config::ExecutionConfig,
    corpus: &std::path::Path,
    payload: String,
    model: &Value,
) -> Result<Arc<NativeBuyMockIo>> {
    let now = chrono::Utc::now();
    let wire = crate::execution_transaction_wire::decode_message(&payload, |_| Ok(()))?;
    let signature = model["transaction"]["signatures"][0]
        .as_str()
        .unwrap()
        .to_owned();
    let quote = || {
        let body = super::run15_buy_wire_fixture::quote(f::MINT, 10_000_000, OWNED, 50);
        QuoteSample {
            http_request_started_ts: Some(now),
            quote_response_available_ts: Some(now),
            in_amount: "10000000".into(),
            out_amount: OWNED.to_string(),
            response_json: body.to_string(),
            price_impact_pct: Some(0.0),
            route_plan_json: Some(body["routePlan"].to_string()),
            in_decimals: Some(9),
            out_decimals: Some(9),
            latency_ms: 0,
        }
    };
    let counts = Arc::new(Mutex::new(NativeBuyMockCounts::default()));
    let mint: Value = serde_json::from_slice(&std::fs::read(corpus.join("response-719.json"))?)?;
    assert_eq!(mint["params"][0],json!([f::MINT]));
    let source: Value = serde_json::from_slice(&std::fs::read(corpus.join("response-727.json"))?)?;
    let mut finalized_source=source["result"].clone();
    let message=&mut finalized_source["transaction"]["message"];
    let signers=message["header"]["numRequiredSignatures"].as_u64().unwrap() as usize;
    let readonly=message["header"]["numReadonlyUnsignedAccounts"].as_u64().unwrap() as usize;
    let keys=message["accountKeys"].as_array().unwrap();let len=keys.len();
    // Equivalent jsonParsed account-key projection from the saved raw header.
    message["accountKeys"]=json!(keys.iter().enumerate().map(|(i,k)|json!({
        "pubkey":k,"signer":i<signers,"writable":i<len-readonly})).collect::<Vec<_>>());
    let wallet = wire.binding.accounts[0].pubkey;
    let initial_sol = crate::execution_native_rpc::synthetic_classic_funding(
        &payload,
        wallet,
        1_000_000_000,
        19000,
        451313057,
    )?;
    let receipt = parsed(model, &payload)?;
    Ok(Arc::new(NativeBuyMockIo {
        runner: Some(NativeBuyRunnerOperands {
            finalized_genesis: "11111111111111111111111111111111".into(),
            finalized_transaction: finalized_source,
            mint_account: json!({"context":mint["result"]["context"],"value":mint["result"]["value"][0]}),
            initial_quote: quote(),
            fresh_quote: quote(),
            priority: PriorityFeeSample {
                status: "ok".into(),
                lamports: Some(2000),
                json: Some(super::priority_fee_fixture::total_json(2000)),
                error: None,
            },
            adapter: NativeBuyMockAdapter {
                config: c.clone(),
                payload,
                signature: signature.clone(),
                counts: counts.clone(),
            },
        }),
        initial_sol,
        fee_lamports: 19000,
        fee_slot: 451313057,
        expected_message_sha256: wire.binding.message_sha256,
        submit_signature: Some(signature),
        confirmation: json!({"result":{"value":[{"err":null,"slot":451313057,"confirmationStatus":"confirmed"}]}}),
        receipt: json!({"jsonrpc":"2.0","result":receipt}),
        counts,
    }))
}
