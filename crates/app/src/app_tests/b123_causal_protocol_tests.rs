//! Synthetic API-boundary harness derived from120; never a daemon lifecycle.
use super::b123_causal_fixture::Boundary;
use super::token2022_ata_inputs_tests::{key, save, Spec, MINT, PAYER, RESERVE};
use crate::execution_submit_adapter::*;
use anyhow::{ensure, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{json, Value};
use std::{
    path::Path,
    time::{Duration, Instant},
};

async fn response(
    mut r: reqwest::Response,
    dir: &Path,
    label: &str,
    limit: usize,
) -> Result<Value> {
    let status = r.status();
    let mut bytes = Vec::new();
    while let Some(chunk) = r.chunk().await? {
        ensure!(bytes.len() + chunk.len() <= limit, "response_size:{label}");
        bytes.extend_from_slice(&chunk);
    }
    std::fs::write(dir.join(format!("{label}-response.body")), &bytes)?;
    ensure!(status.is_success(), "HTTP_refusal:{label}:{status}");
    let value: Value = serde_json::from_slice(&bytes)?;
    ensure!(
        value.get("error").is_none_or(Value::is_null),
        "provider_error:{label}:{}",
        value["error"]
    );
    ensure!(
        value.get("simulationError").is_none_or(Value::is_null),
        "provider_simulation_error:{label}:{}",
        value["simulationError"]
    );
    Ok(value)
}

async fn protocol(dir: &Path, stage: &mut &'static str) -> Result<Value> {
    let base = std::env::var("B120_BASE")?;
    ensure!(
        base.starts_with("http://127.0.0.1:"),
        "loopback_only_harness"
    );
    let config = super::generic_buy_fixture::config(&base);
    let http = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .build()?;
    *stage = "quote";
    let request = crate::execution_quote_http::build_quote_request(
        &http,
        &config.quote_canary_base_url,
        "",
        10_000,
        super::generic_buy_fixture::WSOL,
        MINT,
        "10000000",
        500,
    )?;
    save(
        dir,
        "quote-request.json",
        &json!({"method":"GET","query":request.url().query()}),
    );
    let start = chrono::Utc::now();
    let timer = Instant::now();
    let reply = http.execute(request).await?;
    let quote = response(reply, dir, "quote", 2 * 1024 * 1024).await?;
    save(
        dir,
        "quote-availability.json",
        &json!({"request_started":start,"response_available":chrono::Utc::now(),"elapsed_ms":timer.elapsed().as_millis(),"kind":"harness transport timestamps; replay timestamps not chain freshness"}),
    );
    let mut req = super::generic_buy_fixture::request("buy", quote.clone())?;
    req.order_id = "batch120-unsigned-generic".into();
    req.signal_id = "batch120-synthetic-signal".into();
    req.client_order_id = "batch120-synthetic-client".into();
    req.metadata.quote_event_id = Some("batch120-one-quote".into());
    req.metadata.priority_fee_source = Some("batch120-fixed-cap-input".into());
    let plan =
        JupiterMetisDryRunExecutionAdapter::new(config.clone()).build_transaction_plan(&req)?;
    let binding = crate::execution_instruction_bundle_binding::BundleRequest::capture(&plan)?;
    save(dir, "bundle-request.json", binding.body());
    save(
        dir,
        "plan.json",
        &json!({"plan":format!("{plan:#?}"),"quote":quote,"request":format!("{req:#?}")}),
    );
    *stage = "instructions";
    let reply = http
        .post(format!("{base}/swap/v1/swap-instructions"))
        .timeout(Duration::from_secs(10))
        .json(binding.body())
        .send()
        .await?;
    let value = response(
        reply,
        dir,
        "instructions",
        crate::execution_instruction_bundle::MAX_RESPONSE_BYTES,
    )
    .await?;
    *stage = "binding_assembly";
    let bundle = binding.bind(&value)?;
    let transaction =
        crate::execution_guarded_generic_buy::assemble(&config, &plan, &bundle, RESERVE)?;
    let bytes = STANDARD.decode(&transaction.serialized_transaction_base64)?;
    ensure!(
        bytes.len() <= 1232 && bytes[0] == 1 && bytes[1..65].iter().all(|b| *b == 0),
        "unsigned_wire"
    );
    std::fs::write(dir.join("transaction.bin"), &bytes)?;
    let decoded = crate::execution_transaction_wire::decode_message(
        &transaction.serialized_transaction_base64,
        |_| Ok(()),
    )?;
    let keys: Vec<_> = decoded
        .binding
        .accounts
        .iter()
        .map(|a| bs58::encode(a.pubkey).into_string())
        .collect();
    ensure!(keys[0] == PAYER);
    save(
        dir,
        "wire.json",
        &json!({"payload":transaction.serialized_transaction_base64,"bytes":bytes.len(),
        "transaction_sha256":decoded.binding.transaction_sha256,"message_sha256":decoded.binding.message_sha256,
        "message_base64":STANDARD.encode(&decoded.binding.message_bytes),"keys":keys,
        "source":transaction.source,"original_blockhash":value["blockhashWithMetadata"],
        "instructions":format!("{:#?}",decoded.instructions),"zero_signature_placeholders":true,"reserve":RESERVE}),
    );
    *stage = "typed_prerequisites";
    // Only wallet/payload are consumed by Boundary. No supplied account, fee or rent facts.
    let spec = Spec {
        payload: transaction.serialized_transaction_base64.clone(),
        wallet: key(PAYER),
        message: STANDARD.encode(&decoded.binding.message_bytes),
        keys: json!(keys),
        rows: vec![],
        fee: None,
        rent: 0,
        rent170: 0,
    };
    let f = Boundary::new(&spec, "buy", dir)?;
    let before = f.sql()?;
    let state = crate::execution_tiny_submit_state::eligible(&f.store, &f.request)
        .map_err(anyhow::Error::msg)?;
    let transport = RpcExecutionSubmitTransport::new(format!("{base}/rpc"));
    *stage = "before_send";
    let timer = Instant::now();
    let out = crate::execution_initial_sol_submit::before_send(
        &f.store,
        &f.request,
        &f.envelope,
        &f.intent,
        &f.gate,
        &transport,
        &state,
        f.now,
    )
    .await;
    save(
        dir,
        "funding.json",
        &json!({"admitted":out.is_none(),"result":format!("{out:#?}"),
        "elapsed_ms":timer.elapsed().as_millis(),"before":before,"after":f.sql()?,"actual_before_send_calls":1,
        "fixture_prior_simulation":"synthetic local status only; zero prior new chain simulations",
        "sign":0,"submit":0,"key_loader":0}),
    );
    if let Some(out) = out {
        anyhow::bail!(
            "before_send_refusal:{}",
            out.error.clone().unwrap_or_else(|| format!("{out:?}"))
        );
    }
    *stage = "simulation";
    let simulation =
        crate::execution_transaction_rpc_simulation::verify_serialized_transaction_rpc_simulation(
            &http,
            &config,
            &transaction.serialized_transaction_base64,
            &transaction.source,
            Duration::from_secs(10),
        )
        .await;
    save(
        dir,
        "simulation.json",
        &json!({"actual_parser":format!("{simulation:#?}"),"sql_after":f.sql()?,
        "sigVerify":false,"replaceRecentBlockhash":true,"commitment":"confirmed","original_landing_proof":false}),
    );
    let simulation = simulation?;
    ensure!(
        matches!(
            simulation,
            crate::execution_transaction_rpc_simulation::RpcSimulationOutcome::Passed { .. }
        ),
        "simulation_skipped"
    );
    *stage = "complete";
    Ok(json!({"funding_admitted":true,"simulation":format!("{simulation:?}")}))
}

#[tokio::test]
#[ignore = "requires configured loopback replay harness"]
async fn batch120_protocol() -> Result<()> {
    let dir = std::path::PathBuf::from(std::env::var("B120_OUTPUT")?);
    let mut stage = "start";
    let result = protocol(&dir, &mut stage).await;
    save(
        &dir,
        "outcome.json",
        &json!({"stage":stage,"result":format!("{result:#?}"),"success":result.is_ok(),
        "scope":"generic seam + typed unsigned selected funding; not full daemon admission/ownership/landing"}),
    );
    Ok(())
}
