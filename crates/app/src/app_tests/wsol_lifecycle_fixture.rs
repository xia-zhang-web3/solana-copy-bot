//! Actual daemon producer with LOCAL canonical instruction/quote and parsed RPC mocks.
use super::{owner_buy_wire_fixture as buy, wsol_lifecycle_oracle as model};
use crate::execution_instruction_bundle_binding::BundleRequest;
use crate::execution_pumpswap_accounts::*;
use crate::execution_solana_tx::{PubkeyBytes, SolanaInstruction as I};
use crate::execution_submit_adapter::*;
use anyhow::{ensure, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use copybot_config::ExecutionConfig;
use serde_json::{json, Value};
pub(super) fn bundle(ix: &[I]) -> Result<Value> {
    let encode = |i: &I| json!({"programId":bs58::encode(i.program_id).into_string(),"accounts":i.accounts.iter().map(|a|json!({"pubkey":bs58::encode(a.pubkey).into_string(),"isSigner":a.is_signer,"isWritable":a.is_writable})).collect::<Vec<_>>(),"data":STANDARD.encode(&i.data)});
    let route = ix
        .iter()
        .position(|i| i.data.starts_with(&[229, 23, 203, 151, 122, 227, 173, 42]))
        .unwrap();
    let budget = ix
        .iter()
        .take_while(|i| i.program_id == compute_budget_program_id())
        .count();
    Ok(
        json!({"tokenLedgerInstruction":null,"computeBudgetInstructions":ix[..budget].iter().map(encode).collect::<Vec<_>>(),"setupInstructions":ix[budget..route].iter().map(encode).collect::<Vec<_>>(),"swapInstruction":encode(&ix[route]),"cleanupInstruction":ix.get(route+1).map(encode),"otherInstructions":[],"addressLookupTableAddresses":[],"simulationError":null,"blockhashWithMetadata":{"blockhash":vec![9;32],"lastValidBlockHeight":1,"fetchedAt":{"secs_since_epoch":1,"nanos_since_epoch":0}}}),
    )
}
/// The modeled provider chooses the already-supported compact Raydium-v2 leg.
pub(super) fn compact_buy(ix: &[I]) -> Vec<I> {
    let mut ix = ix.to_vec();
    if let Some(r) = ix.iter_mut().find(|i| {
        i.data.starts_with(&[229, 23, 203, 151, 122, 227, 173, 42]) && i.data.get(12) == Some(&7)
    }) {
        let mut a = r.accounts[..9].to_vec();
        for n in [9, 10, 11, 12, 13, 14, 24, 25, 26] {
            a.push(r.accounts[n].clone());
        }
        r.accounts = a;
        r.data[12] = 105;
    }
    ix
}
pub(super) fn build(c: &ExecutionConfig, r: &ExecutionSubmitRequest, ix: &[I]) -> Result<String> {
    let ix = compact_buy(ix);
    let plan = JupiterMetisDryRunExecutionAdapter::new(c.clone()).build_transaction_plan(r)?;
    let binding = BundleRequest::capture(&plan)?;
    ensure!(
        binding.body()["useSharedAccounts"] == false && binding.body()["dynamicSlippage"] == false,
        "local_producer_closed_body"
    );
    let bound = binding.clone().bind(&bundle(&ix)?)?;
    let result = if r.side == "buy" {
        crate::execution_guarded_generic_buy::assemble_with_rent(
            c,
            &plan,
            &bound,
            0,
            Some(model::RENT),
        )?
    } else {
        crate::execution_guarded_generic_sell::assemble_unsigned_with_rent(
            c,
            &plan,
            &bound,
            Some(model::RENT),
        )?
    };
    Ok(result.serialized_transaction_base64)
}
pub(super) fn exercise(
    role: &str,
    c: &ExecutionConfig,
    r: &ExecutionSubmitRequest,
    payload: &str,
) -> Result<Value> {
    let wallet = parse_pubkey(&r.wallet_pubkey, "local_wallet")?;
    let canonical = associated_token_address(&wallet, &wsol_mint(), &token_program_id());
    let (_, temp) = crate::execution_wsol_lifecycle::temporary(r)?;
    crate::execution_native_floor_policy::verify_submit_payload_scoped(
        r,
        payload,
        c.pretrade_min_sol_reserve,
        &c.canary_wallet_pubkey,
        Some(c),
    )?;
    let mut lost = r.clone();
    lost.metadata = serde_json::from_value(serde_json::to_value(&r.metadata)?)?;
    assert!(
        crate::execution_native_floor_policy::verify_submit_payload_scoped(
            &lost,
            payload,
            c.pretrade_min_sol_reserve,
            &c.canary_wallet_pubkey,
            Some(c)
        )
        .is_err()
    );
    let msg = crate::execution_transaction_wire::decode_message(payload, |_| Ok(()))?;
    assert_eq!(msg.binding.required_signatures, 1);
    assert!(!msg.binding.accounts.iter().any(|a| a.pubkey == canonical));
    let old = crate::execution_wsol_lifecycle::canonical_view_scoped(r, payload)?;
    let mut legacy = model::Bank::new(wallet)?;
    legacy.canonical(7_777_777);
    legacy.execute(&old)?;
    assert!(
        !legacy.accounts.contains_key(&canonical),
        "legacy schedule must expose the old sweep"
    );
    let mut bank = model::Bank::new(wallet)?;
    // The owner creates/funds canonical WSOL after our (absent) snapshot.
    bank.canonical(7_777_777);
    let external = bank.accounts[&canonical].clone();
    let mut receipt = bank.execute(payload)?;
    assert_eq!(bank.accounts.get(&canonical), Some(&external));
    assert!(!bank.accounts.contains_key(&temp));
    let mut existing = model::Bank::new(wallet)?;
    existing.canonical(88_888_888);
    let snapshot = existing.accounts[&canonical].clone();
    existing.execute(payload)?;
    assert_eq!(existing.accounts.get(&canonical), Some(&snapshot));
    let facts = crate::execution_native_rpc::synthetic_classic_funding(
        payload,
        wallet,
        20_000_000,
        model::FEE,
        151,
    )?;
    let required = crate::execution_wsol_lifecycle::check_initial(r, payload, &facts)?;
    assert!(required <= 15_000_000);
    if let Some(proof) = r.metadata.protected_capital.as_deref() {
        crate::execution_initial_sol::check_with_policy(payload, wallet, 0, &facts, Some(proof))?;
    }
    let short = crate::execution_native_rpc::synthetic_classic_funding(
        payload,
        wallet,
        u64::try_from(required)? - 1,
        model::FEE,
        151,
    )?;
    assert!(crate::execution_wsol_lifecycle::check_initial(r, payload, &short).is_err());
    let mut rent = facts.clone();
    rent.rent.lamports += 1;
    assert!(crate::execution_wsol_lifecycle::check_initial(r, payload, &rent).is_err());
    // Occupied/prefunded deterministic address is not idempotently reused.
    let mut occupied = model::Bank::new(wallet)?;
    occupied.canonical(9_999);
    occupied.accounts.insert(
        temp,
        model::Account {
            lamports: 1,
            token: None,
            initialized: false,
        },
    );
    let mut expected = occupied.clone();
    expected.accounts.get_mut(&wallet).unwrap().lamports -= model::FEE;
    assert!(occupied
        .execute(payload)
        .unwrap_err()
        .to_string()
        .contains("already_in_use"));
    assert_eq!(occupied, expected); // only the bounded modeled network fee remains
    assert!(occupied.execute(payload).is_err()); // same order cannot select a fresh address
    let mut occupied_facts = facts.clone();
    occupied_facts
        .native
        .accounts
        .value
        .iter_mut()
        .find(|a| a.pubkey == temp)
        .unwrap()
        .account = crate::execution_native_rpc::types::AccountObservation::Present {
        lamports: 1,
        owner_program: system_program_id(),
        executable: false,
        data: vec![],
    };
    assert!(crate::execution_wsol_lifecycle::check_initial(r, payload, &occupied_facts).is_err());
    let mut foreign = r.clone();
    foreign.order_id.push_str("-foreign");
    assert!(
        crate::execution_native_floor_policy::verify_submit_payload_scoped(
            &foreign,
            payload,
            c.pretrade_min_sol_reserve,
            &c.canary_wallet_pubkey,
            Some(c)
        )
        .is_err()
    );
    assert!(crate::execution_wsol_lifecycle::canonical_view_scoped(&foreign, payload).is_err());
    let mut changed = msg
        .instructions
        .iter()
        .map(|i| I {
            program_id: i.program.pubkey,
            accounts: i.accounts.clone(),
            data: i.data.clone(),
        })
        .collect::<Vec<_>>();
    changed
        .iter_mut()
        .find(|i| i.program_id == token_program_id() && i.data == [9])
        .unwrap()
        .accounts[0]
        .pubkey = canonical;
    let bad = STANDARD.encode(
        crate::execution_solana_tx::serialize_unsigned_legacy_transaction(
            wallet, [9; 32], &changed,
        )?,
    );
    assert!(crate::execution_wsol_lifecycle::canonical_view_scoped(r, &bad).is_err());
    let mut no_init = msg
        .instructions
        .iter()
        .map(|i| I {
            program_id: i.program.pubkey,
            accounts: i.accounts.clone(),
            data: i.data.clone(),
        })
        .filter(|i| !(i.program_id == token_program_id() && i.data.first() == Some(&18)))
        .collect::<Vec<_>>();
    let bad = STANDARD.encode(
        crate::execution_solana_tx::serialize_unsigned_legacy_transaction(
            wallet, [9; 32], &no_init,
        )?,
    );
    assert!(crate::execution_wsol_lifecycle::canonical_view_scoped(r, &bad).is_err());
    no_init.clear();
    receipt["result"]["transaction"]["signatures"] =
        json!([if role.starts_with("source") && role.ends_with("buy") {
            super::receipt_lot_custody_fixture::BUY_SIG.to_owned()
        } else if role.starts_with("source") && role.ends_with("sell") {
            super::receipt_lot_custody_fixture::SELL_SIG.to_owned()
        } else if role == "bot-buy" {
            serde_json::from_slice::<Value>(&std::fs::read(
                super::b135_fixture::inputs().join("chain.json"),
            )?)?["our"]["signature"]
                .as_str()
                .unwrap()
                .to_owned()
        } else {
            bs58::encode([231; 64]).into_string()
        }]);
    receipt["result"]["slot"] = json!(if role.ends_with("buy") {
        if role.starts_with("source") && role.ends_with("buy") {
            100
        } else {
            120
        }
    } else if role.starts_with("source") {
        150
    } else {
        151
    });
    export(role, c, r, payload, &receipt)?;
    Ok(receipt)
}
pub(super) fn export(
    role: &str,
    c: &ExecutionConfig,
    r: &ExecutionSubmitRequest,
    payload: &str,
    receipt: &Value,
) -> Result<()> {
    if role.starts_with("source-boundary") {
        return Ok(());
    }
    let Ok(root) = std::env::var("WSOL_LIFECYCLE_FIXTURE_DIR") else {
        return Ok(());
    };
    let path = std::path::Path::new(&root).join(role);
    std::fs::create_dir_all(&path)?;
    let plan = JupiterMetisDryRunExecutionAdapter::new(c.clone()).build_transaction_plan(r)?;
    let body =
        crate::execution_swap_http_request::swap_request_body(&plan, &r.wallet_pubkey, "LOCAL")?;
    let (seed, temp) = crate::execution_wsol_lifecycle::temporary(r)?;
    for (name, value) in [
        (
            "request.json",
            json!({"order_id":r.order_id,"signal_id":r.signal_id,"client_order_id":r.client_order_id,"attempt":r.attempt,"route":r.route,"wallet_id":r.wallet_id,"token":r.token,"side":r.side,"buy_size_sol":r.buy_size_sol,"slippage_tolerance_bps":r.slippage_tolerance_bps,"wallet_pubkey":r.wallet_pubkey,"entry_route_plan_json":r.entry_route_plan_json,"metadata":r.metadata}),
        ),
        (
            "config.json",
            json!({"controlled_receipt_lot":c.controlled_receipt_lot,"canary_wallet_pubkey":c.canary_wallet_pubkey,"execution_signer_pubkey":c.execution_signer_pubkey,"pretrade_max_priority_fee_lamports":c.pretrade_max_priority_fee_lamports,"pretrade_min_sol_reserve":c.pretrade_min_sol_reserve,"canary_buy_size_sol":c.canary_buy_size_sol,"canary_max_daily_loss_sol":c.canary_max_daily_loss_sol,"canary_max_open_positions":c.canary_max_open_positions,"max_submit_attempts":c.max_submit_attempts,"canary_route":c.canary_route}),
        ),
        ("swap-body.json", body),
        ("receipt.json", receipt.clone()),
        (
            "profile.json",
            json!({"kind":"LOCAL_SYNTHETIC_PREPARED_WIRE_INSTRUCTION_ORACLE","profile":copybot_config::TRANSACTION_OWNED_WSOL_V1,"seed_domain":"copybot-transaction-owned-wsol-v1","seed":seed,"temporary":bs58::encode(temp).into_string(),"order":r.order_id,"signatures_created":0,"external_requests":0,"simulation":"instruction-level System/Token + verified ExactIn modeled AMM; not VM/onchain","cash":"partial_unknown retained when parsed CPI evidence insufficient"}),
        ),
    ] {
        std::fs::write(path.join(name), serde_json::to_vec_pretty(&value)?)?;
    }
    std::fs::write(path.join("unsigned.base64"), payload)?;
    std::fs::write(
        path.join("legacy_provider.base64"),
        crate::execution_wsol_lifecycle::canonical_view_scoped(r, payload)?,
    )?;
    if c.owned_sell_preparation.is_some() {
        std::fs::write(
            path.join("identity-inputs.json"),
            serde_json::to_vec_pretty(&super::wsol_lifecycle_identity_fixture::projection(c))?,
        )?;
        std::fs::write(
            path.join("policy-identities.json"),
            serde_json::to_vec_pretty(
                &json!({"native_buy_policy_identity":crate::execution_native_buy_rpc::policy_identity(c)?,"owned_sell_identity":crate::execution_owned_sell_rpc::identity(c)?}),
            )?,
        )?;
    }
    Ok(())
}
pub(super) async fn native_buy(
    db: &super::association_fixture::Db,
    c: &ExecutionConfig,
    order: &copybot_storage_core::ExecutionCanaryOrder,
) -> Result<(String, Value)> {
    let signal = db
        .store
        .load_copy_signal_by_signal_id(&order.signal_id)?
        .unwrap();
    let mut q = buy::quote();
    q["outAmount"] = json!(super::protected_usdc_fixture::H.to_string());
    q["otherAmountThreshold"] = json!("6965");
    q["routePlan"][0]["swapInfo"]["outAmount"] = q["outAmount"].clone();
    let mut metadata = super::priority_fee_fixture::metadata();
    metadata.quote_event_id = Some("LOCAL native BUY quote".into());
    metadata.quote_in_amount_raw = Some("10000000".into());
    metadata.quote_out_amount_raw = Some(q["outAmount"].as_str().unwrap().into());
    metadata.quote_response_json = Some(q.to_string());
    metadata.route_plan_json = Some(q["routePlan"].to_string());
    let mut r = ExecutionSubmitRequest {
        order_id: order.order_id.clone(),
        signal_id: order.signal_id.clone(),
        client_order_id: order.client_order_id.clone(),
        attempt: order.attempt,
        route: order.route.clone(),
        wallet_id: signal.wallet_id,
        token: buy::USDC.into(),
        side: "buy".into(),
        buy_size_sol: 0.01,
        slippage_tolerance_bps: 50,
        wallet_pubkey: c.canary_wallet_pubkey.clone(),
        entry_route_plan_json: None,
        metadata,
    };
    crate::execution_native_floor_policy::protected::prepare_request(
        &db.store,
        c,
        &mut r,
        chrono::Utc::now(),
    )
    .await?;
    let wallet = parse_pubkey(&r.wallet_pubkey, "LOCALbot")?;
    let mut ix = buy::instructions(wallet)?;
    ix.iter_mut()
        .find(|i| i.data.starts_with(&[229, 23, 203, 151, 122, 227, 173, 42]))
        .unwrap()
        .data[24..32]
        .copy_from_slice(&super::protected_usdc_fixture::H.to_le_bytes());
    let payload = build(c, &r, &ix)?;
    let receipt = exercise("bot-buy", c, &r, &payload)?;
    Ok((payload, receipt))
}
pub(super) fn snapshot(role: &str, sql: &rusqlite::Connection) -> Result<()> {
    if let Ok(root) = std::env::var("WSOL_LIFECYCLE_FIXTURE_DIR") {
        let p = std::path::Path::new(&root).join(role);
        std::fs::create_dir_all(&p)?;
        sql.execute(
            "VACUUM INTO ?1",
            [p.join("live_runtime.db").to_string_lossy().as_ref()],
        )?;
    }
    Ok(())
}
/// Prior receipt-domain fixtures model external chain confirmations. Their scoped
/// mock now supplies a complete isolated message, not an invented parsed receipt.
pub(super) fn external_mock(
    order: &copybot_storage_core::ExecutionCanaryOrder,
    wallet: &str,
    side: &str,
    quantity: u64,
    signature: &str,
    slot: u64,
) -> Result<(String, Value)> {
    let payer = parse_pubkey(wallet, "LOCALexternal")?;
    let r = ExecutionSubmitRequest {
        order_id: order.order_id.clone(),
        signal_id: order.signal_id.clone(),
        client_order_id: order.client_order_id.clone(),
        attempt: order.attempt,
        route: order.route.clone(),
        wallet_id: wallet.into(),
        token: buy::USDC.into(),
        side: side.into(),
        buy_size_sol: 0.01,
        slippage_tolerance_bps: 50,
        wallet_pubkey: wallet.into(),
        entry_route_plan_json: None,
        metadata: Default::default(),
    };
    let ix = if side == "buy" {
        buy::instructions(payer)?
    } else {
        super::owner_exit_test_fixture::instructions(payer)?
    };
    let mut ix = compact_buy(&ix);
    let route = ix
        .iter_mut()
        .find(|i| i.data.starts_with(&[229, 23, 203, 151, 122, 227, 173, 42]))
        .unwrap();
    if side == "buy" {
        route.data[24..32].copy_from_slice(&quantity.to_le_bytes());
    } else {
        route.data[16..24].copy_from_slice(&quantity.to_le_bytes());
    }
    if side == "buy" || order.signal_id.starts_with("owner-exit:") {
        let wire =
            crate::execution_native_floor::prepare_final_native_floor(payer, [9; 32], &ix, 1)?;
        let m = crate::execution_transaction_wire::decode_message(wire.payload(), |_| Ok(()))?;
        ix = m
            .instructions
            .into_iter()
            .map(|i| I {
                program_id: i.program.pubkey,
                accounts: i.accounts,
                data: i.data,
            })
            .collect();
    }
    let ix = crate::execution_wsol_lifecycle::normalize_instructions(&r, &ix, model::RENT)?;
    let payload = STANDARD.encode(
        crate::execution_solana_tx::serialize_unsigned_legacy_transaction(payer, [9; 32], &ix)?,
    );
    let mut bank = model::Bank::new(payer)?;
    bank.canonical(123_456);
    let mut receipt = bank.execute(&payload)?;
    receipt["result"]["transaction"]["signatures"] = json!([signature]);
    receipt["result"]["slot"] = json!(slot);
    Ok((payload, receipt))
}
pub(super) fn submitted(
    store: &copybot_storage_core::SqliteStore,
    path: &std::path::Path,
    o: &copybot_storage_core::ExecutionCanaryOrder,
    wallet: &str,
    side: &str,
    sig: &str,
    payload: &str,
) -> Result<()> {
    let now = chrono::Utc::now();
    let binding = crate::execution_transaction_wire::decode_message(payload, |_| Ok(()))?.binding;
    store.mark_execution_canary_built(&o.order_id, now)?;
    store.mark_execution_canary_simulated(
        &o.order_id,
        now,
        copybot_storage_core::EXECUTION_SIMULATION_STATUS_PASSED,
        None,
    )?;
    store.mark_execution_canary_submitted(&o.order_id, now, sig)?;
    rusqlite::Connection::open(path)?.execute("INSERT INTO execution_canary_dispatch(order_id,signal_id,client_order_id,route,attempt,wallet,token,side,tx_signature,message_sha256,transaction_sha256,claimed_at,transport_note)VALUES(?1,?2,?3,?4,1,?5,?6,?7,?8,?9,?10,?11,'LOCAL exact produced message mocked send; no signature')",rusqlite::params![o.order_id,o.signal_id,o.client_order_id,o.route,wallet,buy::USDC,side,sig,binding.message_sha256,binding.transaction_sha256,now.to_rfc3339()])?;
    Ok(())
}
pub(super) fn receipt_checks(
    store: &copybot_storage_core::SqliteStore,
    r: &ExecutionSubmitRequest,
    receipt: &Value,
) -> Result<()> {
    let proof = copybot_storage_core::ExecutionCanaryReceiptProof {
        tx_signature: receipt["result"]["transaction"]["signatures"][0]
            .as_str()
            .unwrap()
            .into(),
        wallet_pubkey: r.wallet_pubkey.clone(),
        token: r.token.clone(),
        side: r.side.clone(),
        confirmation_status: "confirmed".into(),
        slot: receipt["result"]["slot"].as_u64(),
        confirmed_at: chrono::Utc::now(),
        reason: "LOCAL evidence check".into(),
    };
    crate::execution_wsol_lifecycle::verify_receipt(store, &r.order_id, &proof, receipt)?;
    let mut missing = receipt.clone();
    missing["result"]["transaction"]["message"]
        .as_object_mut()
        .unwrap()
        .remove("recentBlockhash");
    assert!(
        crate::execution_wsol_lifecycle::verify_receipt(store, &r.order_id, &proof, &missing)
            .is_err()
    );
    let mut foreign = receipt.clone();
    let create = foreign["result"]["transaction"]["message"]["instructions"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|i| i["parsed"]["type"] == "createAccountWithSeed")
        .unwrap();
    create["parsed"]["info"]["seed"] = json!("f".repeat(32));
    assert!(
        crate::execution_wsol_lifecycle::verify_receipt(store, &r.order_id, &proof, &foreign)
            .unwrap_err()
            .to_string()
            .contains("message_mismatch")
    );
    // A known failed instruction retains the identical message binding. Existing
    // failed-expense accounting handles the fee; this verifier never creates a fill.
    let mut failed = receipt.clone();
    failed["result"]["meta"]["err"] = json!({"InstructionError":[2,{"Custom":0}]});
    crate::execution_wsol_lifecycle::verify_receipt(store, &r.order_id, &proof, &failed)?;
    assert!(!store.execution_canary_fill_exists(&r.order_id)?);
    Ok(())
}
