//! No keys or signatures. Real private ContextProof and actual account/fee guards.
use super::{owner_buy_wire_fixture as wire, receipt_lot_custody_fixture as f};
use crate::execution_submit_adapter::*;
use anyhow::Result;
use chrono::Utc;
use copybot_config::*;
use copybot_storage_core::*;
#[tokio::test]
async fn wsol_lifecycle_source_buy_and_sell_actual_producer_custody() -> Result<()> {
    let c = f::Case::new().await?;
    let mut cfg = super::owner_buy_fixture::config(
        f::SOURCE,
        "http://127.0.0.1:1",
        &c.root.path().join("neverSTOP").to_string_lossy(),
    );
    cfg.controlled_receipt_lot = Some(serde_json::from_value({
        let mut v = serde_json::to_value(&c.scope)?;
        v["wsol_lifecycle"] = serde_json::json!(copybot_config::TRANSACTION_OWNED_WSOL_V1);
        v
    })?);
    cfg.owned_sell_preparation = None;
    cfg.pretrade_min_sol_reserve = 0.0;
    cfg.tiny_experiment.id = Some(c.scope.source_run_id.clone());
    cfg.tiny_experiment.policy_mode = TinyPolicyMode::ProtectedNativeCapital;
    cfg.quote_canary_buy_slippage_bps = 50;
    cfg.quote_canary_sell_slippage_bps = 50;
    cfg.max_submit_attempts = 1;
    let bot_copy = c.root.path().join("bot-native-gate.db");
    c.bot
        .db
        .sql
        .execute("VACUUM INTO ?1", [bot_copy.to_string_lossy().as_ref()])?;
    let bot_identity = crate::execution_native_buy_rpc::policy_identity(&c.bot.config)?;
    let p = cfg.owner_technical_buy.as_mut().unwrap();
    p.policy = OWNER_EXECUTION_ROUNDTRIP_V1.into();
    p.intent_id = c.scope.source_buy_intent_id.clone();
    p.run_id = c.scope.source_run_id.clone();
    p.mint = wire::USDC.into();
    p.min_reserve_lamports = 0;
    p.max_slippage_bps = 50;
    p.activated_at = c.buy.activated_at.to_rfc3339();
    p.expires_at = (chrono::DateTime::parse_from_rfc3339(
        &c.bot.config.technical_cohort.as_ref().unwrap().deadline,
    )? + chrono::Duration::seconds(60))
    .to_rfc3339();
    p.source_copy_exit_gate = Some(OwnerSourceCopyGateConfig {
        policy: CONFIRMED_NATIVE_BOT_BUY_V1.into(),
        bot_state_db_path: bot_copy.to_string_lossy().into(),
        bot_wallet_pubkey: c.scope.bot_wallet.clone(),
        bot_run_id: c.scope.bot_run_id.clone(),
        bot_policy_identity: bot_identity,
        bot_kill_switch_path: c.root.path().join("BOTSTOP").to_string_lossy().into(),
    });
    let mut db = SqliteStore::open(c.root.path().join("native-policy.db"))?;
    db.run_migrations(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    db.register_controlled_receipt_lot_scope(&c.scope, f::SOURCE)?;
    let intent = crate::execution_owner_buy_authority::configured(&cfg)?.unwrap();
    db.register_owner_technical_buy_intent(&intent)?;
    let now = Utc::now();
    let order = db
        .reserve_owner_technical_buy_order(&intent.intent_id, || Ok(now))?
        .order;
    let policy = db.prepare_tiny_native_policy_for_owner_buy(
        &intent.run_id,
        f::SOURCE,
        1_000_000_000,
        0,
        120,
        now,
        &intent,
        &order,
        || Ok(now),
    )?;
    assert_eq!(policy.floor_lamports, 0);
    let mut q = wire::quote();
    q["outAmount"] = serde_json::json!("10000");
    q["otherAmountThreshold"] = serde_json::json!("9950");
    q["routePlan"][0]["swapInfo"]["outAmount"] = serde_json::json!("10000");
    let mut m = super::priority_fee_fixture::metadata();
    m.quote_event_id = Some("LOCAL controlled BUY quote".into());
    m.quote_in_amount_raw = Some("10000000".into());
    m.quote_out_amount_raw = Some("10000".into());
    m.quote_response_json = Some(q.to_string());
    m.route_plan_json = Some(q["routePlan"].to_string());
    let mut request = ExecutionSubmitRequest {
        order_id: order.order_id,
        signal_id: order.signal_id,
        client_order_id: order.client_order_id,
        attempt: 1,
        route: intent.route,
        wallet_id: f::SOURCE.into(),
        token: wire::USDC.into(),
        side: "buy".into(),
        buy_size_sol: 0.01,
        slippage_tolerance_bps: 50,
        wallet_pubkey: f::SOURCE.into(),
        entry_route_plan_json: None,
        metadata: m,
    };
    crate::execution_native_floor_policy::protected::prepare_request(&db, &cfg, &mut request, now)
        .await?;
    let proof = request.metadata.protected_capital.as_deref().unwrap();
    let wallet = crate::execution_pumpswap_accounts::parse_pubkey(f::SOURCE, "LOCALwallet")?;
    let mut ix = wire::instructions(wallet)?;
    ix.iter_mut()
        .find(|i| {
            i.program_id
                == crate::execution_pumpswap_accounts::parse_pubkey(
                    crate::execution_owner_buy_wire::JUPITER,
                    "localJupiter",
                )
                .unwrap()
        })
        .unwrap()
        .data[24..32]
        .copy_from_slice(&10_000u64.to_le_bytes());
    let payload = super::wsol_lifecycle_fixture::build(&cfg, &request, &ix)?;
    crate::execution_owner_buy_wire::verify(&request, &payload)?;
    let mut generic = request.clone();
    generic.metadata.protected_capital = None;
    assert!(crate::execution_owner_buy_wire::verify(&generic, &payload).is_err());
    // Owner removed 98% of the old balance. Only actual pending test debit matters.
    let facts = crate::execution_native_rpc::synthetic_classic_funding(
        &payload, wallet, 20_000_000, 19_000, 120,
    )?;
    let budget =
        crate::execution_initial_sol::check_with_policy(&payload, wallet, 0, &facts, Some(proof))?;
    assert!(budget.required < 15_000_000);
    let short = crate::execution_native_rpc::synthetic_classic_funding(
        &payload,
        wallet,
        u64::try_from(budget.required)? - 1,
        19_000,
        120,
    )?;
    assert!(crate::execution_initial_sol::check_with_policy(
        &payload,
        wallet,
        0,
        &short,
        Some(proof)
    )
    .unwrap_err()
    .to_string()
    .contains("initial_sol_insufficient"));
    let expensive = crate::execution_native_rpc::synthetic_classic_funding(
        &payload,
        wallet,
        100_000_000,
        6_000_000,
        120,
    )?;
    assert!(crate::execution_initial_sol::check_with_policy(
        &payload,
        wallet,
        0,
        &expensive,
        Some(proof)
    )
    .unwrap_err()
    .to_string()
    .contains("receipt_lot_native_spend_cap"));
    let mut unrelated = facts.clone();
    let wsol = crate::execution_wsol_lifecycle::address(&request)?;
    let row = unrelated
        .native
        .accounts
        .value
        .iter_mut()
        .find(|x| x.pubkey == wsol)
        .unwrap();
    row.account = crate::execution_native_rpc::types::AccountObservation::Present {
        lamports: 2_039_280,
        owner_program: crate::execution_pumpswap_accounts::token_program_id(),
        executable: false,
        data: super::native_ata_fixture::token_bytes(
            crate::execution_pumpswap_accounts::wsol_mint(),
            wallet,
            0,
            Some(2_039_280),
        ),
    };
    assert!(crate::execution_initial_sol::check_with_policy(
        &payload,
        wallet,
        0,
        &unrelated,
        Some(proof)
    )
    .is_err());
    assert_eq!(
        db.tiny_native_policy(&c.scope.source_run_id, f::SOURCE, Utc::now())?,
        policy
    );
    assert!(crate::execution_initial_sol::check(&payload, wallet, 0, &facts).is_err()); // generic remains strict

    // Same canonical SOURCE BUY then private SOURCE EXIT zero-floor byte scope.
    let native_path = c.root.path().join("native-policy.db");
    let order = db.load_execution_canary_order(&request.order_id)?.unwrap();
    super::wsol_lifecycle_fixture::submitted(
        &db,
        &native_path,
        &order,
        f::SOURCE,
        "buy",
        f::BUY_SIG,
        &payload,
    )?;
    let confirmation =
        super::wsol_lifecycle_fixture::exercise("source-boundary-buy", &cfg, &request, &payload)?;
    let result =
        super::protected_usdc_fixture::confirm(&db, &order.order_id, f::SOURCE, confirmation)
            .await?;
    assert_eq!(
        result.buy_opened, 1,
        "actual modeled receipt refused: {result:?}"
    );
    let exit_cfg = crate::execution_owner_roundtrip::exit_config(&cfg, &db)?
        .expect("confirmed BOT gate permits SOURCE exit");
    let exit = crate::execution_owner_exit_authority::configured(&exit_cfg)?.unwrap();
    db.register_owner_exit_intent(&exit)?;
    let exit_order = db
        .reserve_owner_exit_order(&exit.intent_id, || Ok(Utc::now()))?
        .order;
    let mut quote = super::owner_exit_test_fixture::quote();
    quote["inAmount"] = serde_json::json!("10000");
    quote["routePlan"][0]["swapInfo"]["inAmount"] = serde_json::json!("10000");
    let mut metadata = super::priority_fee_fixture::metadata();
    metadata.quote_event_id = Some("LOCAL SOURCE exit quote".into());
    metadata.quote_in_amount_raw = Some("10000".into());
    metadata.quote_out_amount_raw = Some(super::owner_exit_test_fixture::OUT.to_string());
    metadata.quote_response_json = Some(quote.to_string());
    metadata.route_plan_json = Some(quote["routePlan"].to_string());
    let mut sell = ExecutionSubmitRequest {
        order_id: exit_order.order_id,
        signal_id: exit_order.signal_id,
        client_order_id: exit_order.client_order_id,
        attempt: 1,
        route: exit.route.clone(),
        wallet_id: f::SOURCE.into(),
        token: wire::USDC.into(),
        side: "sell".into(),
        buy_size_sol: 0.01,
        slippage_tolerance_bps: 50,
        wallet_pubkey: f::SOURCE.into(),
        entry_route_plan_json: None,
        metadata,
    };
    crate::execution_controlled_receipt_lot::stamp_exit(
        &exit_cfg,
        &db,
        &exit,
        &mut sell,
        Utc::now(),
    )?;
    let mut instructions = super::owner_exit_test_fixture::instructions(wallet)?;
    instructions
        .iter_mut()
        .find(|i| i.data.starts_with(&[229, 23, 203, 151, 122, 227, 173, 42]))
        .unwrap()
        .data[16..24]
        .copy_from_slice(&10_000u64.to_le_bytes());
    // The accepted SOURCE native SELL adapter requires its RaydiumV2 sentinel.
    let route = instructions
        .iter_mut()
        .find(|i| i.data.starts_with(&[229, 23, 203, 151, 122, 227, 173, 42]))
        .unwrap();
    route.accounts[4] = crate::execution_solana_tx::SolanaAccountMeta::readonly(
        crate::execution_pumpswap_accounts::parse_pubkey(
            crate::execution_owner_buy_wire::JUPITER,
            "LOCALjup",
        )?,
    );
    sell.metadata.priority_fee_lamports = Some(0);
    sell.metadata.priority_fee_json = Some(super::priority_fee_fixture::total_json(0));
    let sell_payload = super::wsol_lifecycle_fixture::build(&exit_cfg, &sell, &instructions)?;
    let confirmation = super::wsol_lifecycle_fixture::exercise(
        "source-boundary-sell",
        &exit_cfg,
        &sell,
        &sell_payload,
    )?;
    let order = db.load_execution_canary_order(&sell.order_id)?.unwrap();
    super::wsol_lifecycle_fixture::snapshot(
        "source-boundary-sell",
        &rusqlite::Connection::open(&native_path)?,
    )?;
    super::wsol_lifecycle_fixture::submitted(
        &db,
        &native_path,
        &order,
        f::SOURCE,
        "sell",
        f::SELL_SIG,
        &sell_payload,
    )?;
    assert_eq!(
        super::protected_usdc_fixture::confirm(&db, &sell.order_id, f::SOURCE, confirmation)
            .await?
            .sell_closed,
        1
    );
    if let Ok(root) = std::env::var("WSOL_LIFECYCLE_FIXTURE_DIR") {
        rusqlite::Connection::open(&native_path)?.execute(
            "VACUUM INTO ?1",
            [std::path::Path::new(&root)
                .join("source-boundary-confirmed.db")
                .to_string_lossy()
                .as_ref()],
        )?;
    }
    crate::execution_owner_exit_wire::verify(&sell, &sell_payload)?;
    sell.metadata.controlled_exit_wire = None;
    assert!(crate::execution_owner_exit_wire::verify(&sell, &sell_payload).is_err());
    Ok(())
}
