//! Affected production boundary cases only. Providers/signers/transport are MOCKS.
use super::{protected_usdc_fixture as f, protected_usdc_prepare as money};
use crate::execution_owned_sell_rpc::fractional::transport::Parsed;
use anyhow::Result;
use chrono::Utc;
use copybot_storage_core::{ExecutionDispatchClaim, SqliteStore};
use serde_json::{json, Value};
#[tokio::test]
async fn protected_usdc_100_receipt_buy_native_sell_closes_exact_lot() -> Result<()> {
    let mut f = f::Case::new().await?;
    let original = f.claim()?;
    let claim = f.bind(original).await?;
    let fraction = claim.binding.fractional.as_ref().unwrap();
    assert_eq!(
        fraction.inventory.numerator.to_string(),
        fraction.inventory.denominator
    );
    assert_eq!(fraction.owned_raw, f::H);
    assert_eq!(claim.binding.raw, f::H);
    assert_eq!(
        fraction
            .protected_execution_baseline
            .as_ref()
            .unwrap()
            .amount_raw,
        100
    );
    let (p, r) = money::prepare(&f, &claim)?;
    for _ in 0..2 {
        boundary(&f, &p, &r, f::H + 100, &p.payload).await?;
    }
    let (d, b) = money::dispatch(&p);
    assert!(matches!(
        f.db.store
            .claim_owned_sell_dispatch(&p, &d, &b, || Ok(Utc::now()))?,
        ExecutionDispatchClaim::New
    ));
    // A mocked single external send has Unknown outcome until canonical receipt returns.
    let reopened = SqliteStore::open(&f.db.path)?;
    assert!(matches!(
        reopened.claim_owned_sell_dispatch(&p, &d, &b, || Ok(Utc::now()))?,
        ExecutionDispatchClaim::Existing
    ));
    assert!(reopened
        .recheck_owned_sell_prepared(&p, Utc::now())
        .is_err());
    let receipt = f::receipt(
        &r.wallet_pubkey,
        &d.tx_signature,
        "sell",
        f::H + 100,
        100,
        151,
    );
    let result = f::confirm(&reopened, &d.order_id, &r.wallet_pubkey, receipt.clone()).await?;
    assert_eq!(result.sell_closed, 1);
    assert!(reopened
        .load_execution_canary_open_position(f::USDC)?
        .is_none());
    let facts = reopened
        .load_execution_canary_receipt_facts(&d.order_id)?
        .unwrap();
    assert_eq!(facts.token_delta.unwrap().raw, -i128::from(f::H));
    assert_eq!(
        receipt["result"]["meta"]["postTokenBalances"][0]["uiTokenAmount"]["amount"],
        "100"
    );
    let cash = reopened
        .load_execution_canary_cash_settlement(&d.order_id)?
        .unwrap();
    assert_eq!(cash.sold_quantity.raw(), f::H);
    assert_eq!(cash.remaining_quantity.raw(), 0);
    assert_eq!(cash.remaining_entry_basis.as_u64(), 0);
    let unresolved: i64 = f.db.sql.query_row(
        "SELECT count(*) FROM execution_tiny_reservations WHERE outcome IS NULL",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(unresolved, 0);
    let closed:(String,String,u64)=f.db.sql.query_row("SELECT state,qty_raw,cost_lamports FROM positions WHERE accounting_bucket='execution_canary'",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
    assert_eq!(closed, ("closed".into(), "0".into(), 0));
    assert_eq!(
        f::confirm(&reopened, &d.order_id, &r.wallet_pubkey, receipt)
            .await?
            .reason
            .as_deref(),
        Some("receipt_already_accounted")
    );
    Ok(())
}
async fn boundary(
    f: &f::Case,
    p: &copybot_storage_core::rpc_owned_sell_handoff::dispatch::Prepared,
    r: &crate::execution_submit_adapter::ExecutionSubmitRequest,
    held: u64,
    payload: &str,
) -> Result<()> {
    let result = f::inventory(&f.config, held);
    let io = Parsed(move |request: Value| {
        assert_eq!(request["method"], "getTokenAccountsByOwner");
        assert_eq!(
            request["params"][2]["minContextSlot"],
            p.handoff.snapshot.sell.facts.slot
        );
        std::future::ready(Ok(
            json!({"jsonrpc":"2.0","id":request["id"],"result":result}),
        ))
    });
    crate::execution_protected_token_baseline::before_financial_boundary(
        &f.config,
        &f.db.store,
        r,
        payload,
        io,
    )
    .await
}
#[tokio::test]
async fn protected_usdc_oversell_wire_and_h_plus_99_refuse_before_keys_send() -> Result<()> {
    let mut f = f::Case::new().await?;
    let c = f.claim()?;
    let c = f.bind(c).await?;
    let (p, r) = money::prepare(&f, &c)?;
    for (held, payload, expected) in [
        (
            f::H + 100,
            money::payload(&f.config, f::H + 1)?,
            "owner_exit_wire_amount_or_fee",
        ),
        (
            f::H + 99,
            p.payload.clone(),
            "protected_usdc_owned_lot_or_baseline_short",
        ),
    ] {
        // The real function is called at both production hooks before key loading/claim.
        let mut key_loads = 0;
        let mut sends = 0;
        let result = boundary(&f, &p, &r, held, &payload).await.map(|_| {
            key_loads += 1;
            sends += 1;
        });
        let error = result.expect_err("must refuse before mocked key/send");
        assert!(error.to_string().contains(expected), "{error:#}");
        assert_eq!((key_loads, sends), (0, 0));
    }
    let dispatches: i64 =
        f.db.sql
            .query_row("SELECT count(*) FROM rpc_owned_sell_dispatches", [], |r| {
                r.get(0)
            })?;
    assert_eq!(dispatches, 0);
    Ok(())
}
#[tokio::test]
async fn protected_usdc_inventory_identity_and_collection_unknown_are_fail_closed() -> Result<()> {
    let mut f = f::Case::new().await?;
    let c = f.claim()?;
    f.evidence.execution_accounts = f::inventory(&f.config, f::H + 99);
    assert!(f.bind(c.clone()).await.is_err());
    assert!(f
        .db
        .store
        .begin_fractional_sell(
            &c,
            super::association_parent_fixture::limits(),
            Utc::now(),
            &"a".repeat(64)
        )
        .is_err());
    let reopened = SqliteStore::open(&f.db.path)?;
    assert!(!reopened.recheck_strict_sell_quote(
        &c,
        super::association_parent_fixture::limits(),
        Utc::now()
    )?);
    let mut f = f::Case::new().await?;
    let c = f.claim()?;
    let c = f.bind(c).await?;
    let (p, _) = money::prepare(&f, &c)?;
    for fault in ["ata", "wallet", "slot", "decimals", "policy"] {
        let mut value = f::inventory(&f.config, f::H + 100);
        let mut config = f.config.clone();
        match fault {
            "ata" => value["value"][0]["pubkey"] = json!("wrong"),
            "wallet" => {
                value["value"][0]["account"]["data"]["parsed"]["info"]["owner"] = json!("wrong")
            }
            "slot" => value["context"]["slot"] = json!(149),
            "decimals" => {
                value["value"][0]["account"]["data"]["parsed"]["info"]["tokenAmount"]["decimals"] =
                    json!(3)
            }
            _ => {
                config
                    .owned_sell_preparation
                    .as_mut()
                    .unwrap()
                    .protected_bot_usdc_baseline
                    .as_mut()
                    .unwrap()
                    .amount_raw = 101
            }
        };
        assert!(
            crate::execution_protected_token_baseline::balance(&config, &p, &value).is_err(),
            "{fault}"
        );
    }
    Ok(())
}
#[test]
fn protected_usdc_six_actual_configs_and_none_identity_reuse() -> Result<()> {
    let Ok(root) = std::env::var("COPYBOT_PROTECTED_USDC_CONFIG_ROOT") else {
        return Ok(());
    };
    let mut outputs = Vec::new();
    for scope in ["source", "bot"] {
        for profile in ["TEMPLATE", "INACTIVE", "RUNTIME"] {
            let path = std::path::Path::new(&root).join(format!("{scope}.{profile}.toml"));
            let c = copybot_config::load_from_path(&path)?;
            crate::config_contract::validate_execution_runtime_contract(
                &c.execution,
                &c.system.env,
            )?;
            crate::risk_contract::validate_execution_risk_contract(&c.risk)?;
            crate::runtime_bootstrap::validate_live_execution_policy_contract(
                &c.execution,
                &c.risk,
                &c.system.env,
            )?;
            assert!(!c.execution.enabled);
            let mut none_identity = None;
            let hash = if scope == "bot" {
                assert!(
                    crate::execution_protected_token_baseline::configured(&c.execution)?.is_some()
                );
                assert!(!c.execution.technical_cohort.as_ref().unwrap().activate);
                let mut old = c.execution.clone();
                old.owned_sell_preparation
                    .as_mut()
                    .unwrap()
                    .protected_bot_usdc_baseline = None;
                none_identity = Some(crate::execution_native_buy_rpc::policy_identity(&old)?);
                Some(crate::execution_native_buy_rpc::policy_identity(
                    &c.execution,
                )?)
            } else {
                assert!(
                    crate::execution_protected_token_baseline::configured(&c.execution)?.is_none()
                );
                assert!(!c.execution.owner_technical_buy.as_ref().unwrap().activate);
                None
            };
            outputs.push(json!({"scope":scope,"profile":profile,"sha256":crate::execution_owned_sell_rpc::digest(std::fs::read(&path)?),"native_policy_identity":hash,"without_baseline_native_policy_identity":none_identity}));
        }
    }
    let c = money::config(&super::owner_exit_test_fixture::wallet())?;
    let old = crate::execution_owned_sell_rpc::identity(&c)?;
    let data = serde_json::to_value(&outputs)?;
    if let Ok(path) = std::env::var("COPYBOT_PROTECTED_USDC_IDENTITY_OUTPUT") {
        std::fs::write(
            path,
            serde_json::to_vec_pretty(
                &json!({"proof":"actual_six_config_load_new_baseline_no_daemon_or_network","profiles":data,"none_fixture_identity":old}),
            )?,
        )?;
    }
    Ok(())
}

#[tokio::test]
async fn protected_usdc_none_and_owner_wire_requirements_unchanged() -> Result<()> {
    let mut f = f::Case::new().await?;
    let mut c = f.config.clone();
    c.owned_sell_preparation
        .as_mut()
        .unwrap()
        .protected_bot_usdc_baseline = None;
    assert!(crate::execution_protected_token_baseline::configured(&c)?.is_none());
    f.config = c;
    f.evidence.execution_accounts["value"][0]["account"]["data"]["parsed"]["info"]["tokenAmount"]
        ["amount"] = json!(f::H.to_string());
    let claim = f.claim()?;
    let claim = f.bind(claim).await?;
    assert!(claim
        .binding
        .fractional
        .as_ref()
        .unwrap()
        .protected_execution_baseline
        .is_none());
    assert_eq!(claim.binding.raw, f::H);
    let (p, mut r) = money::prepare(&f, &claim)?;
    // None is the unchanged legacy branch; new policy never changes its parser.
    crate::execution_protected_token_baseline::wire(&f.config, &r, "unused-because-none", f::H)?;
    r.signal_id = "owner-exit:legacy-test".into();
    r.wallet_id = r.wallet_pubkey.clone();
    let mut quote = super::owner_exit_test_fixture::quote();
    r.metadata.quote_in_amount_raw = Some("1167085".into());
    r.metadata.quote_response_json = Some(quote.to_string());
    let raw = money::payload(&f.config, 1167085)?;
    assert!(
        crate::execution_owner_exit_wire::verify(&r, &raw).is_err(),
        "owner floor still required"
    );
    let message = crate::execution_transaction_wire::decode_message(&raw, |_| Ok(()))?;
    let mut instructions = super::owner_exit_test_fixture::instructions(
        crate::execution_pumpswap_accounts::parse_pubkey(&r.wallet_pubkey, "fixture-wallet")?,
    )?;
    let floor = crate::execution_native_floor::prepare_final_native_floor(
        crate::execution_pumpswap_accounts::parse_pubkey(&r.wallet_pubkey, "fixture-wallet")?,
        message.recent_blockhash,
        &instructions,
        160200031,
    )?;
    crate::execution_owner_exit_wire::verify(&r, floor.payload())?;
    quote["inAmount"] = json!((1167086u64).to_string());
    r.metadata.quote_response_json = Some(quote.to_string());
    assert!(crate::execution_owner_exit_wire::verify(&r, floor.payload()).is_err());
    assert_eq!(p.handoff.snapshot.quote.raw, f::H);
    instructions.clear();
    Ok(())
}
