//! Receipt-owned fractional source SELL fixture for native BUY runner cases.
use super::*;
pub(in crate::app_tests::fractional) async fn sell_quarter(case: &mut Case) -> Result<()> {
    let raw = case.buy_lamports / 1000;
    let sold = raw / 4;
    seed_source_sell(case).await?;
    let mut evidence = crate::app_tests::fractional::fractional_tests::evidence()?;
    let mut meta: Value = serde_json::from_slice(&std::fs::read(
        crate::app_tests::b135_fixture::inputs().join("chain.json"))?)?;
    meta["our"]["signer"] = json!(case.wallet);
    meta["our"]["signature"] = json!(case.signature);
    let db = crate::app_tests::association_fixture::Db {
        path: case.path.clone().into(), sql: Connection::open(&case.path)?,
        store: SqliteStore::open(&case.path)?,
    };
    let mut f = crate::app_tests::fractional::fractional_fixture::Fixture::from_parts(db, meta)?;
    let step = f.db.store.claim_strict_sell_quote_for_owned_preparation(
        crate::app_tests::association_parent_fixture::limits(),
        "http://127.0.0.1:1/", Utc::now)?;
    let QuoteClaimStep::Claimed(initial) = step else { anyhow::bail!("owned SELL not claimable") };
    assert_eq!(initial.binding.raw, raw);
    evidence.execution_accounts["value"][0]["account"]["data"]["parsed"]["info"]["owner"] =
        json!(case.wallet);
    evidence.execution_accounts["value"][0]["account"]["data"]["parsed"]["info"]["tokenAmount"]["amount"] = json!(raw.to_string());
    let claim = crate::app_tests::fractional::fractional_tests::bind(&mut f, initial, evidence).await?;
    assert_eq!(claim.binding.raw, sold);
    let before = f.db.store.load_execution_canary_open_position(MINT)?.unwrap();
    let prepared = if case.config.technical_cohort.is_some() {
        crate::app_tests::fractional::fractional_financial_fixture::prepare_with_config(
            &f, &claim, &case.config, case.config.tiny_experiment.id.as_deref().unwrap())?
    } else {
        crate::app_tests::fractional::fractional_financial_fixture::prepare(&f, &claim)?
    };
    if let Some(cohort) = &case.config.technical_cohort {
        let h = &prepared.handoff;
        let again = f.db.store.reserve_owned_sell_handoff(
            &h.snapshot, &h.quote, crate::app_tests::association_parent_fixture::limits(),
            &h.config_sha256, &h.authority, &h.experiment_id, &h.wallet, Utc::now,
        ).unwrap_err();
        assert!(again.to_string().contains("technical_cohort_sell_limit"), "{again:#}");
        let deadline = chrono::DateTime::parse_from_rfc3339(&cohort.deadline)?
            .with_timezone(&Utc);
        let denied = f.db.store.check_owned_sell_budget_policy(
            &h.experiment_id, &h.wallet, &h.snapshot.quote.mint,
            &h.snapshot.quote.position_id, true, deadline,
        ).unwrap_err();
        assert!(denied.to_string().contains("technical_cohort_sell_deadline"), "{denied:#}");
    }
    let dispatch = crate::app_tests::fractional::fractional_financial_fixture::dispatch(&f, &prepared)?;
    let sold_receipt = crate::app_tests::fractional::fractional_financial_fixture::receipt(&dispatch, sold);
    f.db.store.mark_execution_canary_confirmed_unreconciled(&dispatch.order_id,
        &ExecutionCanaryReceiptProof { tx_signature: dispatch.tx_signature.clone(),
            wallet_pubkey: dispatch.wallet.clone(), token: dispatch.token.clone(), side: "sell".into(),
            confirmation_status: "confirmed".into(), slot: Some(151), confirmed_at: Utc::now(),
            reason: "mocked canonical receipt".into() }, Utc::now())?;
    f.db.store.record_execution_canary_receipt_facts(&sold_receipt, Utc::now())?;
    f.db.store.apply_execution_canary_sell_settlement(&sold_receipt, Utc::now())?;
    let reopened = SqliteStore::open(&case.path)?;
    reopened.apply_execution_canary_sell_settlement(&sold_receipt, Utc::now())?;
    let after = reopened.load_execution_canary_open_position(MINT)?.unwrap();
    assert_eq!(after.qty_exact.unwrap().raw(), raw-sold);
    let basis = before.cost_lamports.unwrap().as_u64();
    let allocated = (basis * sold).div_ceil(raw);
    assert_eq!((basis, allocated), (case.buy_lamports + 19_000,
        (case.buy_lamports + 19_000) / 4));
    assert_eq!(after.cost_lamports.unwrap().as_u64(), basis-allocated);
    let cash = reopened.load_execution_canary_cash_settlement(&dispatch.order_id)?.unwrap();
    assert_eq!(cash.wallet_native_cash_delta.as_i128(), 981000);
    assert_eq!(cash.allocated_entry_basis.as_u64(), allocated);
    assert_eq!(cash.remaining_entry_basis.as_u64(), basis-allocated);
    let fills: i64 = f.db.sql.query_row("SELECT count(*) FROM fills WHERE order_id=?1",
        [&dispatch.order_id], |r| r.get(0))?;
    assert_eq!(fills, 1);
    let fee: Option<String> = f.db.sql.query_row(
        "SELECT transaction_fee FROM execution_canary_receipt_facts WHERE order_id=?1",
        [&dispatch.order_id], |r| r.get(0))?;
    assert_eq!(fee.as_deref(), Some("19000"));
    Ok(())
}

pub(in crate::app_tests::fractional) async fn seed_source_sell(case: &Case) -> Result<()> {
    let raw = case.buy_lamports / 1000;
    let native = crate::app_tests::fractional::fractional_fixture::native_binding().await?;
    let mut inbox = AssociationInbox::open_ordered_sell_consumer(&case.path,
        InboxLimits { count: 1000, bytes: 8 << 20, busy_ms: 100 })?;
    let mut own = admission();
    own.facts.signature = case.signature.clone();
    own.facts.slot = 120;
    own.facts.wallet = case.wallet.clone();
    own.facts.amount_in_bits = (case.buy_lamports as f64 / 1e9).to_bits();
    own.facts.amount_out_bits = (raw as f64 / 1000.0).to_bits();
    let exact = own.facts.exact_amounts.as_mut().unwrap();
    exact.amount_in_raw = case.buy_lamports.to_string();
    exact.amount_out_raw = raw.to_string();
    inbox.persist_at(&delivery(3, copybot_core_types::association_delivery::DeliveryEvent::Admission(own.clone())),
        &copybot_core_types::association_delivery::CandidateGeneration::Unknown, case.now)?;
    let blockhash = native["anchors"][1]["terminal"]["ProviderAsserted"]["blockhash"]
        .as_str().unwrap().to_owned();
    inbox.persist_at(&delivery(4, copybot_core_types::association_delivery::DeliveryEvent::Terminal {
        signature: case.signature.clone(), expected: own,
        result: copybot_core_types::association_delivery::Terminal::ProviderAsserted(
            copybot_core_types::association_delivery::ProviderAssertion {
                slot: 120, blockhash, signature: case.signature.clone(),
                transaction_index: 0, block_time: copybot_core_types::association_delivery::BlockTime::Missing,
            }),
    }), &copybot_core_types::association_delivery::CandidateGeneration::Unknown, case.now)?;
    let sell: copybot_core_types::association_delivery::AdmissionFacts =
        serde_json::from_value(native["anchors"][2]["identity"]["admission"].clone())?;
    let generation = case.store.association_candidate(&sell.facts);
    inbox.persist_at(&delivery(5, copybot_core_types::association_delivery::DeliveryEvent::Admission(sell.clone())),
        &generation, case.now)?;
    let sell_blockhash = native["anchors"][2]["terminal"]["ProviderAsserted"]["blockhash"]
        .as_str().unwrap().to_owned();
    inbox.persist_at(&delivery(6, copybot_core_types::association_delivery::DeliveryEvent::Terminal {
        signature: sell.facts.signature.clone(), expected: sell.clone(),
        result: copybot_core_types::association_delivery::Terminal::ProviderAsserted(
            copybot_core_types::association_delivery::ProviderAssertion {
                slot: 150, blockhash: sell_blockhash, signature: sell.facts.signature.clone(),
                transaction_index: 0, block_time: copybot_core_types::association_delivery::BlockTime::Missing,
            }),
    }), &generation, case.now)?;
    for (i, path) in native["parent_paths"].as_array().unwrap().iter().take(2).enumerate() {
        let edge = &path["edges"][0];
        let parent = copybot_core_types::association_parent::ParentObservation {
            child: serde_json::from_value(edge["child"].clone())?,
            parent: serde_json::from_value(edge["parent"].clone())?, issue: None,
        };
        inbox.persist_at(&delivery(7+i as u64,
            copybot_core_types::association_delivery::DeliveryEvent::Parent(parent)),
            &copybot_core_types::association_delivery::CandidateGeneration::Unknown, case.now)?;
    }
    for _ in 0..20 {
        if !inbox.has_sell_preparation_work()? { break; }
        inbox.recover_sell_preparation()?;
    }
    assert!(inbox.sell_preparation(&sell.facts.signature)?.is_some());
    drop(inbox);
    Ok(())
}
