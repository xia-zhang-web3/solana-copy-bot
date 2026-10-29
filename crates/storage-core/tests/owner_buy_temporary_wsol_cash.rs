use copybot_core_types::{Lamports, SignedLamports};
use copybot_storage_core::{
    derive_receipt_cash_components, ExecutionCanaryReceiptFacts, NativeAccountObservations,
    ReceiptDecomposition, ReceiptFeeCoverage, ReceiptTokenCoverage, ReceiptTokenDelta,
    ReceiptWsolCoverage,
};
use serde_json::Value;
fn fixture() -> (ExecutionCanaryReceiptFacts, NativeAccountObservations) {
    let v: Value =
        serde_json::from_str(include_str!("fixtures/local_owner_buy_temporary.json")).unwrap();
    let f = &v["facts"];
    let text = |k: &str| f[k].as_str().unwrap().to_owned();
    let facts = ExecutionCanaryReceiptFacts {
        order_id: text("order_id"),
        tx_signature: text("tx_signature"),
        wallet_pubkey: text("wallet_pubkey"),
        token: text("token"),
        side: "buy".into(),
        slot: text("slot").parse().unwrap(),
        wallet_native_pre: Lamports::new(text("wallet_native_pre").parse().unwrap()),
        wallet_native_post: Lamports::new(text("wallet_native_post").parse().unwrap()),
        wallet_native_delta: SignedLamports::new(text("wallet_native_delta").parse().unwrap()),
        transaction_fee: Some(Lamports::new(text("transaction_fee").parse().unwrap())),
        fee_coverage: ReceiptFeeCoverage::Known,
        fee_payer: Some(text("wallet_pubkey")),
        token_delta: Some(ReceiptTokenDelta {
            raw: text("token_delta_raw").parse().unwrap(),
            decimals: 6,
        }),
        token_coverage: ReceiptTokenCoverage::PairedBalances,
        token_coverage_reason: None,
        wsol_coverage: ReceiptWsolCoverage::Observed,
        block_time: None,
        decomposition: ReceiptDecomposition::Unresolved,
    };
    (
        facts,
        serde_json::from_value(v["observations"].clone()).unwrap(),
    )
}
#[test]
fn actual_owner_buy_temporary_wsol_has_proven_input_and_zero_cash_residual() {
    let (f, o) = fixture();
    let c = derive_receipt_cash_components(&f, &o, Some(10_000_000)).unwrap();
    assert_eq!(c.classification, "decomposed");
    assert_eq!(c.swap_native_delta_lamports.as_deref(), Some("-10000000"));
    assert_eq!(c.unclassified_native_delta_lamports.as_deref(), Some("0"));
    // No owner amount is available for the separate native BUY basis.
    let missing = derive_receipt_cash_components(&f, &o, None).unwrap();
    assert_eq!(missing.classification, "partial_unknown");
    assert_eq!(missing.swap_native_delta_lamports, None);
}
#[test]
fn temporary_creator_initializer_funding_and_close_must_all_match() {
    for kind in [
        "createAccountWithSeed",
        "initializeAccount3",
        "closeAccount",
    ] {
        let (f, mut o) = fixture();
        o.instructions
            .retain(|i| i.instruction_type.value.as_deref() != Some(kind));
        assert_eq!(
            derive_receipt_cash_components(&f, &o, Some(10_000_000))
                .unwrap()
                .classification,
            "partial_unknown",
            "{kind}"
        );
    }
    let (f, o) = fixture();
    for (kind, field) in [
        ("createAccountWithSeed", "base"),
        ("createAccountWithSeed", "owner"),
        ("initializeAccount3", "owner"),
        ("closeAccount", "destination"),
    ] {
        let mut changed = o.clone();
        let i = changed
            .instructions
            .iter_mut()
            .find(|i| i.instruction_type.value.as_deref() == Some(kind))
            .unwrap();
        i.fields.get_mut(field).unwrap().value = Some("LOCAL-wrong-binding".into());
        assert_eq!(
            derive_receipt_cash_components(&f, &changed, Some(10_000_000))
                .unwrap()
                .classification,
            "partial_unknown",
            "{kind}/{field}"
        );
    }
    assert_eq!(
        derive_receipt_cash_components(&f, &o, Some(9_999_999))
            .unwrap()
            .classification,
        "partial_unknown"
    );
}

#[test]
fn temporary_wsol_rejects_ambiguous_or_unknown_operands() {
    let (f, o) = fixture();
    let cases = [
        ("createAccountWithSeed", "source", "wrong"),
        ("createAccountWithSeed", "newAccount", "wrong"),
        ("createAccountWithSeed", "seed", "wrong"),
        ("createAccountWithSeed", "space", "164"),
        ("createAccountWithSeed", "lamports", "0"),
        ("createAccountWithSeed", "lamports", "999999999999"),
        ("initializeAccount3", "mint", "wrong"),
        ("initializeAccount3", "account", "wrong"),
        ("closeAccount", "owner", "wrong"),
        ("closeAccount", "account", "wrong"),
    ];
    for (kind, field, value) in cases {
        let mut changed = o.clone();
        let ix = changed
            .instructions
            .iter_mut()
            .find(|i| i.instruction_type.value.as_deref() == Some(kind))
            .unwrap();
        ix.fields.get_mut(field).unwrap().value = Some(value.into());
        assert_eq!(
            derive_receipt_cash_components(&f, &changed, Some(10_000_000))
                .unwrap()
                .classification,
            "partial_unknown",
            "{kind}/{field}"
        );
    }
    for kind in [
        "createAccountWithSeed",
        "initializeAccount3",
        "closeAccount",
    ] {
        for mutation in ["duplicate", "program", "coverage", "field_coverage"] {
            let mut changed = o.clone();
            let at = changed
                .instructions
                .iter()
                .position(|i| i.instruction_type.value.as_deref() == Some(kind))
                .unwrap();
            match mutation {
                "duplicate" => {
                    let mut extra = changed.instructions[at].clone();
                    extra.outer_index = 9999;
                    extra.inner_index = None;
                    changed.instructions.push(extra);
                }
                "program" => changed.instructions[at].program_id.value = Some("wrong".into()),
                "coverage" => {
                    changed.instructions[at].coverage =
                        copybot_storage_core::ObservationCoverage::Missing
                }
                _ => {
                    let field = changed.instructions[at].fields.values_mut().next().unwrap();
                    field.coverage = copybot_storage_core::ObservationCoverage::Missing;
                    field.value = None;
                    field.source = copybot_storage_core::ObservationSource::Unavailable;
                }
            }
            assert_eq!(
                derive_receipt_cash_components(&f, &changed, Some(10_000_000))
                    .unwrap()
                    .classification,
                "partial_unknown",
                "{kind}/{mutation}"
            );
        }
    }
    // Funding and outgoing CPI must both be known and unique for this exact account.
    let account = o
        .instructions
        .iter()
        .find(|i| i.instruction_type.value.as_deref() == Some("initializeAccount3"))
        .unwrap()
        .fields["account"]
        .value
        .clone();
    for direction in ["funding", "outgoing"] {
        let at = o
            .instructions
            .iter()
            .position(|i| {
                i.instruction_type.value.as_deref() == Some("transfer")
                    && if direction == "funding" {
                        i.fields
                            .get("destination")
                            .is_some_and(|v| v.value == account)
                    } else {
                        i.fields.get("source").is_some_and(|v| v.value == account)
                    }
            })
            .unwrap();
        for mutation in ["duplicate", "program", "authority", "coverage"] {
            let mut changed = o.clone();
            let ix = &mut changed.instructions[at];
            match mutation {
                "duplicate" => {
                    let mut extra = changed.instructions[at].clone();
                    extra.outer_index = 9999;
                    extra.inner_index = None;
                    changed.instructions.push(extra);
                }
                "program" => ix.program_id.value = Some("wrong".into()),
                "authority" => {
                    ix.fields
                        .get_mut(if direction == "funding" {
                            "source"
                        } else {
                            "authority"
                        })
                        .unwrap()
                        .value = Some("wrong".into())
                }
                _ => ix.coverage = copybot_storage_core::ObservationCoverage::Missing,
            }
            assert_eq!(
                derive_receipt_cash_components(&f, &changed, Some(10_000_000))
                    .unwrap()
                    .classification,
                "partial_unknown",
                "{direction}/{mutation}"
            );
        }
    }
    let mut wrong_order = f.clone();
    wrong_order.order_id.push_str("-wrong");
    let mut wrong_observations = o.clone();
    wrong_observations.order_id = wrong_order.order_id.clone();
    assert_eq!(
        derive_receipt_cash_components(&wrong_order, &wrong_observations, Some(10_000_000))
            .unwrap()
            .classification,
        "partial_unknown"
    );
}
