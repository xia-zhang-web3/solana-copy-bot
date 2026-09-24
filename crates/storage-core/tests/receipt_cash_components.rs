use copybot_core_types::{Lamports, SignedLamports};
use copybot_storage_core::{
    calculate_receipt_trade_cycle, confirmed_priority_fee, derive_receipt_cash_components,
    ExecutionCanaryReceiptFacts, NativeAccountObservation, NativeAccountObservations,
    NativeInstructionObservation, NativeObservation as Obs, NativeTokenEndpoint,
    ObservationCoverage as Cov, ObservationSource as Src, ReceiptDecomposition, ReceiptFeeCoverage,
    ReceiptTokenCoverage, ReceiptTokenDelta, ReceiptWsolCoverage,
};
use rusqlite::{params, Connection};
use serde_json::json;
use std::collections::BTreeMap;

const SYSTEM: &str = "11111111111111111111111111111111";
const TOKEN: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const ATA: &str = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";
const WSOL: &str = "So11111111111111111111111111111111111111112";

fn known(value: impl ToString) -> Obs {
    Obs::known(value, Src::ParsedInstruction)
}
fn endpoint(raw: &str) -> NativeTokenEndpoint {
    NativeTokenEndpoint {
        mint: known("mint"),
        token_owner: known("wallet"),
        token_program: known(TOKEN),
        decimals: known("6"),
        raw: known(raw),
    }
}
fn ix(
    index: u32,
    program: &str,
    kind: &str,
    fields: &[(&str, &str)],
) -> NativeInstructionObservation {
    NativeInstructionObservation {
        outer_index: index,
        inner_index: None,
        stack_height: Obs::unknown(Cov::Missing),
        program_id: known(program),
        instruction_type: known(kind),
        fields: fields
            .iter()
            .map(|(k, v)| ((*k).into(), known(*v)))
            .collect::<BTreeMap<_, _>>(),
        coverage: Cov::Known,
    }
}
fn facts() -> ExecutionCanaryReceiptFacts {
    ExecutionCanaryReceiptFacts {
        order_id: "order".into(),
        tx_signature: "signature".into(),
        wallet_pubkey: "wallet".into(),
        token: "mint".into(),
        side: "buy".into(),
        slot: 1,
        wallet_native_pre: Lamports::new(20_000_000),
        wallet_native_post: Lamports::new(8_506_560),
        wallet_native_delta: SignedLamports::new(-11_493_440),
        transaction_fee: Some(Lamports::new(5_000)),
        fee_coverage: ReceiptFeeCoverage::Known,
        fee_payer: Some("wallet".into()),
        token_delta: Some(ReceiptTokenDelta {
            raw: 1_167_085,
            decimals: 6,
        }),
        token_coverage: ReceiptTokenCoverage::ProvenLifecycle,
        token_coverage_reason: None,
        wsol_coverage: ReceiptWsolCoverage::Observed,
        block_time: None,
        decomposition: ReceiptDecomposition::Unresolved,
    }
}
fn observations() -> NativeAccountObservations {
    NativeAccountObservations {
        order_id: "order".into(),
        tx_signature: "signature".into(),
        wallet_pubkey: "wallet".into(),
        token: "mint".into(),
        side: "buy".into(),
        slot: "1".into(),
        accounts: vec![NativeAccountObservation {
            account_index: 1,
            pubkey: "target-ata".into(),
            native_pre: known("0"),
            native_post: known("1488440"),
            native_delta: known("1488440"),
            pre_token: endpoint("0"),
            post_token: endpoint("1167085"),
            relevance: vec!["target_mint".into()],
        }],
        instructions: vec![
            ix(
                0,
                ATA,
                "createIdempotent",
                &[
                    ("account", "target-ata"),
                    ("wallet", "wallet"),
                    ("mint", "mint"),
                ],
            ),
            ix(
                1,
                SYSTEM,
                "createAccount",
                &[
                    ("source", "wallet"),
                    ("newAccount", "target-ata"),
                    ("lamports", "1488440"),
                ],
            ),
            ix(
                2,
                ATA,
                "createIdempotent",
                &[
                    ("account", "wsol-ata"),
                    ("wallet", "wallet"),
                    ("mint", WSOL),
                ],
            ),
            ix(
                3,
                SYSTEM,
                "transfer",
                &[
                    ("source", "wallet"),
                    ("destination", "wsol-ata"),
                    ("lamports", "10000000"),
                ],
            ),
            ix(
                4,
                TOKEN,
                "transfer",
                &[
                    ("source", "wsol-ata"),
                    ("authority", "wallet"),
                    ("destination", "pool"),
                    ("amount", "10000000"),
                ],
            ),
            ix(
                5,
                TOKEN,
                "closeAccount",
                &[("account", "wsol-ata"), ("destination", "wallet")],
            ),
        ],
        accounts_coverage: Cov::Missing,
        instructions_coverage: Cov::Unsupported,
        reasons: vec!["parsed_instruction_partial_or_unsupported".into()],
    }
}

#[test]
fn confirmed_owner_buy_separates_swap_fee_and_locked_rent() {
    let c = derive_receipt_cash_components(&facts(), &observations(), Some(10_000_000)).unwrap();
    assert_eq!(c.swap_native_delta_lamports.as_deref(), Some("-10000000"));
    assert_eq!(c.transaction_fee_lamports.as_deref(), Some("5000"));
    assert_eq!(c.target_ata_rent_delta_lamports.as_deref(), Some("1488440"));
    assert_eq!(c.priority_fee_lamports, None);
    assert_eq!(c.unclassified_native_delta_lamports.as_deref(), Some("0"));
    assert_eq!(c.classification, "decomposed");
}

#[test]
fn missing_execution_link_or_rent_does_not_become_zero() {
    let mut o = observations();
    o.instructions
        .retain(|i| i.instruction_type.value.as_deref() != Some("createAccount"));
    let c = derive_receipt_cash_components(&facts(), &o, Some(10_000_000)).unwrap();
    assert_eq!(c.target_ata_rent_delta_lamports, None);
    assert_eq!(c.unclassified_native_delta_lamports, None);
    assert_eq!(c.classification, "partial_unknown");
    let c = derive_receipt_cash_components(&facts(), &observations(), None).unwrap();
    assert_eq!(c.swap_native_delta_lamports, None);
    assert_eq!(c.unclassified_native_delta_lamports, None);
}

#[test]
fn closed_cycle_separates_profit_from_locked_rent_and_partial_remains_unknown() {
    let buy = derive_receipt_cash_components(&facts(), &observations(), Some(10_000_000)).unwrap();
    let mut sell = buy.clone();
    sell.order_id = "sell-order".into();
    sell.side = "sell".into();
    sell.wallet_native_delta_lamports = "11995000".into();
    sell.swap_native_delta_lamports = Some("12000000".into());
    sell.transaction_fee_lamports = Some("5000".into());
    sell.target_ata_rent_delta_lamports = Some("0".into());
    sell.unclassified_native_delta_lamports = Some("0".into());
    let full = calculate_receipt_trade_cycle(
        "sell-order",
        Some("order"),
        "position",
        1_167_085,
        0,
        Some(501_560),
        true,
        Some(&buy),
        Some(&sell),
    )
    .unwrap();
    assert_eq!(full.economic_result_lamports.as_deref(), Some("1990000"));
    assert_eq!(
        full.target_ata_rent_locked_lamports.as_deref(),
        Some("1488440")
    );
    assert_eq!(full.wallet_cash_result_lamports.as_deref(), Some("501560"));
    let partial = calculate_receipt_trade_cycle(
        "sell-order",
        Some("order"),
        "position",
        500_000,
        667_085,
        Some(501_560),
        true,
        Some(&buy),
        Some(&sell),
    )
    .unwrap();
    assert_eq!(partial.state, "partial");
    assert_eq!(partial.economic_result_lamports, None);
    sell.unclassified_native_delta_lamports = None;
    let unresolved = calculate_receipt_trade_cycle(
        "sell-order",
        Some("order"),
        "position",
        1_167_085,
        0,
        Some(501_560),
        true,
        Some(&buy),
        Some(&sell),
    )
    .unwrap();
    assert_eq!(unresolved.economic_result_lamports, None);
    sell.unclassified_native_delta_lamports = Some("0".into());
    let after_prior_partial = calculate_receipt_trade_cycle(
        "sell-order",
        Some("order"),
        "position",
        667_085,
        0,
        Some(501_560),
        false,
        Some(&buy),
        Some(&sell),
    )
    .unwrap();
    assert_eq!(after_prior_partial.economic_result_lamports, None);
}

#[test]
fn priority_fee_requires_signed_hash_and_immutable_reservation_for_buy_and_sell() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE execution_canary_build_plan_metadata(
        order_id TEXT PRIMARY KEY,priority_fee_json TEXT);
        CREATE TABLE execution_canary_dispatch(order_id TEXT PRIMARY KEY,
        message_sha256 TEXT,transaction_sha256 TEXT,tx_signature TEXT);
        CREATE TABLE execution_tiny_reservations(order_id TEXT PRIMARY KEY,
        tx_signature TEXT,wallet TEXT,side TEXT,priority_fee INTEGER);
        CREATE TABLE owner_exit_fee_reservations(order_id TEXT PRIMARY KEY,
        tx_signature TEXT,wallet TEXT,priority_fee INTEGER);",
    )
    .unwrap();
    let message = "a".repeat(64);
    let transaction = "b".repeat(64);
    for side in ["buy", "sell"] {
        let mut receipt = facts();
        receipt.order_id = format!("order-{side}");
        receipt.side = side.into();
        let proof = |price, priority| {
            json!({"fee_proof":{
            "version":1,"message_sha256":message,"transaction_sha256":transaction,
            "requested_compute_unit_limit":1_000_000,
            "micro_lamports_per_compute_unit":price,
            "total_priority_fee_lamports":priority}})
            .to_string()
        };
        conn.execute(
            "INSERT INTO execution_canary_build_plan_metadata VALUES(?1,?2)",
            params![receipt.order_id, proof(0, 0)],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO execution_canary_dispatch VALUES(?1,?2,?3,?4)",
            params![receipt.order_id, message, transaction, receipt.tx_signature],
        )
        .unwrap();
        if side == "buy" {
            conn.execute(
                "INSERT INTO execution_tiny_reservations VALUES(?1,?2,?3,'buy',0)",
                params![
                    receipt.order_id,
                    receipt.tx_signature,
                    receipt.wallet_pubkey
                ],
            )
            .unwrap();
        } else {
            conn.execute(
                "INSERT INTO owner_exit_fee_reservations VALUES(?1,?2,?3,0)",
                params![
                    receipt.order_id,
                    receipt.tx_signature,
                    receipt.wallet_pubkey
                ],
            )
            .unwrap();
        }
        assert_eq!(
            confirmed_priority_fee(&conn, &receipt, Some("5000")).unwrap(),
            Some(0),
            "signed {side} zero price and zero reservation should classify zero"
        );
        // Only the metadata JSON changes. Signed message/transaction hashes,
        // successful receipt and the immutable reservation stay at zero.
        conn.execute("UPDATE execution_canary_build_plan_metadata SET priority_fee_json=?2 WHERE order_id=?1",
            params![receipt.order_id,proof(1,1)]).unwrap();
        assert_eq!(
            confirmed_priority_fee(&conn, &receipt, Some("5000")).unwrap(),
            None,
            "metadata-only {side} price/priority tamper must not classify one"
        );
    }
}
