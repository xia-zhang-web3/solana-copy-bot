use copybot_core_types::{Lamports, SignedLamports};
use copybot_storage_core::{confirmed_priority_fee, ExecutionCanaryReceiptFacts, ReceiptDecomposition,
    ReceiptFeeCoverage, ReceiptTokenCoverage, ReceiptTokenDelta, ReceiptWsolCoverage};
use rusqlite::{params, Connection};
use serde_json::json;

const ID: &str = "exec-canary:rpc-owned-sell:LOCAL-SOURCE-SELL";
fn facts() -> ExecutionCanaryReceiptFacts {
    ExecutionCanaryReceiptFacts {
        order_id: ID.into(), tx_signature: "LOCAL-signature".into(), wallet_pubkey: "LOCAL-wallet".into(),
        token: "LOCAL-mint".into(), side: "sell".into(), slot: 71,
        wallet_native_pre: Lamports::new(20_000_000), wallet_native_post: Lamports::new(31_995_000),
        wallet_native_delta: SignedLamports::new(11_995_000), transaction_fee: Some(Lamports::new(5000)),
        fee_coverage: ReceiptFeeCoverage::Known, fee_payer: Some("LOCAL-wallet".into()),
        token_delta: Some(ReceiptTokenDelta { raw: -988620, decimals: 6 }),
        token_coverage: ReceiptTokenCoverage::PairedBalances, token_coverage_reason: None,
        wsol_coverage: ReceiptWsolCoverage::Observed, block_time: None,
        decomposition: ReceiptDecomposition::Unresolved,
    }
}
fn database() -> Connection {
    let c = Connection::open_in_memory().unwrap();
    c.execute_batch("CREATE TABLE execution_canary_build_plan_metadata(order_id TEXT,priority_fee_json TEXT);
        CREATE TABLE execution_canary_dispatch(order_id TEXT,message_sha256 TEXT,transaction_sha256 TEXT,
        tx_signature TEXT,wallet TEXT,side TEXT,signal_id TEXT,client_order_id TEXT);
        CREATE TABLE execution_tiny_reservations(order_id TEXT,tx_signature TEXT,wallet TEXT,side TEXT,
        priority_fee INTEGER,message_sha256 TEXT,transaction_sha256 TEXT,experiment_id TEXT);
        CREATE TABLE owner_exit_fee_reservations(order_id TEXT,tx_signature TEXT,wallet TEXT,priority_fee INTEGER);
        CREATE TABLE rpc_owned_sell_dispatches(order_id TEXT,intent_id TEXT,handoff_owner TEXT);
        CREATE TABLE rpc_owned_sell_handoffs(intent_id TEXT,owner TEXT,wallet TEXT,experiment_id TEXT);").unwrap();
    let metadata=json!({"fee_proof":{"version":1,"message_sha256":"message","transaction_sha256":"transaction",
        "requested_compute_unit_limit":200000,"micro_lamports_per_compute_unit":10000,
        "total_priority_fee_lamports":2000}}).to_string();
    c.execute("INSERT INTO execution_canary_build_plan_metadata VALUES(?1,?2)",params![ID,metadata]).unwrap();
    c.execute("INSERT INTO execution_canary_dispatch VALUES(?1,'message','transaction','LOCAL-signature',
        'LOCAL-wallet','sell','source-sell:LOCAL-SOURCE-SELL','LOCAL-owner')",[ID]).unwrap();
    c.execute("INSERT INTO execution_tiny_reservations VALUES(?1,'LOCAL-signature','LOCAL-wallet','sell',2000,
        'message','transaction','LOCAL-experiment')",[ID]).unwrap();
    c.execute("INSERT INTO rpc_owned_sell_dispatches VALUES(?1,'source-sell:LOCAL-SOURCE-SELL','LOCAL-owner')",[ID]).unwrap();
    c.execute_batch("INSERT INTO rpc_owned_sell_handoffs VALUES('source-sell:LOCAL-SOURCE-SELL','LOCAL-owner',
        'LOCAL-wallet','LOCAL-experiment');").unwrap();
    c
}
#[test]
fn native_sell_uses_tiny_signed_fee_route_without_owner_exit_reservation() {
    let c=database();
    assert_eq!(confirmed_priority_fee(&c,&facts(),Some("5000")).unwrap(),Some(2000));
    assert_eq!(c.query_row("SELECT count(*) FROM owner_exit_fee_reservations",[],|r|r.get::<_,i64>(0)).unwrap(),0);
    assert_eq!(confirmed_priority_fee(&c,&facts(),None).unwrap(),None);
    assert_eq!(confirmed_priority_fee(&c,&facts(),Some("1999")).unwrap(),None);
}
#[test]
fn native_sell_missing_or_changed_reservation_and_dispatch_proofs_stay_unknown() {
    for sql in [
        "DELETE FROM execution_tiny_reservations",
        "UPDATE execution_tiny_reservations SET side='buy'",
        "UPDATE execution_tiny_reservations SET wallet='other'",
        "UPDATE execution_tiny_reservations SET tx_signature='other'",
        "UPDATE execution_tiny_reservations SET message_sha256='other'",
        "UPDATE execution_tiny_reservations SET transaction_sha256='other'",
        "UPDATE execution_tiny_reservations SET experiment_id='other'",
        "UPDATE execution_tiny_reservations SET priority_fee=2001",
        "UPDATE execution_canary_dispatch SET client_order_id='other'",
        "UPDATE execution_canary_dispatch SET signal_id='other'",
        "UPDATE rpc_owned_sell_dispatches SET handoff_owner='other'",
        "UPDATE rpc_owned_sell_handoffs SET wallet='other'",
        "UPDATE execution_canary_build_plan_metadata SET priority_fee_json=NULL",
    ] {
        let c=database();c.execute_batch(sql).unwrap();
        assert_eq!(confirmed_priority_fee(&c,&facts(),Some("5000")).unwrap(),None,"{sql}");
    }
    let c=database();let mut f=facts();f.tx_signature="UNKNOWN-unbound".into();
    assert_eq!(confirmed_priority_fee(&c,&f,Some("5000")).unwrap(),None);
}
