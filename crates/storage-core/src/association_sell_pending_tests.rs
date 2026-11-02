#![cfg(feature = "local-bounded-session")]
use super::financial::pending;
use rusqlite::Connection;
fn fixture() -> Connection {
    let c=Connection::open_in_memory().unwrap();
    // Minimal SQL operands for the actual reader, reproducing native SELL without
    // a copy signal. Authority/budget behavior is tested by the session API tests.
    c.execute_batch("CREATE TABLE local_bounded_sessions(singleton INTEGER); INSERT INTO local_bounded_sessions VALUES(1);
        CREATE TABLE orders(order_id TEXT,status TEXT,tx_signature TEXT,signal_id TEXT);
        CREATE TABLE copy_signals(signal_id TEXT,token TEXT,side TEXT);
        CREATE TABLE positions(position_id TEXT,state TEXT,qty REAL,cost_sol REAL);
        CREATE TABLE local_bounded_cycles(cycle_id TEXT,state TEXT,position_id TEXT);
        CREATE TABLE local_bounded_orders(order_id TEXT,cycle_id TEXT,side TEXT);
        CREATE TABLE execution_canary_dispatch(order_id TEXT,side TEXT);
        CREATE TABLE execution_canary_receipt_facts(order_id TEXT);
        CREATE TABLE execution_canary_receipt_proofs(order_id TEXT,reason TEXT);
        CREATE TABLE execution_tiny_reservations(order_id TEXT,actual_fee INTEGER,outcome TEXT);
        CREATE TABLE execution_canary_unresolved_dispatch(order_id TEXT);
        CREATE TABLE fills(order_id TEXT);
        CREATE TABLE rpc_owned_sell_dispatches(order_id TEXT,intent_id TEXT,handoff_owner TEXT);
        CREATE TABLE rpc_owned_sell_handoffs(intent_id TEXT,owner TEXT,position_id TEXT);
        INSERT INTO positions VALUES('p','closed',0,0); INSERT INTO local_bounded_cycles VALUES('c','closed','p');
        INSERT INTO orders VALUES('buy','execution_canary_confirmed','buy-sig','buy-signal');
        INSERT INTO copy_signals VALUES('buy-signal','mint','buy');
        INSERT INTO orders VALUES('native-sell','execution_canary_confirmed','sell-sig','source-sell');
        INSERT INTO local_bounded_orders VALUES('buy','c','buy'),('native-sell','c','sell');
        INSERT INTO execution_canary_dispatch VALUES('native-sell','sell');
        INSERT INTO execution_canary_receipt_facts VALUES('buy'),('native-sell');
        INSERT INTO execution_canary_receipt_proofs VALUES('buy','accounting_complete'),('native-sell','accounting_complete');
        INSERT INTO execution_tiny_reservations VALUES('buy',5000,'successful'),('native-sell',5000,'successful');
        INSERT INTO fills VALUES('buy'),('native-sell');
        INSERT INTO rpc_owned_sell_dispatches VALUES('native-sell','i','owner');
        INSERT INTO rpc_owned_sell_handoffs VALUES('i','owner','p');").unwrap();c
}
#[test]
fn local_pending_reader_excludes_only_fully_settled_local_sell() {
    let c=fixture();assert!(pending(&c,"mint").unwrap().is_empty());
    for sql in ["UPDATE orders SET status='execution_canary_submitted' WHERE order_id='native-sell'",
        "UPDATE execution_tiny_reservations SET actual_fee=NULL WHERE order_id='native-sell'",
        "UPDATE execution_canary_receipt_proofs SET reason='accounting_pending' WHERE order_id='native-sell'",
        "INSERT INTO execution_canary_unresolved_dispatch VALUES('native-sell')",
        "UPDATE local_bounded_cycles SET state='active'",
        "UPDATE rpc_owned_sell_dispatches SET handoff_owner='foreign'",
        "DELETE FROM local_bounded_sessions"] {
        let c=fixture();c.execute_batch(sql).unwrap();assert_eq!(pending(&c,"mint").unwrap().len(),1,"{sql}");
    }
    let c=fixture();c.execute_batch("INSERT INTO orders VALUES('pending-buy','execution_canary_submitted','unknown','pending-signal'); INSERT INTO copy_signals VALUES('pending-signal','mint','buy'); INSERT INTO orders VALUES('unknown-origin','execution_canary_submitted','unknown-2','missing');").unwrap();
    let rows=pending(&c,"mint").unwrap();assert_eq!(rows.len(),2);assert!(rows.iter().any(|s|s.contains("pending-buy")));assert!(rows.iter().any(|s|s.contains("unknown-origin")));
}
