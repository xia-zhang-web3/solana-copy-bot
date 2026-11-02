//! Causal replay of actual normal daemon closed-cycle shape, without owner cycles.
use super::*;
use rusqlite::Connection;
#[test]
#[ignore="requires REAL_SESSION_LOSS_REPLAY_DB from actual offline daemon evidence"]
fn native_cash_cap_and_incomplete_receipt_projections_fail_closed()->Result<()>{
    let path=std::env::var("REAL_SESSION_LOSS_REPLAY_DB")?;
    let original=Connection::open_with_flags(path,rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let cols=[
        ("real_session_sessions","singleton,identity_json"),
        ("real_session_cycles","cycle_id,position_id,state"),
        ("real_session_orders","order_id,cycle_id,side"),
        ("orders","order_id,status"),("fills","order_id,position_id"),
        ("positions","position_id,state,qty,cost_sol"),
        ("execution_canary_receipt_facts","order_id,wallet_native_delta,wallet_pubkey,side"),
        ("execution_canary_receipt_proofs","order_id,reason"),
        ("execution_tiny_reservations","order_id,actual_fee,outcome"),
        ("execution_canary_unresolved_dispatch","order_id"),
        ("rpc_owned_sell_handoffs","intent_id,owner,position_id"),
        ("rpc_owned_sell_dispatches","intent_id,handoff_owner")];
    let setup=||->Result<Connection>{
        let c=Connection::open_in_memory()?;
        for (table,columns) in cols {
            c.execute_batch(&format!("CREATE TABLE {table}({columns});"))?;
            let mut q=original.prepare(&format!("SELECT {columns} FROM {table}"))?;
            let n=columns.split(',').count();let mut rows=q.query([])?;
            while let Some(row)=rows.next()? {
                let values=(0..n).map(|i|row.get::<_,rusqlite::types::Value>(i)).collect::<rusqlite::Result<Vec<_>>>()?;
                c.execute(&format!("INSERT INTO {table} VALUES({})",vec!["?";n].join(",")),rusqlite::params_from_iter(values))?;
            }
        }
        Ok(c)
    };
    let c=setup()?;
    let n:u64=c.query_row("SELECT count(*) FROM real_session_cycles WHERE state='closed'",[],|r|r.get(0))?;
    ensure!(n>0,"actual_native_closed_cycles_required");
    loss_within(&c,u64::MAX)?; // Actual closed native receipts; no owner trade_cycles table exists.
    // A causal counterfactual changes one cash fact only in this in-memory copy.
    // Force the first cycle to exactly -1m; profitable later cycles cannot offset it.
    let (order,delta,net):(String,i64,i64)=c.query_row("SELECT r.order_id,CAST(r.wallet_native_delta AS INTEGER),
        (SELECT sum(CAST(f.wallet_native_delta AS INTEGER)) FROM real_session_orders o2 JOIN execution_canary_receipt_facts f ON f.order_id=o2.order_id WHERE o2.cycle_id=o.cycle_id)
        FROM real_session_orders o JOIN execution_canary_receipt_facts r ON r.order_id=o.order_id
        JOIN real_session_cycles x ON x.cycle_id=o.cycle_id WHERE x.state='closed' AND o.side='buy' ORDER BY x.cycle_id LIMIT 1",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
    c.execute("UPDATE execution_canary_receipt_facts SET wallet_native_delta=?2 WHERE order_id=?1",rusqlite::params![order,(delta-net-1_000_000).to_string()])?;
    let loss=1_000_000;
    loss_within(&c,loss+1)?;
    assert_eq!(loss_within(&c,loss).unwrap_err().to_string(),"real_session_loss_cap");
    assert_eq!(loss_within(&c,loss-1).unwrap_err().to_string(),"real_session_loss_cap");
    for mode in ["missing-receipt","unknown-fee","unresolved"] {
        let c=setup()?;
        let order:String=c.query_row("SELECT order_id FROM real_session_orders LIMIT 1",[],|r|r.get(0))?;
        match mode {
            "missing-receipt"=>{c.execute("DELETE FROM execution_canary_receipt_facts WHERE order_id=?1",[&order])?;},
            "unknown-fee"=>{c.execute("UPDATE execution_tiny_reservations SET actual_fee=NULL WHERE order_id=?1",[&order])?;},
            _=>{c.execute("INSERT INTO execution_canary_unresolved_dispatch VALUES(?1)",[&order])?;}
        }
        assert_eq!(loss_within(&c,loss+1).unwrap_err().to_string(),"real_session_cash_accounting_pending","{mode}");
    }
    Ok(())
}
#[test]
#[ignore="requires actual stopped UNKNOWN/restart case; in-memory accounting projection only"]
fn fresh_competing_source_is_blocked_by_unknown_reservation_until_complete()->Result<()>{
    let root=std::path::PathBuf::from(std::env::var("REAL_SESSION_UNKNOWN_CASE")?);
    let before:serde_json::Value=serde_json::from_slice(&std::fs::read(root.join("UNKNOWN_DURABLE_BEFORE_CRASH.json"))?)?;
    let held=&before["snapshot"]["execution_tiny_reservations"][0];
    ensure!(held["actual_fee"].is_null() && held["outcome"].is_null(),"actual_unknown_hold_required");
    let c=Connection::open_in_memory()?;
    let uri=format!("file:{}?mode=ro",root.join("stopped-db/live_runtime.db").display());
    c.execute("ATTACH DATABASE ?1 AS actual",[uri])?;
    for table in ["real_session_sessions","real_session_cycles","real_session_orders","orders","fills","positions",
        "execution_canary_receipt_facts","execution_canary_receipt_proofs","execution_tiny_reservations",
        "execution_canary_unresolved_dispatch","rpc_owned_sell_handoffs","rpc_owned_sell_dispatches"] {
        c.execute_batch(&format!("CREATE TABLE {table} AS SELECT * FROM actual.{table};"))?;
    }
    c.execute_batch("DETACH DATABASE actual;")?;
    let a=crate::real_session::load(&c)?.context("actual_normal_authority_required")?;
    let count:u32=c.query_row("SELECT count(*) FROM real_session_cycles",[],|r|r.get(0))?;
    ensure!(count==1 && count<a.max_cycles,"actual_remaining_cycle_required");
    let messages:serde_json::Value=serde_json::from_slice(&std::fs::read(root.join("package/local-chain/SOURCE_MESSAGES.json"))?)?;
    let signature=messages.as_array().unwrap().iter().find(|m|m["cycle"]==2 && m["side"]=="buy").unwrap()["signature"].as_str().unwrap();
    let original=Connection::open_with_flags(root.join("stopped-db/live_runtime.db"),rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let raw:String=original.query_row("SELECT admission FROM association_inbox_identities WHERE signature=?1",[signature],|r|r.get(0))?;
    let mut event:copybot_core_types::association_delivery::AdmissionFacts=serde_json::from_str(&raw)?;
    let native:String=original.query_row("SELECT admission FROM native_buy_decisions LIMIT 1",[],|r|r.get(0))?;
    event.message_time=serde_json::from_str::<copybot_core_types::association_delivery::AdmissionFacts>(&native)?.message_time;
    ensure!(!matches!(event.message_time,copybot_core_types::association_delivery::MessageTime::RecoveredBlock{..}),"fresh_fixture_time");
    let fence:u64=original.query_row("SELECT max(processed_slot) FROM native_buy_fence_epochs",[],|r|r.get(0))?;
    event.facts.slot=fence+1; // Explicit fresh projection, not the actual recovered competitor.
    let gate=crate::bounded_session::source::event_allowed;
    ensure!(gate(&c,&event.facts,true)?,"positive_candidate_before_unknown_required");
    let order=held["order_id"].as_str().unwrap();
    let (fee,outcome,at,tx,message,bound):(u64,String,String,String,String,u64)=c.query_row(
        "SELECT actual_fee,outcome,reserved_at,tx_signature,message_sha256,fee_bound FROM execution_tiny_reservations WHERE order_id=?1",[order],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?)))?;
    ensure!(held["reserved_at"]==at && held["tx_signature"]==tx && held["message_sha256"]==message && held["fee_bound"]==bound,"actual_unknown_hold_identity");
    c.execute_batch("UPDATE real_session_cycles SET state='active',closed_at=NULL;")?;
    c.execute("UPDATE execution_tiny_reservations SET actual_fee=NULL,outcome=NULL WHERE order_id=?1",[order])?;
    assert!(!gate(&c,&event.facts,true)?); // Same candidate; canonical pending accounting keeps cycle open.
    c.execute("UPDATE execution_tiny_reservations SET actual_fee=?2,outcome=?3 WHERE order_id=?1",rusqlite::params![order,fee,outcome])?;
    assert!(gate(&c,&event.facts,true)?); // Existing canonical completeness closes it; cap and candidate unchanged.
    Ok(())
}
