//! Optional causal replay of a coherent stopped LOCAL case; never the live DB.
#![cfg(feature = "local-bounded-session")]
use anyhow::Result;
use copybot_storage_core::association_inbox::{AssociationInbox, InboxLimits};
#[test]
#[ignore = "requires COPYBOT_LOCAL_REPLAY_DB: a standalone stopped LOCAL fixture copy"]
fn local_pending_actual_stopped_history_replay_preserves_unknown_and_pending_buy() -> Result<()> {
    let input=std::path::PathBuf::from(std::env::var("COPYBOT_LOCAL_REPLAY_DB")?);
    for mode in ["settled","unknown","incomplete","pending-buy"] {
        let dir=tempfile::tempdir()?;let path=dir.path().join("fixture.db");std::fs::copy(&input,&path)?;
        let c=rusqlite::Connection::open(&path)?;
        let sell:String=c.query_row("SELECT source_sell_signature FROM local_bounded_cycles WHERE cycle_no=2",[],|r|r.get(0))?;
        let old:String=c.query_row("SELECT b.order_id FROM local_bounded_orders b JOIN local_bounded_cycles p ON p.cycle_id=b.cycle_id WHERE p.cycle_no=1 AND b.side='sell'",[],|r|r.get(0))?;
        match mode {
            "unknown"=>{c.execute("UPDATE orders SET status='execution_canary_confirmed_unreconciled' WHERE order_id=?1",[&old])?;},
            "incomplete"=>{c.execute("UPDATE execution_tiny_reservations SET actual_fee=NULL,outcome=NULL,reconciled_at=NULL WHERE order_id=?1",[&old])?;},
            "pending-buy"=>{
                drop(c);
                use copybot_core_types::{CopySignalRow,Lamports,COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS};
                let s=copybot_storage_core::SqliteStore::open(&path)?;
                let signal=CopySignalRow {signal_id:"pending-fixture-buy".into(),wallet_id:"fixture-leader".into(),side:"buy".into(),token:copybot_storage_core::OWNER_EXIT_MINT.into(),notional_sol:0.01,notional_lamports:Some(Lamports::new(10000000)),notional_origin:COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.into(),ts:chrono::Utc::now(),status:"shadow_recorded".into()};
                s.insert_copy_signal(&signal)?;s.reserve_execution_canary_order(&signal.signal_id,"fixture",signal.ts)?;
            },
            _=>{}
        }
        let inbox=AssociationInbox::open(&path,InboxLimits {count:500000,bytes:536870912,busy_ms:5000})?;
        let p=inbox.sell_preparation(&sell)?.expect("actual SELL2 preparation");
        assert_eq!(p.first.pending_buys.len(),1,"original evidence retained");
        if mode=="settled" {assert!(p.current.pending_buys.is_empty());}
        else {assert!(!p.current.pending_buys.is_empty(),"{mode}");}
    }
    Ok(())
}
