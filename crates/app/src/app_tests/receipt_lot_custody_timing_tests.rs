//! Original native event/lease survives bounded daemon receipt publication lag.
use super::{
    association_parent_fixture as parent, protected_usdc_fixture as old,
    receipt_lot_custody_fixture as f,
};
use crate::execution_owned_sell_rpc::fractional::{self as fraction, readiness, transport::Parsed};
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
struct Clock {
    now: DateTime<Utc>,
    path: PathBuf,
    order: String,
    publish: bool,
    sleeps: usize,
}
impl readiness::Clock for Clock {
    fn now(&self) -> DateTime<Utc> {
        self.now
    }
    fn sleep(&mut self, d: Duration) -> readiness::Sleep<'_> {
        self.now += chrono::Duration::from_std(d).unwrap();
        self.sleeps += 1;
        if self.publish && self.sleeps == 2 {
            rusqlite::Connection::open(&self.path).unwrap().execute("UPDATE execution_canary_receipt_proofs SET reason='accounting_complete' WHERE order_id=?1",[&self.order]).unwrap();
        }
        Box::pin(std::future::ready(()))
    }
}
#[tokio::test]
async fn receipt_lot_custody_pending_source_sell_defers_then_real_quote_same_lease() -> Result<()> {
    let mut c = f::Case::new().await?;
    let id = c.source_dispatch()?;
    c.native_sell().await?;
    c.source_confirm(&id).await?;
    let sql = rusqlite::Connection::open(&c.source_path)?;
    sql.execute("UPDATE execution_canary_receipt_proofs SET reason='confirmed_pending_accounting' WHERE order_id=?1",[&id])?;
    let claim = c.bot.claim()?;
    let lease = claim.lease_until;
    let binding = claim.binding.clone();
    let accounts = c.accounts(70_000_000)?;
    let count = Arc::new(AtomicUsize::new(0));
    let seen = count.clone();
    let mut rpc = Parsed(move |r: Value| {
        seen.fetch_add(1, Ordering::SeqCst);
        std::future::ready(Ok(
            json!({"jsonrpc":"2.0","id":r["id"],"result":match r["method"].as_str().unwrap(){"getGenesisHash"=>json!(super::owner_buy_fixture::GENESIS),"getTokenAccountsByOwner"=>accounts.clone(),_=>panic!("no other RPC")}}),
        ))
    });
    let clock = Clock {
        now: Utc::now(),
        path: c.source_path.clone(),
        order: id,
        publish: true,
        sleeps: 0,
    };
    let out = fraction::bind_transport_with_clock(
        &mut c.bot.db.store,
        &c.bot.config,
        claim,
        parent::limits(),
        &mut rpc,
        clock,
    )
    .await?;
    assert_eq!(out.lease_until, lease);
    assert_eq!(out.binding.source_signature, binding.source_signature);
    assert_eq!(count.load(Ordering::SeqCst), 2);
    assert_eq!(out.binding.raw, old::H);
    Ok(())
}
#[tokio::test]
async fn receipt_lot_custody_pending_unknown_stop_and_expiry_never_quote_or_rearm() -> Result<()> {
    for mode in ["unknown", "stop", "expiry"] {
        let mut c = f::Case::new().await?;
        let _id = c.source_dispatch()?;
        c.native_sell().await?;
        let claim = c.bot.claim()?;
        let count = Arc::new(AtomicUsize::new(0));
        let n = count.clone();
        let mut rpc = Parsed(move |_: Value| {
            n.fetch_add(1, Ordering::SeqCst);
            std::future::ready(Ok(json!(null)))
        });
        if mode == "stop" {
            c.bot.config.canary_kill_switch_path =
                c.root.path().join("STOP").to_string_lossy().into();
            std::fs::write(&c.bot.config.canary_kill_switch_path, "localstop")?;
        }
        let now = if mode == "expiry" {
            claim.lease_until
        } else {
            Utc::now()
        };
        let clock = Clock {
            now,
            path: c.source_path.clone(),
            order: String::new(),
            publish: false,
            sleeps: 0,
        };
        let e = fraction::bind_transport_with_clock(
            &mut c.bot.db.store,
            &c.bot.config,
            claim.clone(),
            parent::limits(),
            &mut rpc,
            clock,
        )
        .await
        .unwrap_err();
        let expected = match mode {
            "stop" => "kill_switch_active",
            "expiry" => "fraction_collection_deadline",
            _ => "receipt_lot_source_receipt_observations_exhausted",
        };
        assert!(e.to_string().contains(expected), "{mode}: {e:#}");
        assert_eq!(count.load(Ordering::SeqCst), 0);
        assert!(c
            .bot
            .db
            .store
            .begin_fractional_sell(
                &claim,
                parent::limits(),
                Utc::now(),
                &crate::execution_owned_sell_rpc::identity(&c.bot.config)?
            )
            .is_err());
    }
    Ok(())
}
#[tokio::test]
async fn receipt_lot_custody_pending_source_buy_is_not_retimed_or_finalized_without_receipt(
) -> Result<()> {
    let c = f::Case::new().await?;
    let id = copybot_storage_core::owner_technical_buy_order_id(&c.buy.intent_id);
    let sql = rusqlite::Connection::open(&c.source_path)?;
    let original: String = c.bot.db.sql.query_row(
        "SELECT admission FROM association_inbox_identities WHERE signature=?1",
        [f::BUY_SIG],
        |r| r.get(0),
    )?;
    sql.execute("UPDATE execution_canary_receipt_proofs SET reason='confirmed_pending_accounting' WHERE order_id=?1",[&id])?;
    let calls = Arc::new(AtomicUsize::new(0));
    let n = calls.clone();
    let mut rpc = Parsed(move |r: Value| {
        n.fetch_add(1, Ordering::SeqCst);
        std::future::ready(Ok(
            json!({"jsonrpc":"2.0","id":r["id"],"result":match r["method"].as_str().unwrap(){"getGenesisHash"=>json!(super::owner_buy_fixture::GENESIS),"getTransaction"=>json!({"slot":100,"transaction":{"signatures":[f::BUY_SIG],"message":{"accountKeys":[{"pubkey":f::SOURCE,"signer":true}]}},"meta":{"err":null,"postTokenBalances":[{"mint":old::USDC,"owner":f::SOURCE}]}}),"getAccountInfo"=>json!({"context":{"slot":100},"value":{"owner":copybot_storage_core::native_buy::SPL_TOKEN_PROGRAM}}),_=>panic!("unexpected method")}}),
        ))
    });
    let mut check = || Ok(());
    assert!(crate::execution_native_buy_rpc::finalized_source(
        &mut rpc,
        &c.bot.config,
        f::BUY_SIG,
        100,
        f::SOURCE,
        old::USDC,
        &mut check
    )
    .await
    .is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(c
        .bot
        .db
        .store
        .controlled_buy_candidate(&c.scope, f::BUY_SIG, 100, f::SOURCE, old::USDC)
        .is_err());
    sql.execute(
        "UPDATE execution_canary_receipt_proofs SET reason='accounting_complete' WHERE order_id=?1",
        [&id],
    )?;
    crate::execution_native_buy_rpc::finalized_source(
        &mut rpc,
        &c.bot.config,
        f::BUY_SIG,
        100,
        f::SOURCE,
        old::USDC,
        &mut check,
    )
    .await?;
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    c.bot
        .db
        .store
        .controlled_buy_candidate(&c.scope, f::BUY_SIG, 100, f::SOURCE, old::USDC)?;
    let after: String = c.bot.db.sql.query_row(
        "SELECT admission FROM association_inbox_identities WHERE signature=?1",
        [f::BUY_SIG],
        |r| r.get(0),
    )?;
    assert_eq!(after, original);
    Ok(())
}
