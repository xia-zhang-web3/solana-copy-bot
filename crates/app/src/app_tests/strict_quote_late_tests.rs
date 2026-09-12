use super::{
    association_fixture as f, association_parent_fixture as p, association_sell_fixture as s,
    b97_fixture as b, strict_quote_fixture as q,
};
use anyhow::Result;
use copybot_core_types::{association_delivery::*, CopySignalRow, SwapEvent};
use copybot_ingestion::ReplayInput;
use copybot_storage_core::association_inbox::AssociationInbox;
fn root() -> Result<std::path::PathBuf> {
    Ok(std::path::PathBuf::from(std::env::var("B90_FIXTURE_DIR")?)
        .parent()
        .unwrap()
        .join("b97"))
}

#[tokio::test]
#[ignore = "captured late origin -> automatic intent -> loopback quote"]
async fn strict_quote_late_origin_reaches_http() -> Result<()> {
    let root = std::path::PathBuf::from(std::env::var("B90_FIXTURE_DIR")?)
        .parent()
        .unwrap()
        .join("b97");
    let m: serde_json::Value = serde_json::from_slice(&std::fs::read(root.join("meta.json"))?)?;
    let db = f::Db::new("b97-origin");
    let db = db?;
    s::seed(&db, &m)?;
    let o = &m["origin"];
    let text = |k: &str| o[k].as_str().unwrap().to_owned();
    let swap = SwapEvent {
        wallet: text("signer"),
        dex: text("dex_hint"),
        token_in: text("token_in"),
        token_out: text("token_out"),
        amount_in: o["amount_in"].as_f64().unwrap(),
        amount_out: o["amount_out"].as_f64().unwrap(),
        signature: text("signature"),
        slot: o["slot"].as_u64().unwrap(),
        ts_utc: "2026-09-09T00:00:00Z".parse()?,
        exact_amounts: Some(serde_json::from_value(o["exact_amounts"].clone())?),
    };
    let signal = format!(
        "shadow:{}:{}:buy:{}",
        swap.signature, swap.wallet, swap.token_out
    );
    db.store.insert_copy_signal(&CopySignalRow {
        signal_id: signal.clone(),
        wallet_id: swap.wallet.clone(),
        token: swap.token_out.clone(),
        side: "buy".into(),
        notional_sol: 0.1,
        notional_lamports: Some(copybot_core_types::Lamports::new(100_000_000)),
        notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.into(),
        ts: swap.ts_utc,
        status: "shadow_recorded".into(),
    })?;
    db.store
        .insert_shadow_buy_lot(&swap, &signal, 1.0, None, 0.1)?;
    let before = s::snapshot(&db)?;
    let (mut consumer, tx) = f::start(&db, &f::config(&m), "late-origin").await?;
    let input = tx.clone();
    let root_copy = root.clone();
    let producer = tokio::spawn(async move {
        for (n, name) in ["sell", "source", "our"].iter().enumerate() {
            input.send(s::update(name, n as u64 + 1)).await?;
        }
        input
            .send(ReplayInput::Update {
                offset_ns: 4,
                payload: std::fs::read(root_copy.join("block.pb"))?,
            })
            .await?;
        Ok::<_, anyhow::Error>(())
    });
    b::until(&mut consumer, &db, || {
        Ok(db.sql.query_row(
            "SELECT count(*) FROM association_inbox_identities WHERE terminal IS NOT NULL",
            [],
            |r| r.get::<_, i64>(0),
        )? == 3
            && b::idle(&db)?)
    })
    .await?;
    producer.await??;
    assert_eq!(b::count(&db, "ordered_source_sell_intents")?, 0);
    let first = q::first(&db, &m)?;
    tx.send(ReplayInput::Update {
        offset_ns: 5,
        payload: std::fs::read(root.join("origin.pb"))?,
    })
    .await?;
    b::until(&mut consumer, &db, || {
        Ok(b::count(&db, "ordered_source_sell_intents")? == 1 && b::idle(&db)?)
    })
    .await?;
    assert_eq!(q::first(&db, &m)?, first);
    assert_eq!(b::count(&db, "source_sell_signature_claims")?, 1);
    assert_eq!(s::snapshot(&db)?, before);
    tx.send(ReplayInput::End(6)).await?;
    f::drain(&mut consumer, &db).await?;
    let observation = q::quote(&db, &m).await?;
    assert_eq!(observation.binding.unwrap().intent_id, q::id(&m));
    Ok(())
}

#[tokio::test]
#[ignore = "captured late Parent -> automatic intent -> loopback quote"]
async fn strict_quote_late_parent_reaches_http() -> Result<()> {
    let m = p::meta("direct")?;
    let template = f::Db::new("b97-parent-template")?;
    s::seed(&template, &m)?;
    p::stage(
        &template,
        "direct",
        &m,
        p::frames(&m),
        "parent-template",
        false,
    )
    .await?;
    let db = f::Db::new("b97-last-parent")?;
    s::seed(&db, &m)?;
    let mut inbox = AssociationInbox::open(&db.path, p::limits())?;
    let mut omitted = 0;
    for (event, candidate) in b::event_rows(&template)? {
        if matches!(&event.event,DeliveryEvent::Parent(e) if e.child.slot==120) {
            omitted += 1;
            continue;
        }
        inbox.persist(&event, &candidate)?;
    }
    assert_eq!(omitted, 1);
    while inbox.has_sell_preparation_work()? {
        inbox.recover_sell_preparation()?;
    }
    let before = s::snapshot(&db)?;
    let first = q::first(&db, &m)?;
    let (mut consumer, tx) = f::start(&db, &f::config(&m), "last-parent").await?;
    b::until(&mut consumer, &db, || b::idle(&db)).await?;
    assert_eq!(b::count(&db, "ordered_source_sell_intents")?, 0);
    let ids = db.identities()?;
    tx.send(ReplayInput::Update {
        offset_ns: 1,
        payload: std::fs::read(root()?.join("late-parent.pb"))?,
    })
    .await?;
    b::until(&mut consumer, &db, || {
        Ok(b::count(&db, "ordered_source_sell_intents")? == 1 && b::idle(&db)?)
    })
    .await?;
    assert_eq!(db.identities()?, ids);
    assert_eq!(q::first(&db, &m)?, first);
    assert_eq!(b::count(&db, "source_sell_signature_claims")?, 1);
    assert_eq!(s::snapshot(&db)?, before);
    tx.send(ReplayInput::End(2)).await?;
    f::drain(&mut consumer, &db).await?;
    let observation = q::quote(&db, &m).await?;
    assert_eq!(observation.binding.unwrap().intent_id, q::id(&m));
    Ok(())
}
