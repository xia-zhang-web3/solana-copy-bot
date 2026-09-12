use super::{association_fixture as f, association_sell_fixture as s};
use anyhow::Result;
use copybot_storage_core::{
    association_inbox::{AssociationInbox, InboxLimits},
    association_sell_preparation::*,
};
#[tokio::test]
#[ignore = "explicit hash-bound synthetic protobuf three-Info chain"]
async fn b90_actual_service_consumer_prepares_receipt_bound_sell_both_arrival_orders() -> Result<()>
{
    for sell_first in [true, false] {
        let m = s::meta()?;
        let c = f::config(&m);
        let db = f::Db::new(if sell_first {
            "chain-sell-first"
        } else {
            "chain-buy-first"
        })?;
        s::seed(&db, &m)?;
        let before = s::snapshot(&db)?;
        let (mut consumer, tx) = f::start(&db, &c, "b90-chain").await?;
        let producer = tokio::spawn(async move {
            let names = if sell_first {
                vec!["sell", "source", "our", "chain-block"]
            } else {
                vec!["source", "our", "sell", "chain-block"]
            };
            for (i, name) in names.iter().enumerate() {
                tx.send(s::update(name, i as u64 + 1)).await.unwrap();
            }
            tx.send(copybot_ingestion::ReplayInput::End(5))
                .await
                .unwrap();
        });
        f::drain(&mut consumer, &db).await?;
        producer.await?;
        assert_eq!(db.identities()?, 3);
        assert_eq!(s::snapshot(&db)?, before);
        let limits = InboxLimits {
            count: 20000,
            bytes: 128 << 20,
            busy_ms: 100,
        };
        let inbox = AssociationInbox::open(&db.path, limits)?;
        let p = inbox
            .sell_preparation(m["sell"]["signature"].as_str().unwrap())?
            .unwrap();
        assert_eq!(p.current.selected_chain, Check::ProviderOrderedWithinBlock);
        assert_eq!(p.current.anchors.len(), 3);
        assert!(p.current.anchors.iter().all(|a| !a
            .identity
            .as_ref()
            .unwrap()
            .admission
            .info
            .encoded
            .is_empty()));
        assert_eq!(p.current.trade_authority, "trade_authority_none");
        let FirstWitness::Selected(w) = &p.first.witness else {
            panic!("missing witness")
        };
        assert_eq!(
            w.source_signature,
            m["source"]["signature"].as_str().unwrap()
        );
        assert_eq!(
            w.receipt.contributor.tx_signature,
            m["our"]["signature"].as_str().unwrap()
        );
        assert_eq!(
            db.sql.query_row(
                "SELECT count(*) FROM association_sell_preparations",
                [],
                |r| r.get::<_, i64>(0)
            )?,
            1
        );
        std::fs::write(
            db.path.with_extension("preparation.json"),
            serde_json::to_vec_pretty(
                &serde_json::json!({"sell_first":sell_first,"first":p.first,"current":p.current,"financial_delta":0}),
            )?,
        )?;
    }
    Ok(())
}
#[tokio::test]
#[ignore = "same unchanged89 small fixture used by the frozen baseline"]
async fn b90_actual_unknown_time_sell_has_explicit_unknown_preparation() -> Result<()> {
    let m = f::metadata("fixture.json");
    let c = f::config(&m);
    let db = f::Db::new("b90-baseline-pair")?;
    let before = db.financial_counts()?;
    let (mut consumer, tx) = f::start(&db, &c, "b90-baseline-pair").await?;
    let producer = tokio::spawn(async move {
        tx.send(f::update("missing", 1)).await.unwrap();
        tx.send(f::update("block", 2)).await.unwrap();
        tx.send(copybot_ingestion::ReplayInput::End(3))
            .await
            .unwrap();
    });
    f::drain(&mut consumer, &db).await?;
    producer.await?;
    let inbox = AssociationInbox::open(
        &db.path,
        InboxLimits {
            count: 20000,
            bytes: 128 << 20,
            busy_ms: 100,
        },
    )?;
    let p = inbox
        .sell_preparation(m["signature"].as_str().unwrap())?
        .unwrap();
    assert_eq!(
        p.first.witness,
        FirstWitness::Unknown(Reason::InitialCandidateUnknown)
    );
    assert_eq!(
        p.current.selected_chain,
        Check::Unknown(Reason::InitialCandidateUnknown)
    );
    assert_eq!(db.financial_counts()?, before);
    Ok(())
}

#[tokio::test]
#[ignore = "offline actual decoder with internally consistent cross-anchor decimals"]
async fn b90r1_actual_decoded_source_and_sell_decimal_conflicts_block() -> Result<()> {
    for variant in ["control", "source-decimals", "sell-decimals"] {
        let root = std::path::PathBuf::from(std::env::var("B90_FIXTURE_DIR")?);
        let m: serde_json::Value =
            serde_json::from_slice(&std::fs::read(root.join(variant).join("chain.json"))?)?;
        let c = f::config(&m);
        let db = f::Db::new(variant)?;
        s::seed(&db, &m)?;
        let before = s::snapshot(&db)?;
        let (mut consumer, tx) = f::start(&db, &c, "b90r1-decimals").await?;
        let producer = tokio::spawn(async move {
            for (i, name) in ["source", "our", "sell", "chain-block"].iter().enumerate() {
                tx.send(s::update(&format!("{variant}/{name}"), i as u64 + 1))
                    .await
                    .unwrap();
            }
            tx.send(copybot_ingestion::ReplayInput::End(5))
                .await
                .unwrap();
        });
        f::drain(&mut consumer, &db).await?;
        producer.await?;
        assert_eq!(db.identities()?, 3);
        assert_eq!(s::snapshot(&db)?, before);
        let inbox = AssociationInbox::open(
            &db.path,
            InboxLimits {
                count: 20000,
                bytes: 128 << 20,
                busy_ms: 100,
            },
        )?;
        let p = inbox
            .sell_preparation(m["sell"]["signature"].as_str().unwrap())?
            .unwrap();
        let expected = if variant == "control" {
            Check::ProviderOrderedWithinBlock
        } else {
            Check::Blocked(Reason::DecimalsConflict)
        };
        assert_eq!(p.current.selected_chain, expected);
        for anchor in &p.current.anchors {
            assert!(matches!(
                anchor.terminal,
                Some(copybot_core_types::association_delivery::Terminal::ProviderAsserted(_))
            ));
            let a = &anchor.identity.as_ref().unwrap().admission;
            assert!(!a.info.encoded.is_empty());
            let exact = a.facts.exact_amounts.as_ref().unwrap();
            let input = exact.amount_in_raw.parse::<u128>()? as f64
                / 10f64.powi(exact.amount_in_decimals.into());
            let output = exact.amount_out_raw.parse::<u128>()? as f64
                / 10f64.powi(exact.amount_out_decimals.into());
            assert_eq!(input.to_bits(), a.facts.amount_in_bits);
            assert_eq!(output.to_bits(), a.facts.amount_out_bits);
        }
        assert_eq!(p.current.trade_authority, "trade_authority_none");
        std::fs::write(
            db.path.with_extension("preparation.json"),
            serde_json::to_vec_pretty(
                &serde_json::json!({"variant":variant,"first":p.first,"current":p.current,"financial_delta":0}),
            )?,
        )?;
    }
    Ok(())
}
