// This entire file and fixture compile unchanged on accepted90/R1 and candidate.
use super::{
    association_fixture as f, association_parent_fixture as p, association_sell_fixture as s,
};
use anyhow::Result;
use serde_json::json;
#[tokio::test]
#[ignore = "explicit identical offline input on frozen accepted90/R1 and candidate"]
async fn b91_actual_causal_same_cross_block_input() -> Result<()> {
    let baseline = std::env::var("B91_EXPECT_BASELINE").as_deref() == Ok("1");
    for case in ["direct", "multi"] {
        for sell_first in [false, true] {
            let m = p::meta(case)?;
            let db = f::Db::new(&format!("b91-causal-{case}-{sell_first}"))?;
            s::seed(&db, &m)?;
            let before = s::snapshot(&db)?;
            let mut names = p::frames(&m);
            if sell_first {
                names.reverse();
            }
            p::stage(&db, case, &m, names, "b91-causal", false).await?;
            let r = p::read(&db, &m)?;
            let expected = if baseline {
                json!({"Unknown":"CrossSlot"})
            } else {
                json!("ProviderOrderedAcrossBlocks")
            };
            assert_eq!(
                serde_json::to_value(&r.current.selected_chain)?,
                expected,
                "{case} sell_first={sell_first}"
            );
            assert_eq!(r.current.anchors.len(), 3);
            assert!(r.current.anchors.iter().all(|a| matches!(
                a.terminal,
                Some(copybot_core_types::association_delivery::Terminal::ProviderAsserted(_))
            )));
            assert_eq!(r.current.trade_authority, "trade_authority_none");
            assert_eq!(s::snapshot(&db)?, before);
            p::save(&db, &m, "causal")?;
            if baseline {
                use copybot_core_types::association_delivery::*;
                use copybot_storage_core::association_inbox::AssociationInbox;
                let mut inbox = AssociationInbox::open(&db.path, p::limits())?;
                while inbox.has_sell_preparation_work()? {
                    inbox.recover_sell_preparation()?;
                }
                let mut seq = 0;
                while inbox.usage()?.0 < 64 {
                    inbox.persist(
                        &Delivery {
                            session: "full-upgrade".into(),
                            sequence: seq,
                            arrival_offset_ns: seq,
                            event: DeliveryEvent::Session(SessionGap::Reset),
                        },
                        &CandidateGeneration::Unknown,
                    )?;
                    seq += 1;
                }
                let full = inbox.usage()?;
                std::fs::write(
                    db.path.with_extension("upgrade.json"),
                    serde_json::to_vec_pretty(
                        &json!({"count":full.0,"bytes":full.1,"first":r.first,"signature":m["sell"]["signature"]}),
                    )?,
                )?;
            }
        }
    }
    Ok(())
}
