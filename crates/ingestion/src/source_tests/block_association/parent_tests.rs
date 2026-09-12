use super::delivery_tests::{block, input, policy, run};
use super::*;
use crate::ReplayInput;
use copybot_core_types::{association_delivery::*, association_parent::*};
#[test]
fn b91_hash_boundary_matches_existing_ingestion_codec() {
    use crate::source::yellowstone_block_association::valid_blockhash;
    let mut hashes = vec![
        String::new(),
        "1".repeat(31),
        "1".repeat(32),
        "1".repeat(33),
        "0".repeat(32),
        "O".repeat(32),
        "z".repeat(44),
        "a".repeat(45),
    ];
    for n in 0..256u16 {
        let mut bytes = [0u8; 32];
        bytes[(n / 8) as usize] = 1 << (n % 8);
        for wire in [bytes, [n as u8; 32]] {
            let encoded = bs58::encode(wire).into_string();
            hashes.push(encoded.clone());
            hashes.push(format!("1{encoded}"));
            hashes.push(format!(" {encoded}"));
        }
    }
    for size in [31, 32, 33] {
        hashes.push(bs58::encode(vec![17u8; size]).into_string());
    }
    for hash in hashes {
        assert_eq!(valid_hash(&hash), valid_blockhash(&hash), "{hash}");
    }
}
#[tokio::test]
async fn b91_every_admitted_header_precedes_terminal_including_empty_and_duplicate_subsets(
) -> Result<()> {
    let (tx, b) = pair(false, false)?;
    let mut empty = b.clone();
    empty.transactions.clear();
    let mut subset = empty.clone();
    subset.executed_transaction_count += 1;
    let (events, failed) = run(
        policy(),
        vec![
            input(1, block(empty)),
            input(2, tx),
            input(3, block(b)),
            input(4, block(subset)),
            ReplayInput::End(5),
        ],
    )
    .await;
    assert!(!failed);
    let parents: Vec<_> = events
        .iter()
        .filter_map(|d| {
            if let DeliveryEvent::Parent(p) = &d.event {
                Some((d.sequence, p))
            } else {
                None
            }
        })
        .collect();
    assert_eq!(parents.len(), 3);
    assert!(parents
        .iter()
        .all(|(_, p)| *p == parents[0].1 && p.issue.is_none()));
    let terminal = events
        .iter()
        .find(|d| {
            matches!(
                d.event,
                DeliveryEvent::Terminal {
                    result: Terminal::ProviderAsserted(_),
                    ..
                }
            )
        })
        .unwrap();
    assert!(parents[1].0 < terminal.sequence);
    assert!(events
        .windows(2)
        .all(|w| w[1].sequence == w[0].sequence + 1));
    Ok(())
}
#[tokio::test]
async fn b91_bad_parent_does_not_change_full_info_contract_or_drop_checked_facts() -> Result<()> {
    for variant in 0..4 {
        let (tx, mut b) = pair(false, false)?;
        let expected = match variant {
            0 => {
                b.parent_blockhash.clear();
                ParentIssue::MissingParentHash
            }
            1 => {
                b.parent_blockhash = "bad".into();
                ParentIssue::MalformedParentHash
            }
            2 => {
                b.parent_slot = b.slot;
                ParentIssue::NondecreasingParentSlot
            }
            _ => {
                b.parent_slot = b.slot + 1;
                ParentIssue::NondecreasingParentSlot
            }
        };
        assert!(associate(&tx, &b).is_ok());
        let (events, failed) = run(
            policy(),
            vec![input(1, tx), input(2, block(b)), ReplayInput::End(3)],
        )
        .await;
        assert!(!failed);
        assert_eq!(
            events
                .iter()
                .filter(|d| matches!(d.event, DeliveryEvent::Admission(_)))
                .count(),
            1
        );
        assert!(events.iter().any(
            |d| matches!(&d.event,DeliveryEvent::Parent(p) if p.issue==Some(expected.clone()))
        ));
        assert!(events.iter().any(|d| matches!(
            d.event,
            DeliveryEvent::Terminal {
                result: Terminal::ProviderAsserted(_),
                ..
            }
        )));
    }
    Ok(())
}
#[tokio::test]
async fn b91_rejected_input_block_metadata_bounds_never_emit_the_rejected_header() -> Result<()> {
    for variant in 0..3 {
        let (_, mut b) = pair(false, false)?;
        b.transactions.clear();
        let mut c = policy();
        let l = c.ingestion.yellowstone_association.as_mut().unwrap();
        let mut inputs = vec![];
        if variant == 0 {
            l.input_bytes = b.encoded_len() - 1;
        } else if variant == 1 {
            l.blocks.count = 1;
            let mut previous = b.clone();
            previous.slot -= 2;
            inputs.push(input(0, block(previous)));
        } else {
            l.metadata_bytes = 511;
        }
        inputs.push(input(1, block(b.clone())));
        inputs.push(ReplayInput::End(2));
        let (events, failed) = run(c, inputs).await;
        assert!(failed);
        assert!(!events
            .iter()
            .any(|d| matches!(&d.event,DeliveryEvent::Parent(p) if p.child.slot==b.slot)));
        // Transport byte rejection happens before adapter admission; its task
        // error is the explicit refusal. Adapter bounds also persist Rejected.
        if variant != 0 {
            assert!(events
                .iter()
                .any(|d| matches!(d.event, DeliveryEvent::Session(SessionGap::Rejected(_)))));
        }
    }
    Ok(())
}
