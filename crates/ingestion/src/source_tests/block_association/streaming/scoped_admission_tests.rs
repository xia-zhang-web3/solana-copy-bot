use super::*;
use crate::ReplayInput;
use copybot_core_types::association_delivery::{Delivery, DeliveryEvent, SessionGap, Terminal};
use std::collections::HashSet;
use tokio::sync::mpsc;

fn wallet(seed: u8) -> String {
    bs58::encode([seed; 32]).into_string()
}

// Keep full transaction Info and token ownership consistent while varying
// identities. The block assertion is copied from the same changed Info.
fn attributed_pair(
    sell: bool,
    signer_seed: u8,
    signature_id: u16,
    block_index: u8,
) -> Result<(SubscribeUpdate, SubscribeUpdateBlock)> {
    let (mut update, mut block) = pair(sell, false)?;
    let tx = tx_mut(&mut update);
    let info = tx.transaction.as_mut().unwrap();
    let old_signer = bs58::encode(
        &info
            .transaction
            .as_ref()
            .unwrap()
            .message
            .as_ref()
            .unwrap()
            .account_keys[0],
    )
    .into_string();
    let mut signature = vec![91u8; 64];
    signature[..2].copy_from_slice(&signature_id.to_le_bytes());
    info.signature = signature.clone();
    let wire_tx = info.transaction.as_mut().unwrap();
    wire_tx.signatures[0] = signature;
    wire_tx.message.as_mut().unwrap().account_keys[0] = vec![signer_seed; 32];
    let meta = info.meta.as_mut().unwrap();
    for row in meta
        .pre_token_balances
        .iter_mut()
        .chain(&mut meta.post_token_balances)
    {
        if row.owner == old_signer {
            row.owner = wallet(signer_seed);
        }
    }
    tx.slot = 40_000 + u64::from(block_index);
    block.slot = tx.slot;
    block.parent_slot = tx.slot - 1;
    block.blockhash = wallet(block_index + 10);
    block.parent_blockhash = wallet(block_index + 9);
    block.transactions = vec![info.clone()];
    Ok((update, block))
}

fn foreign(id: u16) -> Result<SubscribeUpdate> {
    Ok(attributed_pair(false, 220, id, 1)?.0)
}

async fn scoped_replay(inputs: Vec<ReplayInput>, scope: HashSet<String>) -> Result<Vec<Delivery>> {
    let (tx, rx) = mpsc::channel(2);
    let mut service = crate::IngestionService::with_replay_scoped(
        &super::super::delivery_tests::policy(),
        rx,
        "scoped-pressure".into(),
        Some(scope.clone()),
    )?;
    let mut receiver = service
        .take_delivery_scoped("unused".into(), Some(scope))?
        .unwrap();
    let producer = tokio::spawn(async move {
        for input in inputs {
            if tx.send(input).await.is_err() {
                break;
            }
        }
    });
    let mut deliveries = Vec::new();
    while let Some(event) = receiver.next().await? {
        deliveries.push(event.delivery);
    }
    producer.await?;
    Ok(deliveries)
}

#[test]
fn scoped_pressure_never_consumes_history_or_identity_for_742_foreign_swaps() -> Result<()> {
    let source = YellowstoneGrpcSource::new(&config())?;
    let scope = HashSet::from([wallet(11), wallet(12), wallet(13), wallet(14)]);
    let mut l = limits();
    l.pending.count = 1;
    l.history.count = 1;
    let mut a = adapter(&source.runtime_config, l);
    a.restrict_wallets(&scope);
    // Run 07 admitted about this many irrelevant swaps in 26 seconds. Spread
    // their arrival offsets across an hour without consuming any provider data.
    for n in 0..742u16 {
        let tx = foreign(n)?;
        let ns = u64::from(n) * 3_600_000_000_000 / 742;
        assert!(matches!(
            a.push(context(ns), tx_input(&tx)),
            Ok(Admission::NotChecked { .. })
        ));
        assert!(drain(&mut a).is_empty());
    }
    let (chosen, block) = attributed_pair(false, 11, 900, 1)?;
    let end_ns = 3_600_000_000_001;
    assert_eq!(
        a.push(context(end_ns), tx_input(&chosen)),
        Ok(Admission::Transaction(ResultId(0)))
    );
    assert!(drain(&mut a).is_empty());
    let out = feed(&mut a, end_ns + 1, Input::Block(&block));
    assert_eq!(out.len(), 1);
    assert!(matches!(
        terminal(&out[0]).1,
        Resolution::ProviderAsserted { .. }
    ));
    assert_eq!(terminal(&out[0]).0.facts.signer, wallet(11));
    Ok(())
}

#[tokio::test]
async fn scoped_replay_keeps_three_leaders_bot_buy_sell_and_parent_chain() -> Result<()> {
    use super::super::delivery_tests::{block, input};
    let scope = HashSet::from([wallet(11), wallet(12), wallet(13), wallet(14)]);
    let selected = [
        (false, 11), // first leader BUY
        (false, 14), // bot BUY anchor
        (false, 12), // second leader
        (false, 13), // third leader
        (true, 11),  // source SELL
        (true, 14),  // bot SELL
    ];
    let mut inputs = Vec::new();
    let mut identities = Vec::new();
    let mut chosen_index = 0usize;
    for n in 0..742u16 {
        let ns = u64::from(n) * 3_600_000_000_000 / 742;
        inputs.push(input(ns, foreign(n)?));
        if n % 120 == 119 && chosen_index < selected.len() {
            let (sell, signer) = selected[chosen_index];
            let (tx, b) = attributed_pair(
                sell,
                signer,
                900 + chosen_index as u16,
                chosen_index as u8 + 1,
            )?;
            identities.push((
                wallet(signer),
                bs58::encode(&transaction(&tx).transaction.as_ref().unwrap().signature)
                    .into_string(),
            ));
            inputs.push(input(ns + 1, tx));
            inputs.push(input(ns + 2, block(b)));
            chosen_index += 1;
        }
    }
    assert_eq!(chosen_index, selected.len());
    inputs.push(ReplayInput::End(3_600_000_000_001));
    let deliveries = scoped_replay(inputs, scope).await?;
    assert!(deliveries
        .windows(2)
        .all(|pair| pair[1].sequence == pair[0].sequence + 1));
    assert!(deliveries.iter().all(|d| d.session == "scoped-pressure:0"));
    assert!(matches!(
        deliveries.first().unwrap().event,
        DeliveryEvent::Session(SessionGap::StartedContinuityUnknown)
    ));
    assert!(matches!(
        deliveries.last().unwrap().event,
        DeliveryEvent::Session(SessionGap::End)
    ));
    let admissions: Vec<_> = deliveries
        .iter()
        .filter_map(|d| match &d.event {
            DeliveryEvent::Admission(a) => Some(a),
            _ => None,
        })
        .collect();
    assert_eq!(admissions.len(), selected.len());
    for (admission, (signer, signature)) in admissions.iter().zip(&identities) {
        assert_eq!(&admission.facts.wallet, signer);
        assert_eq!(&admission.facts.signature, signature);
    }
    for (buy, sell) in [(0, 4), (1, 5)] {
        assert_eq!(
            admissions[buy].facts.token_in,
            admissions[sell].facts.token_out
        );
        assert_eq!(
            admissions[buy].facts.token_out,
            admissions[sell].facts.token_in
        );
    }
    let terminal_signatures: HashSet<_> = deliveries
        .iter()
        .filter_map(|d| match &d.event {
            DeliveryEvent::Terminal {
                signature,
                result: Terminal::ProviderAsserted(assertion),
                ..
            } => {
                assert_eq!(signature, &assertion.signature);
                Some(signature.as_str())
            }
            _ => None,
        })
        .collect();
    assert_eq!(terminal_signatures.len(), selected.len());
    assert!(identities
        .iter()
        .all(|(_, signature)| terminal_signatures.contains(signature.as_str())));
    assert!(!deliveries.iter().any(|d| matches!(
        &d.event,
        DeliveryEvent::Terminal {
            result: Terminal::Unresolved(_),
            ..
        } | DeliveryEvent::Duplicate { .. }
            | DeliveryEvent::Session(SessionGap::Rejected(_))
    )));
    let parents: Vec<_> = deliveries
        .iter()
        .filter_map(|d| match &d.event {
            DeliveryEvent::Parent(p) => Some(p),
            _ => None,
        })
        .collect();
    assert_eq!(parents.len(), selected.len());
    for p in &parents {
        assert!(p.issue.is_none());
    }
    for pair in parents.windows(2) {
        assert_eq!(pair[1].parent, pair[0].child);
    }
    Ok(())
}

#[test]
fn scoped_selected_duplicate_and_conflict_remain_linked_to_first_admission() -> Result<()> {
    let source = YellowstoneGrpcSource::new(&config())?;
    let scope = HashSet::from([wallet(11)]);
    let mut a = adapter(&source.runtime_config, limits());
    a.restrict_wallets(&scope);
    let (tx, b) = attributed_pair(true, 11, 950, 1)?;
    assert_eq!(
        a.push(context(0), tx_input(&tx)),
        Ok(Admission::Transaction(ResultId(0)))
    );
    assert!(drain(&mut a).is_empty());
    assert_eq!(
        a.push(context(1), tx_input(&tx)),
        Ok(Admission::Duplicate(ResultId(0)))
    );
    assert!(drain(&mut a).is_empty());
    let mut conflict = tx.clone();
    tx_mut(&mut conflict)
        .transaction
        .as_mut()
        .unwrap()
        .meta
        .as_mut()
        .unwrap()
        .fee += 1;
    assert_eq!(
        a.push(context(2), tx_input(&conflict)),
        Ok(Admission::Duplicate(ResultId(0)))
    );
    let out = drain(&mut a);
    assert_eq!(out.len(), 1);
    unresolved(&out[0], UnresolvedReason::ConflictingTransaction);
    assert_facts(&tx, terminal(&out[0]).0);
    let late = feed(&mut a, 3, Input::Block(&b));
    assert!(matches!(
        &late[..],
        [Outcome::Late {
            id: ResultId(0),
            original: Resolution::Unresolved(UnresolvedReason::ConflictingTransaction),
            evidence: LateEvidence::ProviderAssertion { .. },
            ..
        }]
    ));
    Ok(())
}
