#![allow(dead_code)]
#[path = "association_sell_fixture.rs"]
pub mod old;
use anyhow::Result;
use copybot_core_types::{association_delivery::*, association_parent::*};
pub use old::*;
pub fn hash(tag: u8) -> String {
    format!(
        "{}{}",
        "1".repeat(31),
        b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"[tag as usize] as char
    )
}
pub fn key(slot: u64, tag: u8) -> BlockKey {
    BlockKey {
        slot,
        hash: hash(tag),
    }
}
pub fn edge(child: BlockKey, parent: BlockKey) -> ParentObservation {
    let issue = issue(&child, &parent, valid_hash);
    ParentObservation {
        child,
        parent,
        issue,
    }
}
pub fn graph() -> Vec<ParentObservation> {
    vec![
        edge(key(42, 3), key(40, 2)),
        edge(key(40, 2), key(30, 1)),
        edge(key(60, 6), key(50, 5)),
        edge(key(50, 5), key(45, 4)),
        edge(key(45, 4), key(42, 3)),
    ]
}
pub fn put(f: &mut F, e: ParentObservation) -> Result<()> {
    f.event(DeliveryEvent::Parent(e), CandidateGeneration::Unknown)
}
pub fn anchors(f: &mut F, sell_first: bool) -> Result<()> {
    let mut all = vec![
        (facts("leaderbuy", "leader", true), key(30, 1), 1),
        (facts(&f.our, "execution-wallet", true), key(42, 3), 2),
        (facts("sell", "leader", false), key(60, 6), 3),
    ];
    if sell_first {
        all.rotate_right(1);
    }
    for (mut a, k, index) in all {
        a.facts.slot = k.slot;
        f.admit(a.clone())?;
        f.terminal(&a, index, k.slot, &k.hash)?;
    }
    f.drain()?;
    Ok(())
}
pub fn ready(f: &mut F) -> Result<()> {
    anchors(f, false)?;
    for e in graph() {
        put(f, e)?;
    }
    f.drain()?;
    Ok(())
}
