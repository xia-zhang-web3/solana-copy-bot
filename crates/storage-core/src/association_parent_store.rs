use super::*;
use copybot_core_types::association_delivery::{Delivery, DeliveryEvent};
const ROW: &str = "SELECT json_array(first_observation,first_session,first_sequence,contradiction) FROM association_parent_blocks WHERE block_key=?1";
const HASH: &str = "SELECT json_array(first_slot,contradiction_slot) FROM association_parent_hashes WHERE block_hash=?1";
/// First observation is immutable. Missing/malformed -> known is a contradiction,
/// never an implicit upgrade. No observation at a frontier can still arrive late.
pub(in crate::association_sell_preparation) fn put(
    c: &Connection,
    d: &Delivery,
    p: &mut Readback,
) -> Result<()> {
    let DeliveryEvent::Parent(o) = &d.event else {
        unreachable!()
    };
    ensure!(
        o.issue == o.expected_issue(),
        "unvalidated parent observation tag"
    );
    let was_empty: bool = c.query_row(
        "SELECT NOT EXISTS(SELECT 1 FROM association_parent_blocks)",
        [],
        |r| r.get(0),
    )?;
    let k = key(&o.child)?;
    let value = serde_json::to_string(o)?;
    let old:Option<(String,String,i64,Option<String>)>=c.query_row("SELECT first_observation,first_session,first_sequence,contradiction FROM association_parent_blocks WHERE block_key=?1",[&k],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?;
    let (first, session, seq, conflict) = match old {
        None => {
            let seq = i64::try_from(d.sequence)?;
            c.execute(
                "INSERT INTO association_parent_blocks VALUES(?1,?2,?3,?4,NULL)",
                params![k, value, d.session, seq],
            )?;
            (value, d.session.clone(), seq, None)
        }
        Some((first, session, seq, mut conflict)) => {
            if first != value && conflict.is_none() {
                let evidence = serde_json::to_string(d)?;
                c.execute("UPDATE association_parent_blocks SET contradiction=?2 WHERE block_key=?1 AND contradiction IS NULL",params![k,evidence])?;
                conflict = Some(evidence);
            }
            (first, session, seq, conflict)
        }
    };
    p.expect(
        c,
        ROW,
        vec![k],
        Some(serde_json::to_string(&serde_json::json!([
            first, session, seq, conflict
        ]))?),
    )?;
    for identity in [&o.child, &o.parent] {
        if valid_hash(&identity.hash) {
            hash_identity(c, identity, p)?;
        }
        work::enqueue(c, &identity.hash, p)?;
    }
    if was_empty {
        // Once per graph lifetime, index pre-migration preparations incrementally.
        // All later headers use only indexed hash dependencies.
        super::super::work::initialize(c, p)?;
    }
    Ok(())
}
fn hash_identity(c: &Connection, k: &BlockKey, p: &mut Readback) -> Result<()> {
    let slot = k.slot.to_string();
    let old:Option<(String,Option<String>)>=c.query_row("SELECT first_slot,contradiction_slot FROM association_parent_hashes WHERE block_hash=?1",[&k.hash],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
    let (first, conflict) = match old {
        None => {
            c.execute(
                "INSERT INTO association_parent_hashes VALUES(?1,?2,NULL)",
                params![k.hash, slot],
            )?;
            (slot, None)
        }
        Some((first, mut conflict)) => {
            if first != slot && conflict.is_none() {
                c.execute("UPDATE association_parent_hashes SET contradiction_slot=?2 WHERE block_hash=?1 AND contradiction_slot IS NULL",params![k.hash,slot])?;
                conflict = Some(slot);
            }
            (first, conflict)
        }
    };
    p.expect(
        c,
        HASH,
        vec![k.hash.clone()],
        Some(serde_json::to_string(&serde_json::json!([
            first, conflict
        ]))?),
    )
}
