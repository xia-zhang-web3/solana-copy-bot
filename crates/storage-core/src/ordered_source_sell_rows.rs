use super::*;
use anyhow::ensure;
use rusqlite::{params, Connection, OptionalExtension};

pub(crate) fn load(c: &Connection, id: &str) -> Result<Option<OrderedSourceSellIntent>> {
    let row: Option<(String, u8, String, String)> = c.query_row(
        "SELECT signature,version,policy,record FROM ordered_source_sell_intents WHERE intent_id=?1",
        [id], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
    ).optional()?;
    row.map(|(signature, version, policy, wire)| {
        let intent: OrderedSourceSellIntent = serde_json::from_str(&wire)?;
        ensure!(
            version == 1
                && intent.version == 1
                && policy == PROVIDER_ORDER_STRICT_V1
                && intent.policy == policy
                && id == canonical(&signature)
                && intent.intent_id == id
                && intent.first.version == 1
                && intent.first.sell.admission.facts.signature == signature
                && intent.staged_evaluation.trade_authority == "trade_authority_none"
                && ownership::owner(c, &signature)?.as_deref() == Some(PROVIDER_ORDER_STRICT_V1),
            "corrupt ordered SELL identity/version/claim"
        );
        ensure!(
            policy::evidence(&intent.first, &intent.staged_evaluation)?
                == OrderedSellDecision::ValidatedNow,
            "corrupt ordered SELL staged evidence"
        );
        Ok(intent)
    })
    .transpose()
}
pub(super) fn insert(c: &Connection, i: &OrderedSourceSellIntent) -> Result<()> {
    let wire = serde_json::to_string(i)?;
    let changed = c.execute(
        "INSERT INTO ordered_source_sell_intents(intent_id,signature,version,policy,record) VALUES (?1,?2,?3,?4,?5)",
        params![i.intent_id, i.first.sell.admission.facts.signature, i.version, i.policy, wire],
    )?;
    ensure!(
        changed == 1 && load(c, &i.intent_id)?.as_ref() == Some(i),
        "ordered SELL insert ignored/changed"
    );
    Ok(())
}
