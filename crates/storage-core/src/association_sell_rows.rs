use super::*;
use rusqlite::{params, OptionalExtension};
use std::collections::BTreeMap;

const ROW:&str="SELECT json_array(signature,version,first_binding,initial_evaluation,latest_evaluation,authority) FROM association_sell_preparations WHERE signature=?1";
const DEP:&str="SELECT first_identity FROM association_sell_dependencies WHERE sell_signature=?1 AND anchor_signature=?2";
const DEP_PROOF:&str="SELECT json_array(sell_signature,anchor_signature,first_identity) FROM association_sell_dependencies WHERE sell_signature=?1 AND anchor_signature=?2";
/// Exact precommit and postcommit readback of every changed protocol key.
#[derive(Default)]
pub(crate) struct Readback {
    expected: BTreeMap<(String, Vec<String>), Option<String>>,
    mode: crate::association_inbox::ConsumerMode,
}
impl Readback {
    pub(crate) fn new(mode: crate::association_inbox::ConsumerMode) -> Self {
        Self {
            expected: BTreeMap::new(),
            mode,
        }
    }
    pub(crate) fn automatic(&self) -> bool {
        self.mode == crate::association_inbox::ConsumerMode::ProviderOrderStrictV1
    }
    pub(crate) fn expect(
        &mut self,
        c: &Connection,
        q: &str,
        args: Vec<String>,
        expected: Option<String>,
    ) -> Result<()> {
        let actual: Option<String> = c
            .query_row(q, rusqlite::params_from_iter(&args), |r| r.get(0))
            .optional()?;
        ensure!(
            actual == expected,
            "SELL preparation write ignored/changed: {q}"
        );
        self.expected.insert((q.into(), args), expected);
        Ok(())
    }
    pub(crate) fn verify(&self, c: &Connection) -> Result<()> {
        for ((q, args), expected) in &self.expected {
            let actual: Option<String> = c
                .query_row(q, rusqlite::params_from_iter(args), |r| r.get(0))
                .optional()?;
            ensure!(
                &actual == expected,
                "SELL preparation committed readback mismatch"
            );
        }
        Ok(())
    }
}
pub(super) fn load(
    c: &Connection,
    s: &str,
) -> Result<Option<(FirstBinding, Evaluation, Evaluation)>> {
    let row:Option<(u8,String,String,String,String)>=c.query_row("SELECT version,first_binding,initial_evaluation,latest_evaluation,authority FROM association_sell_preparations WHERE signature=?1",[s],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).optional()?;
    row.map(|(version, b, i, l, a)| {
        let b: FirstBinding = serde_json::from_str(&b)?;
        ensure!(
            version == 1
                && b.version == 1
                && b.sell.admission.facts.signature == s
                && a == "trade_authority_none",
            "corrupt SELL binding identity"
        );
        Ok((b, serde_json::from_str(&i)?, serde_json::from_str(&l)?))
    })
    .transpose()
}
fn check_row(
    c: &Connection,
    b: &FirstBinding,
    i: &str,
    e: &str,
    proof: &mut Readback,
) -> Result<()> {
    let s = &b.sell.admission.facts.signature;
    proof.expect(
        c,
        ROW,
        vec![s.clone()],
        Some(serde_json::to_string(&serde_json::json!([
            s,
            1,
            serde_json::to_string(b)?,
            i,
            e,
            "trade_authority_none"
        ]))?),
    )
}
fn dependencies(c: &Connection, b: &FirstBinding, new: bool, proof: &mut Readback) -> Result<()> {
    let s = &b.sell.admission.facts.signature;
    for key in evaluate::keys(b) {
        let a = evaluate::anchor(c, &key)?;
        let current = a.identity.as_ref().map(serde_json::to_string).transpose()?;
        let old: Option<Option<String>> =
            c.query_row(DEP, params![s, key], |r| r.get(0)).optional()?;
        let expected = match old {
            None => {
                ensure!(new, "missing SELL dependency");
                c.execute("INSERT INTO association_sell_dependencies(sell_signature,anchor_signature,first_identity) VALUES(?1,?2,?3)",params![s,key,current])?;
                current
            }
            Some(None) => {
                if current.is_some() {
                    c.execute("UPDATE association_sell_dependencies SET first_identity=?3 WHERE sell_signature=?1 AND anchor_signature=?2 AND first_identity IS NULL",params![s,key,current])?;
                }
                current
            }
            Some(Some(old)) => Some(old),
        };
        proof.expect(
            c,
            DEP_PROOF,
            vec![s.clone(), key.clone()],
            Some(serde_json::to_string(&serde_json::json!([
                s, key, expected
            ]))?),
        )?;
    }
    Ok(())
}
pub(super) fn prepare(
    c: &Connection,
    s: &str,
    fresh: bool,
    observed: Option<(i64, u32)>,
    l: InboxLimits,
    proof: &mut Readback,
) -> Result<()> {
    if load(c, s)?.is_some() {
        return refresh(c, s, l, proof);
    }
    let sell = crate::association_inbox::identity(c, s)?.context("missing SELL inbox identity")?;
    if sell.admission.facts.token_out != SOL || sell.admission.facts.token_in == SOL {
        return Ok(());
    }
    let b = financial::first(c, &sell, fresh, observed, l)?;
    dependencies(c, &b, true, proof)?;
    let evaluation = evaluate::evaluate(c, &b, l)?;
    parent_graph::bind(c, s, &evaluation.parent_dependencies, proof)?;
    let e = serde_json::to_string(&evaluation)?;
    c.execute("INSERT INTO association_sell_preparations(signature,version,first_binding,initial_evaluation,latest_evaluation) VALUES(?1,1,?2,?3,?3)",params![s,serde_json::to_string(&b)?,e])?;
    check_row(c, &b, &e, &e, proof)?;
    super::automatic::after_refresh(c, s, &evaluation, l, proof)
}
pub(super) fn refresh(c: &Connection, s: &str, l: InboxLimits, proof: &mut Readback) -> Result<()> {
    let (b, i, _) = load(c, s)?.context("dependency without SELL preparation")?;
    dependencies(c, &b, false, proof)?;
    let evaluation = evaluate::evaluate(c, &b, l)?;
    parent_graph::bind(c, s, &evaluation.parent_dependencies, proof)?;
    let e = serde_json::to_string(&evaluation)?;
    c.execute(
        "UPDATE association_sell_preparations SET latest_evaluation=?2 WHERE signature=?1",
        params![s, e],
    )?;
    check_row(c, &b, &serde_json::to_string(&i)?, &e, proof)?;
    super::automatic::after_refresh(c, s, &evaluation, l, proof)
}
