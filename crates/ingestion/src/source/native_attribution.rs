//! Bounded temporary WSOL attribution. Missing witness is terminal, not native cash.
use std::collections::HashSet;

use super::SOL_MINT;
#[path = "native_attribution_ata.rs"]
mod ata;
#[path = "native_attribution_json.rs"]
pub(super) mod json;
#[path = "native_attribution_lifecycle.rs"]
mod lifecycle;
#[path = "native_attribution_pda.rs"]
mod pda;
#[path = "native_attribution_proto.rs"]
pub(super) mod proto;
#[path = "native_attribution_wire.rs"]
pub(super) mod wire;
#[path = "native_attribution_witness.rs"]
mod witness;

pub(super) const ATA: &str = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";
pub(super) const TOKEN: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
pub(super) const SYSTEM: &str = "11111111111111111111111111111111";
const TOKEN_2022: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";

pub(super) enum Attribution {
    Known(Trade),
    Unknown,
    NotApplicable,
}

pub(super) struct Trade {
    pub buy: bool,
    pub target: String,
    pub sol_raw: u64,
    pub target_raw: u64,
    pub target_decimals: u8,
}

pub(super) struct View {
    pub keys: Vec<Option<String>>,
    pub first_signer: Option<String>,
    pub successful: bool,
    pub owned_sol: Option<(Vec<Row>, Vec<Row>)>,
    pub top: Option<Vec<Instruction>>,
    pub inner: Option<Vec<(usize, Vec<Instruction>)>>,
    pub pre: Option<Vec<u64>>,
    pub post: Option<Vec<u64>>,
    pub pre_tokens: Option<Vec<Row>>,
    pub post_tokens: Option<Vec<Row>>,
}

#[derive(Clone)]
pub(super) struct Instruction {
    pub program: Option<String>,
    pub accounts: Option<Vec<usize>>,
    pub data: Option<Vec<u8>>,
    pub depth: Option<u32>,
}

pub(super) struct Row {
    pub index: usize,
    pub mint: String,
    pub owner: String,
    pub program: String,
    pub raw: u64,
    pub decimals: u8,
}

pub(super) fn key(value: &str) -> Option<String> {
    (bs58::decode(value).into_vec().ok()?.len() == 32).then(|| value.to_owned())
}

impl View {
    fn key(&self, index: usize) -> Option<&str> {
        self.keys.get(index)?.as_deref()
    }
    fn row<'a>(&self, rows: &'a [Row], index: usize) -> Option<&'a Row> {
        let mut found = rows.iter().filter(|r| r.index == index);
        let row = found.next()?;
        found.next().is_none().then_some(row)
    }
    fn pair(&self, index: usize, owner: &str, mint: &str) -> Option<(&Row, &Row)> {
        let pre = self.row(self.pre_tokens.as_ref()?, index)?;
        let post = self.row(self.post_tokens.as_ref()?, index)?;
        [pre, post]
            .iter()
            .all(|r| r.owner == owner && r.mint == mint && r.program == TOKEN)
            .then_some(())?;
        (pre.decimals == post.decimals).then_some((pre, post))
    }
    fn persistent_delta(&self, signer: &str) -> Option<i128> {
        let mut delta = 0i128;
        let (sol_pre, sol_post) = self.owned_sol.as_ref()?;
        if sol_pre.len() != sol_post.len() {
            return None;
        }
        for row in sol_pre {
            if row.owner != signer || row.mint != SOL_MINT {
                continue;
            }
            let pre = self.row(sol_pre, row.index)?;
            let post = self.row(sol_post, row.index)?;
            self.key(row.index)?;
            if pre.program != TOKEN || post.program != TOKEN || pre.decimals != post.decimals {
                return None;
            }
            if pre.decimals != 9 {
                return None;
            }
            let change = i128::from(post.raw) - i128::from(pre.raw);
            if change != 0 {
                if *self.pre.as_ref()?.get(row.index)? == 0
                    || *self.post.as_ref()?.get(row.index)? == 0
                {
                    return None;
                }
                delta = delta.checked_add(change)?;
            }
        }
        Some(delta)
    }
}

pub(super) fn attribute(v: &View, signer: &str, programs: &HashSet<String>) -> Attribution {
    let Some(top) = &v.top else {
        return Attribution::Unknown;
    };
    // Select by configured program and supported raw parent, never lifecycle presence.
    let parents: Vec<_> = top
        .iter()
        .enumerate()
        .filter(|(_, ix)| {
            ix.program.as_ref().is_some_and(|p| programs.contains(p))
                && ix.data.as_deref().and_then(wire::swap).is_some()
        })
        .collect();
    if parents.is_empty() {
        return Attribution::NotApplicable;
    }
    if v.persistent_delta(signer).is_some_and(|delta| delta != 0) {
        return Attribution::NotApplicable;
    }
    // An explicitly different base route remains owned by the accepted inference.
    let outside = |ix: &Instruction| -> bool {
        let Some(a) = &ix.accounts else {
            return false;
        };
        a.get(3)
            .and_then(|i| v.key(*i))
            .is_some_and(|mint| mint != SOL_MINT)
            || [11, 12]
                .iter()
                .any(|s| a.get(*s).and_then(|i| v.key(*i)) == Some(TOKEN_2022))
            || a.get(5).is_some_and(|i| {
                v.pre
                    .as_ref()
                    .and_then(|b| b.get(*i))
                    .zip(v.post.as_ref().and_then(|b| b.get(*i)))
                    .is_some_and(|(pre, post)| {
                        (*pre != 0 || *post != 0)
                            && v.pair(*i, signer, SOL_MINT)
                                .is_some_and(|(a, b)| a.decimals == 9 && b.decimals == 9)
                    })
            })
    };
    if parents.iter().all(|(_, ix)| outside(ix)) {
        return Attribution::NotApplicable;
    }
    if parents.len() != 1 {
        return Attribution::Unknown;
    }
    let (index, parent) = parents[0];
    witness::prove(v, signer, programs, index, parent)
        .map(Attribution::Known)
        .unwrap_or(Attribution::Unknown)
}

impl Trade {
    pub(in crate::source) fn legs(self) -> (String, u64, u8, String, u64, u8) {
        if self.buy {
            (
                SOL_MINT.to_owned(),
                self.sol_raw,
                9,
                self.target,
                self.target_raw,
                self.target_decimals,
            )
        } else {
            (
                self.target,
                self.target_raw,
                self.target_decimals,
                SOL_MINT.to_owned(),
                self.sol_raw,
                9,
            )
        }
    }
}
