use super::{attribute, key, Attribution, Instruction, Row, View};
use serde_json::Value;
use std::collections::HashSet;
#[path = "native_attribution_parsed.rs"]
mod parsed;

pub(in crate::source) fn infer(
    result: &Value,
    meta: &Value,
    signer: &str,
    programs: &HashSet<String>,
) -> Attribution {
    let keys: Vec<_> = result
        .pointer("/transaction/message/accountKeys")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|v| {
            v.as_str()
                .or_else(|| v.get("pubkey")?.as_str())
                .and_then(key)
        })
        .collect();
    let instructions = |v: &Value| -> Option<Vec<Instruction>> {
        Some(
            v.as_array()?
                .iter()
                .map(|ix| instruction(ix, &keys))
                .collect(),
        )
    };
    let view = View {
        successful: meta.get("err") == Some(&Value::Null),
        owned_sol: rows(meta.get("preTokenBalances"), Some(signer))
            .zip(rows(meta.get("postTokenBalances"), Some(signer))),
        first_signer: result
            .pointer("/transaction/message/accountKeys")
            .and_then(Value::as_array)
            .and_then(|keys| {
                keys.iter()
                    .find(|k| k.get("signer").and_then(Value::as_bool) == Some(true))
            })
            .and_then(|k| k.get("pubkey")?.as_str())
            .and_then(key),
        top: result
            .pointer("/transaction/message/instructions")
            .and_then(instructions),
        inner: meta
            .get("innerInstructions")
            .and_then(Value::as_array)
            .and_then(|groups| {
                groups
                    .iter()
                    .map(|g| {
                        Some((
                            usize::try_from(g.get("index")?.as_u64()?).ok()?,
                            instructions(g.get("instructions")?)?,
                        ))
                    })
                    .collect()
            }),
        pre: balances(meta.get("preBalances")),
        post: balances(meta.get("postBalances")),
        pre_tokens: rows(meta.get("preTokenBalances"), None),
        post_tokens: rows(meta.get("postTokenBalances"), None),
        keys,
    };
    attribute(&view, signer, programs)
}

fn balances(value: Option<&Value>) -> Option<Vec<u64>> {
    value?.as_array()?.iter().map(Value::as_u64).collect()
}

fn instruction(ix: &Value, keys: &[Option<String>]) -> Instruction {
    let program = ix.get("programId").and_then(Value::as_str).and_then(key);
    let mut result = Instruction {
        program,
        accounts: None,
        data: None,
        depth: ix.get("stackHeight").filter(|v| !v.is_null()).map(|v| {
            v.as_u64()
                .and_then(|n| u32::try_from(n).ok())
                .unwrap_or(u32::MAX)
        }),
    };
    if ix.get("programIdIndex").is_some() {
        return result;
    }
    if !result
        .program
        .as_ref()
        .is_some_and(|p| keys.iter().any(|k| k.as_ref() == Some(p)))
    {
        return result;
    }
    if ix.get("parsed").is_some() {
        // A hybrid has no single authoritative representation, even if one arm parses.
        if ix.get("data").is_some() || ix.get("accounts").is_some() {
            return result;
        }
        if let Some((accounts, data)) = parsed::decode(ix) {
            result.accounts = accounts.iter().map(|a| position(a, keys)).collect();
            result.data = Some(data);
        }
    } else {
        result.accounts = ix
            .get("accounts")
            .and_then(Value::as_array)
            .and_then(|a| a.iter().map(|v| position(v.as_str()?, keys)).collect());
        result.data = ix
            .get("data")
            .and_then(Value::as_str)
            .filter(|s| s.len() <= 2048)
            .and_then(|s| bs58::decode(s).into_vec().ok());
    }
    result
}

fn position(key: &str, keys: &[Option<String>]) -> Option<usize> {
    let mut matches = keys
        .iter()
        .enumerate()
        .filter(|(_, k)| k.as_deref() == Some(key));
    let (index, _) = matches.next()?;
    matches.next().is_none().then_some(index)
}

fn rows(value: Option<&Value>, only_owned_sol: Option<&str>) -> Option<Vec<Row>> {
    value?
        .as_array()?
        .iter()
        .filter(|r| {
            only_owned_sol.is_none_or(|signer| {
                r.get("owner").and_then(Value::as_str) == Some(signer)
                    && r.get("mint").and_then(Value::as_str) == Some(super::SOL_MINT)
            })
        })
        .map(|r| {
            let amount = r.get("uiTokenAmount")?;
            let (raw, decimals) = super::wire::amount(
                amount.get("amount")?.as_str()?,
                amount.get("decimals")?.as_u64()?,
                amount
                    .get("uiAmountString")
                    .filter(|v| !v.is_null())
                    .map(|v| v.as_str())
                    .transpose_option()?,
                amount
                    .get("uiAmount")
                    .filter(|v| !v.is_null())
                    .map(|v| v.as_f64())
                    .transpose_option()?,
            )?;
            Some(Row {
                index: usize::try_from(r.get("accountIndex")?.as_u64()?).ok()?,
                mint: key(r.get("mint")?.as_str()?)?,
                owner: key(r.get("owner")?.as_str()?)?,
                program: key(r.get("programId")?.as_str()?)?,
                raw,
                decimals,
            })
        })
        .collect()
}

trait TransposeOption<T> {
    fn transpose_option(self) -> Option<Option<T>>;
}
impl<T> TransposeOption<T> for Option<Option<T>> {
    fn transpose_option(self) -> Option<Option<T>> {
        match self {
            None => Some(None),
            Some(v) => v.map(Some),
        }
    }
}
