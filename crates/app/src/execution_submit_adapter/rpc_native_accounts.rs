use super::rpc_native_instructions::{self as ix, number, text};
use copybot_core_types::SignedLamports;
use copybot_storage_core::{
    ExecutionCanaryReceiptFacts, NativeAccountObservation, NativeAccountObservations,
    NativeObservation as Obs, NativeTokenEndpoint as End, ObservationCoverage as Cov,
    ObservationSource as Src, MAX_NATIVE_ACCOUNTS,
};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
const WSOL: &str = "So11111111111111111111111111111111111111112";
const MAX_TOKEN_ROWS: usize = 256;
fn token_identity(end: &End) -> bool {
    [
        &end.mint,
        &end.token_owner,
        &end.token_program,
        &end.decimals,
    ]
    .iter()
    .all(|v| v.coverage == Cov::Known)
}
fn rows(
    result: &Value,
    name: &str,
    keys: usize,
    out: &mut NativeAccountObservations,
    candidates: &mut BTreeMap<u32, BTreeSet<String>>,
) -> BTreeMap<u32, End> {
    let mut mapped = BTreeMap::new();
    let Some(rows) = result["meta"][name].as_array() else {
        out.accounts_coverage = if result["meta"].get(name).is_none_or(Value::is_null) {
            Cov::Missing
        } else {
            Cov::Invalid
        };
        out.note("token_rows_unavailable");
        return mapped;
    };
    for row in rows.iter().take(MAX_TOKEN_ROWS) {
        let Some(index) = row["accountIndex"]
            .as_u64()
            .filter(|i| *i < (keys as u64))
            .and_then(|i| u32::try_from(i).ok())
        else {
            out.accounts_coverage = Cov::Invalid;
            out.note("token_row_index_invalid");
            continue;
        };
        for (mint, reason) in [(out.token.as_str(), "target_mint"), (WSOL, "wsol_mint")] {
            if row["mint"] == mint {
                candidates.entry(index).or_default().insert(reason.into());
            }
        }
        let mut end = End {
            mint: text(row.get("mint"), Src::RpcTokenBalance),
            token_owner: text(row.get("owner"), Src::RpcTokenBalance),
            token_program: text(row.get("programId"), Src::RpcTokenBalance),
            decimals: number(row.pointer("/uiTokenAmount/decimals"), Src::RpcTokenBalance),
            raw: Obs::unknown(Cov::Missing),
        };
        // Token raw amounts must be canonical decimal strings; JSON numbers do not qualify.
        end.raw = match row.pointer("/uiTokenAmount/amount") {
            Some(Value::String(_)) => {
                number(row.pointer("/uiTokenAmount/amount"), Src::RpcTokenBalance)
            }
            None | Some(Value::Null) => Obs::unknown(Cov::Missing),
            _ => Obs::unknown(Cov::Invalid),
        };
        if end
            .decimals
            .value
            .as_ref()
            .is_some_and(|d| d.parse::<u8>().is_err())
        {
            end.decimals = Obs::unknown(Cov::Invalid);
        }
        if end
            .token_program
            .value
            .as_deref()
            .is_some_and(|p| !matches!(p, ix::TOKEN | ix::TOKEN_2022))
        {
            end.token_program = Obs::unknown(Cov::Unsupported);
        }
        if mapped.contains_key(&index) {
            end = End::unknown(Cov::Invalid);
            out.accounts_coverage = Cov::Invalid;
            out.note("token_row_duplicate");
        }
        mapped.insert(index, end);
    }
    if rows.len() > MAX_TOKEN_ROWS {
        out.accounts_coverage = Cov::Truncated;
        out.note("token_rows_truncated");
    }
    mapped
}
fn lifecycle_zero(result: &Value, index: u32, opposite: &End, creation: bool) -> Option<End> {
    let wallet = opposite.token_owner.value.as_deref()?;
    let mint = opposite.mint.value.as_deref()?;
    opposite.raw.value.as_ref()?;
    opposite.decimals.value.as_ref()?;
    super::rpc_receipt_lifecycle::validate_missing_row(
        result,
        index as usize,
        wallet,
        mint,
        opposite.token_program.value.as_deref(),
        creation,
    )
    .ok()?;
    let mut end = opposite.clone();
    for v in [
        &mut end.mint,
        &mut end.token_owner,
        &mut end.token_program,
        &mut end.decimals,
    ] {
        v.source = Src::ProvenLifecycle;
    }
    end.raw = Obs::known("0", Src::ProvenLifecycle);
    Some(end)
}
pub(super) fn collect(
    result: &Value,
    facts: &ExecutionCanaryReceiptFacts,
) -> NativeAccountObservations {
    let mut out = NativeAccountObservations::empty(facts);
    ix::collect(result, &mut out);
    let keys = result["transaction"]["message"]["accountKeys"]
        .as_array()
        .unwrap();
    let mut candidates = BTreeMap::new();
    let pre = rows(
        result,
        "preTokenBalances",
        keys.len(),
        &mut out,
        &mut candidates,
    );
    let post = rows(
        result,
        "postTokenBalances",
        keys.len(),
        &mut out,
        &mut candidates,
    );
    // At this point account coverage reflects token-row evidence only. Later
    // funding selection can be partial without invalidating a neighbor's rows.
    let lifecycle_rows_complete = out.accounts_coverage == Cov::Known;
    let links = ix::linked_accounts(&out);
    let mut funding = ix::funding_accounts(&out);
    for (index, key) in keys.iter().enumerate() {
        let pubkey = key["pubkey"].as_str().unwrap();
        let role_link = links.contains(pubkey);
        let funding_link = funding.remove(pubkey);
        let endpoint_index = u32::try_from(index).ok();
        let endpoints = [
            endpoint_index.and_then(|i| pre.get(&i)),
            endpoint_index.and_then(|i| post.get(&i)),
        ];
        let proven_funding = funding_link && endpoints.iter().flatten().any(|e| token_identity(e));
        if funding_link && !proven_funding && !role_link {
            // No token row is also not proof that a SOL destination is a token
            // account. Keep selection unknown instead of inventing an account.
            if out.accounts_coverage == Cov::Known {
                out.accounts_coverage = endpoints
                    .iter()
                    .flatten()
                    .flat_map(|e| [&e.mint, &e.token_owner, &e.token_program, &e.decimals])
                    .map(|v| v.coverage)
                    .find(|c| *c != Cov::Known)
                    .unwrap_or(Cov::Missing);
            }
            out.note("funding_token_identity_unproven");
        }
        if role_link || proven_funding {
            if let Ok(index) = u32::try_from(index) {
                candidates
                    .entry(index)
                    .or_default()
                    .insert("wallet_instruction_link".into());
            } else {
                out.accounts_coverage = Cov::Truncated;
                out.note("account_index_domain_overflow");
            }
        }
    }
    if !funding.is_empty() {
        if out.accounts_coverage == Cov::Known {
            out.accounts_coverage = Cov::Invalid;
        }
        out.note("funding_account_key_unavailable");
    }
    for (index, reasons) in candidates {
        if out.accounts.len() == MAX_NATIVE_ACCOUNTS {
            out.accounts_coverage = Cov::Truncated;
            out.note("native_accounts_truncated");
            break;
        }
        let pubkey = keys[index as usize]["pubkey"].as_str().unwrap();
        if pubkey.len() > 128 {
            out.accounts_coverage = Cov::Invalid;
            out.note("account_key_bounds");
            continue;
        }
        let mut before = pre
            .get(&index)
            .cloned()
            .unwrap_or_else(|| End::unknown(Cov::Missing));
        let mut after = post
            .get(&index)
            .cloned()
            .unwrap_or_else(|| End::unknown(Cov::Missing));
        // Missing arrays are not missing rows; truncated/duplicate data cannot
        // justify a lifecycle zero. Reuse the strict pre-existing lifecycle proof.
        if lifecycle_rows_complete {
            if !pre.contains_key(&index) {
                if let Some(e) = lifecycle_zero(result, index, &after, true) {
                    before = e;
                }
            }
            if !post.contains_key(&index) {
                if let Some(e) = lifecycle_zero(result, index, &before, false) {
                    after = e;
                }
            }
        }
        let relevance = reasons.into_iter().collect();
        let native_pre = result["meta"]["preBalances"][index as usize]
            .as_u64()
            .unwrap();
        let native_post = result["meta"]["postBalances"][index as usize]
            .as_u64()
            .unwrap();
        out.accounts.push(NativeAccountObservation {
            account_index: index,
            pubkey: pubkey.into(),
            native_pre: Obs::known(native_pre, Src::RpcNativeBalance),
            native_post: Obs::known(native_post, Src::RpcNativeBalance),
            native_delta: Obs::known(
                SignedLamports::new(i128::from(native_post) - i128::from(native_pre)).as_i128(),
                Src::RpcNativeBalance,
            ),
            pre_token: before,
            post_token: after,
            relevance,
        });
    }
    if out.accounts_coverage == Cov::Known {
        let partial = out
            .accounts
            .iter()
            .flat_map(|a| [&a.pre_token, &a.post_token])
            .flat_map(|e| {
                [
                    &e.mint,
                    &e.token_owner,
                    &e.token_program,
                    &e.decimals,
                    &e.raw,
                ]
            })
            .map(|o| o.coverage)
            .find(|c| *c != Cov::Known);
        if let Some(coverage) = partial {
            out.accounts_coverage = coverage;
            out.note("token_account_fields_partial");
        }
    }
    // Instruction coverage also bounds the relevant account set: a later known
    // instruction may link an additional token account to the configured wallet.
    if out.accounts_coverage == Cov::Known && out.instructions_coverage != Cov::Known {
        out.accounts_coverage = out.instructions_coverage;
        out.note("account_links_partial");
    }
    // Individual field/count caps bound work; the serialized bundle has its own
    // hard cap. A bounded subset is explicitly partial, never silently complete.
    while serde_json::to_vec(&out)
        .is_ok_and(|v| v.len() > copybot_storage_core::MAX_NATIVE_OBSERVATION_BYTES)
    {
        if out.instructions.pop().is_some() {
            out.instructions_coverage = Cov::Truncated;
        } else if out.accounts.pop().is_some() {
            out.accounts_coverage = Cov::Truncated;
        } else {
            break;
        }
        out.note("observation_bytes_truncated");
    }
    out
}
