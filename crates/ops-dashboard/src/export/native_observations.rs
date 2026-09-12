use super::io::InputReport;
use serde_json::{json, Map, Value};

fn exact(v: &Value, signed: bool) -> bool {
    v.as_str().is_some_and(|s| {
        if signed {
            s.parse::<i128>().is_ok_and(|n| n.to_string() == s)
        } else {
            s.parse::<u64>().is_ok_and(|n| n.to_string() == s)
        }
    })
}
fn select(v: &Value, keys: &[&str]) -> Value {
    Value::Object(
        keys.iter()
            .filter_map(|k| v.get(*k).map(|v| ((*k).into(), v.clone())))
            .collect(),
    )
}
fn observation(v: &Value, numeric: bool, signed: bool) -> Option<Value> {
    let coverage = v["coverage"].as_str()?;
    let source = v["source"].as_str()?;
    if !matches!(
        coverage,
        "known" | "missing" | "invalid" | "unsupported" | "truncated"
    ) || !matches!(
        source,
        "rpc_account_key"
            | "rpc_native_balance"
            | "rpc_token_balance"
            | "parsed_instruction"
            | "proven_lifecycle"
            | "unavailable"
    ) {
        return None;
    }
    if coverage == "known" {
        if source == "unavailable"
            || !v["value"]
                .as_str()
                .is_some_and(|s| !s.is_empty() && s.len() <= 128)
            || (numeric && !exact(&v["value"], signed))
        {
            return None;
        }
    } else if !v["value"].is_null() || source != "unavailable" {
        return None;
    }
    Some(select(v, &["value", "coverage", "source"]))
}
fn bounded_strings(v: &Value, limit: usize) -> bool {
    v.as_array().is_some_and(|a| {
        a.len() <= limit && a.iter().all(|v| v.as_str().is_some_and(|s| s.len() <= 96))
    })
}
fn bundle(v: &Value) -> Option<Value> {
    for key in [
        "order_id",
        "tx_signature",
        "wallet_pubkey",
        "token",
        "side",
        "accounts_coverage",
        "instructions_coverage",
    ] {
        if !v[key].as_str().is_some_and(|s| s.len() <= 256) {
            return None;
        }
    }
    if !exact(&v["slot"], false) || !bounded_strings(&v["reasons"], 16) {
        return None;
    }
    let accounts = v["accounts"].as_array()?;
    let instructions = v["instructions"].as_array()?;
    if accounts.len() > 64 || instructions.len() > 64 {
        return None;
    }
    let mut aout = Vec::new();
    let mut iout = Vec::new();
    for a in accounts {
        if !a["account_index"]
            .as_u64()
            .is_some_and(|i| u32::try_from(i).is_ok())
            || !a["pubkey"].as_str().is_some_and(|s| s.len() <= 128)
            || !bounded_strings(&a["relevance"], 3)
        {
            return None;
        }
        let mut row = select(a, &["account_index", "pubkey", "relevance"]);
        for key in ["native_pre", "native_post", "native_delta"] {
            row[key] = observation(&a[key], true, key == "native_delta")?;
        }
        for side in ["pre_token", "post_token"] {
            let mut end = Map::new();
            for key in ["mint", "token_owner", "token_program", "decimals", "raw"] {
                end.insert(
                    key.into(),
                    observation(&a[side][key], matches!(key, "decimals" | "raw"), false)?,
                );
            }
            row[side] = Value::Object(end);
        }
        aout.push(row);
    }
    for i in instructions {
        if !i["outer_index"]
            .as_u64()
            .is_some_and(|n| u32::try_from(n).is_ok())
            || !(i["inner_index"].is_null()
                || i["inner_index"]
                    .as_u64()
                    .is_some_and(|n| u32::try_from(n).is_ok()))
        {
            return None;
        }
        let mut row = select(i, &["outer_index", "inner_index", "coverage"]);
        for key in ["stack_height", "program_id", "instruction_type"] {
            row[key] = observation(&i[key], key == "stack_height", false)?;
        }
        let fields = i["fields"].as_object()?;
        if fields.len() > 16 {
            return None;
        }
        let mut fout = Map::new();
        for (key, v) in fields {
            if !matches!(
                key.as_str(),
                "source"
                    | "sourceBase"
                    | "sourceOwner"
                    | "destination"
                    | "account"
                    | "newAccount"
                    | "base"
                    | "owner"
                    | "authority"
                    | "newAuthority"
                    | "authorityType"
                    | "mint"
                    | "wallet"
                    | "tokenProgram"
                    | "lamports"
                    | "amount"
                    | "decimals"
                    | "space"
            ) {
                return None;
            }
            fout.insert(
                key.clone(),
                observation(
                    v,
                    matches!(key.as_str(), "lamports" | "amount" | "decimals" | "space"),
                    false,
                )?,
            );
        }
        row["fields"] = Value::Object(fout);
        iout.push(row);
    }
    let mut result = select(
        v,
        &[
            "order_id",
            "tx_signature",
            "wallet_pubkey",
            "token",
            "side",
            "slot",
            "accounts_coverage",
            "instructions_coverage",
            "reasons",
        ],
    );
    result["accounts"] = json!(aout);
    result["instructions"] = json!(iout);
    Some(result)
}
fn sanitized(v: &Value) -> Option<Value> {
    for key in [
        "total_orders",
        "covered_orders",
        "partial_orders",
        "uncovered_orders",
        "conflict_orders",
        "account_rows",
        "instruction_rows",
    ] {
        if !exact(&v[key], false) {
            return None;
        }
    }
    for key in [
        "since",
        "as_of",
        "source_basis",
        "window_basis",
        "coverage",
        "decomposition",
        "ordering",
    ] {
        if !v[key].as_str().is_some_and(|s| s.len() <= 128) {
            return None;
        }
    }
    let rows = v["rows"].as_array()?;
    if rows.len() > 100 {
        return None;
    }
    let mut out = Vec::new();
    for row in rows {
        for key in ["order_id", "operation_at", "side", "coverage"] {
            if !row[key].as_str().is_some_and(|s| s.len() <= 256) {
                return None;
            }
        }
        if !(row["reason"].is_null() || row["reason"].as_str().is_some_and(|s| s.len() <= 128)) {
            return None;
        }
        let mut r = select(
            row,
            &["order_id", "operation_at", "side", "coverage", "reason"],
        );
        r["observations"] = if row["observations"].is_null() {
            Value::Null
        } else {
            bundle(&row["observations"])?
        };
        out.push(r);
    }
    let mut r = select(
        v,
        &[
            "since",
            "as_of",
            "source_basis",
            "window_basis",
            "coverage",
            "decomposition",
            "ordering",
            "total_orders",
            "covered_orders",
            "partial_orders",
            "uncovered_orders",
            "conflict_orders",
            "account_rows",
            "instruction_rows",
            "rows_truncated",
        ],
    );
    r["rows"] = json!(out);
    Some(r)
}
pub(super) fn report(input: &InputReport) -> Value {
    input.value.as_ref().and_then(|v|v.pointer("/tiny_execution_proof/native_observations")).and_then(sanitized)
        .unwrap_or_else(||json!({"coverage":"uncovered","source_basis":"unavailable","decomposition":"unresolved","rows":[]}))
}
