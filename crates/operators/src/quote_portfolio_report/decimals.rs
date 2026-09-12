//! Exact decimal assertions and optional canonical observed-leg metadata binding.
//! No UI/raw ratio, price, RPC, or leader quantity enters the quote operands.
use super::input::{raw, DecimalsSource, QuoteRef, Side};
use anyhow::{ensure, Context, Result};
use copybot_storage_core::SqliteStore;
use serde::{Deserialize, Deserializer};
use serde_json::{json, Value};

const SOL_DECIMALS: u8 = 9;

// Derive's deserialize_struct accepts positional arrays too. Require maps at
// JSON object boundaries without first buffering into Value (which loses duplicates).
fn object<'de, D: Deserializer<'de>, T: Deserialize<'de>>(d: D) -> Result<T, D::Error> {
    struct Object<T>(std::marker::PhantomData<T>);
    impl<'de, T: Deserialize<'de>> serde::de::Visitor<'de> for Object<T> {
        type Value = T;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("quote binding JSON object")
        }
        fn visit_map<A: serde::de::MapAccess<'de>>(self, map: A) -> Result<T, A::Error> {
            T::deserialize(serde::de::value::MapAccessDeserializer::new(map))
        }
    }
    d.deserialize_map(Object(std::marker::PhantomData))
}

pub fn parse(text: &str) -> serde_json::Result<Response> {
    let mut d = serde_json::Deserializer::from_str(text);
    let response = object(&mut d)?;
    d.end()?;
    Ok(response)
}

// A present null is malformed, not an absent assertion. Typed structs also
// reject duplicate supported fields/containers before any source can be used.
#[derive(Default)]
enum Assertion {
    #[default]
    Missing,
    Exact(u8, bool),
}
impl<'de> Deserialize<'de> for Assertion {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let value = Value::deserialize(d)?;
        let numeric = value.is_number();
        let n = match value {
            Value::Number(n) => n.as_u64(),
            Value::String(s) => raw(&s).ok(),
            _ => None,
        }
        .filter(|&n| n <= 19)
        .ok_or_else(|| {
            serde::de::Error::custom("malformed decimals assertion; expected exact 0..19")
        })?;
        Ok(Self::Exact(n as u8, numeric))
    }
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Meta {
    #[serde(default)]
    in_decimals: Assertion,
    #[serde(default)]
    out_decimals: Assertion,
    #[serde(default)]
    input_decimals: Assertion,
    #[serde(default)]
    output_decimals: Assertion,
}
#[derive(Default, Deserialize)]
struct Token {
    #[serde(default)]
    decimals: Assertion,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Response {
    pub input_mint: String,
    pub output_mint: String,
    pub in_amount: String,
    pub out_amount: String,
    #[serde(default)]
    input_decimals: Assertion,
    #[serde(default)]
    output_decimals: Assertion,
    #[serde(default)]
    in_decimals: Assertion,
    #[serde(default)]
    out_decimals: Assertion,
    #[serde(default, deserialize_with = "object")]
    meta: Meta,
    #[serde(default, deserialize_with = "object")]
    input_token: Token,
    #[serde(default, deserialize_with = "object")]
    output_token: Token,
}

pub fn resolve(store: &SqliteStore, q: &QuoteRef, r: &Response) -> Result<Value> {
    let (input, output) = match q.side {
        Side::Buy => (SOL_DECIMALS, q.decimals),
        Side::Sell => (q.decimals, SOL_DECIMALS),
    };
    let mut assertions = Vec::new();
    for (path, assertion, expected) in [
        ("inputDecimals", &r.input_decimals, input),
        ("outputDecimals", &r.output_decimals, output),
        ("inDecimals", &r.in_decimals, input),
        ("outDecimals", &r.out_decimals, output),
        ("meta.inDecimals", &r.meta.in_decimals, input),
        ("meta.outDecimals", &r.meta.out_decimals, output),
        ("meta.inputDecimals", &r.meta.input_decimals, input),
        ("meta.outputDecimals", &r.meta.output_decimals, output),
        ("inputToken.decimals", &r.input_token.decimals, input),
        ("outputToken.decimals", &r.output_token.decimals, output),
    ] {
        if let Assertion::Exact(value, _) = assertion {
            ensure!(*value == expected, "response decimals conflict at {path}");
            assertions.push(json!({"field":path,"decimals":value.to_string()}));
        }
    }
    let mut evidence = match &q.decimals_source {
        None => {
            ensure!(
                matches!(r.input_decimals, Assertion::Exact(_, true))
                    && matches!(r.output_decimals, Assertion::Exact(_, true)),
                "strict root response decimals missing; no explicit metadata source"
            );
            json!({"source":"strict_root_response","fields":["inputDecimals","outputDecimals"]})
        }
        Some(DecimalsSource::ObservedSignature { signature }) => observed(store, q, signature)?,
    };
    evidence["response_assertions"] = json!(assertions);
    evidence["token_decimals"] = json!(q.decimals.to_string());
    evidence["quote_side"] = json!(match q.side {
        Side::Buy => "buy",
        Side::Sell => "sell",
    });
    evidence["sol_decimals"] =
        json!({"source":"protocol_constant","decimals":SOL_DECIMALS.to_string()});
    Ok(evidence)
}

fn observed(store: &SqliteStore, q: &QuoteRef, signature: &str) -> Result<Value> {
    ensure!(
        !signature.trim().is_empty() && !signature.contains(':') && !q.wallet_id.contains(':'),
        "invalid observed signature/canonical identity component"
    );
    // resolve() in binding.rs already verified this reference against the quote
    // row, including signal_id/wallet/mint. Never parse a partial signal suffix.
    let id = q
        .signal_id
        .as_deref()
        .context("quote signal reference missing for decimals")?;
    let signal = store
        .load_copy_signal_by_signal_id(id)
        .context("read-only decimals signal lookup/schema unavailable")?
        .context("decimals source signal row missing")?;
    ensure!(signal.signal_id == id, "decimals signal identity mismatch");
    ensure!(
        signal.wallet_id == q.wallet_id,
        "decimals signal wallet mismatch"
    );
    ensure!(signal.token == q.mint, "decimals signal mint mismatch");
    ensure!(
        matches!(signal.side.as_str(), "buy" | "sell"),
        "decimals signal side unsupported"
    );
    ensure!(
        q.side != Side::Buy || signal.side == "buy",
        "BUY quote requires a BUY metadata signal"
    );
    ensure!(
        id == format!(
            "shadow:{signature}:{}:{}:{}",
            signal.wallet_id, signal.side, signal.token
        ),
        "decimals canonical signal/signature mismatch"
    );
    let leg = store
        .load_execution_canary_observed_leg_by_signature(signature)
        .context("read-only decimals observed lookup/schema unavailable")?
        .context("decimals observed leg missing")?;
    ensure!(
        leg.signature == signature,
        "decimals observed signature mismatch"
    );
    ensure!(
        leg.wallet_id == signal.wallet_id,
        "decimals observed wallet mismatch"
    );
    ensure!(
        leg.token_mint == signal.token,
        "decimals observed mint mismatch"
    );
    let metadata_side = if leg.is_buy { "buy" } else { "sell" };
    ensure!(
        metadata_side == signal.side,
        "decimals observed/signal side mismatch"
    );
    let decimals = leg
        .token_decimals
        .context("exact observed token_decimals missing; UI/raw is not evidence")?;
    ensure!(
        decimals == q.decimals,
        "observed token_decimals differs from caller expectation"
    );
    Ok(
        json!({"source":"observed_signature","signature":leg.signature,
        "field":if leg.is_buy {"observed_swaps.qty_out_decimals"} else {"observed_swaps.qty_in_decimals"},
        "signal_id":signal.signal_id,"wallet_id":signal.wallet_id,"mint":signal.token,
        "metadata_source_side":metadata_side,"binding":"quote.signal_id -> canonical stored copy_signal -> observed leg",
        "scope":"mint metadata only; not source SELL, leader size, costs, coverage, or freshness proof"}),
    )
}
