use super::open_risk_sell_fixture::{SOL, TOKEN};
use super::priority_fee_fixture::transaction;
use anyhow::{bail, ensure, Context, Result};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub(super) type Trace = Arc<Mutex<Vec<(String, Value)>>>;

fn supported_mint(mint: &str) -> bool {
    // Queue's second synthetic owned position uses this deterministic mint.
    mint == TOKEN || mint == bs58::encode([21u8; 32]).into_string()
}

pub(super) async fn serve(
    listener: tokio::net::TcpListener,
    payer: [u8; 32],
    price: u64,
    calls: Trace,
    receipt: Arc<Mutex<Option<Value>>>,
    responses: Trace,
) -> Result<()> {
    loop {
        let (mut stream, _) = listener.accept().await?;
        let (path, body) = read_request(&mut stream).await?;
        {
            let mut calls = calls.lock().unwrap();
            ensure!(calls.len() < 1024, "RPC fixture request budget exhausted");
            calls.push((path.clone(), body.clone()));
        }
        let response = if path.starts_with("GET /quote?") {
            ensure!(body.is_null(), "quote body must be empty");
            quote_response(&path)?
        } else if path == "POST /swap-instructions HTTP/1.1" && body.is_object() {
            if body["quoteResponse"]["outputMint"] == SOL {
                super::generic_sell_synthetic_fixture::bundle(payer, 200_000, price)
            } else {
                json!({"computeBudgetInstructions":[],"setupInstructions":[],"swapInstruction":{},"instructions":[{"programId":"synthetic"}],"simulationError":null})
            }
        } else if path == "POST /swap HTTP/1.1" && body.is_object() {
            json!({"swapTransaction":transaction(payer, 200_000, price),"simulationError":null})
        } else {
            ensure!(
                path == "POST / HTTP/1.1",
                "unexpected loopback endpoint: {path}"
            );
            rpc_response(&body, &receipt)?
        };
        let text = response.to_string();
        stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{text}", text.len()).as_bytes()).await?;
        responses.lock().unwrap().push((path, response));
    }
}

fn quote_response(path: &str) -> Result<Value> {
    let url = reqwest::Url::parse(&format!(
        "http://localhost{}",
        path.split_whitespace()
            .nth(1)
            .context("missing quote URL")?
    ))?;
    ensure!(url.path() == "/quote", "unexpected quote endpoint");
    let pairs: Vec<_> = url.query_pairs().collect();
    let field = |name| -> Result<&str> {
        let matches: Vec<_> = pairs.iter().filter(|(k, _)| k == name).collect();
        ensure!(matches.len() == 1, "quote requires one {name}");
        Ok(matches[0].1.as_ref())
    };
    let input = field("inputMint")?;
    let output = field("outputMint")?;
    let raw = field("amount")?;
    ensure!(
        !raw.is_empty() && raw.bytes().all(|b| b.is_ascii_digit()),
        "invalid quote amount"
    );
    let amount: u64 = raw.parse().context("invalid quote amount")?;
    ensure!(amount > 0, "zero quote amount");
    let (out, minimum) = if supported_mint(input) && output == SOL {
        (amount.checked_mul(10000), amount.checked_mul(9500))
    } else if input == SOL && supported_mint(output) {
        // 3-decimal token at 0.01 SOL: 10000 lamports per raw token unit.
        let out = amount / 10000;
        (Some(out), out.checked_mul(95).map(|v| v / 100))
    } else {
        bail!("unsupported quote pair: {input} -> {output}");
    };
    let out = out.context("quote amount overflow")?;
    let minimum = minimum.context("quote amount overflow")?;
    ensure!(
        out > 0 && minimum > 0,
        "quote amount below fixture precision"
    );
    Ok(
        json!({"inputMint":input,"outputMint":output,"inAmount":amount.to_string(),
        "outAmount":out.to_string(),"otherAmountThreshold":minimum.to_string(),
        "swapMode":"ExactIn","slippageBps":if output == SOL {field("slippageBps")?.parse::<u64>()?} else {500},"priceImpactPct":"0",
        "routePlan":[{"swapInfo":{"label":"Metis"}}]}),
    )
}

fn rpc_response(body: &Value, receipt: &Arc<Mutex<Option<Value>>>) -> Result<Value> {
    ensure!(
        body["jsonrpc"] == "2.0" && (body["id"].is_string() || body["id"].is_number()),
        "malformed RPC envelope"
    );
    let method = body["method"].as_str().context("missing RPC method")?;
    ensure!(
        body["params"].is_array()
            || (method == "qn_estimatePriorityFees" && body["params"].is_object()),
        "malformed RPC params"
    );
    let result = match method {
        "getTokenSupply" => {
            let params = body["params"]
                .as_array()
                .context("invalid getTokenSupply params")?;
            ensure!(params.len() == 1, "getTokenSupply requires one mint");
            let mint = params[0]
                .as_str()
                .context("getTokenSupply mint must be a string")?;
            ensure!(
                supported_mint(mint),
                "unsupported getTokenSupply mint: {mint}"
            );
            json!({"context":{"slot":1},"value":{"amount":"1000000","decimals":3,"uiAmount":1000,"uiAmountString":"1000"}})
        }
        "qn_estimatePriorityFees" => json!({"recommended":600000}),
        "getTokenAccountsByOwner" => {
            json!({"value":[{"account":{"data":{"parsed":{"info":{"tokenAmount":{"amount":"7000","decimals":3}}}}}}]})
        }
        "simulateTransaction" => {
            json!({"context":{"slot":42},"value":{"err":null,"logs":[],"unitsConsumed":1}})
        }
        "sendTransaction" => json!("synthetic-open-risk-sell"),
        "getSignatureStatuses" => json!({"value":[null]}),
        "getTransaction" => {
            if body["params"][0] == "prior-synthetic-tx" {
                receipt.lock().unwrap().clone().unwrap_or(Value::Null)
            } else {
                Value::Null
            }
        }
        other => bail!("unexpected loopback RPC: {other}"),
    };
    Ok(json!({"jsonrpc":"2.0","id":body["id"],"result":result}))
}

async fn read_request(stream: &mut tokio::net::TcpStream) -> Result<(String, Value)> {
    const MAX_BYTES: usize = 64 * 1024;
    let mut bytes = Vec::new();
    loop {
        let mut buf = [0; 8192];
        let count = stream.read(&mut buf).await?;
        ensure!(count > 0, "incomplete HTTP request");
        bytes.extend_from_slice(&buf[..count]);
        ensure!(
            bytes.len() <= MAX_BYTES,
            "HTTP request exceeds fixture bound"
        );
        if let Some(end) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
            let header = std::str::from_utf8(&bytes[..end])?;
            let path = header.lines().next().context("missing HTTP request line")?;
            let parts: Vec<_> = path.split_whitespace().collect();
            ensure!(
                parts.len() == 3 && parts[2] == "HTTP/1.1",
                "malformed HTTP request line"
            );
            let mut length = None;
            for line in header.lines().skip(1) {
                let (key, value) = line.split_once(':').context("malformed HTTP header")?;
                ensure!(
                    !key.eq_ignore_ascii_case("transfer-encoding"),
                    "unsupported HTTP encoding"
                );
                if key.eq_ignore_ascii_case("content-length") {
                    ensure!(length.is_none(), "duplicate HTTP content-length");
                    length = Some(
                        value
                            .trim()
                            .parse::<usize>()
                            .context("invalid HTTP content-length")?,
                    );
                }
            }
            ensure!(
                parts[0] != "POST" || length.is_some(),
                "POST requires content-length"
            );
            let length = length.unwrap_or(0);
            ensure!(
                length <= MAX_BYTES - end - 4,
                "HTTP body exceeds fixture bound"
            );
            if bytes.len() >= end + 4 + length {
                ensure!(
                    bytes.len() == end + 4 + length,
                    "unexpected trailing HTTP data"
                );
                return Ok((
                    path.into(),
                    if length == 0 {
                        Value::Null
                    } else {
                        serde_json::from_slice(&bytes[end + 4..]).context("malformed HTTP JSON")?
                    },
                ));
            }
        }
    }
}
