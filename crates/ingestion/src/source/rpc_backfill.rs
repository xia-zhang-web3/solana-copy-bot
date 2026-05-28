use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::IngestionConfig;
use reqwest::Client;
use serde_json::{json, Value};
use std::collections::HashSet;
use std::time::Duration;
use tokio::time;

use super::{redacted_url_for_log, HeliusWsSource, RawSwapObservation};

const RPC_BACKFILL_MAX_SIGNATURE_LIMIT: usize = 100;
const RPC_BACKFILL_RETRY_ATTEMPTS: usize = 2;
const RPC_BACKFILL_RETRY_BASE_MS: u64 = 250;
const RPC_BACKFILL_SINCE_CLOCK_SKEW_SECONDS: i64 = 5;

#[derive(Debug, Clone, Default)]
pub(crate) struct RpcBackfillRawReport {
    pub(crate) wallets_scanned: usize,
    pub(crate) signatures_seen: usize,
    pub(crate) transactions_fetched: usize,
    pub(crate) rpc_errors: usize,
    pub(crate) observations: Vec<RawSwapObservation>,
}

#[derive(Debug, Clone)]
struct SignatureCandidate {
    signature: String,
    slot: u64,
}

pub(crate) async fn fetch_recent_raw_swaps_for_wallets(
    config: &IngestionConfig,
    wallets: Vec<String>,
    since: DateTime<Utc>,
    signature_limit: usize,
) -> Result<RpcBackfillRawReport> {
    let urls = rpc_http_urls(config);
    if urls.is_empty() {
        return Err(anyhow!(
            "no valid ingestion HTTP URL configured for restart recovery backfill"
        ));
    }

    let client = Client::builder()
        .timeout(Duration::from_millis(
            config.tx_request_timeout_ms.max(1_000),
        ))
        .build()
        .context("failed building restart recovery RPC client")?;
    let interested_program_ids = interested_program_ids(config);
    if interested_program_ids.is_empty() {
        return Err(anyhow!(
            "no interested program ids configured for restart recovery backfill"
        ));
    }
    let raydium_program_ids = config
        .raydium_program_ids
        .iter()
        .cloned()
        .collect::<HashSet<_>>();
    let pumpswap_program_ids = config
        .pumpswap_program_ids
        .iter()
        .cloned()
        .collect::<HashSet<_>>();
    let limit = signature_limit.max(1).min(RPC_BACKFILL_MAX_SIGNATURE_LIMIT);

    let mut report = RpcBackfillRawReport::default();
    let mut seen_signatures = HashSet::new();
    for wallet in wallets {
        report.wallets_scanned = report.wallets_scanned.saturating_add(1);
        let signatures = match fetch_recent_signatures(&client, &urls, &wallet, since, limit).await
        {
            Ok(signatures) => signatures,
            Err(_) => {
                report.rpc_errors = report.rpc_errors.saturating_add(1);
                continue;
            }
        };
        report.signatures_seen = report.signatures_seen.saturating_add(signatures.len());
        for candidate in signatures {
            if !seen_signatures.insert(candidate.signature.clone()) {
                continue;
            }
            report.transactions_fetched = report.transactions_fetched.saturating_add(1);
            match fetch_raw_swap_from_signature(
                &client,
                &urls,
                &candidate,
                &interested_program_ids,
                &raydium_program_ids,
                &pumpswap_program_ids,
            )
            .await
            {
                Ok(Some(raw)) if raw.ts_utc >= since => report.observations.push(raw),
                Ok(_) => {}
                Err(_) => {
                    report.rpc_errors = report.rpc_errors.saturating_add(1);
                }
            }
        }
    }

    report
        .observations
        .sort_by(|a, b| (a.ts_utc, a.slot, &a.signature).cmp(&(b.ts_utc, b.slot, &b.signature)));
    Ok(report)
}

fn rpc_http_urls(config: &IngestionConfig) -> Vec<String> {
    let mut candidates = Vec::new();
    for url in &config.helius_http_urls {
        let trimmed = url.trim();
        if !trimmed.is_empty() && !candidates.iter().any(|existing| existing == trimmed) {
            candidates.push(trimmed.to_string());
        }
    }
    let trimmed = config.helius_http_url.trim();
    if !trimmed.is_empty() && !candidates.iter().any(|existing| existing == trimmed) {
        candidates.push(trimmed.to_string());
    }

    candidates
        .into_iter()
        .filter(|candidate| {
            (candidate.starts_with("http://") || candidate.starts_with("https://"))
                && !candidate.contains("REPLACE_ME")
                && !candidate.contains("REPLACE_ON_SERVER")
        })
        .collect()
}

fn interested_program_ids(config: &IngestionConfig) -> HashSet<String> {
    let mut programs = config
        .yellowstone_program_ids
        .iter()
        .cloned()
        .collect::<HashSet<_>>();
    if programs.is_empty() {
        programs.extend(config.subscribe_program_ids.iter().cloned());
    }
    if programs.is_empty() {
        programs.extend(config.raydium_program_ids.iter().cloned());
        programs.extend(config.pumpswap_program_ids.iter().cloned());
    }
    programs
}

async fn fetch_recent_signatures(
    client: &Client,
    urls: &[String],
    wallet: &str,
    since: DateTime<Utc>,
    limit: usize,
) -> Result<Vec<SignatureCandidate>> {
    let payload = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSignaturesForAddress",
        "params": [
            wallet,
            {
                "limit": limit,
                "commitment": "confirmed"
            }
        ]
    });
    let response = post_rpc_json(client, urls, &payload).await?;
    let result = response
        .get("result")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("missing getSignaturesForAddress result array"))?;
    let since_epoch = since
        .timestamp()
        .saturating_sub(RPC_BACKFILL_SINCE_CLOCK_SKEW_SECONDS);
    let mut out = Vec::new();
    for item in result {
        if item.get("err").map(|err| !err.is_null()).unwrap_or(false) {
            continue;
        }
        if let Some(block_time) = item.get("blockTime").and_then(Value::as_i64) {
            if block_time < since_epoch {
                break;
            }
        }
        let Some(signature) = item.get("signature").and_then(Value::as_str) else {
            continue;
        };
        out.push(SignatureCandidate {
            signature: signature.to_string(),
            slot: item.get("slot").and_then(Value::as_u64).unwrap_or_default(),
        });
    }
    Ok(out)
}

async fn fetch_raw_swap_from_signature(
    client: &Client,
    urls: &[String],
    candidate: &SignatureCandidate,
    interested_program_ids: &HashSet<String>,
    raydium_program_ids: &HashSet<String>,
    pumpswap_program_ids: &HashSet<String>,
) -> Result<Option<RawSwapObservation>> {
    let payload = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTransaction",
        "params": [
            candidate.signature,
            {
                "encoding": "jsonParsed",
                "commitment": "confirmed",
                "maxSupportedTransactionVersion": 0
            }
        ]
    });
    let response = post_rpc_json(client, urls, &payload).await?;
    if response.get("error").is_some() {
        return Ok(None);
    }
    let Some(result) = response.get("result").filter(|value| !value.is_null()) else {
        return Ok(None);
    };
    raw_observation_from_transaction_result(
        &candidate.signature,
        candidate.slot,
        result,
        interested_program_ids,
        raydium_program_ids,
        pumpswap_program_ids,
    )
}

fn raw_observation_from_transaction_result(
    signature: &str,
    slot_hint: u64,
    result: &Value,
    interested_program_ids: &HashSet<String>,
    raydium_program_ids: &HashSet<String>,
    pumpswap_program_ids: &HashSet<String>,
) -> Result<Option<RawSwapObservation>> {
    let Some(meta) = result.get("meta").filter(|value| !value.is_null()) else {
        return Ok(None);
    };
    if meta.get("err").map(|err| !err.is_null()).unwrap_or(false) {
        return Ok(None);
    }

    let account_keys = HeliusWsSource::extract_account_keys(result);
    if account_keys.is_empty() {
        return Ok(None);
    }
    let signer_index = account_keys
        .iter()
        .position(|(_, is_signer)| *is_signer)
        .unwrap_or(0);
    let Some((signer, _)) = account_keys.get(signer_index) else {
        return Ok(None);
    };
    if signer.is_empty() {
        return Ok(None);
    }

    let logs = HeliusWsSource::value_to_string_vec(meta.get("logMessages")).unwrap_or_default();
    let mut program_ids = HeliusWsSource::extract_program_ids(result, meta, &logs);
    if program_ids.is_empty() {
        program_ids.extend(interested_program_ids.iter().cloned());
    }
    if !program_ids
        .iter()
        .any(|program| interested_program_ids.contains(program))
    {
        return Ok(None);
    }

    let Some((token_in, amount_in, token_out, amount_out)) =
        HeliusWsSource::infer_swap_from_json_balances(meta, signer_index, signer)
    else {
        return Ok(None);
    };
    let block_time = result.get("blockTime").and_then(Value::as_i64);
    let ts_utc = block_time
        .and_then(|ts| DateTime::<Utc>::from_timestamp(ts, 0))
        .unwrap_or_else(Utc::now);
    let slot = result
        .get("slot")
        .and_then(Value::as_u64)
        .unwrap_or(slot_hint);
    let dex_hint = HeliusWsSource::detect_dex_hint(
        &program_ids,
        &logs,
        raydium_program_ids,
        pumpswap_program_ids,
    );

    Ok(Some(RawSwapObservation {
        signature: signature.to_string(),
        slot,
        signer: signer.clone(),
        token_in,
        token_out,
        amount_in: amount_in.amount,
        amount_out: amount_out.amount,
        exact_amounts: HeliusWsSource::build_exact_swap_amounts(&amount_in, &amount_out),
        program_ids: program_ids.into_iter().collect(),
        dex_hint,
        ts_utc,
    }))
}

async fn post_rpc_json(client: &Client, urls: &[String], payload: &Value) -> Result<Value> {
    let mut last_error = None;
    for attempt in 0..RPC_BACKFILL_RETRY_ATTEMPTS {
        let url = &urls[attempt % urls.len()];
        match post_rpc_json_once(client, url, payload).await {
            Ok(value) => return Ok(value),
            Err(error) => {
                last_error = Some(error);
                time::sleep(Duration::from_millis(
                    RPC_BACKFILL_RETRY_BASE_MS.saturating_mul((attempt as u64) + 1),
                ))
                .await;
            }
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow!("restart recovery RPC request failed")))
}

async fn post_rpc_json_once(client: &Client, url: &str, payload: &Value) -> Result<Value> {
    let redacted_url = redacted_url_for_log(url);
    let response = client
        .post(url)
        .json(payload)
        .send()
        .await
        .with_context(|| format!("failed restart recovery RPC POST via {redacted_url}"))?;
    let status = response.status();
    if status.as_u16() == 429 || status.is_server_error() {
        return Err(anyhow!(
            "retryable restart recovery RPC status {status} via {redacted_url}"
        ));
    }
    if !status.is_success() {
        return Err(anyhow!(
            "non-success restart recovery RPC status {status} via {redacted_url}"
        ));
    }
    response
        .json::<Value>()
        .await
        .with_context(|| format!("failed parsing restart recovery RPC json via {redacted_url}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn token_balance(owner: &str, mint: &str, amount: &str) -> Value {
        let raw_amount = amount
            .parse::<u64>()
            .expect("token balance fixture must be an integer ui amount")
            .saturating_mul(1_000_000)
            .to_string();
        json!({
            "owner": owner,
            "mint": mint,
            "uiTokenAmount": {
                "uiAmountString": amount,
                "amount": raw_amount,
                "decimals": 6
            }
        })
    }

    #[test]
    fn raw_backfill_parser_recovers_sell_swap_from_get_transaction_result() -> Result<()> {
        let signer = "Leader111111111111111111111111111111111";
        let result = json!({
            "slot": 42,
            "blockTime": 1_700_000_000i64,
            "transaction": {
                "message": {
                    "accountKeys": [
                        {"pubkey": signer, "signer": true}
                    ],
                    "instructions": [
                        {"programId": "raydium-program"}
                    ]
                }
            },
            "meta": {
                "err": null,
                "preBalances": [1_000_000_000u64],
                "postBalances": [2_000_000_000u64],
                "preTokenBalances": [token_balance(signer, "TokenMintA", "100")],
                "postTokenBalances": [token_balance(signer, "TokenMintA", "0")],
                "logMessages": []
            }
        });
        let interested = HashSet::from([String::from("raydium-program")]);
        let raydium = HashSet::from([String::from("raydium-program")]);
        let raw = raw_observation_from_transaction_result(
            "sig-recovered-sell",
            0,
            &result,
            &interested,
            &raydium,
            &HashSet::new(),
        )?
        .expect("sell swap should parse");

        assert_eq!(raw.signature, "sig-recovered-sell");
        assert_eq!(raw.slot, 42);
        assert_eq!(raw.signer, signer);
        assert_eq!(raw.token_in, "TokenMintA");
        assert_eq!(raw.token_out, "So11111111111111111111111111111111111111112");
        assert!((raw.amount_in - 100.0).abs() < 1e-9);
        assert!((raw.amount_out - 1.0).abs() < 1e-9);
        assert_eq!(raw.dex_hint, "raydium");
        Ok(())
    }

    #[test]
    fn rpc_http_urls_drop_placeholders_and_non_http_urls() {
        let mut config = IngestionConfig::default();
        config.helius_http_url = "https://rpc.example.com/?api-key=secret".to_string();
        config.helius_http_urls = vec![
            "wss://not-http.example.com".to_string(),
            "https://YOUR_QUICKNODE_HOST.solana-mainnet.quiknode.pro/REPLACE_ON_SERVER/"
                .to_string(),
        ];

        assert_eq!(
            rpc_http_urls(&config),
            vec!["https://rpc.example.com/?api-key=secret".to_string()]
        );
    }
}
