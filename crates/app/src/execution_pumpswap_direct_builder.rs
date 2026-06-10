use crate::execution_build_plan_age::quote_age_ms_summary_field;
use crate::execution_pumpswap_accounts::{
    associated_token_address, decode_global_config_account, decode_pool_account, format_pubkey,
    global_config_pda, parse_pubkey, wsol_mint,
};
use crate::execution_pumpswap_direct_instructions::{
    build_buy_with_sol_instructions, build_sell_for_sol_instructions, PumpSwapInstructionInputs,
};
use crate::execution_quote_canary_helpers::truncate_for_log;
use crate::execution_route_plan::route_plan_has_pump_fun_amm;
use crate::execution_solana_tx::{serialize_unsigned_legacy_transaction, PubkeyBytes};
use crate::execution_submit_adapter::ExecutionTransactionPlan;
use crate::execution_swap_transaction_http::SwapTransactionDryRunResult;
use crate::execution_transaction_rpc_simulation::verify_serialized_transaction_rpc_simulation;
use anyhow::{anyhow, Context, Result};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use copybot_config::ExecutionConfig;
use serde_json::{json, Value};
use std::time::{Duration as StdDuration, Instant};

const PUMPSWAP_DIRECT_SOURCE: &str = "pumpswap_direct";

pub(crate) async fn fetch_pumpswap_direct_buy_transaction_dry_run(
    http: &reqwest::Client,
    config: &ExecutionConfig,
    plan: &ExecutionTransactionPlan,
) -> Result<Option<SwapTransactionDryRunResult>> {
    if !plan.side.eq_ignore_ascii_case("buy") {
        return Ok(None);
    }
    fetch_pumpswap_direct_transaction_dry_run(http, config, plan).await
}

pub(crate) async fn fetch_pumpswap_direct_transaction_dry_run(
    http: &reqwest::Client,
    config: &ExecutionConfig,
    plan: &ExecutionTransactionPlan,
) -> Result<Option<SwapTransactionDryRunResult>> {
    if !config.swap_transaction_dry_run_enabled || !should_try_pumpswap_direct(plan) {
        return Ok(None);
    }
    let timeout = StdDuration::from_millis(config.quote_canary_timeout_ms.max(1));
    let started = Instant::now();
    let rpc = PumpSwapRpc::new(http, config, timeout)?;
    let side = plan.side.to_ascii_lowercase();
    let transaction = match side.as_str() {
        "buy" => build_pumpswap_direct_buy_transaction(&rpc, plan).await?,
        "sell" => build_pumpswap_direct_sell_transaction(&rpc, plan).await?,
        _ => return Ok(None),
    };
    verify_serialized_transaction_rpc_simulation(
        http,
        config,
        &transaction.serialized_transaction_base64,
        PUMPSWAP_DIRECT_SOURCE,
        timeout,
    )
    .await
    .map_err(|error| anyhow!("{error}{}", quote_age_ms_summary_field(&plan.metadata)))?;
    let summary = format!(
        "pumpswap_direct_{}_transaction_ok base64_len={} serialized_transaction_base64_ready=true latency_ms={} rpc_simulation=passed{}",
        side,
        transaction.serialized_transaction_base64.len(),
        started.elapsed().as_millis(),
        quote_age_ms_summary_field(&plan.metadata)
    );
    Ok(Some(SwapTransactionDryRunResult {
        summary: truncate_for_log(&summary, 500),
        serialized_transaction_base64: transaction.serialized_transaction_base64,
        source: PUMPSWAP_DIRECT_SOURCE.to_string(),
    }))
}

fn should_try_pumpswap_direct(plan: &ExecutionTransactionPlan) -> bool {
    (plan.side.eq_ignore_ascii_case("buy") || plan.side.eq_ignore_ascii_case("sell"))
        && route_plan_has_pump_fun_amm(plan.metadata.route_plan_json.as_deref())
}

struct PumpSwapDirectTransaction {
    serialized_transaction_base64: String,
}

async fn build_pumpswap_direct_buy_transaction(
    rpc: &PumpSwapRpc<'_>,
    plan: &ExecutionTransactionPlan,
) -> Result<PumpSwapDirectTransaction> {
    let blueprint = plan
        .swap_blueprint
        .as_ref()
        .ok_or_else(|| anyhow!("missing swap blueprint for PumpSwap direct builder"))?;
    if parse_pubkey(&blueprint.input_mint, "input_mint")? != wsol_mint() {
        anyhow::bail!("PumpSwap direct buy only supports SOL input");
    }
    let pool_key = extract_pumpswap_amm_key(plan)?;
    let user = parse_pubkey(&plan.wallet_pubkey, "wallet_pubkey")?;
    let expected_output = parse_pubkey(&blueprint.output_mint, "output_mint")?;
    let amount_in = blueprint
        .input_amount_raw
        .parse::<u64>()
        .context("invalid PumpSwap direct input amount")?;
    let min_output = output_threshold(plan)?;
    let [global_config_account, pool_account] = rpc
        .fetch_accounts([global_config_pda(), pool_key])
        .await
        .context("fetch PumpSwap global config and pool accounts")?;
    let global_config = decode_global_config_account(&global_config_account.data)?;
    let pool = decode_pool_account(&pool_account.data)?;
    if pool.base_mint != wsol_mint() {
        anyhow::bail!("PumpSwap direct buy only supports pools with SOL base mint");
    }
    if pool.quote_mint != expected_output {
        anyhow::bail!("PumpSwap direct pool quote mint does not match quote output");
    }
    let [base_mint_account, quote_mint_account] = rpc
        .fetch_accounts([pool.base_mint, pool.quote_mint])
        .await
        .context("fetch PumpSwap mint owners")?;
    let base_token_program = base_mint_account.owner;
    let quote_token_program = quote_mint_account.owner;
    let user_base_ata = associated_token_address(&user, &pool.base_mint, &base_token_program);
    let user_quote_ata = associated_token_address(&user, &pool.quote_mint, &quote_token_program);
    let blockhash = rpc.fetch_latest_blockhash().await?;
    let instructions = build_buy_with_sol_instructions(PumpSwapInstructionInputs {
        user,
        pool_key,
        pool_account_len: pool_account.data.len(),
        pool,
        global_config,
        base_token_program,
        quote_token_program,
        user_base_ata,
        user_quote_ata,
        amount_in,
        min_output,
        priority_fee_lamports: blueprint.priority_fee_lamports,
    });
    let raw = serialize_unsigned_legacy_transaction(user, blockhash, &instructions)?;
    Ok(PumpSwapDirectTransaction {
        serialized_transaction_base64: BASE64_STANDARD.encode(raw),
    })
}

async fn build_pumpswap_direct_sell_transaction(
    rpc: &PumpSwapRpc<'_>,
    plan: &ExecutionTransactionPlan,
) -> Result<PumpSwapDirectTransaction> {
    let blueprint = plan
        .swap_blueprint
        .as_ref()
        .ok_or_else(|| anyhow!("missing swap blueprint for PumpSwap direct builder"))?;
    if parse_pubkey(&blueprint.output_mint, "output_mint")? != wsol_mint() {
        anyhow::bail!("PumpSwap direct sell only supports SOL output");
    }
    let pool_key = extract_pumpswap_amm_key(plan)?;
    let user = parse_pubkey(&plan.wallet_pubkey, "wallet_pubkey")?;
    let expected_input = parse_pubkey(&blueprint.input_mint, "input_mint")?;
    let amount_in = blueprint
        .input_amount_raw
        .parse::<u64>()
        .context("invalid PumpSwap direct input amount")?;
    let min_output = output_threshold(plan)?;
    let [global_config_account, pool_account] = rpc
        .fetch_accounts([global_config_pda(), pool_key])
        .await
        .context("fetch PumpSwap global config and pool accounts")?;
    let global_config = decode_global_config_account(&global_config_account.data)?;
    let pool = decode_pool_account(&pool_account.data)?;
    if pool.base_mint != wsol_mint() {
        anyhow::bail!("PumpSwap direct sell only supports pools with SOL base mint");
    }
    if pool.quote_mint != expected_input {
        anyhow::bail!("PumpSwap direct pool quote mint does not match sell input");
    }
    let [base_mint_account, quote_mint_account] = rpc
        .fetch_accounts([pool.base_mint, pool.quote_mint])
        .await
        .context("fetch PumpSwap mint owners")?;
    let base_token_program = base_mint_account.owner;
    let quote_token_program = quote_mint_account.owner;
    let user_base_ata = associated_token_address(&user, &pool.base_mint, &base_token_program);
    let user_quote_ata = associated_token_address(&user, &pool.quote_mint, &quote_token_program);
    let blockhash = rpc.fetch_latest_blockhash().await?;
    let instructions = build_sell_for_sol_instructions(PumpSwapInstructionInputs {
        user,
        pool_key,
        pool_account_len: pool_account.data.len(),
        pool,
        global_config,
        base_token_program,
        quote_token_program,
        user_base_ata,
        user_quote_ata,
        amount_in,
        min_output,
        priority_fee_lamports: blueprint.priority_fee_lamports,
    });
    let raw = serialize_unsigned_legacy_transaction(user, blockhash, &instructions)?;
    Ok(PumpSwapDirectTransaction {
        serialized_transaction_base64: BASE64_STANDARD.encode(raw),
    })
}

fn extract_pumpswap_amm_key(plan: &ExecutionTransactionPlan) -> Result<PubkeyBytes> {
    for raw in [
        plan.metadata.route_plan_json.as_deref(),
        plan.metadata.quote_response_json.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        if let Some(key) = extract_amm_key_from_json(raw)? {
            return parse_pubkey(&key, "PumpSwap ammKey");
        }
    }
    anyhow::bail!("PumpSwap direct builder missing route ammKey")
}

fn extract_amm_key_from_json(raw: &str) -> Result<Option<String>> {
    let value: Value = serde_json::from_str(raw)
        .map_err(|error| anyhow!("invalid PumpSwap route JSON: {error}"))?;
    let route_plan = value
        .as_array()
        .or_else(|| value.get("routePlan").and_then(Value::as_array));
    let Some(items) = route_plan else {
        return Ok(None);
    };
    Ok(items.iter().find_map(|item| {
        let swap = item.get("swapInfo")?;
        let label = swap.get("label").and_then(Value::as_str)?;
        if !label.eq_ignore_ascii_case("Pump.fun Amm") {
            return None;
        }
        swap.get("ammKey")
            .and_then(Value::as_str)
            .map(str::to_string)
    }))
}

fn output_threshold(plan: &ExecutionTransactionPlan) -> Result<u64> {
    if let Some(raw) = plan
        .metadata
        .quote_response_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
        .and_then(|value| {
            value
                .get("otherAmountThreshold")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
    {
        return raw
            .parse::<u64>()
            .context("invalid PumpSwap direct otherAmountThreshold");
    }
    let out_amount = plan
        .metadata
        .quote_out_amount_raw
        .as_deref()
        .ok_or_else(|| anyhow!("missing PumpSwap direct output amount"))?
        .parse::<u128>()
        .context("invalid PumpSwap direct output amount")?;
    let keep_bps = 10_000_u128.saturating_sub(u128::from(plan.slippage_tolerance_bps.min(10_000)));
    u64::try_from((out_amount.saturating_mul(keep_bps)) / 10_000)
        .context("PumpSwap direct output threshold overflow")
}

struct PumpSwapRpc<'a> {
    http: &'a reqwest::Client,
    rpc_url: String,
    timeout: StdDuration,
}

impl<'a> PumpSwapRpc<'a> {
    fn new(
        http: &'a reqwest::Client,
        config: &ExecutionConfig,
        timeout: StdDuration,
    ) -> Result<Self> {
        let rpc_url = config.submit_adapter_http_url.trim();
        if rpc_url.is_empty() {
            anyhow::bail!("PumpSwap direct builder requires submit_adapter_http_url");
        }
        Ok(Self {
            http,
            rpc_url: rpc_url.to_string(),
            timeout,
        })
    }

    async fn fetch_accounts<const N: usize>(
        &self,
        keys: [PubkeyBytes; N],
    ) -> Result<[RpcAccount; N]> {
        let key_strings = keys.iter().map(format_pubkey).collect::<Vec<_>>();
        let request = json!({
            "jsonrpc": "2.0",
            "id": "execution-pumpswap-direct-get-multiple-accounts",
            "method": "getMultipleAccounts",
            "params": [
                key_strings,
                {"encoding": "base64", "commitment": "confirmed"}
            ],
        });
        let value = self.rpc_json(request).await?;
        let accounts = value
            .pointer("/result/value")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("PumpSwap direct getMultipleAccounts missing result"))?;
        if accounts.len() != N {
            anyhow::bail!("PumpSwap direct getMultipleAccounts length mismatch");
        }
        let parsed = accounts
            .iter()
            .enumerate()
            .map(|(index, account)| {
                account
                    .as_object()
                    .ok_or_else(|| {
                        anyhow!("PumpSwap direct account {} not found", key_strings[index])
                    })
                    .and_then(|_| decode_rpc_account(account, &key_strings[index]))
            })
            .collect::<Result<Vec<_>>>()?;
        parsed
            .try_into()
            .map_err(|_| anyhow!("PumpSwap direct account conversion failed"))
    }

    async fn fetch_latest_blockhash(&self) -> Result<PubkeyBytes> {
        let request = json!({
            "jsonrpc": "2.0",
            "id": "execution-pumpswap-direct-latest-blockhash",
            "method": "getLatestBlockhash",
            "params": [{"commitment": "confirmed"}],
        });
        let value = self.rpc_json(request).await?;
        let blockhash = value
            .pointer("/result/value/blockhash")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("PumpSwap direct latest blockhash missing"))?;
        parse_pubkey(blockhash, "latest blockhash")
    }

    async fn rpc_json(&self, request: Value) -> Result<Value> {
        let response = self
            .http
            .post(&self.rpc_url)
            .timeout(self.timeout)
            .json(&request)
            .send()
            .await
            .map_err(|error| anyhow!("PumpSwap direct RPC request failed: {error}"))?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|error| anyhow!("PumpSwap direct RPC body read failed: {error}"))?;
        if !status.is_success() {
            return Err(anyhow!(
                "PumpSwap direct RPC returned HTTP {status}: {}",
                truncate_for_log(&body, 240)
            ));
        }
        let value: Value = serde_json::from_str(&body)
            .map_err(|error| anyhow!("PumpSwap direct RPC JSON decode failed: {error}"))?;
        if let Some(error) = value.get("error") {
            return Err(anyhow!(
                "PumpSwap direct RPC error: {}",
                truncate_for_log(&error.to_string(), 240)
            ));
        }
        Ok(value)
    }
}

#[derive(Debug, Clone)]
struct RpcAccount {
    owner: PubkeyBytes,
    data: Vec<u8>,
}

fn decode_rpc_account(value: &Value, key: &str) -> Result<RpcAccount> {
    let owner = value
        .get("owner")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("PumpSwap direct account {key} missing owner"))
        .and_then(|owner| parse_pubkey(owner, "account owner"))?;
    let raw_data = value
        .get("data")
        .and_then(Value::as_array)
        .and_then(|items| items.first())
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("PumpSwap direct account {key} missing base64 data"))?;
    let data = BASE64_STANDARD
        .decode(raw_data)
        .map_err(|error| anyhow!("PumpSwap direct account {key} base64 decode failed: {error}"))?;
    Ok(RpcAccount { owner, data })
}
