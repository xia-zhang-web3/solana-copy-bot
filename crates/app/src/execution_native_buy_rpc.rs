//! Bound external evidence for the versioned native BUY fence and finalized source.
//! The durable admission and financial decision are checked by storage-core.
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::{ExecutionConfig, PROCESSED_SLOT_FENCE_AVAILABILITY_V1};
use serde_json::{json, Value};
use crate::execution_owned_sell_rpc::fractional::transport::Transport;

const SPL_TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
static ACQUISITION: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);

#[derive(Debug, Clone)]
pub(crate) struct Fence {
    pub session: String,
    pub processed_slot: u64,
    pub sampled_at: DateTime<Utc>,
    pub genesis_hash: String,
    pub policy_identity: String,
}

pub(crate) fn enabled(c: &ExecutionConfig) -> bool {
    c.native_fresh_buy.as_ref().is_some_and(|p| p.policy == PROCESSED_SLOT_FENCE_AVAILABILITY_V1)
}

pub(crate) fn policy_identity(c: &ExecutionConfig) -> Result<String> {
    ensure!(enabled(c), "native_buy_policy_disabled");
    Ok(crate::execution_owned_sell_rpc::digest(serde_json::to_vec(&(
        PROCESSED_SLOT_FENCE_AVAILABILITY_V1,
        crate::execution_owned_sell_rpc::identity(c)?,
        c.canary_max_signal_age_seconds,
        c.canary_buy_size_sol.to_bits(),
        c.quote_canary_buy_size_sol.to_bits(),
        c.canary_kill_switch_path.as_str(),
    ))?))
}

async fn result<T: Transport + ?Sized>(
    rpc: &mut T,
    request: Value,
    check: &mut (impl FnMut() -> Result<()> + Send),
) -> Result<Value> {
    check()?;
    let response = rpc.read(request.clone(), check).await?;
    check()?;
    ensure!(
        response["jsonrpc"] == "2.0"
            && response["id"] == request["id"]
            && response.get("error").is_none(),
        "native_buy_rpc_binding"
    );
    response.get("result").filter(|v| !v.is_null()).cloned()
        .context("native_buy_rpc_result_missing")
}

async fn genesis<T: Transport + ?Sized>(
    rpc: &mut T,
    c: &ExecutionConfig,
    check: &mut (impl FnMut() -> Result<()> + Send),
) -> Result<String> {
    let expected = &c.owned_sell_preparation.as_ref().context("native_buy_rpc_policy")?.genesis_hash;
    let value = result(rpc, json!({"jsonrpc":"2.0","id":"native-buy-genesis-v1","method":"getGenesisHash","params":[]}), check).await?;
    ensure!(value.as_str() == Some(expected), "native_buy_genesis_mismatch");
    Ok(expected.clone())
}

/// One nonwaiting acquisition; a different collector cannot queue behind it.
/// The age starts before getSlot, conservatively before the returned head sample.
pub(crate) async fn fence<T: Transport + ?Sized>(
    rpc: &mut T,
    c: &ExecutionConfig,
    session: &str,
    check: &mut (impl FnMut() -> Result<()> + Send),
) -> Result<Fence> {
    let _permit = ACQUISITION.try_acquire().context("native_buy_acquisition_busy")?;
    ensure!(enabled(c) && !session.is_empty(), "native_buy_policy_disabled");
    let genesis_hash = genesis(rpc, c, check).await?;
    let sampled_at = Utc::now();
    let slot = result(rpc, json!({"jsonrpc":"2.0","id":"native-buy-fence-v1","method":"getSlot","params":[{"commitment":"processed"}]}), check).await?
        .as_u64().filter(|v| *v > 0).context("native_buy_fence_slot")?;
    check()?;
    Ok(Fence { session: session.to_owned(), processed_slot: slot, sampled_at, genesis_hash, policy_identity: policy_identity(c)? })
}

/// The transaction is finalized on the configured genesis and still matches the
/// admission signature, slot, source signer and SPL mint; provider clocks are unused.
pub(crate) async fn finalized_source<T: Transport + ?Sized>(
    rpc: &mut T,
    c: &ExecutionConfig,
    signature: &str,
    slot: u64,
    wallet: &str,
    mint: &str,
    check: &mut (impl FnMut() -> Result<()> + Send),
) -> Result<()> {
    let _permit = ACQUISITION.try_acquire().context("native_buy_acquisition_busy")?;
    ensure!(enabled(c) && slot > 0 && !signature.is_empty(), "native_buy_source_identity");
    genesis(rpc, c, check).await?;
    let transaction = result(rpc, json!({"jsonrpc":"2.0","id":"native-buy-finalized-tx-v1","method":"getTransaction","params":[signature,{"encoding":"jsonParsed","commitment":"finalized","maxSupportedTransactionVersion":0}]}), check).await?;
    let meta = transaction.get("meta").and_then(Value::as_object)
        .context("native_buy_finalized_meta_missing")?;
    ensure!(transaction["slot"].as_u64() == Some(slot)
        && meta.get("err").is_some_and(Value::is_null)
        && transaction["transaction"]["signatures"][0].as_str() == Some(signature),
        "native_buy_finalized_identity");
    let signer = transaction["transaction"]["message"]["accountKeys"]
        .as_array().context("native_buy_finalized_keys")?
        .iter().any(|v| v["pubkey"].as_str() == Some(wallet) && v["signer"] == true);
    ensure!(signer, "native_buy_finalized_signer");
    let mint_present = transaction["meta"]["postTokenBalances"]
        .as_array().context("native_buy_finalized_balances")?
        .iter().any(|v| v["mint"].as_str() == Some(mint) && v["owner"].as_str() == Some(wallet));
    ensure!(mint_present, "native_buy_finalized_mint");
    let account = result(rpc, json!({"jsonrpc":"2.0","id":"native-buy-mint-owner-v1","method":"getAccountInfo","params":[mint,{"encoding":"base64","commitment":"finalized","minContextSlot":slot}]}), check).await?;
    ensure!(account["context"]["slot"].as_u64().is_some_and(|v| v >= slot)
        && account["value"]["owner"].as_str() == Some(SPL_TOKEN_PROGRAM),
        "native_buy_spl_mint_owner");
    check()?;
    Ok(())
}
