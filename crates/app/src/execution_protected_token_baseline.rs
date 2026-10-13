//! The explicit native canary retains100USDCraw outside receipt-owned positionH.
use crate::execution_owned_sell_prepare::submit::guard;
use crate::execution_owned_sell_rpc as rpc;
use crate::execution_submit_adapter::ExecutionSubmitRequest;
use anyhow::{ensure, Context, Result};
use copybot_config::ExecutionConfig;
use copybot_storage_core::{
    ordered_sell_quote::fractional::inventory::{ProtectedExecutionBaseline, TOKEN, USDC},
    rpc_owned_sell_handoff::dispatch::Prepared,
    SqliteStore,
};
use serde_json::{json, Value};

pub(crate) fn configured(c: &ExecutionConfig) -> Result<Option<ProtectedExecutionBaseline>> {
    let Some(p) = c
        .owned_sell_preparation
        .as_ref()
        .and_then(|p| p.protected_bot_usdc_baseline.as_ref())
    else {
        return Ok(None);
    };
    let baseline = ProtectedExecutionBaseline {
        policy: p.policy.clone(),
        amount_raw: p.amount_raw,
        token_account: p.token_account.clone(),
    };
    baseline.validate()?;
    ensure!(
        c.technical_cohort.is_some()
            && rpc::fractional::enabled(c)
            && copybot_config::owned_sell_dispatch(c)
            && crate::execution_native_buy_rpc::enabled(c),
        "protected_usdc_native_scope"
    );
    use crate::execution_pumpswap_accounts::{
        associated_token_address, parse_pubkey, token_program_id,
    };
    let wallet = parse_pubkey(&c.canary_wallet_pubkey, "protected_usdc_wallet")?;
    let mint = parse_pubkey(USDC, "protected_usdc_mint")?;
    ensure!(
        bs58::encode(associated_token_address(
            &wallet,
            &mint,
            &token_program_id()
        ))
        .into_string()
            == baseline.token_account,
        "protected_usdc_canonical_account"
    );
    Ok(Some(baseline))
}

pub(crate) fn wire(
    c: &ExecutionConfig,
    r: &ExecutionSubmitRequest,
    payload: &str,
    selected_raw: u64,
) -> Result<()> {
    if configured(c)?.is_none() {
        return Ok(());
    }
    ensure!(
        !r.signal_id.starts_with("owner-exit:")
            && r.side == "sell"
            && r.token == USDC
            && r.wallet_pubkey == c.canary_wallet_pubkey,
        "protected_usdc_native_request"
    );
    crate::execution_usdc_sell_wire::verify(r, payload, selected_raw, false)?;
    Ok(())
}

fn bound(c: &ExecutionConfig, p: &Prepared) -> Result<(ProtectedExecutionBaseline, u64)> {
    let baseline = configured(c)?.context("protected_usdc_policy_missing")?;
    let b = &p.handoff.snapshot.quote;
    let f = b
        .fractional
        .as_ref()
        .context("protected_usdc_fraction_missing")?;
    ensure!(
        f.protected_execution_baseline.as_ref() == Some(&baseline)
            && b.mint == USDC
            && b.decimals == 6
            && b.raw == f.selected_raw
            && b.raw > 0
            && b.raw <= f.owned_raw
            && p.handoff.wallet == c.canary_wallet_pubkey,
        "protected_usdc_authority_changed"
    );
    Ok((baseline, f.owned_raw))
}

pub(crate) fn balance(c: &ExecutionConfig, p: &Prepared, value: &Value) -> Result<()> {
    let (baseline, owned) = bound(c, p)?;
    ensure!(
        value["context"]["slot"]
            .as_u64()
            .is_some_and(|s| s >= p.handoff.snapshot.sell.facts.slot),
        "protected_usdc_wallet_stale"
    );
    let rows = value["value"]
        .as_array()
        .context("protected_usdc_wallet_inventory")?;
    ensure!(rows.len() == 1, "protected_usdc_wallet_model");
    let row = &rows[0];
    let parsed = &row["account"]["data"]["parsed"];
    let info = &parsed["info"];
    ensure!(
        row["pubkey"] == baseline.token_account
            && row["account"]["owner"] == TOKEN
            && parsed["type"] == "account"
            && info["owner"] == c.canary_wallet_pubkey
            && info["mint"] == USDC
            && info["tokenAmount"]["decimals"] == 6,
        "protected_usdc_wallet_identity"
    );
    let raw = info["tokenAmount"]["amount"]
        .as_str()
        .context("protected_usdc_wallet_amount")?;
    let held = raw.parse::<u64>()?;
    ensure!(
        held.to_string() == raw
            && held
                == owned
                    .checked_add(baseline.amount_raw)
                    .context("protected_usdc_wallet_overflow")?,
        "protected_usdc_owned_lot_or_baseline_short"
    );
    Ok(())
}

/// The same production method accepts the bounded HTTP transport and explicit test mocks.
/// Both checks finish before the key loader or the synchronous dispatch claim.
pub(crate) async fn before_financial_boundary(
    c: &ExecutionConfig,
    store: &SqliteStore,
    r: &ExecutionSubmitRequest,
    payload: &str,
    mut transport: impl rpc::fractional::transport::Transport,
) -> Result<()> {
    if configured(c)?.is_none() {
        return Ok(());
    }
    let p = guard::request(store, r)?;
    guard::config(c, &p)?;
    bound(c, &p)?;
    wire(c, r, payload, p.handoff.snapshot.quote.raw)?;
    let mut check = || {
        guard::config(c, &p)?;
        guard::live(r)
    };
    check()?;
    let request = json!({"jsonrpc":"2.0","id":"owned-sell-protected-usdc100",
        "method":"getTokenAccountsByOwner","params":[c.canary_wallet_pubkey,{"mint":USDC},
        {"encoding":"jsonParsed","commitment":"finalized",
         "minContextSlot":p.handoff.snapshot.sell.facts.slot}]});
    let response = transport.read(request.clone(), &mut check).await?;
    check()?;
    ensure!(
        guard::request(store, r)? == p,
        "protected_usdc_prepared_changed"
    );
    ensure!(
        response["jsonrpc"] == "2.0"
            && response["id"] == request["id"]
            && response.get("error").is_none(),
        "protected_usdc_rpc_binding"
    );
    balance(
        c,
        &p,
        response
            .get("result")
            .context("protected_usdc_rpc_result")?,
    )?;
    check()
}

pub(crate) async fn current(
    c: &ExecutionConfig,
    store: &SqliteStore,
    r: &ExecutionSubmitRequest,
    payload: &str,
) -> Result<()> {
    if configured(c)?.is_none() {
        return Ok(());
    }
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .build()?;
    before_financial_boundary(
        c,
        store,
        r,
        payload,
        rpc::fractional::transport::Http {
            http: &http,
            config: c,
            url: rpc::endpoint(c)?,
            budget: Default::default(),
        },
    )
    .await
}
