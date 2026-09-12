//! Exact-message RPC observations for selected initial funding, not full reserve proof.
pub(crate) mod fee_only;
pub(crate) mod payer;
pub(crate) mod rent_types;
mod request;
mod response;
pub(crate) mod token2022_rent;
pub(crate) mod types;

use self::rent_types::{ClassicAtaFundingFacts, ClassicAtaRentObservation};
use self::request::{observe, observe_rent, observe_token2022_rent, Method};
use self::token2022_rent::Token2022Collection;
use self::types::{NativeFundingRpcFacts, ObservationTiming};
use crate::execution_native_funding::decode_native_funding_requirements;
use crate::execution_solana_tx::PubkeyBytes;
use anyhow::{anyhow, ensure, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::json;
use std::time::{Duration, Instant, SystemTime};

// Whole-response caps apply even without Content-Length; never return a data prefix.
pub(crate) const FEE_RESPONSE_LIMIT: usize = 16 * 1024;
pub(crate) const ACCOUNTS_RESPONSE_LIMIT: usize = 8 * 1024 * 1024;

/// Construct once for the collector owner. Clones share reqwest's connection pool.
/// The closed client boundary prevents callers from enabling redirects or retries.
#[derive(Clone)]
pub(crate) struct NativeFundingRpcClient {
    http: reqwest::Client,
}

impl NativeFundingRpcClient {
    pub(crate) fn new() -> Result<Self> {
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .build()
            .map_err(|_| anyhow!("native_rpc_client_init"))?;
        Ok(Self { http })
    }

    pub(crate) async fn collect(
        &self,
        endpoint: &str,
        timeout: Duration,
        payload: &str,
        expected_wallet: PubkeyBytes,
        min_context_slot: Option<u64>,
    ) -> Result<NativeFundingRpcFacts> {
        collect_with_client(
            &self.http,
            endpoint,
            timeout,
            payload,
            expected_wallet,
            min_context_slot,
            false,
            false,
        )
        .await
        .map(|(facts, _, _)| facts)
    }

    /// Extended invocation: at most three HTTP requests, under the same total deadline.
    pub(crate) async fn collect_with_classic_ata_rent(
        &self,
        endpoint: &str,
        timeout: Duration,
        payload: &str,
        expected_wallet: PubkeyBytes,
        min_context_slot: Option<u64>,
    ) -> Result<ClassicAtaFundingFacts> {
        let (native, rent, _) = collect_with_client(
            &self.http,
            endpoint,
            timeout,
            payload,
            expected_wallet,
            min_context_slot,
            true,
            false,
        )
        .await?;
        Ok(ClassicAtaFundingFacts {
            native,
            rent: rent.ok_or_else(|| anyhow!("native_rpc_missing_rent"))?,
            token2022: None,
        })
    }
    /// At most four HTTP requests; supported mint/ATA proof comes from the same
    /// ordered accounts response. Scalar170 shares the original collection deadline.
    pub(crate) async fn collect_with_supported_ata_rent(
        &self,
        endpoint: &str,
        timeout: Duration,
        payload: &str,
        expected_wallet: PubkeyBytes,
        min_context_slot: Option<u64>,
    ) -> Result<ClassicAtaFundingFacts> {
        let (native, rent, token2022) = collect_with_client(
            &self.http,
            endpoint,
            timeout,
            payload,
            expected_wallet,
            min_context_slot,
            true,
            true,
        )
        .await?;
        Ok(ClassicAtaFundingFacts {
            native,
            rent: rent.ok_or_else(|| anyhow!("native_rpc_missing_rent"))?,
            token2022,
        })
    }
}

// Private implementation detail: no crate-visible API accepts an arbitrary Client.
async fn collect_with_client(
    http: &reqwest::Client,
    endpoint: &str,
    timeout: Duration,
    payload: &str,
    expected_wallet: PubkeyBytes,
    min_context_slot: Option<u64>,
    with_rent: bool,
    with_token2022: bool,
) -> Result<(
    NativeFundingRpcFacts,
    Option<ClassicAtaRentObservation>,
    Option<Token2022Collection>,
)> {
    let started_at = SystemTime::now();
    let started = Instant::now();
    ensure!(
        !timeout.is_zero() && timeout <= Duration::from_secs(30),
        "native_rpc_timeout_bounds"
    );
    // Bound base64 allocation before the unchanged B18 reader enforces the 1232-byte wire cap.
    ensure!(payload.len() <= 1644, "native_rpc_payload_too_large");
    let requirements = decode_native_funding_requirements(payload, expected_wallet)
        .map_err(|_| anyhow!("native_rpc_invalid_requirements"))?;
    let requested_keys: Vec<_> = requirements
        .binding
        .accounts
        .iter()
        .map(|a| a.pubkey)
        .collect();
    ensure!(requested_keys.len() <= 100, "native_rpc_account_limit");
    let url = reqwest::Url::parse(endpoint).map_err(|_| anyhow!("native_rpc_endpoint"))?;
    ensure!(
        matches!(url.scheme(), "http" | "https"),
        "native_rpc_endpoint"
    );
    let mut config = json!({"commitment":"confirmed"});
    if let Some(floor) = min_context_slot {
        config["minContextSlot"] = json!(floor);
    }
    let fee_params = json!([STANDARD.encode(&requirements.binding.message_bytes), config]);
    config["encoding"] = json!("base64");
    let account_params = json!([
        requested_keys
            .iter()
            .map(|key| bs58::encode(key).into_string())
            .collect::<Vec<_>>(),
        config
    ]);
    let remaining = timeout
        .checked_sub(started.elapsed())
        .ok_or_else(|| anyhow!("native_rpc_timeout"))?;
    // Scoped futures: either error/timeout drops the sibling; no spawn, detached task or retry.
    let (fee, (accounts, token2022), rent) = tokio::time::timeout(remaining, async {
        tokio::try_join!(
            observe(
                http,
                endpoint,
                remaining,
                Method::Fee,
                fee_params,
                min_context_slot,
                response::fee
            ),
            async {
                let accounts = observe(
                    http,
                    endpoint,
                    remaining,
                    Method::Accounts,
                    account_params,
                    min_context_slot,
                    |value| response::accounts(value, &requested_keys),
                )
                .await?;
                let token2022 = if with_token2022 {
                    let rent = if crate::execution_native_ata_funding::token2022::needs_rent(
                        &requirements,
                        &accounts.value,
                    ) {
                        let left = timeout
                            .checked_sub(started.elapsed())
                            .ok_or_else(|| anyhow!("native_rpc_timeout"))?;
                        Some(
                            observe_token2022_rent(
                                http,
                                endpoint,
                                left,
                                &requirements.binding.transaction_sha256,
                            )
                            .await?,
                        )
                    } else {
                        None
                    };
                    Some(Token2022Collection { rent })
                } else {
                    None
                };
                Ok::<_, anyhow::Error>((accounts, token2022))
            },
            async {
                if with_rent {
                    observe_rent(http, endpoint, remaining).await.map(Some)
                } else {
                    Ok(None)
                }
            },
        )
    })
    .await
    .map_err(|_| anyhow!("native_rpc_timeout"))??;
    // The bounded JSON decode is synchronous; do not return a success after its deadline.
    ensure!(started.elapsed() <= timeout, "native_rpc_timeout");
    Ok((
        NativeFundingRpcFacts {
            requirements,
            requested_keys,
            commitment: "confirmed",
            min_context_slot,
            fee,
            accounts,
            timing: ObservationTiming::finish(started_at, started),
        },
        rent,
        token2022,
    ))
}
