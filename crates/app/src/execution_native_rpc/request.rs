use super::rent_types::{ClassicAtaRentObservation, CLASSIC_TOKEN_ACCOUNT_LENGTH};
use super::response;
use super::types::{ObservationTiming, RpcObservation};
use super::{ACCOUNTS_RESPONSE_LIMIT, FEE_RESPONSE_LIMIT};
use anyhow::{anyhow, ensure, Context, Result};
use serde_json::{json, Value};
use std::time::{Duration, Instant, SystemTime};

#[derive(Clone, Copy)]
pub(super) enum Method<'a> {
    Fee,
    Accounts,
    Rent,
    Token2022Rent {
        invocation: u64,
        transaction: &'a str,
    },
}

impl Method<'_> {
    fn name(self) -> &'static str {
        match self {
            Self::Fee => "getFeeForMessage",
            Self::Accounts => "getMultipleAccounts",
            Self::Rent | Self::Token2022Rent { .. } => "getMinimumBalanceForRentExemption",
        }
    }
    fn id(self) -> String {
        match self {
            Self::Fee => "native-funding-fee".into(),
            Self::Accounts => "native-funding-accounts".into(),
            Self::Rent => "native-funding-classic-ata-rent".into(),
            Self::Token2022Rent {
                invocation,
                transaction,
            } => format!("native-funding-token2022-ata-rent-{transaction}-{invocation}"),
        }
    }
    fn limit(self) -> usize {
        match self {
            Self::Fee => FEE_RESPONSE_LIMIT,
            Self::Accounts => ACCOUNTS_RESPONSE_LIMIT,
            Self::Rent | Self::Token2022Rent { .. } => FEE_RESPONSE_LIMIT,
        }
    }
}

pub(super) async fn observe<T>(
    http: &reqwest::Client,
    endpoint: &str,
    timeout: Duration,
    method: Method<'_>,
    params: Value,
    floor: Option<u64>,
    parse: impl FnOnce(&Value) -> Result<T>,
) -> Result<RpcObservation<T>> {
    let ((slot, value), timing) = fetch(http, endpoint, timeout, method, params, |body| {
        let (slot, value) = response::envelope(body, &method.id(), floor)?;
        Ok((slot, parse(value)?))
    })
    .await?;
    Ok(RpcObservation {
        slot,
        timing,
        value,
    })
}

pub(super) async fn observe_rent(
    http: &reqwest::Client,
    endpoint: &str,
    timeout: Duration,
) -> Result<ClassicAtaRentObservation> {
    let (lamports, timing) = fetch(
        http,
        endpoint,
        timeout,
        Method::Rent,
        json!([CLASSIC_TOKEN_ACCOUNT_LENGTH, {"commitment":"confirmed"}]),
        |body| {
            response::result(body, &Method::Rent.id())?
                .as_u64()
                .ok_or_else(|| anyhow!("native_rpc_rent_value"))
        },
    )
    .await?;
    Ok(ClassicAtaRentObservation {
        data_length: CLASSIC_TOKEN_ACCOUNT_LENGTH,
        commitment: "confirmed",
        lamports,
        timing,
    })
}

// A distinct payload + invocation request ID refuses swapped/replayed scalar replies.
// This is request provenance, not a provider-authenticity or freshness proof.
pub(super) async fn observe_token2022_rent(
    http: &reqwest::Client,
    endpoint: &str,
    timeout: Duration,
    transaction: &str,
) -> Result<super::token2022_rent::Token2022RentObservation> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let invocation = NEXT
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
        .map_err(|_| anyhow!("native_rpc_invocation_exhausted"))?;
    let method = Method::Token2022Rent {
        invocation,
        transaction,
    };
    let data_length = crate::execution_native_ata_funding::token2022::ACCOUNT_LENGTH;
    let (lamports, timing) = fetch(
        http,
        endpoint,
        timeout,
        method,
        json!([data_length, {"commitment":"confirmed"}]),
        |body| {
            response::result(body, &method.id())?
                .as_u64()
                .ok_or_else(|| anyhow!("native_rpc_rent_value"))
        },
    )
    .await?;
    Ok(super::token2022_rent::Token2022RentObservation {
        data_length,
        commitment: "confirmed",
        lamports,
        timing,
        transaction_sha256: transaction.into(),
    })
}

async fn fetch<T>(
    http: &reqwest::Client,
    endpoint: &str,
    timeout: Duration,
    method: Method<'_>,
    params: Value,
    parse: impl FnOnce(&Value) -> Result<T>,
) -> Result<(T, ObservationTiming)> {
    let started_at = SystemTime::now();
    let started = Instant::now();
    let value = async {
        let request =
            json!({"jsonrpc":"2.0", "id":method.id(), "method":method.name(), "params":params});
        let mut response = http
            .post(endpoint)
            .timeout(timeout)
            .json(&request)
            .send()
            .await
            .map_err(|error| {
                anyhow!(if error.is_timeout() {
                    "native_rpc_timeout"
                } else {
                    "native_rpc_transport"
                })
            })?;
        ensure!(response.status().is_success(), "native_rpc_http_status");
        let limit = method.limit();
        ensure!(
            response.content_length().is_none_or(|n| n <= limit as u64),
            "native_rpc_response_too_large"
        );
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|error| {
            anyhow!(if error.is_timeout() {
                "native_rpc_timeout"
            } else {
                "native_rpc_body_read"
            })
        })? {
            ensure!(
                chunk.len() <= limit - bytes.len(),
                "native_rpc_response_too_large"
            );
            bytes.extend_from_slice(&chunk);
        }
        let body: Value =
            serde_json::from_slice(&bytes).map_err(|_| anyhow!("native_rpc_invalid_json"))?;
        let value = parse(&body)?;
        Ok((value, ObservationTiming::finish(started_at, started)))
    }
    .await;
    value.with_context(|| format!("native_rpc_method={}", method.name()))
}
