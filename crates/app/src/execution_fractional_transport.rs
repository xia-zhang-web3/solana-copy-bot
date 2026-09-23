//! Actual fractional acquisition; raw bodies/parsed results are moved, never cloned.
use super::super::body;
use anyhow::Result;
use copybot_config::ExecutionConfig;
use serde_json::Value;
use std::{future::Future, pin::Pin};
pub(crate) type Check<'a> = dyn FnMut() -> Result<()> + Send + 'a;
pub(crate) type Pending<'a> = Pin<Box<dyn Future<Output = Result<Value>> + Send + 'a>>;
pub(crate) trait Transport {
    fn read<'a>(&'a mut self, request: Value, check: &'a mut Check<'_>) -> Pending<'a>;
}
pub(crate) struct Http<'a> {
    pub http: &'a reqwest::Client,
    pub config: &'a ExecutionConfig,
    pub url: reqwest::Url,
    pub budget: body::Budget,
}
/// Both production HTTP and mock chunk streams enter this exact reader/decode path.
pub(crate) async fn receive<S: Send + 'static, C: AsRef<[u8]> + Send + 'static>(
    request: &Value,
    head: body::Head,
    deadline: tokio::time::Instant,
    budget: &mut body::Budget,
    check: &mut (impl FnMut() -> Result<()> + ?Sized),
    source: &mut S,
    next: impl for<'a> FnMut(&'a mut S) -> body::Chunk<'a, C>,
) -> Result<Value> {
    let cap = body::fractional_limit(request).min(budget.remaining_wire());
    let bytes = body::bytes(head, cap, deadline, check, source, next).await?;
    let value = budget.decode(&bytes, request)?;
    check()?;
    anyhow::ensure!(
        tokio::time::Instant::now() < deadline,
        "owned_sell_rpc_deadline"
    );
    Ok(value)
}
impl Transport for Http<'_> {
    fn read<'a>(&'a mut self, request: Value, check: &'a mut Check<'_>) -> Pending<'a> {
        Box::pin(async move {
            check()?;
            let timeout = std::time::Duration::from_millis(
                self.config.quote_canary_timeout_ms.clamp(1, 10000),
            );
            let deadline = tokio::time::Instant::now() + timeout;
            let mut response = self
                .http
                .post(self.url.clone())
                .json(&request)
                .timeout(timeout)
                .send()
                .await
                .map_err(|_| anyhow::anyhow!("owned_sell_rpc_transport"))?;
            let head = body::Head {
                endpoint_matches: response.url() == &self.url,
                status: response.status().as_u16(),
                content_length: response.content_length(),
            };
            receive(
                &request,
                head,
                deadline,
                &mut self.budget,
                check,
                &mut response,
                |r| {
                    Box::pin(async move {
                        r.chunk()
                            .await
                            .map_err(|_| anyhow::anyhow!("owned_sell_rpc_body"))
                    })
                },
            )
            .await
        })
    }
}
/// Compatibility test adapter for existing parsed-value cases, not transport evidence.
pub(crate) struct Parsed<F>(pub F);
impl<F, Fut> Transport for Parsed<F>
where
    F: FnMut(Value) -> Fut + Send,
    Fut: Future<Output = Result<Value>> + Send,
{
    fn read<'a>(&'a mut self, request: Value, check: &'a mut Check<'_>) -> Pending<'a> {
        Box::pin(async move {
            check()?;
            let v = (self.0)(request).await?;
            check()?;
            Ok(v)
        })
    }
}
