pub mod capture_replay;
mod parser;
mod source;

use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_config::IngestionConfig;
use copybot_core_types::SwapEvent;
use parser::SwapParser;
use source::{fetch_recent_raw_swaps_for_wallets, IngestionSource, RawSwapObservation};

pub use source::durable::{DeliveryEnvelope, DeliveryReceiver, ReplayInput};
pub use source::IngestionRuntimeSnapshot;

#[derive(Debug, Clone, Default)]
pub struct IngestionBackfillReport {
    pub wallets_scanned: usize,
    pub signatures_seen: usize,
    pub transactions_fetched: usize,
    pub rpc_errors: usize,
    pub swaps: Vec<SwapEvent>,
}

pub struct IngestionService {
    source: Option<IngestionSource>,
    delivery_config: Option<IngestionConfig>,
    prepared_delivery: Option<DeliveryReceiver>,
    parser: SwapParser,
}

impl IngestionService {
    pub fn build(config: &IngestionConfig) -> Result<Self> {
        copybot_config::validate_delivery_source(config)?;
        let durable = config.yellowstone_delivery_mode == "durable_association_v1";
        let source = if durable {
            None
        } else {
            Some(IngestionSource::from_config(config)?)
        };
        let parser = SwapParser::new(
            config.raydium_program_ids.clone(),
            config.pumpswap_program_ids.clone(),
        );
        Ok(Self {
            source,
            parser,
            delivery_config: durable.then(|| config.clone()),
            prepared_delivery: None,
        })
    }

    pub fn build_for_app(config: &copybot_config::AppConfig) -> Result<Self> {
        copybot_config::validate_association_delivery(config)?;
        Self::build(&config.ingestion)
    }
    #[doc(hidden)]
    pub fn with_replay(
        config: &copybot_config::AppConfig,
        input: tokio::sync::mpsc::Receiver<ReplayInput>,
        session: String,
    ) -> Result<Self> {
        let mut service = Self::build_for_app(config)?;
        anyhow::ensure!(
            service.source.is_none(),
            "replay cannot bypass legacy source"
        );
        service.prepared_delivery =
            Some(DeliveryReceiver::replay(&config.ingestion, session, input)?);
        Ok(service)
    }
    pub fn take_delivery(&mut self, session: String) -> Result<Option<DeliveryReceiver>> {
        if let Some(prepared) = self.prepared_delivery.take() {
            self.delivery_config = None;
            return Ok(Some(prepared));
        }
        self.delivery_config
            .take()
            .map(|c| DeliveryReceiver::start(&c, session))
            .transpose()
    }
    pub async fn next_swap(&mut self) -> Result<Option<SwapEvent>> {
        loop {
            let raw = match self
                .source
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("durable association cannot produce SwapEvent"))?
                .next_observation()
                .await?
            {
                Some(raw) => raw,
                None => return Ok(None),
            };

            if let Some(parsed) = self.parser.parse(raw) {
                return Ok(Some(parsed));
            }
        }
    }

    pub fn runtime_snapshot(&self) -> Option<IngestionRuntimeSnapshot> {
        self.source
            .as_ref()
            .and_then(IngestionSource::runtime_snapshot)
    }
}

pub fn summarize_raw_observation(raw: &RawSwapObservation) -> (&str, &str, &str) {
    (&raw.signature, &raw.signer, &raw.dex_hint)
}

pub async fn fetch_recent_swaps_for_wallets(
    config: &IngestionConfig,
    wallets: Vec<String>,
    since: DateTime<Utc>,
    signature_limit: usize,
) -> Result<IngestionBackfillReport> {
    let raw_report =
        fetch_recent_raw_swaps_for_wallets(config, wallets, since, signature_limit).await?;
    let parser = SwapParser::new(
        config.raydium_program_ids.clone(),
        config.pumpswap_program_ids.clone(),
    );
    let swaps = raw_report
        .observations
        .into_iter()
        .filter_map(|raw| parser.parse(raw))
        .collect::<Vec<_>>();
    Ok(IngestionBackfillReport {
        wallets_scanned: raw_report.wallets_scanned,
        signatures_seen: raw_report.signatures_seen,
        transactions_fetched: raw_report.transactions_fetched,
        rpc_errors: raw_report.rpc_errors,
        swaps,
    })
}

#[cfg(test)]
mod tests;
