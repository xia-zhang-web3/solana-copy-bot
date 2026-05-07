mod parser;
mod source;

use anyhow::Result;
use copybot_config::IngestionConfig;
use copybot_core_types::SwapEvent;
use parser::SwapParser;
use source::{IngestionSource, RawSwapObservation};

pub use source::IngestionRuntimeSnapshot;

pub struct IngestionService {
    source: IngestionSource,
    parser: SwapParser,
}

impl IngestionService {
    pub fn build(config: &IngestionConfig) -> Result<Self> {
        let source = IngestionSource::from_config(config)?;
        let parser = SwapParser::new(
            config.raydium_program_ids.clone(),
            config.pumpswap_program_ids.clone(),
        );
        Ok(Self { source, parser })
    }

    pub async fn next_swap(&mut self) -> Result<Option<SwapEvent>> {
        loop {
            let raw = match self.source.next_observation().await? {
                Some(raw) => raw,
                None => return Ok(None),
            };

            if let Some(parsed) = self.parser.parse(raw) {
                return Ok(Some(parsed));
            }
        }
    }

    pub fn runtime_snapshot(&self) -> Option<IngestionRuntimeSnapshot> {
        self.source.runtime_snapshot()
    }
}

pub fn summarize_raw_observation(raw: &RawSwapObservation) -> (&str, &str, &str) {
    (&raw.signature, &raw.signer, &raw.dex_hint)
}

#[cfg(test)]
mod tests;
