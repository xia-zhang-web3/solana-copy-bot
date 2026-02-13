mod parser;
mod source;

use anyhow::Result;
use copybot_config::IngestionConfig;
use copybot_core_types::SwapEvent;
use parser::SwapParser;
use source::{IngestionSource, RawSwapObservation};

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
}

pub fn summarize_raw_observation(raw: &RawSwapObservation) -> (&str, &str, &str) {
    (&raw.signature, &raw.signer, &raw.dex_hint)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn parses_raydium_swap_from_program_id() {
        let parser = SwapParser::new(
            vec!["raydium-program".to_string()],
            vec!["pumpswap-program".to_string()],
        );
        let raw = RawSwapObservation {
            signature: "sig-1".to_string(),
            slot: 100,
            signer: "wallet-a".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "TokenMint".to_string(),
            amount_in: 1.0,
            amount_out: 1000.0,
            program_ids: vec!["raydium-program".to_string()],
            dex_hint: String::new(),
            ts_utc: Utc::now(),
        };

        let parsed = parser.parse(raw).expect("expected parsed swap");
        assert_eq!(parsed.dex, "raydium");
    }
}
