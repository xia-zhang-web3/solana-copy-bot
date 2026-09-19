use crate::source::RawSwapObservation;
use copybot_core_types::SwapEvent;

pub struct SwapParser {
    raydium_program_ids: Vec<String>,
    pumpswap_program_ids: Vec<String>,
}

impl SwapParser {
    pub fn new(raydium_program_ids: Vec<String>, pumpswap_program_ids: Vec<String>) -> Self {
        Self {
            raydium_program_ids,
            pumpswap_program_ids,
        }
    }

    pub fn parse(&self, raw: RawSwapObservation) -> Option<SwapEvent> {
        if raw.signer.is_empty()
            || raw.signature.is_empty()
            || raw.token_in.is_empty()
            || raw.token_out.is_empty()
        {
            return None;
        }
        if !raw.amount_in.is_finite()
            || !raw.amount_out.is_finite()
            || raw.amount_in <= 0.0
            || raw.amount_out <= 0.0
        {
            return None;
        }

        let dex = self.detect_dex(&raw)?;
        Some(SwapEvent {
            wallet: raw.signer,
            dex,
            token_in: raw.token_in,
            token_out: raw.token_out,
            amount_in: raw.amount_in,
            amount_out: raw.amount_out,
            signature: raw.signature,
            slot: raw.slot,
            ts_utc: raw.ts_utc,
            exact_amounts: raw.exact_amounts,
        })
    }

    fn detect_dex(&self, raw: &RawSwapObservation) -> Option<String> {
        // Classify configured families, not one execution venue or route leg.
        // Aggregated observations invoking both families keep that ambiguity explicit.
        let raydium = raw
            .program_ids
            .iter()
            .any(|program| self.raydium_program_ids.iter().any(|id| id == program));
        let pumpswap = raw
            .program_ids
            .iter()
            .any(|program| self.pumpswap_program_ids.iter().any(|id| id == program));
        match (raydium, pumpswap) {
            (true, true) => return Some("multi_dex".to_string()),
            (true, false) => return Some("raydium".to_string()),
            (false, true) => return Some("pumpswap".to_string()),
            (false, false) => {}
        }

        let hint = raw.dex_hint.to_lowercase();
        if hint.contains("raydium") {
            return Some("raydium".to_string());
        }
        if hint.contains("pump") {
            return Some("pumpswap".to_string());
        }
        None
    }
}

#[cfg(test)]
#[path = "parser/tests.rs"]
mod tests;
