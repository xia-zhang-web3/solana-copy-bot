use super::SwapParser;
use crate::source::RawSwapObservation;
use chrono::Utc;

fn sample_raw_swap() -> RawSwapObservation {
    RawSwapObservation {
        signature: "sig-1".to_string(),
        slot: 100,
        signer: "wallet-a".to_string(),
        token_in: "So11111111111111111111111111111111111111112".to_string(),
        token_out: "TokenMint".to_string(),
        amount_in: 1.0,
        amount_out: 100.0,
        exact_amounts: None,
        program_ids: vec!["raydium-program".to_string()],
        dex_hint: String::new(),
        ts_utc: Utc::now(),
    }
}

#[test]
fn parse_rejects_nan_amounts() {
    let parser = SwapParser::new(vec!["raydium-program".to_string()], Vec::new());
    let mut raw = sample_raw_swap();
    raw.amount_in = f64::NAN;

    assert!(parser.parse(raw).is_none(), "NaN amount must be rejected");
}

#[test]
fn parse_rejects_infinite_amounts() {
    let parser = SwapParser::new(vec!["raydium-program".to_string()], Vec::new());
    let mut raw = sample_raw_swap();
    raw.amount_out = f64::INFINITY;

    assert!(
        parser.parse(raw).is_none(),
        "non-finite amount must be rejected"
    );
}
