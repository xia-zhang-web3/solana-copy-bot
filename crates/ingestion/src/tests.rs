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
        exact_amounts: None,
        program_ids: vec!["raydium-program".to_string()],
        dex_hint: String::new(),
        ts_utc: Utc::now(),
    };

    let parsed = parser.parse(raw).expect("expected parsed swap");
    assert_eq!(parsed.dex, "raydium");
}
