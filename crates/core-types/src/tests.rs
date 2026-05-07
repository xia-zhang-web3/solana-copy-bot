use super::{ExactAmountParseError, ExactSwapAmounts, Lamports, SignedLamports, TokenQuantity};

#[test]
fn lamports_checked_math_is_exact() {
    let one = Lamports::new(1);
    let two = Lamports::new(2);
    assert_eq!(one.checked_add(two), Some(Lamports::new(3)));
    assert_eq!(two.checked_sub(one), Some(Lamports::new(1)));
    assert_eq!(one.checked_sub(two), None);
}

#[test]
fn signed_lamports_preserve_signed_deltas() {
    let fee = SignedLamports::new(-5_000);
    let pnl = SignedLamports::new(9_000);
    assert_eq!(fee.checked_add(pnl), Some(SignedLamports::new(4_000)));
    assert_eq!(fee.checked_abs_lamports(), Some(Lamports::new(5_000)));
    assert_eq!(
        SignedLamports::from(Lamports::new(42)),
        SignedLamports::new(42)
    );
}

#[test]
fn exact_swap_amounts_parse_token_quantities() {
    let exact = ExactSwapAmounts {
        amount_in_raw: "1000000000".to_string(),
        amount_in_decimals: 9,
        amount_out_raw: "250000000".to_string(),
        amount_out_decimals: 6,
    };

    assert_eq!(
        exact.amount_in_quantity(),
        Ok(TokenQuantity::new(1_000_000_000, 9))
    );
    assert_eq!(
        exact.amount_out_quantity(),
        Ok(TokenQuantity::new(250_000_000, 6))
    );
}

#[test]
fn exact_swap_amounts_reject_invalid_raw_strings() {
    let exact = ExactSwapAmounts {
        amount_in_raw: "not-a-u64".to_string(),
        amount_in_decimals: 9,
        amount_out_raw: "1".to_string(),
        amount_out_decimals: 0,
    };

    assert_eq!(
        exact.amount_in_quantity(),
        Err(ExactAmountParseError::new(
            "amount_in_raw",
            "not-a-u64".to_string()
        ))
    );
}
