use crate::execution_native_floor_policy::reserve_lamports;

#[test]
fn native_floor_policy_exact_binary_conversion_matches_independent_fraction() {
    for &(bits, expected) in super::native_floor_conversion_cases::CASES {
        let result = reserve_lamports(f64::from_bits(bits));
        if let Some(expected) = expected {
            assert_eq!(result.unwrap(), expected, "bits={bits:016x}");
        } else {
            assert_eq!(
                result.unwrap_err().to_string(),
                "native_floor_invalid_policy",
                "bits={bits:016x}"
            );
        }
    }
    assert_eq!(reserve_lamports(0.05).unwrap(), 50_000_001);
}
