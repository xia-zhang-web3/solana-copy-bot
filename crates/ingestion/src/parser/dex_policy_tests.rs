use super::{sample_raw_swap, SwapParser};

fn parser() -> SwapParser {
    SwapParser::new(vec!["ray".into(), "ray2".into()], vec!["pump".into()])
}
fn label(programs: &[&str], hint: &str) -> Option<String> {
    let mut raw = sample_raw_swap();
    raw.program_ids = programs.iter().map(|p| p.to_string()).collect();
    raw.dex_hint = hint.into();
    parser().parse(raw).map(|event| event.dex)
}
#[test]
fn dex_policy_multi_family_permutations_and_duplicates_are_explicit() {
    for ids in [
        vec!["ray", "pump", "unknown"],
        vec!["pump", "ray", "unknown"],
        vec!["unknown", "pump", "ray"],
        vec!["unknown", "ray", "pump"],
        vec!["ray", "unknown", "pump"],
        vec!["pump", "unknown", "ray"],
        vec!["ray", "pump", "ray", "pump"],
    ] {
        for hint in ["", "raydium", "pumpswap", "unknown"] {
            assert_eq!(label(&ids, hint).as_deref(), Some("multi_dex"));
        }
    }
}
#[test]
fn dex_policy_single_family_and_unrecognized_controls_are_unchanged() {
    for ids in [vec!["ray"], vec!["unknown", "ray2", "ray"]] {
        assert_eq!(label(&ids, "pumpswap").as_deref(), Some("raydium"));
    }
    for ids in [vec!["pump"], vec!["pump", "unknown", "pump"]] {
        assert_eq!(label(&ids, "raydium").as_deref(), Some("pumpswap"));
    }
    assert_eq!(label(&["unknown"], "unknown"), None);
    assert_eq!(label(&[], "RaYdIuM pump").as_deref(), Some("raydium"));
    assert_eq!(label(&["unknown"], "PumpSwap").as_deref(), Some("pumpswap"));
}
#[test]
fn dex_policy_overlapping_config_does_not_claim_one_family() {
    let p = SwapParser::new(vec!["shared".into()], vec!["shared".into()]);
    let mut raw = sample_raw_swap();
    raw.program_ids = vec!["shared".into()];
    assert_eq!(p.parse(raw).unwrap().dex, "multi_dex");
}
#[test]
fn dex_policy_multi_family_keeps_quantity_and_identity_rejection() {
    let mut raw = sample_raw_swap();
    raw.program_ids = vec!["pump".into(), "ray".into()];
    for amount in [f64::NAN, f64::INFINITY, -1.0, 0.0] {
        let mut invalid = raw.clone();
        invalid.amount_in = amount;
        assert!(parser().parse(invalid).is_none());
    }
    raw.signature.clear();
    assert!(parser().parse(raw).is_none());
}
