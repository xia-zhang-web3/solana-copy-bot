use copybot_core_types::association_recovery::ReplayScope;

fn scope() -> ReplayScope {
    ReplayScope {
        policy: "durable_checkpoint_replay_v1".into(),
        wallets: vec!["wallet".into()],
        programs: vec!["interested".into()],
        raydium_programs: vec!["raydium".into()],
        pumpswap_programs: vec!["pumpswap".into()],
    }
}

#[test]
fn pumpswap_only_scope_preserves_empty_raydium_partition() {
    let mut s = scope();
    s.raydium_programs.clear();
    assert!(s.valid());
    assert!(s.raydium_programs.is_empty());
    assert_eq!(s.pumpswap_programs, vec!["pumpswap"]);
    let mut different = s.clone();
    different.raydium_programs.push("raydium".into());
    assert_ne!(s, different);
}

#[test]
fn raydium_only_scope_preserves_empty_pumpswap_partition() {
    let mut s = scope();
    s.pumpswap_programs.clear();
    assert!(s.valid());
    assert!(s.pumpswap_programs.is_empty());
    assert_eq!(s.raydium_programs, vec!["raydium"]);
    let mut different = s.clone();
    different.pumpswap_programs.push("pumpswap".into());
    assert_ne!(s, different);
}

#[test]
fn duplicate_unsorted_and_invalid_partition_identities_remain_refused() {
    for field in 0..4 {
        for values in [vec!["a", "a"], vec!["b", "a"], vec![""]] {
            let mut s = scope();
            let target = match field {
                0 => &mut s.wallets,
                1 => &mut s.programs,
                2 => &mut s.raydium_programs,
                _ => &mut s.pumpswap_programs,
            };
            *target = values.into_iter().map(String::from).collect();
            assert!(!s.valid());
        }
    }
    let mut s = scope();
    s.wallets.clear();
    assert!(!s.valid());
    let mut s = scope();
    s.programs.clear();
    assert!(!s.valid());
    let mut s = scope();
    s.raydium_programs = (0..257).map(|i| format!("ray-{i:03}")).collect();
    assert!(!s.valid());
    let mut s = scope();
    s.pumpswap_programs = vec!["x".repeat(129)];
    assert!(!s.valid());
}
