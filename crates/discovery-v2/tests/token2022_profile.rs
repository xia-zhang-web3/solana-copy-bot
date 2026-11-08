use copybot_discovery_v2::{validate_profile, ProfileFailure as F, Token2022PositionProfile};
#[path = "token2022_profile/fixture.rs"]
mod fixture;
use fixture::*;

fn check(profile: &Token2022PositionProfile) -> Result<copybot_discovery_v2::ProfileAmount, F> {
    validate_profile(profile, &profile.wallet, &profile.mint)
}

#[test]
fn extensionless_and_metadata_only_profiles_use_actual_decimals() {
    for decimals in [0, 3, 6, 9, 255] {
        let mut p = profile(decimals, &[123, 456]);
        let base = check(&p).unwrap();
        assert_eq!(base.raw_total, 579);
        assert_eq!(base.decimals, decimals);
        assert!(base.amount > 0.0 && base.amount.is_finite());
        assert_eq!(base.amount, 579.0 / 10_f64.powi(i32::from(decimals)));
        for account in &mut p.accounts {
            account.raw.data.extend([2, 7, 0, 0, 0]);
        }
        assert_eq!(check(&p).unwrap(), base);
        p.mint_account.data = with_metadata(&p);
        assert_eq!(check(&p).unwrap(), base);
    }
}

#[test]
fn totals_are_u128_and_serialized_raw_is_revalidated() {
    let p = profile(3, &[u64::MAX, u64::MAX]);
    assert_eq!(check(&p).unwrap().raw_total, u128::from(u64::MAX) * 2);
    let encoded = serde_json::to_vec(&p).unwrap();
    let mut saved: Token2022PositionProfile = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(check(&saved).unwrap(), check(&p).unwrap());
    saved.accounts[0].raw.data[64] ^= 1;
    assert_eq!(check(&saved), Err(F::Binding));
    let mut zero = profile(6, &[0, 0]);
    assert_eq!(check(&zero), Err(F::Malformed));
    zero.accounts[0].raw.data[64] = 1;
    zero.accounts[0].expected_raw_amount = 1;
    assert_eq!(check(&zero).unwrap().raw_total, 1);
}

#[test]
fn profile_identity_program_executable_and_inventory_binding_fail_closed() {
    let p = profile(3, &[100]);
    assert_eq!(validate_profile(&p, &key(9), &p.mint), Err(F::Binding));
    assert_eq!(validate_profile(&p, &p.wallet, &key(9)), Err(F::Binding));
    for mint in [true, false] {
        let mut changed = p.clone();
        let raw = if mint {
            &mut changed.mint_account
        } else {
            &mut changed.accounts[0].raw
        };
        raw.program_owner = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".into();
        assert_eq!(check(&changed), Err(F::Binding));
        let mut changed = p.clone();
        let raw = if mint {
            &mut changed.mint_account
        } else {
            &mut changed.accounts[0].raw
        };
        raw.executable = true;
        assert_eq!(check(&changed), Err(F::Binding));
    }
    let mut changed = p.clone();
    changed.accounts[0].expected_raw_amount += 1;
    assert_eq!(check(&changed), Err(F::Binding));
    changed = p.clone();
    changed.accounts[0].expected_decimals += 1;
    assert_eq!(check(&changed), Err(F::Binding));
    changed = p.clone();
    changed.accounts[0].expected_state = "frozen".into();
    assert_eq!(check(&changed), Err(F::Binding));
    for offset in [31, 63] {
        changed = p.clone();
        changed.accounts[0].raw.data[offset] ^= 8;
        assert_eq!(check(&changed), Err(F::Binding));
    }
    changed = p.clone();
    changed.accounts.push(changed.accounts[0].clone());
    assert_eq!(check(&changed), Err(F::Binding));
}

#[test]
fn exact_base58_width_and_valid_alphabet_are_required() {
    for bad in [
        "1".repeat(31),
        "1".repeat(33),
        "z".repeat(44),
        "0".repeat(32),
        "é".repeat(16),
    ] {
        let mut p = profile(6, &[1]);
        p.accounts[0].pubkey = bad.clone();
        assert_eq!(check(&p), Err(F::Malformed), "{bad}");
        p = profile(6, &[1]);
        p.wallet = bad;
        assert_eq!(check(&p), Err(F::Malformed));
    }
}

#[test]
fn missing_version_bounds_and_mint_layouts_are_explicit() {
    let p = profile(6, &[1]);
    let mut changed = p.clone();
    changed.accounts.clear();
    assert_eq!(check(&changed), Err(F::Missing));
    changed = p.clone();
    changed.contract_version += 1;
    assert_eq!(check(&changed), Err(F::Unsupported));
    changed = p.clone();
    changed.accounts = vec![changed.accounts[0].clone(); 129];
    assert_eq!(check(&changed), Err(F::Unsupported));
    for mint in [true, false] {
        changed = p.clone();
        let raw = if mint {
            &mut changed.mint_account
        } else {
            &mut changed.accounts[0].raw
        };
        raw.context_slot = 0;
        assert_eq!(check(&changed), Err(F::Missing));
        changed = p.clone();
        let raw = if mint {
            &mut changed.mint_account
        } else {
            &mut changed.accounts[0].raw
        };
        raw.data.clear();
        assert_eq!(check(&changed), Err(F::Missing));
    }
    for size in [1, 81, 83, 165] {
        changed = p.clone();
        changed.mint_account.data.resize(size, 0);
        assert_eq!(check(&changed), Err(F::Malformed));
    }
    for size in [355, 4097] {
        changed = p.clone();
        changed.mint_account.data.resize(size, 0);
        assert_eq!(check(&changed), Err(F::Unsupported));
    }
    for offset in [82, 164, 165] {
        changed = p.clone();
        changed.mint_account.data = extended_mint(&p);
        changed.mint_account.data[offset] = if offset == 165 { 2 } else { 1 };
        assert_eq!(check(&changed), Err(F::Malformed));
    }
}

#[test]
fn coption_tags_native_and_account_state_are_parsed_from_raw() {
    let p = profile(6, &[1]);
    for at in [0, 46] {
        let mut changed = p.clone();
        changed.mint_account.data[at] = 2;
        assert_eq!(check(&changed), Err(F::Malformed));
    }
    for at in [72, 109, 129] {
        let mut changed = p.clone();
        changed.accounts[0].raw.data[at] = 2;
        assert_eq!(check(&changed), Err(F::Malformed));
    }
    let mut changed = p.clone();
    changed.accounts[0].raw.data[109] = 1;
    assert_eq!(check(&changed), Err(F::Unsupported));
    for state in [0, 2, 3, 255] {
        changed = p.clone();
        changed.accounts[0].raw.data[108] = state;
        assert_eq!(
            check(&changed),
            Err(if state == 2 { F::Frozen } else { F::Malformed })
        );
    }
    changed = p.clone();
    changed.mint_account.data[45] = 0;
    assert_eq!(check(&changed), Err(F::Malformed));
    // SPL ignores unused None payload bytes; do not invent a stricter COption ABI.
    changed = p.clone();
    changed.accounts[0].raw.data[76..108].fill(9);
    changed.mint_account.data[4..36].fill(9);
    assert!(check(&changed).is_ok());
}

#[test]
fn account_extensions_are_only_absent_or_immutable_owner() {
    let p = profile(6, &[1]);
    for size in [164, 167, 168, 169] {
        let mut changed = p.clone();
        changed.accounts[0].raw.data.resize(size, 0);
        assert_eq!(check(&changed), Err(F::Malformed));
    }
    for kind in 0..30u16 {
        let mut changed = p.clone();
        changed.accounts[0].raw.data.push(2);
        changed.accounts[0].raw.data.extend(tlv(kind, &[]));
        assert_eq!(check(&changed).is_ok(), kind == 7, "kind={kind}");
    }
    let mut changed = p.clone();
    changed.accounts[0].raw.data.extend([2, 7, 0, 1, 0]);
    assert_eq!(check(&changed), Err(F::Malformed));
    changed = p.clone();
    changed.accounts[0].raw.data.resize(171, 0);
    assert_eq!(check(&changed), Err(F::Unsupported));
}

#[test]
fn mint_tlv_pair_self_binding_duplicates_unknown_and_trailing_are_strict() {
    let p = profile(6, &[1]);
    for part in [pointer(), tlv(19, &metadata(&[]))] {
        let mut changed = p.clone();
        changed.mint_account.data = extended_mint(&p);
        changed.mint_account.data.extend(part);
        assert_eq!(check(&changed), Err(F::Unsupported));
    }
    let mut changed = p.clone();
    changed.mint_account.data = with_metadata(&p);
    changed.mint_account.data.extend(pointer());
    assert_eq!(check(&changed), Err(F::Malformed));
    changed = p.clone();
    changed.mint_account.data = with_metadata(&p);
    changed.mint_account.data.extend(tlv(19, &metadata(&[])));
    assert_eq!(check(&changed), Err(F::Malformed));
    for tail in [vec![0], vec![0; 4], tlv(1, &[]), vec![18, 0, 64, 0]] {
        changed = p.clone();
        changed.mint_account.data = with_metadata(&p);
        changed.mint_account.data.extend(tail);
        assert!(check(&changed).is_err());
    }
    changed = p.clone();
    changed.mint_account.data = with_metadata(&p);
    changed.mint_account.data[166 + 4 + 63] ^= 1;
    assert_eq!(check(&changed), Err(F::Binding));
    changed = p.clone();
    changed.mint_account.data = extended_mint(&p);
    changed.mint_account.data.extend(tlv(19, &metadata(&[])));
    changed.mint_account.data.extend(pointer());
    assert!(check(&changed).is_ok());
}

#[test]
fn metadata_borsh_is_utf8_complete_unique_and_bound_to_same_mint() {
    let p = profile(6, &[1]);
    let mut cases = Vec::new();
    let mut wrong_mint = metadata(&[]);
    wrong_mint[63] ^= 1;
    cases.push((wrong_mint, F::Binding));
    let mut truncated = metadata(&[]);
    truncated.pop();
    cases.push((truncated, F::Malformed));
    let mut trailing = metadata(&[]);
    trailing.push(0);
    cases.push((trailing, F::Malformed));
    let mut utf8 = metadata(&[]);
    utf8[68] = 255;
    cases.push((utf8, F::Malformed));
    cases.push((
        metadata(&[("duplicate-key", "1"), ("duplicate-key", "2")]),
        F::Malformed,
    ));
    let mut overflow = metadata(&[]);
    let end = overflow.len();
    overflow[end - 4..].copy_from_slice(&u32::MAX.to_le_bytes());
    cases.push((overflow, F::Malformed));
    for (body, failure) in cases {
        let mut changed = p.clone();
        changed.mint_account.data = extended_mint(&p);
        changed.mint_account.data.extend(pointer());
        changed.mint_account.data.extend(tlv(19, &body));
        assert_eq!(check(&changed), Err(failure));
    }
}

#[test]
fn accepted_limits_and_full_width_key_are_exercised() {
    let mut p = profile(9, &[u64::MAX; 128]);
    assert_eq!(check(&p).unwrap().raw_total, u128::from(u64::MAX) * 128);
    p.wallet = "JEKNVnkbo3jma5nREBBJCDoXFVeKkD56V3xKrvRmWxFG".into();
    for account in &mut p.accounts {
        account.raw.data[32..64].fill(255);
    }
    assert!(check(&p).is_ok());
    let available = 4096 - 166 - pointer().len() - 4;
    let base = metadata(&[("padding", "")]).len();
    let value = "x".repeat(available - base);
    p.mint_account.data = extended_mint(&p);
    p.mint_account.data.extend(pointer());
    p.mint_account
        .data
        .extend(tlv(19, &metadata(&[("padding", &value)])));
    assert_eq!(p.mint_account.data.len(), 4096);
    assert!(check(&p).is_ok());
}
