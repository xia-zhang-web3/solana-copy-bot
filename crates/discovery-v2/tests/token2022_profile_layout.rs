use copybot_discovery_v2::{validate_profile, ProfileFailure};
#[allow(dead_code)]
#[path = "token2022_profile/fixture.rs"]
mod fixture;

#[test]
fn r1_mint166_account_type_without_tlv_is_malformed() {
    let mut profile = fixture::profile(3, &[500]);
    profile.mint_account.data = fixture::extended_mint(&profile);
    assert_eq!(profile.mint_account.data.len(), 166);
    assert_eq!(
        validate_profile(&profile, &profile.wallet, &profile.mint),
        Err(ProfileFailure::Malformed)
    );
}

#[test]
fn r1_account166_account_type_without_tlv_is_malformed() {
    let mut profile = fixture::profile(3, &[500]);
    profile.accounts[0].raw.data.push(2);
    assert_eq!(profile.accounts[0].raw.data.len(), 166);
    assert_eq!(
        validate_profile(&profile, &profile.wallet, &profile.mint),
        Err(ProfileFailure::Malformed)
    );
}
