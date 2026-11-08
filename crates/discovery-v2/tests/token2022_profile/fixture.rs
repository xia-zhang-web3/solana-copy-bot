use copybot_discovery_v2::{
    Token2022BoundAccount, Token2022PositionProfile, Token2022RawAccount, PROFILE_VERSION,
    TOKEN_2022_PROGRAM,
};

pub fn key(last: u8) -> String {
    const ALPHABET: &[u8] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
    let mut value = last;
    let mut digits = Vec::new();
    while value > 0 {
        digits.push(ALPHABET[usize::from(value % 58)] as char);
        value /= 58;
    }
    let mut result = "1".repeat(if last == 0 { 32 } else { 31 });
    result.extend(digits.into_iter().rev());
    result
}

pub fn raw(data: Vec<u8>) -> Token2022RawAccount {
    Token2022RawAccount {
        program_owner: TOKEN_2022_PROGRAM.into(),
        context_slot: 100,
        executable: false,
        data,
    }
}

pub fn profile(decimals: u8, amounts: &[u64]) -> Token2022PositionProfile {
    let mut mint = vec![0; 82];
    mint[44] = decimals;
    mint[45] = 1;
    let accounts = amounts
        .iter()
        .enumerate()
        .map(|(index, amount)| {
            let mut data = vec![0; 165];
            data[31] = 1;
            data[64..72].copy_from_slice(&amount.to_le_bytes());
            data[108] = 1;
            Token2022BoundAccount {
                pubkey: key(index as u8 + 2),
                raw: raw(data),
                expected_raw_amount: *amount,
                expected_decimals: decimals,
                expected_state: "initialized".into(),
            }
        })
        .collect();
    Token2022PositionProfile {
        contract_version: PROFILE_VERSION,
        wallet: key(0),
        mint: key(1),
        mint_account: raw(mint),
        accounts,
    }
}

pub fn tlv(kind: u16, body: &[u8]) -> Vec<u8> {
    let mut result = Vec::new();
    result.extend(kind.to_le_bytes());
    result.extend((body.len() as u16).to_le_bytes());
    result.extend(body);
    result
}

pub fn metadata(pairs: &[(&str, &str)]) -> Vec<u8> {
    let mut data = vec![0; 64];
    data[63] = 1;
    for value in ["Token", "TOK", "uri"] {
        string(&mut data, value);
    }
    data.extend((pairs.len() as u32).to_le_bytes());
    for (key, value) in pairs {
        string(&mut data, key);
        string(&mut data, value);
    }
    data
}

fn string(data: &mut Vec<u8>, value: &str) {
    data.extend((value.len() as u32).to_le_bytes());
    data.extend(value.as_bytes());
}

pub fn extended_mint(profile: &Token2022PositionProfile) -> Vec<u8> {
    let mut mint = profile.mint_account.data[..82].to_vec();
    mint.resize(166, 0);
    mint[165] = 1;
    mint
}

pub fn pointer() -> Vec<u8> {
    let mut body = vec![0; 64];
    body[63] = 1;
    tlv(18, &body)
}

pub fn with_metadata(profile: &Token2022PositionProfile) -> Vec<u8> {
    let mut mint = extended_mint(profile);
    mint.extend(pointer());
    mint.extend(tlv(19, &metadata(&[("key", "value")])));
    mint
}
