//! Explicitly modeled classic Jupiter/Raydium custody; not a live receipt.
use curve25519_dalek::edwards::CompressedEdwardsY;
use sha2::{Digest, Sha256};
use yellowstone_grpc_proto::prelude::*;

pub(super) const TOKEN: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
pub(super) const SYSTEM: &str = "11111111111111111111111111111111";
pub(super) const ATA: &str = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";
pub(super) const SOL: &str = "So11111111111111111111111111111111111111112";
pub(super) const MINT: &str = "DVb1znJKBVJzcuzbgvcG3cSghf2i1YzdJqoJFb7ZdQuX";
pub(super) const JUP: &str = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";
pub(super) const AMM: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
pub(super) const INPUT: u64 = 10_000_000;
pub(super) const OUTPUT: u64 = 267_894_178;
pub(super) const ROUTE_INDEX: usize = 4;

fn bytes(s: &str) -> Vec<u8> {
    bs58::decode(s).into_vec().unwrap()
}
// Test-only reference PDA calculation, never calling the production proof.
fn derived(seeds: &[Vec<u8>], program: &str) -> String {
    for bump in (1..=255u8).rev() {
        let mut h = Sha256::new();
        for s in seeds {
            h.update(s);
        }
        h.update([bump]);
        h.update(bytes(program));
        h.update(b"ProgramDerivedAddress");
        let raw: [u8; 32] = h.finalize().into();
        if CompressedEdwardsY(raw).decompress().is_none() {
            return bs58::encode(raw).into_string();
        }
    }
    panic!("model PDA");
}
fn token_row(index: u32, mint: &str, owner: &str, raw: u64) -> TokenBalance {
    TokenBalance {
        account_index: index,
        mint: mint.into(),
        owner: owner.into(),
        program_id: TOKEN.into(),
        ui_token_amount: Some(UiTokenAmount {
            amount: raw.to_string(),
            decimals: 9,
            ui_amount: raw as f64 / 1e9,
            ui_amount_string: (raw as f64 / 1e9).to_string(),
        }),
    }
}
fn raw(program: u32, accounts: &[u8], data: Vec<u8>) -> CompiledInstruction {
    CompiledInstruction {
        program_id_index: program,
        accounts: accounts.to_vec(),
        data,
    }
}
fn inner(program: u32, accounts: &[u8], data: Vec<u8>, depth: u32) -> InnerInstruction {
    InnerInstruction {
        program_id_index: program,
        accounts: accounts.to_vec(),
        data,
        stack_height: Some(depth),
    }
}
fn creation(account: u8, mint: u8, owner: &str, index: u32) -> InnerInstructions {
    let mut create = 0u32.to_le_bytes().to_vec();
    create.extend(2_039_280u64.to_le_bytes());
    create.extend(165u64.to_le_bytes());
    create.extend(bytes(TOKEN));
    let mut init = vec![18];
    init.extend(bytes(owner));
    InnerInstructions {
        index,
        instructions: vec![
            inner(5, &[mint], vec![21, 7, 0], 2),
            inner(6, &[0, account], create, 2),
            inner(5, &[account], vec![22], 2),
            inner(5, &[account, mint], init, 2),
        ],
    }
}
fn transfer(source: u8, destination: u8, authority: u8, raw: u64) -> InnerInstruction {
    inner(
        5,
        &[source, destination, authority],
        [vec![3], raw.to_le_bytes().to_vec()].concat(),
        3,
    )
}
pub(super) fn buy(fresh: bool, event: bool, tag: u8, input: u64) -> SubscribeUpdateTransaction {
    let wallet = bs58::encode([11u8; 32]).into_string();
    let associated = |mint: &str| derived(&[bytes(&wallet), bytes(TOKEN), bytes(mint)], ATA);
    let mut keys = vec![
        wallet.clone(),
        associated(MINT),
        associated(SOL),
        MINT.into(),
        SOL.into(),
        TOKEN.into(),
        SYSTEM.into(),
        ATA.into(),
        JUP.into(),
        derived(&[b"__event_authority".to_vec()], JUP),
        AMM.into(),
    ];
    keys.extend((61..=73).map(|n| bs58::encode([n; 32]).into_string()));
    keys.push("ComputeBudget111111111111111111111111111111".into());
    let r = if tag == 105 {
        vec![10, 5, 11, 12, 14, 15, 2, 1, 0]
    } else {
        vec![
            10, 5, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 2, 1, 0,
        ]
    };
    let mut route_accounts = vec![5, 0, 2, 1, 8, 3, 8, 9, 8];
    route_accounts.extend(&r);
    let mut route = vec![229, 23, 203, 151, 122, 227, 173, 42];
    route.extend(1u32.to_le_bytes());
    route.extend([tag, 100, 0, 1]);
    route.extend(input.to_le_bytes());
    route.extend(OUTPUT.to_le_bytes());
    route.extend(50u16.to_le_bytes());
    route.push(0);
    let mut swap = vec![if tag == 105 { 16 } else { 9 }];
    swap.extend(input.to_le_bytes());
    swap.extend((OUTPUT * 9950 / 10000).to_le_bytes());
    let mut cpis = vec![
        inner(10, &r[1..], swap, 2),
        transfer(2, 15, 0, input),
        transfer(14, 1, 12, OUTPUT),
    ];
    if event {
        let mut data = vec![
            228, 69, 165, 46, 81, 203, 154, 29, 64, 198, 205, 232, 38, 8, 113, 226,
        ];
        data.extend(bytes(&keys[11]));
        data.extend(bytes(SOL));
        data.extend(input.to_le_bytes());
        data.extend(bytes(MINT));
        data.extend(OUTPUT.to_le_bytes());
        cpis.push(inner(8, &[9], data, 2));
    }
    let mut groups = vec![creation(2, 4, &wallet, 0)];
    if fresh {
        groups.push(creation(1, 3, &wallet, 1));
    }
    groups.push(InnerInstructions {
        index: ROUTE_INDEX as u32,
        instructions: cpis,
    });
    let mut pre = vec![0; keys.len()];
    let mut post = pre.clone();
    pre[0] = 1_000_000_000;
    post[0] = pre[0] - input - 19_000 - if fresh { 2_039_280 } else { 0 };
    pre[1] = if fresh { 0 } else { 2_039_280 };
    post[1] = 2_039_280;
    pre[15] = 1_002_039_280;
    post[15] = pre[15] + input;
    pre[14] = 2_039_280;
    post[14] = 2_039_280;
    let mut before = vec![
        token_row(14, MINT, &keys[12], 500_000_000),
        token_row(15, SOL, &keys[12], 1_000_000_000),
    ];
    if !fresh {
        before.insert(0, token_row(1, MINT, &wallet, 0));
    }
    let message = Message {
        header: Some(MessageHeader {
            num_required_signatures: 1,
            ..Default::default()
        }),
        account_keys: keys.iter().map(|k| bytes(k)).collect(),
        recent_blockhash: vec![9; 32],
        instructions: vec![
            raw(7, &[0, 2, 0, 4, 6, 5], vec![1]),
            raw(7, &[0, 1, 0, 3, 6, 5], vec![1]),
            raw(
                6,
                &[0, 2],
                [2u32.to_le_bytes().to_vec(), input.to_le_bytes().to_vec()].concat(),
            ),
            raw(5, &[2], vec![17]),
            raw(8, &route_accounts, route),
            raw(5, &[2, 0, 0], vec![9]),
        ],
        ..Default::default()
    };
    let meta = TransactionStatusMeta {
        fee: 19_000,
        pre_balances: pre,
        post_balances: post,
        pre_token_balances: before,
        post_token_balances: vec![
            token_row(1, MINT, &wallet, OUTPUT),
            token_row(14, MINT, &keys[12], 500_000_000 - OUTPUT),
            token_row(15, SOL, &keys[12], 1_000_000_000 + input),
        ],
        inner_instructions: groups,
        ..Default::default()
    };
    SubscribeUpdateTransaction {
        slot: 451313057,
        transaction: Some(SubscribeUpdateTransactionInfo {
            signature: vec![42; 64],
            transaction: Some(Transaction {
                signatures: vec![vec![42; 64]],
                message: Some(message),
            }),
            meta: Some(meta),
            ..Default::default()
        }),
        ..Default::default()
    }
}
