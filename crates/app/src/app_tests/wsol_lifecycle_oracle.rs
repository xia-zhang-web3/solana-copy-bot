//! LOCAL semantic model for decoded prepared instructions, not a Solana VM or live quote.
//! System/Token effects are modeled; Raydium consumes/produces the already verified ExactIn
//! operands. Signature verification, compute scheduling, rent changes and real CPI are excluded.
use crate::execution_pumpswap_accounts::*;
use crate::execution_solana_tx::PubkeyBytes;
use crate::execution_transaction_wire::decode_message;
use anyhow::{ensure, Context, Result};
use serde_json::{json, Value};
use std::collections::BTreeMap;
pub(super) const RENT: u64 = 2_039_280;
pub(super) const FEE: u64 = 5_000;
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Account {
    pub lamports: u64,
    pub token: Option<(PubkeyBytes, PubkeyBytes, u64)>,
    pub initialized: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Bank {
    pub accounts: BTreeMap<PubkeyBytes, Account>,
    pub wallet: PubkeyBytes,
}
impl Bank {
    pub fn new(wallet: PubkeyBytes) -> Result<Self> {
        let usdc = parse_pubkey(crate::execution_owner_buy_wire::USDC, "local_usdc")?;
        let ata = associated_token_address(&wallet, &usdc, &token_program_id());
        let mut accounts = BTreeMap::new();
        accounts.insert(
            wallet,
            Account {
                lamports: 1_000_000_000,
                token: None,
                initialized: false,
            },
        );
        accounts.insert(
            ata,
            Account {
                lamports: RENT,
                token: Some((usdc, wallet, 100_000_000)),
                initialized: true,
            },
        );
        for (key, mint) in [([63; 32], usdc), ([64; 32], wsol_mint())] {
            accounts.insert(
                key,
                Account {
                    lamports: if mint == wsol_mint() {
                        RENT + 1_000_000_000
                    } else {
                        RENT
                    },
                    token: Some((mint, [62; 32], 1_000_000_000)),
                    initialized: true,
                },
            );
        }
        Ok(Self { accounts, wallet })
    }
    pub fn canonical(&mut self, raw: u64) {
        let ata = associated_token_address(&self.wallet, &wsol_mint(), &token_program_id());
        self.accounts.insert(
            ata,
            Account {
                lamports: RENT + raw,
                token: Some((wsol_mint(), self.wallet, raw)),
                initialized: true,
            },
        );
    }
    pub fn execute(&mut self, payload: &str) -> Result<Value> {
        let m = decode_message(payload, |_| Ok(()))?;
        let before = self.clone();
        let mut work = self.clone();
        work.accounts.get_mut(&self.wallet).unwrap().lamports -= FEE;
        let mut inner = Vec::new();
        let result = (|| -> Result<()> {
            for (index, i) in m.instructions.iter().enumerate() {
                let a = &i.accounts;
                let p = i.program.pubkey;
                if p == compute_budget_program_id() {
                    continue;
                }
                if p == associated_token_program_id() {
                    let key = a[1].pubkey;
                    let mint = a[3].pubkey;
                    if let Some(old) = work.accounts.get(&key) {
                        ensure!(
                            old.token
                                .is_some_and(|(m, o, _)| m == mint && o == self.wallet),
                            "model_ata_wrong_identity"
                        );
                    } else {
                        work.accounts.get_mut(&self.wallet).unwrap().lamports -= RENT;
                        work.accounts.insert(
                            key,
                            Account {
                                lamports: RENT,
                                token: Some((mint, self.wallet, 0)),
                                initialized: true,
                            },
                        );
                    }
                } else if p == system_program_id() {
                    match u32::from_le_bytes(i.data[..4].try_into()?) {
                        3 => {
                            let n =
                                usize::try_from(u64::from_le_bytes(i.data[36..44].try_into()?))?;
                            let seed = std::str::from_utf8(&i.data[44..44 + n])?;
                            let rent = u64::from_le_bytes(i.data[44 + n..52 + n].try_into()?);
                            ensure!(
                                !work.accounts.contains_key(&a[1].pubkey),
                                "model_account_already_in_use"
                            );
                            let expected = crate::execution_wsol_lifecycle::create(
                                self.wallet,
                                a[1].pubkey,
                                seed,
                                rent,
                            );
                            ensure!(
                                expected.data == i.data
                                    && u64::from_le_bytes(i.data[52 + n..60 + n].try_into()?)
                                        == 165,
                                "model_create_shape"
                            );
                            work.accounts.get_mut(&self.wallet).unwrap().lamports = work.accounts
                                [&self.wallet]
                                .lamports
                                .checked_sub(rent)
                                .context("model_native_short")?;
                            work.accounts.insert(
                                a[1].pubkey,
                                Account {
                                    lamports: rent,
                                    token: None,
                                    initialized: false,
                                },
                            );
                        }
                        2 => {
                            let amount = u64::from_le_bytes(i.data[4..].try_into()?);
                            let from = a[0].pubkey;
                            let to = a[1].pubkey;
                            ensure!(
                                work.accounts[&from].lamports >= amount,
                                "model_native_short"
                            );
                            if from != to {
                                work.accounts.get_mut(&from).unwrap().lamports -= amount;
                                work.accounts
                                    .get_mut(&to)
                                    .context("model_missing_transfer_destination")?
                                    .lamports += amount;
                            }
                        }
                        _ => anyhow::bail!("model_unsupported_system"),
                    }
                } else if p == token_program_id() {
                    match i.data[0] {
                        18 => {
                            let t = work
                                .accounts
                                .get_mut(&a[0].pubkey)
                                .context("model_initialize_missing")?;
                            ensure!(
                                !t.initialized && t.lamports == RENT,
                                "model_initialize_invalid"
                            );
                            t.initialized = true;
                            t.token = Some((a[1].pubkey, i.data[1..].try_into()?, 0));
                        }
                        17 => {
                            let t = work.accounts.get_mut(&a[0].pubkey).unwrap();
                            let (m, o, _) = t.token.context("model_sync_uninitialized")?;
                            ensure!(m == wsol_mint(), "model_sync_mint");
                            t.token = Some((
                                m,
                                o,
                                t.lamports.checked_sub(RENT).context("model_sync_rent")?,
                            ));
                        }
                        9 => {
                            let t = work
                                .accounts
                                .get(&a[0].pubkey)
                                .context("model_close_missing")?;
                            ensure!(
                                t.token
                                    .is_some_and(|(m, o, _)| m == wsol_mint() && o == a[2].pubkey),
                                "model_close_authority"
                            );
                            let t = work.accounts.remove(&a[0].pubkey).unwrap();
                            work.accounts.get_mut(&a[1].pubkey).unwrap().lamports += t.lamports;
                        }
                        _ => anyhow::bail!("model_unsupported_token"),
                    }
                } else {
                    ensure!(
                        p == parse_pubkey(
                            crate::execution_owner_buy_wire::JUPITER,
                            "local_jupiter"
                        )?,
                        "model_unsupported_program"
                    );
                    let input = u64::from_le_bytes(i.data[16..24].try_into()?);
                    let output = u64::from_le_bytes(i.data[24..32].try_into()?);
                    let src = a[2].pubkey;
                    let dst = a[3].pubkey;
                    let src_mint = work.accounts[&src].token.context("model_source_token")?.0;
                    let (input_vault, output_vault) = if src_mint == wsol_mint() {
                        ([64; 32], [63; 32])
                    } else {
                        ([63; 32], [64; 32])
                    };
                    for (from, to, amount) in
                        [(src, input_vault, input), (output_vault, dst, output)]
                    {
                        let source = work.accounts.get_mut(&from).context("model_route_source")?;
                        let (mint, owner, raw) = source.token.context("model_route_token")?;
                        ensure!(raw >= amount, "model_token_shortage");
                        source.token = Some((mint, owner, raw - amount));
                        if mint == wsol_mint() {
                            source.lamports -= amount;
                        }
                        let target = work
                            .accounts
                            .get_mut(&to)
                            .context("model_route_destination")?;
                        let (mint, owner, raw) = target.token.context("model_route_token")?;
                        target.token = Some((mint, owner, raw + amount));
                        if mint == wsol_mint() {
                            target.lamports += amount;
                        }
                    }
                    let raw = |program: PubkeyBytes,
                               accounts: Vec<PubkeyBytes>,
                               data: Vec<u8>,
                               depth: u32| json!({"programId":bs58::encode(program).into_string(),"accounts":accounts.iter().map(|a|bs58::encode(a).into_string()).collect::<Vec<_>>(),"data":bs58::encode(data).into_string(),"stackHeight":depth});
                    let mut amm = vec![16];
                    amm.extend(input.to_le_bytes());
                    amm.extend(output.to_le_bytes());
                    let mut transfer = vec![3];
                    transfer.extend(input.to_le_bytes());
                    let mut received = vec![3];
                    received.extend(output.to_le_bytes());
                    inner.push(json!({"index":index,"instructions":[raw(a[9].pubkey,a[10..].iter().map(|a|a.pubkey).collect(),amm,2),raw(token_program_id(),vec![src,input_vault,self.wallet],transfer,3),raw(token_program_id(),vec![output_vault,dst,[62;32]],received,3)]}));
                }
            }
            Ok(())
        })();
        if let Err(e) = result {
            *self = before;
            self.accounts.get_mut(&self.wallet).unwrap().lamports -= FEE;
            return Err(e);
        }
        *self = work;
        let keys = &m.binding.accounts;
        let balances = |bank: &Bank| {
            keys.iter()
                .map(|k| bank.accounts.get(&k.pubkey).map_or(0, |a| a.lamports))
                .collect::<Vec<_>>()
        };
        let tokens = |bank: &Bank| {
            keys.iter().enumerate().filter_map(|(n,k)|bank.accounts.get(&k.pubkey)?.token.map(|(mint,owner,raw)|json!({"accountIndex":n,"owner":bs58::encode(owner).into_string(),"programId":bs58::encode(token_program_id()).into_string(),"mint":bs58::encode(mint).into_string(),"uiTokenAmount":{"amount":raw.to_string(),"decimals":if mint==wsol_mint(){9}else{6},"uiAmountString":(raw as f64/if mint==wsol_mint(){1e9}else{1e6}).to_string()}}))).collect::<Vec<_>>()
        };
        let parsed=m.instructions.iter().map(|i|{
            let a=&i.accounts;let key=|n:usize|bs58::encode(a[n].pubkey).into_string();
            let(kind,info)=if i.program.pubkey==system_program_id() && i.data[..4]==3_u32.to_le_bytes(){let n=usize::try_from(u64::from_le_bytes(i.data[36..44].try_into().unwrap())).unwrap();("createAccountWithSeed",json!({"source":key(0),"newAccount":key(1),"base":bs58::encode(&i.data[4..36]).into_string(),"seed":std::str::from_utf8(&i.data[44..44+n]).unwrap(),"lamports":u64::from_le_bytes(i.data[44+n..52+n].try_into().unwrap()),"space":165,"owner":bs58::encode(token_program_id()).into_string()}))}
            else if i.program.pubkey==system_program_id(){("transfer",json!({"source":key(0),"destination":key(1),"lamports":u64::from_le_bytes(i.data[4..].try_into().unwrap())}))}
            else if i.program.pubkey==token_program_id(){match i.data[0]{18=>("initializeAccount3",json!({"account":key(0),"mint":key(1),"owner":bs58::encode(&i.data[1..]).into_string()})),17=>("syncNative",json!({"account":key(0)})),_=>("closeAccount",json!({"account":key(0),"destination":key(1),"owner":key(2)}))}}
            else if i.program.pubkey==associated_token_program_id(){("createIdempotent",json!({"source":key(0),"account":key(1),"wallet":key(2),"mint":key(3),"tokenProgram":key(5),"systemProgram":key(4)}))}
            else{return json!({"programId":bs58::encode(i.program.pubkey).into_string(),"accounts":a.iter().map(|a|bs58::encode(a.pubkey).into_string()).collect::<Vec<_>>(),"data":bs58::encode(&i.data).into_string()})};
            json!({"programId":bs58::encode(i.program.pubkey).into_string(),"parsed":{"type":kind,"info":info}})
        }).collect::<Vec<_>>();
        Ok(
            json!({"jsonrpc":"2.0","result":{"slot":151,"version":"legacy","transaction":{"signatures":[bs58::encode([231;64]).into_string()],"message":{"recentBlockhash":bs58::encode(m.recent_blockhash).into_string(),"accountKeys":keys.iter().map(|k|json!({"pubkey":bs58::encode(k.pubkey).into_string(),"signer":k.is_signer,"writable":k.is_writable})).collect::<Vec<_>>(),"instructions":parsed}},"meta":{"err":null,"fee":FEE,"preBalances":balances(&before),"postBalances":balances(self),"preTokenBalances":tokens(&before),"postTokenBalances":tokens(self),"innerInstructions":inner}}}),
        )
    }
}
