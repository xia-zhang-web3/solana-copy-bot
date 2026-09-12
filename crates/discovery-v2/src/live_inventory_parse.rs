use crate::live_inventory::LiveTokenPosition;
use crate::live_inventory::{InventoryFailure as Failure, TokenProgram};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, PartialEq, Eq)]
struct TokenAccount {
    program: TokenProgram,
    mint: String,
    raw: u64,
    decimals: u8,
    state: String,
}

pub(crate) struct InventoryParser {
    max_accounts: usize,
    rows_seen: usize,
    accounts: BTreeMap<String, TokenAccount>,
}

impl InventoryParser {
    pub(crate) fn new(max_accounts: usize) -> Self {
        Self {
            max_accounts,
            rows_seen: 0,
            accounts: BTreeMap::new(),
        }
    }

    pub(crate) fn append(
        &mut self,
        response: &Value,
        wallet: &str,
        program: TokenProgram,
    ) -> Result<usize, Failure> {
        let rows = response
            .pointer("/result/value")
            .and_then(Value::as_array)
            .ok_or(Failure::Protocol)?;
        self.rows_seen = self
            .rows_seen
            .checked_add(rows.len())
            .ok_or(Failure::Budget)?;
        if self.rows_seen > self.max_accounts {
            return Err(Failure::Budget);
        }
        let mut unique = 0;
        for row in rows {
            let (pubkey, account) = parse_account(row, wallet, program)?;
            if let Some(previous) = self.accounts.get(&pubkey) {
                if previous != &account {
                    return Err(Failure::Conflict);
                }
            } else {
                self.accounts.insert(pubkey, account);
                unique += 1;
            }
        }
        Ok(unique)
    }

    pub(crate) fn finish(self) -> Result<Vec<LiveTokenPosition>, Failure> {
        let mut totals = BTreeMap::<(TokenProgram, String), (u8, u128)>::new();
        for row in self.accounts.into_values() {
            let total = totals
                .entry((row.program, row.mint))
                .or_insert((row.decimals, 0));
            if total.0 != row.decimals {
                return Err(Failure::Conflict);
            }
            total.1 = total
                .1
                .checked_add(u128::from(row.raw))
                .ok_or(Failure::Malformed)?;
        }
        totals
            .into_iter()
            .filter(|(_, (_, raw))| *raw > 0)
            .map(|((program, mint), (decimals, raw))| {
                let amount = raw as f64 / 10f64.powi(i32::from(decimals));
                if !amount.is_finite() || amount <= 0.0 {
                    return Err(Failure::Malformed);
                }
                Ok(LiveTokenPosition {
                    mint,
                    amount,
                    program,
                })
            })
            .collect()
    }
}

fn parse_account(
    row: &Value,
    wallet: &str,
    program: TokenProgram,
) -> Result<(String, TokenAccount), Failure> {
    let pubkey = identity(row.get("pubkey"))?;
    let account = row
        .get("account")
        .and_then(Value::as_object)
        .ok_or(Failure::Malformed)?;
    if account.get("owner").and_then(Value::as_str) != Some(program.id())
        || account.get("executable").and_then(Value::as_bool) != Some(false)
    {
        return Err(Failure::Malformed);
    }
    let data = account
        .get("data")
        .and_then(Value::as_object)
        .ok_or(Failure::Unsupported)?;
    if data.get("program").and_then(Value::as_str) != Some(program.parsed_name()) {
        return Err(Failure::Unsupported);
    }
    let parsed = data
        .get("parsed")
        .and_then(Value::as_object)
        .ok_or(Failure::Malformed)?;
    if parsed.get("type").and_then(Value::as_str) != Some("account") {
        return Err(Failure::Unsupported);
    }
    let info = parsed
        .get("info")
        .and_then(Value::as_object)
        .ok_or(Failure::Malformed)?;
    if identity(info.get("owner"))? != wallet {
        return Err(Failure::Malformed);
    }
    let mint = identity(info.get("mint"))?;
    let state = info
        .get("state")
        .and_then(Value::as_str)
        .ok_or(Failure::Malformed)?;
    if !matches!(state, "initialized" | "frozen") {
        return Err(Failure::Unsupported);
    }
    let amount = info
        .get("tokenAmount")
        .and_then(Value::as_object)
        .ok_or(Failure::Malformed)?;
    let raw = amount
        .get("amount")
        .and_then(Value::as_str)
        .ok_or(Failure::Malformed)?;
    if raw.is_empty() || !raw.bytes().all(|c| c.is_ascii_digit()) {
        return Err(Failure::Malformed);
    }
    let raw = raw.parse::<u64>().map_err(|_| Failure::Malformed)?;
    let decimals = amount
        .get("decimals")
        .and_then(Value::as_u64)
        .and_then(|n| u8::try_from(n).ok())
        .ok_or(Failure::Malformed)?;
    Ok((
        pubkey.to_string(),
        TokenAccount {
            program,
            mint: mint.to_string(),
            raw,
            decimals,
            state: state.to_string(),
        },
    ))
}

fn identity(value: Option<&Value>) -> Result<&str, Failure> {
    let text = value.and_then(Value::as_str).ok_or(Failure::Malformed)?;
    if valid_pubkey(text) {
        Ok(text)
    } else {
        Err(Failure::Malformed)
    }
}

/// Bounded 32-byte identity validation only; no curve/signature or token-risk claim.
pub(crate) fn valid_pubkey(text: &str) -> bool {
    const ALPHABET: &[u8] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
    if !(32..=44).contains(&text.len()) {
        return false;
    }
    let mut decoded = [0u8; 32];
    for byte in text.bytes() {
        let Some(value) = ALPHABET.iter().position(|c| *c == byte) else {
            return false;
        };
        let mut carry = value as u16;
        for cell in decoded.iter_mut().rev() {
            carry += u16::from(*cell) * 58;
            *cell = carry as u8;
            carry >>= 8;
        }
        if carry != 0 {
            return false;
        }
    }
    let leading_zeros = text.bytes().take_while(|b| *b == b'1').count();
    leading_zeros + decoded.iter().skip_while(|b| **b == 0).count() == 32
}
