use crate::execution_solana_tx::PubkeyBytes;
use anyhow::{anyhow, Context, Result};
use curve25519_dalek::edwards::CompressedEdwardsY;
use sha2::{Digest, Sha256};

pub(crate) fn wsol_mint() -> PubkeyBytes {
    const_pubkey("So11111111111111111111111111111111111111112")
}

pub(crate) fn token_program_id() -> PubkeyBytes {
    const_pubkey("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
}

pub(crate) fn associated_token_program_id() -> PubkeyBytes {
    const_pubkey("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL")
}

pub(crate) fn system_program_id() -> PubkeyBytes {
    const_pubkey("11111111111111111111111111111111")
}

pub(crate) fn compute_budget_program_id() -> PubkeyBytes {
    const_pubkey("ComputeBudget111111111111111111111111111111")
}

pub(crate) fn pump_amm_program_id() -> PubkeyBytes {
    const_pubkey("pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA")
}

pub(crate) fn pump_fee_program_id() -> PubkeyBytes {
    const_pubkey("pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ")
}

const POOL_DISCRIMINATOR: [u8; 8] = [241, 154, 109, 4, 17, 177, 109, 188];
const GLOBAL_CONFIG_DISCRIMINATOR: [u8; 8] = [149, 8, 156, 202, 160, 252, 176, 217];
const PDA_MARKER: &[u8] = b"ProgramDerivedAddress";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PumpSwapPool {
    pub(crate) base_mint: PubkeyBytes,
    pub(crate) quote_mint: PubkeyBytes,
    pub(crate) pool_base_token_account: PubkeyBytes,
    pub(crate) pool_quote_token_account: PubkeyBytes,
    pub(crate) coin_creator: PubkeyBytes,
    pub(crate) is_mayhem_mode: bool,
    pub(crate) is_cashback_coin: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PumpSwapGlobalConfig {
    pub(crate) protocol_fee_recipients: [PubkeyBytes; 8],
    pub(crate) reserved_fee_recipient: PubkeyBytes,
    pub(crate) reserved_fee_recipients: [PubkeyBytes; 7],
    pub(crate) buyback_fee_recipients: [PubkeyBytes; 8],
}

impl PumpSwapGlobalConfig {
    pub(crate) fn fee_recipient(&self, is_mayhem_mode: bool) -> PubkeyBytes {
        if is_mayhem_mode {
            first_non_default(
                std::iter::once(&self.reserved_fee_recipient)
                    .chain(self.reserved_fee_recipients.iter()),
            )
        } else {
            first_non_default(self.protocol_fee_recipients.iter())
        }
    }

    pub(crate) fn buyback_fee_recipient(&self) -> PubkeyBytes {
        first_non_default(self.buyback_fee_recipients.iter())
    }
}

pub(crate) fn parse_pubkey(value: &str, field: &str) -> Result<PubkeyBytes> {
    let bytes = bs58::decode(value.trim())
        .into_vec()
        .map_err(|error| anyhow!("invalid {field}: {error}"))?;
    pubkey_from_slice(&bytes).ok_or_else(|| anyhow!("invalid {field}: expected 32 bytes"))
}

pub(crate) fn format_pubkey(pubkey: &PubkeyBytes) -> String {
    bs58::encode(pubkey).into_string()
}

pub(crate) fn global_config_pda() -> PubkeyBytes {
    pda(&[b"global_config"], &pump_amm_program_id())
}

pub(crate) fn event_authority_pda() -> PubkeyBytes {
    pda(&[b"__event_authority"], &pump_amm_program_id())
}

pub(crate) fn pump_amm_fee_config_pda() -> PubkeyBytes {
    pda(
        &[b"fee_config", &pump_amm_program_id()],
        &pump_fee_program_id(),
    )
}

pub(crate) fn user_volume_accumulator_pda(user: &PubkeyBytes) -> PubkeyBytes {
    pda(&[b"user_volume_accumulator", user], &pump_amm_program_id())
}

pub(crate) fn coin_creator_vault_authority_pda(coin_creator: &PubkeyBytes) -> PubkeyBytes {
    pda(&[b"creator_vault", coin_creator], &pump_amm_program_id())
}

pub(crate) fn pool_v2_pda(base_mint: &PubkeyBytes) -> PubkeyBytes {
    pda(&[b"pool-v2", base_mint], &pump_amm_program_id())
}

pub(crate) fn associated_token_address(
    owner: &PubkeyBytes,
    mint: &PubkeyBytes,
    token_program: &PubkeyBytes,
) -> PubkeyBytes {
    pda(
        &[owner, token_program, mint],
        &associated_token_program_id(),
    )
}

pub(crate) fn decode_pool_account(data: &[u8]) -> Result<PumpSwapPool> {
    let mut cursor = AccountCursor::new(data, "PumpSwap pool")?;
    cursor.expect_discriminator(POOL_DISCRIMINATOR)?;
    let _pool_bump = cursor.read_u8()?;
    let _index = cursor.read_u16()?;
    let _creator = cursor.read_pubkey()?;
    let base_mint = cursor.read_pubkey()?;
    let quote_mint = cursor.read_pubkey()?;
    let _lp_mint = cursor.read_pubkey()?;
    let pool_base_token_account = cursor.read_pubkey()?;
    let pool_quote_token_account = cursor.read_pubkey()?;
    let _lp_supply = cursor.read_u64()?;
    let coin_creator = cursor.read_pubkey()?;
    let is_mayhem_mode = cursor.read_bool()?;
    let is_cashback_coin = cursor.read_bool()?;
    Ok(PumpSwapPool {
        base_mint,
        quote_mint,
        pool_base_token_account,
        pool_quote_token_account,
        coin_creator,
        is_mayhem_mode,
        is_cashback_coin,
    })
}

pub(crate) fn decode_global_config_account(data: &[u8]) -> Result<PumpSwapGlobalConfig> {
    let mut cursor = AccountCursor::new(data, "PumpSwap global config")?;
    cursor.expect_discriminator(GLOBAL_CONFIG_DISCRIMINATOR)?;
    let _admin = cursor.read_pubkey()?;
    let _lp_fee_basis_points = cursor.read_u64()?;
    let _protocol_fee_basis_points = cursor.read_u64()?;
    let _disable_flags = cursor.read_u8()?;
    let protocol_fee_recipients = cursor.read_pubkey_array::<8>()?;
    let _coin_creator_fee_basis_points = cursor.read_u64()?;
    let _admin_set_coin_creator_authority = cursor.read_pubkey()?;
    let _whitelist_pda = cursor.read_pubkey()?;
    let reserved_fee_recipient = cursor.read_pubkey()?;
    let _mayhem_mode_enabled = cursor.read_bool()?;
    let reserved_fee_recipients = cursor.read_pubkey_array::<7>()?;
    let _is_cashback_enabled = cursor.read_bool()?;
    let buyback_fee_recipients = cursor.read_pubkey_array::<8>()?;
    let _buyback_basis_points = cursor.read_u64()?;
    Ok(PumpSwapGlobalConfig {
        protocol_fee_recipients,
        reserved_fee_recipient,
        reserved_fee_recipients,
        buyback_fee_recipients,
    })
}

fn pda(seeds: &[&[u8]], program_id: &PubkeyBytes) -> PubkeyBytes {
    for bump in (0_u8..=u8::MAX).rev() {
        let bump_seed = [bump];
        let mut with_bump = seeds.to_vec();
        with_bump.push(&bump_seed);
        if let Some(address) = create_program_address(&with_bump, program_id) {
            return address;
        }
    }
    panic!("unable to derive Solana PDA");
}

fn create_program_address(seeds: &[&[u8]], program_id: &PubkeyBytes) -> Option<PubkeyBytes> {
    let mut hasher = Sha256::new();
    for seed in seeds {
        hasher.update(seed);
    }
    hasher.update(program_id);
    hasher.update(PDA_MARKER);
    let hash = hasher.finalize();
    let address = pubkey_from_slice(&hash)?;
    if CompressedEdwardsY(address).decompress().is_some() {
        None
    } else {
        Some(address)
    }
}

fn const_pubkey(value: &str) -> PubkeyBytes {
    parse_pubkey(value, "constant pubkey").expect("valid constant pubkey")
}

fn pubkey_from_slice(slice: &[u8]) -> Option<PubkeyBytes> {
    if slice.len() != 32 {
        return None;
    }
    let mut bytes = [0_u8; 32];
    bytes.copy_from_slice(slice);
    Some(bytes)
}

fn first_non_default<'a>(items: impl Iterator<Item = &'a PubkeyBytes>) -> PubkeyBytes {
    items
        .into_iter()
        .copied()
        .find(|item| *item != [0_u8; 32])
        .unwrap_or_default()
}

struct AccountCursor<'a> {
    data: &'a [u8],
    offset: usize,
    label: &'static str,
}

impl<'a> AccountCursor<'a> {
    fn new(data: &'a [u8], label: &'static str) -> Result<Self> {
        if data.len() < 8 {
            anyhow::bail!("{label} account data is truncated");
        }
        Ok(Self {
            data,
            offset: 0,
            label,
        })
    }

    fn expect_discriminator(&mut self, expected: [u8; 8]) -> Result<()> {
        let actual = self.read_bytes::<8>()?;
        if actual != expected {
            anyhow::bail!("{} account discriminator mismatch", self.label);
        }
        Ok(())
    }

    fn read_u8(&mut self) -> Result<u8> {
        Ok(self.read_bytes::<1>()?[0])
    }

    fn read_u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.read_bytes::<2>()?))
    }

    fn read_u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.read_bytes::<8>()?))
    }

    fn read_bool(&mut self) -> Result<bool> {
        match self.read_u8()? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(anyhow!("{} account has invalid bool {value}", self.label)),
        }
    }

    fn read_pubkey(&mut self) -> Result<PubkeyBytes> {
        self.read_bytes::<32>()
    }

    fn read_pubkey_array<const N: usize>(&mut self) -> Result<[PubkeyBytes; N]> {
        let mut values = [[0_u8; 32]; N];
        for value in values.iter_mut() {
            *value = self.read_pubkey()?;
        }
        Ok(values)
    }

    fn read_bytes<const N: usize>(&mut self) -> Result<[u8; N]> {
        let end = self
            .offset
            .checked_add(N)
            .context("account cursor offset overflow")?;
        let slice = self.data.get(self.offset..end).ok_or_else(|| {
            anyhow!(
                "{} account data truncated at offset {}",
                self.label,
                self.offset
            )
        })?;
        self.offset = end;
        let mut bytes = [0_u8; N];
        bytes.copy_from_slice(slice);
        Ok(bytes)
    }
}
