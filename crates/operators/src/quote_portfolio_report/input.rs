use anyhow::{bail, ensure, Context, Result};
use serde::{Deserialize, Serialize};
use std::{fs::File, io::Read, path::Path};

pub const MAX_BYTES: u64 = 1_048_576;
pub const MAX_EVENTS: usize = 256;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Origin {
    Observed(String),
    Assumed(String),
    Synthetic(String),
}
impl Origin {
    pub fn assumed(&self) -> bool {
        !matches!(self, Self::Observed(_))
    }
    pub fn validate(&self) -> Result<()> {
        let (Self::Observed(s) | Self::Assumed(s) | Self::Synthetic(s)) = self;
        ensure!(!s.trim().is_empty(), "empty provenance");
        Ok(())
    }
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Presence {
    Known(String),
    Unknown(String),
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Amount {
    pub value: Presence,
    pub provenance: Origin,
}
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Initial {
    pub cash_lamports: Amount,
    pub max_open_positions: Amount,
    pub inventory_raw: Amount,
    pub external_transfers_lamports: Amount,
}
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Window {
    pub start_unix_ms: String,
    pub end_unix_ms: String,
    pub input_complete: bool,
    pub input_provenance: Origin,
    pub source_coverage_assertion: String,
}
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Input {
    pub version: u32,
    pub scenario_provenance: Origin,
    pub initial: Initial,
    pub window: Window,
    pub events: Vec<EventInput>,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EventInput {
    pub id: String,
    pub position_id: String,
    pub sequence: String,
    /// Declared virtual application instant, not source leader signal time.
    pub unix_ms: String,
    /// Caller assertion for order/time and virtual position association, not DB proof.
    pub identity_provenance: Origin,
    pub action: ActionInput,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ActionInput {
    Buy {
        mint: String,
        decimals: u8,
        input_lamports: String,
        quote: Option<QuoteRef>,
        costs: CostsInput,
        rent_deposit: Option<Amount>,
    },
    Sell {
        raw: String,
        quote: Option<QuoteRef>,
        costs: CostsInput,
    },
    Mark {
        quote: Option<QuoteRef>,
        costs: CostsInput,
    },
    RentRefund {
        deposit_event_id: String,
        amount: Option<Amount>,
    },
    UnsupportedExpense {
        description: String,
        amount: Option<Amount>,
    },
    /// Portfolio-level charge; envelope position_id is a correlation label only.
    FailedAttemptExpense {
        amount: Option<Amount>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        receipt_ref: Option<ReceiptRef>,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReceiptRef {
    pub order_id: String,
    pub tx_signature: String,
    pub wallet: String,
    pub payer: String,
    pub operation_at: String,
    pub recorded_at: String,
    /// Caller identifies evidence origin; table presence never upgrades it.
    pub source_provenance: Origin,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CostsInput {
    pub event_id: String,
    pub position_id: String,
    pub side: Side,
    pub base: Option<Amount>,
    pub priority: Option<Amount>,
    pub setup: Option<Amount>,
    pub exit: Option<Amount>,
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Buy,
    Sell,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QuoteRef {
    pub event_id: String,
    pub wallet_id: String,
    pub signal_id: Option<String>,
    pub shadow_closed_trade_id: Option<String>,
    pub request_ts: String,
    pub position_id: String,
    pub mint: String,
    pub decimals: u8,
    /// Explicit metadata reference, never a numeric override. Absent keeps strict root proof.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decimals_source: Option<DecimalsSource>,
    pub side: Side,
    pub input_raw: String,
    pub output_raw: String,
    pub provenance: Origin,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DecimalsSource {
    ObservedSignature { signature: String },
}

pub fn raw(s: &str) -> Result<u64> {
    ensure!(
        !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()),
        "expected unsigned decimal string"
    );
    ensure!(
        s == "0" || !s.starts_with('0'),
        "noncanonical decimal string"
    );
    s.parse().context("raw amount exceeds u64")
}
pub fn load(path: &Path) -> Result<Input> {
    let file = File::open(path).context("portfolio input unreadable")?;
    ensure!(
        file.metadata()?.is_file(),
        "portfolio input must be a regular file"
    );
    let mut bytes = Vec::new();
    file.take(MAX_BYTES + 1).read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 <= MAX_BYTES,
        "portfolio input exceeds {MAX_BYTES} bytes"
    );
    let input: Input = serde_json::from_slice(&bytes).context("invalid portfolio input")?;
    ensure!(input.version == 1, "unsupported portfolio input version");
    ensure!(
        input.events.len() <= MAX_EVENTS,
        "portfolio input exceeds {MAX_EVENTS} events"
    );
    input.scenario_provenance.validate()?;
    input.window.input_provenance.validate()?;
    ensure!(
        !input.window.source_coverage_assertion.trim().is_empty(),
        "missing source coverage assertion"
    );
    let start = raw(&input.window.start_unix_ms)?;
    let end = raw(&input.window.end_unix_ms)?;
    super::time_binding::instant(start).context("invalid input window start")?;
    super::time_binding::instant(end).context("invalid input window end")?;
    ensure!(start <= end, "inverted input window");
    for e in &input.events {
        e.identity_provenance.validate()?;
        raw(&e.sequence)?;
        let transition = raw(&e.unix_ms)?;
        super::time_binding::instant(transition).context("invalid event transition time")?;
        if !(start..=end).contains(&transition) {
            bail!("event outside declared input window");
        }
    }
    Ok(input)
}
