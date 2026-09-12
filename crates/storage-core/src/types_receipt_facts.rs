use anyhow::{ensure, Result};
use copybot_core_types::{Lamports, SignedLamports};

macro_rules! coverage {
    ($name:ident { $($variant:ident => $text:literal),+ $(,)? }) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum $name { $($variant),+ }
        impl $name {
            pub fn as_str(self) -> &'static str {
                match self { $(Self::$variant => $text),+ }
            }
        }
        impl std::str::FromStr for $name {
            type Err = anyhow::Error;
            fn from_str(value: &str) -> Result<Self> {
                match value {
                    $($text => Ok(Self::$variant)),+,
                    _ => anyhow::bail!(concat!("invalid ", stringify!($name))),
                }
            }
        }
    };
}
coverage!(ReceiptFeeCoverage { Known => "known", Missing => "missing", Invalid => "invalid" });
coverage!(ReceiptTokenCoverage {
    PairedBalances => "paired_balances",
    ProvenLifecycle => "proven_lifecycle",
    Unresolved => "unresolved",
});
coverage!(ReceiptWsolCoverage { Observed => "observed", Unresolved => "unresolved" });
coverage!(ReceiptDecomposition { Unresolved => "unresolved" });

/// Exact aggregate delta for the requested wallet/mint after lifecycle validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReceiptTokenDelta {
    pub raw: i128,
    pub decimals: u8,
}

/// Observations of a successful receipt, never a fill, expense or swap proceeds.
/// SQLite stores every raw integer as canonical decimal TEXT, including u64::MAX.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionCanaryReceiptFacts {
    pub order_id: String,
    pub tx_signature: String,
    pub wallet_pubkey: String,
    pub token: String,
    pub side: String,
    pub slot: u64,
    pub wallet_native_pre: Lamports,
    pub wallet_native_post: Lamports,
    pub wallet_native_delta: SignedLamports,
    pub transaction_fee: Option<Lamports>,
    pub fee_coverage: ReceiptFeeCoverage,
    /// First message account when its parsed signer/writable flags are proven.
    pub fee_payer: Option<String>,
    pub token_delta: Option<ReceiptTokenDelta>,
    /// ProvenLifecycle concerns missing target token rows, not amounts of rent/refunds.
    pub token_coverage: ReceiptTokenCoverage,
    pub token_coverage_reason: Option<String>,
    /// Observed means a WSOL mint appears in token balances; absence is unresolved.
    pub wsol_coverage: ReceiptWsolCoverage,
    pub block_time: Option<i64>,
    pub decomposition: ReceiptDecomposition,
}

impl ExecutionCanaryReceiptFacts {
    pub fn wallet_is_fee_payer(&self) -> Option<bool> {
        self.fee_payer
            .as_ref()
            .map(|payer| payer == &self.wallet_pubkey)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            [
                &self.order_id,
                &self.tx_signature,
                &self.wallet_pubkey,
                &self.token
            ]
            .iter()
            .all(|s| !s.trim().is_empty()),
            "receipt facts identity missing"
        );
        ensure!(
            matches!(self.side.as_str(), "buy" | "sell"),
            "receipt facts side invalid"
        );
        ensure!(
            self.wallet_native_delta.as_i128()
                == i128::from(self.wallet_native_post.as_u64())
                    - i128::from(self.wallet_native_pre.as_u64()),
            "receipt facts native delta mismatch"
        );
        ensure!(
            self.transaction_fee.is_some() == (self.fee_coverage == ReceiptFeeCoverage::Known),
            "receipt facts fee coverage mismatch"
        );
        ensure!(
            self.fee_payer.as_ref().is_none_or(|s| !s.trim().is_empty()),
            "receipt facts payer invalid"
        );
        ensure!(
            self.token_delta.is_some() == (self.token_coverage != ReceiptTokenCoverage::Unresolved),
            "receipt facts token coverage mismatch"
        );
        ensure!(
            self.token_coverage_reason.is_some() == self.token_delta.is_none(),
            "receipt facts token reason mismatch"
        );
        if let Some(reason) = &self.token_coverage_reason {
            ensure!(
                !reason.is_empty()
                    && reason.len() <= 96
                    && reason
                        .bytes()
                        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_'),
                "receipt facts token reason invalid"
            );
        }
        ensure!(
            self.block_time
                .is_none_or(|t| chrono::DateTime::from_timestamp(t, 0).is_some()),
            "receipt facts block time invalid"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiptFactsRecordOutcome {
    Inserted,
    Existing,
    Enriched,
}
