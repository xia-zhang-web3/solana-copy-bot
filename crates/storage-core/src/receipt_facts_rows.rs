use crate::{ExecutionCanaryReceiptFacts, ReceiptTokenDelta};
use anyhow::{ensure, Context, Result};
use copybot_core_types::{Lamports, SignedLamports};
use rusqlite::{Connection, OptionalExtension};

pub(crate) fn load(
    conn: &Connection,
    order_id: &str,
) -> Result<Option<ExecutionCanaryReceiptFacts>> {
    let raw = conn
        .query_row(
            "SELECT order_id, tx_signature, wallet_pubkey, token, side, slot,
         wallet_native_pre, wallet_native_post, wallet_native_delta, transaction_fee,
         fee_coverage, fee_payer, token_delta_raw, token_decimals, token_coverage,
         token_coverage_reason, wsol_coverage, block_time, decomposition
         FROM execution_canary_receipt_facts WHERE order_id = ?1",
            [order_id],
            |r| {
                (0..19)
                    .map(|i| r.get::<_, rusqlite::types::Value>(i))
                    .collect::<rusqlite::Result<Vec<_>>>()
            },
        )
        .optional()
        .context("load canary receipt facts")?;
    raw.map(|r| {
        use rusqlite::types::Value;
        let text = |i: usize| -> Result<&str> {
            match &r[i] {
                Value::Text(s) => Ok(s),
                _ => anyhow::bail!("receipt facts text domain invalid"),
            }
        };
        let optional = |i: usize| -> Result<Option<&str>> {
            if r[i] == Value::Null {
                Ok(None)
            } else {
                text(i).map(Some)
            }
        };
        let token_delta = optional(12)?
            .map(|raw| {
                let decimals = match r[13] {
                    Value::Integer(v) => {
                        u8::try_from(v).context("receipt facts decimals out of domain")?
                    }
                    _ => anyhow::bail!("receipt facts decimals missing"),
                };
                Ok(ReceiptTokenDelta {
                    raw: exact(raw)?,
                    decimals,
                })
            })
            .transpose()?;
        ensure!(
            token_delta.is_some() || r[13] == Value::Null,
            "receipt facts orphan decimals"
        );
        let facts = ExecutionCanaryReceiptFacts {
            order_id: text(0)?.into(),
            tx_signature: text(1)?.into(),
            wallet_pubkey: text(2)?.into(),
            token: text(3)?.into(),
            side: text(4)?.into(),
            slot: exact(text(5)?)?,
            wallet_native_pre: Lamports::new(exact(text(6)?)?),
            wallet_native_post: Lamports::new(exact(text(7)?)?),
            wallet_native_delta: SignedLamports::new(exact(text(8)?)?),
            transaction_fee: optional(9)?.map(exact).transpose()?.map(Lamports::new),
            fee_coverage: text(10)?.parse()?,
            fee_payer: optional(11)?.map(str::to_owned),
            token_delta,
            token_coverage: text(14)?.parse()?,
            token_coverage_reason: optional(15)?.map(str::to_owned),
            wsol_coverage: text(16)?.parse()?,
            block_time: optional(17)?.map(exact).transpose()?,
            decomposition: text(18)?.parse()?,
        };
        facts.validate()?;
        Ok(facts)
    })
    .transpose()
}

fn exact<T: std::str::FromStr + ToString>(value: &str) -> Result<T> {
    let parsed = value
        .parse::<T>()
        .map_err(|_| anyhow::anyhow!("receipt facts integer out of domain"))?;
    ensure!(
        parsed.to_string() == value,
        "receipt facts integer is not canonical"
    );
    Ok(parsed)
}
