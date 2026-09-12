use crate::{ExecutionCanaryCashSettlement, ReceiptDecomposition};
use anyhow::{ensure, Context, Result};
use copybot_core_types::{Lamports, SignedLamports, TokenQuantity};
use rusqlite::{Connection, OptionalExtension};

pub(crate) fn load(conn: &Connection, id: &str) -> Result<Option<ExecutionCanaryCashSettlement>> {
    let row = conn
        .query_row(
            "SELECT order_id,position_id,token,qty_raw,qty_decimals,
        remaining_qty_raw,wallet_native_delta_lamports,entry_basis_lamports,
        remaining_cost_lamports,cash_result_delta_lamports,accumulated_cash_result_lamports
        FROM fills WHERE order_id=?1 AND accounting_basis='receipt_native_cash'",
            [id],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, u8>(4)?,
                    r.get::<_, String>(5)?,
                    r.get::<_, i64>(6)?,
                    r.get::<_, i64>(7)?,
                    r.get::<_, i64>(8)?,
                    r.get::<_, i64>(9)?,
                    r.get::<_, i64>(10)?,
                ))
            },
        )
        .optional()?;
    row.map(
        |(
            order_id,
            position_id,
            token,
            raw,
            decimals,
            remaining,
            native,
            basis,
            cost,
            delta,
            total,
        )| {
            let raw = exact_raw(&raw)?;
            ensure!(raw > 0, "cash settlement sold raw must be positive");
            ensure!(
                i128::from(native) - i128::from(basis) == i128::from(delta),
                "cash settlement result mismatch"
            );
            Ok(ExecutionCanaryCashSettlement {
                order_id,
                position_id,
                token,
                sold_quantity: TokenQuantity::new(raw, decimals),
                remaining_quantity: TokenQuantity::new(exact_raw(&remaining)?, decimals),
                wallet_native_cash_delta: SignedLamports::new(i128::from(native)),
                allocated_entry_basis: Lamports::new(u64::try_from(basis)?),
                remaining_entry_basis: Lamports::new(u64::try_from(cost)?),
                cash_result_delta: SignedLamports::new(i128::from(delta)),
                accumulated_cash_result: SignedLamports::new(i128::from(total)),
                swap_price: None,
                decomposition: ReceiptDecomposition::Unresolved,
            })
        },
    )
    .transpose()
}

fn exact_raw(raw: &str) -> Result<u64> {
    let value: u64 = raw.parse().context("cash settlement raw domain")?;
    ensure!(
        value.to_string() == raw,
        "cash settlement raw not canonical"
    );
    Ok(value)
}
