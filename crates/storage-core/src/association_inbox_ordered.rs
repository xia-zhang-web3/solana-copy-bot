//! Charges only the strict protocol domain, never unrelated legacy claims.
use super::ConsumerMode;
use anyhow::Result;
use rusqlite::Connection;

pub(crate) fn required(c: &Connection, mode: ConsumerMode) -> Result<()> {
    if mode == ConsumerMode::ProviderOrderStrictV1 {
        crate::ordered_source_sell::schema::required(c)?;
        crate::association_sell_preparation::shadow_recovery::schema::required(c)?;
        crate::shadow_lot_origin::schema::required(c)?;
    }
    Ok(())
}
