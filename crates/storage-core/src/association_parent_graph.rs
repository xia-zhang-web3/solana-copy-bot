//! Exact provider parent keys, without canonical/finalized-fork or trade authority.
use super::*;
use copybot_core_types::association_parent::{valid_hash, BlockKey, ParentObservation};
use rusqlite::{params, OptionalExtension};
#[path = "association_parent_reader.rs"]
mod reader;
#[path = "association_parent_schema.rs"]
pub(crate) mod schema;
#[path = "association_parent_store.rs"]
mod store;
#[path = "association_parent_work.rs"]
mod work;
pub(super) use reader::Reader;
pub(super) use store::put;
pub(super) use work::{bind, pending, step};
fn key(k: &BlockKey) -> Result<String> {
    Ok(serde_json::to_string(k)?)
}
