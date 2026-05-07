pub(super) const BULK_INSERT_PARAMS_PER_ROW: usize = 13;
pub(super) const BULK_INSERT_HARD_CAP_ROWS: usize = 512;

mod helpers;
mod helpers_cursor;
mod ops;
