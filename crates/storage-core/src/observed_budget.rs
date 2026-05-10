use rusqlite::ErrorCode;
use std::time::Instant;

pub(crate) struct SqliteProgressDeadline<'a> {
    conn: &'a rusqlite::Connection,
}

impl Drop for SqliteProgressDeadline<'_> {
    fn drop(&mut self) {
        self.conn.progress_handler(0, None::<fn() -> bool>);
    }
}

pub(crate) fn sqlite_progress_deadline(
    conn: &rusqlite::Connection,
    deadline: Instant,
) -> SqliteProgressDeadline<'_> {
    conn.progress_handler(1_000, Some(move || Instant::now() >= deadline));
    SqliteProgressDeadline { conn }
}

pub(crate) fn interrupted_after_deadline(error: &rusqlite::Error, deadline: Instant) -> bool {
    error.sqlite_error_code() == Some(ErrorCode::OperationInterrupted) && Instant::now() >= deadline
}
