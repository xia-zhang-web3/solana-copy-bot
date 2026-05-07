use super::*;

impl SqliteStore {
    pub fn ensure_recent_raw_journal_tables(&self) -> Result<()> {
        ensure_recent_raw_journal_tables_on_conn(&self.conn)
    }

    pub fn recent_raw_journal_state(&self) -> Result<RecentRawJournalStateRow> {
        self.ensure_recent_raw_journal_tables()?;
        recent_raw_journal_state_query(&self.conn)
    }

    pub fn recent_raw_journal_state_read_only(&self) -> Result<RecentRawJournalStateRow> {
        if !self.sqlite_table_exists("observed_swaps")?
            || !self.sqlite_table_exists("recent_raw_journal_state")?
        {
            return Ok(RecentRawJournalStateRow::default());
        }
        recent_raw_journal_state_query(&self.conn)
    }

    pub fn recent_raw_journal_state_cached_read_only_required(
        &self,
    ) -> Result<RecentRawJournalStateRow> {
        if !self.sqlite_table_exists("recent_raw_journal_state")? {
            bail!("cached recent raw journal state table recent_raw_journal_state is missing");
        }
        if !recent_raw_journal_state_row_exists(&self.conn)? {
            bail!("cached recent raw journal state row id=1 is missing");
        }
        recent_raw_journal_state_cached_query(&self.conn)
    }

    pub fn recent_raw_journal_state_cached(&self) -> Result<RecentRawJournalStateRow> {
        self.ensure_recent_raw_journal_tables()?;
        recent_raw_journal_state_cached_query(&self.conn)
    }
}
