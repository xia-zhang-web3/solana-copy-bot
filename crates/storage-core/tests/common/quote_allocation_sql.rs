use anyhow::Result;
impl crate::fixture::Fixture {
    pub fn sql(&self, sql: &str) -> Result<()> {
        rusqlite::Connection::open(self.dir.path().join("allocation.db"))?.execute_batch(sql)?;
        Ok(())
    }
}
