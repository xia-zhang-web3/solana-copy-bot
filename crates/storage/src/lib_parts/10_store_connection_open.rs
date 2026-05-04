impl SqliteStore {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!("failed to create sqlite parent dir: {}", parent.display())
            })?;
        }

        let conn = Connection::open(path)
            .with_context(|| format!("failed to open sqlite db: {}", path.display()))?;
        conn.busy_timeout(StdDuration::from_secs(5))
            .context("failed to set sqlite busy_timeout")?;
        conn.pragma_update(None, "journal_mode", "WAL")
            .context("failed to set sqlite journal mode WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")
            .context("failed to set sqlite synchronous NORMAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")
            .context("failed to enable sqlite foreign keys")?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS schema_migrations (
                version TEXT PRIMARY KEY,
                applied_at TEXT NOT NULL
            );",
        )
        .context("failed to create schema_migrations table")?;

        Ok(Self { conn })
    }

    pub fn open_for_startup(
        path: &Path,
        policy: &SqliteStartupPolicy,
        reporter: Option<&StartupStepProgressReporter>,
    ) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!("failed to create sqlite parent dir: {}", parent.display())
            })?;
        }

        let sqlite_path = path.to_path_buf();
        let conn = run_observed_startup_step(
            "sqlite_open_connection",
            policy.open_step,
            reporter,
            move || {
                Connection::open(&sqlite_path)
                    .with_context(|| format!("failed to open sqlite db: {}", sqlite_path.display()))
            },
        )?;
        let conn = run_observed_startup_step(
            "sqlite_set_busy_timeout",
            policy.pragma_step,
            reporter,
            move || {
                conn.busy_timeout(StdDuration::from_secs(5))
                    .context("failed to set sqlite busy_timeout")?;
                Ok(conn)
            },
        )?;
        let conn = checkpoint_large_startup_wal_if_needed(
            path,
            conn,
            policy.large_wal_checkpoint_step,
            reporter,
            policy.large_wal_checkpoint_threshold_bytes,
        )?;
        let conn = run_observed_startup_step(
            "sqlite_pragma_journal_mode_wal",
            policy.pragma_step,
            reporter,
            move || {
                conn.pragma_update(None, "journal_mode", "WAL")
                    .context("failed to set sqlite journal mode WAL")?;
                Ok(conn)
            },
        )?;
        let conn = run_observed_startup_step(
            "sqlite_pragma_synchronous_normal",
            policy.pragma_step,
            reporter,
            move || {
                conn.pragma_update(None, "synchronous", "NORMAL")
                    .context("failed to set sqlite synchronous NORMAL")?;
                Ok(conn)
            },
        )?;
        let conn = run_observed_startup_step(
            "sqlite_pragma_foreign_keys_on",
            policy.pragma_step,
            reporter,
            move || {
                conn.pragma_update(None, "foreign_keys", "ON")
                    .context("failed to enable sqlite foreign keys")?;
                Ok(conn)
            },
        )?;
        let conn = run_observed_startup_step(
            "sqlite_schema_migrations_bootstrap",
            policy.schema_bootstrap_step,
            reporter,
            move || {
                conn.execute_batch(
                    "CREATE TABLE IF NOT EXISTS schema_migrations (
                        version TEXT PRIMARY KEY,
                        applied_at TEXT NOT NULL
                    );",
                )
                .context("failed to create schema_migrations table")?;
                Ok(conn)
            },
        )?;

        Ok(Self { conn })
    }

    pub fn open_read_only(path: &Path) -> Result<Self> {
        let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .with_context(|| format!("failed to open sqlite db read-only: {}", path.display()))?;
        conn.busy_timeout(StdDuration::from_secs(5))
            .context("failed to set sqlite busy_timeout")?;
        conn.pragma_update(None, "foreign_keys", "ON")
            .context("failed to enable sqlite foreign keys")?;
        Ok(Self { conn })
    }

    pub fn open_read_only_immutable(path: &Path) -> Result<Self> {
        let uri = build_sqlite_immutable_read_only_uri(path);
        let conn = Connection::open_with_flags(
            &uri,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
        )
        .with_context(|| {
            format!(
                "failed to open sqlite db immutable read-only: {}",
                path.display()
            )
        })?;
        conn.busy_timeout(StdDuration::from_secs(5))
            .context("failed to set sqlite busy_timeout")?;
        conn.pragma_update(None, "foreign_keys", "ON")
            .context("failed to enable sqlite foreign keys")?;
        conn.pragma_update(None, "query_only", "ON")
            .context("failed to enable sqlite query_only")?;
        Ok(Self { conn })
    }

    pub fn set_busy_timeout(&self, timeout: StdDuration) -> Result<()> {
        self.conn
            .busy_timeout(timeout)
            .context("failed to set sqlite busy_timeout")?;
        Ok(())
    }

    #[doc(hidden)]
    pub fn set_sqlite_length_limit_for_test(&self, new_val: i32) -> i32 {
        unsafe {
            rusqlite::ffi::sqlite3_limit(
                self.conn.handle(),
                rusqlite::ffi::SQLITE_LIMIT_LENGTH,
                new_val,
            )
        }
    }
}
