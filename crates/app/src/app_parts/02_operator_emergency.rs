fn parse_operator_emergency_stop_reason(content: &str) -> Option<String> {
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        return Some(trimmed.to_string());
    }
    None
}

#[derive(Debug)]
struct OperatorEmergencyStop {
    path: PathBuf,
    poll_interval: Duration,
    next_refresh_at: StdInstant,
    active: bool,
    detail: String,
}

impl OperatorEmergencyStop {
    fn from_env() -> Self {
        let path = env::var("SOLANA_COPY_BOT_EMERGENCY_STOP_FILE")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from(DEFAULT_OPERATOR_EMERGENCY_STOP_PATH));
        let poll_ms = env::var("SOLANA_COPY_BOT_EMERGENCY_STOP_POLL_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(DEFAULT_OPERATOR_EMERGENCY_STOP_POLL_MS)
            .max(100);
        Self {
            path,
            poll_interval: Duration::from_millis(poll_ms),
            next_refresh_at: StdInstant::now(),
            active: false,
            detail: String::new(),
        }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn is_active(&self) -> bool {
        self.active
    }

    fn detail(&self) -> &str {
        if self.detail.is_empty() {
            "operator emergency stop file is present"
        } else {
            self.detail.as_str()
        }
    }

    fn refresh(&mut self, store: &SqliteStore, now: DateTime<Utc>) -> Result<()> {
        let instant_now = StdInstant::now();
        if instant_now < self.next_refresh_at {
            return Ok(());
        }
        self.next_refresh_at = instant_now + self.poll_interval;

        let (active, detail) = match fs::read_to_string(&self.path) {
            Ok(content) => (
                true,
                parse_operator_emergency_stop_reason(&content)
                    .unwrap_or_else(|| "operator emergency stop file is present".to_string()),
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => (false, String::new()),
            Err(error) => match fs::metadata(&self.path) {
                Ok(_) => {
                    let detail = format!("emergency stop file is unreadable: {error}");
                    warn!(
                        path = %self.path.display(),
                        error = %error,
                        "operator emergency stop file exists but cannot be read; failing closed"
                    );
                    (true, detail)
                }
                Err(_) => (false, String::new()),
            },
        };

        if active == self.active && detail == self.detail {
            return Ok(());
        }

        let path_display = self.path.display().to_string();

        if active {
            self.active = true;
            self.detail = detail;
            warn!(
                path = %path_display,
                detail = %self.detail(),
                "operator emergency stop activated"
            );
            let details_json = format!(
                "{{\"path\":\"{}\",\"detail\":\"{}\"}}",
                sanitize_json_value(&path_display),
                sanitize_json_value(self.detail())
            );
            persist_runtime_risk_event_or_warn(
                store,
                "operator_emergency_stop_activated",
                "warn",
                now,
                Some(&details_json),
                "failed to persist operator emergency stop activation event",
                "failed to persist operator emergency stop activation event with fatal sqlite I/O",
            )?;
        } else {
            info!(path = %path_display, "operator emergency stop cleared");
            let details_json = format!(
                "{{\"path\":\"{}\",\"state\":\"cleared\"}}",
                sanitize_json_value(&path_display)
            );
            persist_runtime_risk_event_or_warn(
                store,
                "operator_emergency_stop_cleared",
                "info",
                now,
                Some(&details_json),
                "failed to persist operator emergency stop clear event",
                "failed to persist operator emergency stop clear event with fatal sqlite I/O",
            )?;
            self.active = false;
            self.detail.clear();
        }
        Ok(())
    }
}
