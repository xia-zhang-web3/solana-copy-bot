fn write_flag_atomic(path: &Path, reason: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed creating {}", parent.display()))?;
        }
    }
    let timestamp = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);
    let contents = format!(
        "{reason}\n# operator_emergency_stop_activated_at_utc={timestamp}\n# managed_by=copybot_operator_emergency_stop\n"
    );
    let tmp_path = temp_flag_path(path);
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp_path)
        .with_context(|| format!("failed creating temp flag {}", tmp_path.display()))?;
    if let Err(error) = file.write_all(contents.as_bytes()) {
        let _ = fs::remove_file(&tmp_path);
        return Err(error).with_context(|| format!("failed writing {}", tmp_path.display()));
    }
    if let Err(error) = file.sync_all() {
        let _ = fs::remove_file(&tmp_path);
        return Err(error).with_context(|| format!("failed syncing {}", tmp_path.display()));
    }
    drop(file);
    if let Err(error) = fs::rename(&tmp_path, path) {
        let _ = fs::remove_file(&tmp_path);
        return Err(error).with_context(|| {
            format!(
                "failed renaming temp flag {} to {}",
                tmp_path.display(),
                path.display()
            )
        });
    }
    Ok(())
}

fn temp_flag_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("operator_emergency_stop.flag");
    let tmp_name = format!(
        ".{file_name}.{}.{}.tmp",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    path.with_file_name(tmp_name)
}

fn render_human(output: &OperatorEmergencyStopOutput) -> String {
    [
        format!("event={}", output.event),
        format!("path={}", output.path),
        format!("active={}", output.active),
        format!("changed={}", output.changed),
        format!("reason={}", output.reason.as_deref().unwrap_or("null")),
        format!("verdict={}", output.verdict),
    ]
    .join("\n")
}
