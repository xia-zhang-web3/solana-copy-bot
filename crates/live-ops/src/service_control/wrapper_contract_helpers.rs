fn validate_backend_command(backend_command: &str) -> Result<()> {
    if backend_command.trim().is_empty() {
        bail!("backend command must not be empty");
    }
    if backend_command
        .bytes()
        .any(|byte| byte == b'\n' || byte == b'\r' || byte.is_ascii_whitespace())
    {
        bail!("backend command must be a single executable path or name without whitespace");
    }
    Ok(())
}

fn shell_single_quote(value: &str) -> String {
    let mut quoted = String::from("'");
    for ch in value.chars() {
        if ch == '\'' {
            quoted.push_str("'\"'\"'");
        } else {
            quoted.push(ch);
        }
    }
    quoted.push('\'');
    quoted
}

fn executable_flag(path: &Path) -> Option<bool> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::metadata(path)
            .ok()
            .map(|value| value.permissions().mode() & 0o111 != 0)
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        None
    }
}

fn set_executable(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(path)
            .with_context(|| format!("failed reading wrapper permissions {}", path.display()))?
            .permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms)
            .with_context(|| format!("failed setting wrapper executable bit {}", path.display()))?;
    }
    Ok(())
}
