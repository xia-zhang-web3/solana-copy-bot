fn load_service_state_from_systemctl(service_name: &str) -> Result<ServiceState> {
    let output = Command::new("systemctl")
        .args([
            "show",
            service_name,
            "--property=ActiveState",
            "--property=SubState",
            "--no-pager",
        ])
        .output()
        .with_context(|| format!("failed to run systemctl show {service_name}"))?;
    if !output.status.success() {
        bail!(
            "systemctl show {service_name} failed with status {:?}: {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    parse_systemctl_show_output(&String::from_utf8_lossy(&output.stdout))
}

fn parse_systemctl_show_output(output: &str) -> Result<ServiceState> {
    let mut active_state: Option<String> = None;
    let mut substate: Option<String> = None;
    for line in output.lines() {
        if let Some(value) = line.strip_prefix("ActiveState=") {
            active_state = Some(value.trim().to_string());
        } else if let Some(value) = line.strip_prefix("SubState=") {
            substate = Some(value.trim().to_string());
        }
    }
    let active_state =
        active_state.ok_or_else(|| anyhow!("systemctl output missing ActiveState"))?;
    Ok(ServiceState {
        active_state: active_state.clone(),
        active: active_state == "active",
        substate,
    })
}
