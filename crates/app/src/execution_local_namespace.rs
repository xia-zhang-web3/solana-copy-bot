//! Network-none Linux includes DOWN tunnel placeholders; none may enable egress.
use anyhow::{ensure, Result};
pub(crate) fn offline_interface(name: &str, flags: u32) -> bool {
    name == "lo" || flags & 1 == 0 // IFF_UP
}
pub(crate) fn loopback_routes(routes: &str, header: bool) -> bool {
    routes.lines().skip(usize::from(header)).all(|line| {
        if header {
            line.split_whitespace().next() == Some("lo")
        } else {
            line.split_whitespace().last() == Some("lo")
        }
    })
}
pub(crate) fn validate() -> Result<()> {
    let devices = std::fs::read_to_string("/proc/net/dev")?;
    for line in devices.lines().skip(2) {
        let name = line.split_once(':').ok_or_else(|| anyhow::anyhow!("local_session_interface"))?.0.trim();
        let flags=std::fs::read_to_string(format!("/sys/class/net/{name}/flags"))?;
        let flags=u32::from_str_radix(flags.trim().trim_start_matches("0x"),16)?;
        ensure!(offline_interface(name,flags),"local_session_external_interface_up");
    }
    ensure!(loopback_routes(&std::fs::read_to_string("/proc/net/route")?,true)
        && loopback_routes(&std::fs::read_to_string("/proc/net/ipv6_route")?,false),"local_session_external_route");
    let status=std::fs::read_to_string("/proc/self/status")?;
    let caps=status.lines().find_map(|s|s.strip_prefix("CapEff:"))
        .ok_or_else(||anyhow::anyhow!("local_session_capabilities"))?;
    ensure!(u64::from_str_radix(caps.trim(),16)?==0,"local_session_capabilities_required_empty");
    Ok(())
}
