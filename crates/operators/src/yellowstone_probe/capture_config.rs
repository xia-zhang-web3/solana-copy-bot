use super::config::ProbeMode;
use anyhow::{bail, ensure, Result};
use serde::Serialize;
use std::{collections::BTreeMap, path::PathBuf};

// Small local experiment budgets, not protocol maxima or provider coverage claims.
pub(crate) const METADATA_RESERVE: u64 = 262_144;
const LEGACY_TRANSPORT_BYTES: u64 = 1_048_576;
pub(crate) const DIAGNOSTIC_PROFILE: &str = "diagnostic-v1";
pub(crate) const WINDOW_PROFILE: &str = "window-v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CaptureProfile {
    Legacy,
    DiagnosticV1,
    WindowV1,
}
#[derive(Debug, Clone, Serialize)]
pub(crate) struct CaptureConfig {
    #[serde(skip)]
    pub output: PathBuf,
    #[serde(skip)]
    pub profile: CaptureProfile,
    pub duration_ms: u64,
    pub messages: u64,
    pub message_bytes: u64,
    pub total_bytes: u64,
}

impl CaptureConfig {
    pub fn transport_bytes(&self) -> usize {
        match self.profile {
            CaptureProfile::Legacy => LEGACY_TRANSPORT_BYTES as usize,
            CaptureProfile::DiagnosticV1 | CaptureProfile::WindowV1 => self.message_bytes as usize,
        }
    }
    pub fn metadata_reserve(&self) -> u64 {
        match self.profile {
            CaptureProfile::WindowV1 => 2_097_152,
            _ => METADATA_RESERVE,
        }
    }
    pub fn profile_name(&self) -> Option<&'static str> {
        match self.profile {
            CaptureProfile::Legacy => None,
            CaptureProfile::DiagnosticV1 => Some(DIAGNOSTIC_PROFILE),
            CaptureProfile::WindowV1 => Some(WINDOW_PROFILE),
        }
    }
}

#[derive(Default)]
pub(crate) struct CaptureArgs(BTreeMap<String, String>);
impl CaptureArgs {
    pub fn accepts(&self, flag: &str) -> bool {
        matches!(
            flag,
            "--output-dir"
                | "--duration-ms"
                | "--max-messages"
                | "--max-message-bytes"
                | "--max-total-bytes"
                | "--capture-profile"
        )
    }
    pub fn set(&mut self, flag: &str, value: Option<String>) -> Result<()> {
        ensure!(!self.0.contains_key(flag), "duplicate capture flag");
        self.0.insert(
            flag.into(),
            value.ok_or_else(|| anyhow::anyhow!("missing capture value"))?,
        );
        Ok(())
    }
    pub fn finish(self, mode: ProbeMode) -> Result<Option<CaptureConfig>> {
        if mode != ProbeMode::AssociationCapture {
            ensure!(
                self.0.is_empty(),
                "capture flags require association-capture mode"
            );
            return Ok(None);
        }
        let get = |name| {
            self.0
                .get(name)
                .ok_or_else(|| anyhow::anyhow!("capture requires {name}"))
        };
        let number = |name, max| -> Result<u64> {
            let n: u64 = get(name)?
                .parse()
                .map_err(|_| anyhow::anyhow!("invalid capture integer"))?;
            ensure!(
                n > 0 && n <= max,
                "capture {name} outside local experiment bounds 1..={max}"
            );
            Ok(n)
        };
        let output = PathBuf::from(get("--output-dir")?);
        if output.as_os_str().is_empty() {
            bail!("capture output directory required");
        }
        let (profile, count_cap, message_cap, total_cap) = match self.0.get("--capture-profile") {
            None => (
                CaptureProfile::Legacy,
                256,
                LEGACY_TRANSPORT_BYTES,
                16_777_216,
            ),
            Some(name) if name == DIAGNOSTIC_PROFILE => {
                (CaptureProfile::DiagnosticV1, 256, 8_388_608, 67_108_864)
            }
            Some(name) if name == WINDOW_PROFILE => {
                (CaptureProfile::WindowV1, 4096, 8_388_608, 67_108_864)
            }
            Some(_) => bail!("unsupported capture profile"),
        };
        let config = CaptureConfig {
            output,
            profile,
            duration_ms: number("--duration-ms", 60_000)?,
            messages: number("--max-messages", count_cap)?,
            message_bytes: number("--max-message-bytes", message_cap)?,
            total_bytes: number("--max-total-bytes", total_cap)?,
        };
        ensure!(
            config.total_bytes > config.metadata_reserve(),
            "capture total must exceed metadata reserve"
        );
        Ok(Some(config))
    }
}
