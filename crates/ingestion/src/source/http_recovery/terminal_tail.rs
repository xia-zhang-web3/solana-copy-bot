//! Anchor-only compatibility for a truncated final runtime unwind. No raw log
//! is replaced: all payload records must already be present on both sides.
const MARKER: &str = "Log truncated";

fn key(value: &str) -> Option<[u8; 32]> {
    if value.len() > 44 {
        return None;
    }
    let mut bytes = [0; 32];
    (bs58::decode(value).onto(&mut bytes).ok()? == 32).then_some(bytes)
}
fn pop(stack: &mut Vec<[u8; 32]>, program: [u8; 32]) -> Option<()> {
    (stack.pop()? == program).then_some(())
}
fn prefix_stack(lines: &[String]) -> Option<Vec<[u8; 32]>> {
    let mut stack = Vec::new();
    for line in lines {
        if line == MARKER {
            return None;
        }
        // Program payload must never masquerade as a runtime control record.
        if line.starts_with("Program log:")
            || line.starts_with("Program data:")
            || line.starts_with("Program return:")
        {
            continue;
        }
        let Some(control) = line.strip_prefix("Program ") else {
            continue;
        };
        let (program, record) = control.split_once(' ')?;
        let program = key(program)?;
        if let Some(depth) = record
            .strip_prefix("invoke [")
            .and_then(|s| s.strip_suffix(']'))
        {
            let depth_number: usize = depth.parse().ok()?;
            if depth_number != stack.len().checked_add(1)? || depth != depth_number.to_string() {
                return None;
            }
            stack.push(program);
        } else if record == "success" {
            pop(&mut stack, program)?;
        } else if record
            .strip_prefix("failed: ")
            .is_some_and(|s| !s.is_empty())
        {
            pop(&mut stack, program)?;
        } else if !record.starts_with("consumed ") {
            // Unknown or malformed runtime controls do not establish a stack.
            return None;
        }
    }
    Some(stack)
}
fn direction(truncated: &[String], complete: &[String]) -> bool {
    if truncated.last().map(String::as_str) != Some(MARKER) {
        return false;
    }
    let prefix = &truncated[..truncated.len() - 1];
    if !complete.starts_with(prefix) || complete.iter().any(|s| s == MARKER) {
        return false;
    }
    let Some(mut stack) = prefix_stack(prefix) else {
        return false;
    };
    let tail = &complete[prefix.len()..];
    if stack.is_empty() || tail.len() != stack.len() {
        return false;
    }
    for line in tail {
        let Some(program) = line
            .strip_prefix("Program ")
            .and_then(|s| s.strip_suffix(" success"))
            .and_then(key)
        else {
            return false;
        };
        if pop(&mut stack, program).is_none() {
            return false;
        }
    }
    stack.is_empty()
}
pub(super) fn equivalent(a: &[String], b: &[String]) -> bool {
    direction(a, b) || direction(b, a)
}
