//! Directional completion classification, never Info equality or admission authority.
use super::{identity, index_identity};
use yellowstone_grpc_proto::prelude::{SubscribeUpdateBlock, SubscribeUpdateTransactionInfo};
const TRUNCATED: &str = "Log truncated";

fn program(key: &str) -> Option<[u8; 32]> {
    let mut out = [0; 32];
    (key.len() <= 44 && bs58::decode(key).onto(&mut out).ok()? == 32).then_some(out)
}
fn number(value: &str) -> Option<usize> {
    let n: usize = value.parse().ok()?;
    (value == n.to_string()).then_some(n)
}
fn complete_trace(info: &SubscribeUpdateTransactionInfo) -> bool {
    let Some(meta) = &info.meta else { return false };
    let Some(message) = info.transaction.as_ref().and_then(|t| t.message.as_ref()) else {
        return false;
    };
    let keys: Vec<_> = message
        .account_keys
        .iter()
        .chain(&meta.loaded_writable_addresses)
        .chain(&meta.loaded_readonly_addresses)
        .collect();
    let mut allowed = std::collections::HashSet::new();
    for index in message
        .instructions
        .iter()
        .map(|i| i.program_id_index)
        .chain(
            meta.inner_instructions
                .iter()
                .flat_map(|g| g.instructions.iter().map(|i| i.program_id_index)),
        )
    {
        let Some(key) = keys.get(index as usize) else {
            return false;
        };
        let Ok(key) = <[u8; 32]>::try_from(key.as_slice()) else {
            return false;
        };
        allowed.insert(key);
    }
    let mut stack = Vec::new();
    let mut failed = false;
    for line in &meta.log_messages {
        if failed || line.starts_with(TRUNCATED) {
            return false;
        }
        if line.starts_with("Program log:") || line.starts_with("Program data:") {
            if stack.is_empty() {
                return false;
            }
            continue;
        }
        if let Some(value) = line.strip_prefix("Program return: ") {
            let Some((key, data)) = value.split_once(' ') else {
                return false;
            };
            if program(key).as_ref() != stack.last() || data.is_empty() {
                return false;
            }
            continue;
        }
        let Some((key, record)) = line
            .strip_prefix("Program ")
            .and_then(|v| v.split_once(' '))
        else {
            return false;
        };
        let Some(key) = program(key) else {
            return false;
        };
        if !allowed.contains(&key) {
            return false;
        }
        if let Some(depth) = record
            .strip_prefix("invoke [")
            .and_then(|v| v.strip_suffix(']'))
        {
            if number(depth) != Some(stack.len() + 1) {
                return false;
            }
            stack.push(key);
        } else if record == "success"
            || record
                .strip_prefix("failed: ")
                .is_some_and(|v| !v.is_empty())
        {
            let root_failure = stack.len() == 1 && record != "success";
            if stack.pop() != Some(key) {
                return false;
            }
            failed = root_failure;
        } else if let Some(value) = record
            .strip_prefix("consumed ")
            .and_then(|v| v.strip_suffix(" compute units"))
        {
            let Some((used, total)) = value.split_once(" of ") else {
                return false;
            };
            if stack.last() != Some(&key)
                || !number(used).zip(number(total)).is_some_and(|(a, b)| a <= b)
            {
                return false;
            }
        } else {
            return false;
        }
    }
    !meta.log_messages.is_empty() && stack.is_empty() && failed == meta.err.is_some()
}

fn retained_unwind(http: &[String], gap: usize, tail: &[String]) -> bool {
    // Only closes of invocations already observed BEFORE the marker qualify.
    // Program text may repeat later, so a substring/subsequence search cannot
    // identify the retained close. The complete trace check validates this
    // stack; invocation offsets disambiguate repeated calls to the same program.
    if tail.is_empty() || tail.iter().any(|v| !v.ends_with(" success")) {
        return false;
    }
    let mut stack = Vec::new();
    let mut closes = Vec::new();
    for (offset, line) in http.iter().enumerate() {
        let Some((key, record)) = line
            .strip_prefix("Program ")
            .and_then(|s| s.split_once(' '))
        else {
            continue;
        };
        if program(key).is_none() {
            continue;
        }
        if record.starts_with("invoke [") {
            stack.push(offset);
        } else if record == "success" || record.starts_with("failed: ") {
            let Some(invocation) = stack.pop() else {
                return false;
            };
            if invocation < gap && offset >= gap {
                closes.push(line);
            }
        }
    }
    tail.len() <= closes.len() && tail.iter().zip(closes).all(|(a, b)| a == b)
}

/// Replay Agave's default LogCollector::log, not an arbitrary subsequence match.
/// https://github.com/anza-xyz/agave/blob/master/svm-log-collector/src/lib.rs
/// A rejected record does not consume bytes. Shorter later records can survive;
/// the one warning itself is not charged. The limit is UTF-8 bytes, strictly <.
pub(crate) fn collector_logs_agree(http: &[String], live: &[String]) -> bool {
    const LIMIT: usize = 10_000;
    let mut available = live.iter().map(String::as_str);
    let mut bytes = 0usize;
    let mut warned = false;
    for line in http {
        if line.starts_with(TRUNCATED) {
            return false;
        }
        let next = bytes.saturating_add(line.len());
        let emitted = if next >= LIMIT {
            if warned {
                continue;
            }
            warned = true;
            TRUNCATED
        } else {
            bytes = next;
            line.as_str()
        };
        if available.next() != Some(emitted) {
            return false;
        }
    }
    warned && available.next().is_none()
}

fn known_logs_agree(http: &[String], live: &[String]) -> bool {
    if collector_logs_agree(http, live) {
        return true;
    }
    // Keep the contiguous-gap contract. A retained tail before further HTTP
    // instructions must instead close concrete observed prefix invocations.
    // Current HTTP supplies missing payload, never claimed equal to hidden live
    // data. Execution identity and the complete HTTP trace remain separate checks.
    let Some(gap) = live.iter().position(|v| v == TRUNCATED) else {
        return false;
    };
    !live
        .iter()
        .enumerate()
        .any(|(i, v)| i != gap && v.starts_with(TRUNCATED))
        && http.len() >= live.len()
        && http.starts_with(&live[..gap])
        && (http.ends_with(&live[gap + 1..]) || retained_unwind(http, gap, &live[gap + 1..]))
}
/// Fresh HTTP is trusted for missing payload only after complete execution identity
/// and every available live log record agree. This does not assert hidden equality.
pub(crate) fn eligible(http: &SubscribeUpdateBlock, live: &SubscribeUpdateBlock) -> bool {
    if !identity::headers_equal(http, live) {
        return false;
    }
    let (Ok(h), Ok(g)) = (
        index_identity::execution_order(http),
        index_identity::execution_order(live),
    ) else {
        return false;
    };
    if http.transactions.iter().any(|t| {
        t.meta.as_ref().is_none_or(|m| {
            m.log_messages_none
                || m.log_messages
                    .iter()
                    .any(|line| line.starts_with(TRUNCATED))
        })
    }) {
        return false;
    }
    let mut completed = false;
    for (h, g) in h.into_iter().zip(g) {
        let (Some(h), Some(g)) = (h, g) else {
            return false;
        };
        if identity::info_equal(h, g) {
            continue;
        }
        if !identity::same_execution(h, g) {
            return false;
        }
        let (Some(hm), Some(gm)) = (&h.meta, &g.meta) else {
            return false;
        };
        if gm.log_messages_none
            || !known_logs_agree(&hm.log_messages, &gm.log_messages)
            || !complete_trace(h)
        {
            return false;
        }
        completed = true;
    }
    completed
}
