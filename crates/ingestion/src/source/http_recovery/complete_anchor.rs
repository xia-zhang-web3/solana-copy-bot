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
    for line in &meta.log_messages {
        if line.starts_with(TRUNCATED) {
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
            if stack.pop() != Some(key) {
                return false;
            }
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
    !meta.log_messages.is_empty() && stack.is_empty()
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
        if hm.err.is_some()
            || gm.log_messages_none
            || gm.log_messages.last().map(String::as_str) != Some(TRUNCATED)
            || gm.log_messages[..gm.log_messages.len() - 1]
                .iter()
                .any(|v| v.starts_with(TRUNCATED))
            || !hm
                .log_messages
                .starts_with(&gm.log_messages[..gm.log_messages.len() - 1])
            || !complete_trace(h)
        {
            return false;
        }
        completed = true;
    }
    completed
}
