use super::*;

pub(in crate::observed_swap_writer) fn set_terminal_failure_message(
    terminal_failure_message: &Arc<Mutex<Option<String>>>,
    message: String,
) {
    if let Ok(mut guard) = terminal_failure_message.lock() {
        if guard.is_none() {
            *guard = Some(message);
        }
    }
}

pub(in crate::observed_swap_writer) fn load_terminal_failure_message(
    terminal_failure_message: &Arc<Mutex<Option<String>>>,
) -> Option<String> {
    terminal_failure_message
        .lock()
        .ok()
        .and_then(|message| message.clone())
}

pub(in crate::observed_swap_writer) fn panic_payload_to_string(
    payload: &(dyn std::any::Any + Send),
) -> String {
    if let Some(message) = payload.downcast_ref::<String>() {
        return message.clone();
    }
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        return (*message).to_string();
    }
    "unknown panic payload".to_string()
}
