//! Test-only deterministic pause at the actual cursor-published/continuity gap.
use std::cell::RefCell;
thread_local! { static HOOK: RefCell<Option<Box<dyn FnMut()>>> = RefCell::new(None); }
thread_local! { static TAIL: RefCell<Option<Box<dyn FnMut()>>> = RefCell::new(None); }
thread_local! { static CLEAR: RefCell<Option<Box<dyn FnMut()>>> = RefCell::new(None); }
thread_local! { static INTERRUPTED: RefCell<Option<Box<dyn FnMut()>>> = RefCell::new(None); }
pub(in crate::source) struct Hook;
impl Hook {
    pub(in crate::source) fn set(callback: impl FnMut() + 'static) -> Self {
        HOOK.with(|v| *v.borrow_mut() = Some(Box::new(callback)));
        Self
    }
    pub(in crate::source) fn tail(callback: impl FnMut() + 'static) -> Self {
        TAIL.with(|v| *v.borrow_mut() = Some(Box::new(callback)));
        Self
    }
    pub(in crate::source) fn clear(callback: impl FnMut() + 'static) -> Self {
        CLEAR.with(|v| *v.borrow_mut() = Some(Box::new(callback)));
        Self
    }
    pub(in crate::source) fn interrupted(callback: impl FnMut() + 'static) -> Self {
        INTERRUPTED.with(|v| *v.borrow_mut() = Some(Box::new(callback)));
        Self
    }
}
impl Drop for Hook {
    fn drop(&mut self) {
        HOOK.with(|v| *v.borrow_mut() = None);
        TAIL.with(|v| *v.borrow_mut() = None);
        CLEAR.with(|v| *v.borrow_mut() = None);
        INTERRUPTED.with(|v| *v.borrow_mut() = None);
    }
}
pub(in crate::source) fn at_ack_tail() {
    if let Some(mut callback) = TAIL.with(|v| v.borrow_mut().take()) {
        callback();
    }
}
pub(in crate::source) fn after_clear_false() {
    if let Some(mut callback) = CLEAR.with(|v| v.borrow_mut().take()) {
        callback();
    }
}
pub(in crate::source) fn after_interrupted() {
    if let Some(mut callback) = INTERRUPTED.with(|v| v.borrow_mut().take()) {
        callback();
    }
}
pub(in crate::source) fn after_cursor_ack() {
    let callback = HOOK.with(|v| v.borrow_mut().take());
    if let Some(mut callback) = callback {
        callback();
    }
}
