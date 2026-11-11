//! Generation-local Hold/CloseIfEmpty serialization. Explicit deletion is not
//! blocked by holds, matching the distinction in fixed upstream Room.
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct State {
    holds: usize,
    closed: bool,
}
#[derive(Default)]
pub(crate) struct IdleHoldState(Mutex<State>);
impl IdleHoldState {
    pub(crate) fn acquire(self: &Arc<Self>) -> Option<IdleHold> {
        let mut state = self.0.lock().unwrap_or_else(|p| p.into_inner());
        if state.closed {
            return None;
        }
        state.holds = state.holds.checked_add(1)?;
        Some(IdleHold(self.clone()))
    }
    /// Caller performs the actual synchronous idle close while this lock is
    /// held. Acquire cannot slip between an unheld decision and generation close.
    pub(crate) fn close_if_unheld(&self, close: impl FnOnce()) -> bool {
        let mut state = self.0.lock().unwrap_or_else(|p| p.into_inner());
        if state.closed || state.holds > 0 {
            return false;
        }
        state.closed = true;
        close();
        true
    }
    pub(crate) fn mark_closed(&self) {
        self.0.lock().unwrap_or_else(|p| p.into_inner()).closed = true;
    }
}
/// Not Clone: each successful acquire has exactly one release, including unwind
/// and cancellation. This never deletes or relooks up a room by name.
pub(crate) struct IdleHold(Arc<IdleHoldState>);
impl Drop for IdleHold {
    fn drop(&mut self) {
        let mut state = self.0.0.lock().unwrap_or_else(|p| p.into_inner());
        state.holds -= 1;
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn hold_and_terminal_close_are_serialized_and_old_release_is_local() {
        let old = Arc::new(IdleHoldState::default());
        let hold = old.acquire().unwrap();
        assert!(!old.close_if_unheld(|| panic!("held room closed")));
        old.mark_closed();
        assert!(old.acquire().is_none());
        let new = Arc::new(IdleHoldState::default());
        let next = new.acquire().unwrap();
        drop(hold);
        assert!(!new.close_if_unheld(|| panic!("old release touched new generation")));
        drop(next);
        assert!(new.close_if_unheld(|| {}));
        assert!(new.acquire().is_none());
    }
}
