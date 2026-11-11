//! Process-projectable cumulative source lifecycle outcomes. These are actual
//! room state transitions, never counts of HTTP/API calls or stored labels.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LifecycleCounters {
    pub track_publish_attempts: u64,
    pub track_publish_success: u64,
    pub track_publish_cancels: u64,
    pub track_subscribe_attempts: u64,
    pub track_subscribe_success: u64,
    pub track_subscribe_cancels: u64,
    pub participant_signal_connected: u64,
    pub participant_rtc_init: u64,
    pub participant_rtc_connected: u64,
    pub participant_rtc_canceled: u64,
    pub participant_rtc_active: u64,
}
impl LifecycleCounters {
    /// Each actor is sampled independently. Retiring it cannot subtract its
    /// earlier outcomes or reset another same-name generation's counters.
    pub fn accumulate_delta(&mut self, current: Self, previous: &mut Self) {
        macro_rules! delta { ($($field:ident),+ $(,)?) => {$(
            self.$field = self.$field.saturating_add(current.$field.saturating_sub(previous.$field));
        )+}; }
        delta!(
            track_publish_attempts,
            track_publish_success,
            track_publish_cancels,
            track_subscribe_attempts,
            track_subscribe_success,
            track_subscribe_cancels,
            participant_signal_connected,
            participant_rtc_init,
            participant_rtc_connected,
            participant_rtc_canceled,
            participant_rtc_active
        );
        *previous = current;
    }
}
