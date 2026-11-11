//! Modified source policy from fixed routing/roommanager.go + psrpc retry.go.
//! Copyright LiveKit Inc., Apache-2.0. The method interceptor is always installed,
//! unlike optional generic ClientParams retry. No backoff sleep is introduced.
use crate::service_bus::rpc::RpcError;
use rtc_core::config::RoomDefaults;
use std::time::Duration;

pub(crate) struct CreateRoomRetry {
    max_attempts: i64,
    attempt: i64,
    timeout: Option<Duration>,
}
impl CreateRoomRetry {
    pub(crate) fn new(room: &RoomDefaults) -> Self {
        Self {
            max_attempts: room.create_room_attempts,
            attempt: 1,
            timeout: room
                .create_room_timeout
                .nonnegative_to_std()
                .filter(|d| !d.is_zero()),
        }
    }
    pub(crate) fn timeout(&self) -> Duration {
        self.timeout.unwrap_or(Duration::from_secs(3))
    }
    pub(crate) fn configured_timeout(&self) -> Option<Duration> {
        self.timeout
    }
    pub(crate) fn next(&mut self, error: &RpcError) -> bool {
        if !(error.is_transport()
            || matches!(error.code.as_str(), "deadline_exceeded" | "unavailable"))
            || self.attempt == self.max_attempts
        {
            return false;
        }
        self.attempt = self.attempt.wrapping_add(1);
        true
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn explicit_create_interceptor_does_not_disable_all_zero_and_has_no_backoff() {
        for attempts in [i64::MIN, -1, 0, 1, 2, 3] {
            for nanos in [-1, 0, 1, 37_000_000] {
                let mut room = crate::test_support::config().room;
                room.create_room_attempts = attempts;
                room.create_room_timeout = rtc_core::config::PolicyDuration::from_nanos(nanos);
                let mut r = CreateRoomRetry::new(&room);
                let timeout = if nanos > 0 {
                    Duration::from_nanos(nanos as u64)
                } else {
                    Duration::from_secs(3)
                };
                for n in 1..=4 {
                    assert_eq!(r.timeout(), timeout);
                    assert_eq!(r.configured_timeout(), (nanos > 0).then_some(timeout));
                    assert_eq!(
                        r.next(&RpcError::new("unavailable", "recoverable")),
                        n != attempts
                    );
                    if n == attempts {
                        break;
                    }
                }
            }
        }
    }
    #[test]
    fn application_errors_are_terminal_even_when_attempts_unlimited() {
        let mut room = crate::test_support::config().room;
        room.create_room_attempts = 0;
        assert!(
            !CreateRoomRetry::new(&room).next(&RpcError::new("invalid_argument", "bad request"))
        );
        assert!(!CreateRoomRetry::new(&room).next(&RpcError::closed()));
    }
}
