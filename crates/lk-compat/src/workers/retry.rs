//! Signed fixed protocol/rpc/typed_api.go + psrpc middleware retry policy.
use crate::service_bus::rpc::RpcError;
use rtc_core::config::PolicyDuration;
#[derive(Clone, Copy, Debug)]
pub struct RpcPolicy {
    pub max_attempts: i64,
    pub timeout: PolicyDuration,
    pub backoff: PolicyDuration,
    pub buffer_size: i64,
}
impl Default for RpcPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            timeout: PolicyDuration::from_secs(3),
            backoff: PolicyDuration::from_secs(2),
            buffer_size: 1000,
        }
    }
}
impl RpcPolicy {
    pub const fn from_raw(
        max_attempts: i64,
        timeout_ns: i64,
        backoff_ns: i64,
        buffer_size: i64,
    ) -> Self {
        Self {
            max_attempts,
            timeout: PolicyDuration::from_nanos(timeout_ns),
            backoff: PolicyDuration::from_nanos(backoff_ns),
            buffer_size,
        }
    }
    /// Source ClientParams leaves zero at bus default100, passes nonzero
    /// through. Negative cannot allocate a channel and is an explicit error.
    pub fn handler_channel_size(self) -> Result<usize, &'static str> {
        if self.buffer_size == 0 {
            Ok(100)
        } else {
            usize::try_from(self.buffer_size).map_err(|_| "negative psrpc client buffer_size")
        }
    }
    /// Fixed PSRPC bus default when middleware has no strictly-positive override.
    pub fn request_timeout(self) -> std::time::Duration {
        self.timeout
            .nonnegative_to_std()
            .filter(|value| !value.is_zero())
            .unwrap_or(std::time::Duration::from_secs(3))
    }
    pub fn enabled(self) -> bool {
        self.max_attempts != 0
            || self.timeout.as_nanos_i64() != 0
            || self.backoff.as_nanos_i64() != 0
    }
}
pub(crate) struct GenericRetry {
    policy: RpcPolicy,
    attempt: i64,
    timeout: i64,
}
impl GenericRetry {
    pub fn new(policy: RpcPolicy) -> Self {
        Self {
            policy,
            attempt: 1,
            timeout: policy.timeout.as_nanos_i64(),
        }
    }
    pub fn timeout(&self) -> Option<std::time::Duration> {
        (self.policy.enabled() && self.timeout > 0)
            .then(|| std::time::Duration::from_nanos(self.timeout as u64))
    }
    pub fn next(&mut self, error: &RpcError) -> bool {
        if !self.policy.enabled()
            || !generic_recoverable(error)
            || self.attempt == self.policy.max_attempts
        {
            return false;
        }
        self.timeout = self
            .timeout
            .wrapping_add(self.policy.backoff.as_nanos_i64());
        self.attempt = self.attempt.wrapping_add(1);
        true
    }
}
pub(super) fn generic_recoverable(error: &RpcError) -> bool {
    error.is_transport() || matches!(error.code.as_str(), "deadline_exceeded" | "unavailable")
}
pub(super) fn egress_recoverable(error: &RpcError) -> bool {
    generic_recoverable(error) || error.code == "resource_exhausted"
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fixed_retry_adds_timeout_not_sleep_and_keeps_signed_zero_semantics() {
        let e = RpcError::new("unavailable", "fixture");
        let mut retry = GenericRetry::new(RpcPolicy::default());
        assert_eq!(retry.timeout(), Some(std::time::Duration::from_secs(3)));
        assert!(retry.next(&e));
        assert_eq!(retry.timeout(), Some(std::time::Duration::from_secs(5)));
        assert!(retry.next(&e));
        assert_eq!(retry.timeout(), Some(std::time::Duration::from_secs(7)));
        assert!(!retry.next(&e));
        let mut negative = GenericRetry::new(RpcPolicy {
            max_attempts: 2,
            timeout: PolicyDuration::from_nanos(-3),
            backoff: PolicyDuration::from_nanos(5),
            buffer_size: 1000,
        });
        assert_eq!(negative.timeout(), None);
        assert!(negative.next(&e));
        assert_eq!(negative.timeout(), Some(std::time::Duration::from_nanos(2)));
        let mut disabled = GenericRetry::new(RpcPolicy {
            max_attempts: 0,
            timeout: PolicyDuration::ZERO,
            backoff: PolicyDuration::ZERO,
            buffer_size: 1000,
        });
        assert!(!disabled.next(&e));
        let resource = RpcError::new("resource_exhausted", "fixture");
        assert!(egress_recoverable(&resource));
        assert!(!generic_recoverable(&resource));
    }
}

#[cfg(test)]
mod request_timeout_tests {
    use super::*;
    #[test]
    fn only_positive_middleware_timeout_overrides_fixed_bus_default() {
        for nanos in [0, -1, -3_000_000_000] {
            let policy = RpcPolicy::from_raw(3, nanos, 2_000_000_000, 1000);
            assert_eq!(policy.request_timeout(), std::time::Duration::from_secs(3));
        }
        assert_eq!(
            RpcPolicy::default().request_timeout(),
            std::time::Duration::from_secs(3)
        );
        assert_eq!(
            RpcPolicy::from_raw(3, 17_000_000, 0, 1000).request_timeout(),
            std::time::Duration::from_millis(17)
        );
    }
}
