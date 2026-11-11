// SPDX-License-Identifier: BSD-3-Clause
// Formula derived from libwebrtc goog_cc_network_control.cc425-449 at
//202acee882062f7ca632472e4810f25938828f6a. See SOURCE.json and PATCHES.md.
//! Report-local propagation RTT observation; never an input change to GCC.
use std::time::{Duration, Instant};

/// Remove only receiver waiting evidenced inside this actual received batch.
/// Local and remote Instant values are never compared across clock domains.
/// Duration cannot represent Google's signed negative samples: any invalid
/// received sample makes the entire observation unknown rather than clean.
pub(super) fn min_propagation_rtt(
    feedback_at: Instant,
    received: impl Iterator<Item = (Instant, Instant)> + Clone,
) -> Option<Duration> {
    let max_received = received.clone().map(|(_, remote)| remote).max()?;
    let mut minimum: Option<Duration> = None;
    for (sent, remote) in received {
        let feedback_rtt = feedback_at.checked_duration_since(sent)?;
        let evidenced_wait = max_received.checked_duration_since(remote)?;
        let propagation = feedback_rtt.checked_sub(evidenced_wait)?;
        minimum = Some(minimum.map_or(propagation, |old| old.min(propagation)));
    }
    minimum
}

#[cfg(test)]
mod tests {
    use super::*;
    fn batch(local: Instant, remote: Instant) -> Vec<(Instant, Instant)> {
        (0..=8)
            .map(|i| {
                (
                    local + Duration::from_millis(i * 20),
                    remote + Duration::from_millis(10 + i * 20),
                )
            })
            .collect()
    }
    #[test]
    fn report_wait_is_removed_but_true_return_path_queue_remains() {
        let local = Instant::now();
        let remote = local + Duration::from_secs(90_000);
        let pairs = batch(local, remote);
        assert_eq!(
            min_propagation_rtt(local + Duration::from_millis(180), pairs.iter().copied()),
            Some(Duration::from_millis(20))
        );
        assert_eq!(
            min_propagation_rtt(local + Duration::from_millis(330), pairs.iter().copied()),
            Some(Duration::from_millis(170))
        );
    }
    #[test]
    fn reordered_and_duplicate_pairs_do_not_change_clock_domain_safe_result() {
        let local = Instant::now();
        let mut pairs = batch(local, local + Duration::from_secs(500));
        pairs.reverse();
        pairs.push(pairs[3]);
        assert_eq!(
            min_propagation_rtt(local + Duration::from_millis(180), pairs.iter().copied()),
            Some(Duration::from_millis(20))
        );
    }
    #[test]
    fn empty_received_batch_is_unknown_without_max_or_cached_fallback() {
        assert_eq!(
            min_propagation_rtt(Instant::now(), std::iter::empty()),
            None
        );
    }
    #[test]
    fn one_invalid_negative_pair_makes_whole_batch_unknown() {
        let local = Instant::now();
        let remote = local + Duration::from_secs(500);
        let pairs = [
            (local, remote),
            (local, remote + Duration::from_millis(200)),
        ];
        assert_eq!(
            min_propagation_rtt(local + Duration::from_millis(100), pairs.iter().copied()),
            None
        );
    }
    #[test]
    fn one_future_local_send_makes_whole_batch_unknown() {
        let local = Instant::now();
        let pairs = [(local, local), (local + Duration::from_secs(1), local)];
        assert_eq!(
            min_propagation_rtt(local + Duration::from_millis(100), pairs.iter().copied()),
            None
        );
    }
    #[test]
    fn singleton_cannot_remove_unknown_post_receive_report_wait() {
        let local = Instant::now();
        assert_eq!(
            min_propagation_rtt(
                local + Duration::from_millis(180),
                [(local, local + Duration::from_secs(500))].into_iter()
            ),
            Some(Duration::from_millis(180))
        );
    }
}
