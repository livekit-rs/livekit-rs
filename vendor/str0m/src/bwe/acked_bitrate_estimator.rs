// ALR-exit fast adaptation follows Google WebRTC classic GoogCC at commit
// 202acee882062f7ca632472e4810f25938828f6a. See googcc-alr-ack.patch,
// GOOGLE-WEBRTC-LICENSE and PATCHES.md for source attribution and scope.
use std::time::{Duration, Instant};

use crate::Bitrate;
use crate::rtp_::DataSize;

const SMALL_SAMPLE_THRESHOLD: DataSize = DataSize::bytes(2000);
const SMALL_SAMPLE_UNCERTAINTY: f64 = 25.0;
const UNCERTAINTY: f64 = 10.0;
const ESTIMATE_FLOOR: Bitrate = Bitrate::kbps(40);

pub struct AckedBitrateEstimator {
    /// The initial window to use for the first estimate.
    initial_window: Duration,
    /// The window to use for subsequent estimates after achieving the initial estimate.
    window: Duration,
    /// The estimate of the acked bitrate.
    estimate: Option<Bitrate>,
    /// The estimated variance.
    estimate_var: f64,
    /// The sum in the current window.
    sum: DataSize,
    /// The size of the current window.
    current_window: Duration,
    /// The last time the window was updated.
    last_update: Option<Instant>,
    alr_ended_at: Option<Instant>,
    fast_rate_changes: u64,
}

impl AckedBitrateEstimator {
    pub(super) fn new(initial_window: Duration, window: Duration) -> Self {
        Self {
            initial_window,
            window,
            estimate: None,
            estimate_var: 50.0,
            sum: DataSize::ZERO,
            current_window: Duration::ZERO,
            last_update: None,
            alr_ended_at: None,
            fast_rate_changes: 0,
        }
    }

    /// Arm a single fast adaptation for feedback about packets sent after ALR exit.
    pub(super) fn set_alr_ended_time(&mut self, at: Instant) {
        self.alr_ended_at = Some(at);
    }

    pub(super) fn fast_rate_changes(&self) -> u64 {
        self.fast_rate_changes
    }

    pub(super) fn update_with_send_time(
        &mut self,
        send_time: Instant,
        receive_time: Instant,
        packet_size: DataSize,
    ) {
        if self.alr_ended_at.is_some_and(|ended| send_time > ended) {
            // Google classic BitrateEstimator::ExpectFastRateChange(): boost
            // uncertainty, never set an estimate to requested/probed capacity.
            self.estimate_var += 200.0;
            self.alr_ended_at = None;
            self.fast_rate_changes += 1;
        }
        self.update(receive_time, packet_size);
    }

    pub(super) fn update(&mut self, receive_time: Instant, packet_size: DataSize) {
        let window = if self.estimate.is_none() {
            // Use the initial, larger, window at first
            self.initial_window
        } else {
            self.window
        };
        let Some((sample_estimate, is_small_sample)) =
            self.update_window(receive_time, packet_size, window)
        else {
            // No update
            return;
        };

        let Some(estimate) = self.estimate else {
            // This is the initial estimate, use it to initialize the estimate.
            self.estimate = Some(sample_estimate);
            return;
        };

        let scale = if is_small_sample && sample_estimate < estimate {
            SMALL_SAMPLE_UNCERTAINTY
        } else {
            UNCERTAINTY
        };

        let sample_estimate_bps = sample_estimate.as_f64();
        let estimate_bps = estimate.as_f64();
        // Define the sample uncertainty as a function of how far away it is from the
        // current estimate. With low values of uncertainty_symmetry_cap_ we add more
        // uncertainty to increases than to decreases. For higher values we approach
        // symmetry.
        let sample_uncertainty = scale * (estimate_bps - sample_estimate_bps).abs() / estimate_bps;
        let sample_var = sample_uncertainty.powf(2.0);

        // Update a bayesian estimate of the rate, weighting it lower if the sample
        // uncertainty is large.
        // The bitrate estimate uncertainty is increased with each update to model
        // that the bitrate changes over time.
        let pred_bitrate_estimate_var = self.estimate_var + 5.0;
        let mut new_estimate = (sample_var * estimate_bps
            + pred_bitrate_estimate_var * sample_estimate_bps)
            / (sample_var + pred_bitrate_estimate_var);

        new_estimate = new_estimate.max(ESTIMATE_FLOOR.as_f64());
        self.estimate = Some(Bitrate::bps(new_estimate.ceil() as u64));
        self.estimate_var =
            (sample_var * pred_bitrate_estimate_var) / (sample_var + pred_bitrate_estimate_var);
    }

    pub(super) fn current_estimate(&self) -> Option<Bitrate> {
        self.estimate
    }

    fn update_window(
        &mut self,
        receive_time: Instant,
        packet_size: DataSize,
        window: Duration,
    ) -> Option<(Bitrate, bool)> {
        let time_moved_back = Some(receive_time) < self.last_update;
        if time_moved_back {
            // Time moved backwards, reset state
            self.sum = DataSize::ZERO;
            self.current_window = Duration::ZERO;
            self.last_update = Some(receive_time);

            return None;
        }

        if let Some(last) = self.last_update {
            self.current_window += receive_time - last;
            if receive_time - last >= window {
                // No update for a while, reset estimates.
                self.sum = DataSize::ZERO;
                let remaining = self.current_window.as_micros() as u64 % window.as_micros() as u64;
                self.current_window = Duration::from_micros(remaining);
            }
        }

        self.last_update = Some(receive_time);

        let mut estimate = None;

        let mut is_small = false;
        if self.current_window >= window {
            is_small = self.sum < SMALL_SAMPLE_THRESHOLD;
            estimate = Some(self.sum / window);
            self.sum = DataSize::ZERO;
            self.current_window -= window;
        }

        self.sum += packet_size;

        estimate.map(|e| (e, is_small))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn alr_exit_waits_for_post_exit_send_and_boosts_variance_exactly_once() {
        let now = Instant::now();
        let mut estimator =
            AckedBitrateEstimator::new(Duration::from_millis(500), Duration::from_millis(150));
        estimator.update(now, DataSize::bytes(100));
        let exit = now + Duration::from_millis(100);
        estimator.set_alr_ended_time(exit);
        for sent in [now, exit] {
            estimator.update_with_send_time(
                sent,
                now + Duration::from_millis(120),
                DataSize::bytes(100),
            );
            assert_eq!(
                estimator.fast_rate_changes(),
                0,
                "old/in-flight packets must not trigger"
            );
            assert_eq!(estimator.estimate_var, 50.0);
        }
        estimator.update_with_send_time(
            exit + Duration::from_millis(1),
            now + Duration::from_millis(130),
            DataSize::bytes(100),
        );
        assert_eq!(estimator.fast_rate_changes(), 1);
        assert_eq!(estimator.estimate_var, 250.0);
        estimator.update_with_send_time(
            exit + Duration::from_millis(2),
            now + Duration::from_millis(140),
            DataSize::bytes(100),
        );
        assert_eq!(estimator.fast_rate_changes(), 1);
        assert_eq!(estimator.estimate_var, 250.0);
        assert!(
            estimator.current_estimate().is_none(),
            "variance notification cannot invent capacity before any sample"
        );
    }

    #[test]
    fn a_second_real_alr_exit_can_arm_a_second_adaptation() {
        let now = Instant::now();
        let mut estimator =
            AckedBitrateEstimator::new(Duration::from_millis(500), Duration::from_millis(150));
        estimator.set_alr_ended_time(now);
        estimator.update_with_send_time(
            now + Duration::from_millis(1),
            now + Duration::from_millis(10),
            DataSize::bytes(100),
        );
        estimator.set_alr_ended_time(now + Duration::from_millis(20));
        estimator.update_with_send_time(
            now + Duration::from_millis(21),
            now + Duration::from_millis(30),
            DataSize::bytes(100),
        );
        assert_eq!(estimator.fast_rate_changes(), 2);
        assert_eq!(estimator.estimate_var, 450.0);
    }

    #[test]
    fn test_no_estimate_before_initial_window_has_passed() {
        let now = Instant::now();
        let mut estimator =
            AckedBitrateEstimator::new(Duration::from_millis(500), Duration::from_millis(150));

        estimator.update(now, DataSize::bytes(950));
        estimator.update(now + Duration::from_millis(250), DataSize::bytes(381));
        estimator.update(now + Duration::from_millis(499), DataSize::bytes(1110));

        assert!(
            estimator.current_estimate().is_none(),
            "AckedBitrateEstiamtor should produce no estimate before the initial window is reached"
        );

        estimator.update(now + Duration::from_millis(501), DataSize::bytes(1110));

        let estimate = estimator.current_estimate().expect(
            "After the first window of time has passed AckedBitrateEstimator should produce an estimate"
        );

        assert_eq!(
            estimate.as_u64(),
            39056,
            "AckedBitrateEstiamtor should produce the correct bitrate"
        );
    }

    #[test]
    fn test_correct_estimate_after_initial_window() {
        let now = Instant::now();
        let mut estimator =
            AckedBitrateEstimator::new(Duration::from_millis(500), Duration::from_millis(150));

        estimator.update(now, DataSize::bytes(2500));
        estimator.update(now + Duration::from_millis(250), DataSize::bytes(1392));
        estimator.update(now + Duration::from_millis(499), DataSize::bytes(4021));
        estimator.update(now + Duration::from_millis(500), DataSize::bytes(0));

        assert!(
            estimator.current_estimate().is_some(),
            "After the first window of time has passed AckedBitrateEstimator should produce an estimate"
        );

        estimator.update(now + Duration::from_millis(550), DataSize::bytes(271));
        estimator.update(now + Duration::from_millis(558), DataSize::bytes(813));
        estimator.update(now + Duration::from_millis(648), DataSize::bytes(731));
        // Will not be counted, part of next window
        estimator.update(now + Duration::from_millis(651), DataSize::bytes(900));

        let estimate = estimator.current_estimate().expect(
            "After the first window of time has passed AckedBitrateEstimator should produce an estimate"
        );

        assert_eq!(
            estimate.as_u64(),
            108320,
            "AckedBitrateEstiamtor should produce the correct bitrate"
        );
    }

    #[test]
    fn test_long_gap_preserves_overflow_via_modulo_after_initial() {
        let now = Instant::now();
        let window = Duration::from_millis(150);
        let mut estimator = AckedBitrateEstimator::new(Duration::from_millis(500), window);

        // First, get past the initial 500ms window to establish an estimate
        // Need to send enough data over 500ms to trigger the first estimate
        for i in 0..50 {
            estimator.update(now + Duration::from_millis(i * 10), DataSize::bytes(1000));
        }
        estimator.update(now + Duration::from_millis(501), DataSize::bytes(1000));
        assert!(
            estimator.current_estimate().is_some(),
            "Should have initial estimate after 500ms"
        );

        // Now we're using the 150ms window
        // Build up current_window to 100ms
        estimator.update(now + Duration::from_millis(550), DataSize::bytes(1000));
        estimator.update(now + Duration::from_millis(650), DataSize::bytes(1000));
        // current_window is now 100ms

        // Long gap of 225ms (> 150ms window)
        // current_window becomes 100 + 225 = 325ms
        // After gap detection: current_window = 325ms % 150ms = 25ms (correct)
        //                  or: current_window = 150ms % 150ms = 0ms (buggy)
        estimator.update(now + Duration::from_millis(875), DataSize::bytes(1000));

        // Add 50ms
        estimator.update(now + Duration::from_millis(925), DataSize::bytes(1000));
        // Correct: current_window = 25 + 50 = 75ms
        // Buggy: current_window = 0 + 50 = 50ms

        // Add 80ms
        let estimate_before = estimator.current_estimate().unwrap();
        estimator.update(now + Duration::from_millis(1005), DataSize::bytes(1000));
        // Correct: current_window = 75 + 80 = 155ms (>= 150ms, triggers new sample)
        // Buggy: current_window = 50 + 80 = 130ms (< 150ms, no new sample)
        let estimate_after = estimator.current_estimate().unwrap();

        // With correct modulo, estimate should change (new sample produced)
        // With buggy modulo, estimate stays the same (no sample produced yet)
        assert_ne!(
            estimate_before.as_u64(),
            estimate_after.as_u64(),
            "Estimate should change after accumulating >= 150ms due to correct modulo overflow"
        );
    }
}
