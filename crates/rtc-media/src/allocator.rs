//! Subscriber stream allocation with explicit reserved and overshoot budgets.
//!
//! Audio is reserved before video; unmanaged screen shares are charged first.
//! Managed video obtains base layers before priority-ordered upgrades. Pausing
//! follows the negotiated LiveKit policy: if disabled, unavoidable base-layer
//! overshoot is measured rather than hidden. Payload rates exclude wire overhead.

use rtc_core::ids::TrackSid;
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

use crate::layers::{INVALID_SPATIAL, MAX_SPATIAL};

pub(crate) const INITIAL_CAPACITY_BPS: u64 = 2_500_000;
pub(crate) const AUDIO_ALLOWANCE_BPS: u64 = 64_000;

pub(crate) const PRIORITY_SCREENSHARE: u8 = 255;
pub(crate) const PRIORITY_DEFAULT: u8 = 1;

#[derive(Clone, Debug)]
pub(crate) struct AllocTrack {
    pub priority: u8,
    /// Desired max spatial from subscriber settings (-1 = muted/disabled: not
    /// allocated, cap unchanged).
    pub max_spatial: i32,
    /// Measured bitrate per spatial layer (bps); 0 = not seen / unknown.
    pub bitrates: [u64; 3],
    /// Unmanaged: always gets its optimal layer, usage subtracted first.
    pub exempt: bool,
}

/// Bitrate of the highest measured layer ≤ `max`, and that layer.
fn optimal(t: &AllocTrack) -> (i32, u64) {
    let mut s = t.max_spatial.min(MAX_SPATIAL);
    while s >= 0 {
        let b = t.bitrates[s as usize];
        if b > 0 {
            return (s, b);
        }
        s -= 1;
    }
    (t.max_spatial.clamp(0, MAX_SPATIAL), 0)
}

// Three seconds of immutable validation plus the nominal one-second source
// rate window. This stabilizes demand, never proves network capacity.
const COST_PEAK_WINDOW: Duration = Duration::from_secs(4);
// Bound the recent cost history independently of the configured source
// reporting cadence. Only distinct completed samples consume entries.
const MAX_COST_SAMPLES: usize = 9;

#[derive(Clone, Copy)]
pub(crate) struct LayerBitrateObservation {
    pub generation: u64,
    pub dd_temporal_limit: Option<u8>,
    pub dd_active_layers: Option<[bool; 3]>,
    pub completed_at: [Option<Instant>; 3],
    pub bitrates: [u64; 3],
}

struct LayerCostHistory {
    generation: u64,
    last_epoch: [Option<Instant>; 3],
    last_nonzero: [u64; 3],
    windows: [VecDeque<(Instant, u64)>; 3],
}

impl LayerCostHistory {
    fn new(generation: u64) -> Self {
        Self {
            generation,
            last_epoch: [None; 3],
            last_nonzero: [0; 3],
            windows: std::array::from_fn(|_| VecDeque::new()),
        }
    }
}

/// Recent completed-window peaks stabilize demand through a validation cycle.
/// Silence keeps the last real nonzero cost, while a different source generation
/// discards old codec/RID measurements. Repeated allocation polls add no samples.
#[derive(Default)]
pub(crate) struct LayerBitrateCache(HashMap<(TrackSid, Option<u8>), LayerCostHistory>);

impl LayerBitrateCache {
    pub fn retain(&mut self, active: &HashSet<TrackSid>) {
        self.0.retain(|(sid, _), _| active.contains(sid));
    }

    pub fn resolve(
        &mut self,
        sid: &TrackSid,
        observed: LayerBitrateObservation,
        declared: [u64; 3],
        now: Instant,
    ) -> [u64; 3] {
        let history = self
            .0
            // A higher temporal ceiling's peak must not inflate a capped
            // subscriber's current demand after a settings change.
            .entry((sid.clone(), observed.dd_temporal_limit))
            .or_insert_with(|| LayerCostHistory::new(observed.generation));
        if history.generation != observed.generation {
            *history = LayerCostHistory::new(observed.generation);
        }
        for window in &mut history.windows {
            while window
                .front()
                .is_some_and(|(at, _)| now.saturating_duration_since(*at) >= COST_PEAK_WINDOW)
            {
                window.pop_front();
            }
        }
        for (i, &rate) in observed.bitrates.iter().enumerate() {
            // An inactive DD layer masks the observation. Keep its last
            // completed epoch eligible when the publisher reactivates it.
            if observed.dd_active_layers.is_some_and(|active| !active[i]) {
                continue;
            }
            if let Some(at) = observed.completed_at[i]
                && at <= now
                && history.last_epoch[i].is_none_or(|last| at > last)
            {
                history.last_epoch[i] = Some(at);
                if rate == 0 {
                    continue;
                }
                history.last_nonzero[i] = rate;
                if now.saturating_duration_since(at) < COST_PEAK_WINDOW {
                    history.windows[i].push_back((at, rate));
                    while history.windows[i].len() > MAX_COST_SAMPLES {
                        history.windows[i].pop_front();
                    }
                }
            }
        }
        std::array::from_fn(|i| {
            if observed.dd_active_layers.is_some_and(|active| !active[i]) {
                return 0;
            }
            let fallback = if history.last_nonzero[i] > 0 {
                history.last_nonzero[i]
            } else if declared[i] > 0 {
                declared[i]
            } else {
                [150_000, 500_000, 1_700_000][i]
            };
            history.windows[i]
                .iter()
                .map(|(_, rate)| *rate)
                .max()
                .unwrap_or(fallback)
        })
    }
}

#[derive(Debug)]
pub(crate) struct AllocationPlan {
    pub caps: Vec<i32>,
    /// Reserved audio plus modeled payload cost of granted video layers.
    pub allocated_bps: u64,
    pub video_budget_bps: Option<u64>,
    /// Nonzero only for exempt tracks, unpausable base layers or audio itself.
    pub overshoot_bps: u64,
}

/// Reserve audio plus a 10% RTP/SRTP/RTCP/IP allowance before choosing video.
pub(crate) fn plan(
    capacity: Option<u64>,
    tracks: &[AllocTrack],
    audio_count: u64,
    allow_pause: bool,
) -> AllocationPlan {
    plan_with_unmanaged_policy(capacity, tracks, audio_count, allow_pause, false)
}

/// Original DisableEstimationUnmanagedTracks changes capacity accounting only:
/// exempt tracks still receive their optimal layer and total output is counted.
pub(crate) fn plan_with_unmanaged_policy(
    capacity: Option<u64>,
    tracks: &[AllocTrack],
    audio_count: u64,
    allow_pause: bool,
    disable_estimation_unmanaged_tracks: bool,
) -> AllocationPlan {
    let audio = audio_count.saturating_mul(AUDIO_ALLOWANCE_BPS);
    let payload_capacity = capacity.map(|c| c.saturating_sub(c / 10));
    let video_budget = payload_capacity.map(|c| c.saturating_sub(audio));
    let caps = allocate_with_unmanaged_policy(
        video_budget,
        tracks,
        allow_pause,
        disable_estimation_unmanaged_tracks,
    );
    let video = tracks.iter().zip(&caps).fold(0u64, |sum, (track, cap)| {
        if *cap < 0 {
            sum
        } else {
            sum.saturating_add(track.bitrates[*cap as usize])
        }
    });
    let allocated_bps = audio.saturating_add(video);
    AllocationPlan {
        caps,
        allocated_bps,
        video_budget_bps: video_budget,
        overshoot_bps: payload_capacity.map_or(0, |c| allocated_bps.saturating_sub(c)),
    }
}

/// Wire budget needed for the next useful layer transition. Probe toward a
/// reachable allocation, rather than adding fixed increments below an expensive
/// next layer forever. Paused tracks acquire a base layer before upgrades.
pub(crate) fn next_probe_budget(tracks: &[AllocTrack], plan: &AllocationPlan) -> Option<u64> {
    let mut candidates = Vec::new();
    for (track, &cap) in tracks.iter().zip(&plan.caps) {
        if track.exempt || track.max_spatial < 0 {
            continue;
        }
        let next = ((cap + 1).max(0)..=track.max_spatial.min(MAX_SPATIAL))
            .find(|&layer| track.bitrates[layer as usize] > 0);
        let Some(next) = next else {
            continue;
        };
        let current = if cap < 0 {
            0
        } else {
            track.bitrates[cap as usize]
        };
        let payload = plan
            .allocated_bps
            .saturating_sub(current)
            .saturating_add(track.bitrates[next as usize]);
        let wire = payload.saturating_add(payload.div_ceil(9));
        candidates.push((cap < 0, track.priority, wire));
    }
    candidates.sort_by(|a, b| b.0.cmp(&a.0).then(b.1.cmp(&a.1)).then(a.2.cmp(&b.2)));
    candidates.first().map(|candidate| candidate.2)
}

/// Total bitrate wanted if every track got its desired layer (BWE probing target).
pub(crate) fn desired_usage(tracks: &[AllocTrack]) -> u64 {
    tracks
        .iter()
        .filter(|t| t.max_spatial >= 0)
        .map(|t| optimal(t).1)
        .fold(0u64, u64::saturating_add)
}

/// Returns one spatial cap per input track (same order). `capacity = None` means no
/// estimate yet: everything gets its desired layer.
pub(crate) fn allocate(
    capacity: Option<u64>,
    tracks: &[AllocTrack],
    allow_pause: bool,
) -> Vec<i32> {
    allocate_with_unmanaged_policy(capacity, tracks, allow_pause, false)
}

pub(crate) fn allocate_with_unmanaged_policy(
    capacity: Option<u64>,
    tracks: &[AllocTrack],
    allow_pause: bool,
    disable_estimation_unmanaged_tracks: bool,
) -> Vec<i32> {
    let mut caps: Vec<i32> = tracks
        .iter()
        .map(|t| t.max_spatial.min(MAX_SPATIAL))
        .collect();
    let Some(capacity) = capacity else {
        return caps;
    };

    let mut remaining = capacity;
    if !disable_estimation_unmanaged_tracks {
        for t in tracks.iter().filter(|t| t.exempt && t.max_spatial >= 0) {
            remaining = remaining.saturating_sub(optimal(t).1);
        }
    }

    // Tracks without any measurement yet are not constrained (nothing to budget).
    let mut managed: Vec<usize> = (0..tracks.len())
        .filter(|&i| {
            !tracks[i].exempt
                && tracks[i].max_spatial >= 0
                && tracks[i].bitrates.iter().any(|b| *b > 0)
        })
        .collect();
    if managed.is_empty() {
        return caps;
    }
    if remaining == 0 && allow_pause {
        for &i in &managed {
            caps[i] = INVALID_SPATIAL;
        }
        return caps;
    }
    managed.sort_by(|&a, &b| {
        tracks[b]
            .priority
            .cmp(&tracks[a].priority)
            .then(tracks[b].max_spatial.cmp(&tracks[a].max_spatial))
    });

    let mut granted: Vec<Option<(i32, u64)>> = vec![None; tracks.len()];
    for spatial in 0..=MAX_SPATIAL {
        for &i in &managed {
            let t = &tracks[i];
            if spatial > t.max_spatial {
                continue;
            }
            let needed = t.bitrates[spatial as usize];
            let already = granted[i].map(|g| g.1).unwrap_or(0);
            if needed == 0 {
                continue;
            }
            // Go's provisional pass takes the first measured layer when pause
            // is disabled, even over budget. Charge it before any upgrades.
            if needed <= remaining.saturating_add(already) || (!allow_pause && granted[i].is_none())
            {
                remaining = remaining.saturating_add(already).saturating_sub(needed);
                granted[i] = Some((spatial, needed));
            }
        }
    }

    for &i in &managed {
        caps[i] = match granted[i] {
            Some((s, _)) => s,
            None if allow_pause => INVALID_SPATIAL,
            None => 0,
        };
    }
    caps
}

#[cfg(test)]
mod tests {
    use super::*;

    fn track(priority: u8, max: i32, b: [u64; 3]) -> AllocTrack {
        AllocTrack {
            priority,
            max_spatial: max,
            bitrates: b,
            exempt: false,
        }
    }

    const B: [u64; 3] = [150_000, 500_000, 1_700_000];

    #[test]
    fn disabled_unmanaged_estimation_preserves_optimal_share_but_does_not_charge_managed_budget() {
        let mut share = track(PRIORITY_SCREENSHARE, 0, [500_000, 0, 0]);
        share.exempt = true;
        let camera = track(PRIORITY_DEFAULT, 0, [200_000, 0, 0]);
        let tracks = [share, camera];
        let charged = plan_with_unmanaged_policy(Some(500_000), &tracks, 0, true, false);
        let excluded = plan_with_unmanaged_policy(Some(500_000), &tracks, 0, true, true);
        assert_eq!(charged.caps, [0, -1]);
        assert_eq!(excluded.caps, [0, 0]);
        assert_eq!(charged.allocated_bps, 500_000);
        assert_eq!(
            excluded.allocated_bps, 700_000,
            "total actual output is not hidden from stats"
        );
        assert_eq!(excluded.video_budget_bps, charged.video_budget_bps);
        assert_eq!(
            desired_usage(&tracks),
            700_000,
            "fixed source expected usage still includes unmanaged video"
        );
        let zero = plan_with_unmanaged_policy(Some(0), &tracks, 0, true, true);
        assert_eq!(zero.caps, [0, -1], "no invented managed capacity grant");
        assert_eq!(
            plan_with_unmanaged_policy(None, &tracks, 0, true, true).caps,
            [0, 0]
        );
    }

    #[test]
    fn no_estimate_is_unconstrained() {
        let t = vec![track(1, 2, B), track(1, 1, B)];
        assert_eq!(allocate(None, &t, false), vec![2, 1]);
    }

    #[test]
    fn plenty_of_capacity_gives_desired() {
        let t = vec![track(1, 2, B), track(1, 2, B)];
        assert_eq!(allocate(Some(10_000_000), &t, false), vec![2, 2]);
        assert_eq!(desired_usage(&t), 3_400_000);
    }

    #[test]
    fn lowest_layers_first_then_priority() {
        // layer 0 for both (300k), then layer 1 for the screenshare first (priority)
        let t = vec![track(1, 2, B), track(PRIORITY_SCREENSHARE, 2, B)];
        let caps = allocate(Some(800_000), &t, false);
        assert_eq!(caps, vec![0, 1]);
        let caps = allocate(Some(1_000_000), &t, false);
        assert_eq!(caps, vec![1, 1]);
    }

    #[test]
    fn starved_track_gets_lowest_or_pauses() {
        let t = vec![track(1, 2, B)];
        assert_eq!(allocate(Some(10_000), &t, false), vec![0]);
        assert_eq!(allocate(Some(10_000), &t, true), vec![-1]);
        assert_eq!(allocate(Some(0), &t, true), vec![-1]);
    }

    #[test]
    fn provisional_allocation_charges_unpausable_minimum_before_upgrades() {
        let tracks = [track(2, 2, [50, 200, 0]), track(1, 2, [400, 0, 0])];
        let sparse = [track(1, 2, [0, 400, 0])];
        let vectors = [
            (
                "reserve_unpausable_base_before_upgrade",
                tracks.as_slice(),
                300,
                false,
                vec![0, 0],
                450,
            ),
            (
                "pause_allows_upgrade",
                tracks.as_slice(),
                300,
                true,
                vec![1, -1],
                200,
            ),
            (
                "force_first_measured_sparse_layer",
                sparse.as_slice(),
                300,
                false,
                vec![1],
                400,
            ),
            (
                "pause_sparse_layer_over_budget",
                sparse.as_slice(),
                300,
                true,
                vec![-1],
                0,
            ),
            (
                "fully_funded_upgrade",
                tracks.as_slice(),
                600,
                false,
                vec![1, 0],
                600,
            ),
        ];
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for (name, input, capacity, allow_pause, want, usage) in vectors {
            let caps = allocate(Some(capacity), input, allow_pause);
            let granted_bps = input
                .iter()
                .zip(&caps)
                .filter_map(|(t, cap)| usize::try_from(*cap).ok().map(|index| t.bitrates[index]))
                .sum::<u64>();
            eprintln!("{name}: caps={caps:?} usage={granted_bps}");
            actual.push((caps, granted_bps));
            expected.push((want, usage));
        }
        assert_eq!(actual, expected);
    }

    #[test]
    fn exempt_usage_is_subtracted() {
        let mut share = track(PRIORITY_SCREENSHARE, 0, [2_000_000, 0, 0]);
        share.exempt = true;
        let t = vec![share, track(1, 2, B)];
        let caps = allocate(Some(2_700_000), &t, false);
        assert_eq!(caps, vec![0, 1]);
    }

    #[test]
    fn unknown_bitrates_do_not_block() {
        let t = vec![track(1, 2, [0, 0, 0])];
        assert_eq!(allocate(Some(100_000), &t, true), vec![2]);
        // a layer that was never seen is not granted when others are measured
        let t = vec![track(1, 2, [150_000, 0, 1_700_000])];
        assert_eq!(allocate(Some(1_000_000), &t, false), vec![0]);
    }

    #[test]
    fn disabled_tracks_are_skipped() {
        let t = vec![track(1, -1, B), track(1, 2, B)];
        assert_eq!(allocate(Some(10_000_000), &t, false), vec![-1, 2]);
    }
    #[test]
    fn audio_and_wire_reserves_are_charged_before_video() {
        let tracks = vec![track(1, 2, B), track(1, 2, B)];
        let p = plan(Some(500_000), &tracks, 1, true);
        assert_eq!(p.video_budget_bps, Some(386_000));
        assert_eq!(p.caps, [0, 0]);
        assert_eq!(p.allocated_bps, 364_000);
        assert_eq!(p.overshoot_bps, 0);
    }
    #[test]
    fn managed_budget_is_conserved_across_capacity_and_priority() {
        let tracks = vec![
            track(1, 2, B),
            track(PRIORITY_SCREENSHARE, 2, B),
            track(1, 1, B),
        ];
        for capacity in (0..=7_000_000).step_by(12_345) {
            let p = plan(Some(capacity), &tracks, 1, true);
            // Audio is intentionally not paused; every managed video grant fits.
            assert!(p.allocated_bps <= (capacity - capacity / 10).max(AUDIO_ALLOWANCE_BPS));
            if capacity > AUDIO_ALLOWANCE_BPS * 10 / 9 {
                assert_eq!(p.overshoot_bps, 0);
            }
        }
    }
    #[test]
    fn pause_then_restore_recomputes_targets_without_overshoot() {
        let tracks = vec![track(1, 2, B), track(PRIORITY_SCREENSHARE, 2, B)];
        assert_eq!(plan(Some(100_000), &tracks, 1, true).caps, [-1, -1]);
        assert_eq!(plan(Some(300_000), &tracks, 1, true).caps, [-1, 0]);
        assert_eq!(plan(Some(1_000_000), &tracks, 1, true).caps, [0, 1]);
        assert_eq!(plan(Some(5_000_000), &tracks, 1, true).caps, [2, 2]);
    }
    #[test]
    fn exempt_and_unpausable_costs_report_explicit_overshoot() {
        let mut share = track(PRIORITY_SCREENSHARE, 0, [1_000_000, 0, 0]);
        share.exempt = true;
        let p = plan(Some(500_000), &[share, track(1, 2, B)], 1, false);
        assert_eq!(p.caps, [0, 0]);
        assert_eq!(p.allocated_bps, 1_214_000);
        assert_eq!(p.overshoot_bps, 764_000);
    }
    fn observation(at: Instant, rates: [u64; 3]) -> LayerBitrateObservation {
        LayerBitrateObservation {
            generation: 1,
            dd_temporal_limit: None,
            dd_active_layers: None,
            completed_at: [Some(at); 3],
            bitrates: rates,
        }
    }

    #[test]
    fn completed_window_dips_do_not_undercut_a_validation_request() {
        let now = Instant::now();
        let sid = TrackSid::from("TR_cost_trace");
        let mut cache = LayerBitrateCache::default();
        for (epoch, low) in [114_488, 85_880, 71_710].into_iter().enumerate() {
            let at = now + Duration::from_secs(epoch as u64);
            let stable =
                cache.resolve(&sid, observation(at, [low, 500_000, 1_700_000]), [0; 3], at);
            assert_eq!(stable[0], 114_488);
            let track = track(1, 2, stable);
            let paused = plan(Some(90_377), std::slice::from_ref(&track), 1, true);
            assert_eq!(next_probe_budget(&[track], &paused), Some(198_320));
            // Stabilizing source cost must not grant a budget or send a layer
            // that exceeds the actual externally supplied allocation budget.
            assert_eq!(paused.caps, [-1]);
            assert_eq!(paused.allocated_bps, AUDIO_ALLOWANCE_BPS);
        }
    }

    #[test]
    fn paused_layer_measurements_do_not_become_free() {
        let now = Instant::now();
        let sid = TrackSid::from("TR_cost");
        let mut cache = LayerBitrateCache::default();
        assert_eq!(cache.resolve(&sid, observation(now, B), [0; 3], now), B);
        let at = now + COST_PEAK_WINDOW;
        assert_eq!(
            cache.resolve(&sid, observation(at, [120_000, 0, 0]), [0; 3], at),
            [120_000, 500_000, 1_700_000]
        );
        let at = at + COST_PEAK_WINDOW;
        let restored = cache.resolve(&sid, observation(at, [0; 3]), [0; 3], at);
        assert_eq!(restored, [120_000, 500_000, 1_700_000]);
        assert_eq!(
            allocate(Some(100_000), &[track(1, 2, restored)], true),
            [-1]
        );
        cache.retain(&HashSet::new());
        assert!(cache.0.is_empty());
    }

    #[test]
    fn peaks_expire_by_completed_epoch_not_repeated_polls() {
        let now = Instant::now();
        let sid = TrackSid::from("TR_epoch");
        let mut cache = LayerBitrateCache::default();
        cache.resolve(&sid, observation(now, [300_000, 0, 0]), [0; 3], now);
        let next = now + Duration::from_secs(1);
        let low = observation(next, [100_000, 0, 0]);
        assert_eq!(cache.resolve(&sid, low, [0; 3], next)[0], 300_000);
        for millis in 1001..4000 {
            assert_eq!(
                cache.resolve(&sid, low, [0; 3], now + Duration::from_millis(millis))[0],
                300_000
            );
        }
        assert_eq!(
            cache.resolve(&sid, low, [0; 3], now + COST_PEAK_WINDOW)[0],
            100_000
        );
        assert_eq!(cache.0[&(sid.clone(), None)].windows[0].len(), 1);
        assert_eq!(
            cache.resolve(&sid, low, [0; 3], now + Duration::from_secs(5))[0],
            100_000
        );
        assert!(cache.0[&(sid.clone(), None)].windows[0].is_empty());
    }

    #[test]
    fn new_high_cost_is_immediate_and_never_spends_more_than_fixed_budget() {
        let now = Instant::now();
        let sid = TrackSid::from("TR_up");
        let mut cache = LayerBitrateCache::default();
        cache.resolve(&sid, observation(now, [100_000, 0, 0]), [0; 3], now);
        let at = now + Duration::from_secs(1);
        let cost = cache.resolve(&sid, observation(at, [300_000, 0, 0]), [0; 3], at);
        assert_eq!(cost[0], 300_000);
        let plan = plan(Some(200_000), &[track(1, 2, cost)], 1, true);
        assert_eq!(plan.caps, [-1]);
        assert_eq!(plan.overshoot_bps, 0);
    }

    #[test]
    fn four_second_peak_storage_is_bounded_at_real_minimum_window_frequency() {
        let now = Instant::now();
        let sid = TrackSid::from("TR_bound");
        let mut cache = LayerBitrateCache::default();
        for epoch in 0..100 {
            let at = now + Duration::from_millis(epoch * 500);
            let cost = [100_000 + epoch, 200_000 + epoch, 300_000 + epoch];
            assert_eq!(cache.resolve(&sid, observation(at, cost), [0; 3], at), cost);
            assert!(
                cache.0[&(sid.clone(), None)]
                    .windows
                    .iter()
                    .all(|window| window.len() <= MAX_COST_SAMPLES)
            );
        }
    }

    #[test]
    fn source_generation_partial_future_and_old_epochs_do_not_reuse_old_costs() {
        let now = Instant::now();
        let sid = TrackSid::from("TR_rebind");
        let mut cache = LayerBitrateCache::default();
        cache.resolve(&sid, observation(now, B), [0; 3], now);
        let partial = LayerBitrateObservation {
            generation: 2,
            dd_temporal_limit: None,
            dd_active_layers: None,
            completed_at: [None; 3],
            bitrates: [9_000_000; 3],
        };
        assert_eq!(
            cache.resolve(&sid, partial, [80_000, 200_000, 600_000], now),
            [80_000, 200_000, 600_000]
        );
        let future = LayerBitrateObservation {
            generation: 2,
            dd_temporal_limit: None,
            dd_active_layers: None,
            completed_at: [Some(now + Duration::from_secs(1)); 3],
            bitrates: [9_000_000; 3],
        };
        assert_eq!(
            cache.resolve(&sid, future, [80_000, 200_000, 600_000], now),
            [80_000, 200_000, 600_000]
        );
        let at = now + Duration::from_secs(2);
        let measured = LayerBitrateObservation {
            generation: 2,
            ..observation(at, [90_000, 220_000, 650_000])
        };
        assert_eq!(
            cache.resolve(&sid, measured, [9_000_000; 3], at),
            [90_000, 220_000, 650_000]
        );
        let older = LayerBitrateObservation {
            generation: 2,
            ..observation(now, [9_000_000; 3])
        };
        assert_eq!(
            cache.resolve(&sid, older, [9_000_000; 3], at),
            [90_000, 220_000, 650_000]
        );
    }

    #[test]
    fn large_capacity_does_not_wrap_into_negative_budget() {
        let p = plan(Some(u64::MAX), &[track(1, 2, B)], 1, true);
        assert_eq!(p.caps, [2]);
        assert_eq!(p.overshoot_bps, 0);
    }
    #[test]
    fn probes_pay_for_next_layer_including_reserved_audio_and_wire_cost() {
        let tracks = [track(1, 2, B)];
        let middle = plan(Some(800_000), &tracks, 1, true);
        assert_eq!(middle.caps, [1]);
        assert_eq!(next_probe_budget(&tracks, &middle), Some(1_960_000));
        let paused = plan(Some(100_000), &tracks, 1, true);
        assert_eq!(next_probe_budget(&tracks, &paused), Some(237_778));
        let full = plan(Some(3_000_000), &tracks, 1, true);
        assert_eq!(next_probe_budget(&tracks, &full), None);
    }
}
