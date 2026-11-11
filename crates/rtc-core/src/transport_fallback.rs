//! Fixed transportmanager.go ICE preference state and process-local TTL cache.
//! Protocol-neutral policy; only genuine signaling/network observations feed it.
use crate::ids::{Identity, RoomKey, TransportId};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum IcePreference {
    #[default]
    Any,
    Tcp,
    Tls,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum IceConnection {
    #[default]
    Unknown,
    Udp,
    Tcp,
    Turn,
}
#[derive(Debug, Default)]
struct Cache(Mutex<BTreeMap<(RoomKey, Identity), (Instant, IcePreference)>>);
#[derive(Clone, Debug)]
pub struct FallbackPolicy {
    pub allow_tcp: bool,
    pub rtt_threshold_ms: i64,
    pub allow_udp_unstable: bool,
    pub turns_enabled: bool,
    cache: Arc<Cache>,
}
impl Default for FallbackPolicy {
    fn default() -> Self {
        Self {
            allow_tcp: true,
            rtt_threshold_ms: 0,
            allow_udp_unstable: false,
            turns_enabled: false,
            cache: Arc::new(Cache::default()),
        }
    }
}
// Policy equality is configuration equality, independent of runtime cache state.
impl PartialEq for FallbackPolicy {
    fn eq(&self, other: &Self) -> bool {
        self.allow_tcp == other.allow_tcp
            && self.rtt_threshold_ms == other.rtt_threshold_ms
            && self.allow_udp_unstable == other.allow_udp_unstable
            && self.turns_enabled == other.turns_enabled
    }
}
impl FallbackPolicy {
    pub fn new(
        allow_tcp: bool,
        rtt_threshold_ms: i64,
        allow_udp_unstable: bool,
        turns_enabled: bool,
    ) -> Self {
        Self {
            allow_tcp,
            rtt_threshold_ms,
            allow_udp_unstable,
            turns_enabled,
            ..Self::default()
        }
    }
    pub fn cached(&self, room: &RoomKey, identity: &Identity, now: Instant) -> IcePreference {
        let mut cache = self.cache.0.lock().expect("ICE config cache poisoned");
        cache.retain(|_, (expires, _)| *expires > now);
        cache
            .get(&(room.clone(), identity.clone()))
            .map_or(IcePreference::Any, |(_, p)| *p)
    }
    pub fn remember(
        &self,
        room: &RoomKey,
        identity: &Identity,
        preference: IcePreference,
        now: Instant,
    ) {
        let mut cache = self.cache.0.lock().expect("ICE config cache poisoned");
        cache.retain(|_, (expires, _)| *expires > now);
        cache.insert(
            (room.clone(), identity.clone()),
            (now + Duration::from_secs(300), preference),
        );
    }
}
#[derive(Debug)]
pub struct TransportFallback {
    pub preference: IcePreference,
    paths: BTreeMap<TransportId, (IceConnection, Option<Instant>)>,
    pub last_signal: Instant,
    failed_transports: BTreeSet<TransportId>,
    signal_rtt: u32,
    udp_rtt: u32,
    failures: u32,
    last_failure: Option<Instant>,
    reconfigured: bool,
    loss_bits: u32,
    loss_max: u8,
    loss_at: Option<Instant>,
    loss_enabled: bool,
}
impl TransportFallback {
    pub fn new(preference: IcePreference, now: Instant) -> Self {
        Self {
            preference,
            paths: BTreeMap::new(),
            last_signal: now,
            failed_transports: BTreeSet::new(),
            signal_rtt: 0,
            udp_rtt: 0,
            failures: 0,
            last_failure: None,
            reconfigured: false,
            loss_bits: 0,
            loss_max: 0,
            loss_at: None,
            loss_enabled: preference == IcePreference::Any,
        }
    }
    pub fn resume(&mut self, preference: IcePreference, now: Instant) {
        self.preference = preference;
        self.failures = 0;
        self.last_failure = None;
        self.reconfigured = false;
        self.loss_bits = 0;
        self.failed_transports.clear();
        let _ = now;
        if preference != IcePreference::Any {
            self.loss_enabled = false;
        }
    }
    pub fn signal(&mut self, now: Instant) {
        self.last_signal = now;
    }
    pub fn signal_rtt(&mut self, rtt: u32) {
        self.signal_rtt = rtt;
    }
    pub fn media_rtt(&mut self, rtt: u32) {
        self.udp_rtt = if self.udp_rtt == 0 {
            rtt
        } else {
            (i64::from(self.udp_rtt) + (i64::from(rtt) - i64::from(self.udp_rtt)) / 2) as u32
        };
    }
    pub fn path(
        &mut self,
        transport: TransportId,
        connection: IceConnection,
        first_connected: Option<Instant>,
    ) {
        self.paths.insert(transport, (connection, first_connected));
    }
    fn fresh(&self, now: Instant) -> bool {
        now.checked_duration_since(self.last_signal)
            .is_some_and(|d| d < Duration::from_secs(15))
    }
    pub fn failed(
        &mut self,
        policy: &FallbackPolicy,
        transport: TransportId,
        signal_valid: bool,
        one_shot: bool,
        ice_tcp: bool,
        now: Instant,
    ) -> Option<IcePreference> {
        if !self.failed_transports.insert(transport) {
            return None;
        }
        let short = self
            .paths
            .get(&transport)
            .and_then(|(_, at)| *at)
            .is_some_and(|at| {
                now.checked_duration_since(at)
                    .is_some_and(|d| d < Duration::from_secs(90))
            });
        self.failed_inner(policy, short, signal_valid, one_shot, ice_tcp, now)
    }
    pub fn tcp_timed_out(
        &mut self,
        policy: &FallbackPolicy,
        transport: TransportId,
        signal_valid: bool,
        one_shot: bool,
        ice_tcp: bool,
        now: Instant,
    ) -> Option<IcePreference> {
        if !self.failed_transports.insert(transport) {
            return None;
        }
        self.failed_inner(policy, true, signal_valid, one_shot, ice_tcp, now)
    }

    /// Fixed HandleClientReconnect resets reconfiguration only for a short
    /// real PC selected by publisher/subscriber failure reason, before restart.
    pub fn client_reconnect(
        &mut self,
        policy: &FallbackPolicy,
        transport: TransportId,
        signal_valid: bool,
        one_shot: bool,
        ice_tcp: bool,
        now: Instant,
    ) -> Option<IcePreference> {
        let short = self
            .paths
            .get(&transport)
            .and_then(|(_, at)| *at)
            .is_some_and(|at| {
                now.checked_duration_since(at)
                    .is_some_and(|d| d < Duration::from_secs(90))
            });
        if !short {
            return None;
        }
        self.failures = 0;
        self.last_failure = None;
        self.reconfigured = false;
        self.loss_bits = 0;
        self.failed_inner(policy, true, signal_valid, one_shot, ice_tcp, now)
    }

    fn failed_inner(
        &mut self,
        policy: &FallbackPolicy,
        short: bool,
        signal_valid: bool,
        one_shot: bool,
        ice_tcp: bool,
        now: Instant,
    ) -> Option<IcePreference> {
        if !policy.allow_tcp || one_shot || self.reconfigured {
            return None;
        }
        if !signal_valid || !self.fresh(now) {
            self.failures = 0;
            self.last_failure = None;
            return None;
        }
        let lowest = self
            .paths
            .values()
            .map(|(kind, _)| *kind)
            .max()
            .unwrap_or_default();
        if !short {
            self.failures = self.failures.wrapping_add(1);
            let previous = self.last_failure.replace(now);
            if self.failures < 2
                || previous.is_none_or(|last| {
                    now.saturating_duration_since(last) > Duration::from_secs(60)
                })
            {
                return None;
            }
        }
        let next = match lowest {
            IceConnection::Udp if self.preference == IcePreference::Any => {
                if ice_tcp
                    && (policy.rtt_threshold_ms == 0
                        || i64::from(self.signal_rtt) < policy.rtt_threshold_ms)
                {
                    IcePreference::Tcp
                } else if policy.turns_enabled {
                    IcePreference::Tls
                } else {
                    IcePreference::Any
                }
            }
            IceConnection::Tcp if policy.turns_enabled => IcePreference::Tls,
            IceConnection::Tcp | IceConnection::Turn => self.preference,
            _ => IcePreference::Any,
        };
        if next == self.preference {
            return None;
        }
        self.preference = next;
        self.reconfigured = true;
        self.failures = 0;
        self.last_failure = None;
        self.loss_bits = 0;
        if next != IcePreference::Any {
            self.loss_enabled = false
        }
        Some(next)
    }
    /// Genuine RR fraction loss. Preserve MediaLossProxy's max aggregation and
    /// strict >1s callback interval; periodic averaged PeerStats are not a substitute.
    pub fn receiver_loss(
        &mut self,
        policy: &FallbackPolicy,
        loss: u8,
        signal_valid: bool,
        one_shot: bool,
        ice_tcp: bool,
        now: Instant,
    ) -> bool {
        if !self.loss_enabled {
            return false;
        }
        self.loss_max = self.loss_max.max(loss);
        if self
            .loss_at
            .is_some_and(|last| now.saturating_duration_since(last) <= Duration::from_secs(1))
        {
            return false;
        }
        let loss = std::mem::take(&mut self.loss_max);
        self.loss_at = Some(now);
        if policy.rtt_threshold_ms == 0 || !policy.allow_udp_unstable {
            return false;
        }
        self.loss_bits = self.loss_bits.wrapping_shl(1);
        if loss < 63 {
            return false;
        }
        self.loss_bits |= 1;
        if self.loss_bits.count_ones() < 20
            || self.udp_rtt == 0
            || self.signal_rtt >= ((self.udp_rtt as f32) * 1.3) as u32
            || i64::from(self.signal_rtt) >= policy.rtt_threshold_ms
            || !self.fresh(now)
        {
            return false;
        }
        self.loss_bits = 0;
        self.failed_inner(policy, true, signal_valid, one_shot, ice_tcp, now);
        // Go always invokes its participant failure callback after this condition,
        // even if AllowTCPFallback disabled prevents a preference change.
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn connected(now: Instant, connection: IceConnection) -> (TransportFallback, TransportId) {
        let mut state = TransportFallback::new(IcePreference::Any, now);
        let id = TransportId::next();
        state.path(id, connection, Some(now));
        (state, id)
    }
    #[test]
    fn source_fallback_short_failure_exact_rtt_capability_turn_and_signal_gates() {
        let now = Instant::now();
        for (threshold, rtt, ice_tcp, turns, allow, signal_valid, one_shot, age, expected) in [
            (
                0,
                999,
                true,
                false,
                true,
                true,
                false,
                0,
                Some(IcePreference::Tcp),
            ),
            (
                50,
                49,
                true,
                false,
                true,
                true,
                false,
                0,
                Some(IcePreference::Tcp),
            ),
            (50, 50, true, false, true, true, false, 0, None),
            (-1, 0, true, false, true, true, false, 0, None),
            (
                50,
                50,
                true,
                true,
                true,
                true,
                false,
                0,
                Some(IcePreference::Tls),
            ),
            (
                0,
                0,
                false,
                true,
                true,
                true,
                false,
                0,
                Some(IcePreference::Tls),
            ),
            (0, 0, false, false, true, true, false, 0, None),
            (0, 0, true, true, false, true, false, 0, None),
            (0, 0, true, true, true, false, false, 0, None),
            (0, 0, true, true, true, true, true, 0, None),
            (0, 0, true, true, true, true, false, 15, None),
        ] {
            let policy = FallbackPolicy::new(allow, threshold, false, turns);
            let (mut state, id) = connected(now, IceConnection::Udp);
            state.signal_rtt(rtt);
            assert_eq!(
                state.failed(
                    &policy,
                    id,
                    signal_valid,
                    one_shot,
                    ice_tcp,
                    now + Duration::from_secs(age)
                ),
                expected
            );
            assert_eq!(
                state.failed(
                    &policy,
                    id,
                    signal_valid,
                    one_shot,
                    ice_tcp,
                    now + Duration::from_secs(age)
                ),
                None,
                "one physical failure cannot be counted twice"
            );
        }
    }
    #[test]
    fn source_lowest_priority_of_both_pcs_and_long_connection_two_failure_window() {
        let now = Instant::now();
        let policy = FallbackPolicy::new(true, 0, false, true);
        let (mut state, udp) = connected(now, IceConnection::Udp);
        let tcp = TransportId::next();
        state.path(tcp, IceConnection::Tcp, Some(now));
        assert_eq!(
            state.failed(&policy, udp, true, false, true, now),
            Some(IcePreference::Tls),
            "both PC path types determine the next choice"
        );
        let (mut state, a) = connected(now, IceConnection::Udp);
        let b = TransportId::next();
        state.path(b, IceConnection::Udp, Some(now));
        let at = now + Duration::from_secs(90);
        state.signal(at);
        assert_eq!(
            state.failed(&policy, a, true, false, true, at),
            None,
            "90s exactly is not short"
        );
        assert_eq!(state.failed(&policy, a, true, false, true, at), None);
        assert_eq!(
            state.failed(&policy, b, true, false, true, at),
            Some(IcePreference::Tcp)
        );
    }
    #[test]
    fn source_cache_expires_at_five_minutes_no_touch_on_hit_and_room_project_identity_isolation() {
        let now = Instant::now();
        let policy = FallbackPolicy::default();
        let clone = policy.clone();
        let room = RoomKey::new(crate::ids::ProjectId::local(), "cache");
        let identity = Identity::from("one");
        policy.remember(&room, &identity, IcePreference::Tcp, now);
        assert_eq!(
            clone.cached(&room, &identity, now + Duration::from_secs(299)),
            IcePreference::Tcp
        );
        assert_eq!(
            clone.cached(&room, &identity, now + Duration::from_secs(300)),
            IcePreference::Any
        );
        assert_eq!(
            clone.cached(&room, &Identity::from("other"), now),
            IcePreference::Any
        );
        assert_eq!(
            clone.cached(
                &RoomKey::new(crate::ids::ProjectId::local(), "other"),
                &identity,
                now
            ),
            IcePreference::Any
        );
    }
    #[test]
    fn source_actual_rr_loss_max_aggregation_twenty_of_thirtytwo_rtt_and_resume_reset() {
        let now = Instant::now();
        let policy = FallbackPolicy::new(true, 100, true, false);
        let (mut state, _) = connected(now, IceConnection::Udp);
        state.signal_rtt(10);
        state.media_rtt(100);
        assert!(!state.receiver_loss(&policy, 62, true, false, true, now));
        for i in 1..20 {
            let at = now + Duration::from_millis(i * 1001);
            state.signal(at);
            assert!(!state.receiver_loss(&policy, 63, true, false, true, at));
        }
        let at = now + Duration::from_millis(20 * 1001);
        state.signal(at);
        assert!(state.receiver_loss(&policy, 63, true, false, true, at));
        assert_eq!(state.preference, IcePreference::Tcp);
        assert!(
            !state.receiver_loss(&policy, 255, true, false, true, at + Duration::from_secs(2)),
            "nondefault ICE preference disconnects MediaLossProxy callback"
        );
        state.resume(IcePreference::Tcp, at);
        assert!(!state.reconfigured);
        let (mut state, _) = connected(now, IceConnection::Udp);
        state.media_rtt(100);
        state.signal_rtt(130);
        for i in 0..32 {
            let at = now + Duration::from_millis(i * 1001);
            state.signal(at);
            assert!(!state.receiver_loss(&policy, 255, true, false, true, at));
        }
        assert_eq!(
            state.preference,
            IcePreference::Any,
            "strict relative RTT comparison excludes equality"
        );
    }
}
