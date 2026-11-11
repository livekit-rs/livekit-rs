//! Actual selected route, both-PC preference and source candidate consumers.
use crate::engine::MediaEngine;
use rtc_core::ids::TransportId;
use rtc_core::media_api::MediaEvent;
use rtc_core::transport_fallback::{IceConnection, IcePreference};

pub(crate) fn prefer_tcp(preference: IcePreference) -> bool {
    preference == IcePreference::Tcp
}
pub(crate) fn filter_sdp_candidates(sdp: &str, preference: IcePreference) -> String {
    if !prefer_tcp(preference) {
        return sdp.to_owned();
    }
    // Only candidate attributes change: all ICE credentials, media, BUNDLE,
    // codecs and application sections remain the original native description.
    sdp.split_inclusive('\n')
        .filter(|line| {
            let line = line.trim_end_matches(['\r', '\n']);
            !line.starts_with("a=candidate:")
                || line
                    .split_whitespace()
                    .nth(2)
                    .is_some_and(|protocol| protocol.eq_ignore_ascii_case("tcp"))
        })
        .collect()
}
impl MediaEngine {
    pub(crate) fn set_ice_preference(&mut self, transport: TransportId, preference: IcePreference) {
        if let Some(t) = self.transports.get_mut(&transport) {
            t.spec.ice_preference = preference;
        }
    }
    pub(crate) fn update_tcp_ice_timers(&mut self, now: std::time::Instant) {
        for t in self.transports.values_mut() {
            if t.remote_description_applied
                && t.ice_started_at.is_none()
                && t.rtc.ice_connection_state() == str0m::IceConnectionState::Checking
            {
                t.ice_started_at = Some(now);
                if prefer_tcp(t.spec.ice_preference) && t.signaling_rtt_ms < 1000 {
                    let millis = (u64::from(t.signaling_rtt_ms) * 8).clamp(5000, 12000);
                    t.tcp_ice_deadline = Some(now + std::time::Duration::from_millis(millis));
                }
            }
        }
    }
    pub(crate) fn observe_ice_path(
        &mut self,
        transport: TransportId,
        connected: bool,
        now: std::time::Instant,
    ) {
        let Some(t) = self.transports.get_mut(&transport) else {
            return;
        };
        if connected {
            t.ice_first_connected_at.get_or_insert(now);
        }
        let connection = t
            .rtc
            .selected_ice_transport()
            .map(|(protocol, relay)| {
                if relay {
                    IceConnection::Turn
                } else if protocol == str0m::net::Protocol::Tcp {
                    IceConnection::Tcp
                } else {
                    IceConnection::Udp
                }
            })
            .or_else(|| t.last_ice_path.map(|(kind, _)| kind));
        let Some(connection) = connection else { return };
        let path = (connection, t.ice_first_connected_at);
        if t.last_ice_path == Some(path) {
            return;
        }
        t.last_ice_path = Some(path);
        self.events.push(MediaEvent::IcePath {
            transport,
            connection,
            first_connected: path.1,
        });
    }
}

/// Fixed PCTransport uses Pion selected.Remote.LastReceived with10s until
/// disconnected and another5s until failure. This is NOT the generic native
/// STUN retransmission timeout, and is not inferred from periodic media stats.
const SOURCE_ICE_DISCONNECTED: std::time::Duration = std::time::Duration::from_secs(10);
pub(crate) const SOURCE_ICE_FAILED_TOTAL: std::time::Duration = std::time::Duration::from_secs(15);
// Pinned livekit/ice agent_config.go: defaultCheckInterval. The agent checks
// the strictly-greater-than timeout on contact, not at a separate deadline.
pub(crate) const SOURCE_ICE_CHECK_INTERVAL: std::time::Duration =
    std::time::Duration::from_millis(200);
impl MediaEngine {
    pub(crate) fn next_initial_ice_check(
        t: &crate::transport::Transport,
    ) -> Option<std::time::Instant> {
        if t.failed || t.recovery_pending.is_some() || t.ice_first_connected_at.is_some() {
            None
        } else {
            t.ice_next_check_at
        }
    }

    pub(crate) fn selected_ice_restart_pending(t: &crate::transport::Transport) -> bool {
        t.recovery_pending.is_some()
            || t.ice_restart_pending
            || t.ice_restart_requested
            || t.pending_restart_ufrag.is_some()
    }
    pub(crate) fn selected_ice_failure_deadline(
        t: &crate::transport::Transport,
    ) -> Option<std::time::Instant> {
        if t.failed || t.ice_first_connected_at.is_none() || Self::selected_ice_restart_pending(t) {
            return None;
        }
        t.selected_ice_last_receive
            .map(|last| last + SOURCE_ICE_FAILED_TOTAL + std::time::Duration::from_nanos(1))
    }
    pub(crate) fn update_selected_ice_liveness(&mut self, now: std::time::Instant) {
        for t in self.transports.values_mut() {
            if let Some(pending) = t.recovery_pending {
                if pending.sdp_applied
                    && t.rtc
                        .ice_last_selected_receive()
                        .is_some_and(|received| received >= pending.started)
                {
                    t.recovery_pending = None;
                }
            }
            if Self::selected_ice_restart_pending(t) {
                // A local offer changes credentials only on the real answer.
                // Do not refill its new epoch from the still-visible old pair.
                t.selected_ice_last_receive = None;
                t.disconnected_since = None;
                continue;
            }
            if t.failed || t.ice_first_connected_at.is_none() {
                continue;
            }
            if let Some(received) = t.rtc.ice_last_selected_receive() {
                t.selected_ice_last_receive = Some(
                    t.selected_ice_last_receive
                        .map_or(received, |old| old.max(received)),
                );
            }
            let Some(last) = t.selected_ice_last_receive else {
                continue;
            };
            if now.saturating_duration_since(last) > SOURCE_ICE_DISCONNECTED {
                // Retain the actual source boundary, rather than starting an
                // extra5s clock only after native's later retransmission expiry.
                t.disconnected_since = Some(last + SOURCE_ICE_DISCONNECTED);
            } else if matches!(
                t.rtc.ice_connection_state(),
                str0m::IceConnectionState::Connected | str0m::IceConnectionState::Completed
            ) {
                t.disconnected_since = None;
            }
        }
    }
}
