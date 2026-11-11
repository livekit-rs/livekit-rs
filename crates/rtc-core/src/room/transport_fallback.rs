//! Production participant failure/loss -> shared ICE policy -> both native PCs.
use super::*;
use crate::media_api::{MediaCommand, PcRole};
use crate::transport_fallback::{IceConnection, IcePreference};
impl Room {
    #[track_caller]
    pub(super) fn apply_ice_preference(
        &mut self,
        sid: &ParticipantSid,
        remember: bool,
        now: Instant,
    ) {
        let Some(p) = self.participants.get(sid) else {
            return;
        };
        let preference = p.fallback.preference;
        let identity = p.info.identity.clone();
        let transports = p.transports.iter().map(|(id, _)| *id).collect::<Vec<_>>();
        if remember {
            // Fixed TransportManager.configureICE logs "setting ICE config" at info.
            tracing::info!(
                participant = %sid,
                %identity,
                ?preference,
                at = %std::panic::Location::caller(),
                "setting ICE config"
            );
            self.policy
                .transport_fallback
                .remember(&self.key, &identity, preference, now)
        }
        for transport in transports {
            self.media(MediaCommand::SetIcePreference {
                transport,
                preference,
            })
        }
    }
    pub(super) fn ice_path(
        &mut self,
        transport: TransportId,
        connection: IceConnection,
        first_connected: Option<Instant>,
    ) {
        if let Some(p) = self
            .participants
            .values_mut()
            .find(|p| p.has_transport(transport) && !p.is_disconnected())
        {
            // Go's HandleSignalSourceClose keeps the participant if either
            // ICE transport ever connected, even before DTLS completes.
            p.ice_connected_once |= first_connected.is_some();
            p.fallback.path(transport, connection, first_connected);
        }
    }
    pub(super) fn fallback_transport_failed(
        &mut self,
        sid: &ParticipantSid,
        transport: TransportId,
        now: Instant,
    ) {
        let changed = self
            .participants
            .get_mut(sid)
            .filter(|p| p.has_transport(transport) && !p.is_disconnected())
            .and_then(|p| {
                p.fallback.failed(
                    &self.policy.transport_fallback,
                    transport,
                    p.signal_connected,
                    p.capabilities.one_shot,
                    p.capabilities.ice_tcp,
                    now,
                )
            })
            .is_some();
        if changed {
            self.apply_ice_preference(sid, true, now)
        }
    }
    pub(super) fn fallback_tcp_timed_out(
        &mut self,
        sid: &ParticipantSid,
        transport: TransportId,
        now: Instant,
    ) {
        let changed = self
            .participants
            .get_mut(sid)
            .filter(|p| p.has_transport(transport) && !p.is_disconnected())
            .and_then(|p| {
                p.fallback.tcp_timed_out(
                    &self.policy.transport_fallback,
                    transport,
                    p.signal_connected,
                    p.capabilities.one_shot,
                    p.capabilities.ice_tcp,
                    now,
                )
            })
            .is_some();
        if changed {
            self.apply_ice_preference(sid, true, now)
        }
    }

    pub(super) fn receiver_loss(
        &mut self,
        transport: TransportId,
        track: &TrackSid,
        loss: u8,
        received_at: Instant,
    ) {
        let Some(sid) = self.participants.iter().find_map(|(sid, p)| {
            (!p.is_disconnected()
                && p.has_transport(transport)
                && p.subscriptions
                    .get(track)
                    .is_some_and(|s| s.bound && !s.closing))
            .then_some(sid.clone())
        }) else {
            return;
        };
        let (failed, changed, failing_transport) = {
            let p = self
                .participants
                .get_mut(&sid)
                .expect("resolved participant");
            let old = p.fallback.preference;
            let failed = p.fallback.receiver_loss(
                &self.policy.transport_fallback,
                loss,
                p.signal_connected,
                p.capabilities.one_shot,
                p.capabilities.ice_tcp,
                received_at,
            );
            let transport = if p.transport(PcRole::Single).is_some() {
                p.publish_transport()
            } else {
                p.transport(PcRole::Subscriber).unwrap_or(transport)
            };
            (failed, p.fallback.preference != old, transport)
        };
        if changed {
            self.apply_ice_preference(&sid, true, received_at)
        }
        if failed {
            self.transport_failed(&sid, failing_transport, received_at)
        }
    }
}
