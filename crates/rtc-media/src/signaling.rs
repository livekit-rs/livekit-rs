//! Signaling side of the engine: client offers (publisher / single PC), server offers
//! (dual-PC subscriber), trickle candidates, single-PC section assignment, track
//! binding and data channels (signaling.md §3, §7; sfu-media.md §9, §10).

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Instant;

use rtc_core::ids::{ParticipantSid, TrackSid, TransportId};
use rtc_core::media_api::{DataKind, MediaEvent, PcRole, PublishedTrackSpec, RemoteMedia};
use rtc_core::model::{TrackKind, TrackSource};
use rtc_core::session::{CandidateInit, SdpType, SubscriptionError};
use str0m::Rtc;
use str0m::change::{SdpAnswer, SdpOffer};
use str0m::channel::{ChannelConfig, ChannelData, ChannelId, Reliability};
use str0m::media::{Direction, Media, MediaKind, Mid, Rids};
use str0m::{Candidate, CandidateKind};

use crate::codecs;
use crate::downtrack::DownTrack;
use crate::engine::MediaEngine;
use crate::sdp_munge;
use crate::slab::Slab;
use crate::transport::{
    ANSWER_TIMEOUT, ChannelEntry, LABEL_DATA_TRACK, LABEL_LOSSY, LABEL_RELIABLE, NEGOTIATE_LONG,
    NEGOTIATE_SHORT, NegState, RemoteInfo, Transport,
};
use crate::uptrack::UpTrack;

/// Remote candidates received before the first remote description are queued.
const MAX_QUEUED_CANDIDATES: usize = 64;

/// One audio/video section of a client offer (text-level info str0m does not expose).
struct OfferSection {
    mid: Mid,
    mid_str: String,
    sdp_registration: bool,
    primary_ssrcs: Option<Vec<u32>>,
    msid_track: Option<String>,
    rids: Vec<String>,
}

fn offer_sections(sdp: &str) -> Vec<OfferSection> {
    sdp_munge::split_sections(sdp)
        .1
        .into_iter()
        .filter_map(|s| {
            let kind = sdp_munge::section_kind(s)?;
            if kind != "audio" && kind != "video" {
                return None;
            }
            let mid_str = sdp_munge::section_mid(s)?.to_string();
            let (sources, uses_rids) = sdp_receiver_sources(s);
            Some(OfferSection {
                mid: Mid::from(mid_str.as_str()),
                mid_str,
                sdp_registration: receiver_registration_eligible(&sources, uses_rids),
                primary_ssrcs: (!uses_rids && sources.iter().any(|ssrc| *ssrc != 0))
                    .then_some(sources),
                msid_track: sdp_munge::section_msid(s)
                    .map(|(_, t)| t)
                    .filter(|t| !t.is_empty()),
                rids: sdp_munge::simulcast_send_rids(s),
            })
        })
        .collect()
}

// Copyright (c) 2026 The Pion community. Source-derived MIT code; see
// LICENSES/webrtc-pion-receiver-MIT.txt.
/// Source-derived Pion sdp.go trackDetailsFromSDP/getRids callback eligibility.
/// Repair SSRCs are removed even when FID/FEC-FR appears after their attributes.
#[cfg(test)]
fn sdp_registration_eligible(section: &str) -> bool {
    let (streams, uses_rids) = sdp_receiver_sources(section);
    receiver_registration_eligible(&streams, uses_rids)
}

fn receiver_registration_eligible(streams: &[u32], uses_rids: bool) -> bool {
    !uses_rids && streams.first().is_some_and(|ssrc| *ssrc != 0)
}

fn sdp_receiver_sources(section: &str) -> (Vec<u32>, bool) {
    let mut streams = Vec::<u32>::new();
    let mut repair = std::collections::HashSet::<u32>::new();
    let (mut stream_id, mut track_id) = ("", "");
    let mut has_rid = false;
    for line in section.lines().map(|v| v.trim_end_matches('\r')) {
        if let Some(value) = line.strip_prefix("a=ssrc-group:") {
            let parts: Vec<_> = value.split(' ').collect();
            if parts.len() == 3
                && matches!(parts[0], "FID" | "FEC-FR")
                && parts[1].parse::<u32>().is_ok()
                && let Ok(ssrc) = parts[2].parse::<u32>()
            {
                repair.insert(ssrc);
                streams.retain(|v| *v != ssrc);
            }
        } else if let Some(value) = line.strip_prefix("a=msid:") {
            let parts: Vec<_> = value.split(' ').collect();
            if parts.len() == 2 {
                stream_id = parts[0];
                track_id = parts[1];
            }
        } else if let Some(value) = line.strip_prefix("a=ssrc:") {
            let parts: Vec<_> = value.split(' ').collect();
            if let Ok(ssrc) = parts[0].parse::<u32>() {
                if repair.contains(&ssrc) {
                    continue;
                }
                if parts.len() == 3
                    && let Some(id) = parts[1].strip_prefix("msid:")
                {
                    stream_id = id;
                    track_id = parts[2];
                }
                if !streams.contains(&ssrc) {
                    streams.push(ssrc);
                }
            }
        } else if line.starts_with("a=rid:") {
            has_rid = true;
        }
    }
    // getRids includes every RID attribute; a simulcast line alone creates none.
    let uses_rids = has_rid && !stream_id.is_empty() && !track_id.is_empty();
    (streams, uses_rids)
}

/// Negotiated mime of a media: first remote PT (remote preference order) known to
/// the session codec config.
pub(crate) fn mime_of_media(rtc: &Rtc, m: &Media) -> &'static str {
    m.remote_pts()
        .iter()
        .find_map(|pt| rtc.codec_config().params().iter().find(|p| p.pt() == *pt))
        .map(|p| codecs::mime_of(p.spec().codec))
        .unwrap_or("")
}

fn rids_of(r: &Rids) -> Vec<String> {
    match r {
        Rids::Specific(v) => v.iter().map(|r| r.to_string()).collect(),
        _ => Vec::new(),
    }
}

fn media_kind(k: TrackKind) -> MediaKind {
    match k {
        TrackKind::Video => MediaKind::Video,
        _ => MediaKind::Audio,
    }
}

/// Match an announced publication to an offered section (participant.go:3829-3873):
/// exact cid → codec cid / sdp cid → first unclaimed pending track of the same kind.
fn match_expected<'a>(
    expected: &'a [PublishedTrackSpec],
    kind: TrackKind,
    msid_track: &str,
    claimed: &[TrackSid],
) -> Option<&'a PublishedTrackSpec> {
    let free = |e: &&PublishedTrackSpec| e.kind == kind && !claimed.contains(&e.sid);
    expected
        .iter()
        .filter(free)
        .find(|e| !msid_track.is_empty() && e.cid == msid_track)
        .or_else(|| {
            expected.iter().filter(free).find(|e| {
                !msid_track.is_empty()
                    && e.codecs
                        .iter()
                        .any(|c| c.cid == msid_track || c.sdp_cid == msid_track)
            })
        })
        .or_else(|| expected.iter().find(free))
}

/// `mid → track sid` for every mid of `tid` carrying a down track.
fn mid_to_track_id(
    down_by_mid: &std::collections::HashMap<(TransportId, Mid), usize>,
    downs: &Slab<DownTrack>,
    tid: TransportId,
) -> BTreeMap<String, String> {
    down_by_mid
        .iter()
        .filter(|((t, _), _)| *t == tid)
        .filter_map(|((_, mid), di)| {
            downs
                .get(*di)
                .map(|d| (mid.to_string(), d.spec.track_sid.to_string()))
        })
        .collect()
}

/// Emit candidates only after the local description. While STUN gathering
/// continues, retain the newest eligible candidate so the final flag is
/// carried by a real candidate even if every STUN probe fails.
fn flush_candidates(t: &mut Transport, events: &mut Vec<MediaEvent>) {
    if !t.local_description_sent || t.spec.one_shot {
        return;
    }
    let prefer_tcp = crate::ice_fallback::prefer_tcp(t.spec.ice_preference);
    let mut candidates = t
        .candidates
        .iter()
        .enumerate()
        .skip(t.candidate_cursor)
        .filter(|(_, candidate)| !prefer_tcp || candidate.proto() == str0m::net::Protocol::Tcp)
        .peekable();
    while let Some((index, candidate)) = candidates.next() {
        let last = candidates.peek().is_none();
        if last && t.srflx_pending {
            t.candidate_cursor = index;
            return;
        }
        events.push(local_candidate_event(t, candidate, last));
    }
    t.candidate_cursor = t.candidates.len();
}

fn local_candidate_event(t: &Transport, c: &Candidate, last: bool) -> MediaEvent {
    MediaEvent::LocalCandidate {
        participant: t.spec.participant.clone(),
        target: t.spec.role.target(),
        candidate: CandidateInit {
            candidate: c.to_sdp_string(),
            sdp_mid: t.last_local_first_mid.clone(),
            sdp_mline_index: Some(0),
            username_fragment: None,
        },
        last,
    }
}

/// Local ufrag changed (ICE restart either side): report it and re-trickle.
fn adopt_ufrag(t: &mut Transport, ufrag: &str, changes: &mut Vec<(TransportId, String)>) {
    if t.ufrag == ufrag {
        return;
    }
    t.ufrag = ufrag.to_string();
    t.candidates.retain_mut(|candidate| {
        if candidate.kind() == CandidateKind::ServerReflexive {
            t.rtc.direct_api().invalidate_candidate(candidate);
            return false;
        }
        candidate.set_ufrag(ufrag);
        true
    });
    t.candidate_cursor = 0;
    t.srflx_pending = t.gather_srflx;
    changes.push((t.spec.id, t.ufrag.clone()));
}

impl MediaEngine {
    /// Replace the UDP host choices for the new ICE credentials
    /// before this generation's trickle leaves the room.
    pub fn replace_restarted_udp_host_candidates(
        &mut self,
        tid: TransportId,
        candidates: Vec<crate::LocalCandidate>,
    ) {
        let Some(t) = self.transports.get_mut(&tid) else {
            return;
        };
        let is_udp_host = |candidate: &Candidate| {
            candidate.kind() == CandidateKind::Host
                && candidate.proto() == str0m::net::Protocol::Udp
        };
        t.candidates.retain(|candidate| {
            if is_udp_host(candidate)
                && !candidates
                    .iter()
                    .any(|new| new.addr == candidate.addr() && new.base == candidate.local())
            {
                t.rtc.direct_api().invalidate_candidate(candidate);
                return false;
            }
            true
        });
        for candidate in candidates {
            if t.candidates.iter().any(|existing| {
                is_udp_host(existing)
                    && existing.addr() == candidate.addr
                    && existing.local() == candidate.base
            }) {
                continue;
            }
            let Ok(candidate) = Candidate::builder()
                .udp()
                .mapped_host(candidate.addr, candidate.base)
                .build()
            else {
                continue;
            };
            if let Some(accepted) = t.rtc.add_local_candidate(candidate) {
                t.candidates.push(accepted.clone());
            }
        }
        self.events.retain(|event| {
            !matches!(
                event,
                MediaEvent::LocalCandidate { participant, target, candidate, .. }
                    if *participant == t.spec.participant
                    && *target == t.spec.role.target()
                    && candidate.candidate.split_ascii_whitespace().last() == Some(t.ufrag.as_str())
            )
        });
        t.candidate_cursor = 0;
        flush_candidates(t, &mut self.events);
        self.mark_dirty(tid);
    }

    /// The socket layer will replace the host candidates before an
    /// answer carrying the new one-shot ICE credentials can be published.
    pub fn enable_one_shot_host_refresh(&mut self, tid: TransportId) {
        if let Some(t) = self.transports.get_mut(&tid).filter(|t| t.spec.one_shot) {
            t.refresh_one_shot_host = true;
        }
    }

    /// Install the dedicated UDP candidates discovered when the socket layer
    /// adopts the restart credentials, before the room delivers the trickle.
    pub fn add_restarted_udp_candidates(
        &mut self,
        tid: TransportId,
        candidates: impl IntoIterator<Item = crate::LocalCandidate>,
    ) {
        let Some(t) = self.transports.get_mut(&tid) else {
            return;
        };
        let start = t.candidates.len();
        for candidate in candidates {
            let Ok(candidate) = Candidate::builder()
                .udp()
                .mapped_host(candidate.addr, candidate.base)
                .build()
            else {
                continue;
            };
            if let Some(accepted) = t.rtc.add_local_candidate(candidate) {
                t.candidates.push(accepted.clone());
            }
        }
        if start == t.candidates.len() {
            return;
        }
        if t.candidate_cursor == start
            && !t.srflx_pending
            && !crate::ice_fallback::prefer_tcp(t.spec.ice_preference)
        {
            // Restart host candidates may have been discovered after the
            // generation was flushed, but before its events leave the room.
            for event in self.events.iter_mut().rev() {
                if let MediaEvent::LocalCandidate {
                    participant,
                    target,
                    candidate,
                    last,
                } = event
                    && *participant == t.spec.participant
                    && *target == t.spec.role.target()
                    && candidate.candidate.split_ascii_whitespace().last() == Some(t.ufrag.as_str())
                {
                    *last = false;
                    break;
                }
            }
        }
        flush_candidates(t, &mut self.events);
        self.mark_dirty(tid);
    }

    /// Complete server-side STUN gathering on the current transport generation.
    /// The socket layer has already installed each candidate's physical route.
    pub fn complete_srflx(
        &mut self,
        tid: TransportId,
        ufrag: &str,
        candidates: impl IntoIterator<Item = (SocketAddr, SocketAddr)>,
        now: Instant,
    ) {
        let Some(t) = self
            .transports
            .get_mut(&tid)
            .filter(|t| t.ufrag == ufrag && t.srflx_pending)
        else {
            return;
        };
        for (mapped, base) in candidates {
            let Ok(candidate) = Candidate::builder()
                .udp()
                .server_reflexive(mapped, base)
                .related_address(base)
                .build()
            else {
                continue;
            };
            if let Some(accepted) = t.rtc.add_local_candidate(candidate) {
                t.candidates.push(accepted.clone());
            }
        }
        t.srflx_pending = false;
        flush_candidates(t, &mut self.events);
        self.complete_one_shot_answer(tid, ufrag, now);
        self.mark_dirty(tid);
    }

    pub fn complete_one_shot_answer(&mut self, tid: TransportId, ufrag: &str, now: Instant) {
        let waiting = self
            .transports
            .get_mut(&tid)
            .filter(|t| t.ufrag == ufrag && !t.srflx_pending)
            .and_then(|t| t.one_shot_answer_waiting.take());
        if let Some((id, sdp)) = waiting {
            // Regenerate the native answer with the newly installed sockets.
            self.handle_offer(tid, &sdp, id, now);
        }
    }

    // ------------------------------------------------------------------
    // remote descriptions
    // ------------------------------------------------------------------

    pub(crate) fn on_remote_description(
        &mut self,
        tid: TransportId,
        kind: SdpType,
        sdp: String,
        id: u32,
        now: Instant,
    ) {
        let Some(t) = self.transports.get(&tid) else {
            tracing::debug!(transport = %tid, "remote description for unknown transport");
            return;
        };
        match (kind, t.role()) {
            (SdpType::Offer, PcRole::Publisher | PcRole::Single) => {
                self.handle_offer(tid, &sdp, id, now)
            }
            (SdpType::Answer, PcRole::Subscriber) => self.handle_answer(tid, &sdp, id, now),
            (k, r) => {
                tracing::warn!(transport = %tid, kind = ?k, role = ?r, "unexpected remote description ignored")
            }
        }
    }

    fn handle_offer(&mut self, tid: TransportId, sdp: &str, id: u32, now: Instant) {
        let filtered = self
            .transports
            .get(&tid)
            .map(|t| crate::ice_fallback::filter_sdp_candidates(sdp, t.spec.ice_preference))
            .unwrap_or_else(|| sdp.to_owned());
        let sdp = filtered.as_str();
        let offer = match SdpOffer::from_sdp_string(sdp) {
            Ok(o) => o,
            Err(e) => {
                tracing::warn!(transport = %tid, error = %e, "unparseable client offer");
                self.reject_one_shot_offer(tid, id, e.to_string());
                return;
            }
        };
        // These public parsed fields are the same session/first-media ICE
        // credential projection as the native parser. Source-valid SDP has one
        // consistent pair across all sections; no reset is inferred from text
        // containing merely an ICE restart request or a rejected description.
        let offer_ice = offer
            .session
            .ice_creds()
            .or_else(|| offer.media_lines.iter().find_map(|m| m.ice_creds()))
            .map(|c| (c.ufrag, c.pass));
        let sections = offer_sections(sdp);
        if let Some(t) = self.transports.get_mut(&tid)
            && t.first_remote_offer_has_data.is_none()
        {
            // Input has passed the actual SDP parser; preserve the fixed source's
            // first-offer application-media test, independent of later offers.
            t.first_remote_offer_has_data = Some(
                sdp.lines()
                    .filter_map(|line| line.strip_prefix("m="))
                    .any(|line| {
                        line.split_whitespace()
                            .next()
                            .is_some_and(|kind| kind.eq_ignore_ascii_case("application"))
                    }),
            );
        }

        let answer = {
            let Some(t) = self.transports.get_mut(&tid) else {
                return;
            };
            let actual_restart = offer_ice
                .as_ref()
                .is_some_and(|ice| t.remote_offer_ice.as_ref().is_some_and(|old| old != ice));
            if !t.rtc.is_alive() || (t.failed && !actual_restart) {
                return;
            }
            if let Some(ice) = offer_ice.as_ref()
                && t.remote_offer_ice.as_ref().is_some_and(|old| old != ice)
            {
                t.consume_short_connection_reset();
            }
            match t.rtc.sdp_api().accept_offer(offer) {
                Ok(a) => {
                    if actual_restart {
                        if let Some(pending) = t.recovery_pending.as_mut() {
                            pending.sdp_applied = true;
                        }
                        t.failed = false;
                        t.disconnected_since = None;
                        t.selected_ice_last_receive = None;
                    }
                    t.remote_offer_ice = offer_ice;
                    a
                }
                Err(e) => {
                    tracing::warn!(transport = %tid, error = %e, "client offer rejected");
                    self.reject_one_shot_offer(tid, id, e.to_string());
                    self.mark_dirty(tid);
                    return;
                }
            }
        };
        let mut text = answer.to_sdp_string();
        let restart = {
            let t = self.transports.get_mut(&tid).expect("transport exists");
            let ufrag = sdp_munge::ice_ufrag(&text);
            let changed = ufrag.is_some_and(|ufrag| ufrag != t.ufrag);
            if t.gather_srflx
                && let Some(ufrag) = ufrag
            {
                adopt_ufrag(t, ufrag, &mut self.ufrag_changes);
            }
            changed
        };
        self.drive_now(tid, now);
        self.apply_queued_candidates(tid, now);

        let role = self.transports[&tid].role();
        let red_answer_mids: Vec<_> = sdp_munge::split_sections(&text)
            .1
            .into_iter()
            .filter(|section| {
                sdp_munge::section_kind(section) == Some("audio")
                    && section.lines().any(|line| {
                        line.strip_prefix("a=rtpmap:")
                            .and_then(|value| value.split_once(' '))
                            .is_some_and(|(_, encoding)| {
                                encoding.to_ascii_lowercase().starts_with("red/")
                            })
                    })
            })
            .filter_map(sdp_munge::section_mid)
            .map(Mid::from)
            .collect();

        // ---- remote (published) media in this offer ----
        let mut added = Vec::new();
        let mut not_receiving = Vec::new();
        {
            let t = self.transports.get_mut(&tid).expect("transport exists");
            let participant = t.spec.participant.clone();
            let mut send_sections = Vec::new();
            let mut receive_epoch_resets = Vec::new();
            let mut claimed_audio = Vec::new();
            for s in &sections {
                let Some(m) = t.rtc.media(s.mid) else {
                    continue;
                };
                let receiving = m.direction().is_receiving() && !m.disabled();
                if receiving {
                    let msid_track_id = s
                        .msid_track
                        .clone()
                        .unwrap_or_else(|| m.track_id().to_string());
                    let prefers_audio_red = red_answer_mids.contains(&s.mid)
                        && if let Some(previous) = t
                            .remote_info
                            .get(&s.mid)
                            .filter(|info| info.msid_track_id == msid_track_id)
                        {
                            previous.prefers_audio_red
                        } else {
                            match_expected(
                                &t.expected,
                                TrackKind::Audio,
                                &msid_track_id,
                                &claimed_audio,
                            )
                            .is_some_and(|spec| {
                                claimed_audio.push(spec.sid.clone());
                                red_enabled(spec)
                            })
                        };
                    let info = RemoteInfo {
                        primary_ssrcs: s.primary_ssrcs.clone(),
                        msid_track_id,
                        prefers_audio_red,
                        rids: if s.rids.is_empty() {
                            rids_of(m.rids_rx())
                        } else {
                            s.rids.clone()
                        },
                    };
                    if t.remote_mids.insert(s.mid) {
                        added.push(RemoteMedia {
                            registration: if s.sdp_registration {
                                rtc_core::media_api::PublicationRegistration::SdpReceiver
                            } else {
                                rtc_core::media_api::PublicationRegistration::Pending
                            },
                            participant: participant.clone(),
                            transport: tid,
                            mid: s.mid_str.clone(),
                            kind: codecs::track_kind(m.kind()),
                            msid_track_id: info.msid_track_id.clone(),
                            rids: info.rids.clone(),
                            mime_type: if prefers_audio_red {
                                "audio/red"
                            } else {
                                mime_of_media(&t.rtc, m)
                            }
                            .to_string(),
                        });
                    }
                    if t.remote_info
                        .get(&s.mid)
                        .map(|previous| previous.msid_track_id.as_str())
                        != Some(info.msid_track_id.as_str())
                    {
                        receive_epoch_resets.push(s.mid);
                    }
                    t.remote_info.insert(s.mid, info);
                } else {
                    not_receiving.push(s.mid);
                }
                // A native one-shot client can publish and receive on the same
                // offered m-line. Keep ordinary SDK section allocation unchanged.
                let send_slot = m.direction() == Direction::SendOnly
                    || (t.spec.one_shot && m.direction() == Direction::SendRecv);
                if send_slot && !m.disabled() {
                    send_sections.push((s.mid, m.kind()));
                }
            }
            for mid in receive_epoch_resets {
                t.rtc.direct_api().restart_receive_traffic(mid);
            }
            if role == PcRole::Single {
                t.send_sections = send_sections;
            }
        }

        // ---- single PC: assign waiting down tracks to free send sections ----
        let mut bound_now = Vec::new();
        if role == PcRole::Single {
            let Self {
                transports,
                downs,
                down_by_mid,
                ..
            } = self;
            let t = transports.get_mut(&tid).expect("transport exists");
            let mut free: Vec<(Mid, MediaKind)> = t
                .send_sections
                .iter()
                .filter(|(m, _)| !t.used_mids.contains(m) && !t.retired.contains(m))
                .copied()
                .collect();
            let waiting: Vec<usize> = t.pending_single.drain(..).collect();
            for di in waiting {
                let Some(d) = downs.get_mut(di) else { continue };
                let kind = media_kind(d.kind);
                match free.iter().position(|(_, k)| *k == kind) {
                    Some(pos) => {
                        let (mid, _) = free.remove(pos);
                        d.mid = Some(mid);
                        d.negotiated = true;
                        down_by_mid.insert((tid, mid), di);
                        t.used_mids.insert(mid);
                        t.assigned.push((mid, di));
                        bound_now.push(di);
                    }
                    None => t.pending_single.push_back(di),
                }
            }
            // What the client added in response to earlier requests is consumed.
            t.requested = (0, 0);
            t.requirement_dirty = true;
        }

        // ---- answer munging ----
        let offered_sections = sdp_munge::split_sections(sdp).1;
        {
            let Self {
                transports,
                downs,
                ups,
                up_by_mid,
                ..
            } = self;
            let t = transports.get_mut(&tid).expect("transport exists");
            let mut claimed: Vec<TrackSid> = Vec::new();
            text = sdp_munge::map_sections(&text, |mid_str, sec| {
                let mid = Mid::from(mid_str?);
                let mut out: Option<String> = None;
                let receiving = matches!(
                    sdp_munge::section_direction(sec),
                    Some("recvonly") | Some("sendrecv")
                );
                if sdp_munge::section_kind(sec) == Some("audio") && receiving {
                    let bound = up_by_mid
                        .get(&(tid, mid))
                        .and_then(|ui| ups.get(*ui))
                        .map(|u| (u.spec.stereo, u.spec.disable_dtx));
                    let opts = bound.or_else(|| {
                        let info = t.remote_info.get(&mid)?;
                        let e = match_expected(
                            &t.expected,
                            TrackKind::Audio,
                            &info.msid_track_id,
                            &claimed,
                        )?;
                        claimed.push(e.sid.clone());
                        Some((e.stereo, e.disable_dtx))
                    });
                    if let Some((stereo, no_dtx)) = opts {
                        out = Some(sdp_munge::munge_opus_answer(sec, stereo, no_dtx));
                    }
                    if t.remote_info
                        .get(&mid)
                        .is_some_and(|info| info.prefers_audio_red)
                    {
                        out = Some(sdp_munge::prioritize_publisher_codec(
                            out.as_deref().unwrap_or(sec),
                            "audio/red",
                        ));
                    }
                }
                if sdp_munge::section_kind(sec) == Some("video")
                    && receiving
                    && let Some(offered) = offered_sections
                        .iter()
                        .copied()
                        .find(|section| sdp_munge::section_mid(section) == mid_str)
                {
                    let primary = up_by_mid
                        .get(&(tid, mid))
                        .and_then(|ui| ups.get(*ui))
                        .map(|up| up.mime.as_str())
                        .or_else(|| {
                            let info = t.remote_info.get(&mid)?;
                            let expected = match_expected(
                                &t.expected,
                                TrackKind::Video,
                                &info.msid_track_id,
                                &[],
                            )?;
                            expected
                                .codecs
                                .iter()
                                .find(|codec| {
                                    codec.cid == info.msid_track_id
                                        || codec.sdp_cid == info.msid_track_id
                                })
                                .or_else(|| expected.codecs.first())
                                .map(|codec| codec.mime_type.as_str())
                        });
                    let ordered = primary.map_or_else(
                        || sec.to_string(),
                        |mime| sdp_munge::prioritize_publisher_codec(sec, mime),
                    );
                    let hinted = sdp_munge::preserve_video_bitrate_hints(&ordered, offered);
                    if let Some(hints) = sdp_munge::capture_video_bitrate_hints(&hinted) {
                        t.publisher_bitrate_hints.insert(mid, hints);
                    } else {
                        t.publisher_bitrate_hints.remove(&mid);
                    }
                    out = Some(hinted);
                } else if sdp_munge::section_kind(sec) == Some("video") {
                    match sdp_munge::section_direction(sec) {
                        Some("inactive") if sec.split_whitespace().nth(1) != Some("0") => {
                            if let Some(previous) = t.publisher_bitrate_hints.get(&mid) {
                                out = Some(sdp_munge::restore_inactive_video_bitrate_hints(
                                    sec, previous,
                                ));
                            }
                        }
                        _ => {
                            // A rejected or send-only section no longer owns
                            // the old receiving transceiver's encoder hints.
                            t.publisher_bitrate_hints.remove(&mid);
                        }
                    }
                }
                if role == PcRole::Single {
                    let cur = out.as_deref().unwrap_or(sec);
                    let assigned = t
                        .assigned
                        .iter()
                        .find(|(m, _)| *m == mid)
                        .and_then(|(_, di)| downs.get(*di));
                    if let Some(d) = assigned {
                        out = Some(sdp_munge::rewrite_msid(
                            cur,
                            &d.spec.stream_id,
                            d.spec.track_sid.as_str(),
                        ));
                    } else if sdp_munge::section_direction(cur) == Some("sendonly") {
                        out = Some(if t.retired.contains(&mid) {
                            sdp_munge::make_inactive(cur)
                        } else {
                            sdp_munge::unassigned_send_section(cur)
                        });
                    }
                }
                out
            });
        }

        // ---- RED policy for new audio down tracks ----
        for &di in &bound_now {
            self.configure_red(di, now);
        }

        // ---- events ----
        for m in added {
            self.events.push(MediaEvent::RemoteMediaAdded(m));
        }
        for mid in not_receiving {
            self.report_remote_stopped(tid, mid);
        }
        let mid_map = if role == PcRole::Single {
            mid_to_track_id(&self.down_by_mid, &self.downs, tid)
        } else {
            BTreeMap::new()
        };
        {
            let Self {
                transports,
                events,
                ufrag_changes,
                ..
            } = self;
            let t = transports.get_mut(&tid).expect("transport exists");
            let first_answer = !t.local_description_sent;
            if !t.gather_srflx
                && let Some(ufrag) = sdp_munge::ice_ufrag(&text)
            {
                adopt_ufrag(t, ufrag, ufrag_changes);
            }
            t.last_local_first_mid = sdp_munge::mids(&text).first().map(|(m, _)| m.clone());
            if t.spec.one_shot
                && !t.defer_one_shot_answer
                && (t.srflx_pending || (t.refresh_one_shot_host && restart))
            {
                t.one_shot_answer_waiting = Some((id, sdp.to_owned()));
            } else if !t.defer_one_shot_answer {
                let mut sdp =
                    crate::ice_fallback::filter_sdp_candidates(&text, t.spec.ice_preference);
                if role == PcRole::Single && (first_answer || t.spec.one_shot) {
                    sdp = sdp_munge::pair_inline_ice_components(&sdp);
                }
                events.push(MediaEvent::LocalDescription {
                    participant: t.spec.participant.clone(),
                    transport: tid,
                    target: t.spec.role.target(),
                    kind: SdpType::Answer,
                    sdp,
                    trickle_only: !t.spec.one_shot
                        && if first_answer {
                            role == PcRole::Publisher
                        } else {
                            restart
                        },
                    id,
                    mid_to_track_id: mid_map,
                });
            }
            t.local_description_sent = true;
            flush_candidates(t, events);
        }
        for di in bound_now {
            self.emit_bound(di, now);
        }
        self.mark_dirty(tid);
    }

    fn handle_answer(&mut self, tid: TransportId, sdp: &str, id: u32, now: Instant) {
        let filtered = self
            .transports
            .get(&tid)
            .map(|t| crate::ice_fallback::filter_sdp_candidates(sdp, t.spec.ice_preference))
            .unwrap_or_else(|| sdp.to_owned());
        let sdp = filtered.as_str();
        let answer = match SdpAnswer::from_sdp_string(sdp) {
            Ok(a) => a,
            Err(e) => {
                tracing::warn!(transport = %tid, error = %e, "unparseable subscriber answer");
                return;
            }
        };
        let prev = {
            let Self {
                transports,
                ufrag_changes,
                ..
            } = self;
            let Some(t) = transports.get_mut(&tid) else {
                return;
            };
            let Some(pending) = t.pending_offer.take() else {
                tracing::debug!(transport = %tid, "answer without outstanding offer ignored");
                return;
            };
            if id != 0 && id != t.offer_seq {
                tracing::debug!(transport = %tid, got = id, expected = t.offer_seq, "answer id mismatch");
            }
            let prev = t.neg;
            t.neg = NegState::Idle;
            t.answer_deadline = None;
            if let Err(e) = t.rtc.sdp_api().accept_answer(pending, answer) {
                tracing::warn!(transport = %tid, error = %e, "subscriber answer rejected");
                t.pending_restart_ufrag = None;
                (prev, false)
            } else {
                if let Some(u) = t.pending_restart_ufrag.take() {
                    if let Some(pending) = t.recovery_pending.as_mut() {
                        pending.sdp_applied = true;
                    }
                    t.failed = false;
                    t.disconnected_since = None;
                    adopt_ufrag(t, &u, ufrag_changes);
                }
                (prev, true)
            }
        };
        let (prev, accepted) = prev;
        self.drive_now(tid, now);
        if !accepted {
            // Roll the offered m-lines back so the next offer adds them again.
            let offered: Vec<Mid> = self
                .transports
                .get_mut(&tid)
                .map(|t| std::mem::take(&mut t.offered))
                .unwrap_or_default();
            for mid in offered {
                if let Some(di) = self.down_by_mid.remove(&(tid, mid)) {
                    if let Some(d) = self.downs.get_mut(di) {
                        d.mid = None;
                        d.negotiated = false;
                    }
                    if let Some(t) = self.transports.get_mut(&tid) {
                        t.pending_adds.push(di);
                    }
                }
            }
            self.negotiate(tid, false, now);
            return;
        }
        self.apply_queued_candidates(tid, now);

        // Down tracks of the answered offer are now negotiated.
        let offered: Vec<Mid> = self
            .transports
            .get_mut(&tid)
            .map(|t| std::mem::take(&mut t.offered))
            .unwrap_or_default();
        let mut bound = Vec::new();
        let mut failed = Vec::new();
        for mid in offered {
            let Some(&di) = self.down_by_mid.get(&(tid, mid)) else {
                continue;
            };
            let ok = self
                .transports
                .get(&tid)
                .and_then(|t| t.rtc.media(mid))
                .is_some_and(|m| !m.disabled());
            if ok {
                if let Some(d) = self.downs.get_mut(di) {
                    d.negotiated = true;
                }
                bound.push(di);
            } else {
                failed.push(di);
            }
        }
        for &di in &bound {
            self.configure_red(di, now);
            self.emit_bound(di, now);
        }
        for di in failed {
            if let Some(d) = self.downs.get_mut(di)
                && !d.codec_failed
            {
                d.codec_failed = true;
                self.events.push(MediaEvent::SubscriptionFailed {
                    subscriber: d.spec.subscriber.clone(),
                    track_sid: d.spec.track_sid.clone(),
                    error: SubscriptionError::CodecUnsupported,
                });
            }
        }
        if let Some(t) = self.transports.get_mut(&tid) {
            flush_candidates(t, &mut self.events);
        }
        self.mark_dirty(tid);

        let more = self.transports.get(&tid).is_some_and(|t| {
            !t.pending_adds.is_empty() || !t.pending_stops.is_empty() || t.ice_restart_pending
        });
        if prev == NegState::Retry {
            self.create_offer(tid, now);
        } else if more {
            self.negotiate(tid, false, now);
        }
    }

    fn emit_bound(&mut self, di: usize, now: Instant) {
        let Some(d) = self.downs.get_mut(di) else {
            return;
        };
        let (Some(mid), true) = (d.mid, d.negotiated) else {
            return;
        };
        if d.bound_emitted {
            return;
        }
        if let Some(transport) = d.transport.and_then(|tid| self.transports.get_mut(&tid))
            && let Some(stream) = transport.rtc.direct_api().stream_tx_by_mid(mid, None)
        {
            // Fixed Go limits retransmission history by packet count. Apply the
            // policy at bind so it also covers original VP9/AV1 packet writers.
            let size = if d.is_video() {
                self.cfg.rtc.packet_buffer_size_video
            } else {
                self.cfg.rtc.packet_buffer_size_audio
            };
            stream.set_rtx_cache(size.max(1), std::time::Duration::MAX, None);
        }
        d.bound_emitted = true;
        let up = d.up;
        self.events.push(MediaEvent::SubscriptionBound {
            subscriber: d.spec.subscriber.clone(),
            track_sid: d.spec.track_sid.clone(),
            mid: mid.to_string(),
        });
        self.bind_receiver_report_source(di);
        self.update_dynacast(up, now);
    }

    /// Keep negotiated RED available for a RED source. Active encoding of
    /// plain Opus also requires the publication to permit RED.
    pub(crate) fn configure_red(&mut self, di: usize, now: Instant) {
        let active_red = self.cfg.audio.active_red_encoding;
        let Some(d) = self.downs.get(di) else { return };
        if d.is_video() || d.red_disabled {
            return;
        }
        let publisher_red = self.ups.get(d.up).is_some_and(|up| {
            red_enabled(&up.spec) && (active_red || up.mime.eq_ignore_ascii_case("audio/red"))
        });
        if !d.encrypted && publisher_red {
            return;
        }
        let Some(d) = self.downs.get_mut(di) else {
            return;
        };
        let (Some(tid), Some(mid)) = (d.transport, d.mid) else {
            return;
        };
        d.red_disabled = true;
        if let Some(t) = self.transports.get_mut(&tid) {
            t.rtc.direct_api().set_red_send(mid, false);
        }
        self.drive_now(tid, now);
    }

    /// Drain one transport right after a mutation (events are queued, not handled).
    pub(crate) fn drive_now(&mut self, tid: TransportId, now: Instant) {
        let Self {
            transports,
            out,
            io,
            queue,
            ..
        } = self;
        if let Some(t) = transports.get_mut(&tid) {
            t.drive(now, &**out, io, queue);
        }
    }

    fn apply_queued_candidates(&mut self, tid: TransportId, now: Instant) {
        let queued = match self.transports.get_mut(&tid) {
            Some(t) => {
                t.remote_description_applied = true;
                if t.ice_first_connected_at.is_none() && t.ice_checking_since.is_none() {
                    t.ice_checking_since = Some(now);
                    t.ice_next_check_at =
                        Some(now + crate::ice_fallback::SOURCE_ICE_CHECK_INTERVAL);
                }
                std::mem::take(&mut t.queued_remote_candidates)
            }
            None => return,
        };
        for c in queued {
            if let Some(t) = self.transports.get_mut(&tid) {
                t.rtc.add_remote_candidate(c);
            }
            self.drive_now(tid, now);
        }
    }

    // ------------------------------------------------------------------
    // server offers (dual-PC subscriber)
    // ------------------------------------------------------------------

    /// `Negotiate(force)`: debounced 10 ms (150 ms if the last negotiation was within
    /// 150 ms); `force` offers now.
    pub(crate) fn negotiate(&mut self, tid: TransportId, force: bool, now: Instant) {
        let Some(t) = self.transports.get_mut(&tid) else {
            return;
        };
        if t.role() != PcRole::Subscriber
            || !t.rtc.is_alive()
            || (t.failed && !t.ice_restart_requested)
        {
            return;
        }
        if force {
            t.debounce_at = None;
            self.create_offer(tid, now);
            return;
        }
        if t.debounce_at.is_none() {
            let recent = t
                .last_negotiation
                .is_some_and(|l| now.saturating_duration_since(l) <= NEGOTIATE_LONG);
            t.debounce_at = Some(
                now + if recent {
                    NEGOTIATE_LONG
                } else {
                    NEGOTIATE_SHORT
                },
            );
        }
    }

    /// `createAndSendOffer` with the None/Remote/Retry serialization.
    pub(crate) fn create_offer(&mut self, tid: TransportId, now: Instant) {
        let Self {
            transports,
            downs,
            ups,
            down_by_mid,
            events,
            dirty,
            ..
        } = self;
        let Some(t) = transports.get_mut(&tid) else {
            return;
        };
        if t.role() != PcRole::Subscriber
            || !t.rtc.is_alive()
            || (t.failed && !t.ice_restart_requested)
        {
            return;
        }
        // No local negotiation has started: Go GatheringStateNew skips the
        // request, preserving the armed reset for a later actual restart.
        if t.ice_restart_requested && !t.local_host_gathering_complete {
            t.ice_restart_pending = false;
            t.ice_restart_requested = false;
            return;
        }
        // Go doICERestart consumes reset after gathering, before inspecting
        // negotiation/remote-description state. Native local candidates are a
        // completed input to create_transport, not an asynchronous gather task.
        if t.ice_restart_requested && t.local_host_gathering_complete {
            t.ice_restart_requested = false;
            t.consume_short_connection_reset();
            if !t.remote_description_applied
                && t.neg != NegState::Idle
                && let Some(sdp) = t.last_local_offer_sdp.clone()
            {
                t.neg = NegState::Retry;
                t.offer_seq = t.offer_seq.wrapping_add(1).max(1);
                events.push(MediaEvent::LocalDescription {
                    participant: t.spec.participant.clone(),
                    transport: tid,
                    target: t.spec.role.target(),
                    kind: SdpType::Offer,
                    trickle_only: !t.spec.one_shot,
                    sdp: crate::ice_fallback::filter_sdp_candidates(&sdp, t.spec.ice_preference),
                    id: t.offer_seq,
                    mid_to_track_id: mid_to_track_id(down_by_mid, downs, tid),
                });
                // Keep the original pending offer and answer deadline. The
                // requested credential restart occurs after its real answer.
                return;
            }
        }
        match t.neg {
            NegState::Remote => {
                t.neg = NegState::Retry;
                return;
            }
            NegState::Retry => return,
            NegState::Idle => {}
        }

        // Stops apply to negotiated, still active m-lines only.
        let stops: Vec<Mid> = t
            .pending_stops
            .drain(..)
            .filter(|m| t.rtc.media(*m).is_some_and(|me| !me.disabled()))
            .collect();
        let adds: Vec<usize> = std::mem::take(&mut t.pending_adds);

        let mut api = t.rtc.sdp_api();
        if t.spec.server_data_channels && !t.channels_created {
            api.add_channel_with_config(ChannelConfig {
                label: LABEL_RELIABLE.to_string(),
                ordered: true,
                reliability: Reliability::Reliable,
                ..Default::default()
            });
            api.add_channel_with_config(ChannelConfig {
                label: LABEL_LOSSY.to_string(),
                ordered: false,
                reliability: Reliability::MaxRetransmits { retransmits: 0 },
                ..Default::default()
            });
            if t.spec.enable_data_tracks {
                api.add_channel_with_config(ChannelConfig {
                    label: LABEL_DATA_TRACK.to_string(),
                    ordered: false,
                    reliability: Reliability::MaxRetransmits { retransmits: 0 },
                    ..Default::default()
                });
            }
            t.channels_created = true;
        }
        let mut offered = Vec::new();
        for di in adds {
            let Some(d) = downs.get_mut(di) else { continue };
            if d.mid.is_some() || d.transport != Some(tid) {
                continue;
            }
            let mid = api.add_media(
                media_kind(d.kind),
                Direction::SendOnly,
                Some(d.spec.stream_id.clone()),
                Some(d.spec.track_sid.to_string()),
                None,
            );
            d.mid = Some(mid);
            d.negotiated = false;
            down_by_mid.insert((tid, mid), di);
            offered.push(mid);
        }
        for m in &stops {
            api.stop_media(*m);
        }
        let mut restart_ufrag = None;
        if t.ice_restart_pending {
            t.ice_restart_pending = false;
            restart_ufrag = Some(api.ice_restart(true).ufrag);
            t.selected_ice_last_receive = None;
            t.disconnected_since = None;
        }
        if !api.has_changes() {
            return;
        }
        let Some((offer, pending)) = api.apply() else {
            // Applied without negotiation (e.g. extra data channels).
            if !dirty.contains(&tid) {
                dirty.push(tid);
            }
            return;
        };

        let sdp = sdp_munge::map_sections(&offer.to_sdp_string(), |mid_str, sec| {
            let mid = Mid::from(mid_str?);
            let d = downs.get(*down_by_mid.get(&(tid, mid))?)?;
            let stereo = !d.is_video() && ups.get(d.up).is_some_and(|u| u.spec.stereo);
            stereo.then(|| sdp_munge::add_opus_sprop_stereo(sec))
        });
        let trickle_only =
            !t.spec.one_shot && (!t.local_description_sent || restart_ufrag.is_some());

        t.local_host_gathering_complete = true;
        t.last_local_offer_sdp = Some(sdp.clone());
        t.pending_offer = Some(pending);
        t.neg = NegState::Remote;
        t.offer_seq = t.offer_seq.wrapping_add(1).max(1);
        t.answer_deadline = Some(now + ANSWER_TIMEOUT);
        t.last_negotiation = Some(now);
        t.offered = offered;
        if restart_ufrag.is_some() {
            t.pending_restart_ufrag = restart_ufrag;
        }
        t.last_local_first_mid = sdp_munge::mids(&sdp).first().map(|(m, _)| m.clone());
        events.push(MediaEvent::LocalDescription {
            participant: t.spec.participant.clone(),
            transport: tid,
            target: t.spec.role.target(),
            kind: SdpType::Offer,
            trickle_only,
            sdp: crate::ice_fallback::filter_sdp_candidates(&sdp, t.spec.ice_preference),
            id: t.offer_seq,
            mid_to_track_id: mid_to_track_id(down_by_mid, downs, tid),
        });
        t.local_description_sent = true;
        flush_candidates(t, events);
        if !dirty.contains(&tid) {
            dirty.push(tid);
        }
    }

    // ------------------------------------------------------------------
    // candidates
    // ------------------------------------------------------------------

    pub(crate) fn on_remote_candidate(&mut self, tid: TransportId, init: CandidateInit) {
        if self
            .transports
            .get(&tid)
            .is_some_and(|t| crate::ice_fallback::prefer_tcp(t.spec.ice_preference))
            && !init.candidate.to_lowercase().contains("tcp")
        {
            return;
        }
        let raw = init.candidate.trim();
        if raw.is_empty() {
            // End of candidates: nothing to do (str0m never ends trickle).
            return;
        }
        let raw = raw.strip_prefix("a=").unwrap_or(raw);
        let line = if raw.starts_with("candidate:") {
            raw.to_string()
        } else {
            format!("candidate:{raw}")
        };
        let mut c = match Candidate::from_sdp_string(&line) {
            Ok(c) => c,
            Err(e) => {
                tracing::debug!(transport = %tid, error = %e, "unparseable remote candidate");
                return;
            }
        };
        if c.ufrag().is_none()
            && let Some(uf) = init.username_fragment.as_deref().filter(|u| !u.is_empty())
        {
            c.set_ufrag(uf);
        }
        let Some(t) = self.transports.get_mut(&tid) else {
            return;
        };
        if t.remote_description_applied {
            t.rtc.add_remote_candidate(c);
            self.mark_dirty(tid);
        } else if t.queued_remote_candidates.len() < MAX_QUEUED_CANDIDATES {
            t.queued_remote_candidates.push(c);
        }
    }

    // ------------------------------------------------------------------
    // published media binding
    // ------------------------------------------------------------------

    /// Fallback for str0m's MediaAdded (offers are scanned when accepted).
    pub(crate) fn on_media_added(&mut self, tid: TransportId, mid: Mid) {
        let Some(t) = self.transports.get_mut(&tid) else {
            return;
        };
        if !t.receives_media() || t.remote_mids.contains(&mid) {
            return;
        }
        let Some(m) = t.rtc.media(mid) else { return };
        if !m.direction().is_receiving() || m.disabled() {
            return;
        }
        let info = RemoteInfo {
            prefers_audio_red: false,
            primary_ssrcs: None,
            msid_track_id: m.track_id().to_string(),
            rids: rids_of(m.rids_rx()),
        };
        let ev = RemoteMedia {
            // Event::MediaAdded carries no raw SDP SSRC eligibility proof.
            registration: rtc_core::media_api::PublicationRegistration::Pending,
            participant: t.spec.participant.clone(),
            transport: tid,
            mid: mid.to_string(),
            kind: codecs::track_kind(m.kind()),
            msid_track_id: info.msid_track_id.clone(),
            rids: info.rids.clone(),
            mime_type: mime_of_media(&t.rtc, m).to_string(),
        };
        t.remote_mids.insert(mid);
        t.remote_info.insert(mid, info);
        self.events.push(MediaEvent::RemoteMediaAdded(ev));
    }

    pub(crate) fn report_remote_stopped(&mut self, tid: TransportId, mid: Mid) {
        // An inactive section can resume later without a new str0m MediaAdded
        // event. Let the next receiving offer discover it again.
        if let Some(t) = self.transports.get_mut(&tid) {
            t.remote_mids.remove(&mid);
            t.remote_info.remove(&mid);
            t.publication_rtp.remove(&mid);
        }
        let Some(&ui) = self.up_by_mid.get(&(tid, mid)) else {
            return;
        };
        let Some(u) = self.ups.get_mut(ui) else {
            return;
        };
        if u.stopped_reported {
            return;
        }
        u.stopped_reported = true;
        let publisher = u.publisher.clone();
        let sid = u.sid.clone();
        if self.codec_receivers.get(&sid).is_some_and(|group| {
            group.receivers.iter().any(|r| {
                self.ups
                    .get(r.up)
                    .is_some_and(|receiver| !receiver.stopped_reported)
            })
        }) {
            return;
        }
        self.events.push(MediaEvent::RemoteMediaStopped {
            participant: publisher,
            track_sid: sid,
        });
    }

    pub(crate) fn bind_track(
        &mut self,
        tid: TransportId,
        mid_str: &str,
        track_sid: TrackSid,
        now: Instant,
    ) {
        let mid = Mid::from(mid_str);
        let Some(t) = self.transports.get_mut(&tid) else {
            return;
        };
        let Some(m) = t.rtc.media(mid) else {
            tracing::warn!(transport = %tid, mid = mid_str, "BindTrack for unknown mid");
            return;
        };
        let kind = codecs::track_kind(m.kind());
        let mime = if t
            .remote_info
            .get(&mid)
            .is_some_and(|info| info.prefers_audio_red)
        {
            "audio/red"
        } else {
            mime_of_media(&t.rtc, m)
        }
        .to_string();
        let rids = t
            .remote_info
            .get(&mid)
            .map(|i| i.rids.clone())
            .unwrap_or_default();
        let participant = t.spec.participant.clone();
        let spec = match t.expected.iter().position(|e| e.sid == track_sid) {
            Some(p) => t.expected.remove(p),
            None => self
                .published
                .get(&track_sid)
                .cloned()
                .unwrap_or_else(|| minimal_spec(&track_sid, kind)),
        };
        // The session codec config may normalize payload preference across
        // m-lines. A multi-codec publication's CID identifies its receiver;
        // only accept that declared codec when it was truly negotiated on MID.
        let mime = t
            .remote_info
            .get(&mid)
            .and_then(|info| {
                spec.codecs.iter().find(|codec| {
                    codec.cid == info.msid_track_id || codec.sdp_cid == info.msid_track_id
                })
            })
            .filter(|codec| {
                !codec.mime_type.is_empty()
                    && t.rtc.media(mid).is_some_and(|media| {
                        media.remote_pts().iter().any(|pt| {
                            t.rtc.codec_config().params().iter().any(|param| {
                                param.pt() == *pt
                                    && codecs::mime_of(param.spec().codec)
                                        .eq_ignore_ascii_case(&codec.mime_type)
                            })
                        })
                    })
            })
            .map(|codec| codec.mime_type.clone())
            .unwrap_or(mime);

        if let Some(&old) = self.up_by_mid.get(&(tid, mid)) {
            // Another track was bound on this mid before; its mapping is replaced below.
            if self.ups.get(old).is_some_and(|u| u.sid != track_sid) {
                tracing::warn!(transport = %tid, mid = mid_str, "rebinding mid to a different track");
            }
        }
        if spec.codecs.len() > 1
            && !spec.codecs.iter().any(|codec| {
                codec.mime_type.is_empty() || codec.mime_type.eq_ignore_ascii_case(&mime)
            })
        {
            tracing::warn!(track=%track_sid, %mime, "receiver codec absent from published codec set");
            return;
        }
        // Separate actual receivers per codec; rebinding one codec preserves
        // every other codec's MID/SSRC and its own subscriber list.
        let existing = self
            .codec_receivers
            .get(&track_sid)
            .and_then(|group| group.find(&mime))
            .or_else(|| {
                self.up_by_sid.get(&track_sid).copied().filter(|ui| {
                    self.ups.get(*ui).is_some_and(|up| {
                        up.mime.eq_ignore_ascii_case(&mime) || spec.codecs.len() < 2
                    })
                })
            });
        let ui = match existing {
            Some(ui) => {
                let u = self.ups.get_mut(ui).expect("indexed up track");
                let old_key = (u.transport, u.mid);
                u.rebind_source(tid, mid, rids, &mime, spec, now);
                if old_key != (tid, mid) && self.up_by_mid.get(&old_key) == Some(&ui) {
                    self.up_by_mid.remove(&old_key);
                }
                ui
            }
            None => {
                let mut up = UpTrack::new(
                    spec,
                    participant,
                    tid,
                    mid,
                    kind,
                    mime.clone(),
                    rids,
                    &self.audio_params,
                    self.cfg.video.dynacast_pause_delay,
                    now,
                );
                up.configure_bitrate_reports(self.cfg.video.bitrate_report);
                up.configure_stream_trackers(self.cfg.video.stream_trackers);
                let ui = self.ups.insert(up);
                self.up_by_sid.entry(track_sid.clone()).or_insert(ui);
                ui
            }
        };
        self.codec_receivers
            .entry(track_sid.clone())
            .or_default()
            .bind(&mime, ui);
        self.up_by_mid.insert((tid, mid), ui);
        // A negotiated backup-only subscriber already holds codec demand.
        // Move it to the receiver on bind, before the first packet. Waiting
        // for RTP here can publish an OFF update that stops the backup sender
        // before it produces that packet. Regression and forwarding still
        // require actual authenticated media and the backup keyframe.
        self.select_codec_receivers(ui, now);
        // A first packet can arrive in the same native event drain before core
        // handles RemoteMediaAdded/BindTrack. Replay only its current CID proof.
        if let Some(t) = self.transports.get(&tid)
            && let Some((cid, mime, pt)) = t.publication_rtp.get(&mid)
            && t.remote_info
                .get(&mid)
                .is_some_and(|info| &info.msid_track_id == cid)
        {
            self.events.push(MediaEvent::PublicationRtpRegistered {
                participant: t.spec.participant.clone(),
                transport: tid,
                track_sid: self
                    .ups
                    .get(ui)
                    .expect("bound publication exists")
                    .sid
                    .clone(),
                mime_type: mime.clone(),
                payload_type: *pt,
            });
        }
        self.poll_receiver_traffic(now);
        self.refresh_down_layers(ui, now);
    }

    // ------------------------------------------------------------------
    // down track placement on subscriber transports
    // ------------------------------------------------------------------

    /// Put a down track on its subscriber's sending transport (if one exists).
    pub(crate) fn attach_down(&mut self, di: usize, now: Instant) {
        let Some(d) = self.downs.get(di) else { return };
        let subscriber = d.spec.subscriber.clone();
        let Some(tid) = self.sending_transport(&subscriber) else {
            return;
        };
        if let Some(d) = self.downs.get_mut(di) {
            d.transport = Some(tid);
        }
        let disable_opus_nack = self.downs.get(di).is_some_and(|down| {
            !down.is_video()
                && self.ups.get(down.up).is_some_and(|up| {
                    // WrappedReceiver adds RED to plain Opus only when the track
                    // permits it, active RED encoding is enabled, and unencrypted.
                    // A RED publisher already exposes RED plus primary Opus.
                    red_enabled(&up.spec)
                        && (up.mime.eq_ignore_ascii_case("audio/red")
                            || up.spec.codecs.first().is_some_and(|codec| {
                                codec.mime_type.eq_ignore_ascii_case("audio/red")
                            })
                            || (!up.spec.encrypted && self.cfg.audio.active_red_encoding))
                })
        });
        let Some(t) = self.transports.get_mut(&tid) else {
            return;
        };
        if disable_opus_nack && t.spec.client_supports_red {
            // Go configureSenderAudio removes the Opus NACK default for
            // later senders. Existing plain transceivers keep their NACK.
            t.rtc.direct_api().disable_opus_nack();
        }
        match t.role() {
            PcRole::Subscriber => {
                t.pending_adds.push(di);
                self.negotiate(tid, false, now);
            }
            PcRole::Single => {
                t.pending_single.push_back(di);
                t.requirement_dirty = true;
                // Even with a free section the client must re-offer (§3.2).
                t.requirement_force = true;
            }
            PcRole::Publisher => {}
        }
    }

    pub(crate) fn sending_transport(&self, p: &ParticipantSid) -> Option<TransportId> {
        let list = self.participants.get(p)?;
        list.iter().copied().rfind(|tid| {
            self.transports
                .get(tid)
                .is_some_and(|t| t.sends_media() && !t.failed)
        })
    }

    /// Remove a down track from its transport (stop / retire the m-line).
    pub(crate) fn detach_down(&mut self, di: usize, now: Instant) {
        let Some(d) = self.downs.get(di) else { return };
        let (tid, mid) = (d.transport, d.mid);
        let Some(tid) = tid else { return };
        if let Some(mid) = mid {
            self.down_by_mid.remove(&(tid, mid));
        }
        let Some(t) = self.transports.get_mut(&tid) else {
            return;
        };
        t.pending_adds.retain(|x| *x != di);
        t.pending_single.retain(|x| *x != di);
        match (t.role(), mid) {
            (PcRole::Subscriber, Some(mid)) => {
                t.pending_stops.push(mid);
                t.offered.retain(|m| *m != mid);
                self.negotiate(tid, false, now);
            }
            (PcRole::Single, Some(mid)) => {
                t.assigned.retain(|(m, _)| *m != mid);
                t.retired.insert(mid);
                t.requirement_dirty = true;
                t.requirement_force = true;
            }
            (PcRole::Single, None) => {
                t.requirement_dirty = true;
            }
            _ => {}
        }
    }

    /// Single PC: ask the client for the missing sections (signaling.md §3.2).
    pub(crate) fn flush_requirements(&mut self) {
        let Self {
            transports,
            downs,
            events,
            ..
        } = self;
        for t in transports.values_mut() {
            if t.spec.one_shot || t.role() != PcRole::Single || !t.requirement_dirty {
                continue;
            }
            t.requirement_dirty = false;
            let (mut pa, mut pv) = (0u32, 0u32);
            for di in &t.pending_single {
                match downs.get(*di).map(|d| d.kind) {
                    Some(TrackKind::Video) => pv += 1,
                    Some(_) => pa += 1,
                    None => {}
                }
            }
            let (mut fa, mut fv) = (0u32, 0u32);
            for (m, k) in &t.send_sections {
                if t.used_mids.contains(m) || t.retired.contains(m) {
                    continue;
                }
                match k {
                    MediaKind::Video => fv += 1,
                    MediaKind::Audio => fa += 1,
                }
            }
            let need_a = pa.saturating_sub(fa);
            let need_v = pv.saturating_sub(fv);
            let da = need_a.saturating_sub(t.requested.0);
            let dv = need_v.saturating_sub(t.requested.1);
            if da > 0 || dv > 0 || t.requirement_force {
                t.requested.0 += da;
                t.requested.1 += dv;
                events.push(MediaEvent::MediaSectionsRequirement {
                    participant: t.spec.participant.clone(),
                    audios: da,
                    videos: dv,
                });
            }
            t.requirement_force = false;
        }
    }

    // ------------------------------------------------------------------
    // data channels
    // ------------------------------------------------------------------

    #[cfg(test)]
    pub(crate) fn on_channel_open(&mut self, tid: TransportId, id: ChannelId, label: String) {
        self.on_channel_open_at(tid, id, label, Instant::now());
    }
    pub(crate) fn on_channel_open_at(
        &mut self,
        tid: TransportId,
        id: ChannelId,
        label: String,
        now: Instant,
    ) {
        let Some(t) = self.transports.get_mut(&tid) else {
            return;
        };
        if label == LABEL_DATA_TRACK && !t.spec.enable_data_tracks {
            t.rtc.direct_api().close_data_channel(id);
            self.mark_dirty(tid);
            return;
        }
        let kind = match label.as_str() {
            LABEL_RELIABLE => Some(DataKind::Reliable),
            LABEL_LOSSY => Some(DataKind::Lossy),
            _ => None,
        };
        // Go's onDataChannel closes the previous writer when a new ordinary
        // or data-track channel opens. Keep retirement tied to the old ID.
        let mut replaced_writer = false;
        if kind.is_some() || label == LABEL_DATA_TRACK {
            let replaced: Vec<_> = t
                .channels
                .iter()
                .filter(|c| c.label == label && c.id != id)
                .map(|c| c.id)
                .collect();
            replaced_writer = !replaced.is_empty();
            for old in replaced {
                t.rtc.direct_api().close_data_channel(old);
                t.ordinary_data_writers.remove(&old);
            }
            t.channels.retain(|c| c.label != label);
        }
        if label == LABEL_DATA_TRACK {
            t.data_track_writer = Some((
                id,
                crate::data_track_writer::DataTrackWriter::new(
                    self.cfg.rtc.data_track_target_latency.as_nanos_i64(),
                    now,
                ),
            ));
        }
        t.channels.retain(|c| c.id != id);
        t.channels.push(ChannelEntry { id, label, kind });
        if let Some(kind) = kind {
            t.ordinary_data_writers.insert(
                id,
                crate::ordinary_data_writer::Writer::new(
                    kind == DataKind::Reliable,
                    self.cfg.rtc.data_channel_slow_threshold,
                    self.cfg.rtc.data_channel_max_buffered_amount,
                    self.cfg
                        .rtc
                        .data_channel_lossy_target_latency
                        .as_nanos_i64(),
                    now,
                ),
            );
            t.closed_data_kinds[crate::transport::Transport::data_kind_index(kind)] = false;
            t.opened_data_kinds[crate::transport::Transport::data_kind_index(kind)] = true;
        }
        if replaced_writer {
            self.mark_dirty(tid);
        }
        self.maybe_report_fully_established(tid);
    }

    pub(crate) fn maybe_report_fully_established(&mut self, tid: TransportId) {
        let Some(t) = self.transports.get_mut(&tid) else {
            return;
        };
        if !t.fully_established_reported
            && t.fully_established_connected
            && (t.spec.one_shot
                || t.first_remote_offer_has_data == Some(false)
                || t.opened_data_kinds == [true, true])
        {
            t.fully_established_reported = true;
            self.events.push(MediaEvent::PrimaryTransportEstablished {
                participant: t.spec.participant.clone(),
                transport: tid,
            });
        }
    }

    pub(crate) fn on_channel_data(&mut self, tid: TransportId, d: ChannelData) {
        // Fixed transport.go reads a complete detached message into 65535 bytes;
        // io.ErrShortBuffer ends that reader and closes only this data channel.
        // This is independent of the peer's negotiated outbound message limit.
        if d.data.len() > 65535 {
            if let Some(t) = self.transports.get_mut(&tid) {
                t.rtc.direct_api().close_data_channel(d.id);
            }
            self.mark_dirty(tid);
            return;
        }
        let Some(t) = self.transports.get(&tid) else {
            return;
        };
        let Some((label, kind)) = t
            .channels
            .iter()
            .find(|c| c.id == d.id)
            .map(|c| (c.label.clone(), c.kind.unwrap_or(DataKind::Reliable)))
        else {
            return;
        };
        if label == LABEL_DATA_TRACK && !t.spec.enable_data_tracks {
            return;
        }
        self.events.push(MediaEvent::DataReceived {
            participant: t.spec.participant.clone(),
            kind,
            label,
            payload: d.data,
        });
    }

    /// Data tracks have their own unordered, zero-retransmit SCTP channel.
    /// They never fall back to the protobuf data-packet channels.
    #[cfg(test)]
    pub(crate) fn send_data_track(&mut self, to: &ParticipantSid, payload: &[u8]) {
        self.send_data_track_at(to, payload, Instant::now());
    }
    pub(crate) fn send_data_track_at(&mut self, to: &ParticipantSid, payload: &[u8], now: Instant) {
        let Some((tid, cid)) = self.data_track_channel(to) else {
            return;
        };
        let Some(t) = self.transports.get_mut(&tid) else {
            return;
        };
        let Some((writer_id, writer)) = &mut t.data_track_writer else {
            return;
        };
        if *writer_id != cid {
            return;
        }
        let Some(mut ch) = t.rtc.channel(cid) else {
            return;
        };
        match writer.admit(ch.buffered_amount()) {
            crate::data_track_writer::Admission::Write => {}
            crate::data_track_writer::Admission::HighBuffered => return,
            crate::data_track_writer::Admission::InvalidNegativeTarget => {
                tracing::warn!(participant=%to,"negative DataTrack target latency has no valid source writer");
                return;
            }
        }
        // Source default SCTP writes are nonblocking and have no native 128KiB
        // aggregate budget. Keep ordinary Channel::write behavior unchanged.
        let result = ch.write_with_buffer_budget(true, payload, usize::MAX);
        match result {
            Ok(true) => {
                writer.record(payload.len(), ch.buffered_amount(), now);
                self.accepted_data_out.0 = self
                    .accepted_data_out
                    .0
                    .saturating_add(payload.len() as u64);
                self.accepted_data_out.1 = self.accepted_data_out.1.saturating_add(1);
            }
            Ok(false) => writer.record(0, ch.buffered_amount(), now),
            Err(error) => {
                writer.record(0, ch.buffered_amount(), now);
                tracing::debug!(participant=%to,%error,"data track write failed");
            }
        }
        self.mark_dirty(tid);
    }

    pub(crate) fn data_track_channel(
        &self,
        to: &ParticipantSid,
    ) -> Option<(TransportId, ChannelId)> {
        let list = self.participants.get(to)?;
        list.iter().find_map(|tid| {
            let t = self.transports.get(tid)?;
            if !t.usable() || !t.spec.primary || !t.spec.enable_data_tracks {
                return None;
            }
            let channel = t.channels.iter().find(|c| c.label == LABEL_DATA_TRACK)?;
            Some((*tid, channel.id))
        })
    }
}

/// `IsRedEnabled(ti)` (utils.go:201-207): primary codec is `audio/red`, or RED not
/// disabled when no codec info is present.
fn red_enabled(spec: &PublishedTrackSpec) -> bool {
    match spec
        .codecs
        .first()
        .map(|c| c.mime_type.as_str())
        .filter(|m| !m.is_empty())
    {
        Some(m) => m.eq_ignore_ascii_case("audio/red"),
        None => !spec.disable_red,
    }
}

fn minimal_spec(sid: &TrackSid, kind: TrackKind) -> PublishedTrackSpec {
    PublishedTrackSpec {
        backup_codec_policy: Default::default(),
        sid: sid.clone(),
        kind,
        source: TrackSource::Unknown,
        cid: String::new(),
        layers: Vec::new(),
        codecs: Vec::new(),
        stereo: false,
        disable_dtx: false,
        disable_red: false,
        encrypted: false,
        muted: false,
    }
}

impl MediaEngine {
    fn reject_one_shot_offer(&mut self, tid: TransportId, id: u32, message: String) {
        if let Some(t) = self.transports.get_mut(&tid).filter(|t| t.spec.one_shot) {
            t.one_shot_offer = None;
            t.one_shot_answer_waiting = None;
            t.defer_one_shot_answer = false;
            self.events.push(MediaEvent::RemoteDescriptionRejected {
                participant: t.spec.participant.clone(),
                id,
                message,
            });
        }
    }
    pub(crate) fn prepare_one_shot_offer(
        &mut self,
        tid: TransportId,
        sdp: String,
        id: u32,
        now: Instant,
    ) {
        let Some(t) = self
            .transports
            .get_mut(&tid)
            .filter(|t| t.spec.one_shot && t.role() == PcRole::Single)
        else {
            return;
        };
        t.defer_one_shot_answer = true;
        t.one_shot_offer = Some((id, sdp.clone()));
        self.handle_offer(tid, &sdp, id, now);
    }
    pub(crate) fn finalize_one_shot_answer(&mut self, tid: TransportId, id: u32, now: Instant) {
        let Some(t) = self.transports.get_mut(&tid).filter(|t| t.spec.one_shot) else {
            return;
        };
        let Some((pending_id, sdp)) = t.one_shot_offer.as_ref() else {
            return;
        };
        if *pending_id != id {
            return;
        }
        let sdp = sdp.clone();
        t.defer_one_shot_answer = false;
        // Re-apply the same actually accepted remote offer against the current real
        // DownTrack graph. Native accept_offer produces the final genuine answer.
        self.handle_offer(tid, &sdp, id, now);
        if let Some(t) = self.transports.get_mut(&tid) {
            t.one_shot_offer = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtc_core::model::TrackCodec;

    fn spec(sid: &str, kind: TrackKind, cid: &str) -> PublishedTrackSpec {
        let mut s = minimal_spec(&TrackSid::from(sid), kind);
        s.cid = cid.into();
        s
    }

    #[test]
    fn expected_track_matching_order() {
        let mut with_codec = spec("TR_c", TrackKind::Audio, "x");
        with_codec.codecs.push(TrackCodec {
            sdp_cid: "sdp-cid".into(),
            ..Default::default()
        });
        let list = vec![
            spec("TR_a", TrackKind::Audio, "cid-a"),
            spec("TR_v", TrackKind::Video, "cid-v"),
            with_codec,
        ];
        assert_eq!(
            match_expected(&list, TrackKind::Audio, "cid-a", &[])
                .unwrap()
                .sid
                .as_str(),
            "TR_a"
        );
        assert_eq!(
            match_expected(&list, TrackKind::Audio, "sdp-cid", &[])
                .unwrap()
                .sid
                .as_str(),
            "TR_c"
        );
        // unknown msid → first pending of that kind
        assert_eq!(
            match_expected(&list, TrackKind::Audio, "zzz", &[])
                .unwrap()
                .sid
                .as_str(),
            "TR_a"
        );
        assert_eq!(
            match_expected(&list, TrackKind::Audio, "zzz", &[TrackSid::from("TR_a")])
                .unwrap()
                .sid
                .as_str(),
            "TR_c"
        );
        assert!(
            match_expected(&list, TrackKind::Video, "cid-a", &[TrackSid::from("TR_v")]).is_none()
        );
    }

    #[test]
    fn offer_sections_extracts_text_info() {
        let sdp = "v=0\r\nm=audio 9 X 111\r\na=mid:0\r\na=msid:s t-audio\r\nm=video 9 X 96\r\na=mid:1\r\na=simulcast:send q;h;f\r\nm=application 9 X webrtc-datachannel\r\na=mid:2\r\n";
        let s = offer_sections(sdp);
        assert_eq!(s.len(), 2);
        assert_eq!(s[0].mid_str, "0");
        assert_eq!(s[0].msid_track.as_deref(), Some("t-audio"));
        assert_eq!(s[1].rids, vec!["q".to_string(), "h".into(), "f".into()]);
    }
}

#[cfg(test)]
mod publication_sdp_registration_tests {
    use super::*;
    #[test]
    fn source_receiver_callback_requires_nonzero_ssrc_and_no_send_rid() {
        let audio = "m=audio 9 X 111\r\na=mid:0\r\na=ssrc:7 cname:x\r\n";
        assert!(sdp_registration_eligible(audio));
        let video = "m=video 9 X 96\r\na=mid:0\r\na=ssrc:7 cname:x\r\na=ssrc:8 cname:repair\r\n";
        assert!(sdp_registration_eligible(video));
        assert!(!sdp_registration_eligible("m=video 9 X 96\r\na=mid:0\r\n"));
        assert!(!sdp_registration_eligible(
            &video.replace("a=ssrc:7", "a=ssrc:0")
        ));
        assert!(!sdp_registration_eligible(&format!(
            "{video}a=msid:s t\r\na=rid:f send\r\n"
        )));
        assert!(!sdp_registration_eligible(&format!(
            "{video}a=msid:s t\r\na=rid:f recv\r\n"
        )));
        assert!(sdp_registration_eligible(&format!(
            "{video}a=simulcast:send q;h;f\r\n"
        )));
        assert!(sdp_registration_eligible(
            "m=video 9 X 96\r\na=ssrc:8 cname:repair\r\na=ssrc:7 cname:main\r\na=ssrc-group:FID 7 8\r\n"
        ));
    }
}

#[cfg(test)]
mod actual_receiver_source_oracle {
    use super::*;
    #[test]
    #[ignore = "actual pinned Pion private receiver eligibility oracle, normalized TSV"]
    fn fixed_pion_receiver_callback_vectors() {
        let cases: &[(&str, &str)] = &[
            (
                "audio-primary",
                "v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=mid:0\r\na=sendonly\r\na=rtpmap:111 opus/48000/2\r\na=msid:s t\r\na=ssrc:7 cname:x\r\n",
            ),
            (
                "video-primary",
                "v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\na=mid:0\r\na=sendonly\r\na=rtpmap:96 VP8/90000\r\na=msid:s t\r\na=ssrc:7 cname:x\r\n",
            ),
            (
                "video-zero",
                "v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\na=mid:0\r\na=sendonly\r\na=rtpmap:96 VP8/90000\r\na=msid:s t\r\na=ssrc:0 cname:x\r\n",
            ),
            (
                "video-absent",
                "v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\na=mid:0\r\na=sendonly\r\na=rtpmap:96 VP8/90000\r\na=msid:s t\r\n",
            ),
            (
                "rid-send",
                "v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\na=mid:0\r\na=sendonly\r\na=rtpmap:96 VP8/90000\r\na=msid:s t\r\na=ssrc:7 cname:x\r\na=rid:f send\r\n",
            ),
            (
                "rid-recv-source-guard",
                "v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\na=mid:0\r\na=sendonly\r\na=rtpmap:96 VP8/90000\r\na=msid:s t\r\na=ssrc:7 cname:x\r\na=rid:f recv\r\n",
            ),
            (
                "rid-without-msid",
                "v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\na=mid:0\r\na=sendonly\r\na=rtpmap:96 VP8/90000\r\na=ssrc:7 cname:x\r\na=rid:f send\r\n",
            ),
            (
                "simulcast-without-rid",
                "v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\na=mid:0\r\na=sendonly\r\na=rtpmap:96 VP8/90000\r\na=msid:s t\r\na=ssrc:7 cname:x\r\na=simulcast:send q;h;f\r\n",
            ),
            (
                "repair-before-fid",
                "v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\na=mid:0\r\na=sendonly\r\na=rtpmap:96 VP8/90000\r\na=msid:s t\r\na=ssrc:8 cname:r\r\na=ssrc:7 cname:x\r\na=ssrc-group:FID 7 8\r\n",
            ),
            (
                "repair-before-fec",
                "v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\na=mid:0\r\na=sendonly\r\na=rtpmap:96 VP8/90000\r\na=msid:s t\r\na=ssrc:8 cname:r\r\na=ssrc:7 cname:x\r\na=ssrc-group:FEC-FR 7 8\r\n",
            ),
            (
                "only-repair-declared",
                "v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\na=mid:0\r\na=sendonly\r\na=rtpmap:96 VP8/90000\r\na=msid:s t\r\na=ssrc-group:FID 7 8\r\na=ssrc:8 cname:r\r\n",
            ),
            (
                "legacy-ssrc-msid-rid",
                "v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\na=mid:0\r\na=sendonly\r\na=rtpmap:96 VP8/90000\r\na=ssrc:7 msid:s t\r\na=rid:f send\r\n",
            ),
        ];
        let output = std::fs::read_to_string(
            std::env::var("RECONNECT_RECEIVER_GO_RESULTS")
                .expect("actual Pion receiver TSV output"),
        )
        .unwrap();
        let rows: Vec<_> = output
            .lines()
            .map(|line| line.split('\t').collect::<Vec<_>>())
            .collect();
        assert_eq!(rows.len(), cases.len());
        assert_eq!(rows.len(), 12);
        for ((name, sdp), row) in cases.iter().zip(rows) {
            assert_eq!(row.len(), 3);
            assert_eq!(row[0], *name);
            let section = sdp_munge::split_sections(sdp).1[0];
            assert_eq!(
                sdp_registration_eligible(section),
                row[1].parse::<bool>().unwrap(),
                "{name}"
            );
            let _: usize = row[2].parse().unwrap();
        }
    }
}
