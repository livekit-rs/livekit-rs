//! `SignalRequest` → engine requests and engine signals → `SignalResponse`.

use std::collections::{HashMap, VecDeque};

use lk_protocol::livekit as pb;
use pb::signal_request::Message as Req;
use pb::signal_response::Message as Resp;
use rtc_core::ids::*;
use rtc_core::model::*;
use rtc_core::session::*;

use crate::convert;
use crate::signal::params::add_track_from_proto;

/// Original requests kept so a RequestResponse can echo them.
const REQUEST_CACHE: usize = 256;

pub struct RequestCache {
    next_token: u32,
    entries: VecDeque<(u32, pb::request_response::Request)>,
}

impl Default for RequestCache {
    fn default() -> Self {
        // Tokens 1..=N are used by JoinRequest.add_track_requests.
        Self {
            next_token: 1 << 20,
            entries: VecDeque::new(),
        }
    }
}

impl RequestCache {
    fn store(&mut self, req: pb::request_response::Request) -> u32 {
        self.next_token = self.next_token.wrapping_add(1).max(1 << 20);
        let token = self.next_token;
        if self.entries.len() >= REQUEST_CACHE {
            self.entries.pop_front();
        }
        self.entries.push_back((token, req));
        token
    }

    pub fn store_with(&mut self, token: u32, req: pb::request_response::Request) {
        if self.entries.len() >= REQUEST_CACHE {
            self.entries.pop_front();
        }
        self.entries.push_back((token, req));
    }

    fn take(&mut self, token: u32) -> Option<pb::request_response::Request> {
        let pos = self.entries.iter().position(|(t, _)| *t == token)?;
        self.entries.remove(pos).map(|(_, r)| r)
    }
}

/// What the WebSocket layer must do with an inbound request.
pub enum Inbound {
    Forward(SessionRequest),
    /// Answer locally (ping / pong, trickle parse errors).
    Reply(pb::SignalResponse),
    /// Forward and also answer locally (ping_req with rtt).
    ForwardAndReply(Box<SessionRequest>, pb::SignalResponse),
    Ignore,
}

fn response(m: Resp) -> pb::SignalResponse {
    pb::SignalResponse { message: Some(m) }
}

fn now_ms() -> i64 {
    rtc_core::util::unix_ms()
}

fn target_from_proto(v: i32) -> SignalTarget {
    if v == pb::SignalTarget::Subscriber as i32 {
        SignalTarget::Subscriber
    } else {
        SignalTarget::Publisher
    }
}

fn target_to_proto(t: SignalTarget) -> i32 {
    match t {
        SignalTarget::Publisher => pb::SignalTarget::Publisher as i32,
        SignalTarget::Subscriber => pb::SignalTarget::Subscriber as i32,
    }
}

/// pion `ICECandidateInit` JSON.
#[derive(serde::Deserialize, serde::Serialize)]
struct CandidateJson {
    candidate: String,
    #[serde(rename = "sdpMid")]
    sdp_mid: Option<String>,
    #[serde(rename = "sdpMLineIndex")]
    sdp_mline_index: Option<u32>,
    #[serde(rename = "usernameFragment")]
    username_fragment: Option<String>,
}

fn track_sids(v: &[String]) -> Vec<TrackSid> {
    v.iter().map(|s| TrackSid(s.clone())).collect()
}

fn subscription_sids(s: &pb::UpdateSubscription) -> Vec<TrackSid> {
    let mut out = track_sids(&s.track_sids);
    for pt in &s.participant_tracks {
        out.extend(track_sids(&pt.track_sids));
    }
    out
}

/// Client-side leave reasons kept as-is; anything else is CLIENT_INITIATED.
fn client_leave_reason(v: i32) -> DisconnectReason {
    match convert::disconnect_reason_from_proto(v) {
        r @ (DisconnectReason::UserUnavailable
        | DisconnectReason::UserRejected
        | DisconnectReason::AgentError
        | DisconnectReason::SipTrunkFailure) => r,
        _ => DisconnectReason::ClientInitiated,
    }
}

pub fn decode_request(req: pb::SignalRequest, cache: &mut RequestCache) -> Inbound {
    use pb::request_response::Request as Echo;
    let Some(msg) = req.message else {
        return Inbound::Forward(SessionRequest::SignalActivity);
    };
    match msg {
        Req::Offer(sd) => Inbound::Forward(SessionRequest::Offer {
            sdp: sd.sdp,
            id: sd.id,
        }),
        Req::Answer(sd) => Inbound::Forward(SessionRequest::Answer {
            sdp: sd.sdp,
            id: sd.id,
        }),
        Req::Trickle(t) => match serde_json::from_str::<CandidateJson>(&t.candidate_init) {
            Ok(c) => {
                let token = cache.store(Echo::Trickle(t.clone()));
                Inbound::Forward(SessionRequest::Trickle {
                    token,
                    candidate: CandidateInit {
                        candidate: c.candidate,
                        sdp_mid: c.sdp_mid,
                        sdp_mline_index: c.sdp_mline_index,
                        username_fragment: c.username_fragment,
                    },
                    target: target_from_proto(t.target),
                    last: t.r#final,
                })
            }
            Err(e) => Inbound::ForwardAndReply(
                Box::new(SessionRequest::SignalActivity),
                response(Resp::RequestResponse(pb::RequestResponse {
                    request_id: 0,
                    reason: pb::request_response::Reason::UnclassifiedError as i32,
                    message: e.to_string(),
                    request: Some(Echo::Trickle(t)),
                })),
            ),
        },
        Req::AddTrack(a) => {
            let token = cache.store(Echo::AddTrack(a.clone()));
            Inbound::Forward(SessionRequest::AddTrack(add_track_from_proto(&a, token)))
        }
        Req::Mute(m) => {
            let token = cache.store(Echo::Mute(m.clone()));
            Inbound::Forward(SessionRequest::Mute {
                token,
                sid: TrackSid(m.sid),
                muted: m.muted,
            })
        }
        Req::Subscription(s) => Inbound::Forward(SessionRequest::Subscription {
            track_sids: subscription_sids(&s),
            subscribe: s.subscribe,
        }),
        Req::TrackSetting(s) => Inbound::Forward(SessionRequest::TrackSettings {
            track_sids: track_sids(&s.track_sids),
            settings: TrackSettings {
                disabled: s.disabled,
                quality: convert::video_quality_from_proto(s.quality),
                width: s.width,
                height: s.height,
                fps: s.fps,
                priority: s.priority,
            },
        }),
        Req::Leave(l) => Inbound::Forward(SessionRequest::Leave {
            reason: client_leave_reason(l.reason),
        }),
        Req::UpdateLayers(_) => Inbound::Forward(SessionRequest::SignalActivity), // deprecated payload; Go still updates last seen
        Req::SubscriptionPermission(p) => Inbound::Forward(SessionRequest::SubscriptionPermission(
            convert::subscription_permission_from_proto(&p),
        )),
        Req::SyncState(s) => {
            let subscription = s.subscription.clone().unwrap_or_default();
            Inbound::Forward(SessionRequest::SyncState(SyncState {
                publish_data_tracks: s
                    .publish_data_tracks
                    .iter()
                    .map(|t| {
                        TrackSid(
                            t.info
                                .as_ref()
                                .map(|info| info.sid.clone())
                                .unwrap_or_default(),
                        )
                    })
                    .collect(),
                publish_tracks: s
                    .publish_tracks
                    .iter()
                    .map(|t| TrackSid(t.track.as_ref().map(|t| t.sid.clone()).unwrap_or_default()))
                    .collect(),
                track_sids_disabled: track_sids(&s.track_sids_disabled),
                subscribe: subscription.subscribe,
                subscription_track_sids: subscription_sids(&subscription),
            }))
        }
        Req::Simulate(s) => {
            use pb::simulate_scenario::Scenario as S;
            let sim = match s.scenario {
                Some(S::SpeakerUpdate(n)) => Simulate::SpeakerUpdate { seconds: n },
                Some(S::NodeFailure(_)) => Simulate::NodeFailure,
                Some(S::Migration(_)) => Simulate::Migration,
                Some(S::ServerLeave(_)) => Simulate::ServerLeave,
                Some(S::SwitchCandidateProtocol(p)) => Simulate::SwitchCandidateProtocol(p),
                Some(S::SubscriberBandwidth(b)) => Simulate::SubscriberBandwidth(b),
                Some(S::DisconnectSignalOnResume(_)) => Simulate::DisconnectSignalOnResume,
                Some(S::DisconnectSignalOnResumeNoMessages(_)) => {
                    Simulate::DisconnectSignalOnResumeNoMessages
                }
                Some(S::LeaveRequestFullReconnect(_)) => Simulate::LeaveRequestFullReconnect,
                None => return Inbound::Forward(SessionRequest::SignalActivity),
            };
            Inbound::Forward(SessionRequest::Simulate(sim))
        }
        Req::Ping(_) => Inbound::ForwardAndReply(
            Box::new(SessionRequest::SignalActivity),
            response(Resp::Pong(now_ms())),
        ),
        Req::UpdateMetadata(u) => {
            let token = cache.store(Echo::UpdateMetadata(u.clone()));
            Inbound::Forward(SessionRequest::UpdateMetadata {
                token,
                request_id: u.request_id,
                name: u.name,
                metadata: u.metadata,
                attributes: u.attributes.into_iter().collect(),
            })
        }
        Req::PingReq(p) => {
            let pong = response(Resp::PongResp(pb::Pong {
                last_ping_timestamp: p.timestamp,
                timestamp: now_ms(),
            }));
            if p.rtt > 0 {
                Inbound::ForwardAndReply(Box::new(SessionRequest::SignalRtt(p.rtt as u32)), pong)
            } else {
                Inbound::ForwardAndReply(Box::new(SessionRequest::SignalActivity), pong)
            }
        }
        Req::UpdateAudioTrack(u) => {
            let token = cache.store(Echo::UpdateAudioTrack(u.clone()));
            Inbound::Forward(SessionRequest::UpdateAudioTrack {
                token,
                sid: TrackSid(u.track_sid),
                features: u
                    .features
                    .iter()
                    .map(|f| convert::audio_feature_from_proto(*f))
                    .collect(),
            })
        }
        Req::UpdateVideoTrack(u) => {
            let token = cache.store(Echo::UpdateVideoTrack(u.clone()));
            Inbound::Forward(SessionRequest::UpdateVideoTrack {
                token,
                sid: TrackSid(u.track_sid),
                width: u.width,
                height: u.height,
            })
        }
        Req::PublishDataTrackRequest(r) => {
            let token = cache.store(Echo::PublishDataTrack(r.clone()));
            Inbound::Forward(SessionRequest::PublishDataTrack(PublishDataTrack {
                token,
                pub_handle: r.pub_handle,
                name: r.name,
                encryption: convert::encryption_from_proto(r.encryption),
                frame_encoding: r
                    .frame_encoding
                    .as_ref()
                    .map(convert::data_track_frame_encoding_from_proto),
                schema: r.schema.as_ref().map(convert::data_track_schema_from_proto),
            }))
        }
        Req::UnpublishDataTrackRequest(r) => {
            let token = cache.store(Echo::UnpublishDataTrack(r));
            Inbound::Forward(SessionRequest::UnpublishDataTrack {
                token,
                pub_handle: r.pub_handle,
            })
        }
        Req::UpdateDataSubscription(r) => Inbound::Forward(SessionRequest::DataSubscription(
            r.updates
                .into_iter()
                .map(|update| DataTrackSubscriptionUpdate {
                    track_sid: TrackSid(update.track_sid),
                    subscribe: update.subscribe,
                    target_fps: update.options.and_then(|options| options.target_fps),
                })
                .collect(),
        )),
        Req::StoreDataBlobRequest(r) => Inbound::Forward(SessionRequest::StoreDataBlob {
            request_id: r.request_id,
            blob: r.blob.as_ref().map(blob_from_proto),
        }),
        Req::GetDataBlobRequest(r) => Inbound::Forward(SessionRequest::GetDataBlob {
            request_id: r.request_id,
            participant_identity: Identity(r.participant_identity),
            key: r.key.as_ref().map(blob_key_from_proto),
        }),
    }
}

fn reason_to_proto(o: RequestOutcome) -> i32 {
    use pb::request_response::Reason as R;
    (match o {
        RequestOutcome::Ok => R::Ok,
        RequestOutcome::NotFound => R::NotFound,
        RequestOutcome::NotAllowed => R::NotAllowed,
        RequestOutcome::LimitExceeded => R::LimitExceeded,
        RequestOutcome::Queued => R::Queued,
        RequestOutcome::UnsupportedType => R::UnsupportedType,
        RequestOutcome::UnclassifiedError => R::UnclassifiedError,
        RequestOutcome::InvalidHandle => R::InvalidHandle,
        RequestOutcome::InvalidName => R::InvalidName,
        RequestOutcome::DuplicateHandle => R::DuplicateHandle,
        RequestOutcome::DuplicateName => R::DuplicateName,
        RequestOutcome::InvalidRequest => R::InvalidRequest,
    }) as i32
}

fn session_description(
    kind: &str,
    sdp: String,
    id: u32,
    mids: std::collections::BTreeMap<String, String>,
) -> pb::SessionDescription {
    pb::SessionDescription {
        r#type: kind.to_string(),
        sdp,
        id,
        mid_to_track_id: mids.into_iter().collect::<HashMap<_, _>>(),
    }
}

/// Context the encoder needs that the engine event does not carry.
pub struct EncodeCtx<'a> {
    pub client: &'a ClientMeta,
}

/// Map an engine signal to the wire. `Join`, `Reconnect` and `ClaimsChanged` are
/// composed by the session (they need ICE servers / tokens) and return `None`.
pub fn encode_signal(
    ev: SignalEvent,
    ctx: &EncodeCtx<'_>,
    cache: &mut RequestCache,
) -> Option<pb::SignalResponse> {
    let m = match ev {
        SignalEvent::Join(_) | SignalEvent::Reconnect(_) | SignalEvent::ClaimsChanged(_) => {
            return None;
        }
        SignalEvent::Offer {
            sdp,
            id,
            mid_to_track_id,
        } => Resp::Offer(session_description("offer", sdp, id, mid_to_track_id)),
        SignalEvent::Answer {
            sdp,
            id,
            mid_to_track_id,
        } => Resp::Answer(session_description("answer", sdp, id, mid_to_track_id)),
        SignalEvent::Trickle {
            candidate,
            target,
            last,
        } => {
            let json = serde_json::to_string(&CandidateJson {
                candidate: candidate.candidate,
                sdp_mid: candidate.sdp_mid,
                sdp_mline_index: candidate.sdp_mline_index,
                username_fragment: candidate.username_fragment,
            })
            .unwrap_or_default();
            Resp::Trickle(pb::TrickleRequest {
                candidate_init: json,
                target: target_to_proto(target),
                r#final: last,
            })
        }
        SignalEvent::ParticipantUpdate(infos) => Resp::Update(pb::ParticipantUpdate {
            participants: infos
                .iter()
                .map(convert::participant_info_to_proto)
                .collect(),
        }),
        SignalEvent::TrackPublished { cid, track } => {
            Resp::TrackPublished(pb::TrackPublishedResponse {
                cid,
                track: Some(convert::track_info_to_proto(&track)),
            })
        }
        SignalEvent::DataBlobStored { request_id, key } => {
            Resp::StoreDataBlobResponse(pb::StoreDataBlobResponse {
                request_id,
                key: Some(blob_key_to_proto(&key)),
            })
        }
        SignalEvent::DataBlobRetrieved { request_id, blob } => {
            Resp::GetDataBlobResponse(pb::GetDataBlobResponse {
                request_id,
                blob: Some(pb::DataBlob {
                    key: blob.key.as_ref().map(blob_key_to_proto),
                    contents: blob.contents,
                }),
            })
        }
        SignalEvent::DataTrackPublished(info) => {
            Resp::PublishDataTrackResponse(pb::PublishDataTrackResponse {
                info: Some(convert::data_track_info_to_proto(&info)),
            })
        }
        SignalEvent::DataTrackUnpublished(info) => {
            Resp::UnpublishDataTrackResponse(pb::UnpublishDataTrackResponse {
                info: Some(convert::data_track_info_to_proto(&info)),
            })
        }
        SignalEvent::DataTrackSubscriberHandles(handles) => {
            Resp::DataTrackSubscriberHandles(pb::DataTrackSubscriberHandles {
                sub_handles: handles
                    .into_iter()
                    .map(|(handle, track)| {
                        (
                            handle,
                            pb::data_track_subscriber_handles::PublishedDataTrack {
                                publisher_identity: track.publisher_identity.0,
                                publisher_sid: track.publisher_sid.0,
                                track_sid: track.track_sid.0,
                            },
                        )
                    })
                    .collect(),
            })
        }
        SignalEvent::TrackUnpublished { track_sid } => {
            Resp::TrackUnpublished(pb::TrackUnpublishedResponse {
                track_sid: track_sid.0,
            })
        }
        SignalEvent::Leave { reason, action } => {
            let reason = convert::disconnect_reason_to_proto(reason);
            let leave = if crate::profile::capabilities(ctx.client).regions_in_leave {
                pb::LeaveRequest {
                    can_reconnect: false,
                    reason,
                    action: match action {
                        LeaveAction::Disconnect => pb::leave_request::Action::Disconnect,
                        LeaveAction::Resume => pb::leave_request::Action::Resume,
                        LeaveAction::Reconnect => pb::leave_request::Action::Reconnect,
                    } as i32,
                    regions: None,
                }
            } else {
                pb::LeaveRequest {
                    can_reconnect: action != LeaveAction::Disconnect,
                    reason,
                    action: 0,
                    regions: None,
                }
            };
            Resp::Leave(leave)
        }
        SignalEvent::Mute { track_sid, muted } => Resp::Mute(pb::MuteTrackRequest {
            sid: track_sid.0,
            muted,
        }),
        SignalEvent::SpeakersChanged(s) => Resp::SpeakersChanged(pb::SpeakersChanged {
            speakers: s.iter().map(convert::speaker_info_to_proto).collect(),
        }),
        SignalEvent::RoomUpdate(room) => Resp::RoomUpdate(pb::RoomUpdate {
            room: Some(convert::room_info_to_proto(&room)),
        }),
        SignalEvent::ConnectionQuality(q) => Resp::ConnectionQuality(pb::ConnectionQualityUpdate {
            updates: q
                .iter()
                .map(convert::connection_quality_info_to_proto)
                .collect(),
        }),
        SignalEvent::StreamState(s) => Resp::StreamStateUpdate(pb::StreamStateUpdate {
            stream_states: s.iter().map(convert::stream_state_info_to_proto).collect(),
        }),
        SignalEvent::SubscribedQuality { track_sid, codecs } => {
            let subscribed_codecs: Vec<pb::SubscribedCodec> = codecs
                .iter()
                .map(convert::subscribed_codec_to_proto)
                .collect();
            Resp::SubscribedQualityUpdate(pb::SubscribedQualityUpdate {
                track_sid: track_sid.0,
                subscribed_qualities: subscribed_codecs
                    .first()
                    .map(|c| c.qualities.clone())
                    .unwrap_or_default(),
                subscribed_codecs,
            })
        }
        SignalEvent::SubscriptionPermissionUpdate {
            participant_sid,
            track_sid,
            allowed,
        } => Resp::SubscriptionPermissionUpdate(pb::SubscriptionPermissionUpdate {
            participant_sid: participant_sid.0,
            track_sid: track_sid.0,
            allowed,
        }),
        SignalEvent::SubscriptionResponse { track_sid, error } => {
            Resp::SubscriptionResponse(pb::SubscriptionResponse {
                track_sid: track_sid.0,
                err: convert::subscription_error_to_proto(error),
            })
        }
        SignalEvent::RequestResponse {
            token,
            request_id,
            outcome,
            message,
        } => Resp::RequestResponse(pb::RequestResponse {
            request_id,
            reason: reason_to_proto(outcome),
            message,
            request: cache.take(token),
        }),
        SignalEvent::OwnedMetadataRequestResponse {
            request,
            outcome,
            message,
        } => {
            let request = *request;
            Resp::RequestResponse(pb::RequestResponse {
                request_id: request.request_id,
                reason: reason_to_proto(outcome),
                message,
                request: Some(pb::request_response::Request::UpdateMetadata(
                    pb::UpdateParticipantMetadata {
                        request_id: request.request_id,
                        name: request.name,
                        metadata: request.metadata,
                        attributes: request.attributes.into_iter().collect(),
                    },
                )),
            })
        }
        SignalEvent::TrackSubscribed { track_sid } => Resp::TrackSubscribed(pb::TrackSubscribed {
            track_sid: track_sid.0,
        }),
        SignalEvent::MediaSectionsRequirement { audios, videos } => {
            Resp::MediaSectionsRequirement(pb::MediaSectionsRequirement {
                num_audios: audios,
                num_videos: videos,
            })
        }
    };
    Some(response(m))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ping_answers_in_milliseconds() {
        let mut cache = RequestCache::default();
        let req = pb::SignalRequest {
            message: Some(Req::Ping(1)),
        };
        match decode_request(req, &mut cache) {
            Inbound::ForwardAndReply(
                request,
                pb::SignalResponse {
                    message: Some(Resp::Pong(ms)),
                },
            ) => {
                assert!(matches!(*request, SessionRequest::SignalActivity));
                assert!(ms > 1_600_000_000_000 && ms < 10_000_000_000_000);
            }
            _ => panic!("expected pong"),
        }
    }

    #[test]
    fn bad_trickle_json_is_unclassified_error() {
        let mut cache = RequestCache::default();
        let req = pb::SignalRequest {
            message: Some(Req::Trickle(pb::TrickleRequest {
                candidate_init: "{".into(),
                target: 0,
                r#final: false,
            })),
        };
        match decode_request(req, &mut cache) {
            Inbound::ForwardAndReply(
                request,
                pb::SignalResponse {
                    message: Some(Resp::RequestResponse(r)),
                },
            ) => {
                assert!(matches!(*request, SessionRequest::SignalActivity));
                assert_eq!(
                    r.reason,
                    pb::request_response::Reason::UnclassifiedError as i32
                );
                assert!(matches!(
                    r.request,
                    Some(pb::request_response::Request::Trickle(_))
                ));
            }
            _ => panic!("expected request response"),
        }
    }

    #[test]
    fn old_clients_get_can_reconnect() {
        let client = ClientMeta {
            protocol: 12,
            ..Default::default()
        };
        let mut cache = RequestCache::default();
        let ev = SignalEvent::Leave {
            reason: DisconnectReason::StateMismatch,
            action: LeaveAction::Reconnect,
        };
        match encode_signal(ev, &EncodeCtx { client: &client }, &mut cache).and_then(|r| r.message)
        {
            Some(Resp::Leave(l)) => {
                assert!(l.can_reconnect);
                assert_eq!(l.action, 0);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn trickle_round_trip_json_shape() {
        let client = ClientMeta::default();
        let mut cache = RequestCache::default();
        let ev = SignalEvent::Trickle {
            candidate: CandidateInit {
                candidate: "candidate:1 1 udp 2130706431 1.2.3.4 7882 typ host".into(),
                sdp_mid: Some("0".into()),
                sdp_mline_index: Some(0),
                username_fragment: None,
            },
            target: SignalTarget::Subscriber,
            last: true,
        };
        match encode_signal(ev, &EncodeCtx { client: &client }, &mut cache).and_then(|r| r.message)
        {
            Some(Resp::Trickle(t)) => {
                assert!(t.r#final);
                assert!(t.candidate_init.contains("\"sdpMid\":\"0\""));
                assert!(t.candidate_init.contains("\"usernameFragment\":null"));
            }
            _ => panic!(),
        }
    }
}

#[cfg(test)]
mod profile_leave_tests {
    use super::*;
    #[test]
    fn region_leave_profile_boundary_uses_actual_encoded_wire_semantics() {
        for protocol in [12, 13] {
            let client = ClientMeta {
                protocol,
                ..Default::default()
            };
            let ev = SignalEvent::Leave {
                reason: DisconnectReason::StateMismatch,
                action: LeaveAction::Reconnect,
            };
            let response = encode_signal(
                ev,
                &EncodeCtx { client: &client },
                &mut RequestCache::default(),
            )
            .unwrap();
            match response.message.unwrap() {
                Resp::Leave(leave) if protocol == 12 => {
                    assert!(leave.can_reconnect);
                    assert_eq!(leave.action, pb::leave_request::Action::Disconnect as i32);
                }
                Resp::Leave(leave) => {
                    assert!(!leave.can_reconnect);
                    assert_eq!(leave.action, pb::leave_request::Action::Reconnect as i32);
                }
                _ => panic!("leave must encode as an actual LeaveRequest"),
            }
        }
    }

    #[test]
    fn data_track_requests_decode_real_wire_and_echo_original_errors() {
        use prost::Message;
        let original = pb::PublishDataTrackRequest {
            pub_handle: 65535,
            name: "位置".into(),
            encryption: pb::encryption::Type::Gcm as i32,
            frame_encoding: Some(pb::DataTrackFrameEncoding {
                value: Some(pb::data_track_frame_encoding::Value::WellKnown(934)),
            }),
            schema: Some(pb::DataTrackSchemaId {
                name: "schema".into(),
                encoding: Some(pb::DataTrackSchemaEncoding {
                    value: Some(pb::data_track_schema_encoding::Value::Custom(
                        "schema-encoding".into(),
                    )),
                }),
            }),
        };
        let wire = pb::SignalRequest {
            message: Some(Req::PublishDataTrackRequest(original.clone())),
        }
        .encode_to_vec();
        let mut cache = RequestCache::default();
        let Inbound::Forward(SessionRequest::PublishDataTrack(request)) = decode_request(
            pb::SignalRequest::decode(wire.as_slice()).unwrap(),
            &mut cache,
        ) else {
            panic!("publish must reach room")
        };
        assert_eq!(request.pub_handle, 65535);
        assert_eq!(
            request.frame_encoding,
            Some(DataTrackEncoding::WellKnown(934))
        );
        assert_eq!(
            request.schema.as_ref().unwrap().encoding,
            Some(DataTrackEncoding::Custom("schema-encoding".into()))
        );
        let client = ClientMeta::default();
        let reply = encode_signal(
            SignalEvent::RequestResponse {
                token: request.token,
                request_id: 0,
                outcome: RequestOutcome::DuplicateHandle,
                message: "a data track with same handle already exists".into(),
            },
            &EncodeCtx { client: &client },
            &mut cache,
        )
        .unwrap();
        let decoded = pb::SignalResponse::decode(reply.encode_to_vec().as_slice()).unwrap();
        let Some(Resp::RequestResponse(response)) = decoded.message else {
            panic!("expected explicit original request error")
        };
        assert_eq!(
            response.reason,
            pb::request_response::Reason::DuplicateHandle as i32
        );
        assert_eq!(
            response.request,
            Some(pb::request_response::Request::PublishDataTrack(original))
        );
        let Inbound::Forward(SessionRequest::UnpublishDataTrack { pub_handle, .. }) =
            decode_request(
                pb::SignalRequest {
                    message: Some(Req::UnpublishDataTrackRequest(
                        pb::UnpublishDataTrackRequest { pub_handle: 131071 },
                    )),
                },
                &mut cache,
            )
        else {
            panic!("unpublish must reach room")
        };
        assert_eq!(
            pub_handle, 131071,
            "narrowing belongs to pinned core lookup, not adapter"
        );
        let Inbound::Forward(SessionRequest::DataSubscription(updates)) = decode_request(
            pb::SignalRequest {
                message: Some(Req::UpdateDataSubscription(pb::UpdateDataSubscription {
                    updates: vec![
                        pb::update_data_subscription::Update {
                            track_sid: "DTR_x".into(),
                            subscribe: true,
                            options: Some(pb::DataTrackSubscriptionOptions {
                                target_fps: Some(0),
                            }),
                        },
                        pb::update_data_subscription::Update {
                            track_sid: "DTR_y".into(),
                            subscribe: false,
                            options: None,
                        },
                    ],
                })),
            },
            &mut cache,
        ) else {
            panic!("subscription must reach room")
        };
        assert_eq!(updates[0].target_fps, Some(0));
        assert!(!updates[1].subscribe);
    }

    #[test]
    fn data_track_success_responses_and_handle_maps_have_real_wire_variants() {
        use prost::Message;
        let track = DataTrackInfo {
            sid: TrackSid::from("DTR_x"),
            pub_handle: 7,
            name: "servo".into(),
            encryption: EncryptionType::None,
            frame_encoding: None,
            schema: None,
        };
        let client = ClientMeta::default();
        let mut cache = RequestCache::default();
        for (event, published) in [
            (SignalEvent::DataTrackPublished(track.clone()), true),
            (SignalEvent::DataTrackUnpublished(track.clone()), false),
        ] {
            let reply = encode_signal(event, &EncodeCtx { client: &client }, &mut cache).unwrap();
            let reply = pb::SignalResponse::decode(reply.encode_to_vec().as_slice()).unwrap();
            match reply.message {
                Some(Resp::PublishDataTrackResponse(response)) if published => assert_eq!(
                    response.info.unwrap(),
                    convert::data_track_info_to_proto(&track)
                ),
                Some(Resp::UnpublishDataTrackResponse(response)) if !published => assert_eq!(
                    response.info.unwrap(),
                    convert::data_track_info_to_proto(&track)
                ),
                other => panic!("wrong success response {other:?}"),
            }
        }
        let handles = std::collections::BTreeMap::from([(
            65535,
            DataTrackHandleInfo {
                publisher_identity: Identity::from("publisher"),
                publisher_sid: ParticipantSid::from("PA_x"),
                track_sid: track.sid,
            },
        )]);
        let reply = encode_signal(
            SignalEvent::DataTrackSubscriberHandles(handles),
            &EncodeCtx { client: &client },
            &mut cache,
        )
        .unwrap();
        let Some(Resp::DataTrackSubscriberHandles(reply)) = reply.message else {
            panic!("missing handle mapping")
        };
        assert_eq!(reply.sub_handles[&65535].track_sid, "DTR_x");
        assert_eq!(reply.sub_handles[&65535].publisher_sid, "PA_x");
    }

    #[test]
    fn sync_state_preserves_missing_data_track_info_as_invalid_resume_inventory() {
        let mut cache = RequestCache::default();
        let request = pb::SignalRequest {
            message: Some(Req::SyncState(pb::SyncState {
                publish_data_tracks: vec![
                    pb::PublishDataTrackResponse { info: None },
                    pb::PublishDataTrackResponse {
                        info: Some(pb::DataTrackInfo {
                            sid: "DTR_known".into(),
                            ..Default::default()
                        }),
                    },
                ],
                ..Default::default()
            })),
        };
        let Inbound::Forward(SessionRequest::SyncState(state)) =
            decode_request(request, &mut cache)
        else {
            panic!("sync must reach room")
        };
        assert_eq!(
            state.publish_data_tracks,
            vec![TrackSid::from(""), TrackSid::from("DTR_known")]
        );
    }
}

pub(super) fn blob_key_from_proto(key: &pb::DataBlobKey) -> DataBlobKey {
    match key.key.as_ref() {
        None => DataBlobKey::unset(),
        Some(pb::data_blob_key::Key::Generic(value)) => DataBlobKey::generic(value.clone()),
        Some(pb::data_blob_key::Key::SchemaId(schema)) => {
            DataBlobKey::schema(convert::data_track_schema_from_proto(schema))
        }
    }
}
fn blob_key_to_proto(key: &DataBlobKey) -> pb::DataBlobKey {
    pb::DataBlobKey {
        key: match &key.value {
            DataBlobKeyValue::Unset => None,
            DataBlobKeyValue::Generic(value) => {
                Some(pb::data_blob_key::Key::Generic(value.clone()))
            }
            DataBlobKeyValue::Schema(schema) => Some(pb::data_blob_key::Key::SchemaId(
                convert::data_track_schema_to_proto(schema),
            )),
        },
    }
}
fn blob_from_proto(blob: &pb::DataBlob) -> DataBlob {
    DataBlob {
        key: blob.key.as_ref().map(blob_key_from_proto),
        contents: blob.contents.clone(),
        wire: None,
    }
}

#[cfg(test)]
mod participant_blob_tests {
    use super::*;
    use prost::Message;
    #[test]
    fn blob_known_schema_presence_and_request_ids_reach_engine_and_real_response_variants() {
        let mut cache = RequestCache::default();
        let key = pb::DataBlobKey {
            key: Some(pb::data_blob_key::Key::SchemaId(pb::DataTrackSchemaId {
                name: "schema".into(),
                encoding: Some(pb::DataTrackSchemaEncoding {
                    value: Some(pb::data_track_schema_encoding::Value::WellKnown(934)),
                }),
            })),
        };
        let original = pb::DataBlob {
            key: Some(key.clone()),
            contents: vec![0, 255, 1],
        };
        let Inbound::Forward(SessionRequest::StoreDataBlob {
            request_id: 71,
            blob: Some(blob),
        }) = decode_request(
            pb::SignalRequest {
                message: Some(Req::StoreDataBlobRequest(pb::StoreDataBlobRequest {
                    request_id: 71,
                    blob: Some(original.clone()),
                })),
            },
            &mut cache,
        )
        else {
            panic!("must forward store, not Unsupported")
        };
        let client = ClientMeta::default();
        let reply = encode_signal(
            SignalEvent::DataBlobRetrieved {
                request_id: 72,
                blob,
            },
            &EncodeCtx { client: &client },
            &mut cache,
        )
        .unwrap();
        assert!(
            matches!(reply.message, Some(Resp::GetDataBlobResponse(pb::GetDataBlobResponse { request_id: 72, blob: Some(actual) })) if actual == original)
        );
        let Inbound::Forward(SessionRequest::GetDataBlob {
            request_id: 73,
            participant_identity,
            key: Some(actual),
        }) = decode_request(
            pb::SignalRequest {
                message: Some(Req::GetDataBlobRequest(pb::GetDataBlobRequest {
                    request_id: 73,
                    participant_identity: "publisher".into(),
                    key: Some(key.clone()),
                })),
            },
            &mut cache,
        )
        else {
            panic!("must forward get")
        };
        assert_eq!(participant_identity, Identity("publisher".into()));
        let reply = encode_signal(
            SignalEvent::DataBlobStored {
                request_id: 74,
                key: actual,
            },
            &EncodeCtx { client: &client },
            &mut cache,
        )
        .unwrap();
        assert!(
            matches!(reply.message, Some(Resp::StoreDataBlobResponse(pb::StoreDataBlobResponse { request_id: 74, key: Some(actual) })) if actual == key)
        );
    }

    #[test]
    fn owner_metadata_failure_encodes_actual_update_without_a_cache_token() {
        let original = pb::UpdateParticipantMetadata {
            request_id: 0,
            name: String::new(),
            metadata: "rejected-metadata".into(),
            attributes: [("x".into(), "é".into()), ("deleted".into(), String::new())].into(),
        };
        let mut cache = RequestCache::default();
        let client = ClientMeta::default();
        let reply = encode_signal(
            SignalEvent::OwnedMetadataRequestResponse {
                request: Box::new(rtc_core::session::MetadataRequestEcho {
                    request_id: original.request_id,
                    name: original.name.clone(),
                    metadata: original.metadata.clone(),
                    attributes: original.attributes.clone().into_iter().collect(),
                }),
                outcome: RequestOutcome::LimitExceeded,
                message: "exceeds name length limit".into(),
            },
            &EncodeCtx { client: &client },
            &mut cache,
        )
        .unwrap();
        let decoded = pb::SignalResponse::decode(reply.encode_to_vec().as_slice()).unwrap();
        let Some(Resp::RequestResponse(actual)) = decoded.message else {
            panic!("real request response");
        };
        assert_eq!(actual.request_id, 0);
        assert_eq!(
            actual.reason,
            pb::request_response::Reason::LimitExceeded as i32
        );
        assert_eq!(actual.message, "exceeds name length limit");
        assert_eq!(
            actual.request,
            Some(pb::request_response::Request::UpdateMetadata(original))
        );
    }
}

#[cfg(test)]
mod ping_req_functional_regression {
    use super::*;

    #[test]
    fn ping_request_preserves_echo_and_delivers_only_positive_uint32_rtt() {
        for (rtt, expected) in [
            (-1, None),
            (0, None),
            (95, Some(95)),
            (i64::MAX, Some(u32::MAX)),
            (4294967296, Some(0)),
            (4294967297, Some(1)),
        ] {
            let mut cache = RequestCache::default();
            let inbound = decode_request(
                pb::SignalRequest {
                    message: Some(Req::PingReq(pb::Ping {
                        timestamp: 1234,
                        rtt,
                    })),
                },
                &mut cache,
            );
            let response = match (expected, inbound) {
                (None, Inbound::ForwardAndReply(request, response)) => {
                    assert!(matches!(*request, SessionRequest::SignalActivity));
                    response
                }
                (Some(expected), Inbound::ForwardAndReply(request, response)) => {
                    assert!(
                        matches!(*request, SessionRequest::SignalRtt(actual) if actual == expected)
                    );
                    response
                }
                _ => panic!("positive RTT alone is forwarded; every ping still receives pong"),
            };
            assert!(matches!(
                response.message,
                Some(Resp::PongResp(pb::Pong {
                    last_ping_timestamp: 1234,
                    ..
                }))
            ));
        }
    }
}

#[cfg(test)]
mod rtt_original_conversion_observer {
    use super::*;

    #[test]
    #[ignore = "requires independently executed Go8 uint32 conversion observations"]
    fn actual_original_go8_positive_rtt_conversion() {
        let data =
            std::fs::read(std::env::var_os("RTT_CAST_GO_RESULTS").expect("actual Go8 output"))
                .unwrap();
        let rows: Vec<serde_json::Value> = data
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice(line).unwrap())
            .collect();
        let inputs = [-1, 0, 1, 95, 4294967295, 4294967296, 4294967297, i64::MAX];
        assert_eq!(rows.len(), inputs.len());
        for (row, input) in rows.into_iter().zip(inputs) {
            assert_eq!(row["input"].as_i64(), Some(input));
            let req = pb::SignalRequest {
                message: Some(Req::PingReq(pb::Ping {
                    timestamp: 4567,
                    rtt: input,
                })),
            };
            let mut cache = RequestCache::default();
            let response = match (
                row["forward"].as_bool().unwrap(),
                decode_request(req, &mut cache),
            ) {
                (false, Inbound::ForwardAndReply(request, response)) => {
                    assert!(matches!(*request, SessionRequest::SignalActivity));
                    response
                }
                (true, Inbound::ForwardAndReply(request, response)) => {
                    let SessionRequest::SignalRtt(actual) = *request else {
                        panic!("original handler forwards RTT")
                    };
                    assert_eq!(u64::from(actual), row["converted"].as_u64().unwrap());
                    response
                }
                _ => panic!("actual original Go conversion/forwarding difference"),
            };
            assert!(matches!(
                response.message,
                Some(Resp::PongResp(pb::Pong {
                    last_ping_timestamp: 4567,
                    ..
                }))
            ));
        }
    }
}
