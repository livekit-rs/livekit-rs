//! Parsed native offer/answer and real candidate event consumers; no socket claim.
use super::*;
use crate::net::LocalCandidate;
use rtc_core::media_api::MediaCommand;
use rtc_core::session::CandidateInit;
use rtc_core::transport_fallback::IcePreference;
use str0m::Candidate;

#[test]
fn source_ice_preference_filters_actual_both_pc_sdp_and_trickle_and_remote_admission() {
    for preference in [IcePreference::Any, IcePreference::Tcp, IcePreference::Tls] {
        for role in [PcRole::Publisher, PcRole::Subscriber] {
            let now = Instant::now();
            let mut engine = engine(now);
            let tid = TransportId::next();
            let udp = "192.0.2.10:9000".parse().unwrap();
            let tcp = "192.0.2.10:9001".parse().unwrap();
            engine.create_transport(
                TransportSpec {
                    ice_preference: preference,
                    one_shot: false,
                    id: tid,
                    participant: ParticipantSid::from("PA_wire_fallback"),
                    role,
                    ice_lite: true,
                    server_data_channels: role == PcRole::Subscriber,
                    enable_data_tracks: false,
                    primary: true,
                    client_supports_red: false,
                    codec_switch_requires_renegotiation: false,
                    disable_playout_delay: false,
                    allow_pause: true,
                },
                IceCredentials::random(),
                vec![
                    LocalCandidate {
                        proto: Proto::Udp,
                        addr: udp,
                        base: udp,
                    },
                    LocalCandidate {
                        proto: Proto::Tcp,
                        addr: tcp,
                        base: tcp,
                    },
                ],
                now,
            );
            let mut peer = client(now);
            peer.add_local_candidate(
                Candidate::host("192.0.2.11:10000".parse().unwrap(), "udp").unwrap(),
            );
            let mut events = Vec::new();
            if role == PcRole::Publisher {
                events = offer(&mut engine, &mut peer, tid, now, |api| {
                    api.add_media(MediaKind::Audio, Direction::SendOnly, None, None, None);
                });
            } else {
                engine.drain_events(&mut events);
                let (id, sdp) = events
                    .iter()
                    .find_map(|event| match event {
                        MediaEvent::LocalDescription {
                            kind: SdpType::Offer,
                            id,
                            sdp,
                            ..
                        } => Some((*id, sdp.clone())),
                        _ => None,
                    })
                    .expect("actual initial subscriber offer");
                let answer = peer
                    .sdp_api()
                    .accept_offer(str0m::change::SdpOffer::from_sdp_string(&sdp).unwrap())
                    .unwrap();
                engine.on_remote_description(tid, SdpType::Answer, answer.to_sdp_string(), id, now);
            }
            let sdp = events
                .iter()
                .find_map(|event| match event {
                    MediaEvent::LocalDescription { sdp, .. } => Some(sdp),
                    _ => None,
                })
                .unwrap();
            let candidates = sdp
                .lines()
                .filter(|line| line.starts_with("a=candidate:"))
                .collect::<Vec<_>>();
            assert!(
                !candidates.is_empty(),
                "validate actual SDP, not an empty-filter helper"
            );
            let wire = events
                .iter()
                .filter_map(|event| match event {
                    MediaEvent::LocalCandidate {
                        candidate, last, ..
                    } => Some((&candidate.candidate, *last)),
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert!(!wire.is_empty());
            assert!(wire.last().unwrap().1);
            if preference == IcePreference::Tcp {
                assert!(candidates.iter().all(|line| {
                    line.split_whitespace()
                        .nth(2)
                        .unwrap()
                        .eq_ignore_ascii_case("tcp")
                }));
                assert!(wire.iter().all(|(line, _)| {
                    line.split_whitespace()
                        .nth(2)
                        .unwrap()
                        .eq_ignore_ascii_case("tcp")
                }));
            } else {
                assert!(candidates.iter().any(|line| {
                    line.split_whitespace()
                        .nth(2)
                        .unwrap()
                        .eq_ignore_ascii_case("udp")
                }));
                assert!(wire.iter().any(|(line, _)| {
                    line.split_whitespace()
                        .nth(2)
                        .unwrap()
                        .eq_ignore_ascii_case("udp")
                }));
            }
            engine.handle_command(MediaCommand::CloseTransport(tid), now);
            assert!(engine.transports.is_empty());
        }
        let now = Instant::now();
        let mut engine = engine(now);
        let tid = transport(&mut engine, PcRole::Publisher, now);
        engine.handle_command(
            MediaCommand::SetIcePreference {
                transport: tid,
                preference,
            },
            now,
        );
        for raw in [
            "candidate:udp 1 udp 123 192.0.2.11 5000 typ host",
            "candidate:tcp 1 tcp 122 192.0.2.11 5001 typ host tcptype active",
        ] {
            engine.on_remote_candidate(
                tid,
                CandidateInit {
                    candidate: raw.into(),
                    sdp_mid: None,
                    sdp_mline_index: None,
                    username_fragment: None,
                },
            );
        }
        let queued = &engine.transports[&tid].queued_remote_candidates;
        assert_eq!(
            queued.len(),
            if preference == IcePreference::Tcp {
                1
            } else {
                2
            }
        );
        if preference == IcePreference::Tcp {
            assert_eq!(queued[0].proto(), str0m::net::Protocol::Tcp);
        }
        engine.handle_command(MediaCommand::CloseTransport(tid), now);
        assert!(engine.transports.is_empty());
    }
}

#[test]
fn actual_remote_sdp_starts_one_shot_tcp_checking_timer_and_close_releases_it() {
    for (rtt, delay) in [
        (0, Some(5000)),
        (624, Some(5000)),
        (625, Some(5000)),
        (999, Some(7992)),
        (1000, None),
    ] {
        let now = Instant::now();
        let mut engine = engine(now);
        let tid = transport(&mut engine, PcRole::Publisher, now);
        let participant = engine.transports[&tid].spec.participant.clone();
        engine.handle_command(
            MediaCommand::SetIcePreference {
                transport: tid,
                preference: IcePreference::Tcp,
            },
            now,
        );
        engine.handle_command(
            MediaCommand::SetSignalingRtt {
                participant,
                rtt_ms: rtt,
            },
            now,
        );
        engine.update_tcp_ice_timers(now);
        assert_eq!(
            engine.transports[&tid].tcp_ice_deadline, None,
            "no timer before real remote ICE credentials"
        );
        let mut peer = client(now);
        peer.add_local_candidate(
            Candidate::host("192.0.2.11:10000".parse().unwrap(), "tcp").unwrap(),
        );
        offer(&mut engine, &mut peer, tid, now, |api| {
            api.add_media(MediaKind::Audio, Direction::SendOnly, None, None, None);
        });
        engine.update_tcp_ice_timers(now);
        assert_eq!(
            engine.transports[&tid].rtc.ice_connection_state(),
            str0m::IceConnectionState::Checking
        );
        assert_eq!(
            engine.transports[&tid].tcp_ice_deadline,
            delay.map(|ms| now + Duration::from_millis(ms))
        );
        engine.update_tcp_ice_timers(now + Duration::from_secs(1));
        assert_eq!(
            engine.transports[&tid].tcp_ice_deadline,
            delay.map(|ms| now + Duration::from_millis(ms)),
            "subsequent cycles cannot slide the fixed first-checking deadline"
        );
        engine.handle_command(MediaCommand::CloseTransport(tid), now);
        assert!(engine.transports.is_empty());
    }
}
