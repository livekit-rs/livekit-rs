//! Real ICE/DTLS/SRTP public API, logical clock and explicit simulated NAT.
//! No OS/network/SDK performance or physical NAT deployment claim.
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use str0m::media::{Direction, Frequency, MediaKind, MediaTime};
use str0m::net::{Protocol, Receive};
use str0m::{Candidate, CandidateKind, Event, Input, Output, Rtc};
fn rtc(now: Instant, lite: bool) -> Rtc {
    Rtc::builder()
        .set_crypto_provider(rtc_crypto::provider().into())
        .set_dtls_cert(rtc_crypto::dtls_cert().unwrap())
        .set_ice_lite(lite)
        .build(now)
}
fn transfer(
    from: &mut Rtc,
    to: &mut Rtc,
    now: Instant,
    from_server: bool,
    route: (SocketAddr, SocketAddr, SocketAddr),
    decoded: &mut usize,
) {
    let (public, base, peer) = route;
    let mut due = 0;
    for _ in 0..4096 {
        match from.poll_output().unwrap() {
            Output::Transmit(packet) => {
                let (source, destination) = if from_server {
                    assert_eq!(
                        packet.source, base,
                        "native output must retain actual selected physical base, not public alias or another candidate"
                    );
                    assert_eq!(packet.destination, peer);
                    (public, peer)
                } else {
                    assert_eq!(packet.source, peer);
                    assert_eq!(packet.destination, public);
                    (peer, base)
                };
                to.handle_input(Input::Receive(
                    now,
                    Receive::new(Protocol::Udp, source, destination, &packet.contents).unwrap(),
                ))
                .unwrap();
            }
            Output::Event(Event::MediaData(data)) => {
                assert_eq!(data.data.as_ref(), [0xf8, 0xff, 0xfe]);
                *decoded += 1;
            }
            Output::Event(_) => {}
            Output::Timeout(t) if t <= now && due < 15 => {
                due += 1;
                from.handle_input(Input::Timeout(now)).unwrap();
            }
            Output::Timeout(_) => return,
        }
    }
    panic!("native public pair failed to settle in bounded work")
}
#[test]
fn mapped_host_keeps_two_physical_bases_and_authenticated_audio_on_either_selected_path() {
    let public: SocketAddr = "203.0.113.8:5000".parse().unwrap();
    let bases: [SocketAddr; 2] = [
        "10.0.0.2:5000".parse().unwrap(),
        "10.1.0.2:5000".parse().unwrap(),
    ];
    let peer: SocketAddr = "192.0.2.9:6000".parse().unwrap();
    for selected in bases {
        let start = Instant::now();
        let mut server = rtc(start, true);
        let mut client = rtc(start, false);
        let mut foundations = Vec::new();
        for base in bases {
            let candidate = Candidate::builder()
                .udp()
                .mapped_host(public, base)
                .build()
                .unwrap();
            assert_eq!(candidate.kind(), CandidateKind::Host);
            assert_eq!(candidate.addr(), public);
            assert_eq!(candidate.local(), base);
            let accepted = server
                .add_local_candidate(candidate)
                .expect("same public address with distinct physical base is not redundant");
            let sdp = accepted.to_sdp_string();
            assert!(sdp.contains("203.0.113.8 5000 typ host"));
            foundations.push(sdp.split_whitespace().next().unwrap().to_owned());
        }
        assert_ne!(
            foundations[0], foundations[1],
            "foundation retains different base IPs"
        );
        client.add_local_candidate(Candidate::host(peer, "udp").unwrap());
        let mut change = client.sdp_api();
        let mid = change.add_media(MediaKind::Audio, Direction::SendOnly, None, None, None);
        let (offer, pending) = change.apply().unwrap();
        let answer = server.sdp_api().accept_offer(offer).unwrap();
        client.sdp_api().accept_answer(pending, answer).unwrap();
        let mut decoded = 0;
        for tick in 0..1600u64 {
            let now = start + Duration::from_millis(tick);
            client.handle_input(Input::Timeout(now)).unwrap();
            server.handle_input(Input::Timeout(now)).unwrap();
            if tick == 1000 {
                assert!(
                    server.is_connected() && client.is_connected(),
                    "real ICE and DTLS must connect before publishing"
                );
            }
            if tick >= 1000 && tick % 20 == 0 {
                let writer = client.writer(mid).unwrap();
                let pt = writer
                    .payload_params()
                    .find(|p| p.spec().codec == str0m::format::Codec::Opus)
                    .unwrap()
                    .pt();
                writer
                    .write(
                        pt,
                        now,
                        MediaTime::new(tick * 48, Frequency::FORTY_EIGHT_KHZ),
                        vec![0xf8, 0xff, 0xfe],
                    )
                    .unwrap();
            }
            transfer(
                &mut client,
                &mut server,
                now,
                false,
                (public, selected, peer),
                &mut decoded,
            );
            transfer(
                &mut server,
                &mut client,
                now,
                true,
                (public, selected, peer),
                &mut decoded,
            );
        }
        assert!(
            decoded >= 10,
            "actual authenticated Opus frames must arrive through mapped host base"
        );
    }
}
