//! Real encrypted downstream video NACK → RTX serving, measured on the wire.
//!
//! The receiver-estimate congestion detector arms on *repeated* NACKs, and the Go reference
//! arms it from the retransmissions it actually served (`sequencer.getExtPacketMetas` →
//! `epm.nacked > 1` → `totalRepeatedNACKs`, then `downtrack.GetNackStats` → `nackTracker`).
//! That counter is therefore a property of the sender answering a NACK, not of the receiver
//! asking. This test pins the serving half of that chain on a real ICE/DTLS/SRTP video
//! session: the subscriber's NACK is answered out of the retransmission cache, the repair is
//! a real RTX packet (its own payload type) that reaches the subscriber, and the bytes are
//! attributable to the injected loss rather than to background traffic.
//!
//! The repeat half is bounded by the same gate. `sequencer.go:385` refuses a
//! retransmission for a sequence number until `min(ignoreRetransmission=100ms, 2*rtt)` has
//! passed since the last accepted one, and it seeds that deadline with the original
//! transmission (`lastNack: s.getRefTime(packetTime)`, "delay retransmissions after the
//! original transmission"). Loopback RTT measures 0 ms, which both sides replace with
//! `defaultRtt` 70 ms, so the window is 100 ms; this receiver's NACK register gives up on a
//! sequence after `MAX_NACKS = 5` reports at `NACK_MIN_INTERVAL = 33 ms`
//! (streams/register_nack.rs:9, session.rs:47). A single loss therefore gets one repair and
//! no repeat credit, which is what Go does with the same stimulus; a burst long enough to
//! outlive the 100 ms window is what arms `repeated_nacks`, and the last test pins that too.
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use str0m::format::Codec;
use str0m::media::{Direction, Frequency, MediaKind, MediaTime, Pt};
use str0m::net::{Protocol, Receive};
use str0m::rtp::RawPacket;
use str0m::{Candidate, Input, Output, Rtc};

const SERVER: &str = "127.0.0.1:9911";
const CLIENT: &str = "127.0.0.1:9912";
/// Virtual length of the session: ICE/DTLS, then the impaired media window.
const WARMUP_MS: u64 = 2000;
const MEDIA_MS: u64 = 8000;

fn rtc(now: Instant) -> Rtc {
    Rtc::builder()
        .set_crypto_provider(rtc_crypto::provider().into())
        .set_dtls_cert(rtc_crypto::dtls_cert().unwrap())
        .enable_raw_packets(true)
        // Production default of rtc.packet_buffer_size_video, which is also the
        // width of the retransmission meter's eligibility window.
        .set_send_buffer_video(500)
        .build(now)
}

struct Impair {
    drop_primaries: usize,
    drop_rtx: usize,
    dropped_primary: Vec<u16>,
    delivered_rtx: usize,
}

/// What the subscriber itself decrypted on the RTX payload type: the original sequence
/// number carried in the two-byte RTX header.
#[derive(Debug, Default)]
struct Repairs {
    osn: Vec<u16>,
}

/// Static properties of one wire leg: addressing plus the payload types that tell a
/// primary stream from its retransmission.
struct Wire {
    from_addr: SocketAddr,
    to_addr: SocketAddr,
    primary_pt: u8,
    rtx_pt: u8,
}

/// One direction of the wire. Only the server→client leg is impaired, like the
/// interop network proxy (which shapes downstream and passes upstream NACKs).
///
/// `now` is the single monotonic virtual clock both peers see. A peer's own timer deadline
/// advances it (never past `limit`) instead of being clamped to the current step: clamping
/// collapses a 33 ms NACK cadence into one instant, so every interval gate in the sender
/// would measure zero elapsed time.
fn pump(
    from: &mut Rtc,
    to: &mut Rtc,
    now: &mut Instant,
    limit: Instant,
    wire: &Wire,
    mut impair: Option<&mut Impair>,
    mut repairs: Option<&mut Repairs>,
) {
    let (primary_pt, rtx_pt) = (wire.primary_pt, wire.rtx_pt);
    for _ in 0..4096 {
        match from.poll_output().unwrap() {
            Output::Transmit(p) => {
                let bytes = &p.contents[..];
                let pt = bytes.get(1).map(|v| v & 0x7f).unwrap_or(0);
                let is_rtp = bytes.len() >= 12 && bytes[0] >> 6 == 2;
                if is_rtp
                    && pt == primary_pt
                    && let Some(i) = impair.as_mut()
                    && i.drop_primaries > 0
                {
                    i.drop_primaries -= 1;
                    i.dropped_primary
                        .push(u16::from_be_bytes([bytes[2], bytes[3]]));
                    continue;
                }
                if is_rtp
                    && pt == rtx_pt
                    && let Some(i) = impair.as_mut()
                {
                    if i.drop_rtx > 0 {
                        i.drop_rtx -= 1;
                        continue;
                    }
                    i.delivered_rtx += 1;
                }
                to.handle_input(Input::Receive(
                    *now,
                    Receive::new(Protocol::Udp, wire.from_addr, wire.to_addr, bytes).unwrap(),
                ))
                .unwrap();
            }
            Output::Event(e) => {
                if let Some(r) = repairs.as_mut()
                    && let Some(RawPacket::RtpRx(header, payload)) = e.as_raw_packet()
                    && header.payload_type == Pt::from(rtx_pt)
                    && payload.len() >= 2
                {
                    r.osn.push(u16::from_be_bytes([payload[0], payload[1]]));
                }
            }
            Output::Timeout(at) if at <= limit => {
                let at = at.max(*now);
                *now = at;
                from.handle_input(Input::Timeout(at)).unwrap();
            }
            Output::Timeout(_) => return,
        }
    }
    panic!("output work did not settle");
}

#[derive(Debug, Default)]
struct Outcome {
    connected: bool,
    nacks_received: u64,
    nacks_sent: u64,
    retransmit_packets_out: u64,
    packets_primary: u32,
    packets_rtx: u64,
    bytes_rtx: u64,
    repeated_nacks: u32,
    frames: u64,
    dropped_primary: Vec<u16>,
    delivered_rtx: usize,
    rx_rtx_osns: Vec<u16>,
}

/// Write 30 ms VP8 frames for `MEDIA_MS` and read back the sender's own counters.
/// `drop_primaries` primary packets once the session is `after_ms` old, then swallow the
/// first `drop_rtx` RTX datagrams so the receiver has to keep asking.
fn session(after_ms: u64, drop_primaries: usize, drop_rtx: usize) -> Outcome {
    let start = Instant::now();
    let server_addr: SocketAddr = SERVER.parse().unwrap();
    let client_addr: SocketAddr = CLIENT.parse().unwrap();
    let mut server = rtc(start);
    // Production installs this owner on every sending transport (engine.rs), not
    // only in the Remote/interceptor feedback modes.
    server.direct_api().set_remote_feedback_meter(true);
    let mut client = rtc(start);
    server.add_local_candidate(Candidate::host(server_addr, "udp").unwrap());
    client.add_local_candidate(Candidate::host(client_addr, "udp").unwrap());

    let mut change = server.sdp_api();
    let mid = change.add_media(MediaKind::Video, Direction::SendOnly, None, None, None);
    let (offer, pending) = change.apply().unwrap();
    let answer = client.sdp_api().accept_offer(offer).unwrap();
    server.sdp_api().accept_answer(pending, answer).unwrap();

    let params: Vec<_> = server
        .writer(mid)
        .expect("writer after negotiation")
        .payload_params()
        .cloned()
        .collect();
    let vp8 = params
        .iter()
        .find(|p| p.spec().codec == Codec::Vp8)
        .expect("subscriber accepts VP8");
    let primary_pt = *vp8.pt();
    let rtx_pt = *vp8.resend().expect("VP8 RTX payload type is negotiated");

    let mut impair = Impair {
        drop_primaries,
        drop_rtx,
        dropped_primary: Vec::new(),
        delivered_rtx: 0,
    };
    let mut outcome = Outcome::default();
    let mut repairs = Repairs::default();
    let mut now = start;
    let mut frames = 0u64;
    let mut next_frame = start + Duration::from_millis(WARMUP_MS);
    let mut connected = false;

    for ms in 0..(WARMUP_MS + MEDIA_MS) {
        let limit = start + Duration::from_millis(ms);
        if limit < now {
            continue;
        }
        if connected && limit >= next_frame {
            // A VP8 start-of-partition keyframe-ish dependency descriptor prefix; the
            // payload content is irrelevant, the counters under test are per packet.
            server
                .writer(mid)
                .unwrap()
                .write(
                    Pt::from(primary_pt),
                    limit,
                    MediaTime::new(frames * 2700, Frequency::NINETY_KHZ),
                    vec![0x10, 0, 0, 0x9d, 1, 0x2a, 0x80, 2, 0x68, 1],
                )
                .unwrap();
            frames += 1;
            next_frame = limit + Duration::from_millis(30);
        }
        let impairing = connected && ms >= WARMUP_MS.saturating_add(after_ms);
        pump(
            &mut server,
            &mut client,
            &mut now,
            limit,
            &Wire {
                from_addr: server_addr,
                to_addr: client_addr,
                primary_pt,
                rtx_pt,
            },
            impairing.then_some(&mut impair),
            None,
        );
        pump(
            &mut client,
            &mut server,
            &mut now,
            limit,
            &Wire {
                from_addr: client_addr,
                to_addr: server_addr,
                primary_pt,
                rtx_pt,
            },
            None,
            Some(&mut repairs),
        );
        if !connected && server.is_connected() && client.is_connected() {
            connected = true;
        }
        if !connected {
            continue;
        }
        let media = server.media_traffic_stats();
        outcome.nacks_received = media.nacks_received;
        outcome.nacks_sent = client.media_traffic_stats().nacks_sent;
        outcome.retransmit_packets_out = media.retransmit_packets_out;
        if let Some(s) = server
            .direct_api()
            .remote_feedback_stats()
            .iter()
            .find(|s| s.mid == mid)
        {
            outcome.packets_primary = s.packets_primary;
            outcome.packets_rtx = s.packets_rtx;
            outcome.bytes_rtx = s.bytes_rtx;
            outcome.repeated_nacks = s.repeated_nacks;
        }
    }
    outcome.frames = frames;
    outcome.connected = connected;
    outcome.dropped_primary = impair.dropped_primary;
    outcome.delivered_rtx = impair.delivered_rtx;
    outcome.rx_rtx_osns = repairs.osn;
    outcome
}

/// A clean run cannot produce retransmissions: it proves the impaired run's RTX
/// counters are attributable to the injected loss and not to background traffic.
#[test]
fn unimpaired_downstream_video_emits_no_rtx() {
    let clean = session(u64::MAX, 0, 0);
    eprintln!("downstream_nack_rtx clean: {clean:?}");
    assert!(clean.connected, "ICE/DTLS completed on loopback");
    assert!(clean.frames > 100, "video kept flowing: {}", clean.frames);
    assert_eq!(clean.nacks_received, 0, "nothing to NACK: {clean:?}");
    assert_eq!(clean.nacks_sent, 0, "{clean:?}");
    assert_eq!(clean.retransmit_packets_out, 0, "{clean:?}");
    assert_eq!(clean.packets_rtx, 0, "{clean:?}");
    assert_eq!(clean.bytes_rtx, 0, "{clean:?}");
    assert_eq!(clean.repeated_nacks, 0, "{clean:?}");
    assert!(clean.rx_rtx_osns.is_empty(), "{clean:?}");
    assert!(clean.packets_primary > 100, "{clean:?}");
}

/// Every lost primary is answered once out of the retransmission cache, as a real RTX
/// packet the subscriber decrypts and attributes back to the lost sequence number.
///
/// sequencer.go:385, seeded by sequencer.go:256: at the 70 ms default RTT the gate
/// requires 100 ms between two accepted retransmissions of one sequence number, while
/// this receiver stops asking a sequence after `MAX_NACKS = 5` reports spaced
/// `NACK_MIN_INTERVAL = 33 ms` apart. So one answer per loss is Go parity, not
/// under-serving: a loosened gate would serve the extra asks and flip the equality.
#[test]
fn downstream_video_nack_is_answered_once_per_loss() {
    let impaired = session(1000, 2, 0);
    eprintln!("downstream_nack_rtx gate: {impaired:?}");
    assert!(impaired.connected, "ICE/DTLS completed on loopback");
    assert_eq!(
        impaired.dropped_primary.len(),
        2,
        "two primary packets were lost: {impaired:?}"
    );
    assert!(
        impaired.nacks_received > 0,
        "the subscriber NACKed: {impaired:?}"
    );
    assert_eq!(
        impaired.nacks_received, impaired.nacks_sent,
        "every requested sequence number reached the SFU: {impaired:?}"
    );
    assert!(
        impaired.nacks_sent > impaired.dropped_primary.len() as u64,
        "the subscriber asked again for the same sequences: {impaired:?}"
    );
    assert_eq!(
        impaired.packets_rtx as usize,
        impaired.dropped_primary.len(),
        "each loss was answered exactly once inside the gate window: {impaired:?}"
    );
    assert_eq!(
        impaired.retransmit_packets_out as usize, impaired.packets_rtx as usize,
        "the RTX the sender counted is the RTX it put on the wire: {impaired:?}"
    );
    assert!(impaired.bytes_rtx > 0, "{impaired:?}");
    let mut expected = impaired.dropped_primary.clone();
    expected.sort_unstable();
    let mut actual = impaired.rx_rtx_osns.clone();
    actual.sort_unstable();
    assert_eq!(
        actual, expected,
        "the subscriber recovered each lost sequence number over RTX: {impaired:?}"
    );
    assert_eq!(
        impaired.repeated_nacks, 0,
        "no repeat was accepted inside the gate window: {impaired:?}"
    );
}

/// The `repeated_nacks` the downstream congestion detector consumes is counted from
/// retransmissions the SFU *served* a second time (downtrack.go:2494, `epm.nacked > 1`),
/// so a sustained loss burst that outlives the gate window is what arms it.
#[test]
fn downstream_video_sustained_loss_arms_the_repeated_nack_counter() {
    let impaired = session(1000, 4, 2);
    eprintln!("downstream_nack_rtx arming: {impaired:?}");
    assert!(impaired.connected, "ICE/DTLS completed on loopback");
    assert_eq!(impaired.dropped_primary.len(), 4, "{impaired:?}");
    assert!(
        impaired.packets_rtx as usize > impaired.dropped_primary.len(),
        "the gate window elapsed and a repeat was served: {impaired:?}"
    );
    assert!(
        impaired.repeated_nacks > 0,
        "a served repeat is a repeated NACK: {impaired:?}"
    );
    assert!(
        impaired.delivered_rtx > 0 && !impaired.rx_rtx_osns.is_empty(),
        "repairs still reached the subscriber while repeats accumulated: {impaired:?}"
    );
    assert!(
        impaired
            .rx_rtx_osns
            .iter()
            .all(|osn| impaired.dropped_primary.contains(osn)),
        "every subscriber-side repair belongs to an injected loss: {impaired:?}"
    );
    assert!(
        impaired.delivered_rtx < impaired.packets_rtx as usize,
        "the swallowed RTX is what kept the burst open: {impaired:?}"
    );
}
