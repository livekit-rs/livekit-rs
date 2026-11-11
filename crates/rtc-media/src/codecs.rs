//! Codec, payload-type and header-extension configuration per transport
//! (sfu-media.md §8.3-§8.5).
//!
//! str0m lets us pick payload types for video via `CodecConfig::add_config` /
//! `add_h264` / `add_h265`; opus/RED use `enable_opus` whose fixed PTs (111, RED 63)
//! already equal LiveKit's, PCMU/PCMA are static (0/8). When the client offers, str0m
//! adopts the client's PTs for matching codecs, so the LiveKit PTs only matter for
//! server-generated offers (dual-PC subscriber).

use rtc_core::media_api::PcRole;
use rtc_core::model::{CodecSpec, TrackKind};
use str0m::RtcConfig;
use str0m::format::{Codec, FormatParams};
use str0m::media::{Frequency, Pt};
use str0m::rtp::Extension;

fn pt(v: u8) -> Pt {
    Pt::new_with_value(v)
}

/// `room.enabled_codecs` lookups. A config entry with an empty fmtp enables by mime
/// only (mediaengine.go:147-161); a non-empty fmtp only enables an exactly matching
/// registration.
struct Enabled<'a>(&'a [CodecSpec]);

impl Enabled<'_> {
    fn mime(&self, mime: &str) -> bool {
        self.0
            .iter()
            .any(|c| c.mime.eq_ignore_ascii_case(mime) && c.fmtp_line.trim().is_empty())
    }

    /// Strict H264 entries: enabled only when the config lists this exact
    /// profile-level-id + packetization-mode.
    fn h264_exact(&self, profile_level_id: u32, packetization_mode: u8) -> bool {
        self.0.iter().any(|c| {
            if !c.mime.eq_ignore_ascii_case("video/h264") || c.fmtp_line.trim().is_empty() {
                return false;
            }
            let f = FormatParams::parse_line(&c.fmtp_line);
            f.profile_level_id == Some(profile_level_id)
                && f.packetization_mode.unwrap_or(0) == packetization_mode
        })
    }
}

/// Configure the codec set of one transport. Starts from an empty codec list.
pub(crate) fn configure_codecs(
    cfg: RtcConfig,
    enabled_codecs: &[CodecSpec],
    role: PcRole,
    _client_supports_red: bool,
) -> RtcConfig {
    let en = Enabled(enabled_codecs);
    let mut cfg = cfg.clear_codecs();

    // ---- audio ----
    let opus = en.mime("audio/opus");
    // Source mediaengine.go registers configured RED with Opus for every role.
    // SupportsAudioRED changes per-track Opus NACK in transport.go, not SDP registration.
    // The sticky NACK consumer is applied when attach_down configures a sender.
    let red = opus && en.mime("audio/red");
    if opus {
        // PT 111, fmtp minptime=10;useinbandfec=1, RED PT 63 with fmtp 111/111.
        cfg = cfg.enable_opus(true, red);
    }
    if en.mime("audio/pcmu") {
        cfg = cfg.enable_pcmu(true, false);
    }
    if en.mime("audio/pcma") {
        cfg = cfg.enable_pcma(true, false);
    }

    // ---- video ---- (registration order of mediaengine.go)
    let rtx = en.mime("video/rtx");
    let r = |v: u8| if rtx { Some(pt(v)) } else { None };
    {
        let cc = cfg.codec_config();
        if en.mime("video/vp8") {
            cc.add_config(
                pt(96),
                r(97),
                Codec::Vp8,
                Frequency::NINETY_KHZ,
                None,
                FormatParams::default(),
            );
        }
        if en.mime("video/vp9") {
            let p0 = FormatParams {
                profile_id: Some(0),
                ..Default::default()
            };
            let p1 = FormatParams {
                profile_id: Some(1),
                ..Default::default()
            };
            cc.add_config(pt(98), r(99), Codec::Vp9, Frequency::NINETY_KHZ, None, p0);
            cc.add_config(pt(100), r(101), Codec::Vp9, Frequency::NINETY_KHZ, None, p1);
        }
        if en.mime("video/h264") {
            cc.add_h264(pt(125), r(126), false, 0x42e01f);
            cc.add_h264(pt(108), r(109), true, 0x42e01f);
            // H264 High is filtered out on the subscriber PC (transport.go:348-355).
            if role != PcRole::Subscriber {
                cc.add_h264(pt(123), r(124), true, 0x640032);
            }
        }
        // Strict-fmtp H264 entries, off unless listed with that exact fmtp.
        for (p, rp, pm, plid) in [
            (104u8, 105u8, 0u8, 0x42001fu32),
            (102, 103, 1, 0x42001f),
            (39, 40, 0, 0x4d001f),
            (106, 107, 1, 0x4d001f),
        ] {
            if en.h264_exact(plid, pm) {
                cc.add_h264(pt(p), r(rp), pm == 1, plid);
            }
        }
        if en.mime("video/av1") {
            cc.add_config(
                pt(35),
                r(36),
                Codec::Av1,
                Frequency::NINETY_KHZ,
                None,
                FormatParams::default(),
            );
        }
        if en.mime("video/h265") {
            // str0m's default H265 profile/tier/level (Main, Main tier, level 6.0);
            // str0m mirrors the remote's fmtp shape on negotiation.
            cc.add_h265(pt(116), r(117), 1, 0, 180);
        }

        // ---- RTCP feedback (§8.3) ----
        for p in cc.iter_mut() {
            let codec = p.spec().codec;
            if codec.is_video() {
                p.set_fb_fir(true);
                p.set_fb_pli(true);
                p.set_fb_nack(true);
                // transport-cc stays on everywhere: str0m's send-side BWE needs TWCC
                // feedback (deviation: upstream subscriber default is REMB-only).
                p.set_fb_transport_cc(true);
                p.set_fb_remb(role != PcRole::Publisher);
            } else if codec == Codec::Opus {
                p.set_fb_nack(true);
                p.set_fb_transport_cc(true);
            }
        }
    }
    cfg
}

// Header extension ids. Clients that offer remap them; only server offers
// (dual-PC subscriber) use these numbers verbatim.
const EXT_AUDIO_LEVEL: u8 = 1;
const EXT_ABS_SEND_TIME: u8 = 2;
const EXT_TWCC: u8 = 3;
const EXT_MID: u8 = 4;
const EXT_ABS_CAPTURE_TIME: u8 = 5;
const EXT_PLAYOUT_DELAY: u8 = 6;
const EXT_FRAME_MARKING: u8 = 7;
const EXT_RID: u8 = 10;
const EXT_REPAIRED_RID: u8 = 11;

/// Header extensions per §8.4, as far as str0m supports them. Not available in str0m:
/// dependency-descriptor is supplied by our custom serializer. One map per transport (str0m's map is
/// session-wide and filtered per media kind).
pub(crate) fn configure_extensions(cfg: RtcConfig, role: PcRole, playout_delay: bool) -> RtcConfig {
    let receives = matches!(role, PcRole::Publisher | PcRole::Single);
    let sends = matches!(role, PcRole::Subscriber | PcRole::Single);
    let mut cfg = cfg
        .clear_extension_map()
        .set_extension(
            13,
            Extension::with_serializer(crate::av1::URI, crate::av1::Serializer),
        )
        .set_extension(EXT_MID, Extension::RtpMid)
        .set_extension(EXT_TWCC, Extension::TransportSequenceNumber)
        .set_extension(EXT_ABS_CAPTURE_TIME, Extension::AbsoluteCaptureTime);
    if sends {
        // Go's split publisher negotiates TWCC without absolute send time.
        cfg = cfg.set_extension(EXT_ABS_SEND_TIME, Extension::AbsoluteSendTime);
    }
    if receives {
        cfg = cfg
            .set_extension(EXT_AUDIO_LEVEL, Extension::AudioLevel)
            .set_extension(EXT_RID, Extension::RtpStreamId)
            .set_extension(EXT_REPAIRED_RID, Extension::RepairedRtpStreamId)
            .set_extension(EXT_FRAME_MARKING, Extension::FrameMarking);
    }
    if sends && playout_delay {
        cfg = cfg.set_extension(EXT_PLAYOUT_DELAY, Extension::PlayoutDelay);
    }
    cfg
}

/// LiveKit mime string of a str0m codec (`audio/opus`, `video/VP8`, ...).
pub(crate) fn mime_of(codec: Codec) -> &'static str {
    match codec {
        Codec::Opus => "audio/opus",
        Codec::PCMU => "audio/PCMU",
        Codec::PCMA => "audio/PCMA",
        Codec::G722 => "audio/G722",
        Codec::Vp8 => "video/VP8",
        Codec::Vp9 => "video/VP9",
        Codec::H264 => "video/H264",
        Codec::H265 => "video/H265",
        Codec::Av1 => "video/AV1",
        Codec::Red => "audio/red",
        _ => "",
    }
}

/// Dynacast codec name: lower-case mime without the `video/` prefix (§3.4).
pub(crate) fn dynacast_codec_name(mime: &str) -> String {
    let lc = mime.to_ascii_lowercase();
    lc.strip_prefix("video/").map(str::to_string).unwrap_or(lc)
}

pub(crate) fn track_kind(kind: str0m::media::MediaKind) -> TrackKind {
    match kind {
        str0m::media::MediaKind::Audio => TrackKind::Audio,
        str0m::media::MediaKind::Video => TrackKind::Video,
    }
}

/// Opus silence frame (RFC 6716 TOC 0xf8: CELT FB 20 ms mono, code 0; payload
/// 0xff 0xfe), the frame upstream injects on mute.
pub(crate) const OPUS_SILENCE_FRAME: [u8; 3] = [0xf8, 0xff, 0xfe];

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(m: &str, f: &str) -> CodecSpec {
        CodecSpec {
            mime: m.into(),
            fmtp_line: f.into(),
        }
    }

    #[test]
    fn mime_and_strict_matching() {
        let list = vec![
            spec("audio/opus", ""),
            spec("video/H264", "profile-level-id=4d001f;packetization-mode=1"),
        ];
        let en = Enabled(&list);
        assert!(en.mime("Audio/Opus"));
        assert!(!en.mime("video/h264"));
        assert!(en.h264_exact(0x4d001f, 1));
        assert!(!en.h264_exact(0x4d001f, 0));
    }

    #[test]
    fn dynacast_names() {
        assert_eq!(dynacast_codec_name("video/VP8"), "vp8");
        assert_eq!(dynacast_codec_name("video/H264"), "h264");
        assert_eq!(dynacast_codec_name("vp9"), "vp9");
    }

    #[test]
    fn publisher_answer_negotiates_twcc_without_absolute_send_time() {
        use str0m::media::{Direction, MediaKind};

        let now = std::time::Instant::now();
        let builder = || {
            str0m::Rtc::builder()
                .set_crypto_provider(rtc_crypto::provider().into())
                .set_dtls_cert(rtc_crypto::dtls_cert().unwrap())
        };
        let mut client = builder()
            .set_extension(EXT_ABS_SEND_TIME, Extension::AbsoluteSendTime)
            .set_extension(EXT_TWCC, Extension::TransportSequenceNumber)
            .build(now);
        let mut change = client.sdp_api();
        change.add_media(MediaKind::Video, Direction::SendOnly, None, None, None);
        let (offer, _) = change.apply().unwrap();
        let offer = offer.to_sdp_string();
        let abs_send_time = Extension::AbsoluteSendTime.as_uri();
        let twcc = Extension::TransportSequenceNumber.as_uri();
        assert!(offer.contains(abs_send_time));
        assert!(offer.contains(twcc));

        for (role, expected_abs_send_time) in [(PcRole::Publisher, false), (PcRole::Single, true)] {
            let mut server = configure_extensions(builder(), role, false).build(now);
            let answer = server
                .sdp_api()
                .accept_offer(str0m::change::SdpOffer::from_sdp_string(&offer).unwrap())
                .unwrap()
                .to_sdp_string();
            assert!(answer.contains(twcc), "{role:?}");
            assert_eq!(
                answer.contains(abs_send_time),
                expected_abs_send_time,
                "{role:?}"
            );
        }
    }
}

#[cfg(test)]
mod red_registration_source_tests {
    use super::*;
    #[test]
    fn configured_red_registers_for_all_roles_even_when_sender_capability_is_false() {
        for role in [PcRole::Publisher, PcRole::Subscriber, PcRole::Single] {
            for supports_red in [false, true] {
                for (opus, red) in [(true, true), (true, false), (false, true), (false, false)] {
                    let enabled = [("audio/opus", opus), ("audio/red", red)]
                        .into_iter()
                        .filter(|(_, enabled)| *enabled)
                        .map(|(mime, _)| CodecSpec {
                            mime: mime.into(),
                            fmtp_line: String::new(),
                        })
                        .collect::<Vec<_>>();
                    let mut cfg =
                        configure_codecs(str0m::Rtc::builder(), &enabled, role, supports_red);
                    let observed = cfg
                        .codec_config()
                        .iter()
                        .map(|p| p.spec().codec)
                        .collect::<Vec<_>>();
                    assert_eq!(
                        observed.contains(&Codec::Opus),
                        opus,
                        "{role:?} supports={supports_red}"
                    );
                    assert_eq!(
                        cfg.codec_config()
                            .iter()
                            .any(|p| p.spec().codec == Codec::Opus && p.red().is_some()),
                        opus && red,
                        "{role:?} supports={supports_red}"
                    );
                }
            }
        }
    }
}
