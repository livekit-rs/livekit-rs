use std::collections::{HashMap, VecDeque};
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::Event;
use crate::bwe::BweKind;
use crate::bwe_::Bwe;
use crate::config::KeyingMaterial;
use crate::config_mod::RtcpReportIntervals;
use crate::crypto::CryptoProvider;
use crate::crypto::dtls::SrtpProfile;
use crate::format::CodecConfig;
use crate::format::PayloadParams;
use crate::format::Vp9PacketizerMode;
use crate::io::DatagramSend;
use crate::media::AppSpecificFeedback;
use crate::media::Media;
use crate::media::{KeyframeRequestKind, MID_PROBE};
use crate::media::{MediaAdded, MediaChanged};
use crate::pacer::PacerControl;
use crate::pacer::{Pacer, PacerImpl};
use crate::packet::{RedBlock, RedDecoder, red_same_pt_blocks};
use crate::rtp::{Extension, RawPacket};
use crate::rtp_::Direction;
use crate::rtp_::ExtensionValues;
use crate::rtp_::MediaTime;
use crate::rtp_::MidRid;
use crate::rtp_::Pli;
use crate::rtp_::Pt;
use crate::rtp_::SRTCP_OVERHEAD;
use crate::rtp_::SeqNo;
use crate::rtp_::{Bitrate, ExtensionMap, Goodbye, Mid, ReportList, Rtcp, RtcpFb};
use crate::rtp_::{Dlrr, DlrrItem, ExtendedReport, ReportBlock};
use crate::rtp_::{RtpHeader, SessionId, TwccPacketId, extend_u16};
use crate::rtp_::{SrtpContext, Ssrc};
use crate::rtp_::{TwccRecvRegister, TwccSendRegister};
use crate::stats::StatsSnapshot;
use crate::streams::{RtpPacket, StreamTimeoutConfig, Streams};
use crate::util::{Soonest, SystemTimeExt, already_happened, not_happening};
use crate::{Reason, net};
use crate::{RtcConfig, RtcError};

/// Minimum time we delay between sending nacks. This should be
/// set high enough to not cause additional problems in very bad
/// network conditions.
const NACK_MIN_INTERVAL: Duration = Duration::from_millis(33);

/// Delay between reports of TWCC. This is deliberately very low.
const TWCC_INTERVAL: Duration = Duration::from_millis(50);

struct LiveKitPublisherTwcc {
    sender_ssrc: Ssrc,
    last_report: Option<Instant>,
    packets_held: usize,
}

impl LiveKitPublisherTwcc {
    fn record(
        &mut self,
        register: &mut TwccRecvRegister,
        header: &RtpHeader,
        sequence: u16,
        now: Instant,
        max_report_bytes: usize,
        feedback: &mut VecDeque<Rtcp>,
    ) {
        self.packets_held += usize::from(register.record_publisher_receipt(sequence, now));
        let should_report = self.packets_held > 20
            && (self.packets_held > 100
                || self.last_report.is_none_or(|last| {
                    let elapsed = now.saturating_duration_since(last);
                    elapsed >= Duration::from_millis(100)
                        || (header.marker && elapsed >= Duration::from_millis(50))
                }));
        if !should_report {
            return;
        }

        self.last_report = Some(now);
        self.packets_held = 0;
        while let Some(mut report) = register.build_publisher_report(max_report_bytes) {
            report.sender_ssrc = self.sender_ssrc;
            report.ssrc = header.ssrc;
            trace!("Created publisher feedback TWCC: {:?}", report);
            register.observe_built(&report, now);
            feedback.push_back(Rtcp::Twcc(report));
        }
    }
}

/// Maximum number of pending RRTR entries before oldest are dropped.
const MAX_PENDING_RRTRS: usize = 300;

/// Maximum number of DLRR items to include in a single ExtendedReport.
const MAX_DLRR_PER_REPORT: usize = 50;

pub(crate) struct Session {
    id: SessionId,

    // These fields are pub to allow session_sdp.rs modify them.
    // Notice the fields are maybe not in m-line index order since the app
    // might be spliced in somewhere.
    pub medias: Vec<Media>,

    // The actual RTP encoded streams.
    pub streams: Streams,

    /// The app m-line. Spliced into medias above.
    app: Option<(Mid, usize)>,

    reordering_size_audio: usize,
    reordering_size_video: usize,
    reordering_timeout_audio: Option<Duration>,
    resume_opus_after_outage: bool,
    reordering_timeout_video: Option<Duration>,
    intervals: RtcpReportIntervals,
    pub send_buffer_audio: usize,
    pub send_buffer_video: usize,

    /// Extension mappings are _per BUNDLE_, but we can only have one a=group BUNDLE
    /// in WebRTC (one ice connection), so they are effectively per session.
    pub exts: ExtensionMap,

    // Configuration of how we are sending/receiving media.
    pub codec_config: CodecConfig,
    /// See [`crate::RtcConfig::set_answer_in_offer_order`].
    pub(crate) answer_in_offer_order: bool,

    // Payload params for SSRC 0 BWE probes.
    // Lazy init when we see the PT.
    probe_payload_params: Option<PayloadParams>,

    pub(crate) receiver_report_rtt_enabled: bool,
    pub(crate) require_fir_media_ssrc: bool,
    pub(crate) receiver_report_loss: VecDeque<crate::ReceiverReportLoss>,
    pub(crate) receiver_report_rtt: VecDeque<crate::ReceiverReportRtt>,
    pub(crate) rtp_stream_restart_detection: bool,
    pub(crate) forwarding_timing_enabled: bool,
    srtp_rx: Option<SrtpContext>,
    srtp_tx: Option<SrtpContext>,
    last_nack: Instant,
    last_twcc: Instant,
    livekit_publisher_twcc: Option<LiveKitPublisherTwcc>,
    twcc: u64,
    twcc_rx_register: TwccRecvRegister,
    twcc_tx_register: TwccSendRegister,
    max_rx_seq_lookup: HashMap<Ssrc, SeqNo>,

    bwe: Option<Bwe>,
    bwe_last_event: Option<(Bitrate, bool)>,

    enable_twcc_feedback: bool,
    send_transport_cc: bool,
    rtp_output_started: bool,
    remote_feedback_enabled: bool,
    send_side_observer: Option<crate::send_side_feedback::SendSideObserver>,
    rtcp_admission: Option<fn(&[u8]) -> bool>,
    pending_receiver_estimates: VecDeque<(Bitrate, Vec<u32>)>,
    receiver_estimate_drops: u64,
    remote_padding: crate::remote_padding::PaddingExecutor,

    /// A pacer for sending RTP at specific rate.
    pacer: PacerImpl,
    pacer_control: PacerControl,

    // temporary buffer when getting the next (unencrypted) RTP packet from Media line.
    poll_packet_buf: Vec<u8>,

    // Next packet for RtpPacket event.
    pending_packet: Option<RtpPacket>,
    pub(crate) video_packet_events: bool,
    pub(crate) all_video_packet_events: bool,
    pending_media_packet: Option<crate::media::MediaPacket>,

    // Total RTP payload bytes for media, including retransmissions.
    media_bytes_rx: u64,
    pub(crate) node_traffic: crate::MediaTraffic,
    media_bytes_tx: u64,

    pub ice_lite: bool,

    /// Whether we are running in RTP-mode.
    pub rtp_mode: bool,

    /// VP9 packetizer mode.
    vp9_packetizer_mode: Vp9PacketizerMode,

    feedback_tx: VecDeque<Rtcp>,
    feedback_rx: VecDeque<Rtcp>,
    rtp_closing: bool,

    /// Pending RRTRs from remote SSRCs in FIFO order, deduplicated by SSRC.
    /// A new RRTR for an already-pending SSRC updates its value in place.
    /// Capped at 300 entries; at most 50 are consumed per DLRR report.
    pending_rrtrs: VecDeque<(Ssrc, LastRrtr)>,

    raw_packets: Option<VecDeque<Box<RawPacket>>>,

    // Pending application-specific feedback (PSFB FMT=15) messages to emit as events.
    pending_app_feedback: Option<AppSpecificFeedback>,

    /// Target MTU (start) and warn threshold (end). Buffer sizing uses the
    /// target; oversized outgoing datagrams above the warn threshold log a warning.
    mtu: RangeInclusive<usize>,

    #[cfg(feature = "_internal_test_exports")]
    pending_probe: Option<crate::bwe_::ProbeClusterConfig>,
}

/// Last RRTR (Receiver Reference Time Report, RFC 3611 §4.4) received from a remote SSRC.
/// Stored at session level so we can respond with a DLRR regardless of which local stream
/// the remote is observing.
#[derive(Debug, Clone, Copy)]
struct LastRrtr {
    /// Middle 32 bits of the RRTR NTP timestamp (compact NTP / LRR).
    lrr: u32,
    /// When the RRTR arrived locally.
    received_at: Instant,
}

impl Session {
    pub fn new(config: &RtcConfig) -> Self {
        let mut id = SessionId::new();
        // Max 2^62 - 1: https://bugzilla.mozilla.org/show_bug.cgi?id=861895
        const MAX_ID: u64 = 2_u64.pow(62) - 1;
        while *id > MAX_ID {
            id = (*id >> 1).into();
        }
        let (pacer, bwe) = if let Some(config) = &config.bwe_config {
            let rate = config.initial_bitrate;
            let pacer = PacerImpl::leaky_bucket(rate * 2.0);
            let bwe = Bwe::new(rate);
            (pacer, Some(bwe))
        } else {
            (PacerImpl::null(), None)
        };

        let enable_stats = config.stats_interval.is_some();

        Session {
            id,
            medias: vec![],
            streams: Streams::new(enable_stats, *config.mtu.end(), config.pause_threshold),
            app: None,
            reordering_size_audio: config.reordering_size_audio,
            reordering_size_video: config.reordering_size_video,
            reordering_timeout_audio: config.reordering_timeout_audio,
            resume_opus_after_outage: config.resume_opus_after_outage,
            reordering_timeout_video: config.reordering_timeout_video,
            intervals: config.intervals,
            send_buffer_audio: config.send_buffer_audio,
            send_buffer_video: config.send_buffer_video,
            exts: config.exts.clone(),

            // Both sending and receiving starts from the configured codecs.
            // These can then be changed in the SDP OFFER/ANSWER dance.
            codec_config: config.codec_config.clone(),
            answer_in_offer_order: config.answer_in_offer_order,
            probe_payload_params: None,

            receiver_report_rtt_enabled: false,
            require_fir_media_ssrc: false,
            receiver_report_loss: VecDeque::new(),
            receiver_report_rtt: VecDeque::new(),
            rtp_stream_restart_detection: false,
            forwarding_timing_enabled: false,
            srtp_rx: None,
            srtp_tx: None,
            last_nack: already_happened(),
            last_twcc: already_happened(),
            livekit_publisher_twcc: None,
            twcc: 0,
            twcc_rx_register: TwccRecvRegister::new(100),
            twcc_tx_register: TwccSendRegister::new(1000),
            max_rx_seq_lookup: HashMap::new(),
            bwe,
            bwe_last_event: None,
            enable_twcc_feedback: false,
            send_transport_cc: true,
            rtp_output_started: false,
            remote_feedback_enabled: false,
            send_side_observer: None,
            rtcp_admission: None,
            pending_receiver_estimates: VecDeque::new(),
            receiver_estimate_drops: 0,
            remote_padding: Default::default(),
            pacer,
            pacer_control: PacerControl::new(),
            poll_packet_buf: vec![0; 2000],
            pending_packet: None,
            video_packet_events: false,
            all_video_packet_events: false,
            pending_media_packet: None,
            media_bytes_rx: 0,
            node_traffic: Default::default(),
            media_bytes_tx: 0,
            ice_lite: config.ice_lite,
            rtp_mode: config.rtp_mode,
            vp9_packetizer_mode: config.vp9_packetizer_mode,
            feedback_tx: VecDeque::new(),
            feedback_rx: VecDeque::new(),
            rtp_closing: false,
            pending_rrtrs: VecDeque::new(),
            raw_packets: if config.enable_raw_packets {
                Some(VecDeque::new())
            } else {
                None
            },
            pending_app_feedback: None,
            mtu: config.mtu.clone(),
            #[cfg(feature = "_internal_test_exports")]
            pending_probe: None,
        }
    }

    fn mtu(&self) -> usize {
        *self.mtu.start()
    }

    fn mtu_warn(&self) -> usize {
        *self.mtu.end()
    }

    pub(crate) fn set_send_transport_cc(&mut self, enabled: bool) {
        self.send_transport_cc = enabled;
    }

    fn independent_padding_source(&mut self) -> Option<MidRid> {
        let medias = &self.medias;
        self.streams.streams_tx().find_map(|stream| {
            let pt = stream.padding_pt()?;
            if stream.rtx().is_none()
                || !medias.iter().any(|media| {
                    media.mid() == stream.mid()
                        && !media.stopped()
                        && media.direction().is_sending()
                        && (media.remote_pts().is_empty() || media.remote_pts().contains(&pt))
                })
            {
                return None;
            }
            Some(MidRid(stream.mid(), stream.rid()))
        })
    }

    pub(crate) fn request_padding(
        &mut self,
        bytes: u64,
        rate: Bitrate,
        duration: Duration,
        now: Instant,
    ) -> Result<u64, crate::PaddingRequestError> {
        if self.bwe.is_some() {
            return Err(crate::PaddingRequestError::NativeBweEnabled);
        }
        if self.rtp_closing || !self.ready_for_srtp() {
            return Err(crate::PaddingRequestError::NotReady);
        }
        if self.independent_padding_source().is_none() {
            return Err(crate::PaddingRequestError::NoSource);
        }
        self.remote_padding.start(bytes, rate, duration, now)
    }

    pub(crate) fn request_padding_burst(
        &mut self,
        bytes: u64,
        duration: Duration,
        now: Instant,
    ) -> Result<u64, crate::PaddingRequestError> {
        if self.bwe.is_some() {
            return Err(crate::PaddingRequestError::NativeBweEnabled);
        }
        if self.rtp_closing || !self.ready_for_srtp() {
            return Err(crate::PaddingRequestError::NotReady);
        }
        if self.independent_padding_source().is_none() {
            return Err(crate::PaddingRequestError::NoSource);
        }
        self.remote_padding.start_burst(bytes, duration, now)
    }
    pub(crate) fn cancel_padding(&mut self, id: u64) -> bool {
        self.remote_padding.cancel(id)
    }
    pub(crate) fn padding_status(&self) -> Option<crate::PaddingObservation> {
        self.remote_padding.observation()
    }

    fn poll_remote_padding(&mut self, now: Instant) -> Option<DatagramSend> {
        if !self.remote_padding.due(now) {
            return None;
        }
        let Some(source) = self.independent_padding_source() else {
            self.remote_padding
                .stop(crate::PaddingStatus::SourceUnavailable);
            return None;
        };
        let result = self.poll_packet_on(now, source, None, true);
        if result.is_none()
            && self
                .remote_padding
                .observation()
                .is_some_and(|o| o.status == crate::PaddingStatus::Active)
        {
            self.remote_padding
                .stop(crate::PaddingStatus::SourceUnavailable);
        }
        result
    }

    pub fn id(&self) -> SessionId {
        self.id
    }

    pub fn set_app(&mut self, mid: Mid, index: usize) -> Result<(), String> {
        if let Some((mid_existing, index_existing)) = self.app {
            if mid_existing != mid {
                return Err(format!("App mid changed {} != {}", mid, mid_existing,));
            }
            if index_existing != index {
                return Err(format!("App index changed {} != {}", index, index_existing,));
            }
        } else {
            self.app = Some((mid, index));
        }
        Ok(())
    }

    pub fn app(&self) -> &Option<(Mid, usize)> {
        &self.app
    }

    pub(crate) fn receive_srtp_auth_tag_len(&self) -> Option<usize> {
        self.srtp_rx.as_ref().map(SrtpContext::rtp_auth_tag_len)
    }

    pub fn set_keying_material(
        &mut self,
        mat: KeyingMaterial,
        crypto: &CryptoProvider,
        srtp_profile: SrtpProfile,
        active: bool,
    ) {
        // TODO: rename this to `initialise_srtp_context`?
        // Whether we're active or passive determines if we use the left or right
        // hand side of the key material to derive input/output.
        let left = active;

        self.srtp_rx = Some(SrtpContext::new(crypto, srtp_profile, &mat, !left));
        self.srtp_tx = Some(SrtpContext::new(crypto, srtp_profile, &mat, left));
    }

    pub fn handle_timeout(&mut self, now: Instant) -> Result<(), RtcError> {
        if self.rtp_closing {
            return Ok(());
        }

        // Payload ready media.
        self.do_payload(now)?;

        let sender_ssrc = self.streams.first_ssrc_local();

        let do_nack = now >= self.nack_at().unwrap_or(not_happening());

        self.streams.handle_timeout(
            now,
            sender_ssrc,
            do_nack,
            &self.medias,
            StreamTimeoutConfig {
                codecs: &self.codec_config,
                intervals: self.intervals,
            },
            &mut self.feedback_tx,
        );

        // Flush any pending RRTRs as a DLRR alongside the Sender Report compound packet.
        // We piggyback on the SR cadence so the DLRR doesn't generate extra standalone
        // RTCP traffic that could perturb bandwidth estimation.
        // Consume at most 50 entries per report; the rest remain for subsequent SRs.
        if !self.pending_rrtrs.is_empty()
            && self
                .feedback_tx
                .iter()
                .any(|r| matches!(r, Rtcp::SenderReport(_)))
        {
            let n = self.pending_rrtrs.len().min(MAX_DLRR_PER_REPORT);
            let items: Vec<DlrrItem> = self
                .pending_rrtrs
                .drain(..n)
                .map(|(ssrc, rrtr)| {
                    let delay = now.saturating_duration_since(rrtr.received_at);
                    let last_rr_delay = ((delay.as_micros() * 65_536) / 1_000_000) as u32;
                    DlrrItem {
                        ssrc,
                        last_rr_time: rrtr.lrr,
                        last_rr_delay,
                    }
                })
                .collect();

            self.feedback_tx
                .push_back(Rtcp::ExtendedReport(ExtendedReport {
                    ssrc: sender_ssrc,
                    blocks: vec![ReportBlock::Dlrr(Dlrr { items })],
                }));
        }

        if do_nack {
            self.last_nack = now;
        }

        let probe_media = self.handle_timeout_bwe(now);
        self.update_queue_state(now, probe_media);

        if let Some(twcc_at) = self.twcc_at() {
            if now >= twcc_at {
                self.create_twcc_feedback(sender_ssrc, now);
            }
        }

        Ok(())
    }

    fn handle_timeout_bwe(&mut self, now: Instant) -> Option<(Mid, Pt)> {
        self.bwe.as_ref()?;

        // We can request probes once SRTP keys are available and a non-stopped,
        // sending media section has both a transport sequence extension and a
        // payload type supporting TWCC feedback. No prior media packet or RTX
        // stream is required. The Direct API uses its configured capabilities.
        let probe_media = self.srtp_tx.as_ref().and_then(|_| self.probe_media());
        let do_probe = probe_media.is_some();
        let bwe = self.bwe.as_mut()?;

        if let Some(probe_config) = bwe.handle_timeout(now, do_probe) {
            // Only start the probe in the pacer if the estimator accepted it.
            if bwe.start_probe(probe_config, now) {
                #[cfg(feature = "_internal_test_exports")]
                {
                    self.pending_probe = Some(probe_config);
                }
                self.pacer.start_probe(probe_config);
            }
        }

        if bwe.validation_should_stop() {
            self.pacer.cancel_probes(true);
        }

        // Drain completion/cancellation events before rebuilding the probe
        // binding. Clear generated padding from regular RTX sources as well as
        // the fallback stream, preserving queued media and retransmissions.
        let mut ended_probe = false;
        while let Some(cluster_id) = self.pacer.check_probe_complete(now) {
            bwe.end_probe(now, cluster_id);
            ended_probe = true;
        }
        if ended_probe {
            self.streams.clear_pending_padding();
            self.streams.set_probe_media(None);
        }
        probe_media
    }

    fn update_queue_state(&mut self, now: Instant, probe_media: Option<(Mid, Pt)>) {
        // Do not make unsendable media/padding ready before DTLS supplies keys.
        // Otherwise an immediate pacer deadline can starve the application's I/O.
        if self.srtp_tx.is_none() {
            return;
        }

        let probe_media = probe_media.filter(|_| self.pacer.active_cluster().is_some());
        self.streams.set_probe_media(probe_media);
        let medias = &self.medias;
        let codecs = &self.codec_config;
        let iter = self.streams.send_queue_states(now, |mid, pt| {
            // Reuse the already-validated fallback binding when it matches.
            probe_media == Some((mid, pt))
                || medias
                    .iter()
                    .find(|m| m.mid() == mid)
                    .is_some_and(|media| Self::probe_pt(media, codecs, Some(pt)).is_some())
        });

        let Some(padding_request) = self.pacer.handle_timeout(now, iter) else {
            return;
        };

        let stream = self
            .streams
            .stream_tx_by_midrid(padding_request.midrid)
            .expect("pacer to use an existing stream");

        stream.generate_padding(padding_request.padding);
    }

    // Use a negotiated PT and MID so browser transports can route the packet to
    // their receive-side congestion controller before discarding the empty payload.
    fn probe_media(&self) -> Option<(Mid, Pt)> {
        self.medias.iter().find_map(|media| {
            Self::probe_pt(media, &self.codec_config, None).map(|pt| (media.mid(), pt))
        })
    }

    // A regular padding source needs feedback for its actual payload type;
    // the SSRC 0 fallback can use any eligible payload type on this media.
    fn probe_pt(media: &Media, codecs: &CodecConfig, padding_pt: Option<Pt>) -> Option<Pt> {
        if media.stopped() || !media.direction().is_sending() {
            return None;
        }
        media
            .remote_extmap()
            .id_of(Extension::TransportSequenceNumber)?;
        codecs
            .params()
            .iter()
            .find(|p| {
                padding_pt.is_none_or(|pt| p.pt() == pt)
                    && p.fb_transport_cc()
                    && p.spec().codec.is_audio() == media.kind().is_audio()
                    && (media.remote_pts().is_empty() || media.remote_pts().contains(&p.pt()))
            })
            .map(|p| p.pt())
    }

    fn create_twcc_feedback(&mut self, sender_ssrc: Ssrc, now: Instant) -> Option<()> {
        self.last_twcc = now;
        let mut twcc = self.twcc_rx_register.build_report(self.mtu() - 100)?;

        // These SSRC are on media level, but twcc is on session level,
        // we fill in the first discovered media SSRC in each direction.
        twcc.sender_ssrc = sender_ssrc;
        twcc.ssrc = self.streams.first_ssrc_remote();

        trace!("Created feedback TWCC: {:?}", twcc);
        self.twcc_rx_register.observe_built(&twcc, now);
        self.feedback_tx.push_front(Rtcp::Twcc(twcc));
        Some(())
    }

    /// Enqueue an application-specific feedback message (PSFB FMT=15, PT=206) for
    /// transmission as part of the regular RTCP compound packet.
    pub(crate) fn send_app_specific_feedback(
        &mut self,
        sender_ssrc: Ssrc,
        media_ssrc: Ssrc,
        payload: impl Into<Arc<[u8]>>,
    ) {
        use crate::rtp_::AppSpecificFeedback as RtcpAppFeedback;
        let feedback = RtcpAppFeedback {
            sender_ssrc,
            media_ssrc,
            payload: payload.into(),
        };
        self.feedback_tx
            .push_back(Rtcp::AppSpecificFeedback(feedback));
    }

    /// Enqueue a Picture Loss Indication (PLI, PT=206 FMT=1) for `media_ssrc`, to be
    /// sent as part of the regular RTCP compound packet. The feedback packet's sender
    /// SSRC is set to `sender_ssrc`.
    pub(crate) fn send_pli_feedback(&mut self, sender_ssrc: Ssrc, media_ssrc: Ssrc) {
        self.feedback_tx.push_back(Rtcp::Pli(Pli {
            sender_ssrc,
            ssrc: media_ssrc,
        }));
    }

    pub fn handle_rtp_receive(&mut self, now: Instant, message: &[u8]) {
        let Some(header) = RtpHeader::parse(message, &self.exts) else {
            trace!("Failed to parse RTP header");
            return;
        };

        self.handle_rtp(now, header, message);
    }

    pub fn handle_rtcp_receive(&mut self, now: Instant, message: &[u8]) {
        // According to spec, the outer enclosing SRTCP packet should always be a SR or RR,
        // even if it's irrelevant and empty.
        // In practice I'm not sure that is happening, because libWebRTC hates empty packets.
        self.handle_rtcp(now, message);
    }

    fn mid_and_ssrc_for_header(&mut self, now: Instant, header: &RtpHeader) -> Option<(Mid, Ssrc)> {
        let ssrc_header = header.ssrc;

        // SSRC 0 is used for non-media BWE probes from libwebrtc.
        // These probes need TWCC feedback but don't carry actual media.
        if ssrc_header.is_probe() {
            return self.ensure_probe_stream(header.payload_type);
        }

        if let Some(r) = self.streams.mid_ssrc_rx_by_ssrc_or_rtx(now, ssrc_header) {
            return Some(r);
        }

        // Attempt to dynamically map this header to some Media/ReceiveStream.
        self.map_dynamic(header);

        // The dynamic mapping might have added an entry by now.
        if let Some(r) = self.streams.mid_ssrc_rx_by_ssrc_or_rtx(now, ssrc_header) {
            return Some(r);
        }

        None
    }

    /// Creates the probe stream on-demand for handling SSRC 0 BWE probes.
    ///
    /// No Media is created since probes don't carry real media - they only need
    /// SRTP decryption and TWCC feedback.
    fn ensure_probe_stream(&mut self, pt: Pt) -> Option<(Mid, Ssrc)> {
        let ssrc: Ssrc = 0.into();
        let midrid = MidRid(MID_PROBE, None);

        // Add PayloadParams for this PT if not already configured.
        // Probes may use different PTs, so we add them as we see them.
        if self
            .probe_payload_params
            .as_ref()
            .is_none_or(|p| p.pt() != pt)
        {
            self.probe_payload_params = Some(PayloadParams::new_probe(pt));
        }

        // Create the stream with NACK suppressed (probes don't need retransmission).
        // Reusing it must not invalidate the cached NACK state on every packet.
        if !self.streams.has_stream_rx(ssrc) {
            self.streams.expect_stream_rx(ssrc, None, midrid, true);
        }

        Some((MID_PROBE, ssrc))
    }

    fn map_dynamic(&mut self, header: &RtpHeader) {
        // There are two strategies for dynamically mapping SSRC. Both use the RTP "mid"
        // header extension.
        // A) Mid+Rid - used when doing simulcast. Rid points out which
        //              simulcast layer is in use. There is a separate header
        //              to indicate repair (RTX) stream.
        // B) Mid+PT - when not doing simulcast, the PT identifies whether
        //             this is a repair stream.

        let Some(mid) = header.ext_vals.mid else {
            return;
        };
        let rid = header.ext_vals.rid.or(header.ext_vals.rid_repair);

        // The media the mid points out. Bail if the mid points to something
        // we don't know about.
        let Some(media) = self.medias.iter_mut().find(|m| m.mid() == mid) else {
            return;
        };

        // Figure out which payload the PT maps to. Either main or RTX.
        let maybe_payload = self.codec_config.iter().find(|p| {
            p.pt() == header.payload_type
                || p.resend() == Some(header.payload_type)
                || p.red() == Some(header.payload_type)
        });

        // If we don't find it, bail out.
        let Some(payload) = maybe_payload else {
            return;
        };

        if let Some(rid) = rid {
            // Case A - use the rid_repair header to identify RTX.
            let is_main = header.ext_vals.rid.is_some();

            let midrid = MidRid(mid, Some(rid));

            self.streams
                .map_dynamic_by_rid(header.ssrc, midrid, media, *payload, is_main);
        } else {
            // Case B - the payload type identifies RTX. RED rides the main SSRC, so a RED
            // PT is also "main".
            let is_main =
                payload.pt() == header.payload_type || payload.red() == Some(header.payload_type);

            let midrid = MidRid(mid, None);

            self.streams
                .map_dynamic_by_pt(header.ssrc, midrid, media, *payload, is_main);
        }
    }

    pub(crate) fn handle_rtp(&mut self, now: Instant, mut header: RtpHeader, buf: &[u8]) {
        // Rewrite absolute-send-time (if present) to be relative to now.
        header.ext_vals.update_absolute_send_time(now);

        trace!("Handle RTP: {:?}", header);

        // The ssrc is the _main_ ssrc (no the rtx, that might be in the header).
        let Some((mid, ssrc)) = self.mid_and_ssrc_for_header(now, &header) else {
            debug!("No mid/SSRC for header: {:?}", header);
            return;
        };

        let max_twcc_report_bytes = self.mtu().saturating_sub(100);
        let srtp = match self.srtp_rx.as_mut() {
            Some(v) => v,
            None => {
                trace!("Rejecting SRTP while missing SrtpContext");
                return;
            }
        };

        // mid_and_ssrc_for_header guarantees stream exists.
        // Media may not exist for internal probe streams (SSRC 0).
        let stream = self.streams.stream_rx(&ssrc).unwrap();

        let maybe_params = if ssrc.is_probe() {
            self.probe_payload_params.as_ref()
        } else {
            main_payload_params(&self.codec_config, header.payload_type)
        };

        let params = match maybe_params {
            Some(p) => p,
            None => {
                trace!(
                    "No payload params could be found (main or RTX) for {:?}",
                    header.payload_type
                );
                return;
            }
        };
        let packet_params = *params;
        let clock_rate = params.spec().rtp_clock_rate();
        let pt = params.pt();
        let is_red = params.red() == Some(header.payload_type);
        let is_repair = !is_red && pt != header.payload_type;

        // Copy the authenticated SSRC nonce watermark rather than retain a
        // map-borrowing closure across the later authenticated commit.
        let previous_crypto_seq = self.max_rx_seq_lookup.get(&header.ssrc).copied();

        // is_repair controls whether update is updating the main register or the RTX register.
        // Either way we get a seq_no_outer which is used to decrypt the SRTP.
        let (mut seq_no, reset_crypto_roc) =
            stream.extend_crypto_seq(&header, is_repair, previous_crypto_seq);

        let is_new_wire_packet = stream.is_new_packet(is_repair, seq_no);

        // The decrypted plaintext borrows from the SRTP scratch buffer. We
        // narrow it down to the actual payload via slicing only, then build
        // the final `Arc<[u8]>` in a single allocation at the end.
        let data = match srtp.unprotect_rtp(buf, &header, *seq_no) {
            Some(d) => d,
            None => {
                trace!(
                    "Failed to unprotect SRTP for SSRC: {} pt: {}  mid: {} \
                    rid: {:?} seq_no: {} is_repair: {}",
                    header.ssrc,
                    pt,
                    stream.mid(),
                    stream.rid(),
                    seq_no,
                    is_repair
                );
                return;
            }
        };

        let authenticated_rtp_bytes = header.header_len + data.len();
        let data = if header.has_padding {
            match RtpHeader::unpad_payload(data) {
                Some(d) => d,
                None => {
                    trace!("unpadding of unprotected payload failed");
                    return;
                }
            }
        } else {
            data
        };

        // Fixed livekit/webrtc-pion warp.6 rejects a repair payload shorter
        // than its two-byte OSN. Exactly two bytes still reach Buffer/RTPStats
        // after a source starts, despite carrying no deliverable media.
        // Media admission must not suppress transport-wide feedback: Pion's
        // TWCC interceptor sees authenticated blank RTX padding before its
        // repair parser discards a payload without a complete OSN.
        let logical_rtp_bytes =
            authenticated_rtp_bytes.saturating_sub(if is_repair { 2 } else { 0 });
        let mut media_admission = crate::receive_traffic::Admission::Bypass;
        if !header.ssrc.is_probe() && (!is_repair || data.len() >= 2) {
            self.node_traffic.receive_rtp(logical_rtp_bytes);
            media_admission = stream.receive_traffic.receive(
                logical_rtp_bytes,
                params.spec().codec,
                params.pt(),
                is_repair,
                is_red,
                *seq_no,
                is_repair.then(|| u16::from_be_bytes([data[0], data[1]])),
                if is_repair {
                    data.len() > 2
                } else {
                    !data.is_empty()
                },
                header.timestamp,
                clock_rate.get(),
                self.rtp_stream_restart_detection,
                now,
            );
        }
        let source_restarted = media_admission.restarted();
        let admitted_flow = media_admission.flow();
        if let Some(raw_packets) = &mut self.raw_packets {
            raw_packets.push_back(Box::new(RawPacket::RtpRx(header.clone(), data.to_vec())));
        }

        // Record authenticated wire RTP before logical media de-duplication.
        if let Some(transport_cc) = header.ext_vals.transport_cc {
            if let Some(profile) = &mut self.livekit_publisher_twcc {
                let video_twcc = if ssrc.is_probe() {
                    // Pion binds SSRC 0 to an implicit video receiver. The
                    // synthetic probe codec is only for SRTP and has no SDP.
                    self.medias.iter().any(|media| {
                        !media.stopped()
                            && media.kind().is_video()
                            && media.direction().is_receiving()
                            && media
                                .remote_extmap()
                                .id_of(Extension::TransportSequenceNumber)
                                .is_some()
                            && self.codec_config.iter().any(|p| {
                                p.fb_transport_cc() && media.remote_pts().contains(&p.pt())
                            })
                    })
                } else {
                    packet_params.spec().codec.is_video()
                        && (is_repair || packet_params.fb_transport_cc())
                };
                if self.enable_twcc_feedback && video_twcc {
                    profile.record(
                        &mut self.twcc_rx_register,
                        &header,
                        transport_cc,
                        now,
                        max_twcc_report_bytes,
                        &mut self.feedback_tx,
                    );
                }
            } else {
                let prev = self.twcc_rx_register.max_seq();
                let extended = extend_u16(prev.map(|s| *s), transport_cc);
                self.twcc_rx_register.update_seq(extended.into(), now);
            }
        }

        // Original RTPStats ignores pre-start primary padding. Preserve its
        // transport feedback, but do not let it initialize the primary replay
        // register (and suppress a later real packet with this same SN).
        let source_started = stream.receive_traffic.is_started();
        if !is_repair && data.is_empty() && !source_started {
            return;
        }

        // Store largest seen seq_no for the SSRC. This is used in case we get SSRC changes
        // like A -> B -> A. When we go back to A, we must keep the ROC.
        if reset_crypto_roc {
            self.max_rx_seq_lookup.insert(header.ssrc, seq_no);
        } else {
            update_max_seq(&mut self.max_rx_seq_lookup, header.ssrc, seq_no);
        }

        // Transport feedback precedes logical media de-duplication. A fresh
        // RTX packet can carry an already received OSN (including padding),
        // but its new transport sequence still acknowledges real delivery.
        // Commit the outer crypto/RTX register independently of inner media.
        if media_admission.rejected()
            || media_admission.duplicated()
            || (admitted_flow.is_none() && !is_new_wire_packet)
        {
            if is_repair {
                stream.ensure_media_register(true);
                stream.update_register(now, &header, clock_rate, true, seq_no);
            }
            return;
        }
        if source_restarted {
            // Keep DTLS/SRTP, ICE, SCTP and logical subscription alive. Only
            // this source's packet history, jitter/reorder and NACK state reset.
            stream.reset_detected_source();
            if let Some(media) = self.medias.iter_mut().find(|media| media.mid() == mid) {
                media.reset_depayloaders_for_rid(stream.rid());
            }
        }

        stream.ensure_media_register(is_repair);
        if !is_repair {
            if let Some(flow) = admitted_flow {
                seq_no = flow.seq.into();
                stream.set_admitted_timestamp(flow.timestamp);
            }
        }
        // Register reception in nack registers.
        let receipt_outer = stream.update_register(now, &header, clock_rate, is_repair, seq_no);
        // This is the media parser boundary, after TWCC/transport reception.
        // Blank padding still has to acknowledge its transport sequence.
        if is_repair && (data.len() < 2 || !source_started) {
            // A pre-start RTX must not initialize the primary receive/replay
            // register via its OSN. Only the outer repair transport was received.
            return;
        }

        // RED-recovered packets (frame mode) are collected here and delivered after the
        // primary, filling single-packet losses from the next packet's redundancy.
        let mut red_recovered: Vec<(RtpHeader, Arc<[u8]>, SeqNo, MediaTime)> = Vec::new();

        // Capture the full wire payload length before RED unwrapping narrows `data` to the primary
        // block. The sender counts the whole RED-wrapped payload, so receive stats must too.
        let wire_payload_len = data.len();

        // RTX packets must be rewritten to be a normal packet. This only changes the
        // the seq_no, however MediaTime might be different when interpreted against the
        // the "main" register.
        let (receipt, data) = if is_repair {
            // Rewrite the header, and strip the resent seq_no prefix from the body.
            let data = stream.un_rtx(&mut header, data, pt);

            // Buffer media sequence/timestamp extension is independent of the
            // authenticated repair transport's SRTP ROC. A recovered OSN must
            // never advance the primary cipher watermark.
            stream.ensure_media_register(false);
            if let Some(flow) = admitted_flow {
                seq_no = flow.seq.into();
                stream.set_admitted_timestamp(flow.timestamp);
            } else {
                seq_no =
                    stream.extend_seq(&header, false, make_max_seq_lookup(&self.max_rx_seq_lookup));
            }

            // Now update the "main" register with the repaired packet info.
            let receipt = stream.update_register(now, &header, clock_rate, false, seq_no);
            // Original Buffer counts an initialized OSN-only repair but drops
            // its empty payload before delivery, in frame and RTP modes alike.
            if data.is_empty() {
                return;
            }
            (receipt, data)
        } else if is_red && !self.rtp_mode {
            // Frame mode: RED rides the main SSRC/seq. Decode to the primary (Opus) payload
            // and rewrite the header PT; drop the packet if the RED structure is malformed.
            let Some(un) = un_red(&mut header, data, pt) else {
                return;
            };
            // Each redundant block carries an earlier frame. Place it at the sequence number it
            // belongs to (derived from its timestamp and the packets received around it, see
            // `StreamRx::red_locate_seq`) and, if that is still missing, deliver it as a
            // recovered packet so a loss is filled without a retransmission. Only same-PT blocks
            // are considered: RFC 2198 permits a redundant block of another codec, which must
            // never be fed to this depayloader.
            for b in red_same_pt_blocks(&un.redundant, *pt) {
                // Skip recovery (rather than inject a bad timestamp) if the offset somehow
                // exceeds the primary's extended media time.
                let Some(numer) = receipt_outer
                    .time
                    .numer()
                    .checked_sub(b.timestamp_offset as u64)
                else {
                    continue;
                };
                // `None` when the block can't be tied to a currently-missing seq no (see
                // `red_locate_seq` for the exact vs. best-effort cases), or when that seq no
                // was already received or recovered. Recovery only ever fills a hole.
                let Some(rec_seq) = stream.red_locate_seq(seq_no, numer) else {
                    continue;
                };
                // Mark it so the seq is neither NACKed nor recovered again from a later packet.
                // It is not counted as received (it was lost on the wire), so reception reports
                // still carry the real loss. `red_locate_seq` now rejects this seq, which also
                // dedups a malformed packet repeating the same offset.
                stream.mark_red_recovered(rec_seq, numer);

                let rec_time = MediaTime::new(numer, clock_rate);
                let mut rec_header = header.clone();
                rec_header.sequence_number = *rec_seq as u16;
                // RFC 2198: the marker bit is not carried for redundant blocks. Clearing it also
                // stops a recovered (historical) audio frame being seen as a start-of-talkspurt.
                rec_header.marker = false;
                // Redundant blocks carry no header extensions, so a recovered frame cannot carry
                // its own (e.g. audio level). Clear the carrier's extensions rather than let the
                // recovered frame inherit the current packet's metadata.
                rec_header.ext_vals = ExtensionValues::default();
                red_recovered.push((rec_header, Arc::from(b.payload), rec_seq, rec_time));
            }
            (receipt_outer, un.primary)
        } else {
            // This is not RTX, the outer seq and time is what we use. The first
            // stream.update will have updated the main register.
            (receipt_outer, data)
        };

        // Probe packets (SSRC 0) contain only padding, no real media to process
        if ssrc.is_probe() {
            return;
        }

        // For RED (frame mode) `data` is now just the unwrapped primary; count the full RED wire
        // payload instead so receive accounting matches the sender's byte count.
        let rx_bytes = if is_red && !self.rtp_mode {
            wire_payload_len
        } else {
            data.len()
        };
        self.media_bytes_rx += rx_bytes as u64;

        // One-shot conversion to Arc<[u8]>: a single allocation that copies
        // only the trimmed payload bytes out of the SRTP scratch buffer.
        let payload: Arc<[u8]> = Arc::from(data);

        let mut packet = stream.handle_rtp(now, header, payload, seq_no, receipt.time, rx_bytes);
        if self.forwarding_timing_enabled {
            packet
                .header
                .ext_vals
                .user_values
                .set(crate::media::ForwardingPacketMeta(admitted_flow.map(
                    |flow| crate::media::ForwardingPacket {
                        sequence: flow.seq,
                        spatial: None,
                        temporal: None,
                        extension_index: 0,
                        received: now,
                        out_of_order: !flow.in_order,
                        // Codec assembly is real work, not prebinding replay.
                        buffered: false,
                    },
                )));
        }

        if self.video_packet_events
            && (matches!(
                packet_params.spec().codec,
                crate::format::Codec::Av1 | crate::format::Codec::Vp9
            ) || (self.all_video_packet_events && packet_params.spec().codec.is_video()))
            && receipt.is_new_packet
        {
            self.pending_media_packet = Some(crate::media::MediaPacket {
                source_id: stream.receive_traffic.source_id(),
                received_out_of_order: admitted_flow.is_some_and(|flow| !flow.in_order),
                logical_rtp_bytes,
                mid,
                rid: stream.rid(),
                params: packet_params,
                packet: Box::new(packet.clone()),
            });
        }
        if self.rtp_mode {
            // In RTP mode, we store the packet temporarily here for the next poll_output().
            // However only if this is a packet not seen before. This filters out spurious
            // resends for padding.
            if receipt.is_new_packet {
                self.pending_packet = Some(packet);
            }
        } else {
            // In non-RTP mode, we let the Media use a Depayloader.
            // unwrap is fine because mid_and_ssrc_for_header guarantees it.
            let media = self.medias.iter_mut().find(|m| m.mid() == mid).unwrap();
            let outage_primary = self.resume_opus_after_outage
                && !is_repair
                && !is_red
                && receipt.is_new_packet
                && !packet.payload.is_empty();
            media.depayload(
                stream.rid(),
                packet,
                if packet_params.spec().codec == crate::format::Codec::Vp9 {
                    logical_rtp_bytes
                } else {
                    0
                },
                self.reordering_size_audio,
                self.reordering_size_video,
                outage_primary,
                &self.codec_config,
            );
        }

        // Deliver any RED-recovered packets (frame mode only) through the same depayload
        // path. The jitter buffer orders them and dedups against any later real arrival.
        for (rec_header, rec_payload, rec_seq, rec_time) in red_recovered {
            // make_rtp_packet (not handle_rtp): recovered packets must not inflate RX stats.
            let rec_packet =
                stream.make_rtp_packet(now, rec_header, rec_payload, rec_seq, rec_time);
            let media = self.medias.iter_mut().find(|m| m.mid() == mid).unwrap();
            media.depayload(
                stream.rid(),
                rec_packet,
                0,
                self.reordering_size_audio,
                self.reordering_size_video,
                false,
                &self.codec_config,
            );
        }
    }

    fn handle_rtcp(&mut self, now: Instant, buf: &[u8]) -> Option<()> {
        let srtp: &mut SrtpContext = self.srtp_rx.as_mut()?;
        let unprotected = srtp.unprotect_rtcp(buf)?;

        // Validate the whole authenticated datagram before *any* native RTCP
        // dispatch, including REMB, repeated NACK, TWCC, stats and raw events.
        // The optional application policy is independent of native GCC.
        if self
            .rtcp_admission
            .is_some_and(|admit| !admit(&unprotected))
        {
            return None;
        }
        if let Some(observer) = &mut self.send_side_observer {
            observer.report(now, &unprotected);
        }
        Rtcp::read_packet(&unprotected, &mut self.feedback_rx);
        let mut need_configure_pacer = false;

        if let Some(raw_packets) = &mut self.raw_packets {
            for fb in &self.feedback_rx {
                raw_packets.push_back(Box::new(RawPacket::RtcpRx(fb.clone())));
            }
        }

        // Original DownTrack.handleRTCP consumes ReceiverReport packets only,
        // not report blocks piggybacked on SenderReport. Native RtcpFb erases
        // that origin for transport statistics, so preserve it for this owner.
        let rtt_reports = if self.receiver_report_rtt_enabled {
            self.feedback_rx
                .iter()
                .filter_map(|packet| match packet {
                    Rtcp::ReceiverReport(report) => {
                        Some(report.reports.iter().copied().collect::<Vec<_>>())
                    }
                    _ => None,
                })
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };

        for fb in RtcpFb::from_rtcp(self.feedback_rx.drain(..), self.require_fir_media_ssrc) {
            if self.remote_feedback_enabled
                && let RtcpFb::Remb(report) = fb
            {
                // Preserve the actual full identity; old dispatch used only first
                // SSRC and could discard a valid RTX-only/subset report.
                if self.pending_receiver_estimates.len() >= 256 {
                    self.pending_receiver_estimates.pop_front();
                    self.receiver_estimate_drops = self.receiver_estimate_drops.saturating_add(1);
                }
                self.pending_receiver_estimates
                    .push_back((Bitrate::from(report.bitrate as f64), report.ssrcs));
                continue;
            }
            if let RtcpFb::AppSpecificFeedback(asf) = fb {
                self.pending_app_feedback = Some(AppSpecificFeedback {
                    sender_ssrc: asf.sender_ssrc,
                    media_ssrc: asf.media_ssrc,
                    payload: asf.payload,
                });
                continue;
            }

            if let RtcpFb::Twcc(twcc) = fb {
                trace!("Handle TWCC: {:?}", twcc);
                let maybe_records = self.twcc_tx_register.apply_report(twcc, now);

                if let (Some(maybe_records), Some(bwe)) = (maybe_records, &mut self.bwe) {
                    bwe.update(maybe_records, now);
                }
                need_configure_pacer = true;

                // The funky thing about TWCC reports is that they are never stapled
                // together with other RTCP packet. If they were though, we want to
                // handle more packets.
                continue;
            }

            if let RtcpFb::Rrtr((rrtr, ssrc)) = fb {
                // DLRR responder (RFC 3611 §4.5): store the remote's RRTR so we can
                // reply with a DLRR, letting the remote compute RTT.
                let lrr = (rrtr.ntp_time.as_ntp_64() >> 16) as u32;
                let entry = LastRrtr {
                    lrr,
                    received_at: now,
                };

                // Update in place if this SSRC is already pending (preserves queue order).
                if let Some(existing) = self.pending_rrtrs.iter_mut().find(|(s, _)| *s == ssrc) {
                    existing.1 = entry;
                } else {
                    // Cap at 300 entries; drop the oldest if full.
                    if self.pending_rrtrs.len() >= MAX_PENDING_RRTRS {
                        self.pending_rrtrs.pop_front();
                    }
                    self.pending_rrtrs.push_back((ssrc, entry));
                }
                continue;
            }

            if fb.is_for_rx() {
                let Some(stream) = self.streams.stream_rx(&fb.ssrc()) else {
                    continue;
                };
                stream.handle_rtcp(now, fb);
            } else {
                let Some(stream) = self.streams.stream_tx(&fb.ssrc()) else {
                    continue;
                };
                if let RtcpFb::Nack(_, reports) = &fb {
                    // Go DownTrack counts every requested SN, including repeated
                    // requests, not one count per RTCP feedback packet.
                    let count = reports
                        .iter()
                        .map(|entry| 1 + entry.blp.count_ones() as u64)
                        .sum::<u64>();
                    self.node_traffic.nacks_received =
                        self.node_traffic.nacks_received.saturating_add(count);
                }
                stream.handle_rtcp(now, fb);
            }
        }

        for reports in rtt_reports {
            let mut report_losses: Vec<crate::ReceiverReportLoss> = Vec::new();
            for report in reports {
                if let Some(stream) = self.streams.stream_tx(&report.ssrc) {
                    if stream.ssrc() != report.ssrc {
                        continue;
                    }
                    // RTT change/LSR gates must not erase loss feedback. Bind to the
                    // real primary SSRC, retaining its current logical lifetime.
                    let sample = crate::ReceiverReportLoss {
                        source_id: stream.receiver_report_source_id(),
                        received_at: now,
                        mid: stream.mid(),
                        ssrc: *report.ssrc,
                        fraction_lost: report.fraction_lost,
                    };
                    // Fixed DownTrack filters each real RR to its primary SSRC,
                    // then MediaLossProxy takes max across that RR's valid blocks
                    // BEFORE its strict one-second callback. Preserve this cohort;
                    // the first low block cannot consume the callback prematurely.
                    if let Some(existing) = report_losses
                        .iter_mut()
                        .find(|old| old.ssrc == sample.ssrc && old.source_id == sample.source_id)
                    {
                        existing.fraction_lost = existing.fraction_lost.max(sample.fraction_lost);
                    } else {
                        report_losses.push(sample);
                    }
                    stream.observe_receiver_report_rtt(now, report);
                }
            }
            self.receiver_report_loss.extend(report_losses);
        }

        // DownTrack.handleRTCP reports at most one final changed RTT per
        // sender for this compound datagram, after processing all RR blocks.
        for stream in self.streams.streams_tx() {
            if let Some(sample) = stream.take_receiver_report_rtt() {
                if self.receiver_report_rtt_enabled {
                    self.receiver_report_rtt.push_back(sample);
                }
            }
        }

        // Not in the above if due to lifetime issues, still okay because the method
        // doesn't do anything when BWE isn't configured.
        if need_configure_pacer {
            self.configure_pacer();
        }

        Some(())
    }

    pub fn poll_event(&mut self) -> Option<Event> {
        if let Some(event) = self
            .send_side_observer
            .as_mut()
            .and_then(|observer| observer.poll())
        {
            return Some(Event::SendSideFeedback(event));
        }
        #[cfg(feature = "_internal_test_exports")]
        {
            if let Some(probe) = self.pending_probe.take() {
                return Some(Event::Probe(probe));
            }
        }

        let can_probe =
            self.bwe.is_some() && self.srtp_tx.is_some() && self.probe_media().is_some();
        if let Some(bwe) = &mut self.bwe {
            // Drain estimates even while unavailable so they cannot be delivered later.
            let estimate = bwe.poll_estimate();
            let estimate = match (can_probe, self.bwe_last_event) {
                (false, Some((last, true))) => Some(last),
                (false, _) => None,
                (true, Some((_, true))) => estimate,
                (true, _) => bwe.last_estimate(),
            };
            if let Some(estimate) = estimate {
                self.bwe_last_event = Some((estimate, can_probe));
                let kind = BweKind::Twcc {
                    estimate,
                    can_probe,
                };
                return Some(Event::EgressBitrateEstimate(kind));
            }
        }

        // If we're not ready to flow media, don't send any events.
        if !self.ready_for_srtp() {
            return None;
        }

        if let Some(raw_packets) = &mut self.raw_packets {
            if let Some(p) = raw_packets.pop_front() {
                return Some(Event::RawPacket(p));
            }
        }

        if let Some(feedback) = self.pending_app_feedback.take() {
            return Some(Event::AppSpecificFeedback(feedback));
        }

        // This must be before pending_packet.take() since we need to emit the unpaused event
        // before the first packet causing the unpause.
        if let Some(paused) = self.streams.poll_stream_paused() {
            return Some(Event::StreamPaused(paused));
        }

        if let Some(packet) = self.pending_media_packet.take() {
            return Some(Event::MediaPacket(packet));
        }
        if self.rtp_mode {
            if let Some(packet) = self.pending_packet.take() {
                return Some(Event::RtpPacket(packet));
            }
        }

        if let Some(req) = self.streams.poll_keyframe_request() {
            return Some(Event::KeyframeRequest(req));
        }

        if let Some(report) = self.streams.poll_sender_feedback() {
            return Some(Event::SenderFeedback(report));
        }

        if let Some((estimate, ssrcs)) = self.pending_receiver_estimates.pop_front() {
            return Some(Event::EgressBitrateEstimate(BweKind::ReceiverEstimate {
                estimate,
                ssrcs,
            }));
        }
        if let Some((mid, bitrate)) = self.streams.poll_remb_request() {
            return Some(Event::EgressBitrateEstimate(BweKind::Remb {
                estimate: bitrate,
                mid,
            }));
        }

        for media in &mut self.medias {
            if media.need_open_event {
                media.need_open_event = false;

                return Some(Event::MediaAdded(MediaAdded {
                    mid: media.mid(),
                    kind: media.kind(),
                    direction: media.direction(),
                    simulcast: media.simulcast().map(|s| s.clone().into()),
                }));
            }

            if media.need_changed_event {
                media.need_changed_event = false;
                return Some(Event::MediaChanged(MediaChanged {
                    mid: media.mid(),
                    direction: media.direction(),
                }));
            }
        }

        None
    }

    pub fn poll_event_fallible(&mut self, now: Instant) -> Result<Option<Event>, RtcError> {
        // Not relevant in rtp_mode, where the packets are picked up by poll_event().
        if self.rtp_mode {
            return Ok(None);
        }

        for media in &mut self.medias {
            let timeout = if media.kind().is_video() {
                self.reordering_timeout_video
            } else {
                self.reordering_timeout_audio
            };
            if let Some(e) = media.poll_sample(&self.codec_config, now, timeout)? {
                return Ok(Some(Event::MediaData(e)));
            }
        }

        Ok(None)
    }

    fn ready_for_srtp(&self) -> bool {
        self.srtp_rx.is_some() && self.srtp_tx.is_some()
    }

    pub fn poll_datagram(&mut self, now: Instant) -> Option<net::DatagramSend> {
        self.remote_padding.expire(now);
        // Time must have progressed forward from start value.
        if now == already_happened() {
            return None;
        }

        let x = if self.rtp_closing {
            self.poll_feedback(now)
        } else {
            None.or_else(|| self.poll_feedback(now))
                .or_else(|| self.poll_packet(now))
                .or_else(|| self.poll_remote_padding(now))
        };

        if let Some(x) = &x {
            // In RTP mode we trust the API user feeds the RTP packet sizes they
            // need for the MTU they are targeting. This warning is only for when
            // str0m does the RTP packetization.
            if !self.rtp_mode && x.len() > self.mtu_warn() {
                warn!("RTP above MTU {}: {}", self.mtu_warn(), x.len());
            }
        }

        x
    }

    /// To be called in lieu of [`Self::poll_datagram`] when the owner is not in a position to transmit any
    /// generated feedback, and thus such feedback should be dropped.
    pub fn clear_feedback(&mut self) {
        self.feedback_rx.clear();
        self.feedback_tx.clear();
    }

    pub fn close_rtp(&mut self) {
        if self.rtp_closing {
            return;
        }

        self.rtp_closing = true;
        if let Some(active) = self.remote_padding.observation() {
            self.remote_padding.cancel(active.id);
        }
        self.clear_feedback();
        self.streams.reset_send_buffers();

        for reports in ReportList::lists_from_iter(self.streams.local_sender_ssrcs()) {
            self.feedback_tx
                .push_back(Rtcp::Goodbye(Goodbye { reports }));
        }
    }

    pub fn is_rtp_closed(&self) -> bool {
        self.rtp_closing && self.feedback_tx.is_empty()
    }

    fn poll_feedback(&mut self, now: Instant) -> Option<net::DatagramSend> {
        if self.feedback_tx.is_empty() {
            return None;
        }

        // Round to nearest multiple of 4 bytes.
        let encryptable_mtu: usize = (self.mtu() - SRTCP_OVERHEAD) & !3;
        assert!(encryptable_mtu % 4 == 0);

        let mut data = vec![0_u8; encryptable_mtu];

        let mut raw_packets = self.raw_packets.as_mut();
        let twcc_register = &mut self.twcc_rx_register;
        let output = move |fb| {
            if let Rtcp::Twcc(report) = &fb {
                twcc_register.observe_emitted(report, now);
            }
            if let Some(raw_packets) = &mut raw_packets {
                raw_packets.push_back(Box::new(RawPacket::RtcpTx(fb)));
            }
        };

        let len = Rtcp::write_packet(&mut self.feedback_tx, &mut data, output);

        if len == 0 {
            return None;
        }

        data.truncate(len);

        let srtp = self.srtp_tx.as_mut()?;
        let protected = srtp.protect_rtcp(&data);

        assert!(
            protected.len() < self.mtu(),
            "Encrypted SRTCP should be less than MTU"
        );

        Some(protected.into())
    }

    fn poll_packet(&mut self, now: Instant) -> Option<DatagramSend> {
        self.srtp_tx.as_ref()?;
        // Figure out which, if any, queue to poll
        // The cluster_id is captured by the pacer at poll time, before register_send() might clear it
        let (midrid, cluster_id) = self.pacer.poll_queue()?;
        self.poll_packet_on(now, midrid, cluster_id, false)
    }

    fn poll_packet_on(
        &mut self,
        now: Instant,
        midrid: MidRid,
        cluster_id: Option<crate::rtp_::TwccClusterId>,
        independent_padding: bool,
    ) -> Option<DatagramSend> {
        let srtp_tx = self.srtp_tx.as_mut()?;
        let stream = self.streams.stream_tx_by_midrid(midrid)?;
        let Some(media) = self.medias.iter().find(|m| m.mid() == stream.mid()) else {
            trace!("Pacer pointed to mid {} which has no media", midrid.mid());
            return None;
        };
        let buf = &mut self.poll_packet_buf;
        let twcc_seq = self.twcc;

        let params = &self.codec_config;
        let exts = media.remote_extmap();
        stream.retained_opus_nack = media.retained_opus_nack;

        // TWCC might not be enabled for this m-line. Firefox do use TWCC, but not
        // for audio. This is indiciated via the SDP.
        let bwe_participating = stream.bwe_participating();
        let twcc_enabled = self.send_transport_cc
            && bwe_participating
            && exts.id_of(Extension::TransportSequenceNumber).is_some();
        let twcc = twcc_enabled.then_some(&mut self.twcc);

        let receipt = if independent_padding {
            stream.poll_independent_padding(now, exts, twcc, params, buf)?
        } else {
            stream.poll_packet(now, exts, twcc, params, buf)?
        };

        let PacketReceipt {
            header,
            seq_no,
            is_padding,
            is_retransmission,
            payload_size,
        } = receipt;

        trace!(payload_size, is_padding, "Poll RTP: {:?}", header);

        #[cfg(feature = "_internal_dont_use_log_stats")]
        {
            let kind = if is_padding { "padding" } else { "media" };

            crate::log_stat!("PACKET_SENT", header.ssrc, payload_size, kind);
        }

        if !independent_padding && let Some(raw_packets) = &mut self.raw_packets {
            raw_packets.push_back(Box::new(RawPacket::RtpTx(header.clone(), buf.clone())));
        }

        let primary = header.ssrc == stream.ssrc() && !is_retransmission;
        let plain_size = buf.len();
        let pion_payload_size =
            plain_size
                .saturating_sub(header.header_len)
                .saturating_sub(if header.has_padding {
                    usize::from(buf.last().copied().unwrap_or(0))
                } else {
                    0
                });
        let protected = srtp_tx.protect_rtp(buf, &header, *seq_no);

        if independent_padding
            && (protected.len() > crate::remote_padding::MAX_PADDING_PACKET
                || protected.len() as u64 > self.remote_padding.remaining())
        {
            self.remote_padding
                .stop(crate::PaddingStatus::EncodingLimit);
            self.twcc = twcc_seq;
            return None;
        }
        if independent_padding && let Some(raw_packets) = &mut self.raw_packets {
            raw_packets.push_back(Box::new(RawPacket::RtpTx(header.clone(), buf.clone())));
        }

        if !header.ssrc.is_probe() {
            let rtx_prefix = if is_retransmission && header.ssrc != stream.ssrc() {
                2
            } else {
                0
            };
            self.node_traffic.send_rtp(
                plain_size.saturating_sub(rtx_prefix),
                is_retransmission,
                is_padding,
            );
        }
        self.rtp_output_started = true;
        if let Some(observer) = &mut self.send_side_observer {
            observer.push(crate::SendSideFeedback::Sent(crate::SendSidePacket {
                at: now,
                sequence: header.sequence_number,
                ssrc: *header.ssrc,
                header_bytes: header.header_len,
                payload_bytes: pion_payload_size,
                twcc_negotiated: twcc_enabled,
                transport_sequence: header.ext_vals.transport_cc,
            }));
        }
        if self.remote_feedback_enabled {
            stream.observe_remote_output(
                *seq_no,
                plain_size,
                primary,
                is_padding,
                self.send_buffer_video,
                now,
            );
        }

        // Validation budgets and measures the same actually protected datagram
        // bytes. Preserve payload-byte pacing for ordinary Google probes/media.
        let validation = self
            .bwe
            .as_ref()
            .is_some_and(|bwe| bwe.is_validation_cluster(cluster_id));
        let paced_bytes = if validation {
            protected.len()
        } else {
            payload_size
        };
        if independent_padding {
            self.remote_padding.sent(now, protected.len());
        } else {
            self.pacer.register_send(now, paced_bytes.into(), midrid);
        }
        if let Some(event) = self.pacer.take_validation_credit_discard()
            && let Some(bwe) = self.bwe.as_mut()
        {
            bwe.on_validation_credit_discard(
                event.cluster_id,
                twcc_seq,
                event.sent_at,
                event.discarded,
            );
        }

        if twcc_enabled {
            if validation && let (Some(id), Some(bwe)) = (cluster_id, self.bwe.as_mut()) {
                bwe.on_validation_sent(id, twcc_seq, now);
            }
            let packet_id = if let Some(cluster) = cluster_id {
                TwccPacketId::with_cluster(twcc_seq, cluster)
            } else {
                TwccPacketId::new(twcc_seq)
            };
            self.twcc_tx_register.register_seq_with_packet_size(
                packet_id,
                now,
                payload_size,
                protected.len(),
            );
        }

        // Update BWE subsystem
        if bwe_participating && let Some(bwe) = self.bwe.as_mut() {
            bwe.on_media_sent(payload_size.into(), is_padding, now);
        }

        if !is_padding && !header.ssrc.is_probe() {
            self.media_bytes_tx += payload_size as u64;
        }

        Some(protected.into())
    }

    pub fn poll_timeout(&mut self) -> (Option<Instant>, Reason) {
        if self.rtp_closing {
            return if self.feedback_tx.is_empty() {
                (None, Reason::NotHappening)
            } else {
                (Some(already_happened()), Reason::Feedback)
            };
        }

        let feedback_at = self.regular_feedback_at();
        let nack_at = self.nack_at();
        let twcc_at = self.twcc_at();
        let pacing_at = self.pacer.poll_timeout();
        let packetize_at = self.medias.iter().filter_map(|m| m.poll_timeout()).min();
        let receive_at = self
            .medias
            .iter_mut()
            .filter_map(|media| {
                let timeout = if media.kind().is_video() {
                    self.reordering_timeout_video
                } else {
                    self.reordering_timeout_audio
                };
                media.poll_receive_timeout(timeout)
            })
            .min();
        let paused_at = self.paused_at();
        let send_stream_at = self.streams.send_stream();

        // Gives us built-in reason
        let bwe_at = self
            .bwe
            .as_ref()
            .map(|bwe| bwe.poll_timeout())
            // We should never see this reason.
            .unwrap_or((None, Reason::BweDelayControl));

        (feedback_at, Reason::Feedback)
            .soonest((nack_at, Reason::Nack))
            .soonest((twcc_at, Reason::Twcc))
            .soonest(pacing_at)
            .soonest((receive_at, Reason::ReceiveReorder))
            .soonest((packetize_at, Reason::Packetize))
            .soonest((paused_at, Reason::PauseCheck))
            .soonest((send_stream_at, Reason::SendStream))
            .soonest(bwe_at)
            .soonest((
                self.remote_padding.timeout(),
                Reason::Pacer(crate::pacer::PacerReason::Handle),
            ))
    }

    pub fn has_mid(&self, mid: Mid) -> bool {
        self.medias.iter().any(|m| m.mid() == mid)
    }

    fn regular_feedback_at(&self) -> Option<Instant> {
        self.streams.regular_feedback_at(self.intervals)
    }

    fn paused_at(&self) -> Option<Instant> {
        self.streams.paused_at()
    }

    fn nack_at(&mut self) -> Option<Instant> {
        if !self.streams.any_nack_enabled() {
            return None;
        }

        Some(self.last_nack + NACK_MIN_INTERVAL)
    }

    fn twcc_at(&self) -> Option<Instant> {
        if self.livekit_publisher_twcc.is_some() {
            return None;
        }
        let is_receiving = self.streams.is_receiving();
        if is_receiving && self.enable_twcc_feedback && self.twcc_rx_register.has_unreported() {
            Some(self.last_twcc + TWCC_INTERVAL)
        } else {
            None
        }
    }

    pub(crate) fn enable_livekit_publisher_twcc(&mut self) {
        self.twcc_rx_register.enable_publisher_history();
        self.livekit_publisher_twcc = Some(LiveKitPublisherTwcc {
            sender_ssrc: self.streams.new_ssrc(),
            last_report: None,
            packets_held: 0,
        });
    }

    pub fn enable_twcc_feedback(&mut self) {
        if !self.enable_twcc_feedback {
            debug!("Enable TWCC feedback");
            self.enable_twcc_feedback = true;
        }
    }

    pub fn visit_stats(&mut self, now: Instant, snapshot: &mut StatsSnapshot) {
        for stream in self.streams.streams_tx() {
            stream.visit_stats(snapshot, now);
        }

        for stream in self.streams.streams_rx() {
            stream.visit_stats(snapshot, now);
        }

        snapshot.tx = self.media_bytes_tx;
        snapshot.rx = self.media_bytes_rx;
        snapshot.bwe_tx = self.bwe.as_ref().and_then(|bwe| bwe.last_estimate());

        snapshot.egress_loss_fraction = self.twcc_tx_register.loss(Duration::from_secs(1), now);
        snapshot.rtt = self.twcc_tx_register.rtt();
        snapshot.ingress_loss_fraction = self.twcc_rx_register.loss();
    }

    pub fn set_incoming_twcc_observations(&mut self, enabled: bool) {
        self.twcc_rx_register.set_observations_enabled(enabled);
    }

    pub fn incoming_twcc_snapshot(&self) -> Option<crate::bwe::BweIncomingTwccObservation> {
        self.twcc_rx_register.incoming_twcc_snapshot()
    }

    pub fn bwe_snapshot(&self) -> Option<crate::bwe::BweSnapshot> {
        self.bwe.as_ref().map(Bwe::snapshot)
    }

    pub fn set_bwe_desired_bitrate(&mut self, desired_bitrate: Bitrate) {
        if let Some(bwe) = self.bwe.as_mut() {
            bwe.set_desired_bitrate(desired_bitrate);
            self.configure_pacer();
        }
    }

    #[cfg(feature = "_internal_test_exports")]
    pub(crate) fn protect_rtcp_test(&mut self, plain: &[u8]) -> Option<Vec<u8>> {
        // Caller supplies at least a complete RTCP header and sender SSRC.
        if plain.len() < 8 {
            return None;
        }
        Some(self.srtp_tx.as_mut()?.protect_rtcp(plain))
    }

    pub(crate) fn set_rtcp_admission(&mut self, admission: Option<fn(&[u8]) -> bool>) {
        self.rtcp_admission = admission;
    }

    pub(crate) fn set_send_side_feedback(&mut self, enabled: bool) {
        if enabled && self.send_side_observer.is_none() {
            assert!(
                !self.rtp_output_started,
                "select application estimator before first RTP output"
            );
            self.send_side_observer = Some(crate::send_side_feedback::SendSideObserver::default());
        } else if !enabled {
            self.send_side_observer = None;
        }
    }
    pub(crate) fn set_remote_feedback_meter(&mut self, enabled: bool) {
        self.remote_feedback_enabled = enabled;
    }
    pub(crate) fn receiver_estimate_drops(&self) -> u64 {
        self.receiver_estimate_drops
    }
    pub(crate) fn remote_feedback_stats(&self) -> Vec<crate::RemoteFeedbackStats> {
        self.streams.remote_feedback_stats()
    }

    pub fn configured_bwe_pacing_rate(&self) -> Option<Bitrate> {
        self.bwe.as_ref()?;
        self.pacer.configured_pacing_rate()
    }

    pub fn set_automatic_probing(&mut self, enabled: bool, now: Instant) {
        let Some(bwe) = self.bwe.as_mut() else {
            return;
        };
        if !bwe.set_automatic_probing(enabled) {
            return;
        }
        if !enabled {
            let (active_cancelled, cancelled) = self.pacer.cancel_automatic_probes();
            for id in cancelled {
                bwe.end_probe(now, id);
            }
            if active_cancelled {
                self.streams.clear_pending_padding();
                self.streams.set_probe_media(None);
            }
        }
    }

    pub fn set_periodic_alr_probing(&mut self, enabled: bool, now: Instant) {
        let Some(bwe) = self.bwe.as_mut() else {
            return;
        };
        bwe.set_periodic_alr_probing(enabled, now);
        if !enabled {
            let (active_cancelled, cancelled) = self.pacer.cancel_periodic_alr_probes();
            for id in cancelled {
                bwe.end_probe(now, id);
            }
            if active_cancelled {
                self.streams.clear_pending_padding();
                self.streams.set_probe_media(None);
            }
        }
    }

    pub fn start_bwe_validation(&mut self, target: Bitrate, now: Instant) -> Option<u64> {
        if self.srtp_tx.is_none() || self.probe_media().is_none() {
            return None;
        }
        let bwe = self.bwe.as_mut()?;
        let config = bwe.begin_validation(target, now)?;
        self.pacer.cancel_probes(false);
        while let Some(id) = self.pacer.check_probe_complete(now) {
            bwe.end_probe(now, id);
        }
        self.streams.clear_pending_padding();
        self.streams.set_probe_media(None);
        self.pacer.start_probe(config);
        Some(*config.cluster())
    }

    pub fn start_bwe_validation_with_pacing(
        &mut self,
        floor: Bitrate,
        pacing: Bitrate,
        now: Instant,
    ) -> Option<u64> {
        if self.srtp_tx.is_none() || self.probe_media().is_none() {
            return None;
        }
        let bwe = self.bwe.as_mut()?;
        let config = bwe.begin_validation_with_pacing(floor, pacing, now)?;
        self.pacer.cancel_probes(false);
        while let Some(id) = self.pacer.check_probe_complete(now) {
            bwe.end_probe(now, id);
        }
        self.streams.clear_pending_padding();
        self.streams.set_probe_media(None);
        self.pacer.start_probe(config);
        Some(*config.cluster())
    }

    pub fn cancel_bwe_validation(&mut self, now: Instant) {
        if let Some(bwe) = self.bwe.as_mut() {
            bwe.cancel_validation(now);
        }
        self.pacer.cancel_probes(true);
        self.streams.clear_pending_padding();
    }

    pub fn reset_bwe(&mut self, init_bitrate: Bitrate) {
        // New estimator IDs must never attach to an old train. This resets only
        // probe/padding state, preserving queued real media, RTX and SRTP keys.
        self.pacer.cancel_probes(false);
        while self
            .pacer
            .check_probe_complete(already_happened())
            .is_some()
        {}
        self.streams.clear_pending_padding();
        self.streams.set_probe_media(None);
        if let Some(bwe) = self.bwe.as_mut() {
            bwe.reset(init_bitrate);
        }
    }

    #[allow(dead_code)]
    pub fn line_count(&self) -> usize {
        self.medias.len() + if self.app.is_some() { 1 } else { 0 }
    }

    pub fn add_media(&mut self, media: Media) {
        self.medias.push(media);
    }

    pub fn medias(&self) -> &[Media] {
        &self.medias
    }

    pub fn remove_media(&mut self, mid: Mid) {
        if self
            .pending_media_packet
            .as_ref()
            .is_some_and(|packet| packet.mid == mid)
        {
            self.pending_media_packet = None;
        }

        self.medias.retain(|media| media.mid() != mid);
        self.streams.remove_streams_by_mid(mid);
    }

    fn configure_pacer(&mut self) {
        let Some(bwe) = self.bwe.as_mut() else {
            return;
        };

        let Some(current_estimate) = bwe.last_estimate() else {
            // No estimate yet, no padding
            return;
        };
        let is_overuse = bwe.is_overusing();

        let has_active_media = self.has_active_outgoing_media();

        // Calculate pacing and padding rates
        let result = self
            .pacer_control
            .calculate(has_active_media, current_estimate, is_overuse);

        self.pacer.set_padding_rate(result.padding_rate);
        self.pacer.set_pacing_rate(result.pacing_rate);
    }

    fn has_active_outgoing_media(&self) -> bool {
        self.medias
            .iter()
            .any(|m| m.direction().is_sending() && !m.disabled())
    }

    pub fn media_by_mid(&self, mid: Mid) -> Option<&Media> {
        self.medias.iter().find(|m| m.mid() == mid)
    }

    pub fn media_by_mid_mut(&mut self, mid: Mid) -> Option<&mut Media> {
        self.medias.iter_mut().find(|m| m.mid() == mid)
    }

    fn do_payload(&mut self, now: Instant) -> Result<(), RtcError> {
        let mtu = self.mtu();
        for m in &mut self.medias {
            m.do_payload(
                now,
                &mut self.streams,
                &self.codec_config,
                self.vp9_packetizer_mode,
                mtu,
                self.codec_config.red_distances(),
            )?;
        }

        Ok(())
    }

    /// Remove the sticky default for future senders while preserving Opus
    /// NACK on already negotiated sending m-lines.
    pub fn disable_opus_nack(&mut self) {
        let Some(opus_pt) = self
            .codec_config
            .iter()
            .find(|payload| payload.spec().codec == crate::format::Codec::Opus && payload.fb_nack())
            .map(|payload| payload.pt())
        else {
            return;
        };
        for payload in self.codec_config.iter_mut() {
            if payload.spec().codec == crate::format::Codec::Opus {
                payload.set_fb_nack(false);
            }
        }
        for media in self
            .medias
            .iter_mut()
            .filter(|media| media.kind().is_audio())
        {
            if media.direction().is_sending()
                && !media.disabled()
                && media.remote_pts().contains(&opus_pt)
            {
                media.retained_opus_nack = true;
            } else {
                for stream in self.streams.streams_tx_by_mid(media.mid()) {
                    stream.clear_retransmissions();
                }
            }
        }
    }

    /// Toggle RFC 2198 RED wrapping on the send side at runtime. No renegotiation: RED availability
    /// is fixed at negotiation (or via DirectAPI); this only decides whether we wrap right now.
    /// Returns whether the setting changed.
    pub fn set_red_send(&mut self, mid: Mid, enabled: bool) -> bool {
        let Some(media) = self.media_by_mid_mut(mid) else {
            return false;
        };
        media.set_red_send(enabled)
    }

    pub fn set_direction(&mut self, mid: Mid, direction: Direction) -> bool {
        let Some(media) = self.media_by_mid_mut(mid) else {
            return false;
        };
        let old_dir = media.direction();
        if old_dir == direction {
            return false;
        }

        media.set_direction(direction);

        if old_dir.is_sending() && !direction.is_sending() {
            self.streams.reset_buffers_tx(mid);
        }

        let max_seq_lookup = make_max_seq_lookup(&self.max_rx_seq_lookup);
        if old_dir.is_receiving() && !direction.is_receiving() {
            self.streams.reset_buffers_rx(mid, max_seq_lookup);
        }

        true
    }

    pub fn stop_media(&mut self, mid: Mid) -> bool {
        let Some(media) = self.media_by_mid_mut(mid) else {
            return false;
        };
        if media.disabled() {
            return false;
        }

        media.mark_stopped();
        self.set_direction(mid, Direction::Inactive);

        true
    }

    pub fn is_request_keyframe_possible(&self, kind: KeyframeRequestKind) -> bool {
        // TODO: It's possible to have different set of feedback enabled for different
        // payload types. I.e. we could have FIR enabled for H264, but not for VP8.
        // We might want to make this check more fine grained by testing which PT is
        // in "active use" right now.
        self.codec_config.iter().any(|r| match kind {
            KeyframeRequestKind::Pli => r.fb_pli,
            KeyframeRequestKind::Fir => r.fb_fir,
        })
    }

    /// Checks whether the SRTP contexts are up.
    pub fn is_connected(&self) -> bool {
        self.srtp_rx.is_some() && self.srtp_tx.is_some()
    }
}

pub struct PacketReceipt {
    pub header: RtpHeader,
    pub seq_no: SeqNo,
    pub is_padding: bool,
    pub is_retransmission: bool,
    pub payload_size: usize,
}

/// Find the PayloadParams for the given Pt, either when the Pt is the main Pt for the Codec or
/// when it's the RTX Pt.
fn main_payload_params(c: &CodecConfig, pt: Pt) -> Option<&PayloadParams> {
    c.iter()
        .find(|p| p.pt == pt || p.resend == Some(pt) || p.red == Some(pt))
}

/// A decoded RED (RFC 2198) packet: the primary block plus any redundant (older) blocks.
struct UnRed<'a> {
    primary: &'a [u8],
    redundant: Vec<RedBlock<'a>>,
}

/// Decode a RED packet, rewrite the header to the primary PT, and return the primary payload
/// plus the redundant blocks. Returns `None` (drop) when the RED structure is malformed.
fn un_red<'a>(header: &mut RtpHeader, data: &'a [u8], pt: Pt) -> Option<UnRed<'a>> {
    let blocks = RedDecoder::decode(data).ok()?;
    let mut primary = None;
    let mut redundant = Vec::new();
    for b in blocks {
        if b.is_primary {
            // RFC 2198 lets redundant blocks use a different PT, but the primary block must carry
            // the codec this RED stream negotiated (`pt`, e.g. Opus). A mismatch means this is not
            // the primary we expect, so drop the packet rather than mislabel a foreign codec (e.g.
            // reinterpret a PCMU primary as Opus).
            if b.pt != *pt {
                return None;
            }
            primary = Some(b.payload);
        } else {
            redundant.push(b);
        }
    }
    header.payload_type = pt;
    Some(UnRed {
        primary: primary?,
        redundant,
    })
}

fn make_max_seq_lookup(map: &HashMap<Ssrc, SeqNo>) -> impl Fn(Ssrc) -> Option<SeqNo> + '_ {
    |ssrc| map.get(&ssrc).cloned()
}

fn update_max_seq(map: &mut HashMap<Ssrc, SeqNo>, ssrc: Ssrc, seq_no: SeqNo) {
    let current = map.entry(ssrc).or_insert(seq_no);
    if seq_no > *current {
        *current = seq_no;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RtcConfig;
    use crate::io::DATAGRAM_MTU_TARGET;
    use crate::packet::RedEncoder;

    #[test]
    fn livekit_publisher_twcc_matches_packet_trigger_thresholds() {
        let start = Instant::now();
        let mut session = Session::new(&RtcConfig::default());
        session.enable_twcc_feedback();
        session.enable_livekit_publisher_twcc();
        let profile = session.livekit_publisher_twcc.as_mut().unwrap();
        let sender_ssrc = profile.sender_ssrc;
        let mut receipts = TwccRecvRegister::new(100);
        receipts.enable_publisher_history();
        let mut feedback = VecDeque::new();
        let mut packet = RtpHeader {
            ssrc: 123.into(),
            marker: false,
            ..RtpHeader::default()
        };
        let record = |profile: &mut LiveKitPublisherTwcc,
                      receipts: &mut TwccRecvRegister,
                      feedback: &mut VecDeque<Rtcp>,
                      packet: &RtpHeader,
                      sequence: u16,
                      elapsed_ms: u64| {
            profile.record(
                receipts,
                packet,
                sequence,
                start + Duration::from_millis(elapsed_ms),
                1100,
                feedback,
            );
        };

        for sequence in 1..=20 {
            record(profile, &mut receipts, &mut feedback, &packet, sequence, 0);
        }
        record(profile, &mut receipts, &mut feedback, &packet, 20, 101);
        assert!(feedback.is_empty(), "duplicate does not count as a receipt");
        record(profile, &mut receipts, &mut feedback, &packet, 21, 101);
        assert_eq!(feedback.len(), 1);
        feedback.clear();

        for sequence in 22..=42 {
            record(
                profile,
                &mut receipts,
                &mut feedback,
                &packet,
                sequence,
                149,
            );
        }
        packet.marker = true;
        record(profile, &mut receipts, &mut feedback, &packet, 43, 150);
        assert!(feedback.is_empty(), "marker arrives at 49 ms");
        packet.ssrc = 456.into();
        record(profile, &mut receipts, &mut feedback, &packet, 44, 151);
        let Some(Rtcp::Twcc(report)) = feedback.pop_front() else {
            panic!("marker at 50 ms should trigger a report");
        };
        assert_eq!(report.sender_ssrc, sender_ssrc);
        assert_eq!(report.ssrc, packet.ssrc);
        assert_eq!(report.base_seq, 22);
        assert_eq!(report.status_count, 23);

        packet.marker = false;
        for sequence in 45..=144 {
            record(
                profile,
                &mut receipts,
                &mut feedback,
                &packet,
                sequence,
                151,
            );
        }
        assert!(
            feedback.is_empty(),
            "100 held packets does not trigger a report"
        );
        record(profile, &mut receipts, &mut feedback, &packet, 145, 151);
        assert_eq!(feedback.len(), 1, "101 held packets triggers a report");
        feedback.clear();

        for sequence in 146..=166 {
            record(
                profile,
                &mut receipts,
                &mut feedback,
                &packet,
                sequence,
                250,
            );
        }
        assert!(feedback.is_empty(), "elapsed 99 ms without a marker");
        record(profile, &mut receipts, &mut feedback, &packet, 167, 251);
        assert_eq!(feedback.len(), 1, "elapsed 100 ms without a marker");

        // An armed timer cannot flush a trailing packet in the LiveKit profile.
        session.twcc_rx_register = receipts;
        session.twcc_rx_register.update_seq(168.into(), start);
        assert!(session.twcc_rx_register.has_unreported());
        assert_eq!(session.twcc_at(), None);
    }

    #[test]
    fn livekit_publisher_twcc_matches_pion_receipt_history() {
        struct Case {
            start: Instant,
            profile: LiveKitPublisherTwcc,
            receipts: TwccRecvRegister,
            feedback: VecDeque<Rtcp>,
        }

        impl Case {
            fn new() -> Self {
                let mut receipts = TwccRecvRegister::new(100);
                receipts.enable_publisher_history();
                Self {
                    start: Instant::now(),
                    profile: LiveKitPublisherTwcc {
                        sender_ssrc: 10.into(),
                        last_report: None,
                        packets_held: 0,
                    },
                    receipts,
                    feedback: VecDeque::new(),
                }
            }

            fn push(&mut self, seq: u64, us: u64) {
                self.profile.record(
                    &mut self.receipts,
                    &RtpHeader {
                        ssrc: 123.into(),
                        ..RtpHeader::default()
                    },
                    seq as u16,
                    self.start + Duration::from_micros(us),
                    1100,
                    &mut self.feedback,
                );
            }

            fn check(
                &mut self,
                label: &str,
                packets: usize,
                spans: &[(u16, u16, bool)],
            ) -> Vec<Instant> {
                let mut actual = Vec::<(u16, u16, bool)>::new();
                let mut times = Vec::new();
                let mut sizes = Vec::new();
                while let Some(packet) = self.feedback.pop_front() {
                    let mut encoded = [0u8; 1100];
                    let size =
                        Rtcp::write_packet(&mut VecDeque::from([packet]), &mut encoded, |packet| {
                            assert!(matches!(packet, Rtcp::Twcc(_)))
                        });
                    assert!((24..=1100).contains(&size));
                    sizes.push(size);
                    let Rtcp::Twcc(report) = Rtcp::try_from(&encoded[..size]).unwrap() else {
                        panic!("expected wire TWCC");
                    };
                    assert_eq!(report.ssrc, 123.into());
                    let base = u64::from(report.base_seq).into();
                    let count = usize::from(report.status_count);
                    let entries: Vec<_> = report.into_iter(self.start, base).collect();
                    assert_eq!(entries.len(), count);
                    for (seq, _, time) in entries {
                        let seq = *seq as u16;
                        let received = time.is_some();
                        times.extend(time);
                        if let Some((base, count, previous)) = actual.last_mut() {
                            if base.wrapping_add(*count) == seq && *previous == received {
                                *count += 1;
                                continue;
                            }
                        }
                        actual.push((seq, 1, received));
                    }
                }
                assert_eq!(sizes.len(), packets, "{label}");
                assert_eq!(actual, spans, "{label}");
                println!("{label}: {actual:?}; bytes={sizes:?}");
                times
            }
        }

        let mut burst = Case::new();
        for seq in 1..=324 {
            burst.push(seq, seq * 250);
        }
        burst.check("high-rate-initial-4-reports", 4, &[(1, 324, true)]);
        burst.push(10, 100_000);
        for seq in 325..=344 {
            burst.push(seq, 181_000 + (seq - 325) * 250);
        }
        burst.check("retained-duplicate-does-not-trigger-on-20-new", 0, &[]);
        burst.push(345, 186_000);
        let times = burst.check(
            "retained-duplicate-rewinds-336-statuses-with-original-timestamp",
            1,
            &[(10, 336, true)],
        );
        let original_delta = times[1] - times[0];
        assert_eq!(original_delta, Duration::from_micros(250));
        println!(
            "retained-duplicate-relative-micros={}",
            original_delta.as_micros()
        );

        for (age, label, expected) in [
            (
                499_999,
                "499999us-old-prefix-retained",
                &[(10, 33, true)][..],
            ),
            (
                500_000,
                "500000us-old-prefix-expired",
                &[(10, 1, true), (11, 11, false), (22, 20, true)][..],
            ),
        ] {
            let mut window = Case::new();
            for seq in 1..=21 {
                window.push(seq, 0);
            }
            window.check("window-initial", 1, &[(1, 21, true)]);
            window.push(22, age);
            window.push(10, age);
            for seq in 23..=42 {
                window.push(seq, age + 250);
            }
            window.check(label, 1, expected);
        }

        let mut wrap = Case::new();
        for seq in (65_525..=65_546).filter(|seq| *seq != 65_532) {
            wrap.push(seq, (seq - 65_525) * 250);
        }
        wrap.check(
            "wrap-with-gap",
            1,
            &[(65_525, 7, true), (65_532, 1, false), (65_533, 14, true)],
        );
        wrap.push(65_532, 100_000);
        wrap.push(65_530, 100_250);
        for seq in 65_547..=65_566 {
            wrap.push(seq, 100_500 + (seq - 65_547) * 250);
        }
        wrap.check(
            "late-wrap-hole-repaired-and-duplicate-rewind",
            1,
            &[(65_530, 37, true)],
        );

        let mut cap = Case::new();
        for seq in [65_000, 65_200, 64_500, 32_000] {
            cap.push(seq, 0);
        }
        for seq in 65_201..=65_217 {
            cap.push(seq, 250);
        }
        cap.check(
            "late-unwrapping-and-exact-32768-cap",
            1,
            &[
                (64_500, 1, true),
                (64_501, 499, false),
                (65_000, 1, true),
                (65_001, 199, false),
                (65_200, 18, true),
            ],
        );
        for seq in 65_218..=65_238 {
            cap.push(seq, 100_250);
        }
        cap.check("exact-32768-cap-next-report", 1, &[(65_218, 21, true)]);

        for (sequence, label, expected) in [
            (
                32_633,
                "map-range-exactly-32768-includes-edge",
                &[
                    (32_633, 1, true),
                    (32_634, 32_747, false),
                    (65_381, 20, true),
                ][..],
            ),
            (
                32_632,
                "map-range-32769-excludes-edge",
                &[(65_381, 20, true)][..],
            ),
        ] {
            let mut edge = Case::new();
            for seq in (65_382..=65_400).rev() {
                edge.push(seq, 0);
            }
            edge.push(sequence, 250);
            edge.push(65_381, 500);
            edge.check(label, 1, expected);
        }

        let mut gap = Case::new();
        for seq in 1..=21 {
            gap.push(seq, 0);
        }
        gap.check("long-gap-first", 1, &[(1, 21, true)]);
        for seq in 20_000..=20_020 {
            gap.push(seq, 100_000);
        }
        gap.check(
            "long-gap-roundtrip-13-bit-run-length",
            1,
            &[(22, 19_978, false), (20_000, 21, true)],
        );

        let mut mtu = Case::new();
        for seq in 1..=1233 {
            mtu.push(seq, seq * 250);
        }
        mtu.check("high-rate-mtu-baseline", 13, &[(1, 1233, true)]);
        mtu.push(2, 310_000);
        for seq in 1234..=1254 {
            mtu.push(seq, 410_000);
        }
        mtu.check(
            "large-go-feedback-covers-retained-1253-statuses",
            2,
            &[(2, 1253, true)],
        );
    }

    #[test]
    fn livekit_publisher_twcc_reports_missing_sequences_across_reports_and_wrap() {
        let now = Instant::now();
        let mut receipts = TwccRecvRegister::new(100);
        receipts.update_seq(65_533.into(), now);
        let (first, next) = receipts.build_report_from(1100, None).unwrap();
        assert_eq!(first.base_seq, 65_533);
        assert_eq!(*next, 65_534);
        for seq in [65_537, 65_539] {
            receipts.update_seq(seq.into(), now + Duration::from_millis(10));
        }
        let (report, next) = receipts.build_report_from(1100, Some(next)).unwrap();
        assert_eq!(report.base_seq, 65_534);
        assert_eq!(*next, 65_540);
        let received: Vec<_> = report
            .into_iter(now, 65_534.into())
            .map(|(seq, _, time)| (*seq, time.is_some()))
            .collect();
        assert_eq!(
            received,
            [
                (65_534, false),
                (65_535, false),
                (65_536, false),
                (65_537, true),
                (65_538, false),
                (65_539, true)
            ]
        );

        // The previous cursor may be many packets behind after a long gap.
        // Match Pion's maximum leading loss run and split a distant receive time.
        receipts.update_seq(110_000.into(), now + Duration::from_secs(9));
        receipts.update_seq(110_002.into(), now + Duration::from_secs(18));
        let (report, next) = receipts.build_report_from(1100, Some(next)).unwrap();
        assert_eq!(report.base_seq, (110_000 - 0x7ffe) as u16);
        assert_eq!(report.status_count, 0x7fff);
        assert_eq!(*next, 110_001);
        let (report, next) = receipts.build_report_from(1100, Some(next)).unwrap();
        assert_eq!(report.base_seq, 110_001_u32 as u16);
        assert_eq!(report.status_count, 2);
        assert_eq!(*next, 110_003);
    }

    #[test]
    fn livekit_publisher_twcc_acknowledges_authenticated_ssrc_zero_video_probes() {
        let now = Instant::now();
        let crypto = crate::crypto::test_default_provider();
        let profile = SrtpProfile::AES128_CM_SHA1_80;
        let key = KeyingMaterial::new(&vec![0x61; profile.keying_material_len()]);
        let mut publisher = SrtpContext::new(crypto, profile, &key, true);
        let mut session = Session::new(&RtcConfig::default());
        session.srtp_rx = Some(SrtpContext::new(crypto, profile, &key, true));
        session.srtp_tx = Some(SrtpContext::new(crypto, profile, &key, false));
        session.enable_twcc_feedback();
        session.enable_livekit_publisher_twcc();

        let mut extensions = ExtensionMap::empty();
        extensions.set(3, Extension::TransportSequenceNumber);
        session.exts = extensions.clone();
        let vp8 = session
            .codec_config
            .iter()
            .find(|p| p.spec().codec == crate::format::Codec::Vp8)
            .unwrap();
        assert!(vp8.fb_transport_cc());
        let pt = vp8.pt();
        let mut media = Media::from_direct_api(
            Mid::from("v"),
            0,
            crate::media::MediaKind::Video,
            ExtensionMap::empty(),
        );
        media.set_remote_pts(vec![pt]);
        session.add_media(media);

        let mut protect = |sequence: u16| {
            let mut packet = vec![0xb0, *pt, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
            let [hi, lo] = sequence.to_be_bytes();
            packet[2..4].copy_from_slice(&[hi, lo]);
            packet.extend_from_slice(&[0xbe, 0xde, 0, 1, 0x31, hi, lo, 0, 0, 0, 0, 4]);
            let header = RtpHeader::parse(&packet, &extensions).unwrap();
            publisher.protect_rtp(&packet, &header, u64::from(sequence))
        };

        session.handle_rtp_receive(now, &protect(1));
        assert_eq!(session.twcc_rx_register.max_seq(), None);
        session.medias[0].set_remote_extmap(extensions.clone());

        let mut corrupt = protect(2);
        *corrupt.last_mut().unwrap() ^= 1;
        session.handle_rtp_receive(now, &corrupt);
        assert_eq!(session.twcc_rx_register.max_seq(), None);

        let traffic_before = session.node_traffic;
        for sequence in 3..=23 {
            session.handle_rtp_receive(now, &protect(sequence));
        }
        assert_eq!(session.node_traffic, traffic_before);
        assert!(session.pending_packet.is_none());
        let Some(Rtcp::Twcc(report)) = session.feedback_tx.front() else {
            panic!("authenticated video probe should trigger TWCC");
        };
        assert_eq!(report.ssrc, 0.into());
        assert_eq!(report.base_seq, 3);
        assert_eq!(report.status_count, 21);
        assert_eq!(report.delta.len(), 21);
        let wire = session.poll_feedback(now).unwrap();
        assert_eq!(wire[0] & 0x1f, 15);
        assert_eq!(wire[1], 205);
    }

    #[test]
    fn original_packet_observer_receives_authenticated_incomplete_vp9_before_assembly_and_retires()
    {
        let now = Instant::now();
        let crypto = crate::crypto::test_default_provider();
        let profile = SrtpProfile::AES128_CM_SHA1_80;
        let material = vec![0x61; profile.keying_material_len()];
        let key = KeyingMaterial::new(&material);
        let mut tx = SrtpContext::new(crypto, profile, &key, true);
        let mut session = Session::new(&RtcConfig::default().enable_vp9(true));
        session.video_packet_events = true;
        session.srtp_rx = Some(SrtpContext::new(crypto, profile, &key, true));
        let params = session
            .codec_config
            .params()
            .iter()
            .find(|p| p.spec().codec == crate::format::Codec::Vp9)
            .copied()
            .unwrap();
        let mid = Mid::from("v");
        let mut media = Media::from_direct_api(
            mid,
            0,
            crate::media::MediaKind::Video,
            ExtensionMap::empty(),
        );
        media.set_remote_pts(vec![params.pt()]);
        session.add_media(media);
        session
            .streams
            .expect_stream_rx(101.into(), None, MidRid(mid, None), false);
        let mut actual_source = None;
        for sequence in 100u16..103 {
            let mut bytes = vec![0x80, *params.pt(), 0, 0, 0, 0, 0, 0, 0, 0, 0, 101];
            bytes[2..4].copy_from_slice(&sequence.to_be_bytes());
            bytes[4..8].copy_from_slice(&3000u32.to_be_bytes());
            // Genuine encrypted RTP, intentionally no E/marker or complete codec frame.
            let payload = [0xa8, 0x81, 0x23, 0x60, 0x82, sequence as u8];
            bytes.extend_from_slice(&payload);
            let header = RtpHeader::parse(&bytes, &ExtensionMap::empty()).unwrap();
            let encrypted = tx.protect_rtp(&bytes, &header, u64::from(sequence));
            let mut corrupt = encrypted.clone();
            *corrupt.last_mut().unwrap() ^= 1;
            session.handle_rtp_receive(now, &corrupt);
            assert!(
                session.pending_media_packet.is_none(),
                "unauthenticated source cannot measure rates"
            );
            session.handle_rtp_receive(now, &encrypted);
            assert!(
                session.pending_media_packet.is_some(),
                "actual authenticated native producer queued this original packet before polling"
            );
            if sequence == 100 {
                assert!(
                    !session.ready_for_srtp(),
                    "fixture has not installed its real outgoing SRTP context yet"
                );
                assert!(
                    session.poll_event().is_none(),
                    "production events correctly wait for both SRTP directions"
                );
                assert!(
                    session.pending_media_packet.is_some(),
                    "readiness gating retains, rather than loses, the accepted source packet"
                );
                session.srtp_tx = Some(SrtpContext::new(crypto, profile, &key, false));
                assert!(session.ready_for_srtp());
            }
            let mut observed = None;
            while let Some(event) = session.poll_event() {
                match event {
                    Event::MediaPacket(packet) => {
                        assert!(observed.is_none());
                        observed = Some(packet)
                    }
                    Event::MediaData(_) => panic!("incomplete codec frame must not be fabricated"),
                    _ => {}
                }
            }
            assert!(
                session.poll_event_fallible(now).unwrap().is_none(),
                "original incomplete payload must not fabricate an assembled frame"
            );
            let packet =
                observed.expect("every accepted original packet emits before a final fragment");
            assert_ne!(packet.source_id, 0);
            if let Some(first) = actual_source {
                assert_eq!(
                    packet.source_id, first,
                    "one actual receiver lifetime remains stable across original fragments"
                );
            }
            actual_source = Some(packet.source_id);
            assert_eq!(*packet.packet.seq_no, u64::from(sequence));
            assert_eq!(packet.packet.payload.as_ref(), &payload);
            assert!(!packet.packet.header.marker);
            session.handle_rtp_receive(now, &encrypted);
            assert!(
                session.pending_media_packet.is_none(),
                "duplicate source cannot inflate rates"
            );
        }
        session.remove_media(mid);
        assert!(session.pending_media_packet.is_none());
        assert!(session.streams.stream_rx(&101.into()).is_none());
        let previous_source = actual_source.unwrap();
        let mut media = Media::from_direct_api(
            mid,
            0,
            crate::media::MediaKind::Video,
            ExtensionMap::empty(),
        );
        media.set_remote_pts(vec![params.pt()]);
        session.add_media(media);
        session
            .streams
            .expect_stream_rx(101.into(), None, MidRid(mid, None), false);
        let mut bytes = vec![0x80, *params.pt(), 0, 200, 0, 0, 0x17, 0x70, 0, 0, 0, 101];
        bytes.extend_from_slice(&[0xac, 7, 0, 7, 0x80]);
        let header = RtpHeader::parse(&bytes, &ExtensionMap::empty()).unwrap();
        let encrypted = tx.protect_rtp(&bytes, &header, 200);
        session.handle_rtp_receive(now + Duration::from_millis(1), &encrypted);
        let fresh = session
            .pending_media_packet
            .as_ref()
            .expect("same SSRC/MID reuse receives real authenticated media in its new lifetime");
        assert_ne!(
            fresh.source_id, previous_source,
            "actual source identity, not merely SSRC, fences retired rate parsers"
        );
        assert!(
            session.ready_for_srtp(),
            "source receiver retirement retained the real transport crypto"
        );
        session.remove_media(mid);
        assert!(session.pending_media_packet.is_none());
    }

    #[test]
    fn authenticated_primary_padding_reports_original_receive_order_without_timing_observer() {
        for timing in [false, true] {
            let now = Instant::now();
            let crypto = crate::crypto::test_default_provider();
            let profile = SrtpProfile::AES128_CM_SHA1_80;
            let material = vec![0x62; profile.keying_material_len()];
            let key = KeyingMaterial::new(&material);
            let mut tx = SrtpContext::new(crypto, profile, &key, true);
            let mut session = Session::new(&RtcConfig::default().enable_vp9(true));
            session.video_packet_events = true;
            session.forwarding_timing_enabled = timing;
            session.srtp_rx = Some(SrtpContext::new(crypto, profile, &key, true));
            session.srtp_tx = Some(SrtpContext::new(crypto, profile, &key, false));
            let params = session
                .codec_config
                .params()
                .iter()
                .find(|p| p.spec().codec == crate::format::Codec::Vp9)
                .copied()
                .unwrap();
            let mid = Mid::from("v");
            let mut media = Media::from_direct_api(
                mid,
                0,
                crate::media::MediaKind::Video,
                ExtensionMap::empty(),
            );
            media.set_remote_pts(vec![params.pt()]);
            session.add_media(media);
            session
                .streams
                .expect_stream_rx(101.into(), None, MidRid(mid, None), false);
            for (index, (sequence, padding, out_of_order)) in [
                (1000u16, false, false),
                (1002, false, false),
                (1001, true, true),
                (1003, false, false),
                (1004, true, false),
            ]
            .into_iter()
            .enumerate()
            {
                let mut bytes = vec![
                    if padding { 0xa0 } else { 0x80 },
                    *params.pt() | 0x80,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    101,
                ];
                bytes[2..4].copy_from_slice(&sequence.to_be_bytes());
                bytes[4..8].copy_from_slice(&(3000 * (u32::from(sequence) - 999)).to_be_bytes());
                if padding {
                    bytes.extend_from_slice(&[0, 0, 0, 4]);
                } else {
                    bytes.extend_from_slice(&[0xbc, 0x81, sequence as u8, 0, 0x80, 0]);
                }
                let header = RtpHeader::parse(&bytes, &ExtensionMap::empty()).unwrap();
                let encrypted = tx.protect_rtp(&bytes, &header, u64::from(sequence));
                let at = now + Duration::from_millis(index as u64);
                session.handle_rtp_receive(at, &encrypted);
                let mut packet = None;
                while let Some(event) = session.poll_event() {
                    if let Event::MediaPacket(value) = event {
                        assert!(packet.is_none());
                        packet = Some(value)
                    }
                }
                let packet=packet.expect("each authenticated nonduplicate initialized primary, including padding, has its actual packet observer event");
                assert_eq!(*packet.packet.seq_no, u64::from(sequence));
                assert_eq!(
                    packet.received_out_of_order, out_of_order,
                    "receive order comes from the source admission flow, never optional telemetry"
                );
                assert_eq!(packet.packet.payload.is_empty(), padding);
                assert_eq!(packet.packet.forwarding_observation().is_some(), timing);
                while session.poll_event_fallible(at).unwrap().is_some() {}
                session.handle_rtp_receive(at, &encrypted);
                assert!(
                    session.pending_media_packet.is_none(),
                    "authenticated replay cannot produce a second padding/control transition"
                );
            }
            session.remove_media(mid);
            assert!(session.pending_media_packet.is_none());
        }
    }

    #[test]
    fn session_mtu_matches_config() {
        let cfg = RtcConfig::default();
        let s = Session::new(&cfg);
        assert_eq!(s.mtu(), DATAGRAM_MTU_TARGET);

        let cfg = RtcConfig::default().set_mtu(900..=1280);
        let s = Session::new(&cfg);
        assert_eq!(s.mtu(), 900);
    }

    #[test]
    fn authenticated_fragmented_frame_preserves_each_arrival_and_original_out_of_order_flag() {
        for enabled in [false, true] {
            let start = Instant::now();
            let crypto = crate::crypto::test_default_provider();
            let profile = SrtpProfile::AES128_CM_SHA1_80;
            let material = vec![0x61; profile.keying_material_len()];
            let key = KeyingMaterial::new(&material);
            let mut tx = SrtpContext::new(crypto, profile, &key, true);
            let mut session = Session::new(&RtcConfig::default());
            session.forwarding_timing_enabled = enabled;
            session.srtp_rx = Some(SrtpContext::new(crypto, profile, &key, true));
            let params = session
                .codec_config
                .params()
                .iter()
                .find(|p| p.spec().codec == crate::format::Codec::Vp8)
                .copied()
                .unwrap();
            let mid = Mid::from("v");
            let mut media = Media::from_direct_api(
                mid,
                0,
                crate::media::MediaKind::Video,
                ExtensionMap::empty(),
            );
            media.set_remote_pts(vec![params.pt()]);
            session.add_media(media);
            session
                .streams
                .expect_stream_rx(101.into(), None, MidRid(mid, None), false);
            // A complete codec frame has three distinct real encrypted RTP
            // packets; the middle sequence arrives last and is out of order.
            for (sn, ms, marker, payload) in [
                (100u16, 0u64, false, vec![0x10, 0, 0]),
                (102, 40, true, vec![0, 2]),
                (101, 60, false, vec![0, 1]),
            ] {
                let mut bytes = vec![
                    0x80,
                    *params.pt() | if marker { 0x80 } else { 0 },
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    101,
                ];
                bytes[2..4].copy_from_slice(&sn.to_be_bytes());
                bytes[4..8].copy_from_slice(&3000u32.to_be_bytes());
                bytes.extend_from_slice(&payload);
                let header = RtpHeader::parse(&bytes, &ExtensionMap::empty()).unwrap();
                let encrypted = tx.protect_rtp(&bytes, &header, u64::from(sn));
                session.handle_rtp_receive(start + Duration::from_millis(ms), &encrypted);
            }
            let event = session
                .poll_event_fallible(start + Duration::from_secs(1))
                .unwrap()
                .expect("actual SRTP codec frame must assemble");
            let Event::MediaData(data) = event else {
                panic!("expected actual media frame")
            };
            if enabled {
                assert_eq!(data.forwarding_packets().len(), 3);
                let times = data
                    .forwarding_packets()
                    .iter()
                    .map(|p| {
                        (
                            p.received.duration_since(start).as_millis(),
                            p.out_of_order,
                            p.buffered,
                        )
                    })
                    .collect::<Vec<_>>();
                assert_eq!(
                    times,
                    vec![(0, false, false), (60, true, false), (40, false, false)]
                );
            } else {
                assert!(
                    data.forwarding_packets().is_empty(),
                    "disabled instrumentation must not allocate a packet cohort"
                );
            }
            assert!(
                session
                    .poll_event_fallible(start + Duration::from_secs(1))
                    .unwrap()
                    .is_none(),
                "one frame consumes its timing cohort exactly once"
            );
            drop(data);
            session.remove_media(mid);
            assert!(session.medias.is_empty());
            assert!(
                session.streams.stream_rx(&101.into()).is_none(),
                "retirement releases bounded packet/metadata owners together"
            );
        }
    }

    #[test]
    fn actual_srtp_vp9_packet_layers_survive_assembly_without_last_packet_aliasing() {
        let start = Instant::now();
        let crypto = crate::crypto::test_default_provider();
        let profile = SrtpProfile::AES128_CM_SHA1_80;
        let material = vec![0x63; profile.keying_material_len()];
        let key = KeyingMaterial::new(&material);
        let mut tx = SrtpContext::new(crypto, profile, &key, true);
        let mut session = Session::new(&RtcConfig::default());
        session.forwarding_timing_enabled = true;
        session.srtp_rx = Some(SrtpContext::new(crypto, profile, &key, true));
        let params = session
            .codec_config
            .params()
            .iter()
            .find(|p| p.spec().codec == crate::format::Codec::Vp9)
            .copied()
            .unwrap();
        let mid = Mid::from("v");
        let mut media = Media::from_direct_api(
            mid,
            0,
            crate::media::MediaKind::Video,
            ExtensionMap::empty(),
        );
        media.set_remote_pts(vec![params.pt()]);
        session.add_media(media);
        session
            .streams
            .expect_stream_rx(101.into(), None, MidRid(mid, None), false);
        for (spatial, ms) in [(0u16, 0u64), (2, 40), (1, 60)] {
            let sn = 1000 + spatial;
            let mut bytes = vec![
                0x80,
                *params.pt() | if spatial == 2 { 0x80 } else { 0 },
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                101,
            ];
            bytes[2..4].copy_from_slice(&sn.to_be_bytes());
            bytes[4..8].copy_from_slice(&3000u32.to_be_bytes());
            bytes.extend_from_slice(&[
                0x3c,
                0x20 | ((spatial as u8) << 1) | u8::from(spatial > 0),
                0x80 + spatial as u8,
                0,
            ]);
            let header = RtpHeader::parse(&bytes, &ExtensionMap::empty()).unwrap();
            let encrypted = tx.protect_rtp(&bytes, &header, u64::from(sn));
            session.handle_rtp_receive(start + Duration::from_millis(ms), &encrypted);
        }
        let Event::MediaData(data) = session
            .poll_event_fallible(start + Duration::from_secs(1))
            .unwrap()
            .expect("real encrypted VP9 spatial frame")
        else {
            panic!("expected media")
        };
        let actual = data
            .forwarding_packets()
            .iter()
            .map(|p| {
                (
                    p.sequence,
                    p.spatial,
                    p.temporal,
                    p.extension_index,
                    p.out_of_order,
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            actual,
            vec![
                (1000, Some(0), Some(1), 0, false),
                (1001, Some(1), Some(1), 1, true),
                (1002, Some(2), Some(1), 2, false)
            ]
        );
        assert_eq!(data.data.as_ref(), &[0x80, 0, 0x81, 0, 0x82, 0]);
    }

    #[test]
    fn authenticated_duplicate_osn_and_replay_still_acknowledge_fresh_transport_sequences() {
        let now = Instant::now();
        let crypto = crate::crypto::test_default_provider();
        let profile = SrtpProfile::AES128_CM_SHA1_80;
        let material = vec![0x62; profile.keying_material_len()];
        let key = KeyingMaterial::new(&material);
        let mut tx = SrtpContext::new(crypto, profile, &key, true);
        let mut session = Session::new(&RtcConfig::default().set_rtp_mode(true));
        session.srtp_rx = Some(SrtpContext::new(crypto, profile, &key, true));
        let params = session
            .codec_config
            .params()
            .iter()
            .find(|p| p.spec().codec == crate::format::Codec::Vp8 && p.resend().is_some())
            .copied()
            .unwrap();
        session.streams.expect_stream_rx(
            101.into(),
            Some(201.into()),
            MidRid("v".into(), None),
            false,
        );
        let mut exts = ExtensionMap::empty();
        exts.set(3, Extension::TransportSequenceNumber);
        session.exts = exts.clone();
        let mut feed = |session: &mut Session, repair: bool, sn: u16, tcc: u16, payload: &[u8]| {
            let mut bytes = vec![
                0x90,
                if repair {
                    *params.resend().unwrap()
                } else {
                    *params.pt()
                },
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
            ];
            bytes[2..4].copy_from_slice(&sn.to_be_bytes());
            bytes[4..8].copy_from_slice(&3000u32.to_be_bytes());
            bytes[8..12].copy_from_slice(&(if repair { 201u32 } else { 101u32 }).to_be_bytes());
            bytes.extend_from_slice(&[0xbe, 0xde, 0, 1, 0x31, (tcc >> 8) as u8, tcc as u8, 0]);
            bytes.extend_from_slice(payload);
            let header = RtpHeader::parse(&bytes, &exts).unwrap();
            let encrypted = tx.protect_rtp(&bytes, &header, u64::from(sn));
            session.handle_rtp_receive(now, &encrypted);
        };
        feed(&mut session, false, 100, 400, &[0x10, 0, 1]);
        assert!(session.pending_packet.take().is_some());
        session.streams.take_receive_traffic();
        for (outer, tcc, payload) in [
            (10u16, 401u16, vec![0, 100]),
            (11, 402, vec![0, 100, 0x10, 0, 1]),
            (11, 403, vec![0, 100, 0x10, 0, 1]),
        ] {
            feed(&mut session, true, outer, tcc, &payload);
            assert!(
                session.pending_packet.is_none(),
                "duplicate original media must never redeliver"
            );
            assert_eq!(
                session.twcc_rx_register.max_seq().map(|s| *s),
                Some(u64::from(tcc)),
                "fresh transport delivery must not appear lost because media OSN repeats"
            );
            assert_eq!(
                session.max_rx_seq_lookup[&201.into()],
                SeqNo::from(u64::from(outer))
            );
        }
        let source = session.streams.take_receive_traffic().pop().unwrap();
        assert_eq!(
            (source.packets, source.expected_packets, source.duplicates),
            (4, 1, 3)
        );
    }

    #[test]
    fn authenticated_timestamp_rejection_and_restart_keep_crypto_and_twcc_alive() {
        for enabled in [false, true] {
            let now = Instant::now();
            let crypto = crate::crypto::test_default_provider();
            let profile = SrtpProfile::AES128_CM_SHA1_80;
            let material = vec![0x51; profile.keying_material_len()];
            let key = KeyingMaterial::new(&material);
            let mut tx = SrtpContext::new(crypto, profile, &key, true);
            let mut session = Session::new(&RtcConfig::default().set_rtp_mode(true));
            session.rtp_stream_restart_detection = enabled;
            session.srtp_rx = Some(SrtpContext::new(crypto, profile, &key, true));
            let params = session
                .codec_config
                .params()
                .iter()
                .find(|p| p.spec().codec == crate::format::Codec::Vp8)
                .copied()
                .unwrap();
            let mid = Mid::from("v");
            session
                .streams
                .expect_stream_rx(101.into(), None, MidRid(mid, None), false);
            let mut exts = ExtensionMap::empty();
            exts.set(3, Extension::TransportSequenceNumber);
            session.exts = exts.clone();
            let mut feed = |session: &mut Session, sn: u16, ts: u32, transport: u16, ms: u64| {
                let mut bytes = vec![0u8; 12];
                bytes[0] = 0x90;
                bytes[1] = *params.pt();
                bytes[2..4].copy_from_slice(&sn.to_be_bytes());
                bytes[4..8].copy_from_slice(&ts.to_be_bytes());
                bytes[8..12].copy_from_slice(&101u32.to_be_bytes());
                bytes.extend_from_slice(&[
                    0xbe,
                    0xde,
                    0,
                    1,
                    0x31,
                    (transport >> 8) as u8,
                    transport as u8,
                    0,
                ]);
                bytes.extend_from_slice(&[0x10, 0, 1]);
                let header = RtpHeader::parse(&bytes, &exts).unwrap();
                let protected = tx.protect_rtp(&bytes, &header, u64::from(sn));
                session.handle_rtp_receive(now + Duration::from_millis(ms), &protected);
            };
            feed(&mut session, 1000, 90000, 400, 0);
            assert!(session.pending_packet.take().is_some());
            let old = session.streams.take_receive_traffic().pop().unwrap();
            for i in 1..=5 {
                feed(
                    &mut session,
                    1000 + i,
                    i as u32 * 1000,
                    400 + i,
                    i as u64 * 10,
                );
                assert_eq!(
                    session.twcc_rx_register.max_seq().map(|s| *s),
                    Some(u64::from(400 + i))
                );
                assert_eq!(
                    session.pending_packet.take().is_some(),
                    enabled && i == 5,
                    "fifth rejected packet must replay only when enabled"
                );
            }
            let snapshots = session.streams.take_receive_traffic();
            if enabled {
                assert!(
                    snapshots.iter().any(|s| s.source_id == old.source_id
                        && s.discarded_on_restart
                        && s.retired)
                );
                let new = snapshots
                    .iter()
                    .find(|s| s.restarted_from == Some(old.source_id))
                    .unwrap();
                assert_eq!((new.packets, new.expected_packets, new.bytes), (1, 1, 23));
                assert_ne!(new.source_id, old.source_id);
            } else {
                assert!(
                    snapshots.is_empty(),
                    "disabled detector cannot commit rejected packets"
                );
            }
            feed(
                &mut session,
                1006,
                if enabled { 8000 } else { 93000 },
                406,
                60,
            );
            assert!(
                session.pending_packet.take().is_some(),
                "future authenticated media must continue without a new DTLS/transport"
            );
            assert!(session.srtp_rx.is_some());
        }
        // Media's forced sequence rollover must never change the SRTP nonce ROC.
        let now = Instant::now();
        let crypto = crate::crypto::test_default_provider();
        let profile = SrtpProfile::AES128_CM_SHA1_80;
        let bytes = vec![0x52; profile.keying_material_len()];
        let key = KeyingMaterial::new(&bytes);
        let mut tx = SrtpContext::new(crypto, profile, &key, true);
        let mut session = Session::new(&RtcConfig::default().set_rtp_mode(true));
        session.srtp_rx = Some(SrtpContext::new(crypto, profile, &key, true));
        let pt = session
            .codec_config
            .params()
            .iter()
            .find(|p| p.spec().codec == crate::format::Codec::Vp8)
            .unwrap()
            .pt();
        session
            .streams
            .expect_stream_rx(101.into(), None, MidRid("v".into(), None), false);
        for (index, (sn, ts)) in [(30000u16, 100000u32), (1000, 103000), (1001, 106000)]
            .into_iter()
            .enumerate()
        {
            let mut bytes = vec![0x80, *pt, 0, 0, 0, 0, 0, 0, 0, 0, 0, 101];
            bytes[2..4].copy_from_slice(&sn.to_be_bytes());
            bytes[4..8].copy_from_slice(&ts.to_be_bytes());
            bytes.extend_from_slice(&[0x10, 0, 1]);
            let header = RtpHeader::parse(&bytes, &ExtensionMap::empty()).unwrap();
            let encrypted = tx.protect_rtp(&bytes, &header, u64::from(sn));
            session.handle_rtp_receive(now + Duration::from_millis(index as u64 * 33), &encrypted);
            assert!(
                session.pending_packet.take().is_some(),
                "media forced rollover must not poison next SRTP nonce"
            );
        }
        let source = session.streams.take_receive_traffic().pop().unwrap();
        assert_eq!(source.expected_packets, 66538 - 30000);
        assert_eq!(source.packets, 3);
        assert_eq!(session.max_rx_seq_lookup[&101.into()], SeqNo::from(30000));
    }

    #[test]
    fn authenticated_repair_length_boundary_counts_osn_only_without_delivering_empty_media() {
        let now = Instant::now();
        let crypto = crate::crypto::test_default_provider();
        let profile = SrtpProfile::AES128_CM_SHA1_80;
        let material = vec![0x41; profile.keying_material_len()];
        let key = KeyingMaterial::new(&material);
        let mut tx = SrtpContext::new(crypto, profile, &key, true);
        let mut session = Session::new(&RtcConfig::default().set_rtp_mode(true));
        session.srtp_rx = Some(SrtpContext::new(crypto, profile, &key, true));
        let params = session
            .codec_config
            .params()
            .iter()
            .find(|p| p.spec().codec == crate::format::Codec::Vp8 && p.resend().is_some())
            .copied()
            .unwrap();
        let mid = Mid::from("v");
        session
            .streams
            .expect_stream_rx(101.into(), Some(201.into()), MidRid(mid, None), false);
        // Actual primary padding at SN100 arrives before real media. Its
        // transport sequence399 must be acknowledged without poisoning SN100.
        let mut exts = ExtensionMap::empty();
        exts.set(3, Extension::TransportSequenceNumber);
        session.exts = exts.clone();
        let mut padding = vec![0u8; 12];
        padding[0] = 0xb0;
        padding[1] = *params.pt();
        padding[2..4].copy_from_slice(&100u16.to_be_bytes());
        padding[4..8].copy_from_slice(&3000u32.to_be_bytes());
        padding[8..12].copy_from_slice(&101u32.to_be_bytes());
        padding.extend_from_slice(&[0xbe, 0xde, 0, 1, 0x31, 1, 143, 0]);
        padding.extend_from_slice(&[0, 0, 0, 4]);
        let header = RtpHeader::parse(&padding, &exts).unwrap();
        let protected = tx.protect_rtp(&padding, &header, 100);
        session.handle_rtp_receive(now, &protected);
        assert_eq!(session.twcc_rx_register.max_seq().map(|v| *v), Some(399));
        assert!(session.streams.take_receive_traffic().is_empty());
        assert!(session.pending_packet.is_none());
        let mut feed = |session: &mut Session, repair: bool, seq: u16, payload: &[u8]| {
            let mut bytes = vec![0u8; 12];
            bytes[0] = 0x80;
            bytes[1] = if repair {
                *params.resend().unwrap()
            } else {
                *params.pt()
            };
            bytes[2..4].copy_from_slice(&seq.to_be_bytes());
            bytes[4..8].copy_from_slice(&3000u32.to_be_bytes());
            bytes[8..12].copy_from_slice(&(if repair { 201u32 } else { 101 }).to_be_bytes());
            bytes.extend_from_slice(payload);
            let header = RtpHeader::parse(&bytes, &ExtensionMap::empty()).unwrap();
            let protected = tx.protect_rtp(&bytes, &header, u64::from(seq));
            session.handle_rtp_receive(now, &protected);
        };
        feed(&mut session, true, 8, &[0, 100, 0x10, 0, 1]);
        feed(&mut session, true, 9, &[0, 100]);
        assert!(
            session
                .streams
                .take_receive_traffic()
                .iter()
                .all(|s| s.first_at.is_none())
        );
        assert!(session.pending_packet.is_none());
        feed(&mut session, false, 100, &[0x10, 0, 1]);
        assert!(
            session.pending_packet.take().is_some(),
            "real primary initializes and delivers"
        );
        let started = session.streams.take_receive_traffic().pop().unwrap();
        assert_eq!(
            (started.packets, started.expected_packets, started.bytes),
            (1, 1, 15)
        );
        let raw_before = session.node_traffic;
        feed(&mut session, true, 10, &[0]);
        assert!(
            session.pending_packet.is_none(),
            "authenticated truncated OSN is rejected"
        );
        assert!(session.streams.take_receive_traffic().is_empty());
        assert_eq!(
            session.node_traffic, raw_before,
            "short repair is not a logical RTP input"
        );
        feed(&mut session, true, 11, &[0, 100]);
        assert!(
            session.pending_packet.is_none(),
            "OSN-only duplicate counts without delivery"
        );
        let duplicated = session.streams.take_receive_traffic().pop().unwrap();
        assert_eq!(
            (
                duplicated.packets,
                duplicated.expected_packets,
                duplicated.duplicates,
                duplicated.bytes
            ),
            (2, 1, 1, 27)
        );
        feed(&mut session, true, 12, &[0, 101]);
        assert!(
            session.pending_packet.is_none(),
            "new OSN-only packet also remains undeliverable"
        );
        let empty = session.streams.take_receive_traffic().pop().unwrap();
        assert_eq!(
            (
                empty.packets,
                empty.expected_packets,
                empty.duplicates,
                empty.bytes
            ),
            (3, 2, 1, 39)
        );
        feed(&mut session, true, 13, &[0, 102, 0x10, 0, 1]);
        assert!(
            session.pending_packet.take().is_some(),
            "later real repair still delivers on the same source"
        );
        let repaired = session.streams.take_receive_traffic().pop().unwrap();
        assert_eq!(
            (
                repaired.packets,
                repaired.expected_packets,
                repaired.duplicates,
                repaired.bytes
            ),
            (4, 3, 1, 54)
        );
        // Actual authenticated blank RTX and truncated repair both carry TWCC.
        // They are not logical media, but dropping their transport feedback
        // creates false packet loss and prevents real padding BWE validation.
        let mut exts = ExtensionMap::empty();
        exts.set(3, Extension::TransportSequenceNumber);
        session.exts = exts.clone();
        let before_padding = session.node_traffic;
        for (seq, transport_seq, payload, padding) in [
            (14u16, 400u16, vec![0u8, 0, 0, 4], true),
            (15, 401, vec![0u8], false),
        ] {
            let mut bytes = vec![0u8; 12];
            bytes[0] = if padding { 0xb0 } else { 0x90 };
            bytes[1] = *params.resend().unwrap();
            bytes[2..4].copy_from_slice(&seq.to_be_bytes());
            bytes[4..8].copy_from_slice(&3000u32.to_be_bytes());
            bytes[8..12].copy_from_slice(&201u32.to_be_bytes());
            bytes.extend_from_slice(&[
                0xbe,
                0xde,
                0,
                1,
                0x31,
                (transport_seq >> 8) as u8,
                transport_seq as u8,
                0,
            ]);
            bytes.extend_from_slice(&payload);
            let header = RtpHeader::parse(&bytes, &exts).unwrap();
            assert_eq!(header.ext_vals.transport_cc, Some(transport_seq));
            let protected = tx.protect_rtp(&bytes, &header, u64::from(seq));
            session.handle_rtp_receive(now, &protected);
            assert_eq!(
                session.twcc_rx_register.max_seq().map(|v| *v),
                Some(u64::from(transport_seq)),
                "transport feedback must include authenticated non-media padding/repair"
            );
            assert_eq!(
                session.node_traffic, before_padding,
                "do not manufacture logical RTP traffic"
            );
            assert!(session.streams.take_receive_traffic().is_empty());
            assert!(
                session.pending_packet.is_none(),
                "non-media transport input cannot deliver an RTP payload"
            );
        }
    }

    #[test]
    fn actual_srtcp_livekit_fir_requires_the_common_media_ssrc_in_the_fci() {
        let now = Instant::now();
        let crypto = crate::crypto::test_default_provider();
        let profile = SrtpProfile::AES128_CM_SHA1_80;
        let material = vec![0x42; profile.keying_material_len()];
        let key = KeyingMaterial::new(&material);
        let mut peer = SrtpContext::new(crypto, profile, &key, true);
        let mut session = Session::new(&RtcConfig::default());
        session.srtp_rx = Some(SrtpContext::new(crypto, profile, &key, true));
        for (ssrc, mid) in [(101, "a"), (102, "b")] {
            session
                .streams
                .declare_stream_tx(ssrc.into(), None, MidRid(mid.into(), None));
        }

        let mut deliver = |session: &mut Session, media: u32, targets: [u32; 2], pli| {
            // Real SRTCP through authentication, RTCP parsing, normalization and
            // SSRC-to-send-stream dispatch. Both FCI targets are valid streams.
            let mut bytes = vec![0x84, 206, 0, 6];
            bytes.extend_from_slice(&999u32.to_be_bytes());
            bytes.extend_from_slice(&media.to_be_bytes());
            for ssrc in targets {
                bytes.extend_from_slice(&ssrc.to_be_bytes());
                bytes.extend_from_slice(&[1, 0, 0, 0]);
            }
            if pli {
                bytes.extend_from_slice(&[0x81, 206, 0, 2]);
                bytes.extend_from_slice(&999u32.to_be_bytes());
                bytes.extend_from_slice(&101u32.to_be_bytes());
            }
            session.handle_rtcp_receive(now, &peer.protect_rtcp(&bytes));
            std::iter::from_fn(|| session.streams.poll_keyframe_request())
                .map(|request| (request.mid, request.kind))
                .collect::<Vec<_>>()
        };

        let native = deliver(&mut session, 0, [101, 102], false);
        assert_eq!(native.len(), 2);
        assert!(native.contains(&("a".into(), KeyframeRequestKind::Fir)));
        assert!(native.contains(&("b".into(), KeyframeRequestKind::Fir)));

        session.require_fir_media_ssrc = true;
        assert!(deliver(&mut session, 0, [101, 102], false).is_empty());
        assert!(deliver(&mut session, 101, [102, 102], false).is_empty());
        assert_eq!(
            deliver(&mut session, 101, [102, 101], false),
            [("a".into(), KeyframeRequestKind::Fir)]
        );
        assert_eq!(
            deliver(&mut session, 101, [101, 101], true),
            [("a".into(), KeyframeRequestKind::Pli)]
        );
    }

    #[test]
    fn actual_srtcp_receiver_reports_emit_changed_matched_sender_rtt_and_fence_retired_sources() {
        use crate::streams::RtpWrite;
        let start = Instant::now();
        let crypto = crate::crypto::test_default_provider();
        let profile = SrtpProfile::AES128_CM_SHA1_80;
        let material = vec![0x41; profile.keying_material_len()];
        let key = KeyingMaterial::new(&material);
        let mut peer = SrtpContext::new(crypto, profile, &key, true);
        let mut session = Session::new(&RtcConfig::default());
        session.receiver_report_rtt_enabled = true;
        session.srtp_rx = Some(SrtpContext::new(crypto, profile, &key, true));
        let params = session.codec_config.params().to_vec();
        let pt = params
            .iter()
            .find(|p| p.spec().codec == crate::format::Codec::Vp8)
            .unwrap()
            .pt();
        let source;
        let lsr;
        let codec_config = session.codec_config.clone();
        let media = Media::from_direct_api(
            "v".into(),
            0,
            crate::media::MediaKind::Video,
            ExtensionMap::empty(),
        );
        {
            let stream = session.streams.declare_stream_tx(
                101.into(),
                Some(201.into()),
                MidRid("v".into(), None),
            );
            stream.write_rtp(RtpWrite::new(pt, 100.into(), 3000, start, vec![0x10, 0, 1]));
            // Real send lifecycle: write queues with a future sentinel;
            // production handle_timeout stamps admitted packets before polling.
            stream.handle_timeout(start, || (&media, &codec_config));
            let mut wire = Vec::new();
            let packet = stream
                .poll_packet(start, &ExtensionMap::empty(), None, &params, &mut wire)
                .unwrap();
            assert_eq!(*packet.seq_no, 100);
            // Serialize the actual primary emission through the real crypto
            // provider too; initialization comes from poll_packet, not a fake
            // Tracker::primary test hook.
            let mut primary_crypto = SrtpContext::new(crypto, profile, &key, false);
            let protected = primary_crypto.protect_rtp(&wire, &packet.header, 100);
            assert!(protected.len() > wire.len());
            let mut outgoing = VecDeque::new();
            stream.create_sr_and_update(start, &mut outgoing);
            let Rtcp::SenderReport(sr) = outgoing.pop_front().unwrap() else {
                panic!("actual SR producer")
            };
            lsr = (sr.sender_info.ntp_time.as_ntp_64() >> 16) as u32;
            assert_ne!(lsr, 0);
            source = stream.receiver_report_source_id();
        }
        let report = |session: &mut Session,
                      peer: &mut SrtpContext,
                      at: Instant,
                      ssrc: u32,
                      lsr: u32,
                      dlsr: u32,
                      bad_auth: bool| {
            let mut bytes = vec![0u8; 32];
            bytes[..4].copy_from_slice(&[0x81, 201, 0, 7]);
            bytes[4..8].copy_from_slice(&999u32.to_be_bytes());
            bytes[8..12].copy_from_slice(&ssrc.to_be_bytes());
            bytes[16..20].copy_from_slice(&100u32.to_be_bytes());
            bytes[24..28].copy_from_slice(&lsr.to_be_bytes());
            bytes[28..32].copy_from_slice(&dlsr.to_be_bytes());
            let mut encrypted = peer.protect_rtcp(&bytes);
            if bad_auth {
                let last = encrypted.last_mut().unwrap();
                *last ^= 1;
            }
            session.handle_rtcp_receive(at, &encrypted);
        };
        let at = start + Duration::from_millis(250);
        // SenderReport report blocks feed native transport stats, but fixed
        // Go DownTrack does not treat them as participant-RPC RTT updates.
        let mut sender_report = vec![0u8; 52];
        sender_report[..4].copy_from_slice(&[0x81, 200, 0, 12]);
        sender_report[4..8].copy_from_slice(&999u32.to_be_bytes());
        sender_report[28..32].copy_from_slice(&101u32.to_be_bytes());
        sender_report[36..40].copy_from_slice(&100u32.to_be_bytes());
        sender_report[44..48].copy_from_slice(&lsr.to_be_bytes());
        sender_report[48..52].copy_from_slice(&8192u32.to_be_bytes());
        let encrypted = peer.protect_rtcp(&sender_report);
        session.handle_rtcp_receive(at, &encrypted);
        assert!(
            session.receiver_report_rtt.is_empty(),
            "piggybacked SR blocks are not the original downtrack RR callback"
        );
        report(&mut session, &mut peer, at, 101, lsr, 8192, true);
        report(&mut session, &mut peer, at, 201, lsr, 8192, false);
        report(
            &mut session,
            &mut peer,
            at,
            101,
            lsr.wrapping_add(1),
            8192,
            false,
        );
        report(&mut session, &mut peer, at, 101, 0, 8192, false);
        assert!(
            session.receiver_report_rtt.is_empty(),
            "bad auth, RTX, unknown and zero LSR must not publish RTT"
        );
        report(&mut session, &mut peer, at, 101, lsr, 8192, false);
        let first = session.receiver_report_rtt.pop_front().unwrap();
        assert_eq!(
            (first.source_id, first.sequence, first.ssrc, first.rtt_ms),
            (source, 1, 101, 125)
        );
        assert_eq!(first.received_at, at);
        // DLSR0 is legal in original GetRttMs; neither the old native helper's
        // rejection nor its 70ms NACK fallback may leak into this producer.
        let at = start + Duration::from_millis(500);
        report(&mut session, &mut peer, at, 101, lsr, 0, false);
        let second = session.receiver_report_rtt.pop_front().unwrap();
        assert_eq!((second.sequence, second.rtt_ms), (2, 500));
        report(&mut session, &mut peer, at, 101, lsr, 0, false);
        assert!(
            session.receiver_report_rtt.is_empty(),
            "unchanged RTT emits no new participant observation"
        );
        report(&mut session, &mut peer, at, 101, lsr, u32::MAX / 2, false);
        assert!(
            session.receiver_report_rtt.is_empty(),
            "anachronous subtraction rejected"
        );
        let replacement = session
            .streams
            .stream_tx(&101.into())
            .unwrap()
            .reset_receiver_report_source();
        assert_ne!(replacement, source);
        report(
            &mut session,
            &mut peer,
            start + Duration::from_secs(1),
            101,
            lsr,
            0,
            false,
        );
        assert!(
            session.receiver_report_rtt.is_empty(),
            "retired SR must not populate replacement logical sender"
        );
    }

    #[test]
    fn un_red_rejects_a_primary_payload_type_mismatch() {
        let expected_primary_pt = Pt::new_with_value(111);
        let data = RedEncoder::encode(0, &[1, 2, 3], &[]);
        let mut header = RtpHeader::default();

        assert!(
            un_red(&mut header, &data, expected_primary_pt).is_none(),
            "RED associated with Opus must not reinterpret a PCMU primary block as Opus"
        );
    }
}

#[cfg(test)]
#[path = "transport_fallback_tests.rs"]
mod transport_fallback_tests;
