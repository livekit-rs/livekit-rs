//! Normalized server configuration (ARCH-02). `lk-config` turns the official
//! CLI/YAML/ENV inputs into this; nothing here knows about YAML key names. All
//! defaults and derivations have already been applied when a value reaches here.

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::path::PathBuf;
use std::time::Duration;

use crate::model::CodecSpec;
use crate::permission::RoomConfig;

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub http: HttpConfig,
    pub rtc: RtcConfig,
    pub room: RoomDefaults,
    pub audio: AudioConfig,
    pub video: VideoConfig,
    pub turn: TurnConfig,
    pub webhook: WebhookConfig,
    pub limits: Limits,
    pub logging: LoggingConfig,
    pub prometheus: PrometheusConfig,
    /// API key → secret. Order is the configured order; the first entry signs
    /// refreshed tokens.
    pub keys: Vec<(String, String)>,
    pub region: String,
    pub node_id: String,
    pub development: bool,
    pub enable_data_tracks: bool,
    pub enable_participant_data_blob: bool,
    /// Get/ListParticipants query the live owner instead of participant-change snapshots.
    pub enable_psrpc_for_get_list_participants: bool,
    /// Product extension tree (`livecloud:`); `None` when absent.
    pub extension: Option<ExtensionConfig>,
}

/// Immutable room-state-machine policy, projected from normalized configuration.
/// Includes neutral fallback policy; excludes transport setup, credentials and delivery endpoints.
/// Projection preserves values and units; room-specific overrides remain in Room.
#[derive(Clone, Debug, PartialEq)]
pub struct RoomPolicy {
    pub transport_fallback: crate::transport_fallback::FallbackPolicy,
    pub enable_data_tracks: bool,
    pub enable_participant_data_blob: bool,
    /// UTF-8 bytes; zero is unlimited, negative rejects every custom identifier.
    pub max_data_track_custom_encoding_length: i64,
    pub max_data_blob_key_length: i64,
    pub max_data_blob_size: u32,
    pub enabled_codecs: Vec<CodecSpec>,
    pub max_participants: u32,
    pub empty_timeout: Duration,
    pub departure_timeout: Duration,
    pub enable_remote_unmute: bool,
    /// Default pair; each room resolves request overrides once at creation.
    pub playout_delay: Option<(u32, u32)>,
    pub sync_streams: bool,
    pub enable_full_room_in_webhooks: bool,
    /// Target sum of encoded participant messages; signed Go int, not a hard cap.
    pub update_batch_target_size: i64,
    pub subscription_limit_video: Option<u32>,
    pub subscription_limit_audio: Option<u32>,
    /// UTF-8 byte length; zero alone disables this signed limit.
    pub max_participant_name_length: i64,
    pub max_metadata_size: Option<usize>,
    pub max_attributes_size: Option<usize>,
    pub use_ice_lite: bool,
    pub allow_pause: bool,
    pub reconnect_on_publication_error: bool,
    pub reconnect_on_subscription_error: bool,
    pub reconnect_on_data_channel_error: bool,
    pub region: String,
}

impl From<&ServerConfig> for RoomPolicy {
    fn from(cfg: &ServerConfig) -> Self {
        Self {
            transport_fallback: crate::transport_fallback::FallbackPolicy::new(
                cfg.rtc.allow_tcp_fallback,
                cfg.rtc.tcp_fallback_rtt_threshold,
                cfg.rtc.allow_udp_unstable_fallback,
                (cfg.turn.enabled && cfg.turn.tls_port.is_some())
                    || cfg
                        .rtc
                        .turn_servers
                        .iter()
                        .any(|server| server.protocol == IceServerProtocol::Tls),
            ),
            enable_data_tracks: cfg.enable_data_tracks,
            enable_participant_data_blob: cfg.enable_participant_data_blob,
            max_data_track_custom_encoding_length: cfg.limits.max_data_track_custom_encoding_length,
            max_data_blob_key_length: cfg.limits.max_data_blob_key_length,
            max_data_blob_size: cfg.limits.max_data_blob_size,
            enabled_codecs: cfg.room.enabled_codecs.clone(),
            max_participants: cfg.room.max_participants,
            empty_timeout: cfg.room.empty_timeout,
            departure_timeout: cfg.room.departure_timeout,
            enable_remote_unmute: cfg.room.enable_remote_unmute,
            playout_delay: cfg.room.playout_delay,
            sync_streams: cfg.room.sync_streams,
            enable_full_room_in_webhooks: cfg.room.enable_full_room_in_webhooks,
            update_batch_target_size: cfg.room.update_batch_target_size,
            subscription_limit_video: cfg.limits.subscription_limit_video,
            subscription_limit_audio: cfg.limits.subscription_limit_audio,
            max_participant_name_length: cfg.limits.max_participant_name_length,
            max_metadata_size: cfg.limits.max_metadata_size,
            max_attributes_size: cfg.limits.max_attributes_size,
            use_ice_lite: cfg.rtc.use_ice_lite,
            allow_pause: cfg.rtc.congestion_control.allow_pause,
            reconnect_on_publication_error: cfg.rtc.reconnect_on_publication_error,
            reconnect_on_subscription_error: cfg.rtc.reconnect_on_subscription_error,
            reconnect_on_data_channel_error: cfg.rtc.reconnect_on_data_channel_error,
            region: cfg.region.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct HttpConfig {
    pub port: u16,
    /// Upstream `bind_addresses`: `None` (nil) listens once on all interfaces;
    /// `Some(vec![])` creates no HTTP/metrics listener at all; a `None` entry is
    /// the `""` host (all interfaces). Hostnames are resolved by the
    /// listener at bind time.
    pub bind_addresses: Option<Vec<String>>,
    pub max_request_body: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UdpMode {
    /// Shared mux on these ports (first `min(cpus, len)` of the configured range).
    Mux {
        ports: Vec<u16>,
    },
    /// Ephemeral per-transport ports, inclusive range.
    Range {
        start: u16,
        end: u16,
    },
    Disabled,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NodeIp {
    pub v4: Option<Ipv4Addr>,
    pub v6: Option<Ipv6Addr>,
}

#[derive(Clone, Debug)]
pub struct Filter<T> {
    pub includes: Vec<T>,
    pub excludes: Vec<T>,
}

// Manual impl: `#[derive(Default)]` would require `T: Default`, which
// `(IpAddr, u8)` does not satisfy.
impl<T> Default for Filter<T> {
    fn default() -> Self {
        Self {
            includes: Vec::new(),
            excludes: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IceServerProtocol {
    Udp,
    Tcp,
    Tls,
}

#[derive(Clone, Debug)]
pub struct ExternalTurnServer {
    pub host: String,
    /// Advertised in the ICE URL. The client validates it when parsing the URL.
    pub port: i64,
    pub protocol: IceServerProtocol,
    pub username: String,
    pub credential: String,
    /// Shared secret for time-limited credentials (TURN REST API).
    pub secret: Option<Vec<u8>>,
    pub ttl: Duration,
}

#[derive(Clone, Debug)]
pub struct PliThrottle {
    pub low: Duration,
    pub mid: Duration,
    pub high: Duration,
}

/// Feedback-estimate source, independent of whether allocation control is enabled.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum FeedbackMode {
    #[default]
    ReceiverEstimate,
    InterceptorEstimate,
    NativeSendSide,
}

/// Explicitly requested sending policy; retaining a value is not proof of a runtime consumer.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SendSidePacer {
    /// The explicit empty string; the effective native fallback is selected at transport creation.
    Unspecified,
    PassThrough,
    #[default]
    NoQueue,
    LeakyBucket,
}

/// Signed nanoseconds used by congestion policies. Negative and zero are distinct.
/// This neutral value preserves upstream arithmetic without a Go/config-parser dependency.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PolicyDuration(i64);

impl PolicyDuration {
    pub const ZERO: Self = Self(0);

    pub const fn from_nanos(nanos: i64) -> Self {
        Self(nanos)
    }
    pub const fn as_nanos_i64(self) -> i64 {
        self.0
    }
    pub const fn from_millis(millis: i64) -> Self {
        Self(
            millis
                .checked_mul(1_000_000)
                .expect("policy duration milliseconds overflow"),
        )
    }
    pub const fn from_secs(seconds: i64) -> Self {
        Self(
            seconds
                .checked_mul(1_000_000_000)
                .expect("policy duration seconds overflow"),
        )
    }
    /// The consumer must explicitly decide how a negative policy duration acts.
    /// No negative input is silently converted into a zero timer.
    pub const fn nonnegative_to_std(self) -> Option<Duration> {
        if self.0 < 0 {
            None
        } else {
            Some(Duration::from_nanos(self.0 as u64))
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct CongestionControl {
    /// Allocation toggle, not the estimate source selector.
    pub enabled: bool,
    pub allow_pause: bool,
    pub use_send_side_bwe: bool,
    pub use_send_side_bwe_interceptor: bool,
    pub send_side_bwe_pacer: SendSidePacer,
    pub stream_allocator: StreamAllocatorConfig,
    pub remote_bwe: RemoteBweConfig,
    pub send_side_bwe: SendSideBweConfig,
}

impl CongestionControl {
    pub const fn effective_feedback_mode(&self) -> FeedbackMode {
        if self.use_send_side_bwe {
            FeedbackMode::NativeSendSide
        } else if self.use_send_side_bwe_interceptor {
            FeedbackMode::InterceptorEstimate
        } else {
            FeedbackMode::ReceiverEstimate
        }
    }
}

impl Default for CongestionControl {
    fn default() -> Self {
        Self {
            enabled: true,
            allow_pause: false,
            use_send_side_bwe: false,
            use_send_side_bwe_interceptor: false,
            send_side_bwe_pacer: SendSidePacer::NoQueue,
            stream_allocator: StreamAllocatorConfig::default(),
            remote_bwe: RemoteBweConfig::default(),
            send_side_bwe: SendSideBweConfig::default(),
        }
    }
}

/// Normalized policy values; defaults are frozen independently of YAML decoding.
#[derive(Clone, Debug, PartialEq)]
pub struct StreamAllocatorConfig {
    pub min_channel_capacity: i64,
    pub disable_estimation_unmanaged_tracks: bool,
    pub probe_mode: String,
    pub probe_overage_pct: i64,
    pub probe_min_bps: i64,
    pub paused_min_wait: PolicyDuration,
}
impl Default for StreamAllocatorConfig {
    fn default() -> Self {
        Self {
            min_channel_capacity: 0,
            disable_estimation_unmanaged_tracks: false,
            probe_mode: "padding".to_string(),
            probe_overage_pct: 120,
            probe_min_bps: 200_000,
            paused_min_wait: PolicyDuration::from_secs(5),
        }
    }
}

/// Normalized policy values; defaults are frozen independently of YAML decoding.
#[derive(Clone, Debug, PartialEq)]
pub struct RemoteBweConfig {
    pub nack_ratio_attenuator: f64,
    pub expected_usage_threshold: f64,
    pub channel_observer_probe: ChannelObserverConfig,
    pub channel_observer_non_probe: ChannelObserverConfig,
    pub probe_controller: RemoteProbeControllerConfig,
}
impl Default for RemoteBweConfig {
    fn default() -> Self {
        Self {
            nack_ratio_attenuator: 0.4,
            expected_usage_threshold: 0.95,
            channel_observer_probe: ChannelObserverConfig {
                estimate: TrendDetectorConfig::new(
                    3,
                    3,
                    0.0,
                    PolicyDuration::from_secs(5),
                    PolicyDuration::ZERO,
                    PolicyDuration::from_secs(10),
                ),
                nack: NackTrackerConfig {
                    window_min_duration: PolicyDuration::from_millis(500),
                    window_max_duration: PolicyDuration::from_secs(1),
                    ratio_threshold: 0.04,
                },
            },
            channel_observer_non_probe: ChannelObserverConfig {
                estimate: TrendDetectorConfig::new(
                    12,
                    8,
                    -0.6,
                    PolicyDuration::from_secs(5),
                    PolicyDuration::from_millis(500),
                    PolicyDuration::from_secs(10),
                ),
                nack: NackTrackerConfig {
                    window_min_duration: PolicyDuration::from_secs(2),
                    window_max_duration: PolicyDuration::from_secs(3),
                    ratio_threshold: 0.08,
                },
            },
            probe_controller: RemoteProbeControllerConfig::default(),
        }
    }
}

/// Normalized policy values; defaults are frozen independently of YAML decoding.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ChannelObserverConfig {
    pub estimate: TrendDetectorConfig,
    pub nack: NackTrackerConfig,
}

/// Normalized policy values; defaults are frozen independently of YAML decoding.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TrendDetectorConfig {
    pub required_samples: i64,
    pub required_samples_min: i64,
    pub downward_trend_threshold: f64,
    pub downward_trend_max_wait: PolicyDuration,
    pub collapse_threshold: PolicyDuration,
    pub validity_window: PolicyDuration,
}

/// Normalized policy values; defaults are frozen independently of YAML decoding.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct NackTrackerConfig {
    pub window_min_duration: PolicyDuration,
    pub window_max_duration: PolicyDuration,
    pub ratio_threshold: f64,
}

/// Normalized policy values; defaults are frozen independently of YAML decoding.
#[derive(Clone, Debug, PartialEq)]
pub struct RemoteProbeControllerConfig {
    pub probe_regulator: ProbeRegulatorConfig,
    pub settle_wait_num_rtt: u32,
    pub settle_wait_min: PolicyDuration,
    pub settle_wait_max: PolicyDuration,
}
impl Default for RemoteProbeControllerConfig {
    fn default() -> Self {
        Self {
            probe_regulator: ProbeRegulatorConfig::default(),
            settle_wait_num_rtt: 5,
            settle_wait_min: PolicyDuration::from_millis(250),
            settle_wait_max: PolicyDuration::from_secs(5),
        }
    }
}

/// Normalized policy values; defaults are frozen independently of YAML decoding.
#[derive(Clone, Debug, PartialEq)]
pub struct ProbeRegulatorConfig {
    pub base_interval: PolicyDuration,
    pub backoff_factor: f64,
    pub max_interval: PolicyDuration,
    pub min_duration: PolicyDuration,
    pub max_duration: PolicyDuration,
    pub duration_increase_factor: f64,
}
impl Default for ProbeRegulatorConfig {
    fn default() -> Self {
        Self {
            base_interval: PolicyDuration::from_secs(3),
            backoff_factor: 1.5,
            max_interval: PolicyDuration::from_secs(120),
            min_duration: PolicyDuration::from_millis(200),
            max_duration: PolicyDuration::from_secs(20),
            duration_increase_factor: 1.5,
        }
    }
}

/// Normalized policy values; defaults are frozen independently of YAML decoding.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SendSideBweConfig {
    pub congestion_detector: CongestionDetectorConfig,
}

/// Normalized policy values; defaults are frozen independently of YAML decoding.
#[derive(Clone, Debug, PartialEq)]
pub struct CongestionDetectorConfig {
    pub packet_group: PacketGroupConfig,
    pub packet_group_max_age: PolicyDuration,
    pub probe_packet_group: ProbePacketGroupConfig,
    pub probe_regulator: ProbeRegulatorConfig,
    pub probe_signal: ProbeSignalConfig,
    pub jqr_min_delay: PolicyDuration,
    pub jqr_min_trend_coefficient: f64,
    pub dqr_max_delay: PolicyDuration,
    pub weighted_loss: WeightedLossConfig,
    pub jqr_min_weighted_loss: f64,
    pub dqr_max_weighted_loss: f64,
    pub queuing_delay_early_warning_jqr: CongestionSignalConfig,
    pub queuing_delay_early_warning_dqr: CongestionSignalConfig,
    pub loss_early_warning_jqr: CongestionSignalConfig,
    pub loss_early_warning_dqr: CongestionSignalConfig,
    pub queuing_delay_congested_jqr: CongestionSignalConfig,
    pub queuing_delay_congested_dqr: CongestionSignalConfig,
    pub loss_congested_jqr: CongestionSignalConfig,
    pub loss_congested_dqr: CongestionSignalConfig,
    pub congested_ctr_trend: TrendDetectorConfig,
    pub congested_ctr_epsilon: f64,
    pub congested_packet_group: PacketGroupConfig,
    pub estimation_window_duration: PolicyDuration,
}
impl Default for CongestionDetectorConfig {
    fn default() -> Self {
        Self {
            packet_group: default_packet_group(30, PolicyDuration::from_millis(500)),
            packet_group_max_age: PolicyDuration::from_secs(10),
            probe_packet_group: ProbePacketGroupConfig::default(),
            probe_regulator: ProbeRegulatorConfig::default(),
            probe_signal: ProbeSignalConfig::default(),
            jqr_min_delay: PolicyDuration::from_millis(50),
            jqr_min_trend_coefficient: 0.8,
            dqr_max_delay: PolicyDuration::from_millis(20),
            weighted_loss: WeightedLossConfig::default(),
            jqr_min_weighted_loss: 0.25,
            dqr_max_weighted_loss: 0.1,
            queuing_delay_early_warning_jqr: default_congestion_signal(2, 200),
            queuing_delay_early_warning_dqr: default_congestion_signal(3, 300),
            loss_early_warning_jqr: default_congestion_signal(3, 300),
            loss_early_warning_dqr: default_congestion_signal(4, 400),
            queuing_delay_congested_jqr: default_congestion_signal(4, 400),
            queuing_delay_congested_dqr: default_congestion_signal(5, 500),
            loss_congested_jqr: default_congestion_signal(6, 600),
            loss_congested_dqr: default_congestion_signal(6, 600),
            congested_ctr_trend: TrendDetectorConfig::new(
                4,
                2,
                -0.5,
                PolicyDuration::from_secs(2),
                PolicyDuration::from_millis(500),
                PolicyDuration::from_secs(10),
            ),
            congested_ctr_epsilon: 0.05,
            congested_packet_group: default_packet_group(20, PolicyDuration::from_millis(150)),
            estimation_window_duration: PolicyDuration::from_secs(1),
        }
    }
}

/// Normalized policy values; defaults are frozen independently of YAML decoding.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PacketGroupConfig {
    pub min_packets: i64,
    pub max_window_duration: PolicyDuration,
}

/// Normalized policy values; defaults are frozen independently of YAML decoding.
#[derive(Clone, Debug, PartialEq)]
pub struct ProbePacketGroupConfig {
    pub packet_group: PacketGroupConfig,
    pub settle_wait_num_rtt: u32,
    pub settle_wait_min: PolicyDuration,
    pub settle_wait_max: PolicyDuration,
}
impl Default for ProbePacketGroupConfig {
    fn default() -> Self {
        Self {
            packet_group: default_packet_group(16384, PolicyDuration::from_secs(60)),
            settle_wait_num_rtt: 5,
            settle_wait_min: PolicyDuration::from_millis(250),
            settle_wait_max: PolicyDuration::from_secs(5),
        }
    }
}

/// Normalized policy values; defaults are frozen independently of YAML decoding.
#[derive(Clone, Debug, PartialEq)]
pub struct ProbeSignalConfig {
    pub min_bytes_ratio: f64,
    pub min_duration_ratio: f64,
    pub jqr_min_delay: PolicyDuration,
    pub dqr_max_delay: PolicyDuration,
    pub weighted_loss: WeightedLossConfig,
    pub jqr_min_weighted_loss: f64,
    pub dqr_max_weighted_loss: f64,
}
impl Default for ProbeSignalConfig {
    fn default() -> Self {
        Self {
            min_bytes_ratio: 0.5,
            min_duration_ratio: 0.5,
            jqr_min_delay: PolicyDuration::from_millis(50),
            dqr_max_delay: PolicyDuration::from_millis(20),
            weighted_loss: WeightedLossConfig::default(),
            jqr_min_weighted_loss: 0.25,
            dqr_max_weighted_loss: 0.1,
        }
    }
}

/// Normalized policy values; defaults are frozen independently of YAML decoding.
#[derive(Clone, Debug, PartialEq)]
pub struct WeightedLossConfig {
    pub min_duration_for_loss_validity: PolicyDuration,
    pub base_duration: PolicyDuration,
    pub base_pps: i64,
    pub loss_penalty_factor: f64,
}
impl Default for WeightedLossConfig {
    fn default() -> Self {
        Self {
            min_duration_for_loss_validity: PolicyDuration::from_millis(100),
            base_duration: PolicyDuration::from_millis(500),
            base_pps: 30,
            loss_penalty_factor: 0.25,
        }
    }
}

/// Normalized policy values; defaults are frozen independently of YAML decoding.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CongestionSignalConfig {
    pub min_number_of_groups: i64,
    pub min_duration: PolicyDuration,
}

impl TrendDetectorConfig {
    const fn new(
        required_samples: i64,
        required_samples_min: i64,
        downward_trend_threshold: f64,
        downward_trend_max_wait: PolicyDuration,
        collapse_threshold: PolicyDuration,
        validity_window: PolicyDuration,
    ) -> Self {
        Self {
            required_samples,
            required_samples_min,
            downward_trend_threshold,
            downward_trend_max_wait,
            collapse_threshold,
            validity_window,
        }
    }
}
const fn default_congestion_signal(groups: i64, millis: i64) -> CongestionSignalConfig {
    CongestionSignalConfig {
        min_number_of_groups: groups,
        min_duration: PolicyDuration::from_millis(millis),
    }
}
const fn default_packet_group(
    min_packets: i64,
    max_window_duration: PolicyDuration,
) -> PacketGroupConfig {
    PacketGroupConfig {
        min_packets,
        max_window_duration,
    }
}

/// Signed source SCTP settings. Nonpositive values preserve upstream defaults;
/// positive values are converted to uint32 at the native assembly boundary.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SctpConfig {
    pub min_cwnd: i64,
    pub fast_rtx_wnd: i64,
    pub cwnd_ca_step: i64,
}

#[derive(Clone, Debug)]
pub struct RtcConfig {
    /// Controlling-side ICE nomination waits: host, srflx, prflx, relay.
    /// Fixed Pion defaults apply unless an official duration is strictly positive.
    pub ice_acceptance_min_waits: [Duration; 4],
    /// Shared node forwarding-transit collector; absent when any original interval is zero.
    pub forward_stats: Option<std::sync::Arc<crate::forward_stats::ForwardStats>>,
    pub udp: UdpMode,
    pub tcp_port: Option<u16>,
    pub node_ip: NodeIp,
    /// Node IP was given explicitly or discovered externally: advertise it in place
    /// of local addresses (1:1 NAT).
    pub use_node_ip_mapping: bool,
    pub advertise_internal_ip: bool,
    pub stun_servers: Vec<String>,
    pub turn_servers: Vec<ExternalTurnServer>,
    pub use_ice_lite: bool,
    /// ForceTCP: never bind or advertise ICE/UDP; TCP media only
    /// (mediatransportutil webrtc_config.go:123).
    pub force_tcp: bool,
    /// UseStunPortAsICE: in mux mode add a standalone UDP mux on the STUN
    /// port 3478 alongside the configured ICE ports (webrtc_config.go:162).
    pub use_stun_port_as_ice: bool,
    /// UseExternalIP: discover the public node IP via STUN and advertise it in
    /// place of local addresses (webrtc_config.go:91). Drives the external-IP
    /// discovery path that `external_ip_only` filters on.
    pub use_external_ip: bool,
    /// Skip only the source-bound external-IP hairpin validation after STUN.
    pub skip_external_ip_validation: bool,
    /// ExternalIPOnly: when external-IP discovery resolved mappings, advertise
    /// only the local IPv4s that mapped externally (IPv6 untouched), matching
    /// the narrowed candidate filter at webrtc_config.go:402.
    pub external_ip_only: bool,
    /// BatchIO batched UDP write (webrtc_config.go:146, pion `NewBatchConn`):
    /// datagrams accumulated per batch (`0` disables) and the max flush
    /// interval. Real `sendmmsg` batching only exists on Linux; elsewhere the
    /// writer is a pass-through, matching pion's `runtime.GOOS` gate.
    pub batch_write_size: usize,
    pub batch_write_interval: Duration,
    pub interfaces: Filter<String>,
    /// CIDR filters as (network, prefix length).
    pub ips: Filter<(IpAddr, u8)>,
    pub enable_loopback_candidate: bool,
    /// UseMDNS: accept remote mDNS (`.local`) ICE candidates by resolving them
    /// with an mDNS query. Upstream keeps pion in `QueryOnly` mode when set and
    /// disables mDNS entirely when unset (mediatransportutil webrtc_config.go:85),
    /// where local host candidates keep their IPs in both cases
    /// (`livekit-ice` mdns.go enum).
    pub use_mdns: bool,
    /// `EnableWarp`: the experimental WARP transport bundle. Upstream turns both
    /// halves on for every non-one-shot peer connection
    /// (`pkg/rtc/transport.go:389-393` → `EnableSped(true)` + `EnableSctpSnap(true)`,
    /// gated by `roommanager.go:487` `!useOneShotSignallingMode`). This build
    /// implements SNAP (`a=sctp-init` in SDP) and native authenticated SPED
    /// (DTLS-in-STUN flights and CRC32 ACKs). New SPED execution/official-peer
    /// acceptance remains separate from historical SNAP evidence.
    pub enable_warp: bool,
    /// Original Buffer automatically restarts only when this option is enabled.
    pub enable_rtp_stream_restart_detection: bool,
    /// Signed source DataTrack unreliable writer target (zero bypasses control).
    pub data_track_target_latency: PolicyDuration,
    pub data_channel_slow_threshold: i64,
    pub data_channel_max_buffered_amount: u64,
    pub data_channel_lossy_target_latency: PolicyDuration,
    pub sctp: SctpConfig,
    pub packet_buffer_size_video: usize,
    pub packet_buffer_size_audio: usize,
    pub pli_throttle: PliThrottle,
    pub congestion_control: CongestionControl,
    pub allow_tcp_fallback: bool,
    pub tcp_fallback_rtt_threshold: i64,
    pub allow_udp_unstable_fallback: bool,
    pub reconnect_on_publication_error: bool,
    pub reconnect_on_subscription_error: bool,
    pub reconnect_on_data_channel_error: bool,
}

#[derive(Clone, Debug)]
pub struct RoomDefaults {
    /// Per-owner CreateRoom RPC retry timeout, signed source duration.
    pub create_room_timeout: PolicyDuration,
    /// Exact source attempt limit; zero/negative are not a disable switch.
    pub create_room_attempts: i64,
    pub auto_create: bool,
    pub enabled_codecs: Vec<CodecSpec>,
    pub max_participants: u32,
    pub empty_timeout: Duration,
    pub departure_timeout: Duration,
    pub enable_remote_unmute: bool,
    pub playout_delay: Option<(u32, u32)>,
    pub sync_streams: bool,
    pub enable_full_room_in_webhooks: bool,
    pub update_batch_target_size: i64,
    /// Named presets (`room.room_configurations`). `None` is a nil entry
    /// (`name: ~`), which upstream dereferences when the preset is applied.
    pub presets: BTreeMap<String, Option<RoomConfig>>,
}

#[derive(Clone, Debug)]
pub struct AudioConfig {
    /// dBov threshold (0..=127, lower is louder).
    pub active_level: u8,
    pub min_percentile: u8,
    pub update_interval: Duration,
    pub smooth_intervals: u32,
    pub active_red_encoding: bool,
}

#[derive(Clone, Debug)]
pub struct VideoConfig {
    pub codec_regression_threshold: i64,
    pub dynacast_pause_delay: Duration,
    pub bitrate_report: VideoBitrateReport,
    /// `video.stream_tracker_manager` tracker mode and per-layer settings.
    pub stream_trackers: VideoStreamTrackers,
}

/// One layer's fixed Go `streamtracker.StreamTrackerPacketConfig`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PacketTrackerSpec {
    pub samples_required: u32,
    pub cycles_required: u32,
    pub cycle_duration: Duration,
}

/// The tracker chosen by fixed Go for an ordinary spatial layer.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum StreamTrackerSpec {
    Packet(PacketTrackerSpec),
    Frame { min_fps: f64 },
}

/// Per spatial layer tracker settings; `None` means the layer has no
/// configured tracker, so fixed Go never adds one and the layer never becomes
/// available.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct VideoStreamTrackers {
    pub video: [Option<StreamTrackerSpec>; 3],
    pub screenshare: [Option<StreamTrackerSpec>; 3],
}

impl VideoStreamTrackers {
    pub fn uses_frame_tracking(&self) -> bool {
        self.video
            .iter()
            .chain(&self.screenshare)
            .any(|spec| matches!(spec, Some(StreamTrackerSpec::Frame { .. })))
    }
}

impl Default for VideoStreamTrackers {
    /// `DefaultStreamTrackerPacketConfigVideo` / `...Screenshare`.
    fn default() -> Self {
        let spec = |samples_required, cycles_required, cycle_duration| {
            Some(StreamTrackerSpec::Packet(PacketTrackerSpec {
                samples_required,
                cycles_required,
                cycle_duration,
            }))
        };
        let half = Duration::from_millis(500);
        Self {
            video: [spec(1, 4, half), spec(5, 20, half), spec(5, 20, half)],
            screenshare: [spec(1, 1, Duration::from_secs(2)); 3],
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct VideoBitrateReport {
    pub video: [Option<Duration>; 3],
    pub screenshare: [Option<Duration>; 3],
}

impl Default for VideoBitrateReport {
    fn default() -> Self {
        Self {
            video: [Some(Duration::from_secs(1)); 3],
            screenshare: [Some(Duration::from_secs(4)); 3],
        }
    }
}

#[derive(Clone, Debug)]
pub struct TurnConfig {
    pub enabled: bool,
    pub domain: String,
    pub cert_file: Option<PathBuf>,
    pub key_file: Option<PathBuf>,
    pub tls_port: Option<u16>,
    pub udp_port: Option<u16>,
    pub relay_range: (u16, u16),
    pub external_tls: bool,
    pub bind_addresses: Vec<IpAddr>,
    pub per_user_allocation_limit: u32,
    pub ttl: Duration,
    pub allow_restricted_peer_cidrs: Vec<(IpAddr, u8)>,
    pub deny_peer_cidrs: Vec<(IpAddr, u8)>,
    /// Require a PROXY protocol header on the TURN TCP listener (`turn.proxy_protocol`).
    pub proxy_protocol: bool,
    /// Trusted proxy CIDRs, parsed at config time when `proxy_protocol` is active.
    pub proxy_trusted_cidrs: Vec<(IpAddr, u8)>,
}

#[derive(Clone, Debug, Default)]
pub struct WebhookConfig {
    pub urls: Vec<String>,
    pub api_key: String,
    pub include_events: Vec<String>,
    pub exclude_events: Vec<String>,
    /// Zero selects the upstream default. Negative expires every queued event.
    pub max_age: PolicyDuration,
    /// Zero selects the upstream default. Negative makes every queue full.
    pub max_depth: i64,
}

#[derive(Clone, Debug)]
pub struct Limits {
    /// Preserve the signed upstream limit, including negative rejection semantics.
    pub max_data_track_custom_encoding_length: i64,
    pub max_data_blob_key_length: i64,
    pub max_data_blob_size: u32,
    pub subscription_limit_video: Option<u32>,
    pub subscription_limit_audio: Option<u32>,
    /// `None` = unlimited for metadata/attributes only.
    pub max_metadata_size: Option<usize>,
    pub max_attributes_size: Option<usize>,
    /// UTF-8 bytes; zero alone is unlimited; negative rejects even empty.
    pub max_room_name_length: i64,
    /// UTF-8 bytes: only zero is unlimited; negative rejects every string at the predicate.
    pub max_participant_identity_length: i64,
    pub max_participant_name_length: i64,
    pub signal_message_size_limit: Option<usize>,
}

#[derive(Clone, Debug, Default)]
pub struct LoggingConfig {
    pub sample: bool,
    /// Retained configuration: fixed protocol logger does not consume these two
    /// fields; root sampling uses item_sample_initial/interval instead.
    pub sample_initial: i64,
    pub sample_interval: i64,
    pub item_sample_seconds: i64,
    pub item_sample_initial: i64,
    pub item_sample_interval: i64,
    pub level: String,
    pub json: bool,
    pub component_levels: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default)]
pub struct PrometheusConfig {
    pub port: Option<u16>,
    pub username: Option<String>,
    pub password: Option<String>,
}

/// `livecloud:` extension tree (CLI-C06). Only what is implemented is modelled;
/// unknown keys in the tree are rejected by `lk-config`.
#[derive(Clone, Debug, Default)]
pub struct ExtensionConfig {
    pub schema_version: u32,
    pub deployment: Deployment,
    pub https: Option<HttpsConfig>,
    /// Explicit legacy external-worker integration profile; not an official LiveKit field.
    pub ingress_worker_protocol: String,
}

/// Native HTTPS/WSS is a product extension; official `port` remains plaintext.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpsConfig {
    pub port: u16,
    pub cert_file: PathBuf,
    pub key_file: PathBuf,
    pub bind_addresses: Vec<IpAddr>,
    pub http_enabled: bool,
    pub max_connections: usize,
    pub handshake_timeout: Duration,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Deployment {
    #[default]
    Standalone,
}

impl ServerConfig {
    pub fn secret_for(&self, api_key: &str) -> Option<&str> {
        self.keys
            .iter()
            .find(|(k, _)| k == api_key)
            .map(|(_, s)| s.as_str())
    }
}

/// Match fixed Go signed name limits over UTF-8 byte counts. Only zero disables
/// the limit. A negative limit rejects even zero bytes; callers retain their
/// separate empty/optional-field and permission checks in source order.
pub fn signed_byte_length_allowed(limit: i64, length: usize) -> bool {
    limit == 0 || (limit > 0 && length as u128 <= limit as u128)
}

#[cfg(test)]
mod room_policy_tests {
    use super::*;

    fn normalized_config() -> ServerConfig {
        ServerConfig {
            http: HttpConfig {
                port: 7880,
                bind_addresses: None,
                max_request_body: 10 << 20,
            },
            rtc: RtcConfig {
                ice_acceptance_min_waits: [
                    Duration::ZERO,
                    Duration::from_millis(500),
                    Duration::from_secs(1),
                    Duration::from_secs(2),
                ],
                forward_stats: None,
                udp: UdpMode::Mux { ports: vec![7882] },
                tcp_port: Some(7881),
                node_ip: NodeIp::default(),
                use_node_ip_mapping: false,
                advertise_internal_ip: false,
                stun_servers: vec![],
                turn_servers: vec![],
                use_ice_lite: false,
                force_tcp: false,
                use_stun_port_as_ice: false,
                use_external_ip: false,
                skip_external_ip_validation: false,
                external_ip_only: false,
                batch_write_size: 0,
                batch_write_interval: Duration::ZERO,
                interfaces: Filter::default(),
                ips: Filter::default(),
                enable_loopback_candidate: false,
                use_mdns: false,
                enable_warp: false,
                enable_rtp_stream_restart_detection: false,
                data_track_target_latency: PolicyDuration::from_millis(100),
                data_channel_slow_threshold: 0,
                data_channel_max_buffered_amount: 0,
                data_channel_lossy_target_latency: PolicyDuration::from_nanos(0),
                sctp: Default::default(),
                packet_buffer_size_video: 500,
                packet_buffer_size_audio: 200,
                pli_throttle: PliThrottle {
                    low: Duration::from_millis(500),
                    mid: Duration::from_secs(1),
                    high: Duration::from_secs(1),
                },
                congestion_control: CongestionControl {
                    enabled: true,
                    allow_pause: false,
                    use_send_side_bwe: false,
                    ..CongestionControl::default()
                },
                allow_tcp_fallback: true,
                tcp_fallback_rtt_threshold: 0,
                allow_udp_unstable_fallback: false,
                reconnect_on_publication_error: false,
                reconnect_on_subscription_error: false,
                reconnect_on_data_channel_error: false,
            },
            room: RoomDefaults {
                create_room_timeout: PolicyDuration::from_secs(10),
                create_room_attempts: 3,
                auto_create: true,
                enabled_codecs: vec![
                    CodecSpec {
                        mime: "audio/opus".into(),
                        fmtp_line: String::new(),
                    },
                    CodecSpec {
                        mime: "video/VP8".into(),
                        fmtp_line: String::new(),
                    },
                    CodecSpec {
                        mime: "video/rtx".into(),
                        fmtp_line: String::new(),
                    },
                ],
                max_participants: 0,
                empty_timeout: Duration::from_secs(300),
                departure_timeout: Duration::from_secs(20),
                enable_remote_unmute: false,
                playout_delay: None,
                sync_streams: false,
                enable_full_room_in_webhooks: false,
                update_batch_target_size: 128 << 10,
                presets: BTreeMap::new(),
            },
            audio: AudioConfig {
                active_level: 35,
                min_percentile: 40,
                update_interval: Duration::from_millis(400),
                smooth_intervals: 2,
                active_red_encoding: false,
            },
            video: VideoConfig {
                codec_regression_threshold: 5,
                dynacast_pause_delay: Duration::from_secs(5),
                bitrate_report: VideoBitrateReport::default(),
                stream_trackers: VideoStreamTrackers::default(),
            },
            turn: TurnConfig {
                enabled: false,
                domain: String::new(),
                cert_file: None,
                key_file: None,
                tls_port: None,
                udp_port: None,
                relay_range: (30000, 40000),
                external_tls: false,
                bind_addresses: vec![],
                per_user_allocation_limit: 12,
                ttl: Duration::from_secs(300),
                allow_restricted_peer_cidrs: vec![],
                deny_peer_cidrs: vec![],
                proxy_protocol: false,
                proxy_trusted_cidrs: vec![],
            },
            webhook: WebhookConfig::default(),
            limits: Limits {
                max_data_track_custom_encoding_length: 32,
                max_data_blob_key_length: 256,
                max_data_blob_size: 64000,
                subscription_limit_video: None,
                subscription_limit_audio: None,
                max_metadata_size: Some(512 << 10),
                max_attributes_size: Some(64 << 10),
                max_room_name_length: 256,
                max_participant_identity_length: 256,
                max_participant_name_length: 256,
                signal_message_size_limit: Some(2 << 20),
            },
            logging: LoggingConfig {
                level: "info".into(),
                json: false,
                component_levels: BTreeMap::new(),
                ..LoggingConfig::default()
            },
            prometheus: PrometheusConfig::default(),
            keys: vec![("devkey".into(), "secret".into())],
            region: String::new(),
            node_id: "ND_test".into(),
            development: true,
            enable_data_tracks: false,
            enable_participant_data_blob: false,
            enable_psrpc_for_get_list_participants: false,
            extension: None,
        }
    }

    #[test]
    fn projection_preserves_every_normalized_room_policy_field() {
        let mut cfg = normalized_config();
        let codecs = vec![
            CodecSpec {
                mime: "video/AV1".into(),
                fmtp_line: "profile=1".into(),
            },
            CodecSpec {
                mime: "audio/opus".into(),
                fmtp_line: "stereo=1".into(),
            },
        ];
        cfg.room.enabled_codecs = codecs.clone();
        cfg.room.max_participants = 37;
        cfg.room.playout_delay = Some((30, 90));
        cfg.room.empty_timeout = Duration::from_millis(73_456);
        cfg.room.departure_timeout = Duration::from_millis(9_876);
        cfg.room.enable_remote_unmute = true;
        cfg.room.sync_streams = true;
        cfg.room.enable_full_room_in_webhooks = true;
        cfg.limits.subscription_limit_video = Some(13);
        cfg.limits.subscription_limit_audio = Some(17);
        cfg.limits.max_participant_name_length = 19;
        cfg.limits.max_metadata_size = Some(23);
        cfg.limits.max_attributes_size = Some(29);
        cfg.rtc.use_ice_lite = true;
        cfg.rtc.congestion_control.allow_pause = true;
        cfg.region = "ap-custom".into();
        let policy = RoomPolicy::from(&cfg);
        assert_eq!(
            policy,
            RoomPolicy {
                transport_fallback: Default::default(),
                enable_data_tracks: false,
                enable_participant_data_blob: false,
                max_data_track_custom_encoding_length: 32,
                max_data_blob_key_length: 256,
                max_data_blob_size: 64000,
                enabled_codecs: codecs,
                max_participants: 37,
                empty_timeout: Duration::from_millis(73_456),
                departure_timeout: Duration::from_millis(9_876),
                enable_remote_unmute: true,
                playout_delay: Some((30, 90)),
                sync_streams: true,
                enable_full_room_in_webhooks: true,
                update_batch_target_size: 128 << 10,
                subscription_limit_video: Some(13),
                subscription_limit_audio: Some(17),
                max_participant_name_length: 19,
                max_metadata_size: Some(23),
                max_attributes_size: Some(29),
                use_ice_lite: true,
                allow_pause: true,
                reconnect_on_publication_error: false,
                reconnect_on_subscription_error: false,
                reconnect_on_data_channel_error: false,
                region: "ap-custom".into(),
            }
        );
        // The policy is an owned snapshot, not a live view of unrelated assembly config.
        cfg.room.enabled_codecs.clear();
        cfg.region.clear();
        cfg.keys.clear();
        assert_eq!(policy.enabled_codecs.len(), 2);
        assert_eq!(policy.region, "ap-custom");
    }

    #[test]
    fn projection_preserves_zero_unlimited_and_false_without_new_defaults() {
        let mut cfg = normalized_config();
        cfg.room.enabled_codecs.clear();
        cfg.room.max_participants = 0;
        cfg.room.empty_timeout = Duration::ZERO;
        cfg.room.departure_timeout = Duration::ZERO;
        cfg.room.enable_remote_unmute = false;
        cfg.room.sync_streams = false;
        cfg.room.enable_full_room_in_webhooks = false;
        cfg.limits.subscription_limit_video = None;
        cfg.limits.subscription_limit_audio = Some(0);
        cfg.limits.max_participant_name_length = 0;
        cfg.limits.max_metadata_size = Some(0);
        cfg.limits.max_attributes_size = None;
        cfg.rtc.use_ice_lite = false;
        cfg.rtc.congestion_control.allow_pause = false;
        cfg.region.clear();
        assert_eq!(
            RoomPolicy::from(&cfg),
            RoomPolicy {
                transport_fallback: Default::default(),
                enable_data_tracks: false,
                enable_participant_data_blob: false,
                max_data_track_custom_encoding_length: 32,
                max_data_blob_key_length: 256,
                max_data_blob_size: 64000,
                enabled_codecs: vec![],
                max_participants: 0,
                empty_timeout: Duration::ZERO,
                departure_timeout: Duration::ZERO,
                enable_remote_unmute: false,
                playout_delay: None,
                sync_streams: false,
                enable_full_room_in_webhooks: false,
                update_batch_target_size: 128 << 10,
                subscription_limit_video: None,
                subscription_limit_audio: Some(0),
                max_participant_name_length: 0,
                max_metadata_size: Some(0),
                max_attributes_size: None,
                use_ice_lite: false,
                allow_pause: false,
                reconnect_on_publication_error: false,
                reconnect_on_subscription_error: false,
                reconnect_on_data_channel_error: false,
                region: String::new(),
            }
        );
    }
}

#[cfg(test)]
mod actual_go_signed_name_oracle {
    use super::*;
    #[test]
    #[ignore = "requires actual fixed-Go64 predicate output SIGNED_NAME_GO_RESULTS, normalized TSV"]
    fn actual_fixed_go_room_identity_name_predicates() {
        let cases: &[(&str, i64, &str)] = &[
            ("limit-0-value-0", -9223372036854775808, ""),
            ("limit-0-value-1", -9223372036854775808, "a"),
            ("limit-0-value-2", -9223372036854775808, "é"),
            ("limit-0-value-3", -9223372036854775808, "éa"),
            ("limit-0-value-4", -9223372036854775808, "中"),
            ("limit-0-value-5", -9223372036854775808, "🙂"),
            ("limit-0-value-6", -9223372036854775808, "a\0b"),
            ("limit-0-value-7", -9223372036854775808, "éé"),
            ("limit-1-value-0", -2, ""),
            ("limit-1-value-1", -2, "a"),
            ("limit-1-value-2", -2, "é"),
            ("limit-1-value-3", -2, "éa"),
            ("limit-1-value-4", -2, "中"),
            ("limit-1-value-5", -2, "🙂"),
            ("limit-1-value-6", -2, "a\0b"),
            ("limit-1-value-7", -2, "éé"),
            ("limit-2-value-0", -1, ""),
            ("limit-2-value-1", -1, "a"),
            ("limit-2-value-2", -1, "é"),
            ("limit-2-value-3", -1, "éa"),
            ("limit-2-value-4", -1, "中"),
            ("limit-2-value-5", -1, "🙂"),
            ("limit-2-value-6", -1, "a\0b"),
            ("limit-2-value-7", -1, "éé"),
            ("limit-3-value-0", 0, ""),
            ("limit-3-value-1", 0, "a"),
            ("limit-3-value-2", 0, "é"),
            ("limit-3-value-3", 0, "éa"),
            ("limit-3-value-4", 0, "中"),
            ("limit-3-value-5", 0, "🙂"),
            ("limit-3-value-6", 0, "a\0b"),
            ("limit-3-value-7", 0, "éé"),
            ("limit-4-value-0", 1, ""),
            ("limit-4-value-1", 1, "a"),
            ("limit-4-value-2", 1, "é"),
            ("limit-4-value-3", 1, "éa"),
            ("limit-4-value-4", 1, "中"),
            ("limit-4-value-5", 1, "🙂"),
            ("limit-4-value-6", 1, "a\0b"),
            ("limit-4-value-7", 1, "éé"),
            ("limit-5-value-0", 2, ""),
            ("limit-5-value-1", 2, "a"),
            ("limit-5-value-2", 2, "é"),
            ("limit-5-value-3", 2, "éa"),
            ("limit-5-value-4", 2, "中"),
            ("limit-5-value-5", 2, "🙂"),
            ("limit-5-value-6", 2, "a\0b"),
            ("limit-5-value-7", 2, "éé"),
            ("limit-6-value-0", 3, ""),
            ("limit-6-value-1", 3, "a"),
            ("limit-6-value-2", 3, "é"),
            ("limit-6-value-3", 3, "éa"),
            ("limit-6-value-4", 3, "中"),
            ("limit-6-value-5", 3, "🙂"),
            ("limit-6-value-6", 3, "a\0b"),
            ("limit-6-value-7", 3, "éé"),
            ("limit-7-value-0", 9223372036854775807, ""),
            ("limit-7-value-1", 9223372036854775807, "a"),
            ("limit-7-value-2", 9223372036854775807, "é"),
            ("limit-7-value-3", 9223372036854775807, "éa"),
            ("limit-7-value-4", 9223372036854775807, "中"),
            ("limit-7-value-5", 9223372036854775807, "🙂"),
            ("limit-7-value-6", 9223372036854775807, "a\0b"),
            ("limit-7-value-7", 9223372036854775807, "éé"),
        ];
        let output = std::fs::read_to_string(
            std::env::var("SIGNED_NAME_GO_RESULTS").expect("actual Go result TSV"),
        )
        .unwrap();
        let rows: Vec<_> = output
            .lines()
            .map(|line| line.split('\t').collect::<Vec<_>>())
            .collect();
        assert_eq!(rows.len(), 64);
        assert_eq!(cases.len(), 64);
        for ((id, limit, value), row) in cases.iter().zip(rows) {
            assert_eq!(row.len(), 5);
            assert_eq!(row[0], *id);
            assert_eq!(row[1].parse::<usize>().unwrap(), value.len());
            for result in &row[2..] {
                assert_eq!(
                    signed_byte_length_allowed(*limit, value.len()),
                    result.parse::<bool>().unwrap(),
                    "{id}"
                );
            }
        }
    }
}
