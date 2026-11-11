//! The official LiveKit v1.13.10 YAML tree (`LK/pkg/config/config.go` and the
//! package structs it embeds), with upstream defaults (`DefaultConfig`,
//! `config.go:567-667`).
//!
//! Decoding follows yaml.v3 "decode onto defaults" semantics (impl-notes §4.2):
//! - absent keys keep their default; scalars are overwritten (explicit `false`/`0`
//!   honoured); `null` on a scalar or struct keeps the current value;
//! - `null` on a list/map/pointer resets it to empty/`None`;
//! - lists are replaced, maps are merged key by key (each value decoded fresh);
//! - unknown keys are an error at any depth in strict mode
//!   (`field <key> not found in type <GoType>`) and ignored otherwise.
//!
//! Every struct is declared once with [`yaml_struct!`], which generates the struct,
//! its `Default` (the upstream defaults), the strict/lenient YAML patcher and the
//! [`Walk`] used to enumerate generated CLI flags (`generated.rs`). Field names are
//! the YAML keys verbatim, so flag names, env names and YAML keys cannot drift.

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::duration::GoDuration;
use crate::generated::{Leaf, Walk};
use crate::ports::{NodeIp, PortRange};
use crate::yamlv3::{
    Dec, Decode, Fail, GoType, Id, Kind, MapEntries, NilSlice, StructFields, decode_map,
    decode_pointer, decode_struct, go_pointer,
};

/// `bind_addresses`: Go distinguishes a nil slice (absent / `null`) from an empty
/// one (`[]`), and dev mode only installs the loopback bind for nil.
impl Decode for Option<Vec<String>> {
    fn decode(&mut self, d: &mut Dec<'_>, n: Id, go: &str) -> Result<bool, Fail> {
        let mut nil = NilSlice(self.take());
        let good = nil.decode(d, n, go)?;
        *self = nil.0;
        Ok(good)
    }
}

/// `keys:` (map[string]string) kept in document order, because the first
/// configured key signs refreshed tokens (`rtc_core::config::ServerConfig::keys`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct KeyMap(pub Vec<(String, String)>);

impl KeyMap {
    pub fn insert(&mut self, k: String, v: String) {
        match self.0.iter_mut().find(|(ek, _)| *ek == k) {
            Some(e) => e.1 = v,
            None => self.0.push((k, v)),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl GoType for KeyMap {
    fn go_type() -> String {
        "map[string]string".into()
    }
}

impl Decode for KeyMap {
    fn decode(&mut self, d: &mut Dec<'_>, n: Id, go: &str) -> Result<bool, Fail> {
        decode_map(&mut MapEntries::Ordered(&mut self.0), d, n, go)
    }
}

/// A protobuf sub-message/interface this build does not model (for example
/// non-nil generated oneof interface YAML values). Any value is accepted by the decoder; non-null presence
/// is rejected later as unsupported (CLI-C08).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Opaque {
    pub present: bool,
}

impl GoType for Opaque {
    fn go_type() -> String {
        "interface {}".into()
    }
}

impl Decode for Opaque {
    fn decode(&mut self, d: &mut Dec<'_>, n: Id, _go: &str) -> Result<bool, Fail> {
        self.present = !(d.kind(n) == Kind::Scalar && d.is_null(n));
        Ok(true)
    }
}

impl Walk for KeyMap {
    fn walk(&mut self, _path: &str, _f: &mut dyn FnMut(&str, &mut dyn Leaf)) {}
}

impl Walk for Opaque {
    fn walk(&mut self, _path: &str, _f: &mut dyn FnMut(&str, &mut dyn Leaf)) {}
}

// ---------------------------------------------------------------------------
// yaml_struct!
// ---------------------------------------------------------------------------

/// The Go type of a field: an explicit `as "<go type>"` wins, else the Rust
/// type's [`GoType`] (checked against the original types by the observer).
macro_rules! go_ty {
    ($ty:ty) => {
        <$ty as GoType>::go_type()
    };
    ($ty:ty, $gt:literal) => {
        String::from($gt)
    };
}

macro_rules! yaml_struct {
    (
        $(#[$meta:meta])*
        $name:ident => $goname:literal $(, inline_until $stop:literal)? {
            $( $(#[$fmeta:meta])* $field:ident : $ty:ty $(as $gt:literal)? = $default:expr ),* $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(Clone, Debug, PartialEq)]
        pub struct $name {
            $( $(#[$fmeta])* pub $field: $ty, )*
        }

        impl Default for $name {
            fn default() -> Self {
                Self { $( $field: $default, )* }
            }
        }

        impl $name {
            /// yaml.v3 field names in declaration order.
            pub(crate) const FIELDS: &'static [&'static str] = &[$( stringify!($field) ),*];
        }

        impl GoType for $name {
            fn go_type() -> String {
                $goname.into()
            }
        }

        impl StructFields for $name {
            fn field_index(key: &str) -> Option<usize> {
                Self::FIELDS.iter().position(|f| *f == key)
            }
            #[allow(unused_variables)]
            fn is_inline(index: usize) -> bool {
                $( return Self::FIELDS.iter().position(|f| *f == $stop).is_some_and(|stop| index < stop); )?
                #[allow(unreachable_code)]
                false
            }
            #[allow(unused_variables, unused_assignments, unused_mut)]
            fn decode_field(&mut self, d: &mut Dec<'_>, index: usize, value: Id) -> Result<(), Fail> {
                let mut i = 0usize;
                $(
                    if i == index {
                        d.unmarshal(value, &mut self.$field, &go_ty!($ty $(, $gt)?))?;
                        return Ok(());
                    }
                    i += 1;
                )*
                unreachable!("field index out of range")
            }
        }

        impl Decode for $name {
            fn decode(&mut self, d: &mut Dec<'_>, n: Id, go: &str) -> Result<bool, Fail> {
                decode_struct(self, d, n, go)
            }
        }

        impl Walk for $name {
            #[allow(unused_variables)]
            fn walk(&mut self, path: &str, f: &mut dyn FnMut(&str, &mut dyn Leaf)) {
                $( Walk::walk(&mut self.$field, &crate::generated::join(path, stringify!($field)), f); )*
            }
        }

        #[cfg(test)]
        impl crate::observer::TypeTree for $name {
            #[allow(unused_variables)]
            fn types(path: &str, _go: &str, out: &mut crate::observer::Out) {
                // Protobuf messages are not walked by the original reflection.
                if $goname.starts_with("livekit.") {
                    return;
                }
                $(
                    let p = crate::generated::join(path, stringify!($field));
                    let go = go_ty!($ty $(, $gt)?);
                    out.insert(p.clone(), go.clone());
                    <$ty as crate::observer::TypeTree>::types(&p, &go, out);
                )*
            }
        }

        /// Test-only canonical rendering for the config differential observer.
        #[cfg(test)]
        impl crate::observer::Render for $name {
            #[allow(unused_variables)]
            fn render(&self, path: &str, out: &mut crate::observer::Out) {
                $( crate::observer::Render::render(&self.$field, &crate::generated::join(path, stringify!($field)), out); )*
            }
            #[allow(unused_mut)]
            fn json(&self) -> crate::observer::J {
                let mut m = std::collections::BTreeMap::new();
                $( m.insert(stringify!($field).to_string(), crate::observer::Render::json(&self.$field)); )*
                crate::observer::J::Obj(m)
            }
        }
    };
}

const fn ms(v: i64) -> GoDuration {
    GoDuration::from_millis(v)
}
const fn secs(v: i64) -> GoDuration {
    GoDuration::from_secs(v)
}

// ---------------------------------------------------------------------------
// Root
// ---------------------------------------------------------------------------

yaml_struct! {
    /// `config.Config` (`config.go:78-119`).
    Config => "config.Config" {
        port: u32 = 7880,
        bind_addresses: Option<Vec<String>> as "[]string" = None,
        /// Deprecated; copied to `prometheus.port` with a warning when > 0.
        prometheus_port: u32 = 0,
        prometheus: PrometheusConfig = PrometheusConfig::default(),
        debug_handler: DebugHandlerConfig = DebugHandlerConfig::default(),
        webtransport: WebTransportConfig = WebTransportConfig::default(),
        rtc: RtcConfig = RtcConfig::default(),
        redis: RedisConfig = RedisConfig::default(),
        audio: AudioConfig = AudioConfig::default(),
        video: VideoConfig = VideoConfig::default(),
        room: RoomConfig = RoomConfig::default(),
        turn: TurnConfig = TurnConfig::default(),
        ingress: IngressConfig = IngressConfig::default(),
        sip: SipConfig = SipConfig::default(),
        webhook: WebHookConfig = WebHookConfig::default(),
        node_selector: NodeSelectorConfig = NodeSelectorConfig::default(),
        key_file: String = String::new(),
        keys: KeyMap = KeyMap::default(),
        region: String = String::new(),
        signal_relay: SignalRelayConfig = SignalRelayConfig::default(),
        psrpc: PsrpcConfig = PsrpcConfig::default(),
        /// Deprecated; overwrites `logging.level` when non-empty.
        log_level: String = String::new(),
        logging: LoggingConfig = LoggingConfig::default(),
        limit: LimitConfig = LimitConfig::default(),
        agents: AgentsConfig = AgentsConfig::default(),
        development: bool = false,
        metric: MetricConfig = MetricConfig::default(),
        trace: TracingConfig = TracingConfig::default(),
        node_stats: NodeStatsConfig = NodeStatsConfig::default(),
        enable_data_tracks: bool = true,
        enable_participant_data_blob: bool = false,
        api: ApiConfig = ApiConfig::default(),
    }
}

yaml_struct! { PrometheusConfig => "config.PrometheusConfig" {
    port: u32 = 0,
    username: String = String::new(),
    password: String = String::new(),
}}

yaml_struct! { DebugHandlerConfig => "config.DebugHandlerConfig" {
    port: u32 = 0,
}}

yaml_struct! { WebTransportConfig => "config.WebTransportConfig" {
    port: u32 = 0,
    tls_cert_file: String = String::new(),
    tls_key_file: String = String::new(),
}}

// ---------------------------------------------------------------------------
// rtc.*
// ---------------------------------------------------------------------------

yaml_struct! {
    /// `config.RTCConfig` with inline `rtcconfig.RTCConfig` first.
    RtcConfig => "config.RTCConfig", inline_until "turn_servers" {
        udp_port: PortRange = PortRange::default(),
        tcp_port: u32 = 7881,
        port_range_start: u32 = 0,
        port_range_end: u32 = 0,
        use_stun_port_as_ice: bool = false,
        node_ip: NodeIp = NodeIp::default(),
        stun_servers: Vec<String> = Vec::new(),
        use_external_ip: bool = false,
        require_ipv4: bool = false,
        skip_external_ip_validation: bool = false,
        advertise_internal_ip: bool = false,
        use_ice_lite: bool = false,
        interfaces: InterfacesConfig = InterfacesConfig::default(),
        ips: IpsConfig = IpsConfig::default(),
        enable_loopback_candidate: bool = false,
        use_mdns: bool = false,
        external_ip_only: bool = false,
        batch_io: BatchIoConfig = BatchIoConfig::default(),
        sctp_min_cwnd: i64 = 0,
        sctp_fast_rtx_wnd: i64 = 0,
        sctp_cwnd_ca_step: i64 = 0,
        host_acceptance_min_wait: GoDuration = GoDuration::ZERO,
        srflx_acceptance_min_wait: GoDuration = GoDuration::ZERO,
        prflx_acceptance_min_wait: GoDuration = GoDuration::ZERO,
        relay_acceptance_min_wait: GoDuration = GoDuration::ZERO,
        force_tcp: bool = false,
        // LiveKit's own fields
        turn_servers: Vec<TurnServer> = Vec::new(),
        enable_warp: bool = false,
        strict_acks: bool = false,
        packet_buffer_size: i64 = 500,
        packet_buffer_size_video: i64 = 500,
        packet_buffer_size_audio: i64 = 200,
        pli_throttle: PliThrottleConfig = PliThrottleConfig::default(),
        congestion_control: CongestionControlConfig = CongestionControlConfig::default(),
        allow_tcp_fallback: Option<bool> = None,
        tcp_fallback_rtt_threshold: i64 = 0,
        allow_udp_unstable_fallback: bool = false,
        reconnect_on_publication_error: Option<bool> = None,
        reconnect_on_subscription_error: Option<bool> = None,
        reconnect_on_data_channel_error: Option<bool> = None,
        data_channel_max_buffered_amount: u64 = 0,
        datachannel_slow_threshold: i64 = 0,
        datachannel_lossy_target_latency: GoDuration = GoDuration::ZERO,
        datachannel_data_track_target_latency: GoDuration = ms(100),
        forward_stats: ForwardStatsConfig = ForwardStatsConfig::default(),
        enable_rtp_stream_restart_detection: bool = false,
    }
}

yaml_struct! { InterfacesConfig => "rtcconfig.InterfacesConfig" {
    includes: Vec<String> = Vec::new(),
    excludes: Vec<String> = Vec::new(),
}}

yaml_struct! { IpsConfig => "rtcconfig.IPsConfig" {
    includes: Vec<String> = Vec::new(),
    excludes: Vec<String> = Vec::new(),
}}

yaml_struct! { BatchIoConfig => "rtcconfig.BatchIOConfig" {
    batch_size: i64 = 0,
    max_flush_interval: GoDuration = GoDuration::ZERO,
}}

yaml_struct! { TurnServer => "config.TURNServer" {
    host: String = String::new(),
    port: i64 = 0,
    protocol: String = String::new(),
    username: String = String::new(),
    credential: String = String::new(),
    secret: String = String::new(),
    secret_file: String = String::new(),
    ttl: i64 = 0,
}}

yaml_struct! { PliThrottleConfig => "sfu.PLIThrottleConfig" {
    low_quality: GoDuration = ms(500),
    mid_quality: GoDuration = secs(1),
    high_quality: GoDuration = secs(1),
}}

yaml_struct! { ForwardStatsConfig => "config.ForwardStatsConfig" {
    summary_interval: GoDuration = GoDuration::ZERO,
    report_interval: GoDuration = GoDuration::ZERO,
    report_window: GoDuration = GoDuration::ZERO,
}}

yaml_struct! { CongestionControlConfig => "config.CongestionControlConfig" {
    enabled: bool = true,
    allow_pause: bool = false,
    stream_allocator: StreamAllocatorConfig = StreamAllocatorConfig::default(),
    remote_bwe: RemoteBweConfig = RemoteBweConfig::default(),
    use_send_side_bwe_interceptor: bool = false,
    use_send_side_bwe: bool = false,
    send_side_bwe_pacer: String = "no-queue".to_string(),
    send_side_bwe: SendSideBweConfig = SendSideBweConfig::default(),
}}

yaml_struct! { StreamAllocatorConfig => "streamallocator.StreamAllocatorConfig" {
    min_channel_capacity: i64 as "int64" = 0,
    /// The typo is the real upstream key.
    disable_etimation_unmanaged_tracks: bool = false,
    probe_mode: String as "streamallocator.ProbeMode" = "padding".to_string(),
    probe_overage_pct: i64 as "int64" = 120,
    probe_min_bps: i64 as "int64" = 200_000,
    paused_min_wait: GoDuration = secs(5),
}}

yaml_struct! { RemoteBweConfig => "remotebwe.RemoteBWEConfig" {
    nack_ratio_attenuator: f64 = 0.4,
    expected_usage_threshold: f64 = 0.95,
    channel_observer_probe: ChannelObserverConfig = ChannelObserverConfig {
        estimate: TrendDetectorConfig::new(3, 3, 0.0, secs(5), GoDuration::ZERO, secs(10)),
        nack: NackTrackerConfig { window_min_duration: ms(500), window_max_duration: secs(1), ratio_threshold: 0.04 },
    },
    channel_observer_non_probe: ChannelObserverConfig = ChannelObserverConfig {
        estimate: TrendDetectorConfig::new(12, 8, -0.6, secs(5), ms(500), secs(10)),
        nack: NackTrackerConfig { window_min_duration: secs(2), window_max_duration: secs(3), ratio_threshold: 0.08 },
    },
    probe_controller: ProbeControllerConfig = ProbeControllerConfig::default(),
}}

yaml_struct! { ChannelObserverConfig => "remotebwe.ChannelObserverConfig" {
    estimate: TrendDetectorConfig = TrendDetectorConfig::default(),
    nack: NackTrackerConfig = NackTrackerConfig::default(),
}}

yaml_struct! { TrendDetectorConfig => "ccutils.TrendDetectorConfig" {
    required_samples: i64 = 0,
    required_samples_min: i64 = 0,
    downward_trend_threshold: f64 = 0.0,
    downward_trend_max_wait: GoDuration = GoDuration::ZERO,
    collapse_threshold: GoDuration = GoDuration::ZERO,
    validity_window: GoDuration = GoDuration::ZERO,
}}

impl TrendDetectorConfig {
    const fn new(
        required_samples: i64,
        required_samples_min: i64,
        downward_trend_threshold: f64,
        downward_trend_max_wait: GoDuration,
        collapse_threshold: GoDuration,
        validity_window: GoDuration,
    ) -> Self {
        TrendDetectorConfig {
            required_samples,
            required_samples_min,
            downward_trend_threshold,
            downward_trend_max_wait,
            collapse_threshold,
            validity_window,
        }
    }
}

yaml_struct! { NackTrackerConfig => "remotebwe.NackTrackerConfig" {
    window_min_duration: GoDuration = GoDuration::ZERO,
    window_max_duration: GoDuration = GoDuration::ZERO,
    ratio_threshold: f64 = 0.0,
}}

yaml_struct! { ProbeControllerConfig => "remotebwe.ProbeControllerConfig" {
    probe_regulator: ProbeRegulatorConfig = ProbeRegulatorConfig::default(),
    settle_wait_num_rtt: u32 = 5,
    settle_wait_min: GoDuration = ms(250),
    settle_wait_max: GoDuration = secs(5),
}}

yaml_struct! { ProbeRegulatorConfig => "ccutils.ProbeRegulatorConfig" {
    base_interval: GoDuration = secs(3),
    backoff_factor: f64 = 1.5,
    max_interval: GoDuration = secs(120),
    min_duration: GoDuration = ms(200),
    max_duration: GoDuration = secs(20),
    duration_increase_factor: f64 = 1.5,
}}

yaml_struct! { SendSideBweConfig => "sendsidebwe.SendSideBWEConfig" {
    congestion_detector: CongestionDetectorConfig = CongestionDetectorConfig::default(),
}}

const fn signal(groups: i64, dur_ms: i64) -> CongestionSignalConfig {
    CongestionSignalConfig {
        min_number_of_groups: groups,
        min_duration: ms(dur_ms),
    }
}

const fn packet_group(min_packets: i64, max_window: GoDuration) -> PacketGroupConfig {
    PacketGroupConfig {
        min_packets,
        max_window_duration: max_window,
    }
}

yaml_struct! { CongestionDetectorConfig => "sendsidebwe.CongestionDetectorConfig" {
    packet_group: PacketGroupConfig = packet_group(30, ms(500)),
    packet_group_max_age: GoDuration = secs(10),
    probe_packet_group: ProbePacketGroupConfig = ProbePacketGroupConfig::default(),
    probe_regulator: ProbeRegulatorConfig = ProbeRegulatorConfig::default(),
    probe_signal: ProbeSignalConfig = ProbeSignalConfig::default(),
    jqr_min_delay: GoDuration = ms(50),
    jqr_min_trend_coefficient: f64 = 0.8,
    dqr_max_delay: GoDuration = ms(20),
    weighted_loss: WeightedLossConfig = WeightedLossConfig::default(),
    jqr_min_weighted_loss: f64 = 0.25,
    dqr_max_weighted_loss: f64 = 0.1,
    queuing_delay_early_warning_jqr: CongestionSignalConfig = signal(2, 200),
    queuing_delay_early_warning_dqr: CongestionSignalConfig = signal(3, 300),
    loss_early_warning_jqr: CongestionSignalConfig = signal(3, 300),
    loss_early_warning_dqr: CongestionSignalConfig = signal(4, 400),
    queuing_delay_congested_jqr: CongestionSignalConfig = signal(4, 400),
    queuing_delay_congested_dqr: CongestionSignalConfig = signal(5, 500),
    loss_congested_jqr: CongestionSignalConfig = signal(6, 600),
    loss_congested_dqr: CongestionSignalConfig = signal(6, 600),
    congested_ctr_trend: TrendDetectorConfig = TrendDetectorConfig::new(4, 2, -0.5, secs(2), ms(500), secs(10)),
    congested_ctr_epsilon: f64 = 0.05,
    congested_packet_group: PacketGroupConfig = packet_group(20, ms(150)),
    /// The typo is the real upstream key.
    estimaton_window_duration: GoDuration = secs(1),
}}

yaml_struct! { PacketGroupConfig => "sendsidebwe.PacketGroupConfig" {
    min_packets: i64 = 0,
    max_window_duration: GoDuration = GoDuration::ZERO,
}}

yaml_struct! { ProbePacketGroupConfig => "sendsidebwe.ProbePacketGroupConfig" {
    packet_group: PacketGroupConfig = packet_group(16384, secs(60)),
    settle_wait_num_rtt: u32 = 5,
    settle_wait_min: GoDuration = ms(250),
    settle_wait_max: GoDuration = secs(5),
}}

yaml_struct! { ProbeSignalConfig => "sendsidebwe.ProbeSignalConfig" {
    min_bytes_ratio: f64 = 0.5,
    min_duration_ratio: f64 = 0.5,
    jqr_min_delay: GoDuration = ms(50),
    dqr_max_delay: GoDuration = ms(20),
    weighted_loss: WeightedLossConfig = WeightedLossConfig::default(),
    jqr_min_weighted_loss: f64 = 0.25,
    dqr_max_weighted_loss: f64 = 0.1,
}}

yaml_struct! { WeightedLossConfig => "sendsidebwe.WeightedLossConfig" {
    min_duration_for_loss_validity: GoDuration = ms(100),
    base_duration: GoDuration = ms(500),
    base_pps: i64 = 30,
    loss_penalty_factor: f64 = 0.25,
}}

yaml_struct! { CongestionSignalConfig => "sendsidebwe.CongestionSignalConfig" {
    min_number_of_groups: i64 = 0,
    min_duration: GoDuration = GoDuration::ZERO,
}}

// ---------------------------------------------------------------------------
// redis.*
// ---------------------------------------------------------------------------

yaml_struct! { RedisConfig => "redis.RedisConfig" {
    address: String = String::new(),
    username: String = String::new(),
    password: String = String::new(),
    db: i64 = 0,
    use_tls: bool = false,
    tls: Option<TlsConfig> = None,
    sentinel_master_name: String = String::new(),
    sentinel_username: String = String::new(),
    sentinel_password: String = String::new(),
    sentinel_addresses: Vec<String> = Vec::new(),
    cluster_addresses: Vec<String> = Vec::new(),
    dial_timeout: i64 = 0,
    read_timeout: i64 = 0,
    write_timeout: i64 = 0,
    max_redirects: Option<i64> = None,
    pool_timeout: GoDuration = GoDuration::ZERO,
    pool_size: i64 = 0,
}}

yaml_struct! { TlsConfig => "xtls.Config" {
    enabled: bool = false,
    insecure: bool = false,
    server_name: String = String::new(),
    ca_cert_file: String = String::new(),
    client_cert_file: String = String::new(),
    client_key_file: String = String::new(),
}}

go_pointer!(TlsConfig);

/// Pointer-to-struct fields get no generated flag (impl-notes §3.2).
impl Walk for Option<TlsConfig> {
    fn walk(&mut self, _path: &str, _f: &mut dyn FnMut(&str, &mut dyn Leaf)) {}
}

// ---------------------------------------------------------------------------
// audio / video / room
// ---------------------------------------------------------------------------

yaml_struct! {
    /// `sfu.AudioConfig` with inline `audio.AudioLevelConfig`.
    AudioConfig => "sfu.AudioConfig", inline_until "active_red_encoding" {
        active_level: u8 = 35,
        min_percentile: u8 = 40,
        /// Milliseconds.
        update_interval: u32 = 400,
        smooth_intervals: u32 = 2,
        active_red_encoding: bool = false,
        enable_loss_proxying: bool = false,
    }
}

yaml_struct! { VideoConfig => "config.VideoConfig" {
    dynacast_pause_delay: GoDuration = secs(5),
    stream_tracker_manager: StreamTrackerManagerConfig = StreamTrackerManagerConfig::default(),
    codec_regression_threshold: i64 = 5,
}}

fn layers<V: Clone>(v: V) -> BTreeMap<i32, V> {
    (0..3).map(|i| (i, v.clone())).collect()
}

fn packet_tracker(samples: u32, cycles: u32, cycle: GoDuration) -> StreamTrackerPacketConfig {
    StreamTrackerPacketConfig {
        samples_required: samples,
        cycles_required: cycles,
        cycle_duration: cycle,
    }
}

yaml_struct! { StreamTrackerManagerConfig => "sfu.StreamTrackerManagerConfig" {
    video: StreamTrackerConfig = StreamTrackerConfig {
        stream_tracker_type: "packet".to_string(),
        bitrate_report_interval: layers(secs(1)),
        packet_tracker: BTreeMap::from([
            (0, packet_tracker(1, 4, ms(500))),
            (1, packet_tracker(5, 20, ms(500))),
            (2, packet_tracker(5, 20, ms(500))),
        ]),
        frame_tracker: layers(StreamTrackerFrameConfig { min_fps: 5.0 }),
    },
    screenshare: StreamTrackerConfig = StreamTrackerConfig {
        stream_tracker_type: "packet".to_string(),
        bitrate_report_interval: layers(secs(4)),
        packet_tracker: layers(packet_tracker(1, 1, secs(2))),
        frame_tracker: layers(StreamTrackerFrameConfig { min_fps: 0.5 }),
    },
}}

yaml_struct! { StreamTrackerConfig => "sfu.StreamTrackerConfig" {
    stream_tracker_type: String as "sfu.StreamTrackerType" = String::new(),
    bitrate_report_interval: BTreeMap<i32, GoDuration> = BTreeMap::new(),
    packet_tracker: BTreeMap<i32, StreamTrackerPacketConfig> = BTreeMap::new(),
    frame_tracker: BTreeMap<i32, StreamTrackerFrameConfig> = BTreeMap::new(),
}}

yaml_struct! { StreamTrackerPacketConfig => "streamtracker.StreamTrackerPacketConfig" {
    samples_required: u32 = 0,
    cycles_required: u32 = 0,
    cycle_duration: GoDuration = GoDuration::ZERO,
}}

yaml_struct! { StreamTrackerFrameConfig => "streamtracker.StreamTrackerFrameConfig" {
    min_fps: f64 = 0.0,
}}

/// `room.enabled_codecs` default (pion mime strings, `config.go:603-614`).
pub const DEFAULT_ENABLED_CODECS: [&str; 10] = [
    "audio/PCMU",
    "audio/PCMA",
    "audio/opus",
    "audio/red",
    "video/VP8",
    "video/H264",
    "video/VP9",
    "video/AV1",
    "video/H265",
    "video/rtx",
];

yaml_struct! { RoomConfig => "config.RoomConfig" {
    auto_create: bool = true,
    enabled_codecs: Vec<CodecSpec> = DEFAULT_ENABLED_CODECS
        .iter()
        .map(|m| CodecSpec { mime: (*m).to_string(), fmtp_line: String::new() })
        .collect(),
    max_participants: u32 = 0,
    /// Seconds.
    empty_timeout: u32 = 300,
    /// Seconds.
    departure_timeout: u32 = 20,
    enable_remote_unmute: bool = false,
    playout_delay: PlayoutDelayConfig = PlayoutDelayConfig::default(),
    sync_streams: bool = false,
    create_room_timeout: GoDuration = secs(10),
    create_room_attempts: i64 = 3,
    enable_full_room_in_webhooks: bool = false,
    update_batch_target_size: i64 = 128 * 1024,
    /// Legacy, overrides `limit.max_metadata_size` when non-zero.
    max_metadata_size: u32 = 0,
    max_room_name_length: i64 = 0,
    max_participant_identity_length: i64 = 0,
    /// `map[string]*livekit.RoomConfiguration`: `None` is a nil entry (`name: ~`).
    room_configurations: BTreeMap<String, Option<RoomConfiguration>> = BTreeMap::new(),
}}

yaml_struct! { CodecSpec => "config.CodecSpec" {
    mime: String = String::new(),
    fmtp_line: String = String::new(),
}}

yaml_struct! { PlayoutDelayConfig => "config.PlayoutDelayConfig" {
    enabled: bool = false,
    min: i64 = 0,
    max: i64 = 0,
}}

yaml_struct! { RoomAgentDispatch => "livekit.RoomAgentDispatch" {
    agent_name: String = String::new(),
    metadata: String = String::new(),
    restart_policy: i32 = 0,
    deployment: String = String::new(),
    attributes: BTreeMap<String, String> = BTreeMap::new(),
}}

/// The protocol YAML decoder appends generated message values: absent and empty
/// repeated lists retain nil, while null elements allocate default messages.
/// The optional DTO wrapper retains the generated nil distinction for projection.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RoomAgentDispatches(pub Option<Vec<Option<RoomAgentDispatch>>>);

impl GoType for RoomAgentDispatches {
    fn go_type() -> String {
        "[]*livekit.RoomAgentDispatch".into()
    }
}

/// Decoded from the validated protocol projection: `null` = nil list, a
/// `null` element = nil message (the projection already turned source `null`
/// elements into empty messages).
impl Decode for RoomAgentDispatches {
    fn decode(&mut self, d: &mut Dec<'_>, n: Id, go: &str) -> Result<bool, Fail> {
        let mut list: NilSlice<ProtoMessage<RoomAgentDispatch>> = NilSlice(
            self.0
                .take()
                .map(|v| v.into_iter().map(ProtoMessage).collect()),
        );
        let good = list.decode(d, n, go)?;
        self.0 = list.0.map(|v| v.into_iter().map(|m| m.0).collect());
        Ok(good)
    }
}

/// A `*Message` element of a validated protocol list (`null` stays nil).
#[derive(Clone, Debug, Default, PartialEq)]
struct ProtoMessage<T>(Option<T>);

impl<T: Decode + Default + GoType> Decode for ProtoMessage<T> {
    fn decode(&mut self, d: &mut Dec<'_>, n: Id, go: &str) -> Result<bool, Fail> {
        decode_pointer(&mut self.0, d, n, go)
    }
}

impl Walk for RoomAgentDispatches {
    fn walk(&mut self, _path: &str, _f: &mut dyn FnMut(&str, &mut dyn Leaf)) {}
}

yaml_struct! { RecordingProxy => "livekit.ProxyConfig" {url:String=String::new(),username:String=String::new(),password:String=String::new(),}}
yaml_struct! { RecordingS3 => "livekit.S3Upload" {
 access_key:String=String::new(),secret:String=String::new(),region:String=String::new(),endpoint:String=String::new(),bucket:String=String::new(),force_path_style:bool=false,metadata:BTreeMap<String,String> =BTreeMap::new(),tagging:String=String::new(),content_disposition:String=String::new(),proxy:Option<RecordingProxy> =None,session_token:String=String::new(),assume_role_arn:String=String::new(),assume_role_external_id:String=String::new(),
}}
yaml_struct! { RecordingGcp => "livekit.GCPUpload" {credentials:String=String::new(),bucket:String=String::new(),proxy:Option<RecordingProxy> =None,}}
yaml_struct! { RecordingAzure => "livekit.AzureBlobUpload" {account_name:String=String::new(),account_key:String=String::new(),container_name:String=String::new(),}}
yaml_struct! { RecordingAli => "livekit.AliOSSUpload" {access_key:String=String::new(),secret:String=String::new(),region:String=String::new(),endpoint:String=String::new(),bucket:String=String::new(),}}
yaml_struct! { RecordingAdvanced => "livekit.EncodingOptions" {width:i32=0,height:i32=0,depth:i32=0,framerate:i32=0,audio_codec:i32=0,audio_bitrate:i32=0,audio_frequency:i32=0,video_codec:i32=0,video_bitrate:i32=0,key_frame_interval:f64=0.0,audio_quality:i32=0,video_quality:i32=0,}}
// Fixed RoomConfiguration.UnmarshalYAML -> protoyaml uses descriptor fields,
// including snake_case / flattened oneof names. Actual Go 24-row oracle corrected
// the old lowercase generated-struct inference. Absent/null/empty are distinct.
yaml_struct! { RecordingFile => "livekit.EncodedFileOutput" {
 file_type:i32=0, filepath:String=String::new(), disable_manifest:bool=false, s3:Option<RecordingS3> = None,gcp:Option<RecordingGcp> = None,azure:Option<RecordingAzure> = None,#[allow(non_snake_case)] aliOSS:Option<RecordingAli> = None,
}}
yaml_struct! { RecordingSegments => "livekit.SegmentedFileOutput" {
 protocol:i32=0, filename_prefix:String=String::new(), playlist_name:String=String::new(),
 live_playlist_name:String=String::new(), segment_duration:u32=0, filename_suffix:i32=0,
 disable_manifest:bool=false, s3:Option<RecordingS3> = None,gcp:Option<RecordingGcp> = None,azure:Option<RecordingAzure> = None,#[allow(non_snake_case)] aliOSS:Option<RecordingAli> = None,
}}
yaml_struct! { RecordingStream => "livekit.StreamOutput" {protocol:i32=0, urls:Vec<String> =Vec::new(),}}
yaml_struct! { RecordingImage => "livekit.ImageOutput" {
 capture_interval:u32=0,width:i32=0,height:i32=0,filename_prefix:String=String::new(),
 filename_suffix:i32=0,image_codec:i32=0,disable_manifest:bool=false,s3:Option<RecordingS3> = None,gcp:Option<RecordingGcp> = None,azure:Option<RecordingAzure> = None,#[allow(non_snake_case)] aliOSS:Option<RecordingAli> = None,
}}
yaml_struct! { RecordingFilter => "livekit.FilterParams" {include_events:Vec<String> =Vec::new(),exclude_events:Vec<String> =Vec::new(),}}
yaml_struct! { RecordingWebhook => "livekit.WebhookConfig" {url:String=String::new(),signing_key:String=String::new(),filter_params:Option<RecordingFilter> =None,}}
yaml_struct! { RecordingComposite => "livekit.RoomCompositeEgressRequest" {
 room_name:String=String::new(),layout:String=String::new(),audio_only:bool=false,audio_mixing:i32=0,video_only:bool=false,
 custom_base_url:String=String::new(),file:Option<RecordingFile> = None,stream:Option<RecordingStream> = None,segments:Option<RecordingSegments> = None,preset:Option<i32> = None,advanced:Option<RecordingAdvanced> = None,
 file_outputs:Vec<Option<RecordingFile>> =Vec::new(),stream_outputs:Vec<Option<RecordingStream>> =Vec::new(),
 segment_outputs:Vec<Option<RecordingSegments>> =Vec::new(),image_outputs:Vec<Option<RecordingImage>> =Vec::new(),
 webhooks:Vec<Option<RecordingWebhook>> =Vec::new(),
}}
yaml_struct! { RecordingParticipant => "livekit.AutoParticipantEgress" {
 preset:Option<i32> = None,advanced:Option<RecordingAdvanced> = None,file_outputs:Vec<Option<RecordingFile>> =Vec::new(),segment_outputs:Vec<Option<RecordingSegments>> =Vec::new(),
}}
yaml_struct! { RecordingTrack => "livekit.AutoTrackEgress" {filepath:String=String::new(),disable_manifest:bool=false,s3:Option<RecordingS3> = None,gcp:Option<RecordingGcp> = None,azure:Option<RecordingAzure> = None,#[allow(non_snake_case)] aliOSS:Option<RecordingAli> = None,}}
yaml_struct! { RecordingRoomEgress => "livekit.RoomEgress" {
 room:Option<RecordingComposite> =None,participant:Option<RecordingParticipant> =None,tracks:Option<RecordingTrack> =None,
}}
// protoyaml message null calls Mutable(field) before returning: presence true.
// A message value is decoded fresh (the projection is complete and validated).
fn decode_recording<T: Decode + Default>(
    this: &mut Option<T>,
    d: &mut Dec<'_>,
    n: Id,
    go: &str,
) -> Result<bool, Fail> {
    let mut value = T::default();
    let good = if d.kind(n) == Kind::Scalar && d.is_null(n) {
        true
    } else {
        value.decode(d, n, go.strip_prefix('*').unwrap_or(go))?
    };
    *this = Some(value);
    Ok(good)
}
macro_rules! recording_pointer {($($t:ty),* $(,)?)=>{$(
 impl Decode for Option<$t>{fn decode(&mut self,d:&mut Dec<'_>,n:Id,go:&str)->Result<bool,Fail>{decode_recording(self,d,n,go)}}
 impl Walk for Option<$t>{fn walk(&mut self,_:&str,_:&mut dyn FnMut(&str,&mut dyn Leaf)) {}}
)*};}
recording_pointer!(
    RecordingFilter,
    RecordingComposite,
    RecordingParticipant,
    RecordingTrack,
    RecordingRoomEgress,
    RecordingFile,
    RecordingStream,
    RecordingSegments,
    RecordingAdvanced,
    RecordingS3,
    RecordingGcp,
    RecordingAzure,
    RecordingAli,
    RecordingProxy
);
go_pointer!(i32);
// List elements of the validated projection (source `null` elements are
// already default messages there).
go_pointer!(RecordingImage, RecordingWebhook);
impl Walk for Option<i32> {
    fn walk(&mut self, _: &str, _: &mut dyn FnMut(&str, &mut dyn Leaf)) {}
}
impl RecordingRoomEgress {
    pub(crate) fn unsupported_oneof(&self) -> bool {
        let storage = |s: bool, g: bool, a: bool, l: bool| {
            [s, g, a, l].into_iter().filter(|x| *x).count() > 1
        };
        let file = |v: &RecordingFile| {
            storage(
                v.s3.is_some(),
                v.gcp.is_some(),
                v.azure.is_some(),
                v.aliOSS.is_some(),
            )
        };
        let segments = |v: &RecordingSegments| {
            storage(
                v.s3.is_some(),
                v.gcp.is_some(),
                v.azure.is_some(),
                v.aliOSS.is_some(),
            )
        };
        self.tracks.as_ref().is_some_and(|v| {
            storage(
                v.s3.is_some(),
                v.gcp.is_some(),
                v.azure.is_some(),
                v.aliOSS.is_some(),
            )
        }) || self.participant.as_ref().is_some_and(|v| {
            (v.preset.is_some() && v.advanced.is_some())
                || v.file_outputs.iter().flatten().any(file)
                || v.segment_outputs.iter().flatten().any(segments)
        }) || self.room.as_ref().is_some_and(|v| {
            [v.file.is_some(), v.stream.is_some(), v.segments.is_some()]
                .into_iter()
                .filter(|x| *x)
                .count()
                > 1
                || (v.preset.is_some() && v.advanced.is_some())
                || v.file.as_ref().is_some_and(file)
                || v.segments.as_ref().is_some_and(segments)
                || v.file_outputs.iter().flatten().any(file)
                || v.segment_outputs.iter().flatten().any(segments)
                || v.image_outputs.iter().flatten().any(|v| {
                    storage(
                        v.s3.is_some(),
                        v.gcp.is_some(),
                        v.azure.is_some(),
                        v.aliOSS.is_some(),
                    )
                })
        })
    }
}

yaml_struct! {
    /// `RoomConfiguration.UnmarshalYAML` delegates to protoyaml, whose
    /// canonical descriptor names are snake_case (not Go struct lowercase).
    RoomConfiguration => "livekit.RoomConfiguration" {
        name: String = String::new(),
        empty_timeout: u32 = 0,
        departure_timeout: u32 = 0,
        max_participants: u32 = 0,
        metadata: String = String::new(),
        egress: Option<RecordingRoomEgress> = None,
        min_playout_delay: u32 = 0,
        max_playout_delay: u32 = 0,
        sync_streams: bool = false,
        agents: RoomAgentDispatches = RoomAgentDispatches::default(),
        tags: BTreeMap<String,String> = BTreeMap::new(),
    }
}

// ---------------------------------------------------------------------------
// turn / ingress / sip / webhook / node_selector
// ---------------------------------------------------------------------------

yaml_struct! { TurnConfig => "config.TURNConfig" {
    enabled: bool = false,
    domain: String = String::new(),
    cert_file: String = String::new(),
    key_file: String = String::new(),
    tls_port: i64 = 0,
    udp_port: i64 = 0,
    relay_range_start: u16 = 0,
    relay_range_end: u16 = 0,
    external_tls: bool = false,
    bind_addresses: Vec<String> = vec!["0.0.0.0".to_string()],
    proxy_protocol: bool = false,
    proxy_protocol_trusted_cidrs: Vec<String> = vec!["127.0.0.0/8".to_string(), "::1/128".to_string()],
    per_user_relay_allocation_limit: i64 = 12,
    ttl_seconds: i64 = 300,
    allow_restricted_peer_cidrs: Vec<String> = Vec::new(),
    deny_peer_cidrs: Vec<String> = Vec::new(),
}}

yaml_struct! { IngressConfig => "config.IngressConfig" {
    rtmp_base_url: String = String::new(),
    whip_base_url: String = String::new(),
    enable_udp_url_pull: bool = false,
}}

yaml_struct! {
    /// `config.SIPConfig` is an empty struct: `sip: {}` is fine, any child key
    /// fails strict parsing.
    SipConfig => "config.SIPConfig" {}
}

yaml_struct! { WebHookConfig => "webhook.WebHookConfig" {
    urls: Vec<String> = Vec::new(),
    api_key: String = String::new(),
    url_notifier: UrlNotifierConfig = UrlNotifierConfig::default(),
    resource_url_notifier: ResourceUrlNotifierConfig = ResourceUrlNotifierConfig::default(),
    filter_params: FilterParams = FilterParams::default(),
}}

yaml_struct! { UrlNotifierConfig => "webhook.URLNotifierConfig" {
    num_workers: i64 = 10,
    queue_size: i64 = 100,
}}

yaml_struct! { ResourceUrlNotifierConfig => "webhook.ResourceURLNotifierConfig" {
    max_age: GoDuration = secs(30),
    max_depth: i64 = 200,
}}

yaml_struct! { FilterParams => "webhook.FilterParams" {
    include_events: Vec<String> = Vec::new(),
    exclude_events: Vec<String> = Vec::new(),
}}

yaml_struct! { NodeSelectorConfig => "config.NodeSelectorConfig" {
    kind: String = "any".to_string(),
    sort_by: String = "random".to_string(),
    algorithm: String = "lowest".to_string(),
    cpu_load_limit: f32 = 0.9,
    sysload_limit: f32 = 0.9,
    regions: Vec<RegionConfig> = Vec::new(),
}}

yaml_struct! { RegionConfig => "config.RegionConfig" {
    name: String = String::new(),
    lat: f64 = 0.0,
    lon: f64 = 0.0,
}}

// ---------------------------------------------------------------------------
// signal_relay / psrpc / logging / limit / agents / metric / trace / node_stats / api
// ---------------------------------------------------------------------------

yaml_struct! { SignalRelayConfig => "config.SignalRelayConfig" {
    retry_timeout: GoDuration = ms(7500),
    min_retry_interval: GoDuration = ms(500),
    max_retry_interval: GoDuration = secs(4),
    stream_buffer_size: i64 = 1000,
    connect_attempts: i64 = 3,
}}

yaml_struct! { PsrpcConfig => "rpc.PSRPCConfig" {
    max_attempts: i64 = 3,
    timeout: GoDuration = secs(3),
    backoff: GoDuration = secs(2),
    buffer_size: i64 = 1000,
    compression: CompressionConfig = CompressionConfig::default(),
}}

yaml_struct! { CompressionConfig => "rpc.CompressionConfig" {
    quality: i64 = 0,
    threshold: i64 = 1024,
    max_decompressed_size: i64 = 0,
}}

yaml_struct! {
    /// `config.LoggingConfig` with inline `logger.Config`.
    LoggingConfig => "config.LoggingConfig", inline_until "pion_level" {
        json: bool = false,
        level: String = String::new(),
        sample: bool = false,
        component_levels: BTreeMap<String, String> = BTreeMap::new(),
        sample_initial: i64 = 0,
        sample_interval: i64 = 0,
        item_sample_seconds: i64 = 0,
        item_sample_initial: i64 = 0,
        item_sample_interval: i64 = 0,
        pion_level: String = "error".to_string(),
    }
}

yaml_struct! { LimitConfig => "config.LimitConfig" {
    num_tracks: i32 = 0,
    bytes_per_sec: f32 = 0.0,
    subscription_limit_video: i32 = 0,
    subscription_limit_audio: i32 = 0,
    max_metadata_size: u32 = 512 * 1024,
    max_attributes_size: u32 = 64 * 1024,
    max_room_name_length: i64 = 256,
    max_participant_identity_length: i64 = 256,
    max_participant_name_length: i64 = 256,
    max_data_blob_key_length: i64 = 256,
    max_data_blobs_size: u32 = 64_000,
    max_data_track_custom_encoding_length: i64 = 32,
    signal_message_size_limit: i64 as "int64" = 2 << 20,
    agent_signal_message_size_limit: i64 as "int64" = 2 << 20,
    max_api_request_body_size: i64 as "int64" = 10 << 20,
}}

yaml_struct! { AgentsConfig => "agent.Config" {
    enable_user_data_recording: bool = false,
    enable_user_data_redaction: bool = false,
    target_load: f32 = 0.7,
    endpoints: EndpointsConfig = EndpointsConfig::default(),
}}

yaml_struct! { EndpointsConfig => "agent.EndpointsConfig" {
    disabled: bool = false,
    max_streams: u32 = 0,
    max_tunnels: u32 = 0,
    tunnel_drain_timeout: GoDuration = GoDuration::ZERO,
}}

yaml_struct! { MetricConfig => "metric.MetricConfig" {
    timestamper_config: MetricTimestamperConfig = MetricTimestamperConfig::default(),
    collector: MetricsCollectorConfig = MetricsCollectorConfig::default(),
    reporter: MetricsReporterConfig = MetricsReporterConfig::default(),
}}

yaml_struct! { MetricTimestamperConfig => "metric.MetricTimestamperConfig" {
    one_way_delay_estimator_min_interval: GoDuration = secs(5),
    one_way_delay_estimator_max_batch: i64 = 100,
}}

yaml_struct! { MetricsCollectorConfig => "metric.MetricsCollectorConfig" {
    sampling_interval_ms: u32 = 3000,
    batch_interval_ms: u32 = 10_000,
}}

yaml_struct! { MetricsReporterConfig => "metric.MetricsReporterConfig" {
    reporting_interval_ms: u32 = 10_000,
}}

yaml_struct! { TracingConfig => "config.TracingConfig" {
    jaeger_url: String = String::new(),
}}

yaml_struct! { NodeStatsConfig => "config.NodeStatsConfig" {
    stats_update_interval: GoDuration = secs(2),
    stats_rate_measurement_intervals: Vec<GoDuration> = vec![secs(10)],
    stats_max_delay: GoDuration = secs(30),
}}

yaml_struct! { ApiConfig => "config.APIConfig" {
    execution_timeout: GoDuration = secs(2),
    check_interval: GoDuration = ms(100),
    max_check_interval: GoDuration = secs(300),
    enable_psrpc_for_get_list_participants: bool = false,
}}

// ---------------------------------------------------------------------------
// livecloud: extension tree (spec §6.3, CLI-C06)
// ---------------------------------------------------------------------------

/// Raw `livecloud:` section. Always strict (unknown keys are errors even with
/// `--disable-strict-config`), validated in `load.rs`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LivecloudSection {
    #[serde(default)]
    pub schema_version: Option<u32>,
    #[serde(default)]
    pub deployment: Option<String>,
    #[serde(default)]
    pub https: Option<HttpsSection>,
    #[serde(default)]
    pub ingress_worker_protocol: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpsSection {
    pub port: Option<u16>,
    pub cert_file: Option<String>,
    pub key_file: Option<String>,
    pub bind_addresses: Option<Vec<String>>,
    pub http_enabled: Option<bool>,
    pub max_connections: Option<usize>,
    pub handshake_timeout_ms: Option<u64>,
}

/// `RoomConfiguration.UnmarshalYAML` (protocol `types.go`): the node is
/// re-marshalled and decoded by protoyaml. `null` keeps a nil entry, as
/// `d.prepare` never calls the unmarshaler for a null node.
impl Decode for Option<RoomConfiguration> {
    fn decode(&mut self, d: &mut Dec<'_>, n: Id, go: &str) -> Result<bool, Fail> {
        if d.is_null(n) {
            return match d.kind(n) {
                Kind::Scalar => {
                    *self = None;
                    Ok(true)
                }
                _ => Ok(d.not_a_collection(n, go)),
            };
        }
        let value = crate::protoyaml::decode_room_configuration(d.doc, n).map_err(Fail)?;
        *self = Some(value);
        Ok(true)
    }
}

/// Root document: the official tree plus the split-off extension key. The
/// extension is taken out *before* the official strict check sees it, so strict
/// validation of every official key stays on (CLI-C06).
struct ConfigFile<'a> {
    official: &'a mut Config,
    livecloud: Option<LivecloudSection>,
}

impl StructFields for ConfigFile<'_> {
    fn field_index(key: &str) -> Option<usize> {
        if key == "livecloud" {
            return Some(Config::FIELDS.len());
        }
        Config::field_index(key)
    }
    fn decode_field(&mut self, d: &mut Dec<'_>, index: usize, value: Id) -> Result<(), Fail> {
        if index == Config::FIELDS.len() {
            let de = crate::yamlv3::serde_bridge::NodeDeserializer {
                doc: d.doc,
                id: value,
            };
            self.livecloud = Option::<LivecloudSection>::deserialize(de)
                .map_err(|e| Fail(format!("livecloud: {e}")))?;
            return Ok(());
        }
        self.official.decode_field(d, index, value)
    }
}

impl Decode for ConfigFile<'_> {
    fn decode(&mut self, d: &mut Dec<'_>, n: Id, go: &str) -> Result<bool, Fail> {
        decode_struct(self, d, n, go)
    }
}

/// Decodes the input bytes onto `into` exactly as upstream `NewConfig` does
/// (`yaml.NewDecoder(strings.NewReader(text))`, `KnownFields(strict)`,
/// one `Decode`). The YAML reader detects the encoding from the BOM.
/// Errors are the message *after* the upstream
/// `could not parse config: ` prefix (the caller adds it).
pub(crate) fn decode_yaml(
    input: impl AsRef<[u8]>,
    strict: bool,
    into: &mut Config,
) -> Result<Option<LivecloudSection>, String> {
    let mut file = ConfigFile {
        official: into,
        livecloud: None,
    };
    crate::yamlv3::decoder_decode(input.as_ref(), strict, &mut file, "config.Config")?;
    Ok(file.livecloud)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(text: &str, strict: bool) -> Result<Config, String> {
        let mut c = Config::default();
        decode_yaml(text, strict, &mut c)?;
        Ok(c)
    }

    #[test]
    fn defaults_kept_for_absent_keys() {
        let c = decode("rtc:\n  tcp_port: 9000\n", true).unwrap();
        assert_eq!(c.rtc.tcp_port, 9000);
        assert_eq!(c.port, 7880);
        assert_eq!(c.rtc.packet_buffer_size_audio, 200);
        assert!(c.rtc.congestion_control.enabled);
        assert_eq!(c.room.enabled_codecs.len(), 10);
    }

    #[test]
    fn null_section_keeps_defaults() {
        let c = decode("turn:\nrtc:\n  tcp_port:\n", true).unwrap();
        assert_eq!(c.turn.ttl_seconds, 300);
        assert_eq!(c.rtc.tcp_port, 7881);
    }

    #[test]
    fn strict_unknown_key_is_error() {
        let err = decode("rtc:\n  bogus: 1\n", true).unwrap_err();
        assert!(
            err.contains("field bogus not found in type config.RTCConfig"),
            "{err}"
        );
        // non-strict ignores it
        let c = decode("rtc:\n  bogus: 1\n", false).unwrap();
        assert_eq!(c.rtc.tcp_port, 7881);
        // the sample file's wrong key
        assert!(decode("debug_handler_port: 1\n", true).is_err());
    }

    /// Texts recorded from the original `getConfig` (w1-config-observer
    /// go-results.json), minus the `could not parse config: ` prefix.
    #[test]
    fn yaml_v3_error_texts_match_original() {
        for (yaml, want) in [
            (
                "rtc:\n  tcp_port: abc\n",
                "yaml: unmarshal errors:\n  line 2: cannot unmarshal !!str `abc` into uint32",
            ),
            (
                "rtc:\n  tcp_port: -1\n",
                "yaml: unmarshal errors:\n  line 2: cannot unmarshal !!int `-1` into uint32",
            ),
            (
                "audio:\n  active_level: 300\n",
                "yaml: unmarshal errors:\n  line 2: cannot unmarshal !!int `300` into uint8",
            ),
            (
                "rtc:\n  packet_buffer_size: 99999999999999999999\n",
                "yaml: unmarshal errors:\n  line 2: cannot unmarshal !!float `9999999...` into int",
            ),
            (
                "rtc:\n  use_ice_lite: maybe\n",
                "yaml: unmarshal errors:\n  line 2: cannot unmarshal !!str `maybe` into bool",
            ),
            (
                "rtc:\n  use_ice_lite: 1\n",
                "yaml: unmarshal errors:\n  line 2: cannot unmarshal !!int `1` into bool",
            ),
            (
                "node_selector:\n  cpu_load_limit: abc\n",
                "yaml: unmarshal errors:\n  line 2: cannot unmarshal !!str `abc` into float32",
            ),
            (
                "rtc:\n  pli_throttle:\n    low_quality: 500\n",
                "yaml: unmarshal errors:\n  line 3: cannot unmarshal !!int `500` into time.Duration",
            ),
            (
                "rtc:\n  pli_throttle:\n    low_quality: 5x\n",
                "yaml: unmarshal errors:\n  line 3: cannot unmarshal !!str `5x` into time.Duration",
            ),
            (
                "port: \"\"\n",
                "yaml: unmarshal errors:\n  line 1: cannot unmarshal !!str `` into uint32",
            ),
            (
                "nope: 1\n",
                "yaml: unmarshal errors:\n  line 1: field nope not found in type config.Config",
            ),
            (
                "rtc:\n  turn_servers:\n  - host: h\n    nope: 1\n",
                "yaml: unmarshal errors:\n  line 4: field nope not found in type config.TURNServer",
            ),
            (
                "hello\n",
                "yaml: unmarshal errors:\n  line 1: cannot unmarshal !!str `hello` into config.Config",
            ),
            (
                "- a\n",
                "yaml: unmarshal errors:\n  line 1: cannot unmarshal !!seq into config.Config",
            ),
            (
                "rtc: 5\n",
                "yaml: unmarshal errors:\n  line 1: cannot unmarshal !!int `5` into config.RTCConfig",
            ),
            (
                "rtc:\n  stun_servers: abc\n",
                "yaml: unmarshal errors:\n  line 2: cannot unmarshal !!str `abc` into []string",
            ),
            (
                "rtc: [\n",
                "yaml: line 1: did not find expected node content",
            ),
            (
                "port: 1\nport: 2\n",
                "yaml: unmarshal errors:\n  line 2: mapping key \"port\" already defined at line 1",
            ),
            (
                "rtc:\n  udp_port: \"7890-7880\"\n",
                "end port 7880 must be greater than start port 7890",
            ),
            ("rtc:\n  node_ip: \"1.2.3\"\n", "invalid ip 1.2.3"),
        ] {
            assert_eq!(decode(yaml, true).unwrap_err(), want, "{yaml:?}");
        }
    }

    #[test]
    fn null_tagged_mapping_panics_only_for_inline_fields() {
        // Original: reflect panic for an embedded (`,inline`) field ...
        assert_eq!(
            decode("rtc: !!null {tcp_port: 1}\n", true).unwrap_err(),
            "panic: reflect: call of reflect.Value.Type on zero Value"
        );
        // ... while an own field of the same struct decodes normally.
        assert!(
            decode("rtc: !!null {strict_acks: true}\n", true)
                .unwrap()
                .rtc
                .strict_acks
        );
    }

    #[test]
    fn yaml_v3_conversions_match_original() {
        // float -> int truncates; first document wins; null preset is nil.
        assert_eq!(
            decode("rtc:\n  packet_buffer_size: 1.5\n", true)
                .unwrap()
                .rtc
                .packet_buffer_size,
            1
        );
        assert_eq!(decode("port: 1\n---\nport: 2\n", true).unwrap().port, 1);
        let c = decode("room:\n  room_configurations:\n    rc1: ~\n", true).unwrap();
        assert_eq!(c.room.room_configurations.get("rc1"), Some(&None));
        assert_eq!(
            decode("agents:\n  target_load: 1.2\n", true)
                .unwrap()
                .agents
                .target_load,
            1.2f32
        );
        // YAML 1.1 booleans only for typed bool targets
        assert!(
            decode("rtc:\n  use_ice_lite: yes\n", true)
                .unwrap()
                .rtc
                .use_ice_lite
        );
        assert_eq!(decode("region: yes\n", true).unwrap().region, "yes");
        // aggregated type errors keep document order
        assert_eq!(
            decode("port: a\nrtc:\n  tcp_port: b\n", true).unwrap_err(),
            "yaml: unmarshal errors:\n  line 1: cannot unmarshal !!str `a` into uint32\n  line 3: cannot unmarshal !!str `b` into uint32"
        );
    }

    #[test]
    fn comment_only_is_eof() {
        assert_eq!(decode("# nothing\n\n   # here\n", true).unwrap_err(), "EOF");
    }

    #[test]
    fn duration_must_have_unit() {
        assert!(decode("room:\n  create_room_timeout: 5\n", true).is_err());
        let c = decode("room:\n  create_room_timeout: 1m30s\n", true).unwrap();
        assert_eq!(c.room.create_room_timeout, GoDuration::from_secs(90));
    }

    #[test]
    fn lists_replace_maps_merge() {
        let c = decode(
            "room:\n  enabled_codecs:\n    - mime: audio/opus\nkeys:\n  a: b\nvideo:\n  stream_tracker_manager:\n    video:\n      packet_tracker:\n        1:\n          samples_required: 9\n",
            true,
        )
        .unwrap();
        assert_eq!(c.room.enabled_codecs.len(), 1);
        assert_eq!(c.keys.0, vec![("a".to_string(), "b".to_string())]);
        let pt = &c.video.stream_tracker_manager.video.packet_tracker;
        assert_eq!(pt.len(), 3);
        // replaced element is decoded fresh: unspecified fields are zero
        assert_eq!(pt[&1].samples_required, 9);
        assert_eq!(pt[&1].cycles_required, 0);
        assert_eq!(pt[&0].cycles_required, 4);
    }

    #[test]
    fn port_range_scalar_forms() {
        let c = decode("rtc:\n  udp_port: 7882\n", true).unwrap();
        assert_eq!(
            c.rtc.udp_port,
            PortRange {
                start: 7882,
                end: 0
            }
        );
        let c = decode("rtc:\n  udp_port: 7882-7892\n", true).unwrap();
        assert_eq!(
            c.rtc.udp_port,
            PortRange {
                start: 7882,
                end: 7892
            }
        );
        let err = decode("rtc:\n  udp_port: 9-3\n", true).unwrap_err();
        assert!(
            err.contains("end port 3 must be greater than start port 9"),
            "{err}"
        );
    }

    #[test]
    fn livecloud_is_split_off_and_strict() {
        let mut c = Config::default();
        let ext = decode_yaml(
            "livecloud:\n  schema_version: 1\n  deployment: standalone\nport: 1\n",
            true,
            &mut c,
        )
        .unwrap()
        .unwrap();
        assert_eq!(ext.schema_version, Some(1));
        assert_eq!(c.port, 1);
        // unknown extension key is an error even when official strictness is off
        let mut c = Config::default();
        let err = decode_yaml("livecloud:\n  hybrid: {}\n", false, &mut c).unwrap_err();
        assert!(err.contains("hybrid"), "{err}");
    }

    #[test]
    fn bind_addresses_nil_vs_empty() {
        assert_eq!(decode("port: 1\n", true).unwrap().bind_addresses, None);
        assert_eq!(
            decode("bind_addresses: []\n", true).unwrap().bind_addresses,
            Some(vec![])
        );
    }

    #[test]
    fn room_agents_yaml_matches_actual_generated_protoyaml_consumer() {
        // Corrected by the independently executed fixed Go source oracle:
        // agents:null is invalid; [] leaves the generated slice nil; a null
        // message element appends a non-nil default message.
        let yaml = "room:\n  room_configurations:\n    nil: {}\n    empty: {agents: []}\n    full:\n      agents:\n        - agent_name: assistant\n          metadata: context\n          restart_policy: JRP_NEVER\n          deployment: prod\n          attributes: {language: ja}\n        - null\n    aliases: {agents: [{agentName: assistant, restartPolicy: JRP_NEVER}]}\n";
        let config = decode(yaml, true).unwrap();
        let presets = &config.room.room_configurations;
        assert!(presets["nil"].as_ref().unwrap().agents.0.is_none());
        assert!(presets["empty"].as_ref().unwrap().agents.0.is_none());
        let full = presets["full"].as_ref().unwrap().agents.0.as_ref().unwrap();
        assert_eq!(full.len(), 2);
        let first = full[0].as_ref().unwrap();
        assert_eq!(first.agent_name, "assistant");
        assert_eq!(first.metadata, "context");
        assert_eq!(first.restart_policy, 1);
        assert_eq!(first.deployment, "prod");
        assert_eq!(first.attributes["language"], "ja");
        assert_eq!(full[1], Some(RoomAgentDispatch::default()));
        let aliases = presets["aliases"]
            .as_ref()
            .unwrap()
            .agents
            .0
            .as_ref()
            .unwrap();
        assert_eq!(aliases[0].as_ref().unwrap().agent_name, "assistant");
        assert_eq!(aliases[0].as_ref().unwrap().restart_policy, 1);
        // Duplicate empty repeated occurrences must not discard prior messages.
        let repeated=decode("room:\n  room_configurations:\n    p: {agents: [{agent_name: assistant}], agents: []}\n", true).unwrap();
        assert_eq!(
            repeated.room.room_configurations["p"]
                .as_ref()
                .unwrap()
                .agents
                .0
                .as_ref()
                .unwrap()
                .len(),
            1
        );
        for invalid in [
            "agents: null",
            "agents: 1",
            "agents: [{agentname: assistant}]",
            "agents: [{restart_policy: JRP_NOT_A_REAL_POLICY}]",
        ] {
            let text = format!("room:\n  room_configurations:\n    p:\n      {invalid}\n");
            assert!(decode(&text, true).is_err(), "{invalid}");
        }
    }
}
