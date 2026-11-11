//! LiveKit v1.13.10 client behavior profile. Kept outside the neutral room
//! engine; browser matching follows the fixed ClientInfo exact EqualFold rules.
use rtc_core::session::{ClientCapabilities, ClientMeta, SdkKind};

// EqualFold against ASCII names uses Unicode simple folding, not lowercase
// expansion and not the static ClientConfiguration strings.ToLower rule.
// Go SimpleFold's only non-ASCII orbits touching ASCII are long-s/S/s and
// Kelvin-sign/K/k. Dotted-I is NOT equivalent to ASCII I/i in EqualFold.
fn browser_equal_fold(value: &str, expected: &str) -> bool {
    debug_assert!(expected.is_ascii());
    value
        .chars()
        .map(|c| match c {
            '\u{017f}' => 's',
            '\u{212a}' => 'k',
            _ => c.to_ascii_lowercase(),
        })
        .eq(expected.chars().map(|c| c.to_ascii_lowercase()))
}
pub fn is_firefox(client: &ClientMeta) -> bool {
    browser_equal_fold(&client.browser, "firefox")
        || browser_equal_fold(&client.browser, "firefox mobile")
}
pub fn is_safari(client: &ClientMeta) -> bool {
    browser_equal_fold(&client.browser, "safari")
}

/// Project every capability after request metadata/UA augmentation. A neutral
/// all-false default is not a projection of protocol0/Unknown SDK.
pub fn capabilities(client: &ClientMeta) -> ClientCapabilities {
    let protocol = client.protocol;
    let firefox = is_firefox(client);
    ClientCapabilities {
        one_shot: false,
        ice_tcp: supports_ice_tcp(client),
        video_publication_requires_rtp: client.sdk == SdkKind::Go,
        codec_switch_requires_renegotiation: matches!(client.sdk, SdkKind::Go | SdkKind::Unknown),
        subscriber_primary: protocol > 2,
        speaker_changed: protocol > 2,
        connection_quality: protocol > 4,
        ice_lite: protocol > 5 && !firefox,
        unpublish: protocol > 6,
        fast_start: protocol > 7,
        disconnected_update: protocol > 8,
        sync_stream_id: protocol > 9 && !firefox,
        disable_playout_delay_for_sync: firefox,
        connection_quality_lost: protocol > 10,
        identity_based_reconnection: protocol > 11,
        regions_in_leave: protocol > 12,
        non_error_signal_response: protocol > 14,
        request_response: client.sdk != SdkKind::Rust || protocol >= 10,
        audio_red: !firefox && !is_safari(client),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn every_protocol_threshold_preserves_before_equal_and_after_behavior() {
        type CapabilitySelector = (i32, fn(ClientCapabilities) -> bool);
        let selectors: [CapabilitySelector; 12] = [
            (2, |c| c.subscriber_primary),
            (2, |c| c.speaker_changed),
            (4, |c| c.connection_quality),
            (5, |c| c.ice_lite),
            (6, |c| c.unpublish),
            (7, |c| c.fast_start),
            (8, |c| c.disconnected_update),
            (9, |c| c.sync_stream_id),
            (10, |c| c.connection_quality_lost),
            (11, |c| c.identity_based_reconnection),
            (12, |c| c.regions_in_leave),
            (14, |c| c.non_error_signal_response),
        ];
        for (threshold, select) in selectors {
            for protocol in [threshold - 1, threshold, threshold + 1] {
                let client = ClientMeta {
                    protocol,
                    ..Default::default()
                };
                assert_eq!(select(capabilities(&client)), protocol > threshold);
            }
        }
    }
    #[test]
    fn unknown_low_protocol_rust_and_exact_browser_exceptions_are_explicit() {
        let unknown = ClientMeta::default();
        let projected = capabilities(&unknown);
        assert!(projected.request_response);
        assert!(projected.audio_red);
        assert_ne!(projected, ClientCapabilities::default());
        for protocol in [0, 9, 10] {
            let rust = ClientMeta {
                sdk: SdkKind::Rust,
                protocol,
                ..Default::default()
            };
            assert_eq!(capabilities(&rust).request_response, protocol >= 10);
        }
        for (browser, firefox, safari) in [
            ("vendor-FIrEFoX-test", false, false),
            ("vendor-SAfARi-test", false, false),
            ("Unknown", false, false),
        ] {
            let client = ClientMeta {
                protocol: 16,
                browser: browser.into(),
                ..Default::default()
            };
            let c = capabilities(&client);
            assert_eq!(c.ice_lite, !firefox);
            assert_eq!(c.sync_stream_id, !firefox);
            assert_eq!(c.audio_red, !firefox && !safari);
        }
    }
}

#[cfg(test)]
mod publication_policy_tests {
    use super::*;
    #[test]
    fn only_go_sdk_video_uses_first_packet_codec_registration_policy() {
        for sdk in [SdkKind::Go, SdkKind::Js, SdkKind::Rust, SdkKind::Unknown] {
            let expects_rtp = sdk == SdkKind::Go;
            let client = ClientMeta {
                sdk,
                ..Default::default()
            };
            assert_eq!(
                capabilities(&client).video_publication_requires_rtp,
                expects_rtp
            );
        }
    }
}

#[cfg(test)]
#[path = "client_browser_tests.rs"]
mod client_browser_tests;

/// Exact fixed ClientInfo.compareVersion uses three Go Atoi components, not
/// semver: missing/invalid components are zero and decimal overflow saturates.
fn supports_ice_tcp(client: &ClientMeta) -> bool {
    if client.sdk == SdkKind::Go {
        return false;
    }
    if client.sdk != SdkKind::Swift {
        return true;
    }
    fn atoi(value: &str) -> i64 {
        let (negative, digits) = if let Some(value) = value.strip_prefix('-') {
            (true, value)
        } else {
            (false, value.strip_prefix('+').unwrap_or(value))
        };
        if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
            return 0;
        }
        let limit = if negative {
            i64::MAX as u64 + 1
        } else {
            i64::MAX as u64
        };
        let number = digits.bytes().fold(0u64, |n, b| {
            n.saturating_mul(10)
                .saturating_add(u64::from(b - b'0'))
                .min(limit)
        });
        if negative {
            (-(number as i128)) as i64
        } else {
            number as i64
        }
    }
    let mut parts = client.version.split('.');
    let actual = std::array::from_fn::<_, 3, _>(|_| atoi(parts.next().unwrap_or("")));
    actual >= [1, 0, 5]
}

#[cfg(test)]
mod ice_tcp_source_tests {
    use super::*;
    #[test]
    fn exact_original_sdk_and_three_atoi_version_boundaries() {
        for (sdk, version, expected) in [
            (SdkKind::Go, "99.99.99", false),
            (SdkKind::Js, "", true),
            (SdkKind::Swift, "1.0.4", false),
            (SdkKind::Swift, "1.0.5", true),
            (SdkKind::Swift, "1.0.5-beta", false),
            (SdkKind::Swift, "1.0.5.0", true),
            (SdkKind::Swift, "1.+0.+5", true),
            (SdkKind::Swift, "1.0.99999999999999999999999999", true),
            (SdkKind::Swift, "1.0.-99999999999999999999999999", false),
            (SdkKind::Swift, "2", true),
        ] {
            let client = ClientMeta {
                sdk: sdk.clone(),
                version: version.into(),
                ..Default::default()
            };
            assert_eq!(capabilities(&client).ice_tcp, expected, "{sdk:?} {version}");
        }
    }
}

#[cfg(test)]
#[test]
fn source_playout_delay_incompatibility_is_firefox_family_only() {
    for (browser, protocol, expected) in [
        ("Firefox", 17, true),
        ("fIrEfOx MoBiLe", 17, true),
        ("Safari", 17, false),
        ("Chrome", 5, false),
        ("Firefox Beta", 17, false),
    ] {
        let profile = capabilities(&ClientMeta {
            browser: browser.into(),
            protocol,
            ..Default::default()
        });
        assert_eq!(
            profile.disable_playout_delay_for_sync, expected,
            "{browser}"
        );
    }
}
