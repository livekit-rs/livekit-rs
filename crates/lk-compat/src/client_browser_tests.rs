use super::*;
fn browser_meta(browser: &str, protocol: i32) -> ClientMeta {
    ClientMeta {
        browser: browser.into(),
        protocol,
        ..Default::default()
    }
}
#[test]
fn old_public_browser_capabilities_require_exact_browser_names() {
    for browser in [
        "vendor-firefox",
        "firefoxmobile",
        "firefox mobile beta",
        " firefox",
        "firefox ",
    ] {
        let client = browser_meta(browser, 17);
        assert!(!is_firefox(&client));
        let c = capabilities(&client);
        assert!(c.ice_lite && c.sync_stream_id && c.audio_red);
    }
    for browser in ["firefox", "FIREFOX", "firefox mobile", "FiReFoX MoBiLe"] {
        let client = browser_meta(browser, 17);
        assert!(is_firefox(&client));
        let c = capabilities(&client);
        assert!(!c.ice_lite && !c.sync_stream_id && !c.audio_red);
    }
    for browser in ["vendor-safari", "safari preview"] {
        let client = browser_meta(browser, 17);
        assert!(!is_safari(&client));
        assert!(capabilities(&client).audio_red);
    }
}
#[test]
fn old_public_browser_capabilities_go_simple_fold_not_lowercase() {
    for browser in ["safari", "SAFARI", "ſafari", "ſAFARI"] {
        let c = browser_meta(browser, 17);
        assert!(is_safari(&c));
        assert!(!capabilities(&c).audio_red);
    }
    for browser in ["SAFARİ", "firefox mobİle", "firefoK", "safKri"] {
        let c = browser_meta(browser, 17);
        assert!(!is_safari(&c) && !is_firefox(&c));
        assert!(capabilities(&c).audio_red);
    }
}
#[test]
fn old_public_browser_capabilities_preserve_protocol_thresholds() {
    for protocol in [0, 5, 6, 9, 10, 17] {
        for browser in ["vendor-firefox", "chrome", "SAFARİ"] {
            let c = capabilities(&browser_meta(browser, protocol));
            assert_eq!(c.ice_lite, protocol > 5);
            assert_eq!(c.sync_stream_id, protocol > 9);
            assert!(c.audio_red);
        }
        let c = capabilities(&browser_meta("firefox mobile", protocol));
        assert!(!c.ice_lite && !c.sync_stream_id && !c.audio_red);
    }
}
#[test]
#[ignore = "requires actual original public ClientInfo/protocol matrix CLIENT_BROWSER_GO_RESULTS"]
fn old_public_browser_capabilities_actual_source_matrix() {
    let cases: Vec<serde_json::Value> =
        serde_json::from_str(include_str!("client_browser_cases.json")).unwrap();
    assert_eq!(cases.len(), 144);
    let text = std::fs::read_to_string(
        std::env::var_os("CLIENT_BROWSER_GO_RESULTS").expect("actual original rows required"),
    )
    .unwrap();
    let rows: Vec<serde_json::Value> = text
        .lines()
        .map(|x| serde_json::from_str(x).unwrap())
        .collect();
    assert_eq!(rows.len(), cases.len());
    let mut seen = std::collections::BTreeSet::new();
    for (case, row) in cases.iter().zip(rows) {
        assert_eq!(
            row.as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect::<std::collections::BTreeSet<_>>(),
            ["id", "input", "audio_red", "ice_lite", "sync_stream_id"]
                .into_iter()
                .collect()
        );
        assert_eq!(row["id"], case["id"]);
        assert!(seen.insert(row["id"].as_str().unwrap().to_owned()));
        assert_eq!(row["input"], case.clone());
        let c = capabilities(&browser_meta(
            case["browser"].as_str().unwrap(),
            i32::try_from(case["protocol"].as_i64().unwrap()).unwrap(),
        ));
        for key in ["audio_red", "ice_lite", "sync_stream_id"] {
            assert!(row[key].is_boolean());
        }
        assert_eq!(row["audio_red"], c.audio_red, "case {}", case["id"]);
        assert_eq!(row["ice_lite"], c.ice_lite, "case {}", case["id"]);
        assert_eq!(
            row["sync_stream_id"], c.sync_stream_id,
            "case {}",
            case["id"]
        );
    }
}
