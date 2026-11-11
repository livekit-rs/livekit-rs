//! Test input through the public configuration boundary, without socket probing.

pub(crate) fn config() -> rtc_core::config::ServerConfig {
    struct LoopbackProbe;
    impl lk_config::netdetect::NetProbe for LoopbackProbe {
        fn local_addresses(&self) -> Vec<std::net::IpAddr> {
            vec![std::net::Ipv4Addr::LOCALHOST.into()]
        }
        fn external_ip(&self, _: &[String], _: bool) -> Result<std::net::IpAddr, String> {
            panic!("local test configuration must not request external discovery")
        }
        fn sleep(&self, _: std::time::Duration) {
            panic!("local test configuration must not retry network discovery")
        }
    }
    let info = lk_config::ProgramInfo {
        binary_name: "livecloud".into(),
        product_version: "test".into(),
        compat_version: "1.13.10",
    };
    let args = ["livecloud", "--dev", "--node-ip", "127.0.0.1"]
        .into_iter()
        .map(Into::into)
        .collect();
    match lk_config::parse_with_probe(args, &lk_config::Env::default(), &info, &LoopbackProbe)
        .unwrap()
    {
        lk_config::EntryAction::RunServer(loaded) => loaded.server,
        action => panic!("expected normalized server configuration, got {action:?}"),
    }
}

/// Actual no-broker application graph with an owned socket-free media router.
/// The caller must shut down the returned router after its assertions.
pub(crate) async fn state() -> (crate::state::AppState, rtc_runtime::net::NetRouter) {
    use rtc_core::events::NullSink;
    use rtc_runtime::net::{LocalInterface, NetConfig, NetRouter};
    use std::sync::Arc;
    let local = std::net::Ipv4Addr::LOCALHOST.into();
    let net = NetRouter::start(NetConfig {
        udp_enabled: false,
        udp_port: None,
        port_range: (0, 0),
        tcp_port: None,
        interfaces: vec![LocalInterface {
            bind: local,
            advertise: local,
        }],
    })
    .await
    .unwrap();
    let state = crate::state::CompatServices::new(Arc::new(crate::test_support::config()))
        .assemble(net.clone(), Arc::new(NullSink));
    (state, net)
}
