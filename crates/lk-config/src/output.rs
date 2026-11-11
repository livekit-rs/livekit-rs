//! Byte-exact stdout of the `generate-keys` and `ports` subcommands
//! (`LK/cmd/server/commands.go:41-88`).

use crate::OfficialConfigView;
use crate::keys::{new_api_key, new_api_secret};

/// `generate-keys`: Go's `fmt.Println("API Key: ", k)` puts a space between
/// operands, hence **two** spaces after each colon.
///
/// ```text
/// API Key:  APIxxxxxxxxxxxx
/// API Secret:  <base62 of 32 random bytes>
/// ```
pub fn generate_keys_output() -> String {
    format!(
        "API Key:  {}\nAPI Secret:  {}\n",
        new_api_key(),
        new_api_secret()
    )
}

/// `ports`: TCP lines first, then UDP lines.
///
/// The ICE/UDP line is `fmt.Sprintf("%s", PortRange.MarshalYAML())` upstream.
/// For a single port `MarshalYAML` returns an `int`, so Go prints the verb
/// mismatch `%!s(int=7882) - ICE/UDP`; an invalid range returns `nil`
/// (`%!s(<nil>) - ICE/UDP`). This output matches upstream byte-for-byte on
/// purpose (see [`crate::ports::PortRange::go_sprintf_s_marshal`]).
pub fn ports_output(cfg: &impl OfficialConfigView) -> String {
    let o = cfg.official_config();
    let mut tcp = vec![format!("{} - HTTP service", o.port)];
    if o.rtc.tcp_port != 0 {
        tcp.push(format!("{} - ICE/TCP", o.rtc.tcp_port));
    }
    let mut udp = Vec::new();
    if o.rtc.udp_port.valid() {
        udp.push(format!(
            "{} - ICE/UDP",
            o.rtc.udp_port.go_sprintf_s_marshal()
        ));
    } else {
        udp.push(format!(
            "{}-{} - ICE/UDP range",
            o.rtc.port_range_start, o.rtc.port_range_end
        ));
    }
    if o.turn.enabled {
        if o.turn.tls_port > 0 {
            tcp.push(format!("{} - TURN/TLS", o.turn.tls_port));
        }
        if o.turn.udp_port > 0 {
            udp.push(format!("{} - TURN/UDP", o.turn.udp_port));
        }
    }
    let mut out = String::from("TCP Ports\n");
    for l in &tcp {
        out.push_str(l);
        out.push('\n');
    }
    out.push_str("UDP Ports\n");
    for l in &udp {
        out.push_str(l);
        out.push('\n');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::{BaseValues, Command, Invocation};
    use crate::env::Env;
    use crate::load::{Mode, load};
    use crate::netdetect::tests::FakeProbe;

    fn ports_for(base: BaseValues) -> String {
        let inv = Invocation {
            command: Command::Ports,
            base,
            generated: Vec::new(),
        };
        let cfg = load(
            &inv,
            &Env::default(),
            Mode::Ports,
            &FakeProbe::local("10.0.0.5"),
        )
        .unwrap();
        ports_output(&cfg)
    }

    #[test]
    fn dev_single_port_quirk() {
        let out = ports_for(BaseValues {
            dev: Some(true),
            ..Default::default()
        });
        assert_eq!(
            out,
            "TCP Ports\n7880 - HTTP service\n7881 - ICE/TCP\nUDP Ports\n%!s(int=7882) - ICE/UDP\n"
        );
    }

    #[test]
    fn prod_range_and_turn() {
        let out = ports_for(BaseValues {
            config_body: Some(
                "turn:\n  enabled: true\n  tls_port: 5349\n  udp_port: 3478\n  domain: turn.example.com\n".into(),
            ),
            ..Default::default()
        });
        assert_eq!(
            out,
            "TCP Ports\n7880 - HTTP service\n7881 - ICE/TCP\n5349 - TURN/TLS\nUDP Ports\n50000-60000 - ICE/UDP range\n3478 - TURN/UDP\n"
        );
    }

    #[test]
    fn udp_range_and_invalid_range() {
        let out = ports_for(BaseValues {
            config_body: Some("rtc:\n  udp_port: 7882-7892\n  tcp_port: 0\n".into()),
            ..Default::default()
        });
        assert_eq!(
            out,
            "TCP Ports\n7880 - HTTP service\nUDP Ports\n7882-7892 - ICE/UDP\n"
        );
        let out = ports_for(BaseValues {
            config_body: Some("rtc:\n  udp_port: 7882-7892\n".into()),
            udp_port: Some("9000".into()),
            ..Default::default()
        });
        assert!(out.contains("%!s(<nil>) - ICE/UDP\n"), "{out}");
    }

    #[test]
    fn generate_keys_format() {
        let out = generate_keys_output();
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].starts_with("API Key:  API"));
        assert_eq!(lines[0].len(), "API Key:  ".len() + 15);
        assert!(lines[1].starts_with("API Secret:  "));
    }
}

/// Render a help/usage action to its actual output streams. On usage errors,
/// urfave prints only the error header to stderr; help and main's bare error
/// belong to stdout. Keep this shared by the binary and differential tests.
pub fn help_output(text: &str, usage_error: bool) -> (String, String) {
    if usage_error {
        if let Some((header, help)) = text.split_once("\n\n")
            && let Some(error) = header.strip_prefix("Incorrect Usage: ")
        {
            return (format!("{help}{error}\n"), format!("{header}\n\n"));
        }
        return (String::new(), text.to_string());
    }
    (text.to_string(), String::new())
}

#[cfg(test)]
mod help_output_tests {
    #[test]
    fn usage_errors_split_stderr_header_and_stdout_help() {
        let (stdout, stderr) =
            super::help_output("Incorrect Usage: bad flag\n\nNAME:\n   server\n", true);
        assert_eq!(stdout, "NAME:\n   server\nbad flag\n");
        assert_eq!(stderr, "Incorrect Usage: bad flag\n\n");
        assert_eq!(
            super::help_output("help\n", false),
            ("help\n".into(), String::new())
        );
    }
}
