//! The wire text of the connection-initialisation commands.
//!
//! Redis matches command names case-insensitively, so nothing in stdout can show a
//! wrong casing — but the bytes are the contract, and go-redis v9.22.0 writes every
//! command name as a lowercase literal. These tests pin the packed bytes against
//! the library source; `tap-connections.py` shows the same bytes on a live run.

use super::*;

fn packed(cmd: &redis::Cmd) -> String {
    String::from_utf8(cmd.get_packed_command())
        .expect("RESP is ASCII")
        .replace("\r\n", "\\r\\n")
}

fn options(username: &str, password: &str, db: i64) -> Options {
    Options {
        username: username.into(),
        password: password.into(),
        db,
        pool_size: 0,
        pool_timeout: Duration::from_secs(4),
        retries: 3,
        dial: None,
        read: None,
        write: None,
        tls: None,
    }
}

#[test]
fn a_plain_handshake_is_the_lowercase_hello_literal() {
    // commands.go:360 Hello: `args = append(args, "hello", ver)`.
    assert_eq!(
        packed(&hello_command(&options("", "", 0))),
        "*2\\r\\n$5\\r\\nhello\\r\\n$1\\r\\n3\\r\\n"
    );
}

#[test]
fn a_password_rides_inside_hello_with_the_default_user() {
    // commands.go:365-370: `hello 3 auth default <password>`, username-less.
    assert_eq!(
        packed(&hello_command(&options("", "s3cret", 0))),
        "*5\\r\\n$5\\r\\nhello\\r\\n$1\\r\\n3\\r\\n$4\\r\\nauth\\r\\n$7\\r\\ndefault\\r\\n$6\\r\\ns3cret\\r\\n"
    );
    assert_eq!(
        packed(&hello_command(&options("alice", "s3cret", 0))),
        "*5\\r\\n$5\\r\\nhello\\r\\n$1\\r\\n3\\r\\n$4\\r\\nauth\\r\\n$5\\r\\nalice\\r\\n$6\\r\\ns3cret\\r\\n"
    );
}

#[test]
fn the_resp2_fallback_auth_matches_the_library_builder() {
    // commands.go:282 (single argument) and :291 (username, password).
    assert_eq!(
        packed(&auth_command(&options("", "s3cret", 0))),
        "*2\\r\\n$4\\r\\nauth\\r\\n$6\\r\\ns3cret\\r\\n"
    );
    assert_eq!(
        packed(&auth_command(&options("alice", "s3cret", 0))),
        "*3\\r\\n$4\\r\\nauth\\r\\n$5\\r\\nalice\\r\\n$6\\r\\ns3cret\\r\\n"
    );
}

#[test]
fn select_and_maint_notifications_are_lowercase_too() {
    // commands.go:311 Select, commands.go:556-566 ClientMaintNotifications.
    assert_eq!(
        packed(&select_command(3)),
        "*2\\r\\n$6\\r\\nselect\\r\\n$1\\r\\n3\\r\\n"
    );
    assert_eq!(
        packed(&maint_notifications_command("cloud")),
        "*5\\r\\n$6\\r\\nclient\\r\\n$19\\r\\nmaint_notifications\\r\\n$2\\r\\non\\r\\n\
         $20\\r\\nmoving-endpoint-type\\r\\n$5\\r\\ncloud\\r\\n"
    );
}

#[test]
fn the_identity_pipeline_uppercases_only_the_library_info_token() {
    // commands.go:339/341: `client setinfo LIB-NAME <lib>` / `client setinfo LIB-VER <ver>`.
    let identity = identity_commands();
    assert_eq!(identity.len(), 2);
    assert_eq!(
        packed(&identity[0]),
        "*4\\r\\n$6\\r\\nclient\\r\\n$7\\r\\nsetinfo\\r\\n$8\\r\\nLIB-NAME\\r\\n$19\\r\\ngo-redis(,go1.26.0)\\r\\n"
    );
    assert_eq!(
        packed(&identity[1]),
        "*4\\r\\n$6\\r\\nclient\\r\\n$7\\r\\nsetinfo\\r\\n$7\\r\\nLIB-VER\\r\\n$6\\r\\n9.22.0\\r\\n"
    );
}

#[test]
fn every_command_name_is_lowercased_at_the_builder() {
    // `command()` is the choke point: a call site that spells the name in caps
    // still gets go-redis' wire text, so the casing cannot regress by accident.
    assert_eq!(
        packed(&wire::command("PING", &[])),
        "*1\\r\\n$4\\r\\nping\\r\\n"
    );
    assert_eq!(
        packed(&wire::command("HVALS", &["nodes"])),
        "*2\\r\\n$5\\r\\nhvals\\r\\n$5\\r\\nnodes\\r\\n"
    );
}
