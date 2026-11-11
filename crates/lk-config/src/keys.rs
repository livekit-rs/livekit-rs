//! API keys: `--keys`/`LIVEKIT_KEYS` parsing, `key_file` loading
//! (`ValidateKeys`, `config.go:820-855`), TURN secret files
//! (`LoadTURNSecrets`, `config.go:857-898`) and `generate-keys` material.

use std::{io::Read, path::Path};

use crate::cli::quote::{quote, quote_bytes};
use crate::error::ConfigError;
use crate::official::KeyMap;
use crate::strconv::trim_space_bytes;
use crate::yamlv3::{Any, MapEntries, decode_map};

/// Upstream `--keys` parse failure text (`config.go:1118`).
pub(crate) const KEYS_PARSE_ERROR: &str =
    "Could not parse keys, it needs to be exactly, \"key: secret\", including the space";

/// `map[string]any` (`unmarshalKeys`' `temp`), in document order.
#[derive(Default)]
struct AnyMap(Vec<(String, Any)>);

impl crate::yamlv3::Decode for AnyMap {
    fn decode(
        &mut self,
        d: &mut crate::yamlv3::Dec<'_>,
        n: crate::yamlv3::Id,
        go: &str,
    ) -> Result<bool, crate::yamlv3::Fail> {
        decode_map(&mut MapEntries::Ordered(&mut self.0), d, n, go)
    }
}

/// `--keys` / `LIVEKIT_KEYS`: YAML into `map[string]any`, keeping only string
/// values (e.g. `k: 123` is dropped silently, as upstream). Replaces `keys`.
pub(crate) fn parse_keys_flag(s: &str) -> Result<KeyMap, ConfigError> {
    let mut temp = AnyMap::default();
    crate::yamlv3::unmarshal(s.as_bytes(), &mut temp, "map[string]interface {}")
        .map_err(|_| ConfigError::msg(KEYS_PARSE_ERROR))?;
    let mut out = KeyMap::default();
    for (k, v) in temp.0 {
        if let Any::Str(secret) = v {
            out.insert(k, secret);
        }
    }
    Ok(out)
}

/// Go-style `*PathError` text: `stat /x: no such file or directory`.
pub(crate) fn path_error(op: &str, path: &Path, error: &std::io::Error) -> ConfigError {
    let mut bytes = format!("{op} ").into_bytes();
    bytes.extend_from_slice(path.as_os_str().as_encoded_bytes());
    bytes.extend_from_slice(format!(": {}", go_errno_text(error)).as_bytes());
    ConfigError::raw(bytes)
}

/// Go prints lower-case `strerror` text without Rust's ` (os error N)` suffix.
pub fn go_errno_text(e: &std::io::Error) -> String {
    let s = e.to_string();
    let s = match s.find(" (os error ") {
        Some(i) => s[..i].to_string(),
        None => s,
    };
    let mut c = s.chars();
    match c.next() {
        Some(first) => first.to_lowercase().collect::<String>() + c.as_str(),
        None => s,
    }
}

/// Rejects files readable/writable/executable by "others" (`perm & 0o007`).
#[cfg(unix)]
fn others_bits_set(meta: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;
    meta.permissions().mode() & 0o007 != 0
}

#[cfg(not(unix))]
fn others_bits_set(_meta: &std::fs::Metadata) -> bool {
    false
}

/// `ValidateKeys` key-file part: stat, permission check, then decode one YAML
/// mapping that **replaces** the inline keys. An empty file is `EOF`.
pub(crate) fn load_key_file(path: &Path) -> Result<KeyMap, ConfigError> {
    let meta = std::fs::metadata(path).map_err(|e| path_error("stat", path, &e))?;
    if others_bits_set(&meta) {
        return Err(ConfigError::msg(
            "key file others permissions must be set to 0",
        ));
    }
    decode_key_file(path)
}

/// `createToken` loads a readable key file without `ValidateKeys`' server-only
/// permissions restriction (`cmd/server/commands.go:119-137`).
pub(crate) fn load_join_token_key_file(path: &Path) -> Result<KeyMap, ConfigError> {
    std::fs::metadata(path).map_err(|e| path_error("stat", path, &e))?;
    decode_key_file(path)
}

fn decode_key_file(path: &Path) -> Result<KeyMap, ConfigError> {
    let mut file = std::fs::File::open(path).map_err(|e| path_error("open", path, &e))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|e| path_error("yaml: input error: read", path, &e))?;
    // yaml.NewDecoder(f).Decode(conf.Keys) onto a fresh map[string]string.
    let mut keys = KeyMap::default();
    crate::yamlv3::decoder_decode(&bytes, false, &mut keys, "map[string]string")
        .map_err(ConfigError::msg)?;
    Ok(keys)
}

/// `LoadTURNSecrets` secret-file part. Returns the trimmed secret.
pub(crate) fn load_turn_secret_file(host: &str, path: &Path) -> Result<Vec<u8>, ConfigError> {
    let meta = std::fs::metadata(path).map_err(|e| path_error("stat", path, &e))?;
    if others_bits_set(&meta) {
        return Err(ConfigError::msg(
            "turn secret file others permissions must be set to 0",
        ));
    }
    let quoted_path = quote_bytes(path.as_os_str().as_encoded_bytes());
    let read_error = |op, error: &std::io::Error| {
        let mut message = format!("reading turn secret file {quoted_path}: ").into_bytes();
        message.extend(path_error(op, path, error).stdout_bytes());
        ConfigError::raw(message)
    };
    let mut file = std::fs::File::open(path).map_err(|error| read_error("open", &error))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|error| read_error("read", &error))?;
    let secret = trim_space_bytes(&bytes);
    if secret.is_empty() {
        return Err(ConfigError::msg(format!(
            "turn server {} secret file {quoted_path}: turn secret is empty",
            quote(host)
        )));
    }
    Ok(secret.to_vec())
}

// ---------------------------------------------------------------------------
// random material
// ---------------------------------------------------------------------------

/// shortuuid alphabet used by `guid.New` (`PROTO/utils/guid/id.go:35`).
pub(crate) const BASE57: &[u8; 57] = b"23456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
const BASE62: &[u8; 62] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

fn os_random(buf: &mut [u8]) {
    // generate-keys and node IDs have no error channel upstream either; an OS
    // RNG failure is unrecoverable.
    getrandom::fill(buf).expect("operating system random source unavailable");
}

/// `n` uniformly random characters from the base57 alphabet.
pub(crate) fn random_base57(n: usize) -> String {
    let mut out = String::with_capacity(n);
    let mut buf = [0u8; 64];
    while out.len() < n {
        os_random(&mut buf);
        for &b in buf.iter() {
            // rejection sampling: 228 = 4 * 57
            if b < 228 {
                out.push(BASE57[(b % 57) as usize] as char);
                if out.len() == n {
                    break;
                }
            }
        }
    }
    out
}

/// Exact jxskiss/base62 v1.1.0 StdEncoding bit-packing encoder.
/// Copyright (c) 2019 Shawn Wang <jxskiss@126.com>, MIT; see F05 source notice.
/// The encoder consumes least-significant trailing groups first and compacts
/// digit values with bits 1..4 set to a five-bit group, as upstream encodeV2.
pub(crate) fn base62(bytes: &[u8]) -> String {
    let mut remaining = bytes.len() * 8;
    let mut out = String::with_capacity(bytes.len() * 9 / 5);
    while remaining > 0 {
        let mut bit_count = remaining & 7;
        let mut index = remaining >> 3;
        if bit_count == 0 {
            index -= 1;
            bit_count = 8;
        }
        let mut value = bytes[index] >> (8 - bit_count);
        if bit_count < 6 && index > 0 {
            value |= bytes[index - 1] << bit_count;
        }
        value &= 0x3f;
        let mut consumed = 6;
        if value & 0x1e == 0x1e {
            if remaining > 6 || value > 0x1f {
                consumed = 5;
            }
            value &= 0x1f;
        }
        out.push(BASE62[value as usize] as char);
        remaining = remaining.saturating_sub(consumed);
    }
    out
}

/// `guid.New("API")`.
pub(crate) fn new_api_key() -> String {
    format!("API{}", random_base57(12))
}

/// `utils.RandomSecret()`: base62 of 32 bytes from the OS RNG.
pub(crate) fn new_api_secret() -> String {
    let mut b = [0u8; 32];
    os_random(&mut b);
    base62(&b)
}

/// `guid.New("ND_")` (`PROTO/utils/guid/id.go:39`).
pub(crate) fn new_node_id() -> String {
    format!("ND_{}", random_base57(12))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_flag_keeps_only_strings() {
        let k = parse_keys_flag("a: b\nc: 123\nd: \"456\"\n").unwrap();
        assert_eq!(
            k.0,
            vec![
                ("a".to_string(), "b".to_string()),
                ("d".to_string(), "456".to_string())
            ]
        );
        assert!(parse_keys_flag("").unwrap().is_empty());
        assert_eq!(
            parse_keys_flag("a:b").unwrap_err().to_string(),
            KEYS_PARSE_ERROR
        );
    }

    #[test]
    fn base62_roundtrip_shape() {
        // Immutable source reasoning vectors; independent Go oracle must also
        // run before delivery, not merely compare this port with itself.
        assert_eq!(base62(&[]), "");
        assert_eq!(base62(&[0]), "AA");
        assert_eq!(base62(&[61]), "9A");
        assert_eq!(base62(&[1, 0]), "AEA");
        assert_eq!(base62(&[255]), "fH");
        assert_eq!(base62(&[0; 32]), "A".repeat(43));
        let s = new_api_secret();
        assert!(s.len() >= 43 && s.len() <= 52, "{s}");
        assert!(s.bytes().all(|b| b.is_ascii_alphanumeric()));
    }

    #[test]
    fn api_key_shape() {
        let k = new_api_key();
        assert_eq!(k.len(), 15);
        assert!(k.starts_with("API"));
        assert!(k[3..].bytes().all(|b| BASE57.contains(&b)));
        assert!(new_node_id().starts_with("ND_"));
    }

    #[test]
    fn errno_text_is_go_like() {
        let e = std::io::Error::from_raw_os_error(2);
        let t = go_errno_text(&e);
        assert!(t.starts_with("no such file"), "{t}");
        assert!(!t.contains("os error"));
    }

    #[cfg(unix)]
    #[test]
    fn key_file_permissions_and_replace() {
        use std::os::unix::fs::PermissionsExt;
        let dir = std::env::temp_dir().join(format!("lkcfg-keys-{}", random_base57(8)));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        let directory_error = format!("yaml: input error: read {}: is a directory", dir.display());
        for error in [
            load_key_file(&dir).unwrap_err(),
            load_join_token_key_file(&dir).unwrap_err(),
        ] {
            assert_eq!(error.stdout_bytes(), directory_error.as_bytes());
            assert_eq!(error.exit_code(), 1);
            assert!(!error.diagnostic_to_stderr());
        }
        let p = dir.join("keys.yaml");
        std::fs::write(&p, "k1: s1\nk2: s2\n").unwrap();
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert_eq!(
            load_key_file(&p).unwrap_err().to_string(),
            "key file others permissions must be set to 0"
        );
        assert_eq!(load_join_token_key_file(&p).unwrap().len(), 2);
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o640)).unwrap();
        let k = load_key_file(&p).unwrap();
        assert_eq!(k.len(), 2);
        std::fs::write(&p, "").unwrap();
        assert_eq!(load_key_file(&p).unwrap_err().to_string(), "EOF");
        assert_eq!(load_join_token_key_file(&p).unwrap_err().to_string(), "EOF");
        std::fs::write(&p, "{}").unwrap();
        assert!(load_join_token_key_file(&p).unwrap().is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }
    #[cfg(unix)]
    #[test]
    fn turn_secret_reader_separates_raw_path_and_quoted_context() {
        use std::os::unix::fs::PermissionsExt;
        let dir = std::env::temp_dir().join(format!("lkcfg-turn-\"{}\n", random_base57(8)));
        std::fs::create_dir(&dir).unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        let result = load_turn_secret_file("turn", &dir).unwrap_err();
        let path = dir.to_str().unwrap();
        assert_eq!(
            result.to_string(),
            format!(
                "reading turn secret file {}: read {path}: is a directory",
                quote(path)
            )
        );
        let file = dir.join("secret");
        std::fs::write(&file, " \u{85}demo-secret\u{2002}\n").unwrap();
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(
            load_turn_secret_file("turn", &file).unwrap(),
            b"demo-secret"
        );
        std::fs::write(&file, b" \xe2\x80\x82\xffA\0\xef\xbf\xbd\xc2\xc2\x85\n").unwrap();
        assert_eq!(
            load_turn_secret_file("turn", &file).unwrap(),
            b"\xffA\0\xef\xbf\xbd\xc2"
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }
    #[test]
    #[ignore = "requires actual pinned Go oracle via CLI_GO_ORACLE_RESULTS"]
    fn pinned_base62_source_oracle() {
        let path =
            std::env::var("CLI_GO_ORACLE_RESULTS").expect("set actual Go oracle results path");
        let expected: Vec<serde_json::Value> = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let inputs = include_str!("../../../tests/oracles/cli/cases.jsonl");
        let mut count = 0;
        for input in inputs.lines() {
            let row: serde_json::Value = serde_json::from_str(input).unwrap();
            let Some(hex) = row["hex"].as_str() else {
                continue;
            };
            let data: Vec<u8> = hex
                .as_bytes()
                .as_chunks::<2>()
                .0
                .iter()
                .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
                .collect();
            let result = expected
                .iter()
                .find(|value| value["name"] == row["name"])
                .expect("missing actual Go vector");
            assert_eq!(
                base62(&data),
                result["encoded"].as_str().unwrap(),
                "vector {}",
                row["name"]
            );
            count += 1;
        }
        assert_eq!(count, 8, "all source encoder fixtures must execute");
    }
}
