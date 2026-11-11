// Copyright 2009 The Go Authors. All rights reserved.
// Go strconv quote.go/IsPrint port. BSD-3-Clause, LICENSES/Go-BSD-3-Clause.txt.
//! Exact CLI %q for valid UTF-8 Rust argv, independent of old diagnostic helpers.
#[path = "cli_quote_tables.rs"]
mod tables;
fn printable(value: u32) -> bool {
    if value <= 0xff {
        return (0x20..=0x7e).contains(&value) || ((0xa1..=0xff).contains(&value) && value != 0xad);
    }
    let (ranges, exceptions) = if value < 0x10000 {
        (tables::IS_PRINT16, tables::IS_NOT_PRINT16)
    } else {
        (tables::IS_PRINT32, tables::IS_NOT_PRINT32)
    };
    let index = ranges.partition_point(|&entry| entry < value);
    if index >= ranges.len() || value < ranges[index & !1] || value > ranges[index | 1] {
        return false;
    }
    if value >= 0x20000 {
        return true;
    }
    let exception = if value >= 0x10000 {
        value - 0x10000
    } else {
        value
    };
    exceptions.binary_search(&exception).is_err()
}
/// Go strconv.Quote on arbitrary bytes; valid spans retain the F51 Unicode tables.
pub(crate) fn quote_bytes(mut input: &[u8]) -> String {
    let mut output = String::from("\"");
    while !input.is_empty() {
        match std::str::from_utf8(input) {
            Ok(valid) => {
                let quoted = quote(valid);
                output.push_str(&quoted[1..quoted.len() - 1]);
                break;
            }
            Err(error) => {
                let valid =
                    std::str::from_utf8(&input[..error.valid_up_to()]).expect("validated prefix");
                let quoted = quote(valid);
                output.push_str(&quoted[1..quoted.len() - 1]);
                input = &input[error.valid_up_to()..];
                output.push_str(&format!("\\x{:02x}", input[0]));
                input = &input[1..];
            }
        }
    }
    output.push('"');
    output
}
pub(crate) fn quote(input: &str) -> String {
    let mut output = String::with_capacity(input.len() + 2);
    output.push('"');
    for rune in input.chars() {
        let value = rune as u32;
        match rune {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            _ if printable(value) => output.push(rune),
            '\x07' => output.push_str("\\a"),
            '\x08' => output.push_str("\\b"),
            '\x0c' => output.push_str("\\f"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            '\x0b' => output.push_str("\\v"),
            _ if value < 0x20 || value == 0x7f => output.push_str(&format!("\\x{value:02x}")),
            _ if value < 0x10000 => output.push_str(&format!("\\u{value:04x}")),
            _ => output.push_str(&format!("\\U{value:08x}")),
        }
    }
    output.push('"');
    output
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    #[ignore = "requires actual fixed Go strconv.Quote result rows"]
    fn actual_go_quote_vectors() {
        let cases: serde_json::Value = serde_json::from_slice(
            &std::fs::read(std::env::var("CLI_QUOTE_GO_CASES").expect("source cases")).unwrap(),
        )
        .unwrap();
        let lines =
            std::fs::read_to_string(std::env::var("CLI_QUOTE_GO_RESULTS").expect("actual Go rows"))
                .unwrap();
        let rows: Vec<serde_json::Value> = lines
            .lines()
            .filter(|s| !s.is_empty())
            .map(|s| serde_json::from_str(s).unwrap())
            .collect();
        let cases = cases.as_array().unwrap();
        assert_eq!(cases.len(), 24);
        assert_eq!(rows.len(), 25);
        for (case, row) in cases.iter().zip(&rows[..24]) {
            assert_eq!(case["id"], row["id"]);
            assert_eq!(
                quote(case["input"].as_str().unwrap()),
                row["quoted"].as_str().unwrap()
            );
        }
        use sha2::{Digest, Sha256};
        let mut digest = Sha256::new();
        let mut count = 0u64;
        for cp in 0..=0x10ffffu32 {
            if let Some(rune) = char::from_u32(cp) {
                digest.update(cp.to_le_bytes());
                let quoted = quote(&rune.to_string());
                digest.update((quoted.len() as u32).to_le_bytes());
                digest.update(quoted.as_bytes());
                count += 1;
            }
        }
        assert_eq!(count, 1_112_064);
        assert_eq!(rows[24]["id"], "all-valid-unicode");
        assert_eq!(rows[24]["count"], count);
        assert_eq!(
            format!("{:x}", digest.finalize()),
            rows[24]["sha256"].as_str().unwrap()
        );
    }
}
