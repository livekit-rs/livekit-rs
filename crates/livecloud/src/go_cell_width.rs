// Source-derived from displaywidth v0.11.0 and tablewriter v1.1.4. See NOTICE.
// Only list-nodes measurement. Never use the measured/stripped value as output text.
#[path = "go_width_data.rs"]
mod data;
#[derive(Clone, Copy, Debug)]
pub(crate) struct Options {
    pub east_asian_width: bool,
    pub force_narrow_borders: bool,
    /// Actual source TabWidth() policy result, supplied explicitly by host.
    pub tab_width: usize,
}
/// Exact displaywidth.Options{EastAsianWidth:...}.Rune for valid Rust chars.
/// Both ControlSequences flags are false in the real twwidth invocation.
pub(crate) fn rune_width(c: char, east_asian_width: bool) -> usize {
    if c.is_ascii() {
        return usize::from(c > '\u{001f}' && c != '\u{007f}');
    }
    let mut buf = [0; 4];
    let bytes = c.encode_utf8(&mut buf).as_bytes();
    let mut n = usize::from(data::INDEX[usize::from(bytes[0])]);
    for byte in &bytes[1..bytes.len() - 1] {
        n = usize::from(data::INDEX[(n << 6) + usize::from(*byte)]);
    }
    let property = data::VALUES[(n << 6) + usize::from(bytes[bytes.len() - 1])];
    match property {
        1 => 0,
        2 => 2,
        3 if east_asian_width => 2,
        _ => 1,
    }
}
fn measured_rune(c: char, opts: Options) -> usize {
    if opts.force_narrow_borders && ('\u{2500}'..='\u{257f}').contains(&c) {
        return 1;
    }
    if c == '\t' {
        return opts.tab_width;
    }
    rune_width(c, opts.east_asian_width)
}
/// End of the exact source CSI/OSC regexp match, if one starts at `at`.
/// OSC dot does not match LF; valid sequences are removed from measurement only.
fn ansi_end(raw: &[u8], at: usize) -> Option<usize> {
    if raw.get(at) != Some(&0x1b) {
        return None;
    }
    match raw.get(at + 1) {
        Some(b'[') => {
            let mut end = at + 2;
            while raw.get(end).is_some_and(|b| (0x30..=0x3f).contains(b)) {
                end += 1;
            }
            while raw.get(end).is_some_and(|b| (0x20..=0x2f).contains(b)) {
                end += 1;
            }
            raw.get(end)
                .is_some_and(|b| (0x40..=0x7e).contains(b))
                .then_some(end + 1)
        }
        Some(b']') => {
            let mut end = at + 2;
            while let Some(byte) = raw.get(end) {
                if *byte == b'\n' {
                    return None;
                }
                if *byte == 7 {
                    return Some(end + 1);
                }
                if *byte == 0x1b && raw.get(end + 1) == Some(&b'\\') {
                    return Some(end + 2);
                }
                end += 1;
            }
            None
        }
        _ => None,
    }
}
/// Source WidthWithOptions: ANSI regex strip, then sum original per-rune widths.
/// Explicit options avoid silently inventing locale, tab/editor or border policy.
pub(crate) fn width_with_options(value: &str, opts: Options) -> usize {
    let mut at = 0;
    let mut width = 0;
    while at < value.len() {
        if let Some(end) = ansi_end(value.as_bytes(), at) {
            at = end;
            continue;
        }
        let c = value[at..].chars().next().expect("character boundary");
        width += measured_rune(c, opts);
        at += c.len_utf8();
    }
    width
}
/// Actual source Width includes its distinct one-byte ASCII fast path.
/// Its existing cache does not change width for the immutable source-init options.
pub(crate) fn width(value: &str, opts: Options) -> usize {
    if value.len() == 1 && value.as_bytes()[0].is_ascii() {
        return if value == "\t" { opts.tab_width } else { 1 };
    }
    width_with_options(value, opts)
}

#[cfg(test)]
mod source_tests {
    use super::*;
    #[test]
    #[ignore = "requires actual original Go raw scalar planes via NODE_RUNE_GO_PLANES"]
    fn every_legal_scalar_matches_actual_original_go_planes() {
        let raw =
            std::fs::read(std::env::var_os("NODE_RUNE_GO_PLANES").expect("actual raw two planes"))
                .unwrap();
        let scalar_count = 0x110000usize - 0x800;
        assert_eq!(raw.len(), scalar_count * 2);
        let mut at = 0;
        for east in [false, true] {
            for cp in 0..=0x10ffff {
                let Some(c) = char::from_u32(cp) else {
                    continue;
                };
                assert_eq!(
                    rune_width(c, east),
                    usize::from(raw[at]),
                    "source scalar U+{cp:04X}, east_asian={east}, offset={at}"
                );
                at += 1;
            }
        }
        assert_eq!(at, raw.len());
    }
    #[test]
    #[ignore = "requires actual original twwidth cases/results via NODE_TWWIDTH_GO_CASES and NODE_TWWIDTH_GO_RESULTS"]
    fn original_rune_and_twwidth_context_vectors() {
        use std::collections::BTreeMap;
        let input =
            std::fs::read_to_string(std::env::var_os("NODE_TWWIDTH_GO_CASES").unwrap()).unwrap();
        let cases: serde_json::Value = serde_json::from_str(&input).unwrap();
        let result =
            std::fs::read_to_string(std::env::var_os("NODE_TWWIDTH_GO_RESULTS").unwrap()).unwrap();
        let mut observed = BTreeMap::new();
        for line in result.lines() {
            let row: serde_json::Value = serde_json::from_str(line).unwrap();
            assert!(
                observed
                    .insert(row["id"].as_str().unwrap().to_owned(), row)
                    .is_none()
            );
        }
        assert_eq!(observed.len(), cases.as_array().unwrap().len());
        assert_eq!(observed.len(), 72);
        for input in cases.as_array().unwrap() {
            let id = input["id"].as_str().unwrap();
            let row = observed.remove(id).unwrap();
            let text = input["text"].as_str().unwrap();
            assert_eq!(row["text"].as_str().unwrap(), text);
            let east = input["east_asian"].as_bool().unwrap();
            let opts = Options {
                east_asian_width: east,
                force_narrow_borders: input["force_narrow_borders"].as_bool().unwrap(),
                tab_width: row["tab_width"].as_u64().unwrap() as usize,
            };
            assert_eq!(row["east_asian"].as_bool().unwrap(), east);
            assert_eq!(
                row["force_narrow_borders"].as_bool().unwrap(),
                opts.force_narrow_borders
            );
            assert_eq!(
                opts.tab_width,
                input["tab_width"].as_u64().unwrap() as usize
            );
            let runes: Vec<_> = text.chars().map(|c| rune_width(c, east)).collect();
            let original: Vec<_> = row["rune_widths"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_u64().unwrap() as usize)
                .collect();
            assert_eq!(runes, original, "actual original Rune {id}");
            assert_eq!(
                width_with_options(text, opts),
                row["width_with_options"].as_u64().unwrap() as usize,
                "actual WidthWithOptions {id}"
            );
            assert_eq!(
                width(text, opts),
                row["width_entry"].as_u64().unwrap() as usize,
                "actual Width entry {id}"
            );
        }
        assert!(observed.is_empty());
    }
}
