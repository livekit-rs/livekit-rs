// Copyright IBM Corp. 2014, 2025. SPDX-License-Identifier: MPL-2.0
// Behavioral port of hashicorp/go-version v1.9.0 NewVersion/Compare only.
// See LICENSE-MPL-2.0 and SOURCE.json. No dependency on a Go runtime.
use std::cmp::Ordering;
#[derive(Debug, PartialEq, Eq)]
struct Version {
    segments: Vec<i64>,
    pre: String,
}
fn parse(text: &str) -> Option<Version> {
    let text = text.strip_prefix('v').unwrap_or(text);
    let (body, meta) = match text.split_once('+') {
        Some((a, b)) => (a, Some(b)),
        None => (text, None),
    };
    fn valid_parts(value: &str) -> bool {
        !value.is_empty()
            && value.split('.').all(|p| {
                !p.is_empty()
                    && p.bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'~')
            })
    }
    if meta.is_some_and(|v| !valid_parts(v)) {
        return None;
    }
    let end = body
        .bytes()
        .position(|b| !b.is_ascii_digit() && b != b'.')
        .unwrap_or(body.len());
    let number = &body[..end];
    if number.is_empty() {
        return None;
    }
    let mut segments: Vec<i64> = number
        .split('.')
        .map(|p| if p.is_empty() { None } else { p.parse().ok() })
        .collect::<Option<_>>()?;
    while segments.len() < 3 {
        segments.push(0);
    }
    let suffix = &body[end..];
    let pre = if suffix.is_empty() {
        ""
    } else {
        if suffix == "-" {
            "-"
        } else {
            suffix.strip_prefix('-').unwrap_or(suffix)
        }
    };
    if !suffix.is_empty() && !valid_parts(pre) {
        return None;
    }
    // A prerelease starting with a number requires the separator, unlike letters.
    if !suffix.is_empty() && !suffix.starts_with('-') && pre.as_bytes()[0].is_ascii_digit() {
        return None;
    }
    Some(Version {
        segments,
        pre: pre.into(),
    })
}
fn compare_part(a: &str, b: &str) -> Ordering {
    if a == b {
        return Ordering::Equal;
    }
    let ai = a.parse::<i64>();
    let bi = b.parse::<i64>();
    if a.is_empty() {
        return if bi.is_ok() {
            Ordering::Less
        } else {
            Ordering::Greater
        };
    }
    if b.is_empty() {
        return if ai.is_ok() {
            Ordering::Greater
        } else {
            Ordering::Less
        };
    }
    match (ai, bi) {
        (Ok(_), Err(_)) => Ordering::Less,
        (Err(_), Ok(_)) => Ordering::Greater,
        (Err(_), Err(_)) if a > b => Ordering::Greater,
        (Ok(a), Ok(b)) if a > b => Ordering::Greater,
        _ => Ordering::Less,
    }
}
fn compare(a: &Version, b: &Version) -> Ordering {
    if a.segments == b.segments {
        match (a.pre.is_empty(), b.pre.is_empty()) {
            (true, true) => return Ordering::Equal,
            (true, false) => return Ordering::Greater,
            (false, true) => return Ordering::Less,
            _ => {}
        }
        let av: Vec<_> = a.pre.split('.').collect();
        let bv: Vec<_> = b.pre.split('.').collect();
        for i in 0..av.len().max(bv.len()) {
            let c = compare_part(
                av.get(i).copied().unwrap_or(""),
                bv.get(i).copied().unwrap_or(""),
            );
            if c != Ordering::Equal {
                return c;
            }
        }
        return Ordering::Equal;
    }
    for i in 0..a.segments.len().max(b.segments.len()) {
        let c = a
            .segments
            .get(i)
            .copied()
            .unwrap_or(0)
            .cmp(&b.segments.get(i).copied().unwrap_or(0));
        if c != Ordering::Equal {
            return c;
        }
    }
    // Go skips prerelease comparison when segment slice lengths differ.
    Ordering::Equal
}
pub(super) fn should_update(existing: &str, current: &str) -> Result<bool, &'static str> {
    let old = parse(if existing.is_empty() {
        "0.0.0"
    } else {
        existing
    })
    .ok_or("malformed stored livekit_version")?;
    let new = parse(current).ok_or("malformed server version")?;
    Ok(compare(&new, &old) == Ordering::Greater)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fixed_version_comparison_keeps_upstream_short_extended_and_prerelease_rules() {
        for (old, current, want) in [
            ("", "1.13.10", true),
            ("1.13.9", "1.13.10", true),
            ("v1.13.10", "1.13.10", false),
            ("1.13.10-rc1", "1.13.10", true),
            ("1.13.10.0-rc1", "1.13.10", false),
            ("1.13.11", "1.13.10", false),
            ("1.13", "1.13.0", false),
        ] {
            assert_eq!(should_update(old, current), Ok(want), "{old} -> {current}");
        }
        assert!(should_update("not a version", "1.13.10").is_err());
    }
}

#[cfg(test)]
mod actual_go_oracle {
    use super::should_update;
    #[test]
    #[ignore = "requires coordinator-executed WORKERS_VERSION_GO_RESULTS fixed Go oracle"]
    fn all_sixty_actual_go_package_rows_match_validity_and_update() {
        let path = std::env::var("WORKERS_VERSION_GO_RESULTS")
            .expect("actual fixed Go oracle results path required");
        let bytes = std::fs::read(path).expect("read actual fixed Go oracle output");
        let rows: Vec<serde_json::Value> =
            serde_json::from_slice(&bytes).expect("actual Go JSON rows");
        assert_eq!(rows.len(), 60, "all frozen source vectors must be compared");
        for row in rows {
            let existing = row["existing"].as_str().expect("existing input");
            let current = row["current"].as_str().expect("current input");
            let valid = row["valid"].as_bool().expect("actual Go validity");
            let got = should_update(existing, current);
            assert_eq!(
                got.is_ok(),
                valid,
                "actual Go validity: {existing:?} -> {current:?}"
            );
            if valid {
                assert_eq!(
                    got.unwrap(),
                    row["update"].as_bool().expect("actual Go update result"),
                    "actual Go comparison: {existing:?} -> {current:?}"
                );
            }
        }
    }
}
