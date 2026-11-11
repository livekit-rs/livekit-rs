//! Text-level SDP helpers. str0m owns SDP generation; these helpers only read
//! attributes str0m does not expose and apply the few rewrites LiveKit clients
//! depend on (opus fmtp §9.1, `sprop-stereo` §9.2, msid §10, idle sections).
//!
//! Everything works on whole lines and preserves each line's own ending (CRLF from
//! str0m, but LF-only input is tolerated). Sections start at a line beginning with
//! `m=`; everything before the first one is the session part.

/// Split into (session part, media sections). Each slice keeps its line endings, so
/// concatenating the parts reproduces the input exactly.
pub(crate) fn split_sections(sdp: &str) -> (&str, Vec<&str>) {
    let mut starts = Vec::new();
    let mut pos = 0;
    for line in sdp.split_inclusive('\n') {
        if line.starts_with("m=") {
            starts.push(pos);
        }
        pos += line.len();
    }
    let Some(&first) = starts.first() else {
        return (sdp, Vec::new());
    };
    let mut sections = Vec::with_capacity(starts.len());
    for (i, &s) in starts.iter().enumerate() {
        let end = starts.get(i + 1).copied().unwrap_or(sdp.len());
        sections.push(&sdp[s..end]);
    }
    (&sdp[..first], sections)
}

/// Line ending used by a chunk of SDP ("\r\n" unless the text is LF-only).
fn eol(text: &str) -> &'static str {
    if text.contains("\r\n") || !text.contains('\n') {
        "\r\n"
    } else {
        "\n"
    }
}

fn strip_eol(line: &str) -> &str {
    line.trim_end_matches(['\r', '\n'])
}

/// Pion includes both component numbers for each candidate it embeds in an
/// SDP answer. The second line shares the first candidate's address and priority.
pub(crate) fn pair_inline_ice_components(sdp: &str) -> String {
    let mut answer = String::with_capacity(sdp.len());
    for line in sdp.split_inclusive('\n') {
        answer.push_str(line);
        let Some((foundation, rest)) = line.split_once(' ') else {
            continue;
        };
        if foundation.starts_with("a=candidate:")
            && let Some(rest) = rest.strip_prefix("1 ")
        {
            if !line.ends_with('\n') {
                answer.push_str(eol(sdp));
            }
            answer.push_str(foundation);
            answer.push_str(" 2 ");
            answer.push_str(rest);
        }
    }
    answer
}

/// Value of the first `a=<name>:` attribute in a section.
pub(crate) fn attr<'a>(section: &'a str, name: &str) -> Option<&'a str> {
    section.split_inclusive('\n').find_map(|l| {
        let l = strip_eol(l);
        l.strip_prefix("a=")?.strip_prefix(name)?.strip_prefix(':')
    })
}

/// `a=mid:` of a section.
pub(crate) fn section_mid(section: &str) -> Option<&str> {
    attr(section, "mid").map(str::trim)
}

/// Media type of a section ("audio", "video", "application").
pub(crate) fn section_kind(section: &str) -> Option<&str> {
    let first = strip_eol(section.split_inclusive('\n').next()?);
    first.strip_prefix("m=")?.split(' ').next()
}

/// `(stream id, track id)` from `a=msid:`; falls back to the first `a=ssrc:<n> msid:`.
pub(crate) fn section_msid(section: &str) -> Option<(String, String)> {
    let parse = |v: &str| {
        let mut it = v.split_whitespace();
        let s = it.next()?.to_string();
        let t = it.next().unwrap_or("").to_string();
        Some((s, t))
    };
    if let Some(v) = attr(section, "msid") {
        return parse(v);
    }
    section.split_inclusive('\n').find_map(|l| {
        let v = strip_eol(l).strip_prefix("a=ssrc:")?;
        let (_, rest) = v.split_once(' ')?;
        parse(rest.strip_prefix("msid:")?)
    })
}

/// Rids of the `send` direction of `a=simulcast` (offer order, `~` stripped, every
/// alternative of a group kept in order).
pub(crate) fn simulcast_send_rids(section: &str) -> Vec<String> {
    let Some(v) = attr(section, "simulcast") else {
        return Vec::new();
    };
    let mut tokens = v.split_whitespace();
    while let Some(t) = tokens.next() {
        let Some(list) = tokens.next() else { break };
        if t == "send" {
            return list
                .split([';', ','])
                .map(|r| r.trim_start_matches('~').to_string())
                .filter(|r| !r.is_empty())
                .collect();
        }
    }
    Vec::new()
}

/// Session-level (or first media-level) `a=ice-ufrag`.
pub(crate) fn ice_ufrag(sdp: &str) -> Option<&str> {
    attr(sdp, "ice-ufrag").map(str::trim)
}

/// Payload types whose `a=rtpmap` encoding name matches `codec` (case-insensitive).
fn payload_types_for(section: &str, codec: &str) -> Vec<String> {
    section
        .split_inclusive('\n')
        .filter_map(|l| {
            let v = strip_eol(l).strip_prefix("a=rtpmap:")?;
            let (pt, enc) = v.split_once(' ')?;
            let name = enc.split('/').next()?;
            name.eq_ignore_ascii_case(codec).then(|| pt.to_string())
        })
        .collect()
}

/// Rebuild a section line by line; `f` returns `None` to drop a line or the
/// replacement text without line ending.
fn map_lines(section: &str, mut f: impl FnMut(&str) -> Option<String>) -> String {
    let mut out = String::with_capacity(section.len() + 64);
    for line in section.split_inclusive('\n') {
        let body = strip_eol(line);
        let ending = &line[body.len()..];
        if let Some(new) = f(body) {
            out.push_str(&new);
            out.push_str(ending);
        }
    }
    out
}

/// Publisher answer opus fmtp munging (participant_sdp.go:325-405, §9.1):
/// `;usedtx=1` unless DTX is disabled (else removed), `;stereo=1;maxaveragebitrate=510000`
/// when stereo (else `;stereo=1` removed).
pub(crate) fn munge_opus_answer(section: &str, stereo: bool, disable_dtx: bool) -> String {
    let pts = payload_types_for(section, "opus");
    if pts.is_empty() {
        return section.to_string();
    }
    let mut seen = false;
    let out = map_lines(section, |l| {
        let Some(v) = l.strip_prefix("a=fmtp:") else {
            return Some(l.to_string());
        };
        let Some((pt, params)) = v.split_once(' ') else {
            return Some(l.to_string());
        };
        if !pts.iter().any(|p| p == pt) {
            return Some(l.to_string());
        }
        seen = true;
        let mut p = params.to_string();
        if !disable_dtx {
            if !has_param(&p, "usedtx", "1") {
                p.push_str(";usedtx=1");
            }
        } else {
            p = remove_param(&p, "usedtx");
        }
        if stereo {
            if !has_param(&p, "stereo", "1") {
                p.push_str(";stereo=1");
            }
            if !has_key(&p, "maxaveragebitrate") {
                p.push_str(";maxaveragebitrate=510000");
            }
        } else {
            p = remove_param(&p, "stereo");
        }
        Some(format!("a=fmtp:{pt} {}", p.trim_start_matches(';')))
    });
    if seen {
        return out;
    }
    // No fmtp line for opus: add one after the rtpmap (only if there is something to
    // say).
    let mut params = Vec::new();
    if !disable_dtx {
        params.push("usedtx=1");
    }
    if stereo {
        params.push("stereo=1;maxaveragebitrate=510000");
    }
    if params.is_empty() {
        return out;
    }
    insert_after_rtpmap(
        &out,
        &pts[0],
        &format!("a=fmtp:{} {}", pts[0], params.join(";")),
    )
}

const VIDEO_BITRATE_HINTS: [&str; 3] = [
    "x-google-start-bitrate",
    "x-google-min-bitrate",
    "x-google-max-bitrate",
];

/// Preserve browser encoder bitrate hints on the same negotiated video section.
/// str0m's typed FormatParams drops these extension parameters.
pub(crate) fn preserve_video_bitrate_hints(answer: &str, offer: &str) -> String {
    merge_video_bitrate_hints(answer, offer, false)
}

/// Keep only negotiated codec attributes and the MID, never the prior SDP's
/// credentials, candidates, media identifiers or transport data.
pub(crate) fn capture_video_bitrate_hints(answer: &str) -> Option<String> {
    let mid = section_mid(answer)?;
    let mut fragment = format!("m=video 9 X\r\na=mid:{mid}\r\n");
    for line in answer.lines().filter(|line| line.starts_with("a=fmtp:")) {
        let (pt, parameters) = line.strip_prefix("a=fmtp:")?.split_once(' ')?;
        if !split_params(parameters).any(|(name, _)| VIDEO_BITRATE_HINTS.contains(&name)) {
            continue;
        }
        let mapping = answer.lines().find(|line| {
            line.strip_prefix("a=rtpmap:")
                .and_then(|mapping| mapping.split_once(' '))
                .is_some_and(|(number, _)| number == pt)
        });
        if let Some(mapping) = mapping {
            fragment.push_str(mapping);
            fragment.push_str("\r\n");
            fragment.push_str(line);
            fragment.push_str("\r\n");
        }
    }
    fragment.contains("a=rtpmap:").then_some(fragment)
}

pub(crate) fn restore_inactive_video_bitrate_hints(answer: &str, previous: &str) -> String {
    merge_video_bitrate_hints(answer, previous, true)
}

fn merge_video_bitrate_hints(answer: &str, offer: &str, require_same_profile: bool) -> String {
    if section_kind(answer) != Some("video")
        || section_kind(offer) != Some("video")
        || section_mid(answer) != section_mid(offer)
        || section_mid(answer).is_none()
    {
        return answer.to_string();
    }
    let mut out = answer.to_string();
    for line in answer.lines() {
        let Some(mapping) = line.strip_prefix("a=rtpmap:") else {
            continue;
        };
        let Some((pt, encoding)) = mapping.split_once(' ') else {
            continue;
        };
        // A payload number alone is insufficient: codec remapping may have
        // selected a different encoding for this m-line.
        let same_codec = offer.lines().any(|line| {
            line.strip_prefix("a=rtpmap:")
                .and_then(|v| v.split_once(' '))
                .is_some_and(|(offered_pt, offered_encoding)| {
                    pt == offered_pt
                        && encoding
                            .trim()
                            .eq_ignore_ascii_case(offered_encoding.trim())
                })
        });
        if !same_codec {
            continue;
        }
        let offered = offer
            .lines()
            .filter_map(|line| line.strip_prefix("a=fmtp:")?.split_once(' '))
            .filter(|(offered_pt, _)| *offered_pt == pt)
            .flat_map(|(_, params)| split_params(params))
            .collect::<Vec<_>>();
        if require_same_profile {
            let mut previous = offered
                .iter()
                .copied()
                .filter(|(name, _)| !VIDEO_BITRATE_HINTS.contains(name))
                .collect::<Vec<_>>();
            let mut current = answer
                .lines()
                .filter_map(|line| line.strip_prefix("a=fmtp:")?.split_once(' '))
                .filter(|(answer_pt, _)| *answer_pt == pt)
                .flat_map(|(_, params)| split_params(params))
                .filter(|(name, _)| !VIDEO_BITRATE_HINTS.contains(name))
                .collect::<Vec<_>>();
            previous.sort_unstable();
            current.sort_unstable();
            if current != previous {
                continue;
            }
        }
        let mut additions = Vec::new();
        for key in VIDEO_BITRATE_HINTS {
            if let Some((_, value)) = offered.iter().find(|(name, value)| {
                *name == key
                    && !value.is_empty()
                    && value.bytes().all(|c| c.is_ascii_digit())
                    && value.parse::<u32>().is_ok()
            }) {
                additions.push((key, *value));
            }
        }
        if additions.is_empty() {
            continue;
        }
        let mut seen = false;
        out = map_lines(&out, |line| {
            let Some((answer_pt, params)) =
                line.strip_prefix("a=fmtp:").and_then(|v| v.split_once(' '))
            else {
                return Some(line.to_string());
            };
            if answer_pt != pt {
                return Some(line.to_string());
            }
            seen = true;
            let mut config = params.to_string();
            for (key, value) in &additions {
                if !has_key(&config, key) {
                    if !config.is_empty() {
                        config.push(';');
                    }
                    config.push_str(key);
                    config.push('=');
                    config.push_str(value);
                }
            }
            Some(format!("a=fmtp:{pt} {config}"))
        });
        if !seen {
            let params = additions
                .iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect::<Vec<_>>()
                .join(";");
            out = insert_after_rtpmap(&out, pt, &format!("a=fmtp:{pt} {params}"));
        }
    }
    out
}

/// Subscriber offer: `;sprop-stereo=1` on opus fmtp for stereo sources (§9.2).
pub(crate) fn add_opus_sprop_stereo(section: &str) -> String {
    let pts = payload_types_for(section, "opus");
    if pts.is_empty() {
        return section.to_string();
    }
    let mut seen = false;
    let out = map_lines(section, |l| {
        if let Some(v) = l.strip_prefix("a=fmtp:")
            && let Some((pt, params)) = v.split_once(' ')
            && pts.iter().any(|p| p == pt)
        {
            seen = true;
            if !has_key(params, "sprop-stereo") {
                return Some(format!("{l};sprop-stereo=1"));
            }
        }
        Some(l.to_string())
    });
    if seen {
        out
    } else {
        insert_after_rtpmap(&out, &pts[0], &format!("a=fmtp:{} sprop-stereo=1", pts[0]))
    }
}

fn insert_after_rtpmap(section: &str, pt: &str, new_line: &str) -> String {
    let nl = eol(section);
    let prefix = format!("a=rtpmap:{pt} ");
    let mut done = false;
    let mut out = String::with_capacity(section.len() + new_line.len() + 2);
    for line in section.split_inclusive('\n') {
        out.push_str(line);
        if !done && strip_eol(line).starts_with(&prefix) {
            if !line.ends_with('\n') {
                out.push_str(nl);
            }
            out.push_str(new_line);
            out.push_str(nl);
            done = true;
        }
    }
    out
}

fn split_params(params: &str) -> impl Iterator<Item = (&str, &str)> {
    params
        .split(';')
        .filter(|s| !s.trim().is_empty())
        .map(|kv| {
            let kv = kv.trim();
            kv.split_once('=').unwrap_or((kv, ""))
        })
}

fn has_param(params: &str, key: &str, value: &str) -> bool {
    split_params(params).any(|(k, v)| k == key && v == value)
}

fn has_key(params: &str, key: &str) -> bool {
    split_params(params).any(|(k, _)| k == key)
}

fn remove_param(params: &str, key: &str) -> String {
    params
        .split(';')
        .filter(|kv| {
            let kv = kv.trim();
            !kv.is_empty() && kv.split_once('=').map(|(k, _)| k).unwrap_or(kv) != key
        })
        .collect::<Vec<_>>()
        .join(";")
}

/// Rewrite the identity of a send section: `a=msid:<stream> <track>` (inserted after
/// the direction attribute when absent) and every `a=ssrc:<n> msid:` line.
pub(crate) fn rewrite_msid(section: &str, stream_id: &str, track_id: &str) -> String {
    let msid = format!("{stream_id} {track_id}");
    let mut had_msid = false;
    let out = map_lines(section, |l| {
        if l.starts_with("a=msid:") {
            had_msid = true;
            return Some(format!("a=msid:{msid}"));
        }
        if let Some(v) = l.strip_prefix("a=ssrc:")
            && let Some((ssrc, rest)) = v.split_once(' ')
            && rest.starts_with("msid:")
        {
            return Some(format!("a=ssrc:{ssrc} msid:{msid}"));
        }
        Some(l.to_string())
    });
    if had_msid {
        return out;
    }
    let nl = eol(section);
    let mut inserted = false;
    let mut res = String::with_capacity(out.len() + msid.len() + 10);
    for line in out.split_inclusive('\n') {
        res.push_str(line);
        let body = strip_eol(line);
        if !inserted
            && matches!(
                body,
                "a=sendonly" | "a=sendrecv" | "a=recvonly" | "a=inactive"
            )
        {
            if !line.ends_with('\n') {
                res.push_str(nl);
            }
            res.push_str("a=msid:");
            res.push_str(&msid);
            res.push_str(nl);
            inserted = true;
        }
    }
    res
}

/// Preserve an unassigned single-PC section's negotiated send direction, as the
/// official server does, while withholding a track identity until one is bound.
pub(crate) fn unassigned_send_section(section: &str) -> String {
    map_lines(section, |line| {
        if line.starts_with("a=msid:")
            || line.starts_with("a=ssrc:")
            || line.starts_with("a=ssrc-group:")
        {
            None
        } else {
            Some(line.to_string())
        }
    })
}

/// Retire a send section: direction `a=inactive`, no msid or SSRC lines.
pub(crate) fn make_inactive(section: &str) -> String {
    map_lines(section, |l| match l {
        "a=sendonly" | "a=sendrecv" => Some("a=inactive".to_string()),
        _ if l.starts_with("a=msid:")
            || l.starts_with("a=ssrc:")
            || l.starts_with("a=ssrc-group:") =>
        {
            None
        }
        _ => Some(l.to_string()),
    })
}

/// Direction attribute of a section, if any.
pub(crate) fn section_direction(section: &str) -> Option<&str> {
    section
        .split_inclusive('\n')
        .map(strip_eol)
        .find_map(|l| match l {
            "a=sendonly" => Some("sendonly"),
            "a=recvonly" => Some("recvonly"),
            "a=sendrecv" => Some("sendrecv"),
            "a=inactive" => Some("inactive"),
            _ => None,
        })
}

/// Apply `f(mid, section)` to every media section; `None` keeps the section.
pub(crate) fn map_sections(
    sdp: &str,
    mut f: impl FnMut(Option<&str>, &str) -> Option<String>,
) -> String {
    let (head, sections) = split_sections(sdp);
    let mut out = String::with_capacity(sdp.len() + 256);
    out.push_str(head);
    for s in sections {
        match f(section_mid(s), s) {
            Some(new) => out.push_str(&new),
            None => out.push_str(s),
        }
    }
    out
}

/// `(mid, kind)` of every media section in order.
pub(crate) fn mids(sdp: &str) -> Vec<(String, String)> {
    split_sections(sdp)
        .1
        .into_iter()
        .filter_map(|s| Some((section_mid(s)?.to_string(), section_kind(s)?.to_string())))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const ANSWER: &str = "v=0\r\n\
o=str0m-0.24.1 1 2 IN IP4 0.0.0.0\r\n\
s=-\r\n\
t=0 0\r\n\
a=group:BUNDLE 0 1\r\n\
a=ice-ufrag:abcd\r\n\
m=audio 9 UDP/TLS/RTP/SAVPF 111\r\n\
c=IN IP4 0.0.0.0\r\n\
a=mid:0\r\n\
a=recvonly\r\n\
a=msid:x y\r\n\
a=rtpmap:111 opus/48000/2\r\n\
a=fmtp:111 minptime=10;useinbandfec=1\r\n\
m=video 9 UDP/TLS/RTP/SAVPF 96\r\n\
c=IN IP4 0.0.0.0\r\n\
a=mid:1\r\n\
a=sendonly\r\n\
a=msid:rnd1 rnd2\r\n\
a=rtpmap:96 VP8/90000\r\n\
a=ssrc:1234 cname:foo\r\n\
a=ssrc:1234 msid:rnd1 rnd2\r\n\
a=ssrc-group:FID 1234 5678\r\n";

    #[test]
    fn video_answer_preserves_same_mid_negotiated_numeric_bitrate_hints() {
        let offered = "m=video 9 X 96 97\r\na=mid:8\r\na=sendonly\r\na=rtpmap:96 VP8/90000\r\na=fmtp:96 x-google-start-bitrate=549;x-google-min-bitrate=30;x-google-max-bitrate=1000;unnegotiated=1\r\na=rtpmap:97 rtx/90000\r\na=fmtp:97 apt=96\r\n";
        let answer = "m=video 9 X 96 97\r\na=mid:8\r\na=recvonly\r\na=rtpmap:96 VP8/90000\r\na=rtpmap:97 rtx/90000\r\na=fmtp:97 apt=96\r\n";
        let result = preserve_video_bitrate_hints(answer, offered);
        assert!(result.contains("a=fmtp:96 x-google-start-bitrate=549;x-google-min-bitrate=30;x-google-max-bitrate=1000\r\n"));
        assert!(result.contains("a=fmtp:97 apt=96\r\n"));
        assert!(!result.contains("unnegotiated"));
        assert_eq!(preserve_video_bitrate_hints(&result, offered), result);
        assert_eq!(
            preserve_video_bitrate_hints(&answer.replace("a=mid:8", "a=mid:3"), offered),
            answer.replace("a=mid:8", "a=mid:3")
        );
        assert_eq!(
            preserve_video_bitrate_hints(&answer.replace("VP8/90000", "VP9/90000"), offered),
            answer.replace("VP8/90000", "VP9/90000")
        );
        assert_eq!(
            preserve_video_bitrate_hints(&answer.replace("96", "98"), offered),
            answer.replace("96", "98")
        );
    }

    #[test]
    fn inactive_hint_restore_requires_same_negotiated_av1_profile() {
        let active = "m=video 9 X 45\r\na=mid:7\r\na=recvonly\r\na=ice-pwd:must-not-copy\r\na=rtpmap:45 AV1/90000\r\na=fmtp:45 profile=0;tier=0;level-idx=5;x-google-start-bitrate=1000\r\n";
        let stored = capture_video_bitrate_hints(active).unwrap();
        assert!(!stored.contains("must-not-copy"));
        let inactive = "m=video 9 X 45\r\na=mid:7\r\na=inactive\r\na=rtpmap:45 AV1/90000\r\na=fmtp:45 level-idx=5;profile=0;tier=0\r\n";
        assert!(
            restore_inactive_video_bitrate_hints(inactive, &stored)
                .contains("x-google-start-bitrate=1000")
        );
        let different_profile = inactive.replace("profile=0", "profile=1");
        assert_eq!(
            restore_inactive_video_bitrate_hints(&different_profile, &stored),
            different_profile
        );
        let different_mid = inactive.replace("a=mid:7", "a=mid:8");
        assert_eq!(
            restore_inactive_video_bitrate_hints(&different_mid, &stored),
            different_mid
        );
    }

    #[test]
    fn video_hint_echo_rejects_invalid_numbers_and_retains_negotiated_fmtp() {
        let offer = "m=video 9 X 96\na=mid:8\na=rtpmap:96 VP8/90000\na=fmtp:96 x-google-start-bitrate=bogus;x-google-min-bitrate=-1;x-google-max-bitrate=4294967296\n";
        let answer = "m=video 9 X 96\na=mid:8\na=rtpmap:96 VP8/90000\na=fmtp:96 negotiated=1;x-google-start-bitrate=300\n";
        assert_eq!(preserve_video_bitrate_hints(answer, offer), answer);
        let valid = offer
            .replace("bogus", "549")
            .replace("-1", "30")
            .replace("4294967296", "1000");
        let result = preserve_video_bitrate_hints(answer, &valid);
        assert!(result.contains("negotiated=1;x-google-start-bitrate=300;x-google-min-bitrate=30;x-google-max-bitrate=1000"));
    }

    #[test]
    fn unassigned_send_sections_keep_direction_without_phantom_track_identity() {
        let input = "m=video 9 X 96\r\na=mid:3\r\na=sendonly\r\na=msid:random random\r\na=ssrc:1 cname:random\r\na=ssrc-group:FID 1 2\r\na=rtpmap:96 VP8/90000\r\n";
        let result = unassigned_send_section(input);
        assert!(result.contains("a=sendonly\r\n"));
        assert!(!result.contains("a=msid:"));
        assert!(!result.contains("a=ssrc"));
        assert!(make_inactive(&result).contains("a=inactive\r\n"));
    }

    #[test]
    fn split_round_trips() {
        let (head, secs) = split_sections(ANSWER);
        assert!(head.starts_with("v=0\r\n"));
        assert_eq!(secs.len(), 2);
        let joined: String = std::iter::once(head).chain(secs.iter().copied()).collect();
        assert_eq!(joined, ANSWER);
        assert_eq!(section_mid(secs[0]), Some("0"));
        assert_eq!(section_kind(secs[1]), Some("video"));
        assert_eq!(
            mids(ANSWER),
            vec![
                ("0".to_string(), "audio".to_string()),
                ("1".to_string(), "video".to_string())
            ]
        );
        assert_eq!(ice_ufrag(ANSWER), Some("abcd"));
        assert_eq!(section_direction(secs[1]), Some("sendonly"));
    }

    #[test]
    fn opus_munging_dtx_and_stereo() {
        let (_, secs) = split_sections(ANSWER);
        let m = munge_opus_answer(secs[0], true, false);
        assert!(m.contains(
            "a=fmtp:111 minptime=10;useinbandfec=1;usedtx=1;stereo=1;maxaveragebitrate=510000\r\n"
        ));
        let m2 = munge_opus_answer(&m, false, true);
        assert!(m2.contains("a=fmtp:111 minptime=10;useinbandfec=1;maxaveragebitrate=510000\r\n"));
        // idempotent
        assert_eq!(munge_opus_answer(&m, true, false), m);
        // video sections are untouched
        assert_eq!(munge_opus_answer(secs[1], true, false), secs[1]);
    }

    #[test]
    fn opus_munging_adds_missing_fmtp() {
        let s = "m=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=mid:0\r\na=rtpmap:111 opus/48000/2\r\na=rtcp-fb:111 nack\r\n";
        let m = munge_opus_answer(s, false, false);
        assert_eq!(
            m,
            "m=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=mid:0\r\na=rtpmap:111 opus/48000/2\r\na=fmtp:111 usedtx=1\r\na=rtcp-fb:111 nack\r\n"
        );
    }

    #[test]
    fn sprop_stereo() {
        let (_, secs) = split_sections(ANSWER);
        let m = add_opus_sprop_stereo(secs[0]);
        assert!(m.contains("a=fmtp:111 minptime=10;useinbandfec=1;sprop-stereo=1\r\n"));
        assert_eq!(add_opus_sprop_stereo(&m), m);
    }

    #[test]
    fn msid_rewrite_covers_ssrc_lines() {
        let out = map_sections(ANSWER, |mid, s| {
            (mid == Some("1")).then(|| rewrite_msid(s, "PA_a|TR_b", "TR_b"))
        });
        assert!(out.contains("a=msid:PA_a|TR_b TR_b\r\n"));
        assert!(out.contains("a=ssrc:1234 msid:PA_a|TR_b TR_b\r\n"));
        assert!(out.contains("a=ssrc:1234 cname:foo\r\n"));
        assert!(!out.contains("rnd1"));
        // audio section untouched
        assert!(out.contains("a=msid:x y\r\n"));
    }

    #[test]
    fn msid_inserted_when_absent() {
        let s = "m=video 9 X 96\r\na=mid:1\r\na=sendonly\r\na=rtpmap:96 VP8/90000\r\n";
        assert_eq!(
            rewrite_msid(s, "S", "T"),
            "m=video 9 X 96\r\na=mid:1\r\na=sendonly\r\na=msid:S T\r\na=rtpmap:96 VP8/90000\r\n"
        );
    }

    #[test]
    fn inactive_strips_identity() {
        let (_, secs) = split_sections(ANSWER);
        let m = make_inactive(secs[1]);
        assert!(m.contains("a=inactive\r\n"));
        assert!(!m.contains("a=sendonly"));
        assert!(!m.contains("msid"));
        assert!(!m.contains("a=ssrc"));
        assert!(m.contains("a=rtpmap:96 VP8/90000\r\n"));
    }

    #[test]
    fn simulcast_and_msid_parsing() {
        let s = "m=video 9 X 96\r\na=mid:2\r\na=msid:stream track-1\r\na=rid:q send\r\na=rid:h send\r\na=rid:f send\r\na=simulcast:send q;~h;f\r\n";
        assert_eq!(
            simulcast_send_rids(s),
            vec!["q".to_string(), "h".into(), "f".into()]
        );
        assert_eq!(section_msid(s), Some(("stream".into(), "track-1".into())));
        let r = "m=video 9 X 96\r\na=simulcast:recv a;b send c\r\n";
        assert_eq!(simulcast_send_rids(r), vec!["c".to_string()]);
        let ssrc_only = "m=audio 9 X 111\r\na=ssrc:1 msid:- abc\r\n";
        assert_eq!(section_msid(ssrc_only), Some(("-".into(), "abc".into())));
    }

    #[test]
    fn lf_only_input_is_preserved() {
        let s = "v=0\nm=audio 9 X 111\na=mid:0\na=rtpmap:111 opus/48000/2\n";
        let out = map_sections(s, |_, sec| Some(munge_opus_answer(sec, false, false)));
        assert_eq!(
            out,
            "v=0\nm=audio 9 X 111\na=mid:0\na=rtpmap:111 opus/48000/2\na=fmtp:111 usedtx=1\n"
        );
    }
}

/// The LiveKit SDK selects its encoder from the server's answer, using the
/// primary codec agreed through AddTrack (its offer itself can still list VP8
/// first). Reorder only negotiated payload types; their attributes stay intact.
pub(crate) fn prioritize_publisher_codec(answer: &str, mime: &str) -> String {
    let Some((kind, codec)) = mime.split_once('/') else {
        return answer.to_string();
    };
    if !section_kind(answer).is_some_and(|section| section.eq_ignore_ascii_case(kind)) {
        return answer.to_string();
    }
    let preferred: Vec<&str> = answer
        .lines()
        .filter_map(|line| {
            let value = line.strip_prefix("a=rtpmap:")?;
            let (pt, encoding) = value.split_once(' ')?;
            encoding
                .split('/')
                .next()?
                .eq_ignore_ascii_case(codec)
                .then_some(pt)
        })
        .collect();
    if preferred.is_empty() {
        return answer.to_string();
    }
    let repairs: Vec<&str> = answer
        .lines()
        .filter_map(|line| {
            let (pt, params) = line.strip_prefix("a=fmtp:")?.split_once(' ')?;
            params
                .split(';')
                .any(|param| {
                    param
                        .trim()
                        .strip_prefix("apt=")
                        .is_some_and(|apt| preferred.contains(&apt))
                })
                .then_some(pt)
        })
        .collect();
    let Some((answer_line, tail)) = answer.split_once('\n') else {
        return answer.to_string();
    };
    let parts: Vec<_> = answer_line.split_whitespace().collect();
    if parts.len() < 4 {
        return answer.to_string();
    }
    let mut payloads = parts[3..].to_vec();
    payloads.sort_by_key(|pt| {
        if preferred.contains(pt) {
            0
        } else if repairs.contains(pt) {
            1
        } else {
            2
        }
    });
    let mut result = parts[..3].join(" ");
    for pt in payloads {
        result.push(' ');
        result.push_str(pt);
    }
    result.push_str(eol(answer));
    result.push_str(tail);
    result
}

#[cfg(test)]
mod codec_order_tests {
    use super::*;
    #[test]
    fn prefer_negotiated_audio_red_without_changing_its_opus_association() {
        let audio = "m=audio 9 UDP/TLS/RTP/SAVPF 109 112 0\r\na=mid:4\r\na=recvonly\r\na=rtpmap:109 opus/48000/2\r\na=rtpmap:112 red/48000/2\r\na=fmtp:112 109/109\r\n";
        let preferred = prioritize_publisher_codec(audio, "audio/red");
        assert!(preferred.starts_with("m=audio 9 UDP/TLS/RTP/SAVPF 112 109 0\r\n"));
        assert_eq!(
            preferred.split_once('\n').unwrap().1,
            audio.split_once('\n').unwrap().1
        );
        assert_eq!(prioritize_publisher_codec(audio, "video/red"), audio);
        let plain = "m=audio 9 UDP/TLS/RTP/SAVPF 109\r\na=rtpmap:109 opus/48000/2\r\n";
        assert_eq!(prioritize_publisher_codec(plain, "audio/red"), plain);
    }

    #[test]
    fn add_track_primary_codec_wins_without_adding_codecs_or_changing_parameters() {
        let answer = "m=video 9 UDP/TLS/RTP/SAVPF 96 97 98 99\r\na=mid:8\r\na=recvonly\r\na=rtpmap:96 VP8/90000\r\na=rtpmap:98 VP9/90000\r\na=fmtp:99 apt=98\r\n";
        let out = prioritize_publisher_codec(answer, "video/vp9");
        assert!(out.starts_with("m=video 9 UDP/TLS/RTP/SAVPF 98 99 96 97\r\n"));
        assert_eq!(
            out.split_once('\n').unwrap().1,
            answer.split_once('\n').unwrap().1
        );
        assert_eq!(prioritize_publisher_codec(&out, "video/VP9"), out);
        assert_eq!(prioritize_publisher_codec(answer, "video/av1"), answer);
        assert_eq!(prioritize_publisher_codec(answer, "audio/opus"), answer);
    }
}
