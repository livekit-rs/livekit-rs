// Derived from LiveKit protocol/sdp/sdp.go (Apache-2.0), fixed source in SOURCE.json.
// Isolated implementation draft; not compiled or linked into the server.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Ice {
    pub ufrag: String,
    pub pwd: String,
    pub lite: Option<bool>,
    pub options: String,
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Media {
    pub info: String,
    pub mid: String,
    pub ice: Ice,
    pub candidates: Vec<String>,
    pub end_of_candidates: bool,
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Fragment {
    pub group: String,
    pub ice: Ice,
    pub media: Media,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    InvalidMedia,
    TooManyMedia,
    InvalidAttribute,
    MissingMedia,
    BundleMismatch { expected: String, got: String },
    MissingUfrag,
    MissingPwd,
    ConflictingUfrag,
    ConflictingPwd,
    IceLiteMismatch,
    IceOptionsMismatch,
    MissingMid,
    MissingBundle,
    MissingBundleMedia(String),
}
impl Fragment {
    pub fn parse(text: &str) -> Result<Self, Error> {
        let mut group = String::new();
        let mut ice = Ice::default();
        let mut media: Option<Media> = None;
        for input in text.split('\n') {
            let line = input.trim_end_matches([' ', '\r']);
            if line.is_empty() {
                continue;
            }
            if line.starts_with('m') {
                if line.len() < 3 || line.as_bytes()[1] != b'=' {
                    return Err(Error::InvalidMedia);
                }
                if media.is_some() {
                    return Err(Error::TooManyMedia);
                }
                media = Some(Media {
                    info: line[2..].to_owned(),
                    ..Default::default()
                });
            }
            if !line.starts_with('a') {
                continue;
            }
            if line.len() < 2 || line.as_bytes()[1] != b'=' {
                return Err(Error::InvalidAttribute);
            }
            let attr = &line[2..];
            let Some((key, value)) = attr.split_once(':') else {
                if attr == "ice-lite" {
                    media.as_mut().map_or(&mut ice, |m| &mut m.ice).lite = Some(true);
                }
                // Fixed Go Unmarshal ignores bare end-of-candidates here, too.
                continue;
            };
            match key {
                "group" => group = value.to_owned(),
                "ice-ufrag" => {
                    media.as_mut().map_or(&mut ice, |m| &mut m.ice).ufrag = value.to_owned()
                }
                "ice-pwd" => media.as_mut().map_or(&mut ice, |m| &mut m.ice).pwd = value.to_owned(),
                "ice-options" => {
                    media.as_mut().map_or(&mut ice, |m| &mut m.ice).options = value.to_owned()
                }
                "mid" => {
                    if let Some(m) = media.as_mut() {
                        m.mid = value.to_owned();
                    }
                }
                "candidate" => {
                    if let Some(m) = media.as_mut() {
                        m.candidates.push(value.to_owned());
                    }
                }
                "end-of-candidates" => {
                    if let Some(m) = media.as_mut() {
                        m.end_of_candidates = true;
                    }
                }
                _ => {}
            }
        }
        let media = media.ok_or(Error::MissingMedia)?;
        if !group.is_empty() {
            let ids: Vec<_> = group.split(' ').collect();
            if ids.len() > 1 && ids[0].eq_ignore_ascii_case("BUNDLE") && media.mid != ids[1] {
                return Err(Error::BundleMismatch {
                    expected: ids[1].to_owned(),
                    got: media.mid.clone(),
                });
            }
        }
        Ok(Self { group, ice, media })
    }
    pub fn credentials(&self) -> Result<(&str, &str), Error> {
        let ufrags: Vec<_> = [&self.ice.ufrag, &self.media.ice.ufrag]
            .into_iter()
            .filter(|s| !s.is_empty())
            .collect();
        let pwds: Vec<_> = [&self.ice.pwd, &self.media.ice.pwd]
            .into_iter()
            .filter(|s| !s.is_empty())
            .collect();
        let ufrag = ufrags.first().ok_or(Error::MissingUfrag)?;
        let pwd = pwds.first().ok_or(Error::MissingPwd)?;
        if ufrags.iter().any(|s| s != ufrag) {
            return Err(Error::ConflictingUfrag);
        }
        if pwds.iter().any(|s| s != pwd) {
            return Err(Error::ConflictingPwd);
        }
        Ok((ufrag, pwd))
    }
    pub fn marshal(&self) -> String {
        let mut out = String::new();
        let add = |out: &mut String, key: &str, value: &str| {
            out.push_str(key);
            out.push_str(value);
            out.push_str("\r\n");
        };
        if !self.group.is_empty() {
            add(&mut out, "a=group:", &self.group);
        }
        self.ice.marshal_into(&mut out);
        if !self.media.info.is_empty() {
            add(&mut out, "m=", &self.media.info);
        }
        if !self.media.mid.is_empty() {
            add(&mut out, "a=mid:", &self.media.mid);
        }
        self.media.ice.marshal_into(&mut out);
        for c in &self.media.candidates {
            add(&mut out, "a=candidate:", c);
        }
        if self.media.end_of_candidates {
            add(&mut out, "a=end-of-candidates", "");
        }
        out
    }
    /// Patch only an already validated full SDP, never use this as offer validation.
    /// Source patches all bundled media's candidate sets, not just matching mid.
    pub fn patch(&self, document: &mut Document) -> Result<(), Error> {
        check_ice(&self.ice, &document.session)?;
        if self.media.mid.is_empty()
            || !document
                .media
                .iter()
                .any(|lines| attr(lines, "mid") == Some(self.media.mid.as_str()))
        {
            return Err(Error::MissingMid);
        }
        for lines in &document.media {
            check_ice(&self.media.ice, lines)?;
        }
        if !self.ice.ufrag.is_empty() && !self.ice.pwd.is_empty() {
            replace_existing(&mut document.session, &self.ice);
        }
        for lines in &mut document.media {
            replace_existing(lines, &self.media.ice);
            lines.retain(|line| {
                !line.starts_with("a=candidate:")
                    && line != "a=end-of-candidates"
                    && !line.starts_with("a=end-of-candidates:")
            });
            lines.extend(
                self.media
                    .candidates
                    .iter()
                    .map(|c| format!("a=candidate:{c}")),
            );
            if self.media.end_of_candidates {
                lines.push("a=end-of-candidates".to_owned());
            }
        }
        Ok(())
    }
}
impl Ice {
    fn marshal_into(&self, out: &mut String) {
        if !self.ufrag.is_empty() {
            out.push_str(&format!("a=ice-ufrag:{}\r\n", self.ufrag));
        }
        if !self.pwd.is_empty() {
            out.push_str(&format!("a=ice-pwd:{}\r\n", self.pwd));
        }
        if self.lite == Some(true) {
            out.push_str("a=ice-lite\r\n");
        }
        if !self.options.is_empty() {
            out.push_str(&format!("a=ice-options:{}\r\n", self.options));
        }
    }
}
fn attr<'a>(lines: &'a [String], key: &str) -> Option<&'a str> {
    let prefix = format!("a={key}");
    lines.iter().find_map(|line| {
        line.strip_prefix(&prefix).and_then(|tail| {
            if tail.is_empty() {
                Some("")
            } else {
                tail.strip_prefix(':')
            }
        })
    })
}
fn check_ice(ice: &Ice, lines: &[String]) -> Result<(), Error> {
    if ice.lite.is_none() && ice.options.is_empty() {
        return Ok(());
    }
    for line in lines {
        if (line == "a=ice-lite" || line.starts_with("a=ice-lite:")) && ice.lite != Some(true) {
            return Err(Error::IceLiteMismatch);
        }
        if let Some(value) = line.strip_prefix("a=ice-options:")
            && !ice.options.is_empty()
            && ice.options != value
        {
            return Err(Error::IceOptionsMismatch);
        }
    }
    Ok(())
}
fn replace_existing(lines: &mut [String], ice: &Ice) {
    for line in lines {
        if line.starts_with("a=ice-ufrag:") && !ice.ufrag.is_empty() {
            *line = format!("a=ice-ufrag:{}", ice.ufrag);
        } else if line.starts_with("a=ice-pwd:") && !ice.pwd.is_empty() {
            *line = format!("a=ice-pwd:{}", ice.pwd);
        }
    }
}
/// Structural editing representation AFTER a real full SDP parser has accepted it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Document {
    pub session: Vec<String>,
    pub media: Vec<Vec<String>>,
}
impl Document {
    pub fn from_validated(text: &str) -> Self {
        let mut out = Self::default();
        for line in text
            .split('\n')
            .map(|s| s.trim_end_matches('\r'))
            .filter(|s| !s.is_empty())
        {
            if line.starts_with("m=") {
                out.media.push(Vec::new());
            }
            if let Some(media) = out.media.last_mut() {
                media.push(line.to_owned());
            } else {
                out.session.push(line.to_owned());
            }
        }
        out
    }
    pub fn marshal(&self) -> String {
        self.session
            .iter()
            .chain(self.media.iter().flatten())
            .map(|s| format!("{s}\r\n"))
            .collect()
    }
    pub fn extract_fragment(&self) -> Result<Fragment, Error> {
        let group = attr(&self.session, "group").ok_or(Error::MissingBundle)?;
        let ids: Vec<_> = group.split(' ').collect();
        // Actual GetBundleMid accepts case-insensitive BUNDLE and >=2 items.
        if ids.len() < 2 || !ids[0].eq_ignore_ascii_case("BUNDLE") {
            return Err(Error::MissingBundle);
        }
        let mid = ids[1];
        let lines = self
            .media
            .iter()
            .find(|lines| attr(lines, "mid") == Some(mid))
            .ok_or_else(|| Error::MissingBundleMedia(mid.to_owned()))?;
        let extract = |lines: &[String]| Ice {
            ufrag: attr(lines, "ice-ufrag").unwrap_or_default().to_owned(),
            pwd: attr(lines, "ice-pwd").unwrap_or_default().to_owned(),
            lite: attr(lines, "ice-lite").map(|_| true),
            options: attr(lines, "ice-options").unwrap_or_default().to_owned(),
        };
        Ok(Fragment {
            group: group.to_owned(),
            ice: extract(&self.session),
            media: Media {
                info: lines
                    .first()
                    .and_then(|s| s.strip_prefix("m="))
                    .unwrap_or_default()
                    .to_owned(),
                mid: mid.to_owned(),
                ice: extract(lines),
                candidates: lines
                    .iter()
                    .filter_map(|s| s.strip_prefix("a=candidate:").map(str::to_owned))
                    .collect(),
                end_of_candidates: attr(lines, "end-of-candidates").is_some(),
            },
        })
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidMedia => f.write_str("invalid media section"),
            Self::TooManyMedia => f.write_str("too many media sections"),
            Self::InvalidAttribute => f.write_str("invalid attribute"),
            Self::MissingMedia => f.write_str("missing media section"),
            Self::BundleMismatch { expected, got } => {
                write!(f, "bundle media mismatch, expected: {expected}, got: {got}")
            }
            Self::IceLiteMismatch => f.write_str("ice lite mismatch"),
            Self::IceOptionsMismatch => f.write_str("ice options mismatch"),
            Self::MissingMid => f.write_str("could not find media mid"),
            Self::MissingBundle => f.write_str("could not get bundle mid"),
            Self::MissingBundleMedia(mid) => write!(f, "could not find bundle media: {mid}"),
            // Credential errors are translated into ErrInvalidSDPFragment at the HTTP
            // trickle boundary; never emit internal Rust enum names to the client.
            Self::MissingUfrag
            | Self::MissingPwd
            | Self::ConflictingUfrag
            | Self::ConflictingPwd => f.write_str("invalid sdp fragment"),
        }
    }
}
