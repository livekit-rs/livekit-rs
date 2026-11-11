//! yaml.v3 `Node` tree and the event-to-node builder (`decode.go` `parser`).
//!
//! Ported from gopkg.in/yaml.v3 v3.0.1 (MIT / Apache-2.0, Copyright (c)
//! 2011-2019 Canonical Ltd). Nodes live in an arena so aliases can refer to an
//! anchored node that is still being built (`p.anchors` is filled before the
//! node's children are parsed, as upstream).

use std::collections::HashMap;

use super::Fail;
use super::parser::{Event, EventKind, ParseError, Parser, ScalarStyle};
use super::resolve::{MAP_TAG, MERGE_TAG, SEQ_TAG, STR_TAG, resolve, short_tag};

pub(crate) type Id = usize;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    Document,
    Sequence,
    Mapping,
    Scalar,
    Alias,
}

/// `yaml.Style` bits.
pub(crate) const TAGGED: u8 = 1;
pub(crate) const DOUBLE_QUOTED: u8 = 2;
pub(crate) const SINGLE_QUOTED: u8 = 4;
pub(crate) const LITERAL: u8 = 8;
pub(crate) const FOLDED: u8 = 16;
pub(crate) const FLOW: u8 = 32;

#[derive(Clone, Debug)]
pub(crate) struct Node {
    pub kind: Kind,
    pub style: u8,
    pub tag: String,
    pub value: String,
    pub anchor: String,
    pub alias: Option<Id>,
    pub content: Vec<Id>,
    pub line: usize,
    pub column: usize,
    /// Exact float of an already-validated protocol projection (no lexeme).
    pub float_bits: Option<u64>,
}

impl Node {
    pub fn new(kind: Kind, tag: &str, value: &str) -> Node {
        Node {
            kind,
            style: 0,
            tag: tag.to_string(),
            value: value.to_string(),
            anchor: String::new(),
            alias: None,
            content: Vec::new(),
            line: 0,
            column: 0,
            float_bits: None,
        }
    }
}

/// One parsed document stream position (all nodes of one document).
#[derive(Clone, Debug, Default)]
pub(crate) struct Doc {
    pub nodes: Vec<Node>,
}

impl Doc {
    pub fn node(&self, id: Id) -> &Node {
        &self.nodes[id]
    }

    pub fn push(&mut self, node: Node) -> Id {
        self.nodes.push(node);
        self.nodes.len() - 1
    }

    /// `(*Node).indicatedString`.
    pub fn indicated_string(&self, id: Id) -> bool {
        let n = self.node(id);
        n.kind == Kind::Scalar
            && (short_tag(&n.tag) == STR_TAG
                || (n.tag.is_empty() || n.tag == "!")
                    && n.style & (SINGLE_QUOTED | DOUBLE_QUOTED | LITERAL | FOLDED) != 0)
    }

    /// `(*Node).ShortTag`.
    pub fn short_tag(&self, id: Id) -> String {
        if self.indicated_string(id) {
            return STR_TAG.to_string();
        }
        let n = self.node(id);
        if n.tag.is_empty() || n.tag == "!" {
            return match n.kind {
                Kind::Mapping => MAP_TAG.to_string(),
                Kind::Sequence => SEQ_TAG.to_string(),
                Kind::Alias => match n.alias {
                    Some(a) => self.short_tag(a),
                    None => String::new(),
                },
                Kind::Scalar => resolve("", &n.value).map(|(t, _)| t).unwrap_or_default(),
                Kind::Document => String::new(),
            };
        }
        short_tag(&n.tag)
    }

    /// `isMerge`.
    pub fn is_merge(&self, id: Id) -> bool {
        let n = self.node(id);
        n.kind == Kind::Scalar
            && n.value == "<<"
            && (n.tag.is_empty() || n.tag == "!" || short_tag(&n.tag) == MERGE_TAG)
    }
}

/// `yaml.v3` `parser` over the ported libyaml event parser.
pub(crate) struct NodeParser {
    parser: Parser,
    event: Option<Event>,
    done_init: bool,
    anchors: HashMap<String, Id>,
}

fn parse_fail(e: &ParseError) -> Fail {
    Fail(format!("yaml: {}", e.message()))
}

fn failf(msg: String) -> Fail {
    Fail(format!("yaml: {msg}"))
}

fn event_name(kind: &EventKind) -> &'static str {
    // yaml_event_type_t.String()
    match kind {
        EventKind::StreamStart => "stream start",
        EventKind::StreamEnd => "stream end",
        EventKind::DocumentStart { .. } => "document start",
        EventKind::DocumentEnd { .. } => "document end",
        EventKind::Alias { .. } => "alias",
        EventKind::Scalar { .. } => "scalar",
        EventKind::SequenceStart { .. } => "sequence start",
        EventKind::SequenceEnd => "sequence end",
        EventKind::MappingStart { .. } => "mapping start",
        EventKind::MappingEnd => "mapping end",
        EventKind::TailComment => "tail comment",
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Want {
    StreamStart,
    DocumentStart,
    DocumentEnd,
    Alias,
    Scalar,
    SequenceStart,
    SequenceEnd,
    MappingStart,
    MappingEnd,
    TailComment,
}

impl Want {
    fn matches(self, kind: &EventKind) -> bool {
        matches!(
            (self, kind),
            (Want::StreamStart, EventKind::StreamStart)
                | (Want::DocumentStart, EventKind::DocumentStart { .. })
                | (Want::DocumentEnd, EventKind::DocumentEnd { .. })
                | (Want::Alias, EventKind::Alias { .. })
                | (Want::Scalar, EventKind::Scalar { .. })
                | (Want::SequenceStart, EventKind::SequenceStart { .. })
                | (Want::SequenceEnd, EventKind::SequenceEnd)
                | (Want::MappingStart, EventKind::MappingStart { .. })
                | (Want::MappingEnd, EventKind::MappingEnd)
                | (Want::TailComment, EventKind::TailComment)
        )
    }
    fn name(self) -> &'static str {
        match self {
            Want::StreamStart => "stream start",
            Want::DocumentStart => "document start",
            Want::DocumentEnd => "document end",
            Want::Alias => "alias",
            Want::Scalar => "scalar",
            Want::SequenceStart => "sequence start",
            Want::SequenceEnd => "sequence end",
            Want::MappingStart => "mapping start",
            Want::MappingEnd => "mapping end",
            Want::TailComment => "tail comment",
        }
    }
}

impl NodeParser {
    /// `newParserFromReader` (yaml.NewDecoder). `newParser` (yaml.Unmarshal)
    /// differs only in replacing empty input with "\n": use [`Self::new_bytes`].
    pub fn new_reader(input: &[u8]) -> NodeParser {
        NodeParser {
            parser: Parser::new(input),
            event: None,
            done_init: false,
            anchors: HashMap::new(),
        }
    }

    /// `newParser` (yaml.Unmarshal).
    pub fn new_bytes(input: &[u8]) -> NodeParser {
        if input.is_empty() {
            Self::new_reader(b"\n")
        } else {
            Self::new_reader(input)
        }
    }

    fn fill(&mut self) -> Result<&Event, Fail> {
        if self.event.is_none() {
            let event = self.parser.parse().map_err(|e| parse_fail(&e))?;
            self.event = Some(event);
        }
        Ok(self.event.as_ref().expect("event filled"))
    }

    /// `(*parser).expect`.
    fn expect(&mut self, want: Want) -> Result<Event, Fail> {
        let event = self.fill()?.clone();
        if matches!(event.kind, EventKind::StreamEnd) {
            return Err(failf(
                "attempted to go past the end of stream; corrupted value?".into(),
            ));
        }
        if !want.matches(&event.kind) {
            // p.parser.problem = ...; p.fail(): no error marks are set here,
            // so the message carries no line prefix.
            return Err(failf(format!(
                "expected {} event but got {}",
                want.name(),
                event_name(&event.kind)
            )));
        }
        self.event = None;
        Ok(event)
    }

    fn peek(&mut self) -> Result<&EventKind, Fail> {
        Ok(&self.fill()?.kind)
    }

    fn init(&mut self) -> Result<(), Fail> {
        if self.done_init {
            return Ok(());
        }
        self.anchors.clear();
        self.expect(Want::StreamStart)?;
        self.done_init = true;
        Ok(())
    }

    /// `(*parser).parse` for the next document: `Ok(None)` at stream end.
    pub fn parse_document(&mut self) -> Result<Option<(Doc, Id)>, Fail> {
        let mut doc = Doc::default();
        match self.parse(&mut doc)? {
            Some(root) => Ok(Some((doc, root))),
            None => Ok(None),
        }
    }

    fn parse(&mut self, doc: &mut Doc) -> Result<Option<Id>, Fail> {
        self.init()?;
        let kind = self.peek()?.clone();
        let id = match kind {
            EventKind::Scalar { .. } => self.scalar(doc)?,
            EventKind::Alias { .. } => self.alias(doc)?,
            EventKind::MappingStart { .. } => self.mapping(doc)?,
            EventKind::SequenceStart { .. } => self.sequence(doc)?,
            EventKind::DocumentStart { .. } => self.document(doc)?,
            EventKind::StreamEnd => return Ok(None),
            EventKind::TailComment => {
                panic!("internal error: unexpected tail comment event (please report)")
            }
            other => panic!(
                "internal error: attempted to parse unknown event (please report): {}",
                event_name(&other)
            ),
        };
        Ok(Some(id))
    }

    /// `(*parser).node`.
    fn node(&self, kind: Kind, default_tag: &str, tag: &str, value: &str) -> Node {
        let mut style = 0;
        let tag = if !tag.is_empty() && tag != "!" {
            style = TAGGED;
            short_tag(tag)
        } else if !default_tag.is_empty() {
            default_tag.to_string()
        } else if kind == Kind::Scalar {
            resolve("", value).map(|(t, _)| t).unwrap_or_default()
        } else {
            tag.to_string()
        };
        let mut n = Node::new(kind, &tag, value);
        n.style = style;
        if let Some(event) = &self.event {
            n.line = event.start.line + 1;
            n.column = event.start.column + 1;
        }
        n
    }

    fn anchor(&mut self, doc: &mut Doc, id: Id, anchor: &Option<String>) {
        if let Some(anchor) = anchor {
            doc.nodes[id].anchor = anchor.clone();
            self.anchors.insert(anchor.clone(), id);
        }
    }

    fn parse_child(&mut self, doc: &mut Doc, parent: Id) -> Result<Id, Fail> {
        let child = self
            .parse(doc)?
            .expect("parse_child is only called before an end event");
        doc.nodes[parent].content.push(child);
        Ok(child)
    }

    fn document(&mut self, doc: &mut Doc) -> Result<Id, Fail> {
        let n = self.node(Kind::Document, "", "", "");
        let id = doc.push(n);
        self.expect(Want::DocumentStart)?;
        self.parse_child(doc, id)?;
        self.expect(Want::DocumentEnd)?;
        Ok(id)
    }

    fn alias(&mut self, doc: &mut Doc) -> Result<Id, Fail> {
        let name = match &self.fill()?.kind {
            EventKind::Alias { anchor } => anchor.clone(),
            _ => unreachable!(),
        };
        let mut n = self.node(Kind::Alias, "", "", &name);
        n.alias = self.anchors.get(&name).copied();
        if n.alias.is_none() {
            return Err(failf(format!("unknown anchor '{name}' referenced")));
        }
        let id = doc.push(n);
        self.expect(Want::Alias)?;
        Ok(id)
    }

    fn scalar(&mut self, doc: &mut Doc) -> Result<Id, Fail> {
        let (anchor, tag, value, style) = match &self.fill()?.kind {
            EventKind::Scalar {
                anchor,
                tag,
                value,
                style,
                ..
            } => (anchor.clone(), tag.clone(), value.clone(), *style),
            _ => unreachable!(),
        };
        let node_style = match style {
            ScalarStyle::DoubleQuoted => DOUBLE_QUOTED,
            ScalarStyle::SingleQuoted => SINGLE_QUOTED,
            ScalarStyle::Literal => LITERAL,
            ScalarStyle::Folded => FOLDED,
            ScalarStyle::Plain | ScalarStyle::Any => 0,
        };
        let default_tag = if node_style == 0 {
            if value == "<<" { MERGE_TAG } else { "" }
        } else {
            STR_TAG
        };
        let mut n = self.node(
            Kind::Scalar,
            default_tag,
            tag.as_deref().unwrap_or(""),
            &value,
        );
        n.style |= node_style;
        let id = doc.push(n);
        self.anchor(doc, id, &anchor);
        self.expect(Want::Scalar)?;
        Ok(id)
    }

    fn sequence(&mut self, doc: &mut Doc) -> Result<Id, Fail> {
        let (anchor, tag, flow) = match &self.fill()?.kind {
            EventKind::SequenceStart {
                anchor, tag, flow, ..
            } => (anchor.clone(), tag.clone(), *flow),
            _ => unreachable!(),
        };
        let mut n = self.node(Kind::Sequence, SEQ_TAG, tag.as_deref().unwrap_or(""), "");
        if flow {
            n.style |= FLOW;
        }
        let id = doc.push(n);
        self.anchor(doc, id, &anchor);
        self.expect(Want::SequenceStart)?;
        while !matches!(self.peek()?, EventKind::SequenceEnd) {
            self.parse_child(doc, id)?;
        }
        self.expect(Want::SequenceEnd)?;
        Ok(id)
    }

    fn mapping(&mut self, doc: &mut Doc) -> Result<Id, Fail> {
        let (anchor, tag, flow) = match &self.fill()?.kind {
            EventKind::MappingStart {
                anchor, tag, flow, ..
            } => (anchor.clone(), tag.clone(), *flow),
            _ => unreachable!(),
        };
        let mut n = self.node(Kind::Mapping, MAP_TAG, tag.as_deref().unwrap_or(""), "");
        if flow {
            n.style |= FLOW;
        }
        let id = doc.push(n);
        self.anchor(doc, id, &anchor);
        self.expect(Want::MappingStart)?;
        while !matches!(self.peek()?, EventKind::MappingEnd) {
            self.parse_child(doc, id)?;
            self.parse_child(doc, id)?;
            if matches!(self.peek()?, EventKind::TailComment) {
                self.expect(Want::TailComment)?;
            }
        }
        self.expect(Want::MappingEnd)?;
        Ok(id)
    }
}
