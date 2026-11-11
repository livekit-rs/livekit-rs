// Event parser (reader + scanner + parser) of gopkg.in/yaml.v3 v3.0.1, ported to Rust.
//
// Ported from gopkg.in/yaml.v3 v3.0.1 (the Go port of libyaml), files:
//   readerc.go, scannerc.go, parserc.go, yamlh.go, yamlprivateh.go
//   (plus yaml_insert_token / yaml_string_read_handler from apic.go and the
//   (*parser).fail() error rendering from decode.go)
//
// Licensed under the MIT license (libyaml-derived files) / Apache-2.0 (yaml.v3):
//   Copyright (c) 2006-2010 Kirill Simonov
//   Copyright (c) 2011-2019 Canonical Ltd
//
//   Permission is hereby granted, free of charge, to any person obtaining a copy of
//   this software and associated documentation files (the "Software"), to deal in
//   the Software without restriction, including without limitation the rights to
//   use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
//   of the Software, and to permit persons to whom the Software is furnished to do
//   so, subject to the following conditions:
//
//   The above copyright notice and this permission notice shall be included in all
//   copies or substantial portions of the Software.
//
//   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//   IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//   FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//   AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//   LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//   OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
//   SOFTWARE.
//
// The code intentionally mirrors the Go structure function by function (Go
// identifiers are kept in comments / method names) so that it can be diffed
// against the original. Functions return `bool` exactly like the Go code: `false`
// means an error was recorded in the parser's error fields.
//
// Porting notes:
// * The reader keeps the same raw (512 bytes) and working (1536 bytes) buffer
//   sizes and refill policy as yaml.v3, so reader (encoding) errors surface at
//   exactly the same point of the event stream as in Go.
// * The token queue emulates the Go slice exactly (backing array of length
//   cap, Go's append growth rule for the 144-byte yaml_token_t, in-place
//   compaction in yaml_insert_token, stale slots). This is observable: in
//   yaml_parser_parse_flow_sequence_entry_mapping_value yaml.v3 dereferences a
//   *yaml_token_t taken before skip_token/peek_token, which may by then point
//   at a slot that compaction overwrote (or into an abandoned backing array).
// * Go `[]byte` buffers are `Vec<u8>`; event strings are converted with
//   `String::from_utf8` (lossy only for tag URIs built from `%xx` escapes that
//   do not form valid UTF-8, which Go would carry as raw bytes).

use std::collections::HashMap;

/// The pointer position (yaml_mark_t). All fields are 0-based; `index` and
/// `column` count characters (not bytes), exactly like yaml.v3.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Mark {
    pub index: usize,
    pub line: usize,
    pub column: usize,
}

/// Scalar styles (yaml_scalar_style_t).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScalarStyle {
    Any,
    Plain,
    SingleQuoted,
    DoubleQuoted,
    Literal,
    Folded,
}

/// Event payloads (yaml_event_type_t + the relevant yaml_event_t fields).
#[derive(Clone, Debug, PartialEq)]
pub enum EventKind {
    StreamStart,
    StreamEnd,
    DocumentStart {
        implicit: bool,
    },
    DocumentEnd {
        implicit: bool,
    },
    Alias {
        anchor: String,
    },
    Scalar {
        anchor: Option<String>,
        tag: Option<String>,
        value: String,
        style: ScalarStyle,
        plain_implicit: bool,
        quoted_implicit: bool,
    },
    SequenceStart {
        anchor: Option<String>,
        tag: Option<String>,
        implicit: bool,
        flow: bool,
    },
    SequenceEnd,
    MappingStart {
        anchor: Option<String>,
        tag: Option<String>,
        implicit: bool,
        flow: bool,
    },
    MappingEnd,
    TailComment,
}

/// The event structure (yaml_event_t).
#[derive(Clone, Debug, PartialEq)]
pub struct Event {
    pub kind: EventKind,
    pub start: Mark,
    pub end: Mark,
    pub head_comment: String,
    pub line_comment: String,
    pub foot_comment: String,
}

/// Error classes (yaml_READER_ERROR, yaml_SCANNER_ERROR, yaml_PARSER_ERROR).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorKind {
    Reader,
    Scanner,
    Parser,
}

/// The error fields of yaml_parser_t at the time the error was reported.
#[derive(Clone, Debug, PartialEq)]
pub struct ParseError {
    pub kind: ErrorKind,
    pub problem: String,
    pub problem_mark: Mark,
    pub problem_offset: usize,
    pub problem_value: i32,
    pub context: String,
    pub context_mark: Mark,
}

impl ParseError {
    /// Exactly yaml.v3 decode.go `(*parser).fail()` text without the leading
    /// "yaml: " prefix.
    pub fn message(&self) -> String {
        // func (p *parser) fail()
        let mut line = 0;
        if self.context_mark.line != 0 {
            line = self.context_mark.line;
            // Scanner errors don't iterate line before returning error
            if self.kind == ErrorKind::Scanner {
                line += 1;
            }
        } else if self.problem_mark.line != 0 {
            line = self.problem_mark.line;
            // Scanner errors don't iterate line before returning error
            if self.kind == ErrorKind::Scanner {
                line += 1;
            }
        }
        let mut out = String::new();
        if line != 0 {
            out.push_str("line ");
            out.push_str(&line.to_string());
            out.push_str(": ");
        }
        if !self.problem.is_empty() {
            out.push_str(&self.problem);
        } else {
            out.push_str("unknown problem parsing YAML content");
        }
        out
    }
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message())
    }
}

impl std::error::Error for ParseError {}

// ---------------------------------------------------------------------------
// yamlprivateh.go
// ---------------------------------------------------------------------------

// The size of the input raw buffer.
const INPUT_RAW_BUFFER_SIZE: usize = 512;

// The size of the input buffer.
// It should be possible to decode the whole raw buffer.
const INPUT_BUFFER_SIZE: usize = INPUT_RAW_BUFFER_SIZE * 3;

// Check if the character at the specified position is an alphabetical
// character, a digit, '_', or '-'.
fn is_alpha(b: &[u8], i: usize) -> bool {
    b[i].is_ascii_digit()
        || b[i].is_ascii_uppercase()
        || b[i].is_ascii_lowercase()
        || b[i] == b'_'
        || b[i] == b'-'
}

// Check if the character at the specified position is a digit.
fn is_digit(b: &[u8], i: usize) -> bool {
    b[i].is_ascii_digit()
}

// Get the value of a digit.
fn as_digit(b: &[u8], i: usize) -> i32 {
    i32::from(b[i]) - i32::from(b'0')
}

// Check if the character at the specified position is a hex-digit.
fn is_hex(b: &[u8], i: usize) -> bool {
    b[i].is_ascii_hexdigit()
}

// Get the value of a hex-digit.
fn as_hex(b: &[u8], i: usize) -> i64 {
    let bi = b[i];
    if bi.is_ascii_uppercase() && bi <= b'F' {
        return i64::from(bi) - i64::from(b'A') + 10;
    }
    if bi.is_ascii_lowercase() && bi <= b'f' {
        return i64::from(bi) - i64::from(b'a') + 10;
    }
    i64::from(bi) - i64::from(b'0')
}

// Check if the character at the specified position is NUL.
fn is_z(b: &[u8], i: usize) -> bool {
    b[i] == 0x00
}

// Check if the beginning of the buffer is a BOM.
// [Go] Note that yaml.v3 checks the start of the buffer, not position i.
fn is_bom(b: &[u8]) -> bool {
    b[0] == 0xEF && b[1] == 0xBB && b[2] == 0xBF
}

// Check if the character at the specified position is space.
fn is_space(b: &[u8], i: usize) -> bool {
    b[i] == b' '
}

// Check if the character at the specified position is tab.
fn is_tab(b: &[u8], i: usize) -> bool {
    b[i] == b'\t'
}

// Check if the character at the specified position is blank (space or tab).
fn is_blank(b: &[u8], i: usize) -> bool {
    b[i] == b' ' || b[i] == b'\t'
}

// Check if the character at the specified position is a line break.
fn is_break(b: &[u8], i: usize) -> bool {
    b[i] == b'\r' // CR (#xD)
        || b[i] == b'\n' // LF (#xA)
        || b[i] == 0xC2 && b[i + 1] == 0x85 // NEL (#x85)
        || b[i] == 0xE2 && b[i + 1] == 0x80 && b[i + 2] == 0xA8 // LS (#x2028)
        || b[i] == 0xE2 && b[i + 1] == 0x80 && b[i + 2] == 0xA9 // PS (#x2029)
}

fn is_crlf(b: &[u8], i: usize) -> bool {
    b[i] == b'\r' && b[i + 1] == b'\n'
}

// Check if the character is a line break or NUL.
fn is_breakz(b: &[u8], i: usize) -> bool {
    is_break(b, i) || is_z(b, i)
}

// Check if the character is a line break, space, tab, or NUL.
fn is_blankz(b: &[u8], i: usize) -> bool {
    is_blank(b, i) || is_breakz(b, i)
}

// Determine the width of the character.
fn width(b: u8) -> usize {
    if b & 0x80 == 0x00 {
        return 1;
    }
    if b & 0xE0 == 0xC0 {
        return 2;
    }
    if b & 0xF0 == 0xE0 {
        return 3;
    }
    if b & 0xF8 == 0xF0 {
        return 4;
    }
    0
}

// ---------------------------------------------------------------------------
// yamlh.go
// ---------------------------------------------------------------------------

// The stream encoding (yaml_encoding_t).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Encoding {
    Any,     // yaml_ANY_ENCODING
    Utf8,    // yaml_UTF8_ENCODING
    Utf16Le, // yaml_UTF16LE_ENCODING
    Utf16Be, // yaml_UTF16BE_ENCODING
}

// Token types (yaml_token_type_t).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TokenType {
    NoToken,            // yaml_NO_TOKEN
    StreamStart,        // yaml_STREAM_START_TOKEN
    StreamEnd,          // yaml_STREAM_END_TOKEN
    VersionDirective,   // yaml_VERSION_DIRECTIVE_TOKEN
    TagDirective,       // yaml_TAG_DIRECTIVE_TOKEN
    DocumentStart,      // yaml_DOCUMENT_START_TOKEN
    DocumentEnd,        // yaml_DOCUMENT_END_TOKEN
    BlockSequenceStart, // yaml_BLOCK_SEQUENCE_START_TOKEN
    BlockMappingStart,  // yaml_BLOCK_MAPPING_START_TOKEN
    BlockEnd,           // yaml_BLOCK_END_TOKEN
    FlowSequenceStart,  // yaml_FLOW_SEQUENCE_START_TOKEN
    FlowSequenceEnd,    // yaml_FLOW_SEQUENCE_END_TOKEN
    FlowMappingStart,   // yaml_FLOW_MAPPING_START_TOKEN
    FlowMappingEnd,     // yaml_FLOW_MAPPING_END_TOKEN
    BlockEntry,         // yaml_BLOCK_ENTRY_TOKEN
    FlowEntry,          // yaml_FLOW_ENTRY_TOKEN
    Key,                // yaml_KEY_TOKEN
    Value,              // yaml_VALUE_TOKEN
    Alias,              // yaml_ALIAS_TOKEN
    Anchor,             // yaml_ANCHOR_TOKEN
    Tag,                // yaml_TAG_TOKEN
    Scalar,             // yaml_SCALAR_TOKEN
}

// The token structure (yaml_token_t).
#[derive(Clone, Debug)]
struct Token {
    // The token type.
    typ: TokenType,
    // The start/end of the token.
    start_mark: Mark,
    end_mark: Mark,
    // The alias/anchor/scalar value or tag/tag directive handle.
    value: Vec<u8>,
    // The tag suffix (for yaml_TAG_TOKEN).
    suffix: Vec<u8>,
    // The tag directive prefix (for yaml_TAG_DIRECTIVE_TOKEN).
    prefix: Vec<u8>,
    // The scalar style (for yaml_SCALAR_TOKEN).
    style: ScalarStyle,
    // The version directive major/minor (for yaml_VERSION_DIRECTIVE_TOKEN).
    major: i8,
    minor: i8,
}

impl Token {
    fn new(typ: TokenType, start_mark: Mark, end_mark: Mark) -> Token {
        Token {
            typ,
            start_mark,
            end_mark,
            value: Vec::new(),
            suffix: Vec::new(),
            prefix: Vec::new(),
            style: ScalarStyle::Any,
            major: 0,
            minor: 0,
        }
    }
}

// Event types (yaml_event_type_t).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EventType {
    NoEvent,       // yaml_NO_EVENT
    StreamStart,   // yaml_STREAM_START_EVENT
    StreamEnd,     // yaml_STREAM_END_EVENT
    DocumentStart, // yaml_DOCUMENT_START_EVENT
    DocumentEnd,   // yaml_DOCUMENT_END_EVENT
    Alias,         // yaml_ALIAS_EVENT
    Scalar,        // yaml_SCALAR_EVENT
    SequenceStart, // yaml_SEQUENCE_START_EVENT
    SequenceEnd,   // yaml_SEQUENCE_END_EVENT
    MappingStart,  // yaml_MAPPING_START_EVENT
    MappingEnd,    // yaml_MAPPING_END_EVENT
    TailComment,   // yaml_TAIL_COMMENT_EVENT
}

// The internal event structure (yaml_event_t), converted to `Event` on return.
#[derive(Clone, Debug)]
struct RawEvent {
    typ: EventType,
    start_mark: Mark,
    end_mark: Mark,
    head_comment: Vec<u8>,
    line_comment: Vec<u8>,
    foot_comment: Vec<u8>,
    anchor: Vec<u8>,
    tag: Vec<u8>,
    value: Vec<u8>,
    implicit: bool,
    quoted_implicit: bool,
    // style: scalar style, or flow (true) / block (false) for collections.
    scalar_style: ScalarStyle,
    flow: bool,
}

impl RawEvent {
    fn new(typ: EventType, start_mark: Mark, end_mark: Mark) -> RawEvent {
        RawEvent {
            typ,
            start_mark,
            end_mark,
            head_comment: Vec::new(),
            line_comment: Vec::new(),
            foot_comment: Vec::new(),
            anchor: Vec::new(),
            tag: Vec::new(),
            value: Vec::new(),
            implicit: false,
            quoted_implicit: false,
            scalar_style: ScalarStyle::Any,
            flow: false,
        }
    }
}

// The tag directive data (yaml_tag_directive_t).
#[derive(Clone, Debug)]
struct TagDirective {
    handle: Vec<u8>,
    prefix: Vec<u8>,
}

// A Go `*yaml_token_t` pointing at parser.tokens[idx] of backing array `gen`.
#[derive(Clone, Copy, Debug)]
struct TokenWatch {
    array_gen: u64,
    idx: usize,
    // start_mark of the slot when the backing array was abandoned by append.
    frozen: Option<Mark>,
}

// unsafe.Sizeof(yaml_token_t{}) on 64-bit platforms.
const GO_TOKEN_SIZE: usize = 144;

// Go runtime size classes (internal/runtime/gc/sizeclasses.go).
const GO_SIZE_CLASSES: [usize; 67] = [
    8, 16, 24, 32, 48, 64, 80, 96, 112, 128, 144, 160, 176, 192, 208, 224, 240, 256, 288, 320, 352,
    384, 416, 448, 480, 512, 576, 640, 704, 768, 896, 1024, 1152, 1280, 1408, 1536, 1792, 2048,
    2304, 2688, 3072, 3200, 3456, 4096, 4864, 5376, 6144, 6528, 6784, 6912, 8192, 9472, 9728,
    10240, 10880, 12288, 13568, 14336, 16384, 18432, 19072, 20480, 21760, 24576, 27264, 28672,
    32768,
];

// The capacity Go's append (runtime.growslice) picks when appending one
// element to a full []yaml_token_t of capacity old_cap (Go >= 1.22).
fn go_token_slice_grow(old_cap: usize) -> usize {
    let new_len = old_cap + 1;
    // nextslicecap
    let mut newcap = old_cap;
    let doublecap = newcap + newcap;
    if new_len > doublecap {
        newcap = new_len;
    } else if old_cap < 256 {
        newcap = doublecap;
    } else {
        while newcap < new_len {
            newcap += (newcap + 3 * 256) >> 2;
        }
    }
    // roundupsize(capmem, noscan=false): yaml_token_t contains pointers.
    let size = newcap * GO_TOKEN_SIZE;
    let capmem = if size <= 32768 - 8 {
        let mut req = size;
        if req > 512 {
            req += 8; // malloc header
        }
        let class = GO_SIZE_CLASSES
            .iter()
            .copied()
            .find(|&c| c >= req)
            .unwrap_or(32768);
        class - (req - size)
    } else {
        (size + 8191) & !8191
    };
    capmem / GO_TOKEN_SIZE
}

// yaml_simple_key_t
#[derive(Clone, Copy, Debug, Default)]
struct SimpleKey {
    possible: bool,      // Is a simple key possible?
    required: bool,      // Is a simple key required?
    token_number: usize, // The number of the token.
    mark: Mark,          // The position mark.
}

// yaml_parser_state_t
// (BlockNodeOrIndentlessSequence and FlowNode are never assigned by yaml.v3 either.)
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ParserState {
    StreamStart,                   // yaml_PARSE_STREAM_START_STATE
    ImplicitDocumentStart,         // yaml_PARSE_IMPLICIT_DOCUMENT_START_STATE
    DocumentStart,                 // yaml_PARSE_DOCUMENT_START_STATE
    DocumentContent,               // yaml_PARSE_DOCUMENT_CONTENT_STATE
    DocumentEnd,                   // yaml_PARSE_DOCUMENT_END_STATE
    BlockNode,                     // yaml_PARSE_BLOCK_NODE_STATE
    BlockNodeOrIndentlessSequence, // yaml_PARSE_BLOCK_NODE_OR_INDENTLESS_SEQUENCE_STATE
    FlowNode,                      // yaml_PARSE_FLOW_NODE_STATE
    BlockSequenceFirstEntry,       // yaml_PARSE_BLOCK_SEQUENCE_FIRST_ENTRY_STATE
    BlockSequenceEntry,            // yaml_PARSE_BLOCK_SEQUENCE_ENTRY_STATE
    IndentlessSequenceEntry,       // yaml_PARSE_INDENTLESS_SEQUENCE_ENTRY_STATE
    BlockMappingFirstKey,          // yaml_PARSE_BLOCK_MAPPING_FIRST_KEY_STATE
    BlockMappingKey,               // yaml_PARSE_BLOCK_MAPPING_KEY_STATE
    BlockMappingValue,             // yaml_PARSE_BLOCK_MAPPING_VALUE_STATE
    FlowSequenceFirstEntry,        // yaml_PARSE_FLOW_SEQUENCE_FIRST_ENTRY_STATE
    FlowSequenceEntry,             // yaml_PARSE_FLOW_SEQUENCE_ENTRY_STATE
    FlowSequenceEntryMappingKey,   // yaml_PARSE_FLOW_SEQUENCE_ENTRY_MAPPING_KEY_STATE
    FlowSequenceEntryMappingValue, // yaml_PARSE_FLOW_SEQUENCE_ENTRY_MAPPING_VALUE_STATE
    FlowSequenceEntryMappingEnd,   // yaml_PARSE_FLOW_SEQUENCE_ENTRY_MAPPING_END_STATE
    FlowMappingFirstKey,           // yaml_PARSE_FLOW_MAPPING_FIRST_KEY_STATE
    FlowMappingKey,                // yaml_PARSE_FLOW_MAPPING_KEY_STATE
    FlowMappingValue,              // yaml_PARSE_FLOW_MAPPING_VALUE_STATE
    FlowMappingEmptyValue,         // yaml_PARSE_FLOW_MAPPING_EMPTY_VALUE_STATE
    End,                           // yaml_PARSE_END_STATE
}

// yaml_comment_t
#[derive(Clone, Debug, Default)]
struct Comment {
    scan_mark: Mark,  // Position where scanning for comments started
    token_mark: Mark, // Position after which tokens will be associated with this comment
    start_mark: Mark, // Position of '#' comment mark
    end_mark: Mark,   // Position where comment terminated

    head: Vec<u8>,
    line: Vec<u8>,
    foot: Vec<u8>,
}

// max_flow_level limits the flow_level
const MAX_FLOW_LEVEL: usize = 10000;

// max_indents limits the indents stack size
const MAX_INDENTS: usize = 10000;

// const max_number_length = 2
const MAX_NUMBER_LENGTH: i8 = 2;

/// The parser structure (yaml_parser_t), restricted to the parsing side.
pub struct Parser {
    // Error handling
    error: Option<ErrorKind>, // Error type (None == yaml_NO_ERROR).
    problem: String,          // Error description.
    problem_offset: usize,
    problem_value: i32,
    problem_mark: Mark,
    context: String,
    context_mark: Mark,

    // Reader stuff
    input: Vec<u8>, // String input data (strings.Reader).
    input_pos: usize,
    eof: bool, // EOF flag

    buffer: Vec<u8>,   // The working buffer (fixed capacity input_buffer_size).
    buffer_len: usize, // len(parser.buffer) in Go.
    buffer_pos: usize, // The current position of the buffer.

    unread: isize, // The number of unread characters in the buffer.

    newlines: usize, // The number of line breaks since last non-break/non-blank character

    raw_buffer: Vec<u8>, // The raw buffer (fixed capacity input_raw_buffer_size).
    raw_buffer_len: usize, // len(parser.raw_buffer) in Go.
    raw_buffer_pos: usize, // The current position of the buffer.

    encoding: Encoding, // The input encoding.

    offset: usize, // The offset of the current position (in bytes).
    mark: Mark,    // The mark of the current position.

    // Comments
    head_comment: Vec<u8>, // The current head comments
    line_comment: Vec<u8>, // The current line comments
    foot_comment: Vec<u8>, // The current foot comments
    tail_comment: Vec<u8>, // Foot comment that happens at the end of a block.
    stem_comment: Vec<u8>, // Comment in item preceding a nested structure (list inside list item, etc)

    comments: Vec<Comment>, // The folded comments for all parsed tokens
    comments_head: usize,

    // Scanner stuff
    stream_start_produced: bool, // Have we started to scan the input stream?
    stream_end_produced: bool,   // Have we reached the end of the input stream?

    flow_level: usize, // The number of unclosed '[' and '{' indicators.

    tokens: Vec<Token>, // The tokens queue (Go backing array; tokens.len() == cap).
    tokens_len: usize,  // len(parser.tokens) in Go.
    tokens_gen: u64,    // Identity of the current backing array (bumped on reallocation).
    token_watch: Option<TokenWatch>, // A Go *yaml_token_t kept across queue mutations.
    tokens_head: usize, // The head of the tokens queue.
    tokens_parsed: usize, // The number of tokens fetched from the queue.
    token_available: bool, // Does the tokens queue contain a token ready for dequeueing.

    indent: isize,       // The current indentation level.
    indents: Vec<isize>, // The indentation levels stack.

    simple_key_allowed: bool, // May a simple key occur at the current position?
    simple_keys: Vec<SimpleKey>, // The stack of simple keys.
    simple_keys_by_tok: HashMap<usize, usize>, // possible simple_key indexes indexed by token_number

    // Parser stuff
    state: ParserState,                // The current parser state.
    states: Vec<ParserState>,          // The parser states stack.
    marks: Vec<Mark>,                  // The stack of marks.
    tag_directives: Vec<TagDirective>, // The list of TAG directives.

    // Rust-side: the last StreamEnd event, repeated after the end of the stream.
    last_stream_end: Option<Event>,
}

fn to_string(b: Vec<u8>) -> String {
    match String::from_utf8(b) {
        Ok(s) => s,
        Err(e) => String::from_utf8_lossy(e.as_bytes()).into_owned(),
    }
}

fn to_opt_string(b: Vec<u8>) -> Option<String> {
    if b.is_empty() {
        None
    } else {
        Some(to_string(b))
    }
}

impl RawEvent {
    fn into_event(self) -> Event {
        let kind = match self.typ {
            EventType::NoEvent | EventType::StreamStart => EventKind::StreamStart,
            EventType::StreamEnd => EventKind::StreamEnd,
            EventType::DocumentStart => EventKind::DocumentStart {
                implicit: self.implicit,
            },
            EventType::DocumentEnd => EventKind::DocumentEnd {
                implicit: self.implicit,
            },
            EventType::Alias => EventKind::Alias {
                anchor: to_string(self.anchor),
            },
            EventType::Scalar => EventKind::Scalar {
                anchor: to_opt_string(self.anchor),
                tag: to_opt_string(self.tag),
                value: to_string(self.value),
                style: self.scalar_style,
                plain_implicit: self.implicit,
                quoted_implicit: self.quoted_implicit,
            },
            EventType::SequenceStart => EventKind::SequenceStart {
                anchor: to_opt_string(self.anchor),
                tag: to_opt_string(self.tag),
                implicit: self.implicit,
                flow: self.flow,
            },
            EventType::SequenceEnd => EventKind::SequenceEnd,
            EventType::MappingStart => EventKind::MappingStart {
                anchor: to_opt_string(self.anchor),
                tag: to_opt_string(self.tag),
                implicit: self.implicit,
                flow: self.flow,
            },
            EventType::MappingEnd => EventKind::MappingEnd,
            EventType::TailComment => EventKind::TailComment,
        };
        Event {
            kind,
            start: self.start_mark,
            end: self.end_mark,
            head_comment: to_string(self.head_comment),
            line_comment: to_string(self.line_comment),
            foot_comment: to_string(self.foot_comment),
        }
    }
}

impl Parser {
    /// yaml_parser_initialize + yaml_parser_set_input_reader over a
    /// strings.Reader (what yaml.NewDecoder(strings.NewReader(..)) does).
    pub fn new(input: &[u8]) -> Parser {
        Parser {
            error: None,
            problem: String::new(),
            problem_offset: 0,
            problem_value: 0,
            problem_mark: Mark::default(),
            context: String::new(),
            context_mark: Mark::default(),
            input: input.to_vec(),
            input_pos: 0,
            eof: false,
            buffer: vec![0; INPUT_BUFFER_SIZE],
            buffer_len: 0,
            buffer_pos: 0,
            unread: 0,
            newlines: 0,
            raw_buffer: vec![0; INPUT_RAW_BUFFER_SIZE],
            raw_buffer_len: 0,
            raw_buffer_pos: 0,
            encoding: Encoding::Any,
            offset: 0,
            mark: Mark::default(),
            head_comment: Vec::new(),
            line_comment: Vec::new(),
            foot_comment: Vec::new(),
            tail_comment: Vec::new(),
            stem_comment: Vec::new(),
            comments: Vec::new(),
            comments_head: 0,
            stream_start_produced: false,
            stream_end_produced: false,
            flow_level: 0,
            tokens: Vec::new(),
            tokens_len: 0,
            tokens_gen: 0,
            token_watch: None,
            tokens_head: 0,
            tokens_parsed: 0,
            token_available: false,
            indent: 0,
            indents: Vec::new(),
            simple_key_allowed: false,
            simple_keys: Vec::new(),
            simple_keys_by_tok: HashMap::new(),
            state: ParserState::StreamStart,
            states: Vec::new(),
            marks: Vec::new(),
            tag_directives: Vec::new(),
            last_stream_end: None,
        }
    }

    /// yaml_parser_parse: the next event, or the error.
    ///
    /// Like yaml.v3's `(*parser).peek()`, an event produced while an error was
    /// recorded is not returned; the error is returned instead. After an error
    /// the same error is returned forever. After STREAM-END, the STREAM-END
    /// event is repeated (Go returns an empty event there).
    pub fn parse(&mut self) -> Result<Event, ParseError> {
        // No events after the end of the stream or error.
        if self.error.is_some() {
            return Err(self.current_error());
        }
        if self.stream_end_produced || self.state == ParserState::End {
            if let Some(ev) = &self.last_stream_end {
                return Ok(ev.clone());
            }
            return Ok(RawEvent::new(EventType::StreamEnd, self.mark, self.mark).into_event());
        }

        // Generate the next event.
        let mut event = RawEvent::new(EventType::NoEvent, Mark::default(), Mark::default());
        let ok = self.yaml_parser_state_machine(&mut event);
        if !ok || self.error.is_some() {
            return Err(self.current_error());
        }
        let ev = event.into_event();
        if ev.kind == EventKind::StreamEnd {
            self.last_stream_end = Some(ev.clone());
        }
        Ok(ev)
    }

    fn current_error(&self) -> ParseError {
        ParseError {
            kind: self.error.unwrap_or(ErrorKind::Parser),
            problem: self.problem.clone(),
            problem_mark: self.problem_mark,
            problem_offset: self.problem_offset,
            problem_value: self.problem_value,
            context: self.context.clone(),
            context_mark: self.context_mark,
        }
    }

    // -----------------------------------------------------------------------
    // readerc.go
    // -----------------------------------------------------------------------

    // Set the reader error and return 0.
    // yaml_parser_set_reader_error
    fn yaml_parser_set_reader_error(&mut self, problem: &str, offset: usize, value: i32) -> bool {
        self.error = Some(ErrorKind::Reader);
        self.problem = problem.to_string();
        self.problem_offset = offset;
        self.problem_value = value;
        false
    }

    // Determine the input stream encoding by checking the BOM symbol. If no BOM is
    // found, the UTF-8 encoding is assumed. Return 1 on success, 0 on failure.
    // yaml_parser_determine_encoding
    fn yaml_parser_determine_encoding(&mut self) -> bool {
        // Ensure that we had enough bytes in the raw buffer.
        while !self.eof && self.raw_buffer_len - self.raw_buffer_pos < 3 {
            if !self.yaml_parser_update_raw_buffer() {
                return false;
            }
        }

        // Determine the encoding.
        let buf = &self.raw_buffer;
        let pos = self.raw_buffer_pos;
        let avail = self.raw_buffer_len - pos;
        if avail >= 2 && buf[pos] == 0xFF && buf[pos + 1] == 0xFE {
            self.encoding = Encoding::Utf16Le;
            self.raw_buffer_pos += 2;
            self.offset += 2;
        } else if avail >= 2 && buf[pos] == 0xFE && buf[pos + 1] == 0xFF {
            self.encoding = Encoding::Utf16Be;
            self.raw_buffer_pos += 2;
            self.offset += 2;
        } else if avail >= 3 && buf[pos] == 0xEF && buf[pos + 1] == 0xBB && buf[pos + 2] == 0xBF {
            self.encoding = Encoding::Utf8;
            self.raw_buffer_pos += 3;
            self.offset += 3;
        } else {
            self.encoding = Encoding::Utf8;
        }
        true
    }

    // Update the raw buffer.
    // yaml_parser_update_raw_buffer
    fn yaml_parser_update_raw_buffer(&mut self) -> bool {
        // Return if the raw buffer is full.
        if self.raw_buffer_pos == 0 && self.raw_buffer_len == INPUT_RAW_BUFFER_SIZE {
            return true;
        }

        // Return on EOF.
        if self.eof {
            return true;
        }

        // Move the remaining bytes in the raw buffer to the beginning.
        if self.raw_buffer_pos > 0 && self.raw_buffer_pos < self.raw_buffer_len {
            self.raw_buffer
                .copy_within(self.raw_buffer_pos..self.raw_buffer_len, 0);
        }
        self.raw_buffer_len -= self.raw_buffer_pos;
        self.raw_buffer_pos = 0;

        // Call the read handler to fill the buffer.
        // (yaml_reader_read_handler over strings.Reader / yaml_string_read_handler)
        if self.input_pos == self.input.len() {
            self.eof = true;
        } else {
            let n = (INPUT_RAW_BUFFER_SIZE - self.raw_buffer_len)
                .min(self.input.len() - self.input_pos);
            self.raw_buffer[self.raw_buffer_len..self.raw_buffer_len + n]
                .copy_from_slice(&self.input[self.input_pos..self.input_pos + n]);
            self.input_pos += n;
            self.raw_buffer_len += n;
        }
        true
    }

    // Ensure that the buffer contains at least `length` characters.
    // Return true on success, false on failure.
    //
    // The length is supposed to be significantly less that the buffer size.
    // yaml_parser_update_buffer
    #[allow(clippy::too_many_lines)]
    fn yaml_parser_update_buffer(&mut self, length: usize) -> bool {
        // [Go] This function was changed to guarantee the requested length size at EOF.

        // Return if the buffer contains enough characters.
        if self.unread >= length as isize {
            return true;
        }

        // Determine the input encoding if it is not known yet.
        if self.encoding == Encoding::Any && !self.yaml_parser_determine_encoding() {
            return false;
        }

        // Move the unread characters to the beginning of the buffer.
        let mut buffer_len = self.buffer_len;
        if self.buffer_pos > 0 && self.buffer_pos < buffer_len {
            self.buffer.copy_within(self.buffer_pos..buffer_len, 0);
            buffer_len -= self.buffer_pos;
            self.buffer_pos = 0;
        } else if self.buffer_pos == buffer_len {
            buffer_len = 0;
            self.buffer_pos = 0;
        }

        // Open the whole buffer for writing, and cut it before returning.
        self.buffer_len = INPUT_BUFFER_SIZE;

        // Fill the buffer until it has enough characters.
        let mut first = true;
        while self.unread < length as isize {
            // Fill the raw buffer if necessary.
            if (!first || self.raw_buffer_pos == self.raw_buffer_len)
                && !self.yaml_parser_update_raw_buffer()
            {
                self.buffer_len = buffer_len;
                return false;
            }
            first = false;

            // Decode the raw buffer.
            'inner: while self.raw_buffer_pos != self.raw_buffer_len {
                let mut value: u32;
                let width: usize;

                let raw_unread = self.raw_buffer_len - self.raw_buffer_pos;

                // Decode the next character.
                match self.encoding {
                    Encoding::Utf8 => {
                        // Decode a UTF-8 character.  Check RFC 3629
                        // (http://www.ietf.org/rfc/rfc3629.txt) for more details.

                        // Determine the length of the UTF-8 sequence.
                        let mut octet = self.raw_buffer[self.raw_buffer_pos];
                        width = if octet & 0x80 == 0x00 {
                            1
                        } else if octet & 0xE0 == 0xC0 {
                            2
                        } else if octet & 0xF0 == 0xE0 {
                            3
                        } else if octet & 0xF8 == 0xF0 {
                            4
                        } else {
                            // The leading octet is invalid.
                            return self.yaml_parser_set_reader_error(
                                "invalid leading UTF-8 octet",
                                self.offset,
                                i32::from(octet),
                            );
                        };

                        // Check if the raw buffer contains an incomplete character.
                        if width > raw_unread {
                            if self.eof {
                                return self.yaml_parser_set_reader_error(
                                    "incomplete UTF-8 octet sequence",
                                    self.offset,
                                    -1,
                                );
                            }
                            break 'inner;
                        }

                        // Decode the leading octet.
                        value = if octet & 0x80 == 0x00 {
                            u32::from(octet & 0x7F)
                        } else if octet & 0xE0 == 0xC0 {
                            u32::from(octet & 0x1F)
                        } else if octet & 0xF0 == 0xE0 {
                            u32::from(octet & 0x0F)
                        } else if octet & 0xF8 == 0xF0 {
                            u32::from(octet & 0x07)
                        } else {
                            0
                        };

                        // Check and decode the trailing octets.
                        for k in 1..width {
                            octet = self.raw_buffer[self.raw_buffer_pos + k];

                            // Check if the octet is valid.
                            if (octet & 0xC0) != 0x80 {
                                return self.yaml_parser_set_reader_error(
                                    "invalid trailing UTF-8 octet",
                                    self.offset + k,
                                    i32::from(octet),
                                );
                            }

                            // Decode the octet.
                            value = (value << 6) + u32::from(octet & 0x3F);
                        }

                        // Check the length of the sequence against the value.
                        let length_ok = width == 1
                            || width == 2 && value >= 0x80
                            || width == 3 && value >= 0x800
                            || width == 4 && value >= 0x10000;
                        if !length_ok {
                            return self.yaml_parser_set_reader_error(
                                "invalid length of a UTF-8 sequence",
                                self.offset,
                                -1,
                            );
                        }

                        // Check the range of the value.
                        if (0xD800..=0xDFFF).contains(&value) || value > 0x10FFFF {
                            return self.yaml_parser_set_reader_error(
                                "invalid Unicode character",
                                self.offset,
                                value as i32,
                            );
                        }
                    }
                    Encoding::Utf16Le | Encoding::Utf16Be => {
                        let (low, high) = if self.encoding == Encoding::Utf16Le {
                            (0, 1)
                        } else {
                            (1, 0)
                        };

                        // The UTF-16 encoding is not as simple as one might
                        // naively think.  Check RFC 2781
                        // (http://www.ietf.org/rfc/rfc2781.txt).

                        // Check for incomplete UTF-16 character.
                        if raw_unread < 2 {
                            if self.eof {
                                return self.yaml_parser_set_reader_error(
                                    "incomplete UTF-16 character",
                                    self.offset,
                                    -1,
                                );
                            }
                            break 'inner;
                        }

                        // Get the character.
                        value = u32::from(self.raw_buffer[self.raw_buffer_pos + low])
                            + (u32::from(self.raw_buffer[self.raw_buffer_pos + high]) << 8);

                        // Check for unexpected low surrogate area.
                        if value & 0xFC00 == 0xDC00 {
                            return self.yaml_parser_set_reader_error(
                                "unexpected low surrogate area",
                                self.offset,
                                value as i32,
                            );
                        }

                        // Check for a high surrogate area.
                        if value & 0xFC00 == 0xD800 {
                            width = 4;

                            // Check for incomplete surrogate pair.
                            if raw_unread < 4 {
                                if self.eof {
                                    return self.yaml_parser_set_reader_error(
                                        "incomplete UTF-16 surrogate pair",
                                        self.offset,
                                        -1,
                                    );
                                }
                                break 'inner;
                            }

                            // Get the next character.
                            let value2 = u32::from(self.raw_buffer[self.raw_buffer_pos + low + 2])
                                + (u32::from(self.raw_buffer[self.raw_buffer_pos + high + 2]) << 8);

                            // Check for a low surrogate area.
                            if value2 & 0xFC00 != 0xDC00 {
                                return self.yaml_parser_set_reader_error(
                                    "expected low surrogate area",
                                    self.offset + 2,
                                    value2 as i32,
                                );
                            }

                            // Generate the value of the surrogate pair.
                            value = 0x10000 + ((value & 0x3FF) << 10) + (value2 & 0x3FF);
                        } else {
                            width = 2;
                        }
                    }
                    Encoding::Any => unreachable!("impossible"),
                }

                // Check if the character is in the allowed range:
                //      #x9 | #xA | #xD | [#x20-#x7E]               (8 bit)
                //      | #x85 | [#xA0-#xD7FF] | [#xE000-#xFFFD]    (16 bit)
                //      | [#x10000-#x10FFFF]                        (32 bit)
                let allowed = value == 0x09
                    || value == 0x0A
                    || value == 0x0D
                    || (0x20..=0x7E).contains(&value)
                    || value == 0x85
                    || (0xA0..=0xD7FF).contains(&value)
                    || (0xE000..=0xFFFD).contains(&value)
                    || (0x10000..=0x10FFFF).contains(&value);
                if !allowed {
                    return self.yaml_parser_set_reader_error(
                        "control characters are not allowed",
                        self.offset,
                        value as i32,
                    );
                }

                // Move the raw pointers.
                self.raw_buffer_pos += width;
                self.offset += width;

                // Finally put the character into the buffer.
                if value <= 0x7F {
                    // 0000 0000-0000 007F . 0xxxxxxx
                    self.buffer[buffer_len] = value as u8;
                    buffer_len += 1;
                } else if value <= 0x7FF {
                    // 0000 0080-0000 07FF . 110xxxxx 10xxxxxx
                    self.buffer[buffer_len] = (0xC0 + (value >> 6)) as u8;
                    self.buffer[buffer_len + 1] = (0x80 + (value & 0x3F)) as u8;
                    buffer_len += 2;
                } else if value <= 0xFFFF {
                    // 0000 0800-0000 FFFF . 1110xxxx 10xxxxxx 10xxxxxx
                    self.buffer[buffer_len] = (0xE0 + (value >> 12)) as u8;
                    self.buffer[buffer_len + 1] = (0x80 + ((value >> 6) & 0x3F)) as u8;
                    self.buffer[buffer_len + 2] = (0x80 + (value & 0x3F)) as u8;
                    buffer_len += 3;
                } else {
                    // 0001 0000-0010 FFFF . 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
                    self.buffer[buffer_len] = (0xF0 + (value >> 18)) as u8;
                    self.buffer[buffer_len + 1] = (0x80 + ((value >> 12) & 0x3F)) as u8;
                    self.buffer[buffer_len + 2] = (0x80 + ((value >> 6) & 0x3F)) as u8;
                    self.buffer[buffer_len + 3] = (0x80 + (value & 0x3F)) as u8;
                    buffer_len += 4;
                }

                self.unread += 1;
            }

            // On EOF, put NUL into the buffer and return.
            if self.eof {
                self.buffer[buffer_len] = 0;
                buffer_len += 1;
                self.unread += 1;
                break;
            }
        }
        // [Go] Read the documentation of this function above. To return true,
        // we need to have the given length in the buffer.
        while buffer_len < length {
            self.buffer[buffer_len] = 0;
            buffer_len += 1;
        }
        self.buffer_len = buffer_len;
        true
    }

    // -----------------------------------------------------------------------
    // scannerc.go
    // -----------------------------------------------------------------------

    // Ensure that the buffer contains the required number of characters.
    // [Go] This was inlined: !cache(A, B) -> unread < B && !update(A, B)
    fn cache(&mut self, length: usize) -> bool {
        self.unread >= length as isize || self.yaml_parser_update_buffer(length)
    }

    // Advance the buffer pointer.
    fn skip(&mut self) {
        if !is_blank(&self.buffer, self.buffer_pos) {
            self.newlines = 0;
        }
        self.mark.index += 1;
        self.mark.column += 1;
        self.unread -= 1;
        self.buffer_pos += width(self.buffer[self.buffer_pos]);
    }

    fn skip_line(&mut self) {
        if is_crlf(&self.buffer, self.buffer_pos) {
            self.mark.index += 2;
            self.mark.column = 0;
            self.mark.line += 1;
            self.unread -= 2;
            self.buffer_pos += 2;
            self.newlines += 1;
        } else if is_break(&self.buffer, self.buffer_pos) {
            self.mark.index += 1;
            self.mark.column = 0;
            self.mark.line += 1;
            self.unread -= 1;
            self.buffer_pos += width(self.buffer[self.buffer_pos]);
            self.newlines += 1;
        }
    }

    // Copy a character to a string buffer and advance pointers.
    fn read(&mut self, s: &mut Vec<u8>) {
        if !is_blank(&self.buffer, self.buffer_pos) {
            self.newlines = 0;
        }
        let w = width(self.buffer[self.buffer_pos]);
        assert!(w != 0, "invalid character sequence");
        s.extend_from_slice(&self.buffer[self.buffer_pos..self.buffer_pos + w]);
        self.buffer_pos += w;
        self.mark.index += 1;
        self.mark.column += 1;
        self.unread -= 1;
    }

    // Copy a line break character to a string buffer and advance pointers.
    fn read_line(&mut self, s: &mut Vec<u8>) {
        let buf = &self.buffer;
        let pos = self.buffer_pos;
        if buf[pos] == b'\r' && buf[pos + 1] == b'\n' {
            // CR LF . LF
            s.push(b'\n');
            self.buffer_pos += 2;
            self.mark.index += 1;
            self.unread -= 1;
        } else if buf[pos] == b'\r' || buf[pos] == b'\n' {
            // CR|LF . LF
            s.push(b'\n');
            self.buffer_pos += 1;
        } else if buf[pos] == 0xC2 && buf[pos + 1] == 0x85 {
            // NEL . LF
            s.push(b'\n');
            self.buffer_pos += 2;
        } else if buf[pos] == 0xE2
            && buf[pos + 1] == 0x80
            && (buf[pos + 2] == 0xA8 || buf[pos + 2] == 0xA9)
        {
            // LS|PS . LS|PS
            s.extend_from_slice(&buf[pos..pos + 3]);
            self.buffer_pos += 3;
        } else {
            return;
        }
        self.mark.index += 1;
        self.mark.column = 0;
        self.mark.line += 1;
        self.unread -= 1;
        self.newlines += 1;
    }

    // Current character helpers.
    fn ch(&self) -> u8 {
        self.buffer[self.buffer_pos]
    }

    fn ch_at(&self, k: usize) -> u8 {
        self.buffer[self.buffer_pos + k]
    }

    // Set the scanner error and return false.
    // yaml_parser_set_scanner_error
    fn yaml_parser_set_scanner_error(
        &mut self,
        context: &str,
        context_mark: Mark,
        problem: &str,
    ) -> bool {
        self.error = Some(ErrorKind::Scanner);
        self.context = context.to_string();
        self.context_mark = context_mark;
        self.problem = problem.to_string();
        self.problem_mark = self.mark;
        false
    }

    // yaml_parser_set_scanner_tag_error
    fn yaml_parser_set_scanner_tag_error(
        &mut self,
        directive: bool,
        context_mark: Mark,
        problem: &str,
    ) -> bool {
        let context = if directive {
            "while parsing a %TAG directive"
        } else {
            "while parsing a tag"
        };
        self.yaml_parser_set_scanner_error(context, context_mark, problem)
    }

    // yaml_insert_token (apic.go), on an exact model of the Go slice.
    fn yaml_insert_token(&mut self, pos: isize, token: Token) {
        // Check if we can move the queue at the beginning of the buffer.
        if self.tokens_head > 0 && self.tokens_len == self.tokens.len() {
            if self.tokens_head != self.tokens_len {
                // copy(parser.tokens, parser.tokens[parser.tokens_head:])
                for i in 0..self.tokens_len - self.tokens_head {
                    self.tokens[i] = self.tokens[self.tokens_head + i].clone();
                }
            }
            self.tokens_len -= self.tokens_head;
            self.tokens_head = 0;
        }
        // parser.tokens = append(parser.tokens, *token)
        if self.tokens_len == self.tokens.len() {
            let new_cap = go_token_slice_grow(self.tokens.len());
            if let Some(w) = &mut self.token_watch
                && w.array_gen == self.tokens_gen
                && w.frozen.is_none()
            {
                w.frozen = Some(self.tokens[w.idx].start_mark);
            }
            let zero = Token::new(TokenType::NoToken, Mark::default(), Mark::default());
            self.tokens.truncate(self.tokens_len);
            self.tokens.resize(new_cap, zero);
            self.tokens_gen += 1;
        }
        self.tokens[self.tokens_len] = token.clone();
        self.tokens_len += 1;
        if pos < 0 {
            return;
        }
        // copy(parser.tokens[parser.tokens_head+pos+1:], parser.tokens[parser.tokens_head+pos:])
        let at = self.tokens_head + pos as usize;
        let mut i = self.tokens_len - 1;
        while i > at {
            self.tokens[i] = self.tokens[i - 1].clone();
            i -= 1;
        }
        self.tokens[at] = token;
    }

    // Reads `token.start_mark` through a Go *yaml_token_t taken earlier.
    fn watched_token_start_mark(&self, w: TokenWatch) -> Mark {
        match w.frozen {
            Some(m) => m,
            None if w.array_gen == self.tokens_gen => self.tokens[w.idx].start_mark,
            None => unreachable!("abandoned backing array without frozen value"),
        }
    }

    // Ensure that the tokens queue contains at least one token which can be
    // returned to the Parser.
    // yaml_parser_fetch_more_tokens
    fn yaml_parser_fetch_more_tokens(&mut self) -> bool {
        // While we need more tokens to fetch, do it.
        loop {
            // [Go] The comment parsing logic requires a lookahead of two tokens
            // so that foot comments may be parsed in time of associating them
            // with the tokens that are parsed before them, and also for line
            // comments to be transformed into head comments in some edge cases.
            if self.tokens_head + 2 < self.tokens_len {
                // If a potential simple key is at the head position, we need to fetch
                // the next token to disambiguate it.
                match self.simple_keys_by_tok.get(&self.tokens_parsed) {
                    None => break,
                    Some(&head_tok_idx) => {
                        let (valid, ok) = self.yaml_simple_key_is_valid(head_tok_idx);
                        if !ok {
                            return false;
                        } else if !valid {
                            break;
                        }
                    }
                }
            }
            // Fetch the next token.
            if !self.yaml_parser_fetch_next_token() {
                return false;
            }
        }

        self.token_available = true;
        true
    }

    // The dispatcher for token fetchers.
    // yaml_parser_fetch_next_token
    fn yaml_parser_fetch_next_token(&mut self) -> bool {
        // Ensure that the buffer is initialized.
        if !self.cache(1) {
            return false;
        }

        // Check if we just started scanning.  Fetch STREAM-START then.
        if !self.stream_start_produced {
            return self.yaml_parser_fetch_stream_start();
        }

        let scan_mark = self.mark;

        // Eat whitespaces and comments until we reach the next token.
        if !self.yaml_parser_scan_to_next_token() {
            return false;
        }

        // [Go] While unrolling indents, transform the head comments of prior
        // indentation levels observed after scan_start into foot comments at
        // the respective indexes.

        // Check the indentation level against the current column.
        if !self.yaml_parser_unroll_indent(self.mark.column as isize, scan_mark) {
            return false;
        }

        // Ensure that the buffer contains at least 4 characters.  4 is the length
        // of the longest indicators ('--- ' and '... ').
        if !self.cache(4) {
            return false;
        }

        // Is it the end of the stream?
        if is_z(&self.buffer, self.buffer_pos) {
            return self.yaml_parser_fetch_stream_end();
        }

        // Is it a directive?
        if self.mark.column == 0 && self.ch() == b'%' {
            return self.yaml_parser_fetch_directive();
        }

        let buf = &self.buffer;
        let pos = self.buffer_pos;

        // Is it the document start indicator?
        if self.mark.column == 0
            && buf[pos] == b'-'
            && buf[pos + 1] == b'-'
            && buf[pos + 2] == b'-'
            && is_blankz(buf, pos + 3)
        {
            return self.yaml_parser_fetch_document_indicator(TokenType::DocumentStart);
        }

        // Is it the document end indicator?
        if self.mark.column == 0
            && buf[pos] == b'.'
            && buf[pos + 1] == b'.'
            && buf[pos + 2] == b'.'
            && is_blankz(buf, pos + 3)
        {
            return self.yaml_parser_fetch_document_indicator(TokenType::DocumentEnd);
        }

        let mut comment_mark = self.mark;
        if self.tokens_len > 0
            && (self.flow_level == 0 && buf[pos] == b':' || self.flow_level > 0 && buf[pos] == b',')
        {
            // Associate any following comments with the prior token.
            comment_mark = self.tokens[self.tokens_len - 1].start_mark;
        }

        let ok = self.yaml_parser_fetch_next_token_dispatch();

        // [Go] defer func() { ... }()
        if !ok {
            return false;
        }
        if self.tokens_len > 0 && self.tokens[self.tokens_len - 1].typ == TokenType::BlockEntry {
            // Sequence indicators alone have no line comments. It becomes
            // a head comment for whatever follows.
            return true;
        }
        if !self.yaml_parser_scan_line_comment(comment_mark) {
            return false;
        }
        true
    }

    // The part of yaml_parser_fetch_next_token after the deferred line comment
    // scanner was registered.
    fn yaml_parser_fetch_next_token_dispatch(&mut self) -> bool {
        let c = self.ch();

        // Is it the flow sequence start indicator?
        if c == b'[' {
            return self.yaml_parser_fetch_flow_collection_start(TokenType::FlowSequenceStart);
        }

        // Is it the flow mapping start indicator?
        if c == b'{' {
            return self.yaml_parser_fetch_flow_collection_start(TokenType::FlowMappingStart);
        }

        // Is it the flow sequence end indicator?
        if c == b']' {
            return self.yaml_parser_fetch_flow_collection_end(TokenType::FlowSequenceEnd);
        }

        // Is it the flow mapping end indicator?
        if c == b'}' {
            return self.yaml_parser_fetch_flow_collection_end(TokenType::FlowMappingEnd);
        }

        // Is it the flow entry indicator?
        if c == b',' {
            return self.yaml_parser_fetch_flow_entry();
        }

        // Is it the block entry indicator?
        if c == b'-' && is_blankz(&self.buffer, self.buffer_pos + 1) {
            return self.yaml_parser_fetch_block_entry();
        }

        // Is it the key indicator?
        if c == b'?' && (self.flow_level > 0 || is_blankz(&self.buffer, self.buffer_pos + 1)) {
            return self.yaml_parser_fetch_key();
        }

        // Is it the value indicator?
        if c == b':' && (self.flow_level > 0 || is_blankz(&self.buffer, self.buffer_pos + 1)) {
            return self.yaml_parser_fetch_value();
        }

        // Is it an alias?
        if c == b'*' {
            return self.yaml_parser_fetch_anchor(TokenType::Alias);
        }

        // Is it an anchor?
        if c == b'&' {
            return self.yaml_parser_fetch_anchor(TokenType::Anchor);
        }

        // Is it a tag?
        if c == b'!' {
            return self.yaml_parser_fetch_tag();
        }

        // Is it a literal scalar?
        if c == b'|' && self.flow_level == 0 {
            return self.yaml_parser_fetch_block_scalar(true);
        }

        // Is it a folded scalar?
        if c == b'>' && self.flow_level == 0 {
            return self.yaml_parser_fetch_block_scalar(false);
        }

        // Is it a single-quoted scalar?
        if c == b'\'' {
            return self.yaml_parser_fetch_flow_scalar(true);
        }

        // Is it a double-quoted scalar?
        if c == b'"' {
            return self.yaml_parser_fetch_flow_scalar(false);
        }

        // Is it a plain scalar?
        //
        // A plain scalar may start with any non-blank characters except
        //
        //      '-', '?', ':', ',', '[', ']', '{', '}',
        //      '#', '&', '*', '!', '|', '>', '\'', '\"',
        //      '%', '@', '`'.
        //
        // In the block context (and, for the '-' indicator, in the flow context
        // too), it may also start with the characters
        //
        //      '-', '?', ':'
        //
        // if it is followed by a non-space character.
        //
        // The last rule is more restrictive than the specification requires.
        let b = &self.buffer;
        let p = self.buffer_pos;
        if !(is_blankz(b, p)
            || c == b'-'
            || c == b'?'
            || c == b':'
            || c == b','
            || c == b'['
            || c == b']'
            || c == b'{'
            || c == b'}'
            || c == b'#'
            || c == b'&'
            || c == b'*'
            || c == b'!'
            || c == b'|'
            || c == b'>'
            || c == b'\''
            || c == b'"'
            || c == b'%'
            || c == b'@'
            || c == b'`')
            || (c == b'-' && !is_blank(b, p + 1))
            || (self.flow_level == 0 && (c == b'?' || c == b':') && !is_blankz(b, p + 1))
        {
            return self.yaml_parser_fetch_plain_scalar();
        }

        // If we don't determine the token type so far, it is an error.
        let mark = self.mark;
        self.yaml_parser_set_scanner_error(
            "while scanning for the next token",
            mark,
            "found character that cannot start any token",
        )
    }

    // yaml_simple_key_is_valid (operates on parser.simple_keys[idx])
    fn yaml_simple_key_is_valid(&mut self, idx: usize) -> (bool, bool) {
        let simple_key = self.simple_keys[idx];
        if !simple_key.possible {
            return (false, true);
        }

        // The 1.2 specification says:
        //
        //     "If the ? indicator is omitted, parsing needs to see past the
        //     implicit key to recognize it as such. To limit the amount of
        //     lookahead required, the “:” indicator must appear at most 1024
        //     Unicode characters beyond the start of the key. In addition, the key
        //     is restricted to a single line."
        //
        if simple_key.mark.line < self.mark.line || simple_key.mark.index + 1024 < self.mark.index {
            // Check if the potential simple key to be removed is required.
            if simple_key.required {
                return (
                    false,
                    self.yaml_parser_set_scanner_error(
                        "while scanning a simple key",
                        simple_key.mark,
                        "could not find expected ':'",
                    ),
                );
            }
            self.simple_keys[idx].possible = false;
            return (false, true);
        }
        (true, true)
    }

    // Check if a simple key may start at the current position and add it if
    // needed.
    // yaml_parser_save_simple_key
    fn yaml_parser_save_simple_key(&mut self) -> bool {
        // A simple key is required at the current position if the scanner is in
        // the block context and the current column coincides with the indentation
        // level.

        let required = self.flow_level == 0 && self.indent == self.mark.column as isize;

        //
        // If the current position may start a simple key, save it.
        //
        if self.simple_key_allowed {
            let simple_key = SimpleKey {
                possible: true,
                required,
                token_number: self.tokens_parsed + (self.tokens_len - self.tokens_head),
                mark: self.mark,
            };

            if !self.yaml_parser_remove_simple_key() {
                return false;
            }
            let last = self.simple_keys.len() - 1;
            self.simple_keys[last] = simple_key;
            self.simple_keys_by_tok
                .insert(simple_key.token_number, last);
        }
        true
    }

    // Remove a potential simple key at the current flow level.
    // yaml_parser_remove_simple_key
    fn yaml_parser_remove_simple_key(&mut self) -> bool {
        let i = self.simple_keys.len() - 1;
        if self.simple_keys[i].possible {
            // If the key is required, it is an error.
            if self.simple_keys[i].required {
                let mark = self.simple_keys[i].mark;
                return self.yaml_parser_set_scanner_error(
                    "while scanning a simple key",
                    mark,
                    "could not find expected ':'",
                );
            }
            // Remove the key from the stack.
            self.simple_keys[i].possible = false;
            let tn = self.simple_keys[i].token_number;
            self.simple_keys_by_tok.remove(&tn);
        }
        true
    }

    // Increase the flow level and resize the simple key list if needed.
    // yaml_parser_increase_flow_level
    fn yaml_parser_increase_flow_level(&mut self) -> bool {
        // Reset the simple key on the next level.
        self.simple_keys.push(SimpleKey {
            possible: false,
            required: false,
            token_number: self.tokens_parsed + (self.tokens_len - self.tokens_head),
            mark: self.mark,
        });

        // Increase the flow level.
        self.flow_level += 1;
        if self.flow_level > MAX_FLOW_LEVEL {
            let mark = self.simple_keys[self.simple_keys.len() - 1].mark;
            return self.yaml_parser_set_scanner_error(
                "while increasing flow level",
                mark,
                &format!("exceeded max depth of {MAX_FLOW_LEVEL}"),
            );
        }
        true
    }

    // Decrease the flow level.
    // yaml_parser_decrease_flow_level
    fn yaml_parser_decrease_flow_level(&mut self) -> bool {
        if self.flow_level > 0 {
            self.flow_level -= 1;
            let last = self.simple_keys.len() - 1;
            let tn = self.simple_keys[last].token_number;
            self.simple_keys_by_tok.remove(&tn);
            self.simple_keys.truncate(last);
        }
        true
    }

    // Push the current indentation level to the stack and set the new level
    // the current column is greater than the indentation level.  In this case,
    // append or insert the specified token into the token queue.
    // yaml_parser_roll_indent
    fn yaml_parser_roll_indent(
        &mut self,
        column: isize,
        number: isize,
        typ: TokenType,
        mark: Mark,
    ) -> bool {
        // In the flow context, do nothing.
        if self.flow_level > 0 {
            return true;
        }

        if self.indent < column {
            // Push the current indentation level to the stack and set the new
            // indentation level.
            self.indents.push(self.indent);
            self.indent = column;
            if self.indents.len() > MAX_INDENTS {
                let mark = self.simple_keys[self.simple_keys.len() - 1].mark;
                return self.yaml_parser_set_scanner_error(
                    "while increasing indent level",
                    mark,
                    &format!("exceeded max depth of {MAX_INDENTS}"),
                );
            }

            // Create a token and insert it into the queue.
            let token = Token::new(typ, mark, mark);
            let mut number = number;
            if number > -1 {
                number -= self.tokens_parsed as isize;
            }
            self.yaml_insert_token(number, token);
        }
        true
    }

    // Pop indentation levels from the indents stack until the current level
    // becomes less or equal to the column.  For each indentation level, append
    // the BLOCK-END token.
    // yaml_parser_unroll_indent
    fn yaml_parser_unroll_indent(&mut self, column: isize, scan_mark: Mark) -> bool {
        // In the flow context, do nothing.
        if self.flow_level > 0 {
            return true;
        }

        let mut block_mark = scan_mark;
        block_mark.index = block_mark.index.wrapping_sub(1);

        // Loop through the indentation levels in the stack.
        while self.indent > column {
            // [Go] Reposition the end token before potential following
            //      foot comments of parent blocks. For that, search
            //      backwards for recent comments that were at the same
            //      indent as the block that is ending now.
            let mut stop_index = block_mark.index;
            for i in (0..self.comments.len()).rev() {
                let comment = &self.comments[i];

                if comment.end_mark.index < stop_index {
                    // Don't go back beyond the start of the comment/whitespace scan, unless column < 0.
                    // If requested indent column is < 0, then the document is over and everything else
                    // is a foot anyway.
                    break;
                }
                if comment.start_mark.column as isize == self.indent + 1 {
                    // This is a good match. But maybe there's a former comment
                    // at that same indent level, so keep searching.
                    block_mark = comment.start_mark;
                }

                // While the end of the former comment matches with
                // the start of the following one, we know there's
                // nothing in between and scanning is still safe.
                stop_index = comment.scan_mark.index;
            }

            // Create a token and append it to the queue.
            let token = Token::new(TokenType::BlockEnd, block_mark, block_mark);
            self.yaml_insert_token(-1, token);

            // Pop the indentation level.
            self.indent = self.indents.pop().unwrap_or(-1);
        }
        true
    }

    // Initialize the scanner and produce the STREAM-START token.
    // yaml_parser_fetch_stream_start
    fn yaml_parser_fetch_stream_start(&mut self) -> bool {
        // Set the initial indentation.
        self.indent = -1;

        // Initialize the simple key stack.
        self.simple_keys.push(SimpleKey::default());

        self.simple_keys_by_tok = HashMap::new();

        // A simple key is allowed at the beginning of the stream.
        self.simple_key_allowed = true;

        // We have started.
        self.stream_start_produced = true;

        // Create the STREAM-START token and append it to the queue.
        let token = Token::new(TokenType::StreamStart, self.mark, self.mark);
        self.yaml_insert_token(-1, token);
        true
    }

    // Produce the STREAM-END token and shut down the scanner.
    // yaml_parser_fetch_stream_end
    fn yaml_parser_fetch_stream_end(&mut self) -> bool {
        // Force new line.
        if self.mark.column != 0 {
            self.mark.column = 0;
            self.mark.line += 1;
        }

        // Reset the indentation level.
        if !self.yaml_parser_unroll_indent(-1, self.mark) {
            return false;
        }

        // Reset simple keys.
        if !self.yaml_parser_remove_simple_key() {
            return false;
        }

        self.simple_key_allowed = false;

        // Create the STREAM-END token and append it to the queue.
        let token = Token::new(TokenType::StreamEnd, self.mark, self.mark);
        self.yaml_insert_token(-1, token);
        true
    }

    // Produce a VERSION-DIRECTIVE or TAG-DIRECTIVE token.
    // yaml_parser_fetch_directive
    fn yaml_parser_fetch_directive(&mut self) -> bool {
        // Reset the indentation level.
        if !self.yaml_parser_unroll_indent(-1, self.mark) {
            return false;
        }

        // Reset simple keys.
        if !self.yaml_parser_remove_simple_key() {
            return false;
        }

        self.simple_key_allowed = false;

        // Create the YAML-DIRECTIVE or TAG-DIRECTIVE token.
        let mut token = Token::new(TokenType::NoToken, Mark::default(), Mark::default());
        if !self.yaml_parser_scan_directive(&mut token) {
            return false;
        }
        // Append the token to the queue.
        self.yaml_insert_token(-1, token);
        true
    }

    // Produce the DOCUMENT-START or DOCUMENT-END token.
    // yaml_parser_fetch_document_indicator
    fn yaml_parser_fetch_document_indicator(&mut self, typ: TokenType) -> bool {
        // Reset the indentation level.
        if !self.yaml_parser_unroll_indent(-1, self.mark) {
            return false;
        }

        // Reset simple keys.
        if !self.yaml_parser_remove_simple_key() {
            return false;
        }

        self.simple_key_allowed = false;

        // Consume the token.
        let start_mark = self.mark;

        self.skip();
        self.skip();
        self.skip();

        let end_mark = self.mark;

        // Create the DOCUMENT-START or DOCUMENT-END token.
        let token = Token::new(typ, start_mark, end_mark);
        // Append the token to the queue.
        self.yaml_insert_token(-1, token);
        true
    }

    // Produce the FLOW-SEQUENCE-START or FLOW-MAPPING-START token.
    // yaml_parser_fetch_flow_collection_start
    fn yaml_parser_fetch_flow_collection_start(&mut self, typ: TokenType) -> bool {
        // The indicators '[' and '{' may start a simple key.
        if !self.yaml_parser_save_simple_key() {
            return false;
        }

        // Increase the flow level.
        if !self.yaml_parser_increase_flow_level() {
            return false;
        }

        // A simple key may follow the indicators '[' and '{'.
        self.simple_key_allowed = true;

        // Consume the token.
        let start_mark = self.mark;
        self.skip();
        let end_mark = self.mark;

        // Create the FLOW-SEQUENCE-START of FLOW-MAPPING-START token.
        let token = Token::new(typ, start_mark, end_mark);
        // Append the token to the queue.
        self.yaml_insert_token(-1, token);
        true
    }

    // Produce the FLOW-SEQUENCE-END or FLOW-MAPPING-END token.
    // yaml_parser_fetch_flow_collection_end
    fn yaml_parser_fetch_flow_collection_end(&mut self, typ: TokenType) -> bool {
        // Reset any potential simple key on the current flow level.
        if !self.yaml_parser_remove_simple_key() {
            return false;
        }

        // Decrease the flow level.
        if !self.yaml_parser_decrease_flow_level() {
            return false;
        }

        // No simple keys after the indicators ']' and '}'.
        self.simple_key_allowed = false;

        // Consume the token.

        let start_mark = self.mark;
        self.skip();
        let end_mark = self.mark;

        // Create the FLOW-SEQUENCE-END of FLOW-MAPPING-END token.
        let token = Token::new(typ, start_mark, end_mark);
        // Append the token to the queue.
        self.yaml_insert_token(-1, token);
        true
    }

    // Produce the FLOW-ENTRY token.
    // yaml_parser_fetch_flow_entry
    fn yaml_parser_fetch_flow_entry(&mut self) -> bool {
        // Reset any potential simple keys on the current flow level.
        if !self.yaml_parser_remove_simple_key() {
            return false;
        }

        // Simple keys are allowed after ','.
        self.simple_key_allowed = true;

        // Consume the token.
        let start_mark = self.mark;
        self.skip();
        let end_mark = self.mark;

        // Create the FLOW-ENTRY token and append it to the queue.
        let token = Token::new(TokenType::FlowEntry, start_mark, end_mark);
        self.yaml_insert_token(-1, token);
        true
    }

    // Produce the BLOCK-ENTRY token.
    // yaml_parser_fetch_block_entry
    fn yaml_parser_fetch_block_entry(&mut self) -> bool {
        // Check if the scanner is in the block context.
        if self.flow_level == 0 {
            // Check if we are allowed to start a new entry.
            if !self.simple_key_allowed {
                let mark = self.mark;
                return self.yaml_parser_set_scanner_error(
                    "",
                    mark,
                    "block sequence entries are not allowed in this context",
                );
            }
            // Add the BLOCK-SEQUENCE-START token if needed.
            let mark = self.mark;
            if !self.yaml_parser_roll_indent(
                mark.column as isize,
                -1,
                TokenType::BlockSequenceStart,
                mark,
            ) {
                return false;
            }
        } else {
            // It is an error for the '-' indicator to occur in the flow context,
            // but we let the Parser detect and report about it because the Parser
            // is able to point to the context.
        }

        // Reset any potential simple keys on the current flow level.
        if !self.yaml_parser_remove_simple_key() {
            return false;
        }

        // Simple keys are allowed after '-'.
        self.simple_key_allowed = true;

        // Consume the token.
        let start_mark = self.mark;
        self.skip();
        let end_mark = self.mark;

        // Create the BLOCK-ENTRY token and append it to the queue.
        let token = Token::new(TokenType::BlockEntry, start_mark, end_mark);
        self.yaml_insert_token(-1, token);
        true
    }

    // Produce the KEY token.
    // yaml_parser_fetch_key
    fn yaml_parser_fetch_key(&mut self) -> bool {
        // In the block context, additional checks are required.
        if self.flow_level == 0 {
            // Check if we are allowed to start a new key (not nessesary simple).
            if !self.simple_key_allowed {
                let mark = self.mark;
                return self.yaml_parser_set_scanner_error(
                    "",
                    mark,
                    "mapping keys are not allowed in this context",
                );
            }
            // Add the BLOCK-MAPPING-START token if needed.
            let mark = self.mark;
            if !self.yaml_parser_roll_indent(
                mark.column as isize,
                -1,
                TokenType::BlockMappingStart,
                mark,
            ) {
                return false;
            }
        }

        // Reset any potential simple keys on the current flow level.
        if !self.yaml_parser_remove_simple_key() {
            return false;
        }

        // Simple keys are allowed after '?' in the block context.
        self.simple_key_allowed = self.flow_level == 0;

        // Consume the token.
        let start_mark = self.mark;
        self.skip();
        let end_mark = self.mark;

        // Create the KEY token and append it to the queue.
        let token = Token::new(TokenType::Key, start_mark, end_mark);
        self.yaml_insert_token(-1, token);
        true
    }

    // Produce the VALUE token.
    // yaml_parser_fetch_value
    fn yaml_parser_fetch_value(&mut self) -> bool {
        let idx = self.simple_keys.len() - 1;

        // Have we found a simple key?
        let (valid, ok) = self.yaml_simple_key_is_valid(idx);
        if !ok {
            return false;
        } else if valid {
            let simple_key = self.simple_keys[idx];

            // Create the KEY token and insert it into the queue.
            let token = Token::new(TokenType::Key, simple_key.mark, simple_key.mark);
            self.yaml_insert_token(
                simple_key.token_number as isize - self.tokens_parsed as isize,
                token,
            );

            // In the block context, we may need to add the BLOCK-MAPPING-START token.
            if !self.yaml_parser_roll_indent(
                simple_key.mark.column as isize,
                simple_key.token_number as isize,
                TokenType::BlockMappingStart,
                simple_key.mark,
            ) {
                return false;
            }

            // Remove the simple key.
            self.simple_keys[idx].possible = false;
            self.simple_keys_by_tok.remove(&simple_key.token_number);

            // A simple key cannot follow another simple key.
            self.simple_key_allowed = false;
        } else {
            // The ':' indicator follows a complex key.

            // In the block context, extra checks are required.
            if self.flow_level == 0 {
                // Check if we are allowed to start a complex value.
                if !self.simple_key_allowed {
                    let mark = self.mark;
                    return self.yaml_parser_set_scanner_error(
                        "",
                        mark,
                        "mapping values are not allowed in this context",
                    );
                }

                // Add the BLOCK-MAPPING-START token if needed.
                let mark = self.mark;
                if !self.yaml_parser_roll_indent(
                    mark.column as isize,
                    -1,
                    TokenType::BlockMappingStart,
                    mark,
                ) {
                    return false;
                }
            }

            // Simple keys after ':' are allowed in the block context.
            self.simple_key_allowed = self.flow_level == 0;
        }

        // Consume the token.
        let start_mark = self.mark;
        self.skip();
        let end_mark = self.mark;

        // Create the VALUE token and append it to the queue.
        let token = Token::new(TokenType::Value, start_mark, end_mark);
        self.yaml_insert_token(-1, token);
        true
    }

    // Produce the ALIAS or ANCHOR token.
    // yaml_parser_fetch_anchor
    fn yaml_parser_fetch_anchor(&mut self, typ: TokenType) -> bool {
        // An anchor or an alias could be a simple key.
        if !self.yaml_parser_save_simple_key() {
            return false;
        }

        // A simple key cannot follow an anchor or an alias.
        self.simple_key_allowed = false;

        // Create the ALIAS or ANCHOR token and append it to the queue.
        let mut token = Token::new(TokenType::NoToken, Mark::default(), Mark::default());
        if !self.yaml_parser_scan_anchor(&mut token, typ) {
            return false;
        }
        self.yaml_insert_token(-1, token);
        true
    }

    // Produce the TAG token.
    // yaml_parser_fetch_tag
    fn yaml_parser_fetch_tag(&mut self) -> bool {
        // A tag could be a simple key.
        if !self.yaml_parser_save_simple_key() {
            return false;
        }

        // A simple key cannot follow a tag.
        self.simple_key_allowed = false;

        // Create the TAG token and append it to the queue.
        let mut token = Token::new(TokenType::NoToken, Mark::default(), Mark::default());
        if !self.yaml_parser_scan_tag(&mut token) {
            return false;
        }
        self.yaml_insert_token(-1, token);
        true
    }

    // Produce the SCALAR(...,literal) or SCALAR(...,folded) tokens.
    // yaml_parser_fetch_block_scalar
    fn yaml_parser_fetch_block_scalar(&mut self, literal: bool) -> bool {
        // Remove any potential simple keys.
        if !self.yaml_parser_remove_simple_key() {
            return false;
        }

        // A simple key may follow a block scalar.
        self.simple_key_allowed = true;

        // Create the SCALAR token and append it to the queue.
        let mut token = Token::new(TokenType::NoToken, Mark::default(), Mark::default());
        if !self.yaml_parser_scan_block_scalar(&mut token, literal) {
            return false;
        }
        self.yaml_insert_token(-1, token);
        true
    }

    // Produce the SCALAR(...,single-quoted) or SCALAR(...,double-quoted) tokens.
    // yaml_parser_fetch_flow_scalar
    fn yaml_parser_fetch_flow_scalar(&mut self, single: bool) -> bool {
        // A plain scalar could be a simple key.
        if !self.yaml_parser_save_simple_key() {
            return false;
        }

        // A simple key cannot follow a flow scalar.
        self.simple_key_allowed = false;

        // Create the SCALAR token and append it to the queue.
        let mut token = Token::new(TokenType::NoToken, Mark::default(), Mark::default());
        if !self.yaml_parser_scan_flow_scalar(&mut token, single) {
            return false;
        }
        self.yaml_insert_token(-1, token);
        true
    }

    // Produce the SCALAR(...,plain) token.
    // yaml_parser_fetch_plain_scalar
    fn yaml_parser_fetch_plain_scalar(&mut self) -> bool {
        // A plain scalar could be a simple key.
        if !self.yaml_parser_save_simple_key() {
            return false;
        }

        // A simple key cannot follow a flow scalar.
        self.simple_key_allowed = false;

        // Create the SCALAR token and append it to the queue.
        let mut token = Token::new(TokenType::NoToken, Mark::default(), Mark::default());
        if !self.yaml_parser_scan_plain_scalar(&mut token) {
            return false;
        }
        self.yaml_insert_token(-1, token);
        true
    }

    // Eat whitespaces and comments until the next token is found.
    // yaml_parser_scan_to_next_token
    fn yaml_parser_scan_to_next_token(&mut self) -> bool {
        let scan_mark = self.mark;

        // Until the next token is not found.
        loop {
            // Allow the BOM mark to start a line.
            if !self.cache(1) {
                return false;
            }
            if self.mark.column == 0 && is_bom(&self.buffer) {
                self.skip();
            }

            // Eat whitespaces.
            // Tabs are allowed:
            //  - in the flow context
            //  - in the block context, but not at the beginning of the line or
            //  after '-', '?', or ':' (complex value).
            if !self.cache(1) {
                return false;
            }

            while self.ch() == b' '
                || ((self.flow_level > 0 || !self.simple_key_allowed) && self.ch() == b'\t')
            {
                self.skip();
                if !self.cache(1) {
                    return false;
                }
            }

            // Check if we just had a line comment under a sequence entry that
            // looks more like a header to the following content. Similar to this:
            //
            // - # The comment
            //   - Some data
            //
            // If so, transform the line comment to a head comment and reposition.
            if !self.comments.is_empty() && self.tokens_len > 1 {
                let token_a = self.tokens[self.tokens_len - 2].typ;
                let token_b = self.tokens[self.tokens_len - 1].typ;
                let ci = self.comments.len() - 1;
                if token_a == TokenType::BlockSequenceStart
                    && token_b == TokenType::BlockEntry
                    && !self.comments[ci].line.is_empty()
                    && !is_break(&self.buffer, self.buffer_pos)
                {
                    // If it was in the prior line, reposition so it becomes a
                    // header of the follow up token. Otherwise, keep it in place
                    // so it becomes a header of the former.
                    let mark = self.mark;
                    let comment = &mut self.comments[ci];
                    comment.head = std::mem::take(&mut comment.line);
                    if comment.start_mark.line as isize == mark.line as isize - 1 {
                        comment.token_mark = mark;
                    }
                }
            }

            // Eat a comment until a line break.
            if self.ch() == b'#' && !self.yaml_parser_scan_comments(scan_mark) {
                return false;
            }

            // If it is a line break, eat it.
            if is_break(&self.buffer, self.buffer_pos) {
                if !self.cache(2) {
                    return false;
                }
                self.skip_line();

                // In the block context, a new line may start a simple key.
                if self.flow_level == 0 {
                    self.simple_key_allowed = true;
                }
            } else {
                break; // We have found a token.
            }
        }

        true
    }

    // Scan a YAML-DIRECTIVE or TAG-DIRECTIVE token.
    //
    // Scope:
    //      %YAML    1.1    # a comment \n
    //      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    //      %TAG    !yaml!  tag:yaml.org,2002:  \n
    //      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    //
    // yaml_parser_scan_directive
    fn yaml_parser_scan_directive(&mut self, token: &mut Token) -> bool {
        // Eat '%'.
        let start_mark = self.mark;
        self.skip();

        // Scan the directive name.
        let mut name = Vec::new();
        if !self.yaml_parser_scan_directive_name(start_mark, &mut name) {
            return false;
        }

        // Is it a YAML directive?
        if name == b"YAML" {
            // Scan the VERSION directive value.
            let mut major = 0;
            let mut minor = 0;
            if !self.yaml_parser_scan_version_directive_value(start_mark, &mut major, &mut minor) {
                return false;
            }
            let end_mark = self.mark;

            // Create a VERSION-DIRECTIVE token.
            *token = Token::new(TokenType::VersionDirective, start_mark, end_mark);
            token.major = major;
            token.minor = minor;

            // Is it a TAG directive?
        } else if name == b"TAG" {
            // Scan the TAG directive value.
            let mut handle = Vec::new();
            let mut prefix = Vec::new();
            if !self.yaml_parser_scan_tag_directive_value(start_mark, &mut handle, &mut prefix) {
                return false;
            }
            let end_mark = self.mark;

            // Create a TAG-DIRECTIVE token.
            *token = Token::new(TokenType::TagDirective, start_mark, end_mark);
            token.value = handle;
            token.prefix = prefix;

            // Unknown directive.
        } else {
            self.yaml_parser_set_scanner_error(
                "while scanning a directive",
                start_mark,
                "found unknown directive name",
            );
            return false;
        }

        // Eat the rest of the line including any comments.
        if !self.cache(1) {
            return false;
        }

        while is_blank(&self.buffer, self.buffer_pos) {
            self.skip();
            if !self.cache(1) {
                return false;
            }
        }

        if self.ch() == b'#' {
            // [Go] Discard this inline comment for the time being.
            while !is_breakz(&self.buffer, self.buffer_pos) {
                self.skip();
                if !self.cache(1) {
                    return false;
                }
            }
        }

        // Check if we are at the end of the line.
        if !is_breakz(&self.buffer, self.buffer_pos) {
            self.yaml_parser_set_scanner_error(
                "while scanning a directive",
                start_mark,
                "did not find expected comment or line break",
            );
            return false;
        }

        // Eat a line break.
        if is_break(&self.buffer, self.buffer_pos) {
            if !self.cache(2) {
                return false;
            }
            self.skip_line();
        }

        true
    }

    // Scan the directive name.
    //
    // Scope:
    //      %YAML   1.1     # a comment \n
    //       ^^^^
    //      %TAG    !yaml!  tag:yaml.org,2002:  \n
    //       ^^^
    //
    // yaml_parser_scan_directive_name
    fn yaml_parser_scan_directive_name(&mut self, start_mark: Mark, name: &mut Vec<u8>) -> bool {
        // Consume the directive name.
        if !self.cache(1) {
            return false;
        }

        let mut s = Vec::new();
        while is_alpha(&self.buffer, self.buffer_pos) {
            self.read(&mut s);
            if !self.cache(1) {
                return false;
            }
        }

        // Check if the name is empty.
        if s.is_empty() {
            self.yaml_parser_set_scanner_error(
                "while scanning a directive",
                start_mark,
                "could not find expected directive name",
            );
            return false;
        }

        // Check for an blank character after the name.
        if !is_blankz(&self.buffer, self.buffer_pos) {
            self.yaml_parser_set_scanner_error(
                "while scanning a directive",
                start_mark,
                "found unexpected non-alphabetical character",
            );
            return false;
        }
        *name = s;
        true
    }

    // Scan the value of VERSION-DIRECTIVE.
    //
    // Scope:
    //      %YAML   1.1     # a comment \n
    //           ^^^^^^
    // yaml_parser_scan_version_directive_value
    fn yaml_parser_scan_version_directive_value(
        &mut self,
        start_mark: Mark,
        major: &mut i8,
        minor: &mut i8,
    ) -> bool {
        // Eat whitespaces.
        if !self.cache(1) {
            return false;
        }
        while is_blank(&self.buffer, self.buffer_pos) {
            self.skip();
            if !self.cache(1) {
                return false;
            }
        }

        // Consume the major version number.
        if !self.yaml_parser_scan_version_directive_number(start_mark, major) {
            return false;
        }

        // Eat '.'.
        if self.ch() != b'.' {
            return self.yaml_parser_set_scanner_error(
                "while scanning a %YAML directive",
                start_mark,
                "did not find expected digit or '.' character",
            );
        }

        self.skip();

        // Consume the minor version number.
        if !self.yaml_parser_scan_version_directive_number(start_mark, minor) {
            return false;
        }
        true
    }

    // Scan the version number of VERSION-DIRECTIVE.
    //
    // Scope:
    //      %YAML   1.1     # a comment \n
    //              ^
    //      %YAML   1.1     # a comment \n
    //                ^
    // yaml_parser_scan_version_directive_number
    fn yaml_parser_scan_version_directive_number(
        &mut self,
        start_mark: Mark,
        number: &mut i8,
    ) -> bool {
        // Repeat while the next character is digit.
        if !self.cache(1) {
            return false;
        }
        let mut value: i8 = 0;
        let mut length: i8 = 0;
        while is_digit(&self.buffer, self.buffer_pos) {
            // Check if the number is too long.
            length += 1;
            if length > MAX_NUMBER_LENGTH {
                return self.yaml_parser_set_scanner_error(
                    "while scanning a %YAML directive",
                    start_mark,
                    "found extremely long version number",
                );
            }
            value = value * 10 + as_digit(&self.buffer, self.buffer_pos) as i8;
            self.skip();
            if !self.cache(1) {
                return false;
            }
        }

        // Check if the number was present.
        if length == 0 {
            return self.yaml_parser_set_scanner_error(
                "while scanning a %YAML directive",
                start_mark,
                "did not find expected version number",
            );
        }
        *number = value;
        true
    }

    // Scan the value of a TAG-DIRECTIVE token.
    //
    // Scope:
    //      %TAG    !yaml!  tag:yaml.org,2002:  \n
    //          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    //
    // yaml_parser_scan_tag_directive_value
    fn yaml_parser_scan_tag_directive_value(
        &mut self,
        start_mark: Mark,
        handle: &mut Vec<u8>,
        prefix: &mut Vec<u8>,
    ) -> bool {
        let mut handle_value = Vec::new();
        let mut prefix_value = Vec::new();

        // Eat whitespaces.
        if !self.cache(1) {
            return false;
        }

        while is_blank(&self.buffer, self.buffer_pos) {
            self.skip();
            if !self.cache(1) {
                return false;
            }
        }

        // Scan a handle.
        if !self.yaml_parser_scan_tag_handle(true, start_mark, &mut handle_value) {
            return false;
        }

        // Expect a whitespace.
        if !self.cache(1) {
            return false;
        }
        if !is_blank(&self.buffer, self.buffer_pos) {
            self.yaml_parser_set_scanner_error(
                "while scanning a %TAG directive",
                start_mark,
                "did not find expected whitespace",
            );
            return false;
        }

        // Eat whitespaces.
        while is_blank(&self.buffer, self.buffer_pos) {
            self.skip();
            if !self.cache(1) {
                return false;
            }
        }

        // Scan a prefix.
        if !self.yaml_parser_scan_tag_uri(true, &[], start_mark, &mut prefix_value) {
            return false;
        }

        // Expect a whitespace or line break.
        if !self.cache(1) {
            return false;
        }
        if !is_blankz(&self.buffer, self.buffer_pos) {
            self.yaml_parser_set_scanner_error(
                "while scanning a %TAG directive",
                start_mark,
                "did not find expected whitespace or line break",
            );
            return false;
        }

        *handle = handle_value;
        *prefix = prefix_value;
        true
    }

    // yaml_parser_scan_anchor
    fn yaml_parser_scan_anchor(&mut self, token: &mut Token, typ: TokenType) -> bool {
        let mut s = Vec::new();

        // Eat the indicator character.
        let start_mark = self.mark;
        self.skip();

        // Consume the value.
        if !self.cache(1) {
            return false;
        }

        while is_alpha(&self.buffer, self.buffer_pos) {
            self.read(&mut s);
            if !self.cache(1) {
                return false;
            }
        }

        let end_mark = self.mark;

        /*
         * Check if length of the anchor is greater than 0 and it is followed by
         * a whitespace character or one of the indicators:
         *
         *      '?', ':', ',', ']', '}', '%', '@', '`'.
         */

        let c = self.ch();
        if s.is_empty()
            || !(is_blankz(&self.buffer, self.buffer_pos)
                || c == b'?'
                || c == b':'
                || c == b','
                || c == b']'
                || c == b'}'
                || c == b'%'
                || c == b'@'
                || c == b'`')
        {
            let context = if typ == TokenType::Anchor {
                "while scanning an anchor"
            } else {
                "while scanning an alias"
            };
            self.yaml_parser_set_scanner_error(
                context,
                start_mark,
                "did not find expected alphabetic or numeric character",
            );
            return false;
        }

        // Create a token.
        *token = Token::new(typ, start_mark, end_mark);
        token.value = s;

        true
    }

    /*
     * Scan a TAG token.
     */

    // yaml_parser_scan_tag
    fn yaml_parser_scan_tag(&mut self, token: &mut Token) -> bool {
        let mut handle = Vec::new();
        let mut suffix = Vec::new();

        let start_mark = self.mark;

        // Check if the tag is in the canonical form.
        if !self.cache(2) {
            return false;
        }

        if self.ch_at(1) == b'<' {
            // Keep the handle as ''

            // Eat '!<'
            self.skip();
            self.skip();

            // Consume the tag value.
            if !self.yaml_parser_scan_tag_uri(false, &[], start_mark, &mut suffix) {
                return false;
            }

            // Check for '>' and eat it.
            if self.ch() != b'>' {
                self.yaml_parser_set_scanner_error(
                    "while scanning a tag",
                    start_mark,
                    "did not find the expected '>'",
                );
                return false;
            }

            self.skip();
        } else {
            // The tag has either the '!suffix' or the '!handle!suffix' form.

            // First, try to scan a handle.
            if !self.yaml_parser_scan_tag_handle(false, start_mark, &mut handle) {
                return false;
            }

            // Check if it is, indeed, handle.
            if handle[0] == b'!' && handle.len() > 1 && handle[handle.len() - 1] == b'!' {
                // Scan the suffix now.
                if !self.yaml_parser_scan_tag_uri(false, &[], start_mark, &mut suffix) {
                    return false;
                }
            } else {
                // It wasn't a handle after all.  Scan the rest of the tag.
                let head = handle.clone();
                if !self.yaml_parser_scan_tag_uri(false, &head, start_mark, &mut suffix) {
                    return false;
                }

                // Set the handle to '!'.
                handle = vec![b'!'];

                // A special case: the '!' tag.  Set the handle to '' and the
                // suffix to '!'.
                if suffix.is_empty() {
                    std::mem::swap(&mut handle, &mut suffix);
                }
            }
        }

        // Check the character which ends the tag.
        if !self.cache(1) {
            return false;
        }
        if !is_blankz(&self.buffer, self.buffer_pos) {
            self.yaml_parser_set_scanner_error(
                "while scanning a tag",
                start_mark,
                "did not find expected whitespace or line break",
            );
            return false;
        }

        let end_mark = self.mark;

        // Create a token.
        *token = Token::new(TokenType::Tag, start_mark, end_mark);
        token.value = handle;
        token.suffix = suffix;
        true
    }

    // Scan a tag handle.
    // yaml_parser_scan_tag_handle
    fn yaml_parser_scan_tag_handle(
        &mut self,
        directive: bool,
        start_mark: Mark,
        handle: &mut Vec<u8>,
    ) -> bool {
        // Check the initial '!' character.
        if !self.cache(1) {
            return false;
        }
        if self.ch() != b'!' {
            self.yaml_parser_set_scanner_tag_error(
                directive,
                start_mark,
                "did not find expected '!'",
            );
            return false;
        }

        let mut s = Vec::new();

        // Copy the '!' character.
        self.read(&mut s);

        // Copy all subsequent alphabetical and numerical characters.
        if !self.cache(1) {
            return false;
        }
        while is_alpha(&self.buffer, self.buffer_pos) {
            self.read(&mut s);
            if !self.cache(1) {
                return false;
            }
        }

        // Check if the trailing character is '!' and copy it.
        if self.ch() == b'!' {
            self.read(&mut s);
        } else {
            // It's either the '!' tag or not really a tag handle.  If it's a %TAG
            // directive, it's an error.  If it's a tag token, it must be a part of URI.
            if directive && s != b"!" {
                self.yaml_parser_set_scanner_tag_error(
                    directive,
                    start_mark,
                    "did not find expected '!'",
                );
                return false;
            }
        }

        *handle = s;
        true
    }

    // Scan a tag.
    // yaml_parser_scan_tag_uri
    fn yaml_parser_scan_tag_uri(
        &mut self,
        directive: bool,
        head: &[u8],
        start_mark: Mark,
        uri: &mut Vec<u8>,
    ) -> bool {
        let mut s = Vec::new();
        let mut has_tag = !head.is_empty();

        // Copy the head if needed.
        //
        // Note that we don't copy the leading '!' character.
        if head.len() > 1 {
            s.extend_from_slice(&head[1..]);
        }

        // Scan the tag.
        if !self.cache(1) {
            return false;
        }

        // The set of characters that may appear in URI is as follows:
        //
        //      '0'-'9', 'A'-'Z', 'a'-'z', '_', '-', ';', '/', '?', ':', '@', '&',
        //      '=', '+', '$', ',', '.', '!', '~', '*', '\'', '(', ')', '[', ']',
        //      '%'.
        // [Go] TODO Convert this into more reasonable logic.
        loop {
            let c = self.ch();
            if !(is_alpha(&self.buffer, self.buffer_pos)
                || c == b';'
                || c == b'/'
                || c == b'?'
                || c == b':'
                || c == b'@'
                || c == b'&'
                || c == b'='
                || c == b'+'
                || c == b'$'
                || c == b','
                || c == b'.'
                || c == b'!'
                || c == b'~'
                || c == b'*'
                || c == b'\''
                || c == b'('
                || c == b')'
                || c == b'['
                || c == b']'
                || c == b'%')
            {
                break;
            }
            // Check if it is a URI-escape sequence.
            if c == b'%' {
                if !self.yaml_parser_scan_uri_escapes(directive, start_mark, &mut s) {
                    return false;
                }
            } else {
                self.read(&mut s);
            }
            if !self.cache(1) {
                return false;
            }
            has_tag = true;
        }

        if !has_tag {
            self.yaml_parser_set_scanner_tag_error(
                directive,
                start_mark,
                "did not find expected tag URI",
            );
            return false;
        }
        *uri = s;
        true
    }

    // Decode an URI-escape sequence corresponding to a single UTF-8 character.
    // yaml_parser_scan_uri_escapes
    fn yaml_parser_scan_uri_escapes(
        &mut self,
        directive: bool,
        start_mark: Mark,
        s: &mut Vec<u8>,
    ) -> bool {
        // Decode the required number of characters.
        let mut w: usize = 1024;
        while w > 0 {
            // Check for a URI-escaped octet.
            if !self.cache(3) {
                return false;
            }

            if !(self.ch() == b'%'
                && is_hex(&self.buffer, self.buffer_pos + 1)
                && is_hex(&self.buffer, self.buffer_pos + 2))
            {
                return self.yaml_parser_set_scanner_tag_error(
                    directive,
                    start_mark,
                    "did not find URI escaped octet",
                );
            }

            // Get the octet.
            let octet = ((as_hex(&self.buffer, self.buffer_pos + 1) << 4)
                + as_hex(&self.buffer, self.buffer_pos + 2)) as u8;

            // If it is the leading octet, determine the length of the UTF-8 sequence.
            if w == 1024 {
                w = width(octet);
                if w == 0 {
                    return self.yaml_parser_set_scanner_tag_error(
                        directive,
                        start_mark,
                        "found an incorrect leading UTF-8 octet",
                    );
                }
            } else {
                // Check if the trailing octet is correct.
                if octet & 0xC0 != 0x80 {
                    return self.yaml_parser_set_scanner_tag_error(
                        directive,
                        start_mark,
                        "found an incorrect trailing UTF-8 octet",
                    );
                }
            }

            // Copy the octet and move the pointers.
            s.push(octet);
            self.skip();
            self.skip();
            self.skip();
            w -= 1;
        }
        true
    }

    // Scan a block scalar.
    // yaml_parser_scan_block_scalar
    #[allow(clippy::too_many_lines)]
    fn yaml_parser_scan_block_scalar(&mut self, token: &mut Token, literal: bool) -> bool {
        // Eat the indicator '|' or '>'.
        let start_mark = self.mark;
        self.skip();

        // Scan the additional block scalar indicators.
        if !self.cache(1) {
            return false;
        }

        // Check for a chomping indicator.
        let mut chomping: i32 = 0;
        let mut increment: isize = 0;
        if self.ch() == b'+' || self.ch() == b'-' {
            // Set the chomping method and eat the indicator.
            chomping = if self.ch() == b'+' { 1 } else { -1 };
            self.skip();

            // Check for an indentation indicator.
            if !self.cache(1) {
                return false;
            }
            if is_digit(&self.buffer, self.buffer_pos) {
                // Check that the indentation is greater than 0.
                if self.ch() == b'0' {
                    self.yaml_parser_set_scanner_error(
                        "while scanning a block scalar",
                        start_mark,
                        "found an indentation indicator equal to 0",
                    );
                    return false;
                }

                // Get the indentation level and eat the indicator.
                increment = as_digit(&self.buffer, self.buffer_pos) as isize;
                self.skip();
            }
        } else if is_digit(&self.buffer, self.buffer_pos) {
            // Do the same as above, but in the opposite order.

            if self.ch() == b'0' {
                self.yaml_parser_set_scanner_error(
                    "while scanning a block scalar",
                    start_mark,
                    "found an indentation indicator equal to 0",
                );
                return false;
            }
            increment = as_digit(&self.buffer, self.buffer_pos) as isize;
            self.skip();

            if !self.cache(1) {
                return false;
            }
            if self.ch() == b'+' || self.ch() == b'-' {
                chomping = if self.ch() == b'+' { 1 } else { -1 };
                self.skip();
            }
        }

        // Eat whitespaces and comments to the end of the line.
        if !self.cache(1) {
            return false;
        }
        while is_blank(&self.buffer, self.buffer_pos) {
            self.skip();
            if !self.cache(1) {
                return false;
            }
        }
        if self.ch() == b'#' {
            if !self.yaml_parser_scan_line_comment(start_mark) {
                return false;
            }
            while !is_breakz(&self.buffer, self.buffer_pos) {
                self.skip();
                if !self.cache(1) {
                    return false;
                }
            }
        }

        // Check if we are at the end of the line.
        if !is_breakz(&self.buffer, self.buffer_pos) {
            self.yaml_parser_set_scanner_error(
                "while scanning a block scalar",
                start_mark,
                "did not find expected comment or line break",
            );
            return false;
        }

        // Eat a line break.
        if is_break(&self.buffer, self.buffer_pos) {
            if !self.cache(2) {
                return false;
            }
            self.skip_line();
        }

        let mut end_mark = self.mark;

        // Set the indentation level if it was specified.
        let mut indent: isize = 0;
        if increment > 0 {
            if self.indent >= 0 {
                indent = self.indent + increment;
            } else {
                indent = increment;
            }
        }

        // Scan the leading line breaks and determine the indentation level if needed.
        let mut s = Vec::new();
        let mut leading_break = Vec::new();
        let mut trailing_breaks = Vec::new();
        if !self.yaml_parser_scan_block_scalar_breaks(
            &mut indent,
            &mut trailing_breaks,
            start_mark,
            &mut end_mark,
        ) {
            return false;
        }

        // Scan the block scalar content.
        if !self.cache(1) {
            return false;
        }
        let mut leading_blank = false;
        let mut trailing_blank;
        while self.mark.column as isize == indent && !is_z(&self.buffer, self.buffer_pos) {
            // We are at the beginning of a non-empty line.

            // Is it a trailing whitespace?
            trailing_blank = is_blank(&self.buffer, self.buffer_pos);

            // Check if we need to fold the leading line break.
            if !literal
                && !leading_blank
                && !trailing_blank
                && !leading_break.is_empty()
                && leading_break[0] == b'\n'
            {
                // Do we need to join the lines by space?
                if trailing_breaks.is_empty() {
                    s.push(b' ');
                }
            } else {
                s.extend_from_slice(&leading_break);
            }
            leading_break.clear();

            // Append the remaining line breaks.
            s.extend_from_slice(&trailing_breaks);
            trailing_breaks.clear();

            // Is it a leading whitespace?
            leading_blank = is_blank(&self.buffer, self.buffer_pos);

            // Consume the current line.
            while !is_breakz(&self.buffer, self.buffer_pos) {
                self.read(&mut s);
                if !self.cache(1) {
                    return false;
                }
            }

            // Consume the line break.
            if !self.cache(2) {
                return false;
            }

            self.read_line(&mut leading_break);

            // Eat the following indentation spaces and line breaks.
            if !self.yaml_parser_scan_block_scalar_breaks(
                &mut indent,
                &mut trailing_breaks,
                start_mark,
                &mut end_mark,
            ) {
                return false;
            }
        }

        // Chomp the tail.
        if chomping != -1 {
            s.extend_from_slice(&leading_break);
        }
        if chomping == 1 {
            s.extend_from_slice(&trailing_breaks);
        }

        // Create a token.
        *token = Token::new(TokenType::Scalar, start_mark, end_mark);
        token.value = s;
        token.style = if literal {
            ScalarStyle::Literal
        } else {
            ScalarStyle::Folded
        };
        true
    }

    // Scan indentation spaces and line breaks for a block scalar.  Determine the
    // indentation level if needed.
    // yaml_parser_scan_block_scalar_breaks
    fn yaml_parser_scan_block_scalar_breaks(
        &mut self,
        indent: &mut isize,
        breaks: &mut Vec<u8>,
        start_mark: Mark,
        end_mark: &mut Mark,
    ) -> bool {
        *end_mark = self.mark;

        // Eat the indentation spaces and line breaks.
        let mut max_indent: isize = 0;
        loop {
            // Eat the indentation spaces.
            if !self.cache(1) {
                return false;
            }
            while (*indent == 0 || (self.mark.column as isize) < *indent)
                && is_space(&self.buffer, self.buffer_pos)
            {
                self.skip();
                if !self.cache(1) {
                    return false;
                }
            }
            if self.mark.column as isize > max_indent {
                max_indent = self.mark.column as isize;
            }

            // Check for a tab character messing the indentation.
            if (*indent == 0 || (self.mark.column as isize) < *indent)
                && is_tab(&self.buffer, self.buffer_pos)
            {
                return self.yaml_parser_set_scanner_error(
                    "while scanning a block scalar",
                    start_mark,
                    "found a tab character where an indentation space is expected",
                );
            }

            // Have we found a non-empty line?
            if !is_break(&self.buffer, self.buffer_pos) {
                break;
            }

            // Consume the line break.
            if !self.cache(2) {
                return false;
            }
            // [Go] Should really be returning breaks instead.
            self.read_line(breaks);
            *end_mark = self.mark;
        }

        // Determine the indentation level if needed.
        if *indent == 0 {
            *indent = max_indent;
            if *indent < self.indent + 1 {
                *indent = self.indent + 1;
            }
            if *indent < 1 {
                *indent = 1;
            }
        }
        true
    }

    // Scan a quoted scalar.
    // yaml_parser_scan_flow_scalar
    #[allow(clippy::too_many_lines)]
    fn yaml_parser_scan_flow_scalar(&mut self, token: &mut Token, single: bool) -> bool {
        // Eat the left quote.
        let start_mark = self.mark;
        self.skip();

        // Consume the content of the quoted scalar.
        let mut s = Vec::new();
        let mut leading_break = Vec::new();
        let mut trailing_breaks = Vec::new();
        let mut whitespaces = Vec::new();
        loop {
            // Check that there are no document indicators at the beginning of the line.
            if !self.cache(4) {
                return false;
            }

            if self.mark.column == 0
                && ((self.ch_at(0) == b'-' && self.ch_at(1) == b'-' && self.ch_at(2) == b'-')
                    || (self.ch_at(0) == b'.' && self.ch_at(1) == b'.' && self.ch_at(2) == b'.'))
                && is_blankz(&self.buffer, self.buffer_pos + 3)
            {
                self.yaml_parser_set_scanner_error(
                    "while scanning a quoted scalar",
                    start_mark,
                    "found unexpected document indicator",
                );
                return false;
            }

            // Check for EOF.
            if is_z(&self.buffer, self.buffer_pos) {
                self.yaml_parser_set_scanner_error(
                    "while scanning a quoted scalar",
                    start_mark,
                    "found unexpected end of stream",
                );
                return false;
            }

            // Consume non-blank characters.
            let mut leading_blanks = false;
            while !is_blankz(&self.buffer, self.buffer_pos) {
                if single && self.ch_at(0) == b'\'' && self.ch_at(1) == b'\'' {
                    // Is is an escaped single quote.
                    s.push(b'\'');
                    self.skip();
                    self.skip();
                } else if single && self.ch() == b'\'' {
                    // It is a right single quote.
                    break;
                } else if !single && self.ch() == b'"' {
                    // It is a right double quote.
                    break;
                } else if !single
                    && self.ch() == b'\\'
                    && is_break(&self.buffer, self.buffer_pos + 1)
                {
                    // It is an escaped line break.
                    if !self.cache(3) {
                        return false;
                    }
                    self.skip();
                    self.skip_line();
                    leading_blanks = true;
                    break;
                } else if !single && self.ch() == b'\\' {
                    // It is an escape sequence.
                    let mut code_length: usize = 0;

                    // Check the escape character.
                    match self.ch_at(1) {
                        b'0' => s.push(0),
                        b'a' => s.push(b'\x07'),
                        b'b' => s.push(b'\x08'),
                        b't' | b'\t' => s.push(b'\x09'),
                        b'n' => s.push(b'\x0A'),
                        b'v' => s.push(b'\x0B'),
                        b'f' => s.push(b'\x0C'),
                        b'r' => s.push(b'\x0D'),
                        b'e' => s.push(b'\x1B'),
                        b' ' => s.push(b'\x20'),
                        b'"' => s.push(b'"'),
                        b'\'' => s.push(b'\''),
                        b'\\' => s.push(b'\\'),
                        // NEL (#x85)
                        b'N' => s.extend_from_slice(&[0xC2, 0x85]),
                        // #xA0
                        b'_' => s.extend_from_slice(&[0xC2, 0xA0]),
                        // LS (#x2028)
                        b'L' => s.extend_from_slice(&[0xE2, 0x80, 0xA8]),
                        // PS (#x2029)
                        b'P' => s.extend_from_slice(&[0xE2, 0x80, 0xA9]),
                        b'x' => code_length = 2,
                        b'u' => code_length = 4,
                        b'U' => code_length = 8,
                        _ => {
                            self.yaml_parser_set_scanner_error(
                                "while parsing a quoted scalar",
                                start_mark,
                                "found unknown escape character",
                            );
                            return false;
                        }
                    }

                    self.skip();
                    self.skip();

                    // Consume an arbitrary escape code.
                    if code_length > 0 {
                        let mut value: i64 = 0;

                        // Scan the character value.
                        if !self.cache(code_length) {
                            return false;
                        }
                        for k in 0..code_length {
                            if !is_hex(&self.buffer, self.buffer_pos + k) {
                                self.yaml_parser_set_scanner_error(
                                    "while parsing a quoted scalar",
                                    start_mark,
                                    "did not find expected hexdecimal number",
                                );
                                return false;
                            }
                            value = (value << 4) + as_hex(&self.buffer, self.buffer_pos + k);
                        }

                        // Check the value and write the character.
                        if (0xD800..=0xDFFF).contains(&value) || value > 0x10FFFF {
                            self.yaml_parser_set_scanner_error(
                                "while parsing a quoted scalar",
                                start_mark,
                                "found invalid Unicode character escape code",
                            );
                            return false;
                        }
                        if value <= 0x7F {
                            s.push(value as u8);
                        } else if value <= 0x7FF {
                            s.push((0xC0 + (value >> 6)) as u8);
                            s.push((0x80 + (value & 0x3F)) as u8);
                        } else if value <= 0xFFFF {
                            s.push((0xE0 + (value >> 12)) as u8);
                            s.push((0x80 + ((value >> 6) & 0x3F)) as u8);
                            s.push((0x80 + (value & 0x3F)) as u8);
                        } else {
                            s.push((0xF0 + (value >> 18)) as u8);
                            s.push((0x80 + ((value >> 12) & 0x3F)) as u8);
                            s.push((0x80 + ((value >> 6) & 0x3F)) as u8);
                            s.push((0x80 + (value & 0x3F)) as u8);
                        }

                        // Advance the pointer.
                        for _ in 0..code_length {
                            self.skip();
                        }
                    }
                } else {
                    // It is a non-escaped non-blank character.
                    self.read(&mut s);
                }
                if !self.cache(2) {
                    return false;
                }
            }

            if !self.cache(1) {
                return false;
            }

            // Check if we are at the end of the scalar.
            if single {
                if self.ch() == b'\'' {
                    break;
                }
            } else if self.ch() == b'"' {
                break;
            }

            // Consume blank characters.
            while is_blank(&self.buffer, self.buffer_pos) || is_break(&self.buffer, self.buffer_pos)
            {
                if is_blank(&self.buffer, self.buffer_pos) {
                    // Consume a space or a tab character.
                    if !leading_blanks {
                        self.read(&mut whitespaces);
                    } else {
                        self.skip();
                    }
                } else {
                    if !self.cache(2) {
                        return false;
                    }

                    // Check if it is a first line break.
                    if !leading_blanks {
                        whitespaces.clear();
                        self.read_line(&mut leading_break);
                        leading_blanks = true;
                    } else {
                        self.read_line(&mut trailing_breaks);
                    }
                }
                if !self.cache(1) {
                    return false;
                }
            }

            // Join the whitespaces or fold line breaks.
            if leading_blanks {
                // Do we need to fold line breaks?
                if !leading_break.is_empty() && leading_break[0] == b'\n' {
                    if trailing_breaks.is_empty() {
                        s.push(b' ');
                    } else {
                        s.extend_from_slice(&trailing_breaks);
                    }
                } else {
                    s.extend_from_slice(&leading_break);
                    s.extend_from_slice(&trailing_breaks);
                }
                trailing_breaks.clear();
                leading_break.clear();
            } else {
                s.extend_from_slice(&whitespaces);
                whitespaces.clear();
            }
        }

        // Eat the right quote.
        self.skip();
        let end_mark = self.mark;

        // Create a token.
        *token = Token::new(TokenType::Scalar, start_mark, end_mark);
        token.value = s;
        token.style = if single {
            ScalarStyle::SingleQuoted
        } else {
            ScalarStyle::DoubleQuoted
        };
        true
    }

    // Scan a plain scalar.
    // yaml_parser_scan_plain_scalar
    #[allow(clippy::too_many_lines)]
    fn yaml_parser_scan_plain_scalar(&mut self, token: &mut Token) -> bool {
        let mut s = Vec::new();
        let mut leading_break = Vec::new();
        let mut trailing_breaks = Vec::new();
        let mut whitespaces = Vec::new();
        let mut leading_blanks = false;
        let indent = self.indent + 1;

        let start_mark = self.mark;
        let mut end_mark = self.mark;

        // Consume the content of the plain scalar.
        loop {
            // Check for a document indicator.
            if !self.cache(4) {
                return false;
            }
            if self.mark.column == 0
                && ((self.ch_at(0) == b'-' && self.ch_at(1) == b'-' && self.ch_at(2) == b'-')
                    || (self.ch_at(0) == b'.' && self.ch_at(1) == b'.' && self.ch_at(2) == b'.'))
                && is_blankz(&self.buffer, self.buffer_pos + 3)
            {
                break;
            }

            // Check for a comment.
            if self.ch() == b'#' {
                break;
            }

            // Consume non-blank characters.
            while !is_blankz(&self.buffer, self.buffer_pos) {
                // Check for indicators that may end a plain scalar.
                let c = self.ch();
                if (c == b':' && is_blankz(&self.buffer, self.buffer_pos + 1))
                    || (self.flow_level > 0
                        && (c == b','
                            || c == b'?'
                            || c == b'['
                            || c == b']'
                            || c == b'{'
                            || c == b'}'))
                {
                    break;
                }

                // Check if we need to join whitespaces and breaks.
                if leading_blanks || !whitespaces.is_empty() {
                    if leading_blanks {
                        // Do we need to fold line breaks?
                        if leading_break[0] == b'\n' {
                            if trailing_breaks.is_empty() {
                                s.push(b' ');
                            } else {
                                s.extend_from_slice(&trailing_breaks);
                            }
                        } else {
                            s.extend_from_slice(&leading_break);
                            s.extend_from_slice(&trailing_breaks);
                        }
                        trailing_breaks.clear();
                        leading_break.clear();
                        leading_blanks = false;
                    } else {
                        s.extend_from_slice(&whitespaces);
                        whitespaces.clear();
                    }
                }

                // Copy the character.
                self.read(&mut s);

                end_mark = self.mark;
                if !self.cache(2) {
                    return false;
                }
            }

            // Is it the end?
            if !(is_blank(&self.buffer, self.buffer_pos) || is_break(&self.buffer, self.buffer_pos))
            {
                break;
            }

            // Consume blank characters.
            if !self.cache(1) {
                return false;
            }

            while is_blank(&self.buffer, self.buffer_pos) || is_break(&self.buffer, self.buffer_pos)
            {
                if is_blank(&self.buffer, self.buffer_pos) {
                    // Check for tab characters that abuse indentation.
                    if leading_blanks
                        && (self.mark.column as isize) < indent
                        && is_tab(&self.buffer, self.buffer_pos)
                    {
                        self.yaml_parser_set_scanner_error(
                            "while scanning a plain scalar",
                            start_mark,
                            "found a tab character that violates indentation",
                        );
                        return false;
                    }

                    // Consume a space or a tab character.
                    if !leading_blanks {
                        self.read(&mut whitespaces);
                    } else {
                        self.skip();
                    }
                } else {
                    if !self.cache(2) {
                        return false;
                    }

                    // Check if it is a first line break.
                    if !leading_blanks {
                        whitespaces.clear();
                        self.read_line(&mut leading_break);
                        leading_blanks = true;
                    } else {
                        self.read_line(&mut trailing_breaks);
                    }
                }
                if !self.cache(1) {
                    return false;
                }
            }

            // Check indentation level.
            if self.flow_level == 0 && (self.mark.column as isize) < indent {
                break;
            }
        }

        // Create a token.
        *token = Token::new(TokenType::Scalar, start_mark, end_mark);
        token.value = s;
        token.style = ScalarStyle::Plain;

        // Note that we change the 'simple_key_allowed' flag.
        if leading_blanks {
            self.simple_key_allowed = true;
        }
        true
    }

    // yaml_parser_scan_line_comment
    fn yaml_parser_scan_line_comment(&mut self, token_mark: Mark) -> bool {
        if self.newlines > 0 {
            return true;
        }

        let mut start_mark = Mark::default();
        let mut text = Vec::new();

        let mut peek: usize = 0;
        while peek < 512 {
            if !self.cache(peek + 1) {
                break;
            }
            if is_blank(&self.buffer, self.buffer_pos + peek) {
                peek += 1;
                continue;
            }
            if self.ch_at(peek) == b'#' {
                let seen = self.mark.index + peek;
                loop {
                    if !self.cache(1) {
                        return false;
                    }
                    if is_breakz(&self.buffer, self.buffer_pos) {
                        if self.mark.index >= seen {
                            break;
                        }
                        if !self.cache(2) {
                            return false;
                        }
                        self.skip_line();
                    } else if self.mark.index >= seen {
                        if text.is_empty() {
                            start_mark = self.mark;
                        }
                        self.read(&mut text);
                    } else {
                        self.skip();
                    }
                }
            }
            break;
        }
        if !text.is_empty() {
            self.comments.push(Comment {
                token_mark,
                start_mark,
                line: text,
                ..Comment::default()
            });
        }
        true
    }

    // yaml_parser_scan_comments
    #[allow(clippy::too_many_lines)]
    fn yaml_parser_scan_comments(&mut self, scan_mark: Mark) -> bool {
        let mut scan_mark = scan_mark;
        let mut token = &self.tokens[self.tokens_len - 1];

        if token.typ == TokenType::FlowEntry && self.tokens_len > 1 {
            token = &self.tokens[self.tokens_len - 2];
        }
        let token_typ = token.typ;

        let mut token_mark = token.start_mark;
        let mut start_mark = Mark::default();
        let mut next_indent = self.indent;
        if next_indent < 0 {
            next_indent = 0;
        }

        let mut recent_empty = false;
        let mut first_empty = self.newlines <= 1;

        let mut line = self.mark.line as isize;
        let mut column = self.mark.column as isize;

        let mut text: Vec<u8> = Vec::new();

        // The foot line is the place where a comment must start to
        // still be considered as a foot of the prior content.
        // If there's some content in the currently parsed line, then
        // the foot is the line below it.
        let mut foot_line: isize = -1;
        if scan_mark.line > 0 {
            foot_line = self.mark.line as isize - self.newlines as isize + 1;
            if self.newlines == 0 && self.mark.column > 1 {
                foot_line += 1;
            }
        }

        let mk = |index: usize, line: isize, column: isize| Mark {
            index,
            line: line as usize,
            column: column as usize,
        };

        let mut peek: usize = 0;
        while peek < 512 {
            if !self.cache(peek + 1) {
                break;
            }
            column += 1;
            if is_blank(&self.buffer, self.buffer_pos + peek) {
                peek += 1;
                continue;
            }
            let c = self.ch_at(peek);
            let close_flow = self.flow_level > 0 && (c == b']' || c == b'}');
            if close_flow || is_breakz(&self.buffer, self.buffer_pos + peek) {
                // Got line break or terminator.
                if close_flow || !recent_empty {
                    if close_flow
                        || first_empty
                            && (start_mark.line as isize == foot_line
                                && token_typ != TokenType::Value
                                || (start_mark.column as isize) - 1 < next_indent)
                    {
                        // This is the first empty line and there were no empty lines before,
                        // so this initial part of the comment is a foot of the prior token
                        // instead of being a head for the following one. Split it up.
                        // Alternatively, this might also be the last comment inside a flow
                        // scope, so it must be a footer.
                        if !text.is_empty() {
                            if (start_mark.column as isize) - 1 < next_indent {
                                // If dedented it's unrelated to the prior token.
                                token_mark = start_mark;
                            }
                            let end = mk(self.mark.index + peek, line, column);
                            self.comments.push(Comment {
                                scan_mark,
                                token_mark,
                                start_mark,
                                end_mark: end,
                                foot: std::mem::take(&mut text),
                                ..Comment::default()
                            });
                            scan_mark = end;
                            token_mark = scan_mark;
                        }
                    } else if !text.is_empty() && self.ch_at(peek) != 0 {
                        text.push(b'\n');
                    }
                }
                if !is_break(&self.buffer, self.buffer_pos + peek) {
                    break;
                }
                first_empty = false;
                recent_empty = true;
                column = 0;
                line += 1;
                peek += 1;
                continue;
            }

            if !text.is_empty()
                && (close_flow || column - 1 < next_indent && column != start_mark.column as isize)
            {
                // The comment at the different indentation is a foot of the
                // preceding data rather than a head of the upcoming one.
                let end = mk(self.mark.index + peek, line, column);
                self.comments.push(Comment {
                    scan_mark,
                    token_mark,
                    start_mark,
                    end_mark: end,
                    foot: std::mem::take(&mut text),
                    ..Comment::default()
                });
                scan_mark = end;
                token_mark = scan_mark;
            }

            if self.ch_at(peek) != b'#' {
                break;
            }

            if text.is_empty() {
                start_mark = mk(self.mark.index + peek, line, column);
            } else {
                text.push(b'\n');
            }

            recent_empty = false;

            // Consume until after the consumed comment line.
            let seen = self.mark.index + peek;
            loop {
                if !self.cache(1) {
                    return false;
                }
                if is_breakz(&self.buffer, self.buffer_pos) {
                    if self.mark.index >= seen {
                        break;
                    }
                    if !self.cache(2) {
                        return false;
                    }
                    self.skip_line();
                } else if self.mark.index >= seen {
                    self.read(&mut text);
                } else {
                    self.skip();
                }
            }

            peek = 0;
            column = 0;
            line = self.mark.line as isize;
            next_indent = self.indent;
            if next_indent < 0 {
                next_indent = 0;
            }
            // [Go] the for-loop post statement (peek++) still runs here.
            peek += 1;
        }

        if !text.is_empty() {
            self.comments.push(Comment {
                scan_mark,
                token_mark: start_mark,
                start_mark,
                end_mark: mk((self.mark.index + peek).wrapping_sub(1), line, column),
                head: text,
                ..Comment::default()
            });
        }
        true
    }

    // -----------------------------------------------------------------------
    // parserc.go
    // -----------------------------------------------------------------------

    // Peek the next token in the token queue.
    // peek_token
    fn peek_token(&mut self) -> Option<Token> {
        if self.token_available || self.yaml_parser_fetch_more_tokens() {
            let token = self.tokens[self.tokens_head].clone();
            self.yaml_parser_unfold_comments(&token);
            return Some(token);
        }
        None
    }

    // yaml_parser_unfold_comments walks through the comments queue and joins all
    // comments behind the position of the provided token into the respective
    // top-level comment slices in the parser.
    fn yaml_parser_unfold_comments(&mut self, token: &Token) {
        while self.comments_head < self.comments.len()
            && token.start_mark.index >= self.comments[self.comments_head].token_mark.index
        {
            let comment = &self.comments[self.comments_head];
            if !comment.head.is_empty() {
                if token.typ == TokenType::BlockEnd {
                    // No heads on ends, so keep comment.head for a follow up token.
                    break;
                }
                if !self.head_comment.is_empty() {
                    self.head_comment.push(b'\n');
                }
                self.head_comment.extend_from_slice(&comment.head);
            }
            if !comment.foot.is_empty() {
                if !self.foot_comment.is_empty() {
                    self.foot_comment.push(b'\n');
                }
                self.foot_comment.extend_from_slice(&comment.foot);
            }
            if !comment.line.is_empty() {
                if !self.line_comment.is_empty() {
                    self.line_comment.push(b'\n');
                }
                self.line_comment.extend_from_slice(&comment.line);
            }
            self.comments[self.comments_head] = Comment::default();
            self.comments_head += 1;
        }
    }

    // Remove the next token from the queue (must be called after peek_token).
    // skip_token
    fn skip_token(&mut self) {
        self.token_available = false;
        self.tokens_parsed += 1;
        self.stream_end_produced = self.tokens[self.tokens_head].typ == TokenType::StreamEnd;
        self.tokens_head += 1;
    }

    // Set parser error.
    // yaml_parser_set_parser_error
    fn yaml_parser_set_parser_error(&mut self, problem: &str, problem_mark: Mark) -> bool {
        self.error = Some(ErrorKind::Parser);
        self.problem = problem.to_string();
        self.problem_mark = problem_mark;
        false
    }

    // yaml_parser_set_parser_error_context
    fn yaml_parser_set_parser_error_context(
        &mut self,
        context: &str,
        context_mark: Mark,
        problem: &str,
        problem_mark: Mark,
    ) -> bool {
        self.error = Some(ErrorKind::Parser);
        self.context = context.to_string();
        self.context_mark = context_mark;
        self.problem = problem.to_string();
        self.problem_mark = problem_mark;
        false
    }

    fn pop_state(&mut self) {
        // parser.state = parser.states[len(parser.states)-1]
        // parser.states = parser.states[:len(parser.states)-1]
        self.state = self.states.pop().expect("parser states stack underflow");
    }

    // State dispatcher.
    // yaml_parser_state_machine
    fn yaml_parser_state_machine(&mut self, event: &mut RawEvent) -> bool {
        match self.state {
            ParserState::StreamStart => self.yaml_parser_parse_stream_start(event),
            ParserState::ImplicitDocumentStart => {
                self.yaml_parser_parse_document_start(event, true)
            }
            ParserState::DocumentStart => self.yaml_parser_parse_document_start(event, false),
            ParserState::DocumentContent => self.yaml_parser_parse_document_content(event),
            ParserState::DocumentEnd => self.yaml_parser_parse_document_end(event),
            ParserState::BlockNode => self.yaml_parser_parse_node(event, true, false),
            ParserState::BlockNodeOrIndentlessSequence => {
                self.yaml_parser_parse_node(event, true, true)
            }
            ParserState::FlowNode => self.yaml_parser_parse_node(event, false, false),
            ParserState::BlockSequenceFirstEntry => {
                self.yaml_parser_parse_block_sequence_entry(event, true)
            }
            ParserState::BlockSequenceEntry => {
                self.yaml_parser_parse_block_sequence_entry(event, false)
            }
            ParserState::IndentlessSequenceEntry => {
                self.yaml_parser_parse_indentless_sequence_entry(event)
            }
            ParserState::BlockMappingFirstKey => {
                self.yaml_parser_parse_block_mapping_key(event, true)
            }
            ParserState::BlockMappingKey => self.yaml_parser_parse_block_mapping_key(event, false),
            ParserState::BlockMappingValue => self.yaml_parser_parse_block_mapping_value(event),
            ParserState::FlowSequenceFirstEntry => {
                self.yaml_parser_parse_flow_sequence_entry(event, true)
            }
            ParserState::FlowSequenceEntry => {
                self.yaml_parser_parse_flow_sequence_entry(event, false)
            }
            ParserState::FlowSequenceEntryMappingKey => {
                self.yaml_parser_parse_flow_sequence_entry_mapping_key(event)
            }
            ParserState::FlowSequenceEntryMappingValue => {
                self.yaml_parser_parse_flow_sequence_entry_mapping_value(event)
            }
            ParserState::FlowSequenceEntryMappingEnd => {
                self.yaml_parser_parse_flow_sequence_entry_mapping_end(event)
            }
            ParserState::FlowMappingFirstKey => {
                self.yaml_parser_parse_flow_mapping_key(event, true)
            }
            ParserState::FlowMappingKey => self.yaml_parser_parse_flow_mapping_key(event, false),
            ParserState::FlowMappingValue => {
                self.yaml_parser_parse_flow_mapping_value(event, false)
            }
            ParserState::FlowMappingEmptyValue => {
                self.yaml_parser_parse_flow_mapping_value(event, true)
            }
            ParserState::End => panic!("invalid parser state"),
        }
    }

    // Parse the production:
    // stream   ::= STREAM-START implicit_document? explicit_document* STREAM-END
    //              ************
    // yaml_parser_parse_stream_start
    fn yaml_parser_parse_stream_start(&mut self, event: &mut RawEvent) -> bool {
        let Some(token) = self.peek_token() else {
            return false;
        };
        if token.typ != TokenType::StreamStart {
            return self.yaml_parser_set_parser_error(
                "did not find expected <stream-start>",
                token.start_mark,
            );
        }
        self.state = ParserState::ImplicitDocumentStart;
        *event = RawEvent::new(EventType::StreamStart, token.start_mark, token.end_mark);
        self.skip_token();
        true
    }

    // Parse the productions:
    // implicit_document    ::= block_node DOCUMENT-END*
    //                          *
    // explicit_document    ::= DIRECTIVE* DOCUMENT-START block_node? DOCUMENT-END*
    //                          *************************
    // yaml_parser_parse_document_start
    fn yaml_parser_parse_document_start(&mut self, event: &mut RawEvent, implicit: bool) -> bool {
        let Some(mut token) = self.peek_token() else {
            return false;
        };

        // Parse extra document end indicators.
        if !implicit {
            while token.typ == TokenType::DocumentEnd {
                self.skip_token();
                let Some(t) = self.peek_token() else {
                    return false;
                };
                token = t;
            }
        }

        if implicit
            && token.typ != TokenType::VersionDirective
            && token.typ != TokenType::TagDirective
            && token.typ != TokenType::DocumentStart
            && token.typ != TokenType::StreamEnd
        {
            // Parse an implicit document.
            if !self.yaml_parser_process_directives() {
                return false;
            }
            self.states.push(ParserState::DocumentEnd);
            self.state = ParserState::BlockNode;

            let mut head_comment = Vec::new();
            if !self.head_comment.is_empty() {
                // [Go] Scan the header comment backwards, and if an empty line is found, break
                //      the header so the part before the last empty line goes into the
                //      document header, while the bottom of it goes into a follow up event.
                let hc = &self.head_comment;
                let mut split = None;
                let mut i = hc.len() - 1;
                while i > 0 {
                    if hc[i] == b'\n' {
                        if i == hc.len() - 1 {
                            split = Some((hc[..i].to_vec(), hc[i + 1..].to_vec()));
                            break;
                        } else if hc[i - 1] == b'\n' {
                            split = Some((hc[..i - 1].to_vec(), hc[i + 1..].to_vec()));
                            break;
                        }
                    }
                    i -= 1;
                }
                if let Some((head, rest)) = split {
                    head_comment = head;
                    self.head_comment = rest;
                }
            }

            *event = RawEvent::new(EventType::DocumentStart, token.start_mark, token.end_mark);
            event.head_comment = head_comment;
        } else if token.typ != TokenType::StreamEnd {
            // Parse an explicit document.
            let start_mark = token.start_mark;
            if !self.yaml_parser_process_directives() {
                return false;
            }
            let Some(token) = self.peek_token() else {
                return false;
            };
            if token.typ != TokenType::DocumentStart {
                self.yaml_parser_set_parser_error(
                    "did not find expected <document start>",
                    token.start_mark,
                );
                return false;
            }
            self.states.push(ParserState::DocumentEnd);
            self.state = ParserState::DocumentContent;
            let end_mark = token.end_mark;

            *event = RawEvent::new(EventType::DocumentStart, start_mark, end_mark);
            event.implicit = false;
            self.skip_token();
        } else {
            // Parse the stream end.
            self.state = ParserState::End;
            *event = RawEvent::new(EventType::StreamEnd, token.start_mark, token.end_mark);
            self.skip_token();
        }

        true
    }

    // Parse the productions:
    // explicit_document    ::= DIRECTIVE* DOCUMENT-START block_node? DOCUMENT-END*
    //                                                    ***********
    //
    // yaml_parser_parse_document_content
    fn yaml_parser_parse_document_content(&mut self, event: &mut RawEvent) -> bool {
        let Some(token) = self.peek_token() else {
            return false;
        };

        if token.typ == TokenType::VersionDirective
            || token.typ == TokenType::TagDirective
            || token.typ == TokenType::DocumentStart
            || token.typ == TokenType::DocumentEnd
            || token.typ == TokenType::StreamEnd
        {
            self.pop_state();
            return self.yaml_parser_process_empty_scalar(event, token.start_mark);
        }
        self.yaml_parser_parse_node(event, true, false)
    }

    // Parse the productions:
    // implicit_document    ::= block_node DOCUMENT-END*
    //                                     *************
    // explicit_document    ::= DIRECTIVE* DOCUMENT-START block_node? DOCUMENT-END*
    //
    // yaml_parser_parse_document_end
    fn yaml_parser_parse_document_end(&mut self, event: &mut RawEvent) -> bool {
        let Some(token) = self.peek_token() else {
            return false;
        };

        let start_mark = token.start_mark;
        let mut end_mark = token.start_mark;

        let mut implicit = true;
        if token.typ == TokenType::DocumentEnd {
            end_mark = token.end_mark;
            self.skip_token();
            implicit = false;
        }

        self.tag_directives.clear();

        self.state = ParserState::DocumentStart;
        *event = RawEvent::new(EventType::DocumentEnd, start_mark, end_mark);
        event.implicit = implicit;
        self.yaml_parser_set_event_comments(event);
        if !event.head_comment.is_empty() && event.foot_comment.is_empty() {
            event.foot_comment = std::mem::take(&mut event.head_comment);
        }
        true
    }

    // yaml_parser_set_event_comments
    fn yaml_parser_set_event_comments(&mut self, event: &mut RawEvent) {
        event.head_comment = std::mem::take(&mut self.head_comment);
        event.line_comment = std::mem::take(&mut self.line_comment);
        event.foot_comment = std::mem::take(&mut self.foot_comment);
        self.tail_comment.clear();
        self.stem_comment.clear();
    }

    // Parse the productions:
    // block_node_or_indentless_sequence    ::=
    //                          ALIAS
    //                          *****
    //                          | properties (block_content | indentless_block_sequence)?
    //                            **********  *
    //                          | block_content | indentless_block_sequence
    //                            *
    // block_node           ::= ALIAS
    //                          *****
    //                          | properties block_content?
    //                            ********** *
    //                          | block_content
    //                            *
    // flow_node            ::= ALIAS
    //                          *****
    //                          | properties flow_content?
    //                            ********** *
    //                          | flow_content
    //                            *
    // properties           ::= TAG ANCHOR? | ANCHOR TAG?
    //                          *************************
    // block_content        ::= block_collection | flow_collection | SCALAR
    //                                                               ******
    // flow_content         ::= flow_collection | SCALAR
    //                                            ******
    // yaml_parser_parse_node
    #[allow(clippy::too_many_lines)]
    fn yaml_parser_parse_node(
        &mut self,
        event: &mut RawEvent,
        block: bool,
        indentless_sequence: bool,
    ) -> bool {
        let Some(mut token) = self.peek_token() else {
            return false;
        };

        if token.typ == TokenType::Alias {
            self.pop_state();
            *event = RawEvent::new(EventType::Alias, token.start_mark, token.end_mark);
            event.anchor = token.value;
            self.yaml_parser_set_event_comments(event);
            self.skip_token();
            return true;
        }

        let mut start_mark = token.start_mark;
        let mut end_mark = token.start_mark;

        let mut tag_token = false;
        let mut tag_handle = Vec::new();
        let mut tag_suffix = Vec::new();
        let mut anchor = Vec::new();
        let mut tag_mark = Mark::default();
        if token.typ == TokenType::Anchor {
            anchor = token.value;
            start_mark = token.start_mark;
            end_mark = token.end_mark;
            self.skip_token();
            let Some(t) = self.peek_token() else {
                return false;
            };
            token = t;
            if token.typ == TokenType::Tag {
                tag_token = true;
                tag_handle = std::mem::take(&mut token.value);
                tag_suffix = std::mem::take(&mut token.suffix);
                tag_mark = token.start_mark;
                end_mark = token.end_mark;
                self.skip_token();
                let Some(t) = self.peek_token() else {
                    return false;
                };
                token = t;
            }
        } else if token.typ == TokenType::Tag {
            tag_token = true;
            tag_handle = std::mem::take(&mut token.value);
            tag_suffix = std::mem::take(&mut token.suffix);
            start_mark = token.start_mark;
            tag_mark = token.start_mark;
            end_mark = token.end_mark;
            self.skip_token();
            let Some(t) = self.peek_token() else {
                return false;
            };
            token = t;
            if token.typ == TokenType::Anchor {
                anchor = std::mem::take(&mut token.value);
                end_mark = token.end_mark;
                self.skip_token();
                let Some(t) = self.peek_token() else {
                    return false;
                };
                token = t;
            }
        }

        let mut tag = Vec::new();
        if tag_token {
            if tag_handle.is_empty() {
                tag = tag_suffix;
            } else {
                for td in &self.tag_directives {
                    if td.handle == tag_handle {
                        tag = td.prefix.clone();
                        tag.extend_from_slice(&tag_suffix);
                        break;
                    }
                }
                if tag.is_empty() {
                    self.yaml_parser_set_parser_error_context(
                        "while parsing a node",
                        start_mark,
                        "found undefined tag handle",
                        tag_mark,
                    );
                    return false;
                }
            }
        }

        let implicit = tag.is_empty();
        if indentless_sequence && token.typ == TokenType::BlockEntry {
            end_mark = token.end_mark;
            self.state = ParserState::IndentlessSequenceEntry;
            *event = RawEvent::new(EventType::SequenceStart, start_mark, end_mark);
            event.anchor = anchor;
            event.tag = tag;
            event.implicit = implicit;
            event.flow = false;
            return true;
        }
        if token.typ == TokenType::Scalar {
            let mut plain_implicit = false;
            let mut quoted_implicit = false;
            end_mark = token.end_mark;
            if (tag.is_empty() && token.style == ScalarStyle::Plain)
                || (tag.len() == 1 && tag[0] == b'!')
            {
                plain_implicit = true;
            } else if tag.is_empty() {
                quoted_implicit = true;
            }
            self.pop_state();

            *event = RawEvent::new(EventType::Scalar, start_mark, end_mark);
            event.anchor = anchor;
            event.tag = tag;
            event.value = token.value;
            event.implicit = plain_implicit;
            event.quoted_implicit = quoted_implicit;
            event.scalar_style = token.style;
            self.yaml_parser_set_event_comments(event);
            self.skip_token();
            return true;
        }
        if token.typ == TokenType::FlowSequenceStart {
            // [Go] Some of the events below can be merged as they differ only on style.
            end_mark = token.end_mark;
            self.state = ParserState::FlowSequenceFirstEntry;
            *event = RawEvent::new(EventType::SequenceStart, start_mark, end_mark);
            event.anchor = anchor;
            event.tag = tag;
            event.implicit = implicit;
            event.flow = true;
            self.yaml_parser_set_event_comments(event);
            return true;
        }
        if token.typ == TokenType::FlowMappingStart {
            end_mark = token.end_mark;
            self.state = ParserState::FlowMappingFirstKey;
            *event = RawEvent::new(EventType::MappingStart, start_mark, end_mark);
            event.anchor = anchor;
            event.tag = tag;
            event.implicit = implicit;
            event.flow = true;
            self.yaml_parser_set_event_comments(event);
            return true;
        }
        if block && token.typ == TokenType::BlockSequenceStart {
            end_mark = token.end_mark;
            self.state = ParserState::BlockSequenceFirstEntry;
            *event = RawEvent::new(EventType::SequenceStart, start_mark, end_mark);
            event.anchor = anchor;
            event.tag = tag;
            event.implicit = implicit;
            event.flow = false;
            if !self.stem_comment.is_empty() {
                event.head_comment = std::mem::take(&mut self.stem_comment);
            }
            return true;
        }
        if block && token.typ == TokenType::BlockMappingStart {
            end_mark = token.end_mark;
            self.state = ParserState::BlockMappingFirstKey;
            *event = RawEvent::new(EventType::MappingStart, start_mark, end_mark);
            event.anchor = anchor;
            event.tag = tag;
            event.implicit = implicit;
            event.flow = false;
            if !self.stem_comment.is_empty() {
                event.head_comment = std::mem::take(&mut self.stem_comment);
            }
            return true;
        }
        if !anchor.is_empty() || !tag.is_empty() {
            self.pop_state();

            *event = RawEvent::new(EventType::Scalar, start_mark, end_mark);
            event.anchor = anchor;
            event.tag = tag;
            event.implicit = implicit;
            event.quoted_implicit = false;
            event.scalar_style = ScalarStyle::Plain;
            return true;
        }

        let context = if block {
            "while parsing a block node"
        } else {
            "while parsing a flow node"
        };
        self.yaml_parser_set_parser_error_context(
            context,
            start_mark,
            "did not find expected node content",
            token.start_mark,
        );
        false
    }

    // Parse the productions:
    // block_sequence ::= BLOCK-SEQUENCE-START (BLOCK-ENTRY block_node?)* BLOCK-END
    //                    ********************  *********** *             *********
    //
    // yaml_parser_parse_block_sequence_entry
    fn yaml_parser_parse_block_sequence_entry(
        &mut self,
        event: &mut RawEvent,
        first: bool,
    ) -> bool {
        if first {
            let Some(token) = self.peek_token() else {
                return false;
            };
            self.marks.push(token.start_mark);
            self.skip_token();
        }

        let Some(token) = self.peek_token() else {
            return false;
        };

        if token.typ == TokenType::BlockEntry {
            let mark = token.end_mark;
            let prior_head_len = self.head_comment.len();
            self.skip_token();
            self.yaml_parser_split_stem_comment(prior_head_len);
            let Some(token) = self.peek_token() else {
                return false;
            };
            if token.typ != TokenType::BlockEntry && token.typ != TokenType::BlockEnd {
                self.states.push(ParserState::BlockSequenceEntry);
                return self.yaml_parser_parse_node(event, true, false);
            }
            self.state = ParserState::BlockSequenceEntry;
            return self.yaml_parser_process_empty_scalar(event, mark);
        }
        if token.typ == TokenType::BlockEnd {
            self.pop_state();
            self.marks.pop();

            *event = RawEvent::new(EventType::SequenceEnd, token.start_mark, token.end_mark);

            self.skip_token();
            return true;
        }

        let context_mark = self.marks.pop().unwrap_or_default();
        self.yaml_parser_set_parser_error_context(
            "while parsing a block collection",
            context_mark,
            "did not find expected '-' indicator",
            token.start_mark,
        )
    }

    // Parse the productions:
    // indentless_sequence  ::= (BLOCK-ENTRY block_node?)+
    //                           *********** *
    // yaml_parser_parse_indentless_sequence_entry
    fn yaml_parser_parse_indentless_sequence_entry(&mut self, event: &mut RawEvent) -> bool {
        let Some(token) = self.peek_token() else {
            return false;
        };

        if token.typ == TokenType::BlockEntry {
            let mark = token.end_mark;
            let prior_head_len = self.head_comment.len();
            self.skip_token();
            self.yaml_parser_split_stem_comment(prior_head_len);
            let Some(token) = self.peek_token() else {
                return false;
            };
            if token.typ != TokenType::BlockEntry
                && token.typ != TokenType::Key
                && token.typ != TokenType::Value
                && token.typ != TokenType::BlockEnd
            {
                self.states.push(ParserState::IndentlessSequenceEntry);
                return self.yaml_parser_parse_node(event, true, false);
            }
            self.state = ParserState::IndentlessSequenceEntry;
            return self.yaml_parser_process_empty_scalar(event, mark);
        }
        self.pop_state();

        // [Go] Shouldn't this be token.end_mark?
        *event = RawEvent::new(EventType::SequenceEnd, token.start_mark, token.start_mark);
        true
    }

    // Split stem comment from head comment.
    //
    // When a sequence or map is found under a sequence entry, the former head comment
    // is assigned to the underlying sequence or map as a whole, not the individual
    // sequence or map entry as would be expected otherwise. To handle this case the
    // previous head comment is moved aside as the stem comment.
    // yaml_parser_split_stem_comment
    fn yaml_parser_split_stem_comment(&mut self, stem_len: usize) {
        if stem_len == 0 {
            return;
        }

        let Some(token) = self.peek_token() else {
            return;
        };
        if token.typ != TokenType::BlockSequenceStart && token.typ != TokenType::BlockMappingStart {
            return;
        }

        self.stem_comment = self.head_comment[..stem_len].to_vec();
        if self.head_comment.len() == stem_len {
            self.head_comment = Vec::new();
        } else {
            // Copy suffix to prevent very strange bugs if someone ever appends
            // further bytes to the prefix in the stem_comment slice above.
            self.head_comment = self.head_comment[stem_len + 1..].to_vec();
        }
    }

    // Parse the productions:
    // block_mapping        ::= BLOCK-MAPPING_START
    //                          *******************
    //                          ((KEY block_node_or_indentless_sequence?)?
    //                            *** *
    //                          (VALUE block_node_or_indentless_sequence?)?)*
    //
    //                          BLOCK-END
    //                          *********
    //
    // yaml_parser_parse_block_mapping_key
    fn yaml_parser_parse_block_mapping_key(&mut self, event: &mut RawEvent, first: bool) -> bool {
        if first {
            let Some(token) = self.peek_token() else {
                return false;
            };
            self.marks.push(token.start_mark);
            self.skip_token();
        }

        let Some(token) = self.peek_token() else {
            return false;
        };

        // [Go] A tail comment was left from the prior mapping value processed. Emit an event
        //      as it needs to be processed with that value and not the following key.
        if !self.tail_comment.is_empty() {
            *event = RawEvent::new(EventType::TailComment, token.start_mark, token.end_mark);
            event.foot_comment = std::mem::take(&mut self.tail_comment);
            return true;
        }

        if token.typ == TokenType::Key {
            let mark = token.end_mark;
            self.skip_token();
            let Some(token) = self.peek_token() else {
                return false;
            };
            if token.typ != TokenType::Key
                && token.typ != TokenType::Value
                && token.typ != TokenType::BlockEnd
            {
                self.states.push(ParserState::BlockMappingValue);
                return self.yaml_parser_parse_node(event, true, true);
            }
            self.state = ParserState::BlockMappingValue;
            return self.yaml_parser_process_empty_scalar(event, mark);
        } else if token.typ == TokenType::BlockEnd {
            self.pop_state();
            self.marks.pop();
            *event = RawEvent::new(EventType::MappingEnd, token.start_mark, token.end_mark);
            self.yaml_parser_set_event_comments(event);
            self.skip_token();
            return true;
        }

        let context_mark = self.marks.pop().unwrap_or_default();
        self.yaml_parser_set_parser_error_context(
            "while parsing a block mapping",
            context_mark,
            "did not find expected key",
            token.start_mark,
        )
    }

    // Parse the productions:
    // block_mapping        ::= BLOCK-MAPPING_START
    //
    //                          ((KEY block_node_or_indentless_sequence?)?
    //
    //                          (VALUE block_node_or_indentless_sequence?)?)*
    //                           ***** *
    //                          BLOCK-END
    //
    //
    // yaml_parser_parse_block_mapping_value
    fn yaml_parser_parse_block_mapping_value(&mut self, event: &mut RawEvent) -> bool {
        let Some(token) = self.peek_token() else {
            return false;
        };
        if token.typ == TokenType::Value {
            let mark = token.end_mark;
            self.skip_token();
            let Some(token) = self.peek_token() else {
                return false;
            };
            if token.typ != TokenType::Key
                && token.typ != TokenType::Value
                && token.typ != TokenType::BlockEnd
            {
                self.states.push(ParserState::BlockMappingKey);
                return self.yaml_parser_parse_node(event, true, true);
            }
            self.state = ParserState::BlockMappingKey;
            return self.yaml_parser_process_empty_scalar(event, mark);
        }
        self.state = ParserState::BlockMappingKey;
        self.yaml_parser_process_empty_scalar(event, token.start_mark)
    }

    // Parse the productions:
    // flow_sequence        ::= FLOW-SEQUENCE-START
    //                          *******************
    //                          (flow_sequence_entry FLOW-ENTRY)*
    //                           *                   **********
    //                          flow_sequence_entry?
    //                          *
    //                          FLOW-SEQUENCE-END
    //                          *****************
    // flow_sequence_entry  ::= flow_node | KEY flow_node? (VALUE flow_node?)?
    //                          *
    //
    // yaml_parser_parse_flow_sequence_entry
    fn yaml_parser_parse_flow_sequence_entry(&mut self, event: &mut RawEvent, first: bool) -> bool {
        if first {
            let Some(token) = self.peek_token() else {
                return false;
            };
            self.marks.push(token.start_mark);
            self.skip_token();
        }
        let Some(mut token) = self.peek_token() else {
            return false;
        };
        if token.typ != TokenType::FlowSequenceEnd {
            if !first {
                if token.typ == TokenType::FlowEntry {
                    self.skip_token();
                    let Some(t) = self.peek_token() else {
                        return false;
                    };
                    token = t;
                } else {
                    let context_mark = self.marks.pop().unwrap_or_default();
                    return self.yaml_parser_set_parser_error_context(
                        "while parsing a flow sequence",
                        context_mark,
                        "did not find expected ',' or ']'",
                        token.start_mark,
                    );
                }
            }

            if token.typ == TokenType::Key {
                self.state = ParserState::FlowSequenceEntryMappingKey;
                *event = RawEvent::new(EventType::MappingStart, token.start_mark, token.end_mark);
                event.implicit = true;
                event.flow = true;
                self.skip_token();
                return true;
            } else if token.typ != TokenType::FlowSequenceEnd {
                self.states.push(ParserState::FlowSequenceEntry);
                return self.yaml_parser_parse_node(event, false, false);
            }
        }

        self.pop_state();
        self.marks.pop();

        *event = RawEvent::new(EventType::SequenceEnd, token.start_mark, token.end_mark);
        self.yaml_parser_set_event_comments(event);

        self.skip_token();
        true
    }

    //
    // Parse the productions:
    // flow_sequence_entry  ::= flow_node | KEY flow_node? (VALUE flow_node?)?
    //                                      *** *
    //
    // yaml_parser_parse_flow_sequence_entry_mapping_key
    fn yaml_parser_parse_flow_sequence_entry_mapping_key(&mut self, event: &mut RawEvent) -> bool {
        let Some(token) = self.peek_token() else {
            return false;
        };
        if token.typ != TokenType::Value
            && token.typ != TokenType::FlowEntry
            && token.typ != TokenType::FlowSequenceEnd
        {
            self.states.push(ParserState::FlowSequenceEntryMappingValue);
            return self.yaml_parser_parse_node(event, false, false);
        }
        let mark = token.end_mark;
        self.skip_token();
        self.state = ParserState::FlowSequenceEntryMappingValue;
        self.yaml_parser_process_empty_scalar(event, mark)
    }

    // Parse the productions:
    // flow_sequence_entry  ::= flow_node | KEY flow_node? (VALUE flow_node?)?
    //                                                      ***** *
    //
    // yaml_parser_parse_flow_sequence_entry_mapping_value
    fn yaml_parser_parse_flow_sequence_entry_mapping_value(
        &mut self,
        event: &mut RawEvent,
    ) -> bool {
        let Some(token) = self.peek_token() else {
            return false;
        };
        let mut start_mark = token.start_mark;
        if token.typ == TokenType::Value {
            // [Go] `token` is a *yaml_token_t into parser.tokens; the inner
            //      `token :=` below shadows it, and the outer pointer is read
            //      again after skip_token/peek_token may have compacted or
            //      reallocated the queue.
            self.token_watch = Some(TokenWatch {
                array_gen: self.tokens_gen,
                idx: self.tokens_head,
                frozen: None,
            });
            self.skip_token();
            let inner = self.peek_token();
            let w = self.token_watch.take().expect("token watch");
            let Some(inner) = inner else {
                return false;
            };
            if inner.typ != TokenType::FlowEntry && inner.typ != TokenType::FlowSequenceEnd {
                self.states.push(ParserState::FlowSequenceEntryMappingEnd);
                return self.yaml_parser_parse_node(event, false, false);
            }
            start_mark = self.watched_token_start_mark(w);
        }
        self.state = ParserState::FlowSequenceEntryMappingEnd;
        self.yaml_parser_process_empty_scalar(event, start_mark)
    }

    // Parse the productions:
    // flow_sequence_entry  ::= flow_node | KEY flow_node? (VALUE flow_node?)?
    //                                                                      *
    //
    // yaml_parser_parse_flow_sequence_entry_mapping_end
    fn yaml_parser_parse_flow_sequence_entry_mapping_end(&mut self, event: &mut RawEvent) -> bool {
        let Some(token) = self.peek_token() else {
            return false;
        };
        self.state = ParserState::FlowSequenceEntry;
        // [Go] Shouldn't this be end_mark?
        *event = RawEvent::new(EventType::MappingEnd, token.start_mark, token.start_mark);
        true
    }

    // Parse the productions:
    // flow_mapping         ::= FLOW-MAPPING-START
    //                          ******************
    //                          (flow_mapping_entry FLOW-ENTRY)*
    //                           *                  **********
    //                          flow_mapping_entry?
    //                          ******************
    //                          FLOW-MAPPING-END
    //                          ****************
    // flow_mapping_entry   ::= flow_node | KEY flow_node? (VALUE flow_node?)?
    //                          *           *** *
    //
    // yaml_parser_parse_flow_mapping_key
    fn yaml_parser_parse_flow_mapping_key(&mut self, event: &mut RawEvent, first: bool) -> bool {
        if first {
            let Some(token) = self.peek_token() else {
                return false;
            };
            self.marks.push(token.start_mark);
            self.skip_token();
        }

        let Some(mut token) = self.peek_token() else {
            return false;
        };

        if token.typ != TokenType::FlowMappingEnd {
            if !first {
                if token.typ == TokenType::FlowEntry {
                    self.skip_token();
                    let Some(t) = self.peek_token() else {
                        return false;
                    };
                    token = t;
                } else {
                    let context_mark = self.marks.pop().unwrap_or_default();
                    return self.yaml_parser_set_parser_error_context(
                        "while parsing a flow mapping",
                        context_mark,
                        "did not find expected ',' or '}'",
                        token.start_mark,
                    );
                }
            }

            if token.typ == TokenType::Key {
                self.skip_token();
                let Some(token) = self.peek_token() else {
                    return false;
                };
                if token.typ != TokenType::Value
                    && token.typ != TokenType::FlowEntry
                    && token.typ != TokenType::FlowMappingEnd
                {
                    self.states.push(ParserState::FlowMappingValue);
                    return self.yaml_parser_parse_node(event, false, false);
                }
                self.state = ParserState::FlowMappingValue;
                return self.yaml_parser_process_empty_scalar(event, token.start_mark);
            } else if token.typ != TokenType::FlowMappingEnd {
                self.states.push(ParserState::FlowMappingEmptyValue);
                return self.yaml_parser_parse_node(event, false, false);
            }
        }

        self.pop_state();
        self.marks.pop();
        *event = RawEvent::new(EventType::MappingEnd, token.start_mark, token.end_mark);
        self.yaml_parser_set_event_comments(event);
        self.skip_token();
        true
    }

    // Parse the productions:
    // flow_mapping_entry   ::= flow_node | KEY flow_node? (VALUE flow_node?)?
    //                                   *                  ***** *
    //
    // yaml_parser_parse_flow_mapping_value
    fn yaml_parser_parse_flow_mapping_value(&mut self, event: &mut RawEvent, empty: bool) -> bool {
        let Some(mut token) = self.peek_token() else {
            return false;
        };
        if empty {
            self.state = ParserState::FlowMappingKey;
            return self.yaml_parser_process_empty_scalar(event, token.start_mark);
        }
        if token.typ == TokenType::Value {
            self.skip_token();
            let Some(t) = self.peek_token() else {
                return false;
            };
            token = t;
            if token.typ != TokenType::FlowEntry && token.typ != TokenType::FlowMappingEnd {
                self.states.push(ParserState::FlowMappingKey);
                return self.yaml_parser_parse_node(event, false, false);
            }
        }
        self.state = ParserState::FlowMappingKey;
        self.yaml_parser_process_empty_scalar(event, token.start_mark)
    }

    // Generate an empty scalar event.
    // yaml_parser_process_empty_scalar
    fn yaml_parser_process_empty_scalar(&mut self, event: &mut RawEvent, mark: Mark) -> bool {
        *event = RawEvent::new(EventType::Scalar, mark, mark);
        event.implicit = true;
        event.scalar_style = ScalarStyle::Plain;
        true
    }

    // Parse directives.
    // yaml_parser_process_directives (the version/tag directive outputs are not
    // needed by the event API and are dropped)
    fn yaml_parser_process_directives(&mut self) -> bool {
        let mut version_directive_seen = false;

        let Some(mut token) = self.peek_token() else {
            return false;
        };

        while token.typ == TokenType::VersionDirective || token.typ == TokenType::TagDirective {
            if token.typ == TokenType::VersionDirective {
                if version_directive_seen {
                    self.yaml_parser_set_parser_error(
                        "found duplicate %YAML directive",
                        token.start_mark,
                    );
                    return false;
                }
                if token.major != 1 || token.minor != 1 {
                    self.yaml_parser_set_parser_error(
                        "found incompatible YAML document",
                        token.start_mark,
                    );
                    return false;
                }
                version_directive_seen = true;
            } else if token.typ == TokenType::TagDirective {
                let value = TagDirective {
                    handle: token.value.clone(),
                    prefix: token.prefix.clone(),
                };
                if !self.yaml_parser_append_tag_directive(value, false, token.start_mark) {
                    return false;
                }
            }

            self.skip_token();
            let Some(t) = self.peek_token() else {
                return false;
            };
            token = t;
        }

        // default_tag_directives
        let defaults = [
            TagDirective {
                handle: b"!".to_vec(),
                prefix: b"!".to_vec(),
            },
            TagDirective {
                handle: b"!!".to_vec(),
                prefix: b"tag:yaml.org,2002:".to_vec(),
            },
        ];
        for d in defaults {
            if !self.yaml_parser_append_tag_directive(d, true, token.start_mark) {
                return false;
            }
        }
        true
    }

    // Append a tag directive to the directives stack.
    // yaml_parser_append_tag_directive
    fn yaml_parser_append_tag_directive(
        &mut self,
        value: TagDirective,
        allow_duplicates: bool,
        mark: Mark,
    ) -> bool {
        for td in &self.tag_directives {
            if value.handle == td.handle {
                if allow_duplicates {
                    return true;
                }
                return self.yaml_parser_set_parser_error("found duplicate %TAG directive", mark);
            }
        }
        self.tag_directives.push(value);
        true
    }
}
