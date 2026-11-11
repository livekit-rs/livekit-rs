//! Ordered YAML nodes for the protocol-only configuration boundary (built from
//! the yaml.v3 node handed to `UnmarshalYAML`, see `protoyaml.rs`).
use serde_saphyr::granit_parser::ScalarStyle;

#[derive(Clone, Debug)]
pub(super) enum Value {
    Scalar(String, ScalarStyle, String),
    Float(u64),
    Alias(String),
    Map(Vec<(Node, Node)>),
    Seq(Vec<Node>),
}
#[derive(Clone, Debug)]
pub(super) struct Node {
    pub value: Value,
    pub line: usize,
    pub column: usize,
    pub flow: bool,
    pub anchor: String,
    pub anchor_id: usize,
    pub explicit_tag: Option<String>,
}
impl Node {
    pub fn text(&self) -> &str {
        match &self.value {
            Value::Scalar(s, ..) | Value::Alias(s) => s,
            _ => "",
        }
    }
    pub fn kind(&self) -> &'static str {
        match self.value {
            Value::Scalar(..) | Value::Float(_) => "scalar",
            Value::Alias(..) => "alias",
            Value::Map(_) => "mapping",
            Value::Seq(_) => "sequence",
        }
    }
    pub fn null(&self) -> bool {
        self.explicit_tag.as_deref() == Some("!!null")
            || matches!(&self.value,Value::Scalar(_,_,tag) if tag=="!!null")
    }
    pub fn empty_map(&mut self) {
        self.value = Value::Map(Vec::new());
        self.flow = true;
        self.anchor.clear();
        self.anchor_id = 0;
        self.explicit_tag = None;
    }
}

struct Writer {
    text: String,
    line: usize,
    col: usize,
}
impl Writer {
    fn put(&mut self, s: &str) {
        self.text.push_str(s);
        for c in s.chars() {
            if c == '\n' {
                self.line += 1;
                self.col = 1;
            } else {
                self.col += 1;
            }
        }
    }
    fn pad(&mut self, n: usize) {
        self.put(&" ".repeat(n));
    }
    fn loc(&self, node: &mut Node) {
        node.line = self.line;
        node.column = self.col;
    }
    fn inline(&mut self, node: &mut Node) {
        self.loc(node);
        if !node.anchor.is_empty() {
            self.put(&format!("&{} ", node.anchor));
        }
        if let Some(tag) = &node.explicit_tag {
            if tag.starts_with('!') {
                self.put(&format!("{tag} "));
            } else {
                self.put(&format!("!<{tag}> "));
            }
        }
        match &mut node.value {
            Value::Scalar(s, style, tag) => {
                if s == "<<" && *style == ScalarStyle::Plain && node.explicit_tag.is_none() {
                    self.put("!!merge ");
                }
                match style {
                    ScalarStyle::SingleQuoted => self.put(&format!("'{}'", s.replace('\'', "''"))),
                    ScalarStyle::DoubleQuoted => {
                        self.put(&serde_json::to_string(s).expect("string serialization"))
                    }
                    _ => {
                        if tag == "!!str" && s.is_empty() {
                            self.put("\"\"");
                        } else {
                            self.put(s);
                        }
                    }
                }
            }
            Value::Float(bits) => {
                let value = f64::from_bits(*bits);
                let text = if value.is_nan() {
                    ".nan".into()
                } else if value == f64::INFINITY {
                    ".inf".into()
                } else if value == f64::NEG_INFINITY {
                    "-.inf".into()
                } else if value == 0.0 && value.is_sign_negative() {
                    "-0.0".into()
                } else {
                    let mut n = value.to_string();
                    if !n.contains(['.', 'e', 'E']) {
                        n.push_str(".0");
                    }
                    n
                };
                self.put(&text);
            }
            Value::Alias(name) => self.put(&format!("*{name}")),
            Value::Map(entries) => {
                self.put("{");
                for (i, (key, value)) in entries.iter_mut().enumerate() {
                    if i != 0 {
                        self.put(", ");
                    }
                    self.inline(key);
                    self.put(": ");
                    self.inline(value);
                }
                self.put("}");
            }
            Value::Seq(entries) => {
                self.put("[");
                for (i, value) in entries.iter_mut().enumerate() {
                    if i != 0 {
                        self.put(", ");
                    }
                    self.inline(value);
                }
                self.put("]");
            }
        }
    }
    fn block(&mut self, node: &mut Node, indent: usize, first_inline: bool) {
        if node.flow
            || !matches!(&node.value,Value::Map(v) if !v.is_empty())
                && !matches!(&node.value,Value::Seq(v) if !v.is_empty())
        {
            if !first_inline {
                self.pad(indent);
            }
            self.inline(node);
            self.put("\n");
            return;
        }
        self.loc(node);
        match &mut node.value {
            Value::Map(entries) => {
                for (i, (key, value)) in entries.iter_mut().enumerate() {
                    if i != 0 || !first_inline {
                        self.pad(indent);
                    }
                    self.inline(key);
                    self.put(":");
                    let child_block = !value.flow
                        && (matches!(&value.value,Value::Map(v) if !v.is_empty())
                            || matches!(&value.value,Value::Seq(v) if !v.is_empty()));
                    if child_block {
                        self.put("\n");
                        self.block(value, indent + 4, false);
                    } else {
                        self.put(" ");
                        self.inline(value);
                        self.put("\n");
                    }
                }
            }
            Value::Seq(entries) => {
                for value in entries {
                    self.pad(indent);
                    self.put("- ");
                    if !value.flow && matches!(&value.value,Value::Map(v) if !v.is_empty()) {
                        self.block(value, indent + 2, true);
                    } else {
                        self.inline(value);
                        self.put("\n");
                    }
                }
            }
            _ => unreachable!(),
        }
    }
}
pub(super) fn render(node: &mut Node) -> String {
    let mut out = Writer {
        text: String::new(),
        line: 1,
        col: 1,
    };
    out.block(node, 0, false);
    out.text
}
