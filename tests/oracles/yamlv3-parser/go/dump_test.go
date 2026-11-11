// Differential reference dumper for the Rust port of the yaml.v3 event parser.
//
// This file lives inside an unmodified copy of gopkg.in/yaml.v3 v3.0.1 (package
// yaml) so that it can drive the unexported libyaml-style API directly:
// yaml_parser_parse is called in a loop exactly like (*parser).peek() does, and
// every event (or the final error fields plus the real (*parser).fail() text)
// is dumped in a line format that tests/differential.rs reproduces from the
// Rust port. The yaml.Decoder error for the same input is dumped as well.
//
// Run: CORPUS=../../tests/data/corpus.txt DUMP=../../tests/data/go_dump.txt \
//      go test -run TestDumpEvents -count=1 .
package yaml

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
)

func dumpEsc(b []byte) string {
	var sb strings.Builder
	sb.WriteByte('"')
	for _, c := range b {
		if c >= 0x20 && c < 0x7f && c != '\\' && c != '"' {
			sb.WriteByte(c)
		} else {
			fmt.Fprintf(&sb, "\\x%02x", c)
		}
	}
	sb.WriteByte('"')
	return sb.String()
}

func dumpOpt(b []byte) string {
	if len(b) == 0 {
		return "-"
	}
	return dumpEsc(b)
}

func corpusUnescape(s string) []byte {
	var out []byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c != '\\' {
			out = append(out, c)
			continue
		}
		i++
		switch s[i] {
		case 'n':
			out = append(out, '\n')
		case 'r':
			out = append(out, '\r')
		case 't':
			out = append(out, '\t')
		case '\\':
			out = append(out, '\\')
		case 'x':
			v, err := strconv.ParseUint(s[i+1:i+3], 16, 8)
			if err != nil {
				panic(err)
			}
			out = append(out, byte(v))
			i += 2
		default:
			panic("bad corpus escape")
		}
	}
	return out
}

func dumpMark(m yaml_mark_t) string {
	return fmt.Sprintf("%d.%d.%d", m.index, m.line, m.column)
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

func dumpEvent(ev *yaml_event_t) string {
	var sb strings.Builder
	sb.WriteString("EV ")
	switch ev.typ {
	case yaml_STREAM_START_EVENT:
		sb.WriteString("STREAM-START")
	case yaml_STREAM_END_EVENT:
		sb.WriteString("STREAM-END")
	case yaml_DOCUMENT_START_EVENT:
		fmt.Fprintf(&sb, "DOC-START implicit=%d", b2i(ev.implicit))
	case yaml_DOCUMENT_END_EVENT:
		fmt.Fprintf(&sb, "DOC-END implicit=%d", b2i(ev.implicit))
	case yaml_ALIAS_EVENT:
		fmt.Fprintf(&sb, "ALIAS anchor=%s", dumpEsc(ev.anchor))
	case yaml_SCALAR_EVENT:
		fmt.Fprintf(&sb, "SCALAR anchor=%s tag=%s value=%s style=%d pi=%d qi=%d",
			dumpOpt(ev.anchor), dumpOpt(ev.tag), dumpEsc(ev.value), ev.style,
			b2i(ev.implicit), b2i(ev.quoted_implicit))
	case yaml_SEQUENCE_START_EVENT:
		fmt.Fprintf(&sb, "SEQ-START anchor=%s tag=%s implicit=%d flow=%d",
			dumpOpt(ev.anchor), dumpOpt(ev.tag), b2i(ev.implicit),
			b2i(ev.sequence_style() == yaml_FLOW_SEQUENCE_STYLE))
	case yaml_SEQUENCE_END_EVENT:
		sb.WriteString("SEQ-END")
	case yaml_MAPPING_START_EVENT:
		fmt.Fprintf(&sb, "MAP-START anchor=%s tag=%s implicit=%d flow=%d",
			dumpOpt(ev.anchor), dumpOpt(ev.tag), b2i(ev.implicit),
			b2i(ev.mapping_style() == yaml_FLOW_MAPPING_STYLE))
	case yaml_MAPPING_END_EVENT:
		sb.WriteString("MAP-END")
	case yaml_TAIL_COMMENT_EVENT:
		sb.WriteString("TAIL")
	default:
		fmt.Fprintf(&sb, "UNKNOWN-%d", ev.typ)
	}
	fmt.Fprintf(&sb, " %s-%s h=%s l=%s f=%s", dumpMark(ev.start_mark), dumpMark(ev.end_mark),
		dumpEsc(ev.head_comment), dumpEsc(ev.line_comment), dumpEsc(ev.foot_comment))
	return sb.String()
}

func dumpFailText(p *parser) (s string) {
	defer func() {
		if v := recover(); v != nil {
			if e, ok := v.(yamlError); ok {
				s = e.err.Error()
			} else {
				panic(v)
			}
		}
	}()
	p.fail()
	return "NOFAIL"
}

const dumpVerbatimEvents = 200

var dumpEmitted int

func dumpCase(w *bufio.Writer, input []byte) {
	defer func() {
		if v := recover(); v != nil {
			fmt.Fprintf(w, "PANIC %s\n", dumpEsc([]byte(fmt.Sprint(v))))
		}
	}()
	p := newParserFromReader(strings.NewReader(string(input)))
	// Event lines after the first dumpVerbatimEvents are folded into a
	// FNV-1a 64 hash to keep the dump small for very deep inputs.
	var rest uint64 = 14695981039346656037
	restN := 0
	defer func() {
		if restN > 0 {
			fmt.Fprintf(w, "EVREST n=%d fnv64=%016x\n", restN, rest)
		}
	}()
	emit := func(line string) {
		if restN == 0 && dumpEmitted < dumpVerbatimEvents {
			dumpEmitted++
			fmt.Fprintf(w, "%s\n", line)
			return
		}
		for _, c := range []byte(line + "\n") {
			rest ^= uint64(c)
			rest *= 1099511628211
		}
		restN++
	}
	dumpEmitted = 0
	for i := 0; i < 1000000; i++ {
		var ev yaml_event_t
		ok := yaml_parser_parse(&p.parser, &ev)
		if !ok || p.parser.error != yaml_NO_ERROR {
			if restN > 0 {
				fmt.Fprintf(w, "EVREST n=%d fnv64=%016x\n", restN, rest)
				restN = 0
			}
			kind := "?"
			switch p.parser.error {
			case yaml_READER_ERROR:
				kind = "reader"
			case yaml_SCANNER_ERROR:
				kind = "scanner"
			case yaml_PARSER_ERROR:
				kind = "parser"
			}
			fmt.Fprintf(w, "ERR kind=%s problem=%s pmark=%s poff=%d pval=%d ctx=%s cmark=%s\n",
				kind, dumpEsc([]byte(p.parser.problem)), dumpMark(p.parser.problem_mark),
				p.parser.problem_offset, p.parser.problem_value,
				dumpEsc([]byte(p.parser.context)), dumpMark(p.parser.context_mark))
			fmt.Fprintf(w, "MSG %s\n", dumpEsc([]byte(dumpFailText(p))))
			break
		}
		if ev.typ == yaml_NO_EVENT {
			fmt.Fprintf(w, "NOEVENT\n")
			break
		}
		emit(dumpEvent(&ev))
		if ev.typ == yaml_STREAM_END_EVENT {
			break
		}
	}
}

func dumpDecode(w *bufio.Writer, input []byte) {
	defer func() {
		if v := recover(); v != nil {
			fmt.Fprintf(w, "DEC PANIC %s\n", dumpEsc([]byte(fmt.Sprint(v))))
		}
	}()
	var n Node
	err := NewDecoder(strings.NewReader(string(input))).Decode(&n)
	if err == nil {
		fmt.Fprintf(w, "DEC ok\n")
	} else {
		fmt.Fprintf(w, "DEC %s\n", dumpEsc([]byte(err.Error())))
	}
}

func TestDumpEvents(t *testing.T) {
	corpus := os.Getenv("CORPUS")
	dump := os.Getenv("DUMP")
	if corpus == "" || dump == "" {
		t.Skip("CORPUS and DUMP not set")
	}
	data, err := os.ReadFile(corpus)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(dump)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()
	n := 0
	for _, line := range strings.Split(string(data), "\n") {
		if line == "" {
			continue
		}
		tab := strings.IndexByte(line, '\t')
		if tab < 0 {
			t.Fatalf("bad corpus line %q", line)
		}
		input := corpusUnescape(line[tab+1:])
		fmt.Fprintf(w, "=== %s\n", line[:tab])
		dumpCase(w, input)
		dumpDecode(w, input)
		n++
	}
	t.Logf("dumped %d cases", n)
}
