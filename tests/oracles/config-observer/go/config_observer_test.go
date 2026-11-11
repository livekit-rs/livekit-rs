// Config differential observer (SPEC-0073 M1 W1-G1/G2/G3), original side.
//
// This file is copied into a STAGED COPY of references/livekit-stable/cmd/server
// (package main) and run as a Go test, so it calls the real upstream
// `baseFlags`, `config.GenerateCLIFlags` and `getConfig` from main.go. It never
// changes upstream code. See ../README.md for build/run commands.
//
// Modes (env):
//   CONFIG_OBSERVER_LEAVES=<out.json>  writes every generated flag (name, flag
//                                      type, kind, default text, env var).
//   CONFIG_OBSERVER_TYPES=<out.json>   writes the Go type (reflect Type.String())
//                                      of every yaml-tagged field of config.Config,
//                                      the `into <type>` text of yaml.v3 errors.
//   CONFIG_OBSERVER_CASES=<cases.json> CONFIG_OBSERVER_OUT=<results.json>
//                                      runs every case and writes results.
//
// Per case two outcomes are recorded:
//   getconfig: getConfig(c) exactly as startServer calls it.
//   run:       getConfig(c), then conf.ValidateKeys() and conf.LoadTURNSecrets()
//              (the remaining config steps of startServer before service wiring);
//              comparable with lk-config's EntryAction::RunServer.
// Each outcome is {"ok": {path: canonical}} or {"error": "<err.Error()>"}
// (+ "usage": true when cmd.Run printed "Incorrect Usage" before failing).

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/urfave/cli/v3"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/livekit-server/version"
)

type observerCase struct {
	ID        string            `json:"id"`
	Argv      []string          `json:"argv"`
	Env       map[string]string `json:"env"`
	Files     map[string]string `json:"files"`
	FileModes map[string]string `json:"file_modes"`
}

type observerOutcome struct {
	OK    map[string]string `json:"ok,omitempty"`
	Error *string           `json:"error,omitempty"`
	Usage bool              `json:"usage,omitempty"`
	// Set when cmd.Run returned without running the root Action (help, version, subcommand).
	NoAction string `json:"no_action,omitempty"`
}

type observerResult struct {
	ID        string           `json:"id"`
	GetConfig *observerOutcome `json:"getconfig"`
	Run       *observerOutcome `json:"run"`
}

func TestConfigObserver(t *testing.T) {
	if out := os.Getenv("CONFIG_OBSERVER_LEAVES"); out != "" {
		writeLeaves(t, out)
	}
	if out := os.Getenv("CONFIG_OBSERVER_TYPES"); out != "" {
		writeTypes(t, out)
	}
	casesPath := os.Getenv("CONFIG_OBSERVER_CASES")
	if casesPath == "" {
		if os.Getenv("CONFIG_OBSERVER_LEAVES") == "" && os.Getenv("CONFIG_OBSERVER_TYPES") == "" {
			t.Skip("CONFIG_OBSERVER_CASES not set")
		}
		return
	}
	outPath := os.Getenv("CONFIG_OBSERVER_OUT")
	if outPath == "" {
		t.Fatal("CONFIG_OBSERVER_OUT not set")
	}
	raw, err := os.ReadFile(casesPath)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Cases []observerCase `json:"cases"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	results := make([]observerResult, 0, len(doc.Cases))
	for _, c := range doc.Cases {
		results = append(results, runObserverCase(t, c))
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", " ")
	if err := enc.Encode(map[string]any{
		"observer": "go getConfig (references/livekit-stable cmd/server)",
		"version":  version.Version,
		"results":  results,
	}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(outPath, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
}

// cloneFlags returns pristine copies of flag structs (urfave flags keep parse state).
func cloneFlags(in []cli.Flag) []cli.Flag {
	out := make([]cli.Flag, 0, len(in))
	for _, f := range in {
		v := reflect.ValueOf(f)
		nv := reflect.New(v.Type().Elem())
		nv.Elem().Set(v.Elem())
		out = append(out, nv.Interface().(cli.Flag))
	}
	return out
}

func clearObservedEnv() {
	for _, kv := range os.Environ() {
		k, _, _ := strings.Cut(kv, "=")
		if strings.HasPrefix(k, "LIVEKIT_") || strings.HasPrefix(k, "REDIS_") || k == "NODE_IP" || k == "UDP_PORT" {
			_ = os.Unsetenv(k)
		}
	}
}

func runObserverCase(t *testing.T, c observerCase) (res observerResult) {
	res.ID = c.ID
	dir, err := os.MkdirTemp("", "cfgobs-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	sub := func(s string) string { return strings.ReplaceAll(s, "{dir}", dir) }
	for name, body := range c.Files {
		mode := os.FileMode(0o600)
		if m, ok := c.FileModes[name]; ok {
			p, err := strconv.ParseUint(m, 8, 32)
			if err != nil {
				t.Fatal(err)
			}
			mode = os.FileMode(p)
		}
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(sub(body)), mode); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(p, mode); err != nil {
			t.Fatal(err)
		}
	}
	clearObservedEnv()
	defer clearObservedEnv()
	for k, v := range c.Env {
		_ = os.Setenv(k, sub(v))
	}
	argv := []string{"livekit-server"}
	for _, a := range c.Argv {
		argv = append(argv, sub(a))
	}
	errText := func(e error) *string { s := strings.ReplaceAll(e.Error(), dir, "{dir}"); return &s }
	scrub := func(m map[string]string) map[string]string {
		for k, v := range m {
			m[k] = strings.ReplaceAll(v, dir, "{dir}")
		}
		return m
	}

	defer func() {
		if r := recover(); r != nil {
			s := fmt.Sprintf("panic: %v", r)
			res.GetConfig = &observerOutcome{Error: &s}
			res.Run = &observerOutcome{Error: &s}
		}
	}()

	base := cloneFlags(baseFlags)
	generated, err := config.GenerateCLIFlags(base, true)
	if err != nil {
		t.Fatal(err)
	}
	ran := false
	noAction := ""
	var stdout, stderr bytes.Buffer
	stub := func(name string) cli.ActionFunc {
		return func(context.Context, *cli.Command) error { noAction = "subcommand:" + name; return nil }
	}
	cmd := &cli.Command{
		Name:        "livekit-server",
		Usage:       "High performance WebRTC server",
		Description: "run without subcommands to start the server",
		Flags:       append(base, generated...),
		Action: func(ctx context.Context, cc *cli.Command) error {
			ran = true
			conf, err := getConfig(cc)
			if err != nil {
				res.GetConfig = &observerOutcome{Error: errText(err)}
				res.Run = &observerOutcome{Error: errText(err)}
				return err
			}
			res.GetConfig = &observerOutcome{OK: scrub(Canonical(conf))}
			if err := conf.ValidateKeys(); err != nil {
				res.Run = &observerOutcome{Error: errText(err)}
				return err
			}
			if err := conf.LoadTURNSecrets(); err != nil {
				res.Run = &observerOutcome{Error: errText(err)}
				return err
			}
			res.Run = &observerOutcome{OK: scrub(Canonical(conf))}
			return nil
		},
		Commands: []*cli.Command{
			{Name: "generate-keys", Action: stub("generate-keys")},
			{Name: "ports", Action: stub("ports")},
			{Name: "create-join-token", Hidden: true, Action: stub("create-join-token"), Flags: []cli.Flag{
				&cli.StringFlag{Name: "room", Required: true},
				&cli.StringFlag{Name: "identity", Required: true},
				&cli.BoolFlag{Name: "recorder"},
			}},
			{Name: "list-nodes", Action: stub("list-nodes")},
			{Name: "help-verbose", Action: stub("help-verbose")},
		},
		Version:        version.Version,
		Writer:         &stdout,
		ErrWriter:      &stderr,
		ExitErrHandler: func(context.Context, *cli.Command, error) {},
	}
	runErr := cmd.Run(context.Background(), argv)
	if !ran {
		if runErr != nil {
			// urfave prints "Incorrect Usage: <err>" + help only for real usage errors;
			// other pre-Action failures (e.g. env parse errors) are plain errors.
			o := &observerOutcome{Error: errText(runErr), Usage: strings.Contains(stderr.String(), "Incorrect Usage")}
			res.GetConfig, res.Run = o, o
		} else {
			if noAction == "" {
				noAction = "help-or-version"
			}
			o := &observerOutcome{NoAction: noAction}
			res.GetConfig, res.Run = o, o
		}
	}
	return res
}

func writeLeaves(t *testing.T, out string) {
	flags, err := config.GenerateCLIFlags(cloneFlags(baseFlags), true)
	if err != nil {
		t.Fatal(err)
	}
	type leaf struct {
		Name     string `json:"name"`
		FlagType string `json:"flag_type"`
		Kind     string `json:"kind"`
		Default  string `json:"default"`
		Env      string `json:"env"`
	}
	// Flag types are aliases of the generic cli.FlagBase[T, ...]; classify by T.
	kinds := map[string]string{
		"bool": "Bool", "string": "Str", "int": "Int", "int64": "Int",
		"uint": "Uint", "uint64": "Uint64", "float64": "Float", "time.Duration": "Duration",
	}
	leaves := []leaf{}
	for _, f := range flags {
		ft := reflect.TypeOf(f).String()
		dt := ""
		if d, ok := f.(interface{ GetDefaultText() string }); ok {
			dt = d.GetDefaultText()
		}
		env := ""
		if e, ok := f.(interface{ GetEnvVars() []string }); ok && len(e.GetEnvVars()) > 0 {
			env = e.GetEnvVars()[0]
		}
		param := ft
		if i := strings.Index(param, "["); i >= 0 {
			param = param[i+1:]
		}
		if i := strings.Index(param, ","); i >= 0 {
			param = param[:i]
		}
		k, ok := kinds[param]
		if !ok {
			k = "?" + ft
		}
		leaves = append(leaves, leaf{Name: f.Names()[0], FlagType: ft, Kind: k, Default: dt, Env: env})
	}
	sort.Slice(leaves, func(i, j int) bool { return leaves[i].Name < leaves[j].Name })
	defaults := Canonical(&config.DefaultConfig)
	b, _ := json.MarshalIndent(map[string]any{"leaves": leaves, "default_tree": defaults}, "", " ")
	if err := os.WriteFile(out, b, 0o644); err != nil {
		t.Fatal(err)
	}
}

// ---------------------------------------------------------------------------
// Canonical rendering
// ---------------------------------------------------------------------------
//
// Walk the struct by yaml tags (skip `yaml:"-"` and unexported fields, flatten
// `,inline`, untagged exported fields use the lowercased Go name like yaml.v3),
// dotted paths. Scalars: bool true/false, ints decimal, floats
// FormatFloat('g', -1, bitsize), time.Duration via String(), strings raw.
// Slices/maps (and protobuf messages) are compact canonical JSON at their path
// (sorted keys, every nested scalar a JSON string by the same rules, no HTML
// escaping). Nil pointers/slices/maps/interfaces are "null". Protobuf messages
// (room.room_configurations values) are protojson with UseProtoNames,
// UseEnumNumbers, EmitUnpopulated; JSON numbers/bools are then turned into
// strings and null object members removed (unset messages / oneofs).

var (
	durationType = reflect.TypeOf(time.Duration(0))
	protoType    = reflect.TypeOf((*proto.Message)(nil)).Elem()
)

func Canonical(conf *config.Config) map[string]string {
	out := map[string]string{}
	renderValue(reflect.ValueOf(conf).Elem(), "", out)
	return out
}

func joinPath(prefix, name string) string {
	if prefix == "" {
		return name
	}
	return prefix + "." + name
}

func yamlField(f reflect.StructField) (name string, inline, skip bool) {
	if !f.IsExported() {
		return "", false, true
	}
	tag := f.Tag.Get("yaml")
	if tag == "-" {
		return "", false, true
	}
	parts := strings.Split(tag, ",")
	name = parts[0]
	for _, p := range parts[1:] {
		if p == "inline" {
			inline = true
		}
	}
	if name == "" && !inline {
		name = strings.ToLower(f.Name)
	}
	return name, inline, false
}

func isProto(v reflect.Value) bool {
	return v.Kind() == reflect.Ptr && v.Type().Implements(protoType)
}

func renderValue(v reflect.Value, path string, out map[string]string) {
	if isProto(v) {
		if v.IsNil() {
			out[path] = "null"
		} else {
			out[path] = jsonText(toJSON(v))
		}
		return
	}
	switch v.Kind() {
	case reflect.Ptr, reflect.Interface:
		if v.IsNil() {
			out[path] = "null"
			return
		}
		renderValue(v.Elem(), path, out)
	case reflect.Struct:
		t := v.Type()
		for i := 0; i < t.NumField(); i++ {
			name, inline, skip := yamlField(t.Field(i))
			if skip {
				continue
			}
			if inline {
				renderValue(v.Field(i), path, out)
			} else {
				renderValue(v.Field(i), joinPath(path, name), out)
			}
		}
	case reflect.Slice, reflect.Map:
		if v.IsNil() {
			out[path] = "null"
			return
		}
		out[path] = jsonText(toJSON(v))
	default:
		out[path] = scalarText(v)
	}
}

func scalarText(v reflect.Value) string {
	switch v.Kind() {
	case reflect.Bool:
		return strconv.FormatBool(v.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if v.Type() == durationType {
			return time.Duration(v.Int()).String()
		}
		return strconv.FormatInt(v.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return strconv.FormatUint(v.Uint(), 10)
	case reflect.Float32:
		return strconv.FormatFloat(v.Float(), 'g', -1, 32)
	case reflect.Float64:
		return strconv.FormatFloat(v.Float(), 'g', -1, 64)
	case reflect.String:
		return v.String()
	default:
		return "<unsupported:" + v.Kind().String() + ">"
	}
}

func toJSON(v reflect.Value) any {
	if isProto(v) {
		if v.IsNil() {
			return nil
		}
		b, err := protojson.MarshalOptions{UseProtoNames: true, UseEnumNumbers: true, EmitUnpopulated: true}.Marshal(v.Interface().(proto.Message))
		if err != nil {
			return "<protojson error: " + err.Error() + ">"
		}
		d := json.NewDecoder(bytes.NewReader(b))
		d.UseNumber()
		var g any
		if err := d.Decode(&g); err != nil {
			return "<protojson decode error: " + err.Error() + ">"
		}
		return normalizeProtoJSON(g)
	}
	switch v.Kind() {
	case reflect.Ptr, reflect.Interface:
		if v.IsNil() {
			return nil
		}
		return toJSON(v.Elem())
	case reflect.Struct:
		m := map[string]any{}
		structJSON(v, m)
		return m
	case reflect.Slice:
		if v.IsNil() {
			return nil
		}
		a := make([]any, 0, v.Len())
		for i := 0; i < v.Len(); i++ {
			a = append(a, toJSON(v.Index(i)))
		}
		return a
	case reflect.Map:
		if v.IsNil() {
			return nil
		}
		m := map[string]any{}
		iter := v.MapRange()
		for iter.Next() {
			m[fmt.Sprint(iter.Key().Interface())] = toJSON(iter.Value())
		}
		return m
	default:
		return scalarText(v)
	}
}

func structJSON(v reflect.Value, m map[string]any) {
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		name, inline, skip := yamlField(t.Field(i))
		if skip {
			continue
		}
		if inline {
			structJSON(v.Field(i), m)
		} else {
			m[name] = toJSON(v.Field(i))
		}
	}
}

func normalizeProtoJSON(g any) any {
	switch x := g.(type) {
	case map[string]any:
		m := map[string]any{}
		for k, v := range x {
			if v == nil {
				continue
			}
			m[k] = normalizeProtoJSON(v)
		}
		return m
	case []any:
		a := make([]any, len(x))
		for i, v := range x {
			a[i] = normalizeProtoJSON(v)
		}
		return a
	case json.Number:
		return x.String()
	case bool:
		return strconv.FormatBool(x)
	default:
		return x
	}
}

func jsonText(g any) string {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(g); err != nil {
		return "<json error: " + err.Error() + ">"
	}
	return strings.TrimSuffix(buf.String(), "\n")
}

// writeTypes walks config.Config with yaml.v3 field naming (tag name, else the
// lowercased Go name; ",inline" flattens) and records every field's Go type.
// Element types of slices/maps/pointers are recorded under "<path>[]" /
// "<path>{}" so a decoder can name the exact type of nested values.
func writeTypes(t *testing.T, out string) {
	types := map[string]string{"": reflect.TypeOf(config.Config{}).String()}
	var walk func(path string, typ reflect.Type, depth int)
	walk = func(path string, typ reflect.Type, depth int) {
		if depth > 12 {
			return
		}
		switch typ.Kind() {
		case reflect.Ptr:
			walk(path, typ.Elem(), depth+1)
			return
		case reflect.Slice:
			types[path+"[]"] = typ.Elem().String()
			walk(path+"[]", typ.Elem(), depth+1)
			return
		case reflect.Map:
			types[path+"{}"] = typ.Elem().String()
			types[path+"{key}"] = typ.Key().String()
			walk(path+"{}", typ.Elem(), depth+1)
			return
		case reflect.Struct:
		default:
			return
		}
		if typ.PkgPath() == "time" || strings.HasPrefix(typ.PkgPath(), "google.golang.org/protobuf") {
			return
		}
		if _, ok := reflect.New(typ).Interface().(proto.Message); ok {
			return
		}
		for i := 0; i < typ.NumField(); i++ {
			f := typ.Field(i)
			if f.PkgPath != "" {
				continue
			}
			tag := f.Tag.Get("yaml")
			if tag == "-" {
				continue
			}
			name, opts, _ := strings.Cut(tag, ",")
			if strings.Contains(","+opts+",", ",inline,") {
				walk(path, f.Type, depth+1)
				continue
			}
			if name == "" {
				name = strings.ToLower(f.Name)
			}
			p := name
			if path != "" {
				p = path + "." + name
			}
			types[p] = f.Type.String()
			walk(p, f.Type, depth+1)
		}
	}
	walk("", reflect.TypeOf(config.Config{}), 0)
	raw, err := json.MarshalIndent(types, "", " ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(out, append(raw, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}
