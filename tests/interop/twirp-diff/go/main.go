// Twirp/HTTP differential client (SPEC-0073 W2-G1 / W5).
//
// Staged as cmd/twirpdiff inside a copy of references/livekit-stable so it
// resolves the pinned protocol module. For every step it mints ONE token with
// the real protocol `auth` package and sends byte-identical requests to the
// official server (A) and the Rust server (B), recording status, selected
// headers and the body (protobuf bodies are decoded with the pinned
// descriptors). It never interprets results; compare.py does.
package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"
)

type step struct {
	ID       string            `json:"id"`
	Kind     string            `json:"kind"` // twirp (default) | http | sleep
	Service  string            `json:"service"`
	Method   string            `json:"method"`
	HTTPVerb string            `json:"verb"`
	Path     string            `json:"path"`
	Codec    string            `json:"codec"` // json | proto
	Auth     json.RawMessage   `json:"auth"`  // "none" | "bad_secret" | "expired" | "garbage" | {grants}
	Body     json.RawMessage   `json:"body"`
	Raw      *string           `json:"raw"`
	RawB64   *string           `json:"raw_b64"`
	Headers  map[string]string `json:"headers"`
	SleepMs  int               `json:"sleep_ms"`
}

type response struct {
	Status      int               `json:"status"`
	Headers     map[string]string `json:"headers"`
	Body        string            `json:"body"`
	BodyJSON    any               `json:"body_json,omitempty"`
	BodyB64     string            `json:"body_b64,omitempty"`
	DecodeError string            `json:"decode_error,omitempty"`
	Error       string            `json:"error,omitempty"`
}

type result struct {
	ID string    `json:"id"`
	A  *response `json:"a"`
	B  *response `json:"b"`
}

var timeout = flag.Duration("timeout", 20*time.Second, "per-request client timeout")

var keptHeaders = []string{"Content-Type", "Access-Control-Allow-Origin", "Access-Control-Expose-Headers",
	"Access-Control-Allow-Methods", "Access-Control-Allow-Headers", "Allow", "Location", "Www-Authenticate"}

func mint(raw json.RawMessage, key, secret string) (string, error) {
	var s string
	if len(raw) == 0 {
		s = "none"
	} else if err := json.Unmarshal(raw, &s); err != nil {
		s = ""
	}
	switch s {
	case "none":
		return "", nil
	case "garbage":
		return "Bearer not-a-jwt", nil
	}
	var g auth.ClaimGrants
	if s == "" {
		if err := json.Unmarshal(raw, &g); err != nil {
			return "", err
		}
	} else {
		g.Video = &auth.VideoGrant{RoomAdmin: true, RoomCreate: true, RoomList: true}
	}
	sec := secret
	if s == "bad_secret" {
		sec = secret + "-wrong"
	}
	if s == "unknown_key" {
		key = "unknownkey"
	}
	at := auth.NewAccessToken(key, sec)
	id := g.Identity
	if id == "" {
		id = "twirp-diff"
	}
	at.SetIdentity(id).SetValidFor(10 * time.Minute)
	if s == "expired" {
		at.SetValidFor(-time.Minute)
	}
	if g.Video != nil {
		at.SetVideoGrant(g.Video)
	}
	if g.SIP != nil {
		at.SetSIPGrant(g.SIP)
	}
	if g.Agent != nil {
		at.SetAgentGrant(g.Agent)
	}
	if g.RoomConfig != nil {
		at.SetRoomConfig((*livekit.RoomConfiguration)(g.RoomConfig))
	}
	tok, err := at.ToJWT()
	if err != nil {
		return "", err
	}
	switch s {
	case "lower_bearer":
		return "bearer " + tok, nil
	case "no_bearer":
		return tok, nil
	}
	return "Bearer " + tok, nil
}

func methodTypes(service, method string) (protoreflect.MessageDescriptor, protoreflect.MessageDescriptor, error) {
	d, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(service))
	if err != nil {
		return nil, nil, err
	}
	m := d.(protoreflect.ServiceDescriptor).Methods().ByName(protoreflect.Name(method))
	if m == nil {
		return nil, nil, fmt.Errorf("no method %s.%s", service, method)
	}
	return m.Input(), m.Output(), nil
}

func body(s step) ([]byte, protoreflect.MessageDescriptor, error) {
	if s.RawB64 != nil {
		b, err := base64.StdEncoding.DecodeString(*s.RawB64)
		return b, nil, err
	}
	if s.Raw != nil {
		return []byte(*s.Raw), nil, nil
	}
	var out protoreflect.MessageDescriptor
	if s.Kind == "" || s.Kind == "twirp" {
		in, o, err := methodTypes("livekit."+s.Service, s.Method)
		if err == nil {
			out = o
			if s.Codec == "proto" {
				msg := dynamicpb.NewMessage(in)
				b := s.Body
				if len(b) == 0 {
					b = []byte("{}")
				}
				if err := protojson.Unmarshal(b, msg); err != nil {
					return nil, nil, err
				}
				pb, err := proto.MarshalOptions{Deterministic: true}.Marshal(msg)
				return pb, out, err
			}
		}
	}
	if len(s.Body) == 0 {
		return []byte("{}"), out, nil
	}
	return []byte(s.Body), out, nil
}

func send(base string, s step, payload []byte, authz string, out protoreflect.MessageDescriptor) *response {
	verb, path := "POST", s.Path
	if s.Kind == "" || s.Kind == "twirp" {
		path = "/twirp/livekit." + s.Service + "/" + s.Method
	} else if s.HTTPVerb != "" {
		verb = s.HTTPVerb
	}
	var rd io.Reader
	if verb != "GET" && verb != "HEAD" || len(payload) > 0 && s.Raw != nil {
		rd = bytes.NewReader(payload)
	}
	req, err := http.NewRequest(verb, base+path, rd)
	if err != nil {
		return &response{Error: err.Error()}
	}
	if s.Kind == "" || s.Kind == "twirp" {
		if s.Codec == "proto" {
			req.Header.Set("Content-Type", "application/protobuf")
		} else {
			req.Header.Set("Content-Type", "application/json")
		}
	}
	if authz != "" {
		req.Header.Set("Authorization", authz)
	}
	for k, v := range s.Headers {
		req.Header.Set(k, v)
	}
	client := &http.Client{Timeout: *timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	if err != nil {
		return &response{Error: err.Error()}
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	r := &response{Status: resp.StatusCode, Headers: map[string]string{}}
	for _, h := range keptHeaders {
		if v := resp.Header.Values(h); len(v) > 0 {
			r.Headers[h] = strings.Join(v, ", ")
		}
	}
	ct := resp.Header.Get("Content-Type")
	switch {
	case strings.HasPrefix(ct, "application/protobuf") && out != nil && resp.StatusCode == 200:
		r.BodyB64 = base64.StdEncoding.EncodeToString(raw)
		msg := dynamicpb.NewMessage(out)
		if err := proto.Unmarshal(raw, msg); err != nil {
			r.DecodeError = err.Error()
		} else {
			j, _ := protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}.Marshal(msg)
			var v any
			_ = json.Unmarshal(j, &v)
			r.BodyJSON = v
		}
	default:
		r.Body = string(raw)
		var v any
		if json.Unmarshal(raw, &v) == nil {
			r.BodyJSON = v
		}
	}
	return r
}

func main() {
	cases := flag.String("cases", "", "steps json")
	a := flag.String("a", "", "official base URL")
	b := flag.String("b", "", "rust base URL")
	key := flag.String("key", "devkey", "")
	secret := flag.String("secret", "", "")
	out := flag.String("out", "", "")
	only := flag.String("only", "", "comma-separated step id prefixes")
	flag.Parse()
	raw, err := os.ReadFile(*cases)
	if err != nil {
		panic(err)
	}
	var doc struct {
		Steps []step `json:"steps"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		panic(err)
	}
	var results []result
	for _, s := range doc.Steps {
		if *only != "" {
			keep := false
			for _, p := range strings.Split(*only, ",") {
				keep = keep || strings.HasPrefix(s.ID, p)
			}
			if !keep {
				continue
			}
		}
		if s.Kind == "sleep" {
			time.Sleep(time.Duration(s.SleepMs) * time.Millisecond)
			continue
		}
		res := result{ID: s.ID}
		authz, err := mint(s.Auth, *key, *secret)
		if err != nil {
			res.A = &response{Error: "mint: " + err.Error()}
			res.B = res.A
		} else if payload, outDesc, err := body(s); err != nil {
			res.A = &response{Error: "body: " + err.Error()}
			res.B = res.A
		} else {
			res.A = send(*a, s, payload, authz, outDesc)
			res.B = send(*b, s, payload, authz, outDesc)
		}
		results = append(results, res)
	}
	j, _ := json.MarshalIndent(map[string]any{"results": results}, "", " ")
	if err := os.WriteFile(*out, j, 0o644); err != nil {
		panic(err)
	}
}
