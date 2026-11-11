// Actual generated protobuf Twirp client oracle. No handwritten HTTP/RPC client.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/livekit/protocol/auth"
	pb "github.com/livekit/protocol/livekit"
	"github.com/twitchtv/twirp"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"net/http"
	"os"
	"time"
)

type row struct {
	Case      string `json:"case"`
	Method    string `json:"method"`
	Code      string `json:"code"`
	SHA       string `json:"canonical_sha256"`
	canonical string
}

func ptr[T any](v T) *T      { return &v }
func digest(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }
func normalize(v any, ids map[string]string) any {
	switch x := v.(type) {
	case string:
		if ordinal, ok := ids[x]; ok {
			return ordinal
		}
		return x
	case []any:
		for i, v := range x {
			x[i] = normalize(v, ids)
		}
		return x
	case map[string]any:
		for k, v := range x {
			x[k] = normalize(v, ids)
		}
		return x
	}
	return v
}
func campaign(base, key, secret string) (rows []row, err error) {
	token, e := auth.NewAccessToken(key, secret).SetSIPGrant(&auth.SIPGrant{Admin: true}).ToJWT()
	if e != nil {
		return nil, e
	}
	parent, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	ctx, e := twirp.WithHTTPRequestHeaders(parent, http.Header{"Authorization": []string{"Bearer " + token}})
	if e != nil {
		return nil, e
	}
	client := pb.NewSIPProtobufClient(base, &http.Client{Timeout: 5 * time.Second})
	ids := map[string]string{}
	var trunkIDs, ruleIDs []string
	// Always remove exactly task-created entities, even on a failed comparison.
	defer func() {
		for _, id := range ruleIDs {
			client.DeleteSIPDispatchRule(ctx, &pb.DeleteSIPDispatchRuleRequest{SipDispatchRuleId: id})
		}
		for _, id := range trunkIDs {
			client.DeleteSIPTrunk(ctx, &pb.DeleteSIPTrunkRequest{SipTrunkId: id})
		}
	}()
	observe := func(name, method string, message proto.Message, callErr error, expected twirp.ErrorCode) error {
		r := row{Case: name, Method: method, Code: "ok"}
		var canonical []byte
		if callErr != nil {
			var tw twirp.Error
			if !errors.As(callErr, &tw) {
				return fmt.Errorf("%s non-Twirp error", name)
			}
			r.Code = string(tw.Code())
			canonical, _ = json.Marshal(map[string]any{"code": tw.Code(), "message": tw.Msg(), "meta": tw.MetaMap()})
			if tw.Code() != expected {
				return fmt.Errorf("%s unexpected code %s", name, tw.Code())
			}
		} else {
			if expected != "" {
				return fmt.Errorf("%s expected error missing", name)
			}
			raw, e := (protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}).Marshal(message)
			if e != nil {
				return e
			}
			var object any
			if e = json.Unmarshal(raw, &object); e != nil {
				return e
			}
			canonical, e = json.Marshal(normalize(object, ids))
			if e != nil {
				return e
			}
		}
		r.canonical = string(canonical)
		r.SHA = digest(canonical)
		rows = append(rows, r)
		return nil
	}
	// One trunk at a time prevents random generated ID ordering from changing list
	// order across independent servers; no response arrays are reordered by oracle.
	out, e := client.CreateSIPOutboundTrunk(ctx, &pb.CreateSIPOutboundTrunkRequest{Trunk: &pb.SIPOutboundTrunkInfo{Name: "protobuf-outbound", Address: "example.invalid", Numbers: []string{"+12025550101"}, Headers: map[string]string{"X-Test": "fixture"}}})
	if e == nil {
		if out.SipTrunkId == "" {
			return rows, fmt.Errorf("missing actual outbound ID")
		}
		ids[out.SipTrunkId] = "ST_OUT"
		trunkIDs = append(trunkIDs, out.SipTrunkId)
	}
	if e = observe("outbound-create", "CreateSIPOutboundTrunk", out, e, ""); e != nil {
		return rows, e
	}
	oid := out.SipTrunkId
	replacement := proto.Clone(out).(*pb.SIPOutboundTrunkInfo)
	replacement.Name = "protobuf-outbound-replaced"
	out, e = client.UpdateSIPOutboundTrunk(ctx, &pb.UpdateSIPOutboundTrunkRequest{SipTrunkId: oid, Action: &pb.UpdateSIPOutboundTrunkRequest_Replace{Replace: replacement}})
	if e = observe("outbound-replace", "UpdateSIPOutboundTrunk", out, e, ""); e != nil {
		return rows, e
	}
	out, e = client.UpdateSIPOutboundTrunk(ctx, &pb.UpdateSIPOutboundTrunkRequest{SipTrunkId: oid, Action: &pb.UpdateSIPOutboundTrunkRequest_Update{Update: &pb.SIPOutboundTrunkUpdate{Name: ptr("protobuf-outbound-patched"), Numbers: &pb.ListUpdate{Add: []string{"+12025550102"}}}}})
	if e = observe("outbound-list-patch", "UpdateSIPOutboundTrunk", out, e, ""); e != nil {
		return rows, e
	}
	og, e := client.GetSIPOutboundTrunk(ctx, &pb.GetSIPOutboundTrunkRequest{SipTrunkId: oid})
	if e = observe("outbound-get", "GetSIPOutboundTrunk", og, e, ""); e != nil {
		return rows, e
	}
	ol, e := client.ListSIPOutboundTrunk(ctx, &pb.ListSIPOutboundTrunkRequest{TrunkIds: []string{oid}})
	if e = observe("outbound-list", "ListSIPOutboundTrunk", ol, e, ""); e != nil {
		return rows, e
	}
	legacy, e := client.ListSIPTrunk(ctx, &pb.ListSIPTrunkRequest{})
	if e = observe("outbound-legacy-list", "ListSIPTrunk", legacy, e, ""); e != nil {
		return rows, e
	}
	deleted, e := client.DeleteSIPTrunk(ctx, &pb.DeleteSIPTrunkRequest{SipTrunkId: oid})
	if e = observe("outbound-delete", "DeleteSIPTrunk", deleted, e, ""); e != nil {
		return rows, e
	}
	og, e = client.GetSIPOutboundTrunk(ctx, &pb.GetSIPOutboundTrunkRequest{SipTrunkId: oid})
	if e = observe("outbound-missing", "GetSIPOutboundTrunk", og, e, twirp.NotFound); e != nil {
		return rows, e
	}
	in, e := client.CreateSIPInboundTrunk(ctx, &pb.CreateSIPInboundTrunkRequest{Trunk: &pb.SIPInboundTrunkInfo{Name: "protobuf-inbound", Numbers: []string{"+12025550201"}, AllowedAddresses: []string{"192.0.2.0/24"}, Headers: map[string]string{"X-Test": "fixture"}}})
	if e == nil {
		if in.SipTrunkId == "" {
			return rows, fmt.Errorf("missing actual inbound ID")
		}
		ids[in.SipTrunkId] = "ST_IN"
		trunkIDs = append(trunkIDs, in.SipTrunkId)
	}
	if e = observe("inbound-create", "CreateSIPInboundTrunk", in, e, ""); e != nil {
		return rows, e
	}
	iid := in.SipTrunkId
	inReplacement := proto.Clone(in).(*pb.SIPInboundTrunkInfo)
	inReplacement.Name = "protobuf-inbound-replaced"
	in, e = client.UpdateSIPInboundTrunk(ctx, &pb.UpdateSIPInboundTrunkRequest{SipTrunkId: iid, Action: &pb.UpdateSIPInboundTrunkRequest_Replace{Replace: inReplacement}})
	if e = observe("inbound-replace", "UpdateSIPInboundTrunk", in, e, ""); e != nil {
		return rows, e
	}
	in, e = client.UpdateSIPInboundTrunk(ctx, &pb.UpdateSIPInboundTrunkRequest{SipTrunkId: iid, Action: &pb.UpdateSIPInboundTrunkRequest_Update{Update: &pb.SIPInboundTrunkUpdate{Name: ptr("protobuf-inbound-patched"), Numbers: &pb.ListUpdate{Add: []string{"+12025550202"}}}}})
	if e = observe("inbound-list-patch", "UpdateSIPInboundTrunk", in, e, ""); e != nil {
		return rows, e
	}
	ig, e := client.GetSIPInboundTrunk(ctx, &pb.GetSIPInboundTrunkRequest{SipTrunkId: iid})
	if e = observe("inbound-get", "GetSIPInboundTrunk", ig, e, ""); e != nil {
		return rows, e
	}
	il, e := client.ListSIPInboundTrunk(ctx, &pb.ListSIPInboundTrunkRequest{TrunkIds: []string{iid}})
	if e = observe("inbound-list", "ListSIPInboundTrunk", il, e, ""); e != nil {
		return rows, e
	}
	rule, e := client.CreateSIPDispatchRule(ctx, &pb.CreateSIPDispatchRuleRequest{DispatchRule: &pb.SIPDispatchRuleInfo{Name: "protobuf-rule", TrunkIds: []string{iid}, Rule: &pb.SIPDispatchRule{Rule: &pb.SIPDispatchRule_DispatchRuleDirect{DispatchRuleDirect: &pb.SIPDispatchRuleDirect{RoomName: "protobuf-room"}}}}})
	if e == nil {
		if rule.SipDispatchRuleId == "" {
			return rows, fmt.Errorf("missing actual rule ID")
		}
		ids[rule.SipDispatchRuleId] = "SD_RULE"
		ruleIDs = append(ruleIDs, rule.SipDispatchRuleId)
	}
	if e = observe("rule-create", "CreateSIPDispatchRule", rule, e, ""); e != nil {
		return rows, e
	}
	rid := rule.SipDispatchRuleId
	ruleReplacement := proto.Clone(rule).(*pb.SIPDispatchRuleInfo)
	ruleReplacement.Name = "protobuf-rule-replaced"
	rule, e = client.UpdateSIPDispatchRule(ctx, &pb.UpdateSIPDispatchRuleRequest{SipDispatchRuleId: rid, Action: &pb.UpdateSIPDispatchRuleRequest_Replace{Replace: ruleReplacement}})
	if e = observe("rule-replace", "UpdateSIPDispatchRule", rule, e, ""); e != nil {
		return rows, e
	}
	rule, e = client.UpdateSIPDispatchRule(ctx, &pb.UpdateSIPDispatchRuleRequest{SipDispatchRuleId: rid, Action: &pb.UpdateSIPDispatchRuleRequest_Update{Update: &pb.SIPDispatchRuleUpdate{Name: ptr("protobuf-rule-patched"), TrunkIds: &pb.ListUpdate{Set: []string{iid}}}}})
	if e = observe("rule-list-patch", "UpdateSIPDispatchRule", rule, e, ""); e != nil {
		return rows, e
	}
	rl, e := client.ListSIPDispatchRule(ctx, &pb.ListSIPDispatchRuleRequest{DispatchRuleIds: []string{rid}})
	if e = observe("rule-list", "ListSIPDispatchRule", rl, e, ""); e != nil {
		return rows, e
	}
	rule, e = client.DeleteSIPDispatchRule(ctx, &pb.DeleteSIPDispatchRuleRequest{SipDispatchRuleId: rid})
	if e = observe("rule-delete", "DeleteSIPDispatchRule", rule, e, ""); e != nil {
		return rows, e
	}
	rule, e = client.DeleteSIPDispatchRule(ctx, &pb.DeleteSIPDispatchRuleRequest{SipDispatchRuleId: rid})
	if e = observe("rule-missing", "DeleteSIPDispatchRule", rule, e, twirp.NotFound); e != nil {
		return rows, e
	}
	deleted, e = client.DeleteSIPTrunk(ctx, &pb.DeleteSIPTrunkRequest{SipTrunkId: iid})
	if e = observe("inbound-delete", "DeleteSIPTrunk", deleted, e, ""); e != nil {
		return rows, e
	}
	deleted, e = client.DeleteSIPTrunk(ctx, &pb.DeleteSIPTrunkRequest{SipTrunkId: iid})
	if e = observe("inbound-delete-again", "DeleteSIPTrunk", deleted, e, ""); e != nil {
		return rows, e
	}
	ig, e = client.GetSIPInboundTrunk(ctx, &pb.GetSIPInboundTrunkRequest{SipTrunkId: iid})
	if e = observe("inbound-missing", "GetSIPInboundTrunk", ig, e, twirp.NotFound); e != nil {
		return rows, e
	}
	il, e = client.ListSIPInboundTrunk(ctx, &pb.ListSIPInboundTrunkRequest{})
	if e == nil && len(il.Items) != 0 {
		return rows, fmt.Errorf("owned inbound store not empty")
	}
	if e = observe("empty-inbound", "ListSIPInboundTrunk", il, e, ""); e != nil {
		return rows, e
	}
	ol, e = client.ListSIPOutboundTrunk(ctx, &pb.ListSIPOutboundTrunkRequest{})
	if e == nil && len(ol.Items) != 0 {
		return rows, fmt.Errorf("owned outbound store not empty")
	}
	if e = observe("empty-outbound", "ListSIPOutboundTrunk", ol, e, ""); e != nil {
		return rows, e
	}
	rl, e = client.ListSIPDispatchRule(ctx, &pb.ListSIPDispatchRuleRequest{})
	if e == nil && len(rl.Items) != 0 {
		return rows, fmt.Errorf("owned rule store not empty")
	}
	if e = observe("empty-rule", "ListSIPDispatchRule", rl, e, ""); e != nil {
		return rows, e
	}
	return rows, nil
}
func main() {
	first := flag.String("first-url", "", "fixed Go HTTP base")
	second := flag.String("second-url", "", "Rust HTTP base")
	key := flag.String("api-key", "", "same test API key")
	secret := flag.String("api-secret", "", "same test API secret")
	out := flag.String("out", "", "safe report path")
	flag.Parse()
	if *first == "" || *second == "" || *key == "" || *secret == "" || *out == "" {
		fmt.Fprintln(os.Stderr, "all flags required")
		os.Exit(2)
	}
	a, err := campaign(*first, *key, *secret)
	if err != nil {
		fmt.Fprintln(os.Stderr, "first generated-client campaign failed:", err)
		os.Exit(1)
	}
	b, err := campaign(*second, *key, *secret)
	if err != nil {
		fmt.Fprintln(os.Stderr, "second generated-client campaign failed:", err)
		os.Exit(1)
	}
	status := "PASS"
	methods := map[string]bool{}
	if len(a) != len(b) {
		status = "FAIL"
	} else {
		for i := range a {
			methods[a[i].Method] = true
			if a[i].Case != b[i].Case || a[i].canonical != b[i].canonical {
				status = "FAIL"
			}
		}
	}
	if len(methods) != 14 {
		status = "FAIL"
	}
	report := struct {
		Status  string `json:"status"`
		Scope   string `json:"scope"`
		Methods int    `json:"distinct_configuration_methods"`
		First   []row  `json:"first_generated_client_results"`
		Second  []row  `json:"second_generated_client_results"`
	}{status, "actual fixed generated SIP protobuf client; only generated IDs normalized; no telephone worker claim", len(methods), a, b}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		panic(err)
	}
	if err = os.WriteFile(*out, append(data, '\n'), 0600); err != nil {
		panic(err)
	}
	fmt.Printf("%s %d configuration methods %d paired protobuf cases\n", status, len(methods), len(a))
	if status != "PASS" {
		os.Exit(1)
	}
}
