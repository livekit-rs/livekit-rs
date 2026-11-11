// Development-only: real pinned generated messages and Google protojson; no handwritten decoder.
package main

import (
    "bytes"
    "encoding/base64"
    "encoding/json"
    "fmt"
    "os"

    _ "github.com/livekit/protocol/livekit"
    "google.golang.org/protobuf/encoding/protojson"
    "google.golang.org/protobuf/proto"
    "google.golang.org/protobuf/reflect/protoreflect"
    "google.golang.org/protobuf/reflect/protoregistry"
    _ "google.golang.org/protobuf/types/known/emptypb"
    _ "google.golang.org/protobuf/types/known/fieldmaskpb"
    _ "google.golang.org/protobuf/types/known/structpb"
    _ "google.golang.org/protobuf/types/known/wrapperspb"
)

func main() {
    if len(os.Args) != 2 { panic("usage: protojson-oracle cases.json") }
    var corpus struct {
        EmitUnpopulated bool `json:"emit_unpopulated"`
        Cases []struct { Case string `json:"case"`; Type string `json:"type"`; Body *string `json:"body"`; BodyBase64 *string `json:"body_base64"` } `json:"cases"`
    }
    data, err := os.ReadFile(os.Args[1]); if err != nil { panic(err) }
    if err = json.Unmarshal(data, &corpus); err != nil { panic(err) }
    out := json.NewEncoder(os.Stdout)
    for _, c := range corpus.Cases {
        mt, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName(c.Type))
        if err != nil { panic(err) }
        msg := mt.New().Interface()
        // Preserve exact bytes for invalid UTF-8 vectors. Corpus setup errors
        // abort the oracle rather than masquerading as semantic rejections.
        if (c.Body == nil) == (c.BodyBase64 == nil) { panic("exactly one body or body_base64 is required") }
        var body []byte
        if c.Body != nil { body = []byte(*c.Body) } else {
            body, err = base64.StdEncoding.Strict().DecodeString(*c.BodyBase64)
            if err != nil { panic(err) }
        }
        var raw json.RawMessage
        err = json.NewDecoder(bytes.NewReader(body)).Decode(&raw)
        if err == nil { err = (protojson.UnmarshalOptions{DiscardUnknown:true}).Unmarshal(raw, msg) }
        row := map[string]any{"case":c.Case,"type":c.Type,"decoded":err == nil}
        if err != nil { row["error"] = err.Error() } else {
            encoded, err := (protojson.MarshalOptions{UseProtoNames:true, EmitUnpopulated:corpus.EmitUnpopulated}).Marshal(msg)
            if err != nil { panic(err) }
            row["message"] = json.RawMessage(encoded)
            wire, err := proto.Marshal(msg)
            if err != nil { panic(err) }
            row["wire_base64"] = base64.StdEncoding.EncodeToString(wire)
        }
        if err = out.Encode(row); err != nil { panic(err) }
    }
    fmt.Fprintln(os.Stderr, "typed oracle completed")
}
