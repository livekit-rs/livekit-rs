// SPDX-License-Identifier: Apache-2.0
// Source observer: original fixed generated Node decoder, not a wire reimplementation.
package main
import("bufio";"encoding/hex";"encoding/json";"fmt";"os";"github.com/livekit/protocol/livekit";"google.golang.org/protobuf/proto";"google.golang.org/protobuf/encoding/protojson")
func main(){s:=bufio.NewScanner(os.Stdin);s.Buffer(make([]byte,4096),1<<20);out:=json.NewEncoder(os.Stdout);for s.Scan(){var c struct{ID string `json:"id"`;Hex string `json:"hex"`};if e:=json.Unmarshal(s.Bytes(),&c);e!=nil{panic(e)};raw,e:=hex.DecodeString(c.Hex);if e!=nil{panic(e)};var node livekit.Node;row:=map[string]any{"id":c.ID};if e=proto.Unmarshal(raw,&node);e!=nil{row["accepted"]=false;row["error"]=e.Error()}else{row["accepted"]=true;b,e:=protojson.Marshal(&node);if e!=nil{panic(e)};row["known_json"]=json.RawMessage(b)};if e=out.Encode(row);e!=nil{panic(e)}};if e:=s.Err();e!=nil{fmt.Fprintln(os.Stderr,e);os.Exit(1)}}
