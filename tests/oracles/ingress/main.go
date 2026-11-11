// Actual pinned protocol guid.Hash and Go 1.26 net/url oracle. No local clone
// of either implementation appears in this executable adapter.
package main
import("encoding/json";"os";"net/url";"github.com/livekit/protocol/utils/guid")
type vector struct{ Kind string `json:"kind"`; Name string `json:"name"`; Prefix string `json:"prefix"`; Input string `json:"input"` }
func main(){ var rows []vector;if e:=json.NewDecoder(os.Stdin).Decode(&rows);e!=nil{panic(e)};out:=[]map[string]any{}
 for _,v:=range rows {r:=map[string]any{"kind":v.Kind,"name":v.Name,"input":v.Input}
 switch v.Kind {case "guid":r["prefix"]=v.Prefix;r["result"]=guid.Hash(v.Prefix,[]byte(v.Input))
 case "url":u,e:=url.Parse(v.Input);if e!=nil{r["error"]=e.Error()}else{r["scheme"]=u.Scheme;r["normalized"]=u.String();r["opaque"]=u.Opaque;r["raw_path"]=u.RawPath;r["path"]=u.Path;r["host"]=u.Host}
 default:panic("unknown oracle kind")};out=append(out,r)}
 if e:=json.NewEncoder(os.Stdout).Encode(out);e!=nil{panic(e)}
}
