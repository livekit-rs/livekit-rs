// Source-executing oracle: unmodified Go 1.26.3 os/syscall environment readers.
package main
import("encoding/hex";"encoding/json";"os";"strings")
type Query struct{KeyHex string `json:"key_hex"`;GetenvHex string `json:"getenv_hex"`;LookupHex string `json:"lookup_hex"`;Found bool `json:"found"`;EnvironHex []string `json:"environ_hex"`}
func main(){out:=[]Query{};for _,arg:=range os.Args[1:]{b,e:=hex.DecodeString(arg);if e!=nil{panic(e)};key:=string(b);v,found:=os.LookupEnv(key);q:=Query{KeyHex:arg,GetenvHex:hex.EncodeToString([]byte(os.Getenv(key))),LookupHex:hex.EncodeToString([]byte(v)),Found:found,EnvironHex:[]string{}};for _,entry:=range os.Environ(){k,_,ok:=strings.Cut(entry,"=");if ok&&k==key{q.EnvironHex=append(q.EnvironHex,hex.EncodeToString([]byte(entry)))}};out=append(out,q)};if e:=json.NewEncoder(os.Stdout).Encode(out);e!=nil{panic(e)}}
