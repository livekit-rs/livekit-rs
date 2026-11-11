// Test driver only: actual pinned unmodified go-homedir and os.ExpandEnv.
package main
import("encoding/hex";"encoding/json";"fmt";"os";"github.com/mitchellh/go-homedir")
func main(){if len(os.Args)!=2{panic("one hex encoded path required")};p,e:=hex.DecodeString(os.Args[1]);if e!=nil{panic(e)};value,e:=homedir.Expand(os.ExpandEnv(string(p)));message,kind:="","";if e!=nil{message=e.Error();kind=fmt.Sprintf("%T",e)};if e=json.NewEncoder(os.Stdout).Encode(map[string]string{"path_hex":hex.EncodeToString([]byte(value)),"error":message,"error_type":kind});e!=nil{panic(e)}}
