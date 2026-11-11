// Independent actual Go1.26.3 json.Decoder/MaxBytesReader oracle. No Rust port.
package main
import("encoding/hex";"encoding/json";"errors";"io";"net/http";"os")
type input struct{Name string `json:"name"`;Parts []string `json:"parts"`;FinalError bool `json:"final_error"`;Limit int64 `json:"limit"`}
type reader struct{parts []string;fail bool}
func(r *reader)Read(p []byte)(int,error){for len(r.parts)>0 {s:=r.parts[0];if len(s)==0{r.parts=r.parts[1:];continue};n:=copy(p,s);r.parts[0]=s[n:];return n,nil};if r.fail{return 0,errors.New("owned read failure")};return 0,io.EOF}
func main(){d:=json.NewDecoder(os.Stdin);e:=json.NewEncoder(os.Stdout);for{var c input;if err:=d.Decode(&c);err==io.EOF{return}else if err!=nil{panic(err)};var r io.Reader=&reader{parts:c.Parts,fail:c.FinalError};if c.Limit>0{r=http.MaxBytesReader(nil,io.NopCloser(r),c.Limit)};var raw json.RawMessage;err:=json.NewDecoder(r).Decode(&raw);status:="ok";text:="";if err!=nil{status="error";text=err.Error()};if err:=e.Encode(map[string]any{"name":c.Name,"status":status,"raw_hex":hex.EncodeToString(raw),"error":text});err!=nil{panic(err)}}}
