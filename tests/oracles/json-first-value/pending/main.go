// Controlled actual decoder read boundary, not a timeout or fabricated EOF.
// The second Read announces itself and blocks; only after observing whether the
// decoder demanded it does the driver release EOF to collect/clean the goroutine.
package main
import("encoding/json";"io";"os")
type input struct{Name string `json:"name"`;Prefix string `json:"prefix"`}
type reader struct{bytes []byte; attempted chan struct{};release chan struct{};announced bool}
func(r *reader)Read(p []byte)(int,error){if len(r.bytes)>0{n:=copy(p,r.bytes);r.bytes=r.bytes[n:];return n,nil};if !r.announced {close(r.attempted);r.announced=true};<-r.release;return 0,io.EOF}
type result struct{raw json.RawMessage;err error}
func main(){dec:=json.NewDecoder(os.Stdin);enc:=json.NewEncoder(os.Stdout);for{var c input;if err:=dec.Decode(&c);err==io.EOF{return}else if err!=nil{panic(err)};r:=&reader{bytes:[]byte(c.Prefix),attempted:make(chan struct{}),release:make(chan struct{})};done:=make(chan result,1);go func(){var raw json.RawMessage;err:=json.NewDecoder(r).Decode(&raw);done<-result{raw,err}}();demanded:=false;var got result;select {case got=<-done:case <-r.attempted:demanded=true;close(r.release);got=<-done};if !demanded{close(r.release)};text:="";if got.err!=nil{text=got.err.Error()};if err:=enc.Encode(map[string]any{"name":c.Name,"demanded_second_read":demanded,"final_error":text});err!=nil{panic(err)}}}
