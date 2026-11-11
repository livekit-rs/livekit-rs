// Standard Go crypto/tls witness. Build with the pinned reference Go toolchain;
// no SDK, verification callbacks, unsafe bypasses or production changes.
package main
import (
 "crypto/tls"
 "crypto/x509"
 "encoding/json"
 "flag"
 "fmt"
 "io"
 "net"
 "os"
 "runtime"
 "time"
)
func main(){
 address:=flag.String("address","","peer")
 ca:=flag.String("ca","","explicit CA PEM")
 name:=flag.String("name","localhost","verified DNS name")
 version:=flag.String("version","1.3","TLS version")
 seconds:=flag.Int64("time",1830297600,"verification Unix time")
 flag.Parse()
 output:=map[string]any{"runtime":runtime.Version(),"accepted":false}
 err:=func()error{
  pem,err:=os.ReadFile(*ca);if err!=nil{return err}
  roots:=x509.NewCertPool();if !roots.AppendCertsFromPEM(pem){return fmt.Errorf("empty explicit roots")}
  v:=uint16(tls.VersionTLS13);if *version=="1.2"{v=tls.VersionTLS12}
  cfg:=&tls.Config{RootCAs:roots,ServerName:*name,MinVersion:v,MaxVersion:v,Time:func()time.Time{return time.Unix(*seconds,0)}}
  conn,err:=tls.DialWithDialer(&net.Dialer{Timeout:10*time.Second},"tcp",*address,cfg);if err!=nil{return err};defer conn.Close()
  _=conn.SetDeadline(time.Now().Add(10*time.Second))
  if _,err=conn.Write([]byte("ping"));err!=nil{return err}
  buf:=make([]byte,4);if _,err=io.ReadFull(conn,buf);err!=nil{return err};if string(buf)!="pong"{return fmt.Errorf("wrong actual payload")}
  output["version"]=conn.ConnectionState().Version;return nil
 }()
 if err!=nil{output["error"]=err.Error()}else{output["accepted"]=true;output["error"]=""}
 _=json.NewEncoder(os.Stdout).Encode(output)
}
