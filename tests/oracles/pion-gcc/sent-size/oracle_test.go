// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT
package cc
import("bufio";"encoding/hex";"encoding/json";"os";"testing";"time";"github.com/pion/interceptor";"github.com/pion/rtp")
func TestSentSizeSourceOracle(t *testing.T){f,err:=os.Open(os.Getenv("PION_SENT_SIZE_INPUT"));if err!=nil{t.Fatal(err)};defer f.Close();o,err:=os.Create(os.Getenv("PION_SENT_SIZE_OUTPUT"));if err!=nil{t.Fatal(err)};defer o.Close();scan:=bufio.NewScanner(f);enc:=json.NewEncoder(o)
 for scan.Scan(){var c struct{Name string `json:"name"`;Payload int `json:"payload_bytes"`;Padding uint8 `json:"padding_bytes"`;TWCC bool `json:"twcc_negotiated"`;Extension bool `json:"extension_present"`;SSRC uint32 `json:"ssrc"`;CSRC []uint32 `json:"csrc"`;Sequence uint16 `json:"sequence"`;TransportSequence uint16 `json:"transport_sequence"`};if err:=json.Unmarshal(scan.Bytes(),&c);err!=nil{t.Fatal(err)}
  h:=rtp.Header{Version:2,Padding:c.Padding!=0,PaddingSize:c.Padding,SequenceNumber:c.Sequence,SSRC:c.SSRC,CSRC:c.CSRC}
  if c.Extension{ext:=rtp.TransportCCExtension{TransportSequence:c.TransportSequence};b,err:=ext.Marshal();if err!=nil{t.Fatal(err)};if err=h.SetExtension(3,b);err!=nil{t.Fatal(err)}}
  payload:=make([]byte,c.Payload);packet:=rtp.Packet{Header:h,Payload:payload};wire,err:=packet.Marshal();if err!=nil{t.Fatal(err)}
  attributes:=interceptor.Attributes{};if c.TWCC{attributes.Set(TwccExtensionAttributesKey,uint8(3))};a:=NewFeedbackAdapter();err=a.OnSent(time.Unix(1000,0),&h,len(payload),attributes)
  row:=map[string]any{"case":c.Name,"header_bytes":h.MarshalSize(),"payload_bytes":len(payload),"padding_bytes":h.PaddingSize,"wire_bytes":len(wire),"wire_hex":hex.EncodeToString(wire),"history":[]map[string]any{}}
  if err!=nil{row["error"]=err.Error()}else{entries:=[]map[string]any{};for e:=a.history.evictList.Front();e!=nil;e=e.Next(){v:=e.Value.(Acknowledgment);entries=append(entries,map[string]any{"sequence":v.SequenceNumber,"ssrc":v.SSRC,"size":v.Size,"departure_unix_ns":v.Departure.UnixNano()})};row["history"]=entries}
  if err:=enc.Encode(row);err!=nil{t.Fatal(err)}
 };if err:=scan.Err();err!=nil{t.Fatal(err)}
}
