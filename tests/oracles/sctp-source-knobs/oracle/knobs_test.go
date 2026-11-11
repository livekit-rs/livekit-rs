package sctp

import("bufio";"encoding/json";"os";"net";"testing";"fmt")
type sourceKnobInput struct {ID string `json:"id"`;Min int64 `json:"min"`;Fast int64 `json:"fast"`;Step int64 `json:"step"`}
func sourcePositive(v int64)uint32 {if v>0{return uint32(v)};return 0}
func TestSourceKnobObserver(t *testing.T){
 input:=os.Getenv("GO_SCTP_KNOB_INPUTS");if input==""{t.Fatal("GO_SCTP_KNOB_INPUTS required")}
 f,e:=os.Open(input);if e!=nil{t.Fatal(e)};defer f.Close();out:=os.Getenv("GO_SCTP_KNOB_OUTPUT");if out==""{t.Fatal("GO_SCTP_KNOB_OUTPUT required")};o,e:=os.Create(out);if e!=nil{t.Fatal(e)};defer o.Close();enc:=json.NewEncoder(o)
 scan:=bufio.NewScanner(f)
 for scan.Scan(){var c sourceKnobInput;if e=json.Unmarshal(scan.Bytes(),&c);e!=nil{t.Fatal(e)}
 left,right:=net.Pipe()
 cfg,e:=buildClientConfig(WithNetConn(left),WithMTU(1228),WithMinCwnd(sourcePositive(c.Min)),WithFastRtxWnd(sourcePositive(c.Fast)),WithCwndCAStep(sourcePositive(c.Step)))
 if e!=nil{t.Fatal(e)}
 a:=createAssociationFromConfigWithTsn(cfg,1)
 initial:=a.CWND();a.setCWND(1);floor:=a.CWND()
 a.setCWND(5000);caBefore:=a.CWND();a.ssthresh=0;a.partialBytesAcked=caBefore
 a.pendingQueue.push(&chunkPayloadData{tsn:100,beginningFragment:true,endingFragment:true,userData:[]byte{1}})
 a.onCumulativeTSNAckPointAdvanced(0)
 caAfter:=a.CWND()
 // Actual source fast-rtx selection and actual marshalled packet sizes.
 a.cumulativeTSNAckPoint=0
 for i:=uint32(1);i<=5;i++{a.inflightQueue.pushNoCheck(&chunkPayloadData{tsn:i,nSent:1,missIndicator:3,beginningFragment:true,endingFragment:true,userData:make([]byte,500)})}
 a.willRetransmitFast=true
 var consumed bool
 packets:=a.gatherOutboundFastRetransmissionPackets(nil,nil,&consumed)
 sizes:=make([]int,len(packets));for i,b:=range packets{sizes[i]=len(b)}
 selected:=0;for i:=uint32(1);i<=5;i++{chunk,_:=a.inflightQueue.get(i);if chunk.nSent>1{selected++}}
 if e=enc.Encode(map[string]any{"id":c.ID,"min":a.minCwnd,"fast":a.fastRtxWnd,"step":a.cwndCAStep,"mtu":a.MTU(),"initial":initial,"floor":floor,"ca_before":caBefore,"ca_after":caAfter,"fast_selected":selected,"packet_sizes":sizes});e!=nil{t.Fatal(e)}
 _=a.close();_=right.Close()
 }
 if e=scan.Err();e!=nil{t.Fatal(e)};fmt.Fprintf(os.Stderr,"source knob rows observed\n")
}
