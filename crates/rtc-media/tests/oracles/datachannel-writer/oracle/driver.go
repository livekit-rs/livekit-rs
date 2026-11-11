package datachannel

import (
 "context"
 "encoding/json"
 "errors"
 "io"
 "time"
 "github.com/pion/datachannel"
)
var oracleNow = time.Unix(1700000000,0)
func sourceNow() time.Time {return oracleNow}
type Operation struct {AtNS int64 `json:"at_ns"`; Before uint64 `json:"before"`; After uint64 `json:"after"`; Bytes int `json:"bytes"`; WriteN *int `json:"write_n,omitempty"`; Error string `json:"error,omitempty"`; DeadlineError bool `json:"deadline_error,omitempty"`}
type Case struct {ID string `json:"id"`; TargetNS int64 `json:"target_ns"`; Minimum uint64 `json:"minimum"`; Operations []Operation `json:"operations"`}
type Result struct {ID string `json:"id"`; Index int `json:"index"`; N int `json:"n"`; Error string `json:"error"`; Panic bool `json:"panic"`; RawWrites int `json:"raw_writes"`; DeadlineCalls int `json:"deadline_calls"`; DeadlineDeltaNS int64 `json:"deadline_delta_ns"`; Bitrate int `json:"bitrate"`; RateReady bool `json:"rate_ready"`; Buffered uint64 `json:"buffered"`}
type controlledDC struct {datachannel.ReadWriteCloserDeadliner; op Operation; buffered uint64; writes,deadlines int; delta int64}
func (d *controlledDC) BufferedAmount() uint64{return d.buffered}
func (d *controlledDC) SetWriteDeadline(t time.Time)error {d.deadlines++;d.delta=t.Sub(oracleNow).Nanoseconds();if d.op.DeadlineError{return io.ErrClosedPipe};return nil}
func (d *controlledDC) Write(b []byte)(int,error){d.writes++;d.buffered=d.op.After;n:=len(b);if d.op.WriteN!=nil{n=*d.op.WriteN};switch d.op.Error{case "timeout":return n,context.DeadlineExceeded;case "closed":return n,io.ErrClosedPipe};return n,nil}
func runWrite(w *DataChannelWriter[*controlledDC],d *controlledDC,op Operation)(r Result){
 defer func(){if recover()!=nil{r.Panic=true}}()
 r.N=0
 r.NError(w,d,op)
 return
}
func (r *Result) NError(w *DataChannelWriter[*controlledDC],d *controlledDC,op Operation){
 n,err:=w.Write(make([]byte,op.Bytes));r.N=n
 switch{case err==nil:r.Error="";case errors.Is(err,ErrDataDroppedByHighBufferedAmount):r.Error="high_buffered";case errors.Is(err,context.DeadlineExceeded):r.Error="deadline";case errors.Is(err,io.ErrClosedPipe):r.Error="closed_pipe";default:r.Error=err.Error()}
}
func RunOracle(in io.Reader,out io.Writer)error{
 dec:=json.NewDecoder(in);enc:=json.NewEncoder(out)
 for{var c Case;if err:=dec.Decode(&c);err!=nil{if err==io.EOF{return nil};return err}
 oracleNow=time.Unix(1700000000,0)
 d:=&controlledDC{};w:=NewDataChannelWriterUnreliable(d,d,time.Duration(c.TargetNS),c.Minimum)
 for i,op:=range c.Operations{
 oracleNow=time.Unix(1700000000,0).Add(time.Duration(op.AtNS));d.op=op;d.buffered=op.Before
 r:=runWrite(w,d,op);r.ID=c.ID;r.Index=i;r.RawWrites=d.writes;r.DeadlineCalls=d.deadlines;r.DeadlineDeltaNS=d.delta;r.Buffered=d.buffered
 if w.rate!=nil{r.Bitrate,r.RateReady=w.rate.Bitrate(oracleNow)}
 if err:=enc.Encode(r);err!=nil{return err}
 }
 }
}
