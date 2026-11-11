// Actual pinned Pion ICE/DTLS/SCTP + unchanged LiveKit DataChannelWriter.
// Emits one bounded TSV result, never SDP, candidates, payload or keys.
package main

import (
 "bytes"
 "errors"
 "fmt"
 "io"
 "os"
 "time"

 lkdc "github.com/livekit/livekit-server/pkg/sfu/datachannel"
 piondc "github.com/pion/datachannel"
 "github.com/pion/sctp"
 "github.com/pion/webrtc/v4"
)

type opened struct { dc *webrtc.DataChannel; raw piondc.ReadWriteCloserDeadliner }
func fail(err error) { fmt.Fprintln(os.Stderr, "F93 oracle failed:", err);os.Exit(1) }
func check(err error) { if err != nil { fail(err) } }
func await[T any](ch <-chan T, label string) T { select {case v:=<-ch:return v;case <-time.After(10*time.Second):fail(fmt.Errorf("%s deadline",label));var z T;return z} }
func main() { run("reliable"); run("lossy") }
func run(kind string) {
 se:=webrtc.SettingEngine{};se.DetachDataChannels();se.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4})
 api:=webrtc.NewAPI(webrtc.WithSettingEngine(se));left,err:=api.NewPeerConnection(webrtc.Configuration{});check(err);defer left.Close();right,err:=api.NewPeerConnection(webrtc.Configuration{});check(err);defer right.Close()
 a:=make(chan opened,1);b:=make(chan opened,1);errs:=make(chan error,2)
 attach:=func(dc *webrtc.DataChannel,dst chan opened){dc.OnOpen(func(){raw,e:=dc.DetachWithDeadline();if e!=nil{errs<-e;return};dst<-opened{dc,raw}})}
 right.OnDataChannel(func(dc *webrtc.DataChannel){attach(dc,b)})
 var options *webrtc.DataChannelInit
 label:="_reliable"
 if kind=="lossy" { label="_lossy";ordered:=false;retransmits:=uint16(0); options=&webrtc.DataChannelInit{Ordered:&ordered,MaxRetransmits:&retransmits} }
 dc,err:=left.CreateDataChannel(label,options);check(err);attach(dc,a)
 offer,err:=left.CreateOffer(nil);check(err);lg:=webrtc.GatheringCompletePromise(left);check(left.SetLocalDescription(offer));await(lg,"left gathering");check(right.SetRemoteDescription(*left.LocalDescription()))
 answer,err:=right.CreateAnswer(nil);check(err);rg:=webrtc.GatheringCompletePromise(right);check(right.SetLocalDescription(answer));await(rg,"right gathering");check(left.SetRemoteDescription(*right.LocalDescription()))
 var lo,ro opened
 select{case lo=<-a:case err=<-errs:fail(err);case <-time.After(10*time.Second):fail(errors.New("left actual channel open deadline"))}
 select{case ro=<-b:case err=<-errs:fail(err);case <-time.After(10*time.Second):fail(errors.New("right actual channel open deadline"))}
 writer:=lkdc.NewDataChannelWriterReliable(ro.dc,ro.raw,0)
 if kind=="lossy" { writer=lkdc.NewDataChannelWriterUnreliable(ro.dc,ro.raw,0,0) }
 marker:=[]byte("actual-open-write");n,err:=writer.Write(marker);check(err);if n!=len(marker){fail(errors.New("real writer short initial write"))}
 check(lo.raw.SetReadDeadline(time.Now().Add(5*time.Second)));buf:=make([]byte,128);n,err=lo.raw.Read(buf);check(err);if !bytes.Equal(buf[:n],marker){fail(errors.New("actual encrypted peer payload mismatch"))}
 // This is a legitimate peer stream reset, not an injected callback/error.
 check(lo.dc.Close());check(ro.raw.SetReadDeadline(time.Now().Add(5*time.Second)));_,readErr:=ro.raw.Read(buf);if !errors.Is(readErr,io.EOF) && !errors.Is(readErr,io.ErrClosedPipe) && !errors.Is(readErr,sctp.ErrStreamClosed) { fail(fmt.Errorf("real stream reset did not produce terminal read: %w",readErr)) }
 _,writeErr:=writer.Write([]byte("actual-post-reset-write"))
 classification:="other";if writeErr==nil{classification="none"}else if errors.Is(writeErr,sctp.ErrStreamClosed){classification="sctp.ErrStreamClosed"}else if errors.Is(writeErr,io.ErrClosedPipe){classification="io.ErrClosedPipe"}
 // Exact closed-error pair used by fixed TransportManager.handleSendDataResult.
 fatal:=errors.Is(writeErr,sctp.ErrStreamClosed)||errors.Is(writeErr,io.ErrClosedPipe)
 fmt.Printf("real-peer-stream-reset\t%s\ttrue\t%s\t%t\n",kind,classification,fatal)
}
