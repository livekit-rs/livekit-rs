package main
import("os";"log"; source "local.test/livekit-data-writer-oracle/source")
func main(){if len(os.Args)!=2{log.Fatal("cases path required")};f,e:=os.Open(os.Args[1]);if e!=nil{log.Fatal(e)};defer f.Close();if e=source.RunOracle(f,os.Stdout);e!=nil{log.Fatal(e)}}
