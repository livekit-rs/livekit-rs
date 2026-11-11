// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT
// Calls fixed private methods, with no arithmetic or production source changes.
package gcc
import ("bufio"; "encoding/json"; "fmt"; "math"; "os"; "runtime"; "testing"; "time")
type numericCase struct { Name string `json:"name"`; Stage string `json:"stage"`; InitialUncertainty float64 `json:"initial_uncertainty"`; Values []int64 `json:"values"` }
func TestNumericInternalSourceOracle(t *testing.T) {
 input,output:=os.Getenv("PION_NUMERIC_SOURCE_INPUT"),os.Getenv("PION_NUMERIC_SOURCE_OUTPUT")
 if input=="" || output=="" { t.Fatal("explicit owned input and output required") }
 f,err:=os.Open(input); if err!=nil {t.Fatal(err)}; defer f.Close()
 o,err:=os.Create(output); if err!=nil {t.Fatal(err)}; defer o.Close()
 scan:=bufio.NewScanner(f);enc:=json.NewEncoder(o)
 bits:=func(f float64)string{return fmt.Sprintf("%016x",math.Float64bits(f))}
 for scan.Scan(){var c numericCase;if err:=json.Unmarshal(scan.Bytes(),&c);err!=nil{t.Fatal(err)}
  k:=newKalman(initMeasurementUncertainty(c.InitialUncertainty));a:=&exponentialMovingAverage{}
  for i,v:=range c.Values { row:=map[string]any{"case":c.Name,"stage":c.Stage,"index":i,"goos":runtime.GOOS,"goarch":runtime.GOARCH,"goversion":runtime.Version(),"goamd64":os.Getenv("GOAMD64")}
   switch c.Stage {
   case "kalman":row["estimate_ns"]=int64(k.updateEstimate(time.Duration(v)));row["error_bits"]=bits(k.estimateError);row["gain_bits"]=bits(k.gain);row["uncertainty_bits"]=bits(k.measurementUncertainty)
   case "ema":a.update(float64(v));row["mean_bits"]=bits(a.average);row["variance_bits"]=bits(a.variance);row["deviation_bits"]=bits(a.stdDeviation)
   default:t.Fatalf("unknown stage %q",c.Stage)
   };if err:=enc.Encode(row);err!=nil{t.Fatal(err)}
  }
 };if err:=scan.Err();err!=nil{t.Fatal(err)}
}
