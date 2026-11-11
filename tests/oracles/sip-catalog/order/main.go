// Actual standard-library order oracle. Run only with the pinned Go 1.26.0.
package main
import("cmp";"encoding/json";"fmt";"os";"runtime";"slices")
type item struct{Key string `json:"key"`;Ordinal int `json:"ordinal"`}
type fixture struct{Name string `json:"name"`;Input []item `json:"input"`;Expected []int `json:"expected_ordinals"`}
func main(){
 if runtime.Version()!="go1.26.0"{fmt.Fprintln(os.Stderr,"requires actual go1.26.0");os.Exit(2)}
 fixtures:=make([]fixture,0)
 add:=func(name string,items []item){original:=append([]item{},items...);slices.SortFunc(items,func(a,b item)int{return cmp.Compare(a.Key,b.Key)});expected:=make([]int,0,len(items));for _,v:=range items{expected=append(expected,v.Ordinal)};fixtures=append(fixtures,fixture{name,original,expected})}
 for _,n:=range []int{0,1,12,13,49,50,127,1011}{for _,pattern:=range []string{"ascending","descending","equal","sawtooth","nil-groups","organ-pipe","lcg"}{
  items:=make([]item,0,n);var state uint64=137
  for i:=0;i<n;i++{k:=i
   switch pattern{case "descending":k=n-i;case "equal":k=0;case "sawtooth":k=i%13;case "nil-groups":k=i%7;case "organ-pipe":if i>n/2{k=n-i};case "lcg":state=state*6364136223846793005+1;k=int((state>>32)%31)}
   key:=fmt.Sprintf("ST_%04d",k);if pattern=="nil-groups"&&k==0{key=""};items=append(items,item{key,i})
  };add(fmt.Sprintf("%s-%d",pattern,n),items)
 }}
 // The actual 1011-row cross-store regression shape: legacy records, modern
 // inbound bulk, modern outbound; same ID carries different source ordinals.
 merged:=make([]item,0,1011)
 for _,key:=range []string{"ST_a","ST_c","ST_d","ST_a","ST_e"}{merged=append(merged,item{key,len(merged)})}
 for i:=0;i<1005;i++{merged=append(merged,item{fmt.Sprintf("ST_p%04d",i),len(merged)})}
 merged=append(merged,item{"ST_b",len(merged)});add("legacy-modern-merge-1011",merged)
 json.NewEncoder(os.Stdout).Encode(struct{Go string `json:"go_version"`;Fixtures []fixture `json:"fixtures"`}{runtime.Version(),fixtures})
}
