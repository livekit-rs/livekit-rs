from pathlib import Path
import re,json,hashlib
p=Path('/Users/zoujiaqing/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.3.darwin-arm64/src/unicode/tables.go');s=p.read_text().split('var _CaseRanges = []CaseRange{',1)[1].split('\n}',1)[0];found={}
for a,b,upper in re.findall(r'\{0x([0-9A-F]+), 0x([0-9A-F]+), d\{([^,]+),[^}]+\}\}',s):
 lo,hi=int(a,16),int(b,16)
 for cp in range(max(lo,128),hi+1):
  up=lo+((cp-lo)&~1) if upper=='UpperLower' else cp+int(upper)
  if 65<=up<=90:found[cp]=up
assert found=={0x131:ord('I'),0x17f:ord('S')},found
print(json.dumps({'status':'SOURCE_TABLE_PARSE_NO_COMPILER','source':str(p),'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'non_ascii_simple_upper_preimage_of_ASCII_enum_letters':{f'U+{k:04X}':chr(v)for k,v in found.items()},'scope':'finite ASCII names of modeled TrackSource; not a generic Unicode uppercaser'},indent=2))
