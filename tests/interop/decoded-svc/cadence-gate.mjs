export function sourceStride(values){const v=values.slice(1).map((frame,index)=>(frame-values[index]+65536)%65536).filter(value=>value>0).sort((a,b)=>a-b);return v[Math.floor(v.length/2)];}
export function compareCadence(actual,reference){
 const lowStride=sourceStride(actual.lowFrameValues),highStride=sourceStride(actual.highFrameValues);
 const refLow=sourceStride(reference.lowFrameValues),refHigh=sourceStride(reference.highFrameValues);
 const comparisons=[['low FPS',actual.lowDecodedFps,reference.lowDecodedFps,.65,1.35],['high FPS',actual.highDecodedFps,reference.highDecodedFps,.65,1.35],['low source stride',lowStride,refLow,.75,1.25],['high source stride',highStride,refHigh,.75,1.25]].map(([name,value,expected,min,max])=>({name,value,expected,min,max,pass:Number.isFinite(value)&&Number.isFinite(expected)&&expected>0&&value>=expected*min&&value<=expected*max}));
 return {pass:comparisons.every(c=>c.pass),comparisons,lowSourceFrameStride:lowStride,highSourceFrameStride:highStride};
}
