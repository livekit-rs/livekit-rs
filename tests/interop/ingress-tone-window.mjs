// Functional content readiness only: every decoded PCM sample enters one bounded
// consecutive 2s window. No silence filtering, sample skipping or signal repair.
export class DecodedToneWindow {
  constructor({sampleRate=48000,frequency=440,seconds=2}={}) {
    if(!Number.isSafeInteger(sampleRate)||sampleRate<=0||sampleRate>96000||!Number.isSafeInteger(frequency)||frequency<=0||frequency>=sampleRate/2||!Number.isSafeInteger(sampleRate*seconds)||seconds<=0)throw new RangeError('invalid exact tone/window parameters');
    this.sampleRate=sampleRate;this.frequency=frequency;this.capacity=sampleRate*seconds;
    if(this.capacity>96000)throw new RangeError('decoded window exceeds fixed 96000-sample bound');
    const gcd=(a,b)=>{while(b){const t=a%b;a=b;b=t;}return a;};this.period=sampleRate/gcd(sampleRate,frequency);
    this.sin=new Float64Array(this.period);this.cos=new Float64Array(this.period);
    for(let n=0;n<this.period;n++){this.sin[n]=Math.sin(2*Math.PI*frequency*n/sampleRate);this.cos[n]=Math.cos(2*Math.PI*frequency*n/sampleRate);}
    this.ring=new Int16Array(this.capacity);this.samples=0;this.frames=0;this.square=0;this.sine=0;this.cosine=0;this.totalSquare=0;this.totalSine=0;this.totalCosine=0;this.firstTwoSeconds=null;
  }
  push(pcm) {
    if(!(pcm instanceof Int16Array))throw new TypeError('actual decoded Int16Array PCM required');
    this.frames++;
    for(const value of pcm){
      const n=this.samples,slot=n%this.capacity;
      if(n>=this.capacity){const previous=this.ring[slot],oldPhase=(n-this.capacity)%this.period;this.square-=previous*previous;this.sine-=previous*this.sin[oldPhase];this.cosine-=previous*this.cos[oldPhase];}
      const phase=n%this.period,weightedSine=value*this.sin[phase],weightedCosine=value*this.cos[phase];this.ring[slot]=value;this.square+=value*value;this.sine+=weightedSine;this.cosine+=weightedCosine;this.totalSquare+=value*value;this.totalSine+=weightedSine;this.totalCosine+=weightedCosine;this.samples++;
      if(this.samples===this.capacity)this.firstTwoSeconds=this.window();
    }
  }
  metrics(count,square,sine,cosine,start,end) {
    const rms=count?Math.sqrt(Math.max(0,square)/count):0;
    const amplitude=count?2*Math.hypot(sine,cosine)/count:0;
    return {sample_start_inclusive:start,sample_end_exclusive:end,samples:count,rms,tone_power_ratio:rms>0?amplitude*amplitude/(2*rms*rms):0};
  }
  window(){const count=Math.min(this.samples,this.capacity);return this.metrics(count,this.square,this.sine,this.cosine,this.samples-count,this.samples);}
  readyWindow(){const current=this.window();return current.samples===this.capacity&&current.rms>100&&current.tone_power_ratio>0.5?current:null;}
  snapshot(){return {sample_rate:this.sampleRate,frequency_hz:this.frequency,required_consecutive_samples:this.capacity,decoded_frames:this.frames,decoded_samples:this.samples,retained_pcm_samples:this.ring.length,first_two_seconds:this.firstTwoSeconds?{...this.firstTwoSeconds}:null,latest_window:this.window(),whole_stream:this.metrics(this.samples,this.totalSquare,this.totalSine,this.totalCosine,0,this.samples)};}
}
