"""Task-owned loopback OTLP protobuf collector and lossless raw receipts.

The tiny protobuf reader observes standard OTLP fields only; it is not a tracer
implementation. Every raw request is retained so protoc/Go can re-decode it.
"""
import gzip
import hashlib
import http.server
import json
from pathlib import Path
import threading


def varint(data, offset):
    value = 0
    for shift in range(0, 70, 7):
        if offset >= len(data): raise ValueError('truncated varint')
        byte = data[offset]; offset += 1; value |= (byte & 127) << shift
        if not byte & 128: return value, offset
    raise ValueError('oversized varint')


def fields(data):
    offset = 0; result = {}
    while offset < len(data):
        key, offset = varint(data, offset); number, wire = key >> 3, key & 7
        if number == 0: raise ValueError('invalid field zero')
        if wire == 0: value, offset = varint(data, offset)
        elif wire in (1, 5):
            size = 8 if wire == 1 else 4
            if offset + size > len(data): raise ValueError('truncated fixed field')
            value = int.from_bytes(data[offset:offset+size], 'little'); offset += size
        elif wire == 2:
            size, offset = varint(data, offset)
            if offset + size > len(data): raise ValueError('truncated bytes field')
            value = data[offset:offset+size]; offset += size
        else: raise ValueError('unsupported protobuf wire type')
        result.setdefault(number, []).append(value)
    return result


def one(data, field, default=b''): return data.get(field, [default])[-1]
def text(value): return value.decode('utf8', errors='strict')
def attributes(items):
    output = {}
    for raw in items:
        item = fields(raw); value = fields(one(item, 2))
        if 1 in value: decoded = text(one(value, 1))
        elif 2 in value: decoded = bool(one(value, 2))
        elif 3 in value:
            decoded = one(value, 3); decoded = decoded - (1 << 64) if decoded >= (1 << 63) else decoded
        else: decoded = {'raw_any_value_hex': one(item, 2).hex()}
        output[text(one(item, 1))] = decoded
    return output


def decode_export(body):
    spans = []
    for resource_raw in fields(body).get(1, []):
        resource = fields(resource_raw); attrs = attributes(fields(one(resource, 1)).get(1, []))
        for scope_raw in resource.get(2, []):
            scope_spans = fields(scope_raw); scope = fields(one(scope_spans, 1))
            for span_raw in scope_spans.get(2, []):
                span = fields(span_raw); status = fields(one(span, 15))
                spans.append({'resource': attrs, 'scope': {'name': text(one(scope, 1)), 'version': text(one(scope, 2))},
                    'trace_id': one(span, 1).hex(), 'span_id': one(span, 2).hex(), 'parent_span_id': one(span, 4).hex(),
                    'tracestate': text(one(span, 3)), 'name': text(one(span, 5)), 'kind': one(span, 6, 0),
                    'start_ns': one(span, 7, 0), 'end_ns': one(span, 8, 0), 'attributes': attributes(span.get(9, [])),
                    'events': [{'name': text(one(fields(event), 2)), 'attributes': attributes(fields(event).get(3, []))} for event in span.get(11, [])],
                    'status': {'message': text(one(status, 2)), 'code': one(status, 3, 0)}})
    return spans


class Collector:
    def __init__(self, directory):
        self.directory = Path(directory); self.directory.mkdir()
        self.lock = threading.Lock(); self.requests = []; self.spans = []; self.errors = []
        owner = self
        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_): pass
            def do_POST(self):
                try:
                    if self.path != '/v1/traces': raise ValueError('unexpected collector route')
                    size = int(self.headers.get('content-length', '-1'))
                    if not 0 <= size <= 8*1024*1024: raise ValueError('invalid export size')
                    raw = self.rfile.read(size)
                    if len(raw) != size: raise ValueError('truncated export')
                    body = gzip.decompress(raw) if self.headers.get('content-encoding') == 'gzip' else raw
                    if len(body) > 32*1024*1024: raise ValueError('oversized decoded export')
                    decoded = decode_export(body)
                    with owner.lock:
                        index = len(owner.requests); path = owner.directory / f'export-{index:04d}.bin'; path.write_bytes(raw)
                        owner.requests.append({'path': path.name, 'sha256': hashlib.sha256(raw).hexdigest(), 'headers': dict(self.headers), 'spans': len(decoded)})
                        owner.spans.extend(decoded)
                    self.send_response(200); self.send_header('Content-Type', 'application/x-protobuf'); self.send_header('Content-Length', '0'); self.end_headers()
                except Exception as error:
                    with owner.lock: owner.errors.append(str(error))
                    self.send_error(400, 'invalid OTLP export')
        self.server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, name='owned-otlp-collector')
        self.thread.start()
    @property
    def url(self): return f'http://127.0.0.1:{self.server.server_port}/v1/traces'
    def snapshot(self):
        with self.lock: return {'requests': list(self.requests), 'spans': list(self.spans), 'errors': list(self.errors)}
    def close(self):
        self.server.shutdown(); self.server.server_close(); self.thread.join(5)
        result = self.snapshot(); result['thread_exited'] = not self.thread.is_alive()
        (self.directory / 'receipt.json').write_text(json.dumps(result, indent=2)+'\n')
        return result


def invalid_legacy_agent_edge_validator(receipt):
    spans = receipt['spans']; failures = []
    for method in ['JobRequest', 'JobTerminate']:
        sent = [s for s in spans if s['name'] == 'Sent.AgentInternal.'+method]
        recv = [s for s in spans if s['name'] == 'Recv.AgentInternal.'+method]
        if not sent or not recv: failures.append(f'{method}: missing actual client/server export')
        matched = [(a,b) for a in sent for b in recv if a['trace_id'] == b['trace_id'] and a['span_id'] == b['parent_span_id']]
        if not matched: failures.append(f'{method}: no cross-node parent relationship')
        for a,b in matched:
            if a['resource'].get('livekit.parity.side') != 'a' or b['resource'].get('livekit.parity.side') != 'b': failures.append(f'{method}: not actual A-client to B-worker trace')
            if a['kind'] != 3 or b['kind'] != 2: failures.append(f'{method}: wrong CLIENT/SERVER kind')
            for s in [a,b]:
                if s['scope'] != {'name': 'github.com/livekit/psrpc', 'version': 'v0.8.0'}: failures.append(f'{method}: wrong instrumentation scope')
                if [e['name'] for e in s['events']][:2] != ['Outbound message','Inbound message']: failures.append(f'{method}: wrong completion event order')
                if s['end_ns'] < s['start_ns'] or s['start_ns'] == 0: failures.append(f'{method}: invalid span lifetime')
                if s['status']['code'] != 1: failures.append(f'{method}: actual successful lifecycle not OK')
    if receipt['errors']: failures.append('collector decode/export errors')
    return {'accepted': not failures, 'failures': failures, 'actual_span_count': len(spans)}


# Original factory graph: plain AgentInternal has no OTel options. The old
# validator above is retained as the rejected candidate20 fixture assumption.
def _shape(span, side, kind):
    failures=[]
    if span['resource'].get('livekit.parity.side')!=side:failures.append('wrong actual process side')
    if span['kind']!=kind:failures.append('wrong CLIENT/SERVER kind')
    if span['scope']!={'name':'github.com/livekit/psrpc','version':'v0.8.0'}:failures.append('wrong exact instrumentation scope')
    if [e['name'] for e in span['events']][:2]!=['Outbound message','Inbound message']:failures.append('wrong completion event order')
    if span['start_ns']==0 or span['end_ns']<span['start_ns']:failures.append('invalid actual lifetime')
    if span['status']['code']!=1:failures.append('successful business lifecycle did not have OK status')
    if len(span['trace_id'])!=32 or not int(span['trace_id'],16) or len(span['span_id'])!=16 or not int(span['span_id'],16):failures.append('invalid trace/span identity')
    return failures

def validate_business_trace(receipt):
    spans=receipt['spans'];failures=[];chains={}
    unexpected=[s['name'] for s in spans if s['name'].startswith(('Sent.AgentInternal.','Recv.AgentInternal.'))]
    if unexpected:failures.append('plain fixed AgentInternal factory unexpectedly instrumented: '+repr(unexpected))
    for method in ['CreateDispatch','DeleteDispatch']:
        sent=[s for s in spans if s['name']=='Sent.AgentDispatchInternal.'+method]
        pairs=[(outer,inner)for outer in sent for inner in sent if outer['trace_id']==inner['trace_id'] and outer['span_id']==inner['parent_span_id']]
        if not pairs:failures.append(method+': missing original two nested ClientParams/wire Sent interceptors')
        for outer,inner in pairs:
            for span in [outer,inner]:failures.extend(method+': '+f for f in _shape(span,'a',3))
            if not outer['start_ns']<=inner['start_ns']<=inner['end_ns']<=outer['end_ns']:failures.append(method+': invalid nested client lifetime')
        chains[method]=len(pairs)
    if receipt['errors']:failures.append('collector decode/export errors')
    return {'accepted':not failures,'failures':failures,'actual_span_count':len(spans),'profile':'actual AgentInternal absence plus instrumented upper client constructor','nested_client_chains':chains}

def validate_whip_trace(receipt):
    spans=receipt['spans'];failures=[]
    sent=[s for s in spans if s['name']=='Sent.WHIP.Create'];recv=[s for s in spans if s['name']=='Recv.WHIP.Create']
    triples=[(outer,inner,server)for outer in sent for inner in sent for server in recv if outer['trace_id']==inner['trace_id']==server['trace_id'] and outer['span_id']==inner['parent_span_id'] and inner['span_id']==server['parent_span_id']]
    if not triples:failures.append('missing real WHIP A-client outerSent→innerSent→B-ownerRecv chain')
    for outer,inner,server in triples:
        for span,side,kind in [(outer,'a',3),(inner,'a',3),(server,'b',2)]:failures.extend(_shape(span,side,kind))
        if not outer['start_ns']<=inner['start_ns']<=inner['end_ns']<=outer['end_ns']:failures.append('invalid nested client lifetime')
    if any(s['name'].startswith('Recv.WHIPParticipant.') for s in spans):failures.append('plain WHIPParticipant factory unexpectedly instrumented')
    if receipt['errors']:failures.append('collector decode/export errors')
    return {'accepted':not failures,'failures':failures,'actual_span_count':len(spans),'complete_whip_chains':len(triples)}

def validate_sip_ioinfo_trace(receipt):
    spans=receipt['spans'];failures=[]
    # Actual SIP worker NewRPCStateHandler emits this callback during the
    # unchanged media/Digest/REFER campaign. Fixed OSS handler is a placeholder;
    # this proves real tracing admission, NOT a durable SIP-state implementation.
    name='IOInfo.UpdateSIPCallState'
    sent=[s for s in spans if s['name']=='Sent.'+name];recv=[s for s in spans if s['name']=='Recv.'+name]
    pairs=[(a,b)for a in sent for b in recv if a['trace_id']==b['trace_id'] and a['span_id']==b['parent_span_id']]
    if not pairs:failures.append('missing actual original SIP worker A→backend B IOInfo callback trace')
    for client,server in pairs:
        failures.extend(_shape(client,'a',3));failures.extend(_shape(server,'b',2))
        if client['resource'].get('livekit.parity.kind')!='go-sip-worker':failures.append('client was not the actual original SIP worker process')
    if receipt['errors']:failures.append('collector decode/export errors')
    return {'accepted':not failures,'failures':failures,'actual_span_count':len(spans),'complete_ioinfo_chains':len(pairs),'callback_scope':'original OSS placeholder callback; media/business gates remain independent and mandatory'}
