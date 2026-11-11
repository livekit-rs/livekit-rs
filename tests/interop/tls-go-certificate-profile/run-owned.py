#!/usr/bin/env python3
"""Actual Go/Rust TLS1.2/1.3 verdict and peer-alert comparison.

No Cargo/Go builds. Requires prebuilt fixed-Go witness and rtc-tls go_profile
integration test binary. Python SSL is only an external test peer; production
TLS remains RustCrypto/WebPKI. No verdict/error text is normalized to success.
"""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import socket
import ssl
import subprocess
import tempfile
import threading
import time
HERE=Path(__file__).resolve().parent
SERVER=HERE.parents[2]
FIXTURES=SERVER/"crates/rtc-tls/tests/fixtures"
PROFILE=FIXTURES/"go-profile"
sha=lambda value:hashlib.sha256(value).hexdigest()
def identity(path):
    path=Path(path).resolve();return {"path":str(path),"sha256":sha(path.read_bytes())}
def run(argv,env=None):
    process=subprocess.Popen([str(value) for value in argv],env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    try:stdout,stderr=process.communicate(timeout=20)
    except BaseException:
        if process.poll() is None:
            process.terminate()
            try:process.communicate(timeout=2)
            except subprocess.TimeoutExpired:process.kill();process.communicate(timeout=3)
        raise
    return process.returncode,stdout,stderr

def alert_number(reason):
    # Interprets the external TLS engine's actual received alert description;
    # retain its raw error/reason too. This is not a fabricated Go diagnostic.
    return {"SSLV3_ALERT_BAD_CERTIFICATE":42,"TLS_ALERT_BAD_CERTIFICATE":42,
            "SSLV3_ALERT_CERTIFICATE_UNKNOWN":46,"TLS_ALERT_CERTIFICATE_UNKNOWN":46,"TLSV1_ALERT_UNKNOWN_CA":48,
            "SSLV3_ALERT_CERTIFICATE_EXPIRED":45,"SSLV3_ALERT_UNSUPPORTED_CERTIFICATE":43,
            "SSLV3_ALERT_BAD_CERTIFICATE_STATUS_RESPONSE":113}.get(reason)

class Peer:
    def __init__(self,certificate,key,version):
        self.context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        self.context.minimum_version=self.context.maximum_version=ssl.TLSVersion.TLSv1_2 if version=="1.2" else ssl.TLSVersion.TLSv1_3
        self.context.load_cert_chain(certificate,key)
        self.listener=socket.socket();self.listener.bind(("127.0.0.1",0));self.listener.listen();self.listener.settimeout(.2)
        self.address="127.0.0.1:"+str(self.listener.getsockname()[1]);self.stop=threading.Event();self.condition=threading.Condition();self.results={};self.workers=[];self.accepted=0
        self.thread=threading.Thread(target=self.accept,daemon=True);self.thread.start()
    def accept(self):
        while not self.stop.is_set():
            try:connection,_=self.listener.accept()
            except socket.timeout:continue
            except OSError:return
            index=self.accepted;self.accepted+=1
            worker=threading.Thread(target=self.handle,args=(connection,index),daemon=True);self.workers.append(worker);worker.start()
    def handle(self,connection,index):
        result={"accepted":False}
        try:
            connection.settimeout(10)
            with self.context.wrap_socket(connection,server_side=True) as stream:
                result["version"]=stream.version()
                data=bytearray()
                while len(data)<4:
                    part=stream.recv(4-len(data))
                    if not part:raise RuntimeError("client closed before actual request")
                    data.extend(part)
                if bytes(data)!=b"ping":raise RuntimeError("wrong actual client request")
                stream.sendall(b"pong");result["accepted"]=True
        except ssl.SSLError as error:
            result.update({"error":str(error),"reason":getattr(error,"reason",None),"peer_alert":alert_number(getattr(error,"reason",None))})
        except BaseException as error:result["error"]=f"{type(error).__name__}: {error}"
        finally:
            connection.close()
            with self.condition:self.results[index]=result;self.condition.notify_all()
    def result(self,index):
        with self.condition:
            self.condition.wait_for(lambda:index in self.results,timeout=12)
            if index not in self.results:raise RuntimeError("external peer did not finish this actual connection")
            return self.results[index]
    def close(self):
        self.stop.set();self.listener.close();self.thread.join(timeout=2)
        for worker in self.workers:worker.join(timeout=12)
        if self.thread.is_alive() or any(worker.is_alive() for worker in self.workers):raise RuntimeError("owned TLS peer thread failed cleanup")

def save(output,receipt):
    temporary=output/".execution.tmp";temporary.write_text(json.dumps(receipt,indent=2)+"\n");temporary.replace(output/"execution.json")

def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument("--test-binary",type=Path,required=True);parser.add_argument("--go-client",type=Path,required=True);parser.add_argument("--output",type=Path,required=True);args=parser.parse_args()
    if not __debug__:parser.error("assertions must remain enabled")
    if not getattr(ssl,"HAS_TLSv1_3",False):
        parser.error("the external peer requires real TLS1.3 support; select a Python linked to supported OpenSSL (macOS system LibreSSL is insufficient): "+ssl.OPENSSL_VERSION)
    args.test_binary=args.test_binary.resolve();args.go_client=args.go_client.resolve();args.output=args.output.resolve();args.output.mkdir(parents=True,exist_ok=False)
    receipt={"status":"RUNNING","scope":"actual fixed-Go/Rust verified client TLS1.2/1.3 verdicts and received external peer alerts; default strict WebPKI control; platform trust diagnostic wording remains separate","runner":identity(__file__),"test_binary":identity(args.test_binary),"go_client":identity(args.go_client),"go_source":identity(HERE/"go-client.go"),"python_ssl":ssl.OPENSSL_VERSION,"cases":[]}
    receipt["inputs"]=[identity(PROFILE/name) for name in ["ca-leaf.pem","ca-leaf-client-only.pem","ca-leaf-key.pem"]]+[identity(FIXTURES/"root.pem")]
    save(args.output,receipt);signal.signal(signal.SIGTERM,lambda *_:(_ for _ in ()).throw(KeyboardInterrupt()))
    try:
        with tempfile.TemporaryDirectory(prefix="livekit-c07-peer-") as temporary:
            temporary=Path(temporary)
            der=ssl.PEM_cert_to_DER_cert((PROFILE/"ca-leaf.pem").read_text());der=bytearray(der);der[-1]^=1
            corrupt=temporary/"corrupt-signature.pem";corrupt.write_text(ssl.DER_cert_to_PEM_cert(bytes(der)))
            (args.output/"corrupt-signature.pem").write_bytes(corrupt.read_bytes())
            cases=[
                ("trusted-ca-leaf",PROFILE/"ca-leaf.pem",PROFILE/"ca-leaf.pem","localhost",1830297600,True),
                ("wrong-name",PROFILE/"ca-leaf.pem",PROFILE/"ca-leaf.pem","wrong.example",1830297600,False),
                ("untrusted-root",PROFILE/"ca-leaf.pem",FIXTURES/"root.pem","localhost",1830297600,False),
                ("expired",PROFILE/"ca-leaf.pem",PROFILE/"ca-leaf.pem","localhost",4102444800,False),
                ("wrong-purpose",PROFILE/"ca-leaf-client-only.pem",PROFILE/"ca-leaf-client-only.pem","localhost",1830297600,False),
                ("corrupt-signature",corrupt,PROFILE/"ca-leaf.pem","localhost",1830297600,False),
            ]
            for version in ["1.2","1.3"]:
                for name,certificate,ca,server_name,seconds,expected in cases:
                    row={"id":name,"version":version,"expected_accept":expected,"certificate_sha256":sha(certificate.read_bytes()),"ca_sha256":sha(ca.read_bytes()),"sides":[]};receipt["cases"].append(row);save(args.output,receipt)
                    peer=Peer(certificate,PROFILE/"ca-leaf-key.pem",version)
                    try:
                        index=peer.accepted
                        code,stdout,stderr=run([args.go_client,"-address",peer.address,"-ca",ca,"-name",server_name,"-version",version,"-time",str(seconds)])
                        (args.output/f"{name}-{version}-go.stdout").write_bytes(stdout);(args.output/f"{name}-{version}-go.stderr").write_bytes(stderr)
                        observed=json.loads(stdout);assert observed["runtime"]=="go1.26.3",observed["runtime"]
                        go_peer=peer.result(index);row["sides"].append({"side":"go","exit_code":code,"actual":observed,"peer":go_peer});assert code==0 and observed["accepted"]==expected
                        index=peer.accepted
                        env={**os.environ,"C07_ADDRESS":peer.address,"C07_CA":str(ca),"C07_NAME":server_name,"C07_TIME":str(seconds),"C07_PROFILE_MODE":"go","C07_EXPECT_ACCEPT":str(expected).lower()}
                        code,stdout,stderr=run([args.test_binary,"--ignored","--exact","external_go_profile_client_consumer","--nocapture","--test-threads=1"],env)
                        (args.output/f"{name}-{version}-rust.stdout").write_bytes(stdout);(args.output/f"{name}-{version}-rust.stderr").write_bytes(stderr)
                        match=re.search(rb"C07_RESULT:(accepted|rejected):([0-9a-f]*)",stdout);assert match,"missing actual Rust consumer result"
                        accepted=match[1]==b"accepted";error=bytes.fromhex(match[2].decode()).decode();rust_peer=peer.result(index)
                        row["sides"].append({"side":"rust-go-profile","exit_code":code,"actual":{"accepted":accepted,"error":error},"peer":rust_peer})
                        assert code==0 and b"running 1 test" in stdout and b"1 passed" in stdout and accepted==expected
                        assert go_peer["accepted"]==rust_peer["accepted"]==expected
                        if not expected:assert go_peer.get("peer_alert")==rust_peer.get("peer_alert")==42,(go_peer,rust_peer)
                        if name=="trusted-ca-leaf":
                            index=peer.accepted;env.update({"C07_PROFILE_MODE":"default","C07_EXPECT_ACCEPT":"false"})
                            code,stdout,stderr=run([args.test_binary,"--ignored","--exact","external_go_profile_client_consumer","--nocapture","--test-threads=1"],env)
                            (args.output/f"default-strict-{version}.stdout").write_bytes(stdout);(args.output/f"default-strict-{version}.stderr").write_bytes(stderr)
                            default_peer=peer.result(index);assert code==0 and b"C07_RESULT:rejected:" in stdout and b"1 passed" in stdout
                            default_match=re.search(rb"C07_RESULT:rejected:([0-9a-f]*)",stdout);assert default_match
                            default_error=bytes.fromhex(default_match[1].decode()).decode()
                            row["default_control"]={"exit_code":code,"peer":default_peer,"original_error":default_error}
                            assert not default_peer["accepted"] and "CaUsedAsEndEntity" in default_error
                            assert default_peer.get("peer_alert")==46,"default Rustls CertificateUnknown policy must remain unchanged"
                        row["status"]="PASS"
                    finally:peer.close();row["owned_peer_closed"]=True;save(args.output,receipt)
        receipt["private_inputs_removed"]=not temporary.exists()
        receipt["binary_identity_unchanged"]=identity(args.test_binary)["sha256"]==receipt["test_binary"]["sha256"] and identity(args.go_client)["sha256"]==receipt["go_client"]["sha256"]
        receipt["status"]="PASS" if len(receipt["cases"])==12 and all(row.get("status")=="PASS" and row.get("owned_peer_closed") for row in receipt["cases"]) and receipt["binary_identity_unchanged"] and receipt["private_inputs_removed"] else "FAIL"
    except BaseException as error:
        receipt["status"]="FAIL";receipt["error"]=f"{type(error).__name__}: {error}"
        if "temporary" in locals():receipt["private_inputs_removed"]=not Path(temporary).exists()
    finally:save(args.output,receipt)
    print(json.dumps({"status":receipt["status"],"receipt":str(args.output/"execution.json")}));return 0 if receipt["status"]=="PASS" else 1
if __name__=="__main__":raise SystemExit(main())
