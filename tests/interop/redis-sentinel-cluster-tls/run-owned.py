#!/usr/bin/env python3
"""Owned real Sentinel and DNS-only TLS/mTLS Cluster acceptance.

Never invokes Cargo. Requires an already built lk-compat test executable. Creates
only private temporary Redis/certificate inputs, keeps source/binary/public cert
hashes and actual test exit codes, then terminates only processes started here.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import random
import shutil
import signal
import socket
import subprocess
import tempfile
import time

HERE = Path(__file__).resolve().parent
SERVER = HERE.parents[2]
sha = lambda value: hashlib.sha256(value).hexdigest()

def identity(path):
    path = Path(path).resolve()
    return {"path": str(path), "sha256": sha(path.read_bytes())}

def port():
    for _ in range(1024):
        value = random.SystemRandom().randrange(20000, 45000)
        try:
            with socket.socket() as first, socket.socket() as bus:
                first.bind(("127.0.0.1", value))
                bus.bind(("127.0.0.1", value + 10000))
            return value
        except OSError:
            continue
    raise RuntimeError("no private fixture port")

class CommandFailure(RuntimeError):
    def __init__(self, argv, result):
        self.argv, self.result = argv, result
        super().__init__(f"command failed ({result.returncode}): {argv!r}: {result.stderr.decode(errors='replace')[:2048]}")
    def startup_connect_pending(self):
        text = self.result.stderr.decode(errors="replace").lower()
        # redis-cli on Darwin can attempt TCP_NODELAY on the unsuccessful
        # startup-connect socket before the child listener has bound. Retry
        # only these connection establishment failures, never TLS verdicts.
        return "connection refused" in text or ("setsockopt(tcp_nodelay)" in text and "invalid argument" in text)

class Campaign:
    def __init__(self, args, root, receipt):
        self.args, self.root, self.receipt = args, root, receipt
        self.children, self.logs = [], []
    def command(self, argv, *, timeout=30, env=None, check=True):
        argv = [str(value) for value in argv]
        process = subprocess.Popen(argv, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            stdout, stderr = process.communicate(timeout=timeout)
        except BaseException:
            if process.poll() is None:
                process.terminate()
                try: process.communicate(timeout=2)
                except subprocess.TimeoutExpired:
                    process.kill(); process.communicate(timeout=5)
            raise
        result = subprocess.CompletedProcess(argv, process.returncode, stdout, stderr)
        if check and result.returncode:
            raise CommandFailure(argv, result)
        return result
    def cli(self, port, *command, tls=False, check=True):
        argv = [self.args.redis_cli, "-h", "127.0.0.1", "-p", str(port), "--raw"]
        if tls:
            argv += ["--tls", "--sni", "redis.fixture.test", "--cacert", self.root/"ca.crt", "--cert", self.root/"client.crt", "--key", self.root/"client.key"]
        return self.command(argv + list(command), timeout=5, check=check)
    def config(self, name, text):
        path = self.root / name
        path.write_text(text)
        self.receipt.setdefault("configs", []).append({"name":name, "sha256":sha(path.read_bytes())})
        # Retain replayable structure without stale/private absolute key paths.
        (self.args.output/name).write_text(text.replace(str(self.root), "${FIXTURE_ROOT}"))
        return path
    def start(self, name, config, sentinel=False):
        log = self.root/(name+".server.log")
        handle = log.open("wb")
        process = subprocess.Popen([str(self.args.redis_server), str(config)] + (["--sentinel"] if sentinel else []), stdout=handle, stderr=subprocess.STDOUT)
        self.children.append((name,process,handle,log))
        self.receipt.setdefault("processes", []).append({"name":name,"pid":process.pid})
    def wait(self, predicate, label, timeout=20):
        deadline = time.monotonic()+timeout
        while time.monotonic()<deadline:
            if any(process.poll() is not None for _,process,_,_ in self.children):
                raise RuntimeError(f"owned fixture exited before {label}")
            try:
                if predicate(): return
            except CommandFailure as error:
                if not error.startup_connect_pending(): raise
            except (OSError, subprocess.TimeoutExpired):
                pass
            time.sleep(0.1)
        raise RuntimeError("fixture not ready: "+label)
    def certificates(self):
        openssl=self.args.openssl
        # The service certificate intentionally contains no IP SAN. Numeric
        # seed, discovered node and redirect dials require the configured name.
        self.command([openssl,"req","-x509","-newkey","rsa:2048","-nodes","-keyout",self.root/"ca.key","-out",self.root/"ca.crt","-days","2","-subj","/CN=Owned Redis CA"])
        self.command([openssl,"req","-x509","-newkey","rsa:2048","-nodes","-keyout",self.root/"wrong-ca.key","-out",self.root/"wrong-ca.crt","-days","2","-subj","/CN=Untrusted Redis CA"])
        for name, subject, extensions in [
            ("server","redis.fixture.test","subjectAltName=DNS:redis.fixture.test\nextendedKeyUsage=serverAuth,clientAuth\n"),
            ("client","owned-redis-client","extendedKeyUsage=clientAuth\n"),
        ]:
            self.command([openssl,"req","-new","-newkey","rsa:2048","-nodes","-keyout",self.root/(name+".key"),"-out",self.root/(name+".csr"),"-subj","/CN="+subject])
            ext=self.root/(name+".ext");ext.write_text(extensions)
            self.command([openssl,"x509","-req","-in",self.root/(name+".csr"),"-CA",self.root/"ca.crt","-CAkey",self.root/"ca.key","-CAcreateserial","-out",self.root/(name+".crt"),"-days","2","-extfile",ext])
        self.receipt["certificates"]=[]
        for name in ["ca.crt","wrong-ca.crt","server.crt","client.crt"]:
            data=(self.root/name).read_bytes();(self.args.output/name).write_bytes(data)
            self.receipt["certificates"].append({"name":name,"sha256":sha(data)})
        for key in self.root.glob("*.key"):key.chmod(0o600)
    def run_test(self, name, env):
        self.receipt["active_test"]=name
        save(self.args.output, self.receipt)
        started=time.monotonic()
        result=self.command([self.args.test_binary,"--ignored","--exact",name,"--nocapture","--test-threads=1"],timeout=90,env={**os.environ,**env},check=False)
        log=self.args.output/(name.replace("::","_")+".log")
        log.write_bytes(result.stdout+result.stderr)
        text=result.stdout.decode(errors="replace")
        ran_one="running 1 test" in text
        passed_one="1 passed" in text and "0 failed" in text
        row={"test":name,"exit_code":result.returncode,"actual_one_test":ran_one,"seconds":round(time.monotonic()-started,3),"log_sha256":sha(log.read_bytes()),"status":"PASS" if result.returncode==0 and ran_one and passed_one else "FAIL"}
        self.receipt.setdefault("tests",[]).append(row)
        self.receipt.pop("active_test",None)
        save(self.args.output,self.receipt)
    def sentinel(self):
        master,replica,sentinel=port(),port(),port()
        while len({master,replica,sentinel})!=3:master,replica,sentinel=port(),port(),port()
        for name,number,extra in [("sentinel-master",master,""),("sentinel-replica",replica,f"replicaof 127.0.0.1 {master}\n")]:
            directory=self.root/name;directory.mkdir()
            config=self.config(name+".conf",f"bind 127.0.0.1\nport {number}\nsave \"\"\nappendonly no\ndir {directory}\n"+extra)
            self.start(name,config)
        self.wait(lambda:b"PONG" in self.cli(master,"PING").stdout,"master")
        self.wait(lambda:b"master_link_status:up" in self.cli(replica,"INFO","replication").stdout,"replication")
        config=self.config("sentinel.conf",f"bind 127.0.0.1\nport {sentinel}\ndir {self.root}\nsentinel monitor mymaster 127.0.0.1 {master} 1\nsentinel down-after-milliseconds mymaster 1000\nsentinel failover-timeout mymaster 5000\nsentinel parallel-syncs mymaster 1\n")
        self.start("sentinel",config,True)
        self.wait(lambda:b"slave" in self.cli(sentinel,"SENTINEL","REPLICAS","mymaster").stdout,"Sentinel knows replica")
        self.run_test("redis_endpoint::tests::real_sentinel_discovery_command_and_failover_recovery",{
            "LIVEKIT_SENTINEL_TEST_ADDR":f"127.0.0.1:{sentinel}",
            "LIVEKIT_SENTINEL_TEST_MASTER":"mymaster",
            "LIVEKIT_SENTINEL_TEST_REPLICA_ADDR":f"127.0.0.1:{replica}",
        })
    def cluster(self):
        self.certificates()
        ports=[]
        while len(ports)<3:
            value=port()
            if all(value != other and value+10000 != other and value != other+10000 for other in ports):ports.append(value)
        for index,number in enumerate(ports):
            name=f"tls-cluster-{index}";directory=self.root/name;directory.mkdir()
            config=self.config(name+".conf",f"bind 127.0.0.1\nport 0\ntls-port {number}\ntls-cert-file {self.root/'server.crt'}\ntls-key-file {self.root/'server.key'}\ntls-ca-cert-file {self.root/'ca.crt'}\ntls-auth-clients yes\ntls-cluster yes\ntls-replication yes\ncluster-enabled yes\ncluster-config-file nodes.conf\ncluster-node-timeout 1000\ncluster-announce-ip 127.0.0.1\ncluster-announce-tls-port {number}\nsave \"\"\nappendonly no\ndir {directory}\n")
            self.start(name,config)
        for number in ports:self.wait(lambda number=number:b"PONG" in self.cli(number,"PING",tls=True).stdout,"TLS listener")
        argv=[self.args.redis_cli,"--tls","--sni","redis.fixture.test","--cacert",self.root/"ca.crt","--cert",self.root/"client.crt","--key",self.root/"client.key","--cluster","create"]+[f"127.0.0.1:{number}" for number in ports]+["--cluster-replicas","0","--cluster-yes"]
        created=self.command(argv)
        (self.args.output/"cluster-create.log").write_bytes(created.stdout+created.stderr)
        for number in ports:self.wait(lambda number=number:b"cluster_state:ok" in self.cli(number,"CLUSTER","INFO",tls=True).stdout,"all TLS Cluster slots ready")
        self.run_test("redis_endpoint::cluster_tls_tests::real_cluster_tls_identity_slots_ask_moved_and_rejections",{
            "LIVEKIT_TLS_CLUSTER_NODES":",".join(f"rediss://127.0.0.1:{number}/" for number in ports),
            "LIVEKIT_TLS_CLUSTER_NAME":"redis.fixture.test", "LIVEKIT_TLS_CLUSTER_CA":str(self.root/"ca.crt"),
            "LIVEKIT_TLS_CLUSTER_CLIENT_CERT":str(self.root/"client.crt"),"LIVEKIT_TLS_CLUSTER_CLIENT_KEY":str(self.root/"client.key"),"LIVEKIT_TLS_CLUSTER_WRONG_CA":str(self.root/"wrong-ca.crt"),
        })
    def cleanup(self):
        failures=[]
        for name,process,_,_ in reversed(self.children):
            if process.poll() is None:process.terminate()
        for name,process,handle,log in reversed(self.children):
            forced=False
            try:process.wait(timeout=10)
            except subprocess.TimeoutExpired:process.kill();process.wait(timeout=5);forced=True
            handle.close()
            if log.exists():shutil.copyfile(log,self.args.output/log.name)
            self.receipt.setdefault("cleanup",[]).append({"name":name,"pid":process.pid,"exit_code":process.returncode,"forced_kill":forced})
            if forced or process.returncode != 0:failures.append(name)
        return failures

def save(output,receipt):
    stage=output/".execution.tmp";stage.write_text(json.dumps(receipt,indent=2)+"\n");stage.replace(output/"execution.json")

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--test-binary",type=Path,required=True)
    parser.add_argument("--output",type=Path,required=True)
    parser.add_argument("--redis-server",type=Path,default=shutil.which("redis-server"))
    parser.add_argument("--redis-cli",type=Path,default=shutil.which("redis-cli"))
    parser.add_argument("--openssl",type=Path,default=shutil.which("openssl"))
    args=parser.parse_args()
    if not __debug__:parser.error("assertions must remain enabled")
    for name in ["test_binary","redis_server","redis_cli","openssl"]:
        value=getattr(args,name)
        if value is None or not value.is_file():parser.error("missing executable: "+name)
        setattr(args,name,value.resolve())
    args.output=args.output.resolve();args.output.mkdir(parents=True,exist_ok=False)
    receipt={"status":"RUNNING","scope":"owned actual Sentinel event read/subscription migration and verified TLS Cluster slots/ASK/MOVED/mTLS; Rust runtime campaign, not fixed-Go/global parity certification","runner":identity(__file__),"binaries":{name:identity(getattr(args,name)) for name in ["test_binary","redis_server","redis_cli","openssl"]},"cargo_lock":identity(SERVER/"Cargo.lock")}
    receipt["source_observed_at_run"] = [identity(SERVER/path) for path in [
        "crates/lk-compat/src/redis_endpoint.rs", "crates/lk-compat/src/redis_endpoint/sentinel_watch.rs",
        "crates/lk-compat/src/redis_endpoint/cluster_tls_tests.rs", "vendor/redis/SOURCE.json",
    ]]
    save(args.output,receipt)
    signal.signal(signal.SIGTERM,lambda *_:(_ for _ in ()).throw(KeyboardInterrupt()))
    campaign=None
    try:
        with tempfile.TemporaryDirectory(prefix="livekit-c12-owned-") as temporary:
            root=Path(temporary);root.chmod(0o700);campaign=Campaign(args,root,receipt)
            try:campaign.sentinel();campaign.cluster()
            finally:
                forced=campaign.cleanup()
                receipt["forced_cleanup"]=forced
        receipt["private_inputs_removed"]=not root.exists()
        stable=all(identity(getattr(args,name))["sha256"]==row["sha256"] for name,row in receipt["binaries"].items())
        receipt["binary_identity_unchanged"]=stable
        receipt["status"]="PASS" if len(receipt.get("tests",[]))==2 and all(row["status"]=="PASS" for row in receipt["tests"]) and not receipt["forced_cleanup"] and stable and receipt["private_inputs_removed"] else "FAIL"
    except BaseException as error:
        receipt["status"]="FAIL";receipt["error"]=f"{type(error).__name__}: {error}"
        if isinstance(error, CommandFailure):
            receipt["failed_command"]={"argv":error.argv,"exit_code":error.result.returncode,"stdout":error.result.stdout.decode(errors="replace"),"stderr":error.result.stderr.decode(errors="replace")}
        if campaign is not None:receipt["private_inputs_removed"]=not campaign.root.exists()
    finally:save(args.output,receipt)
    print(json.dumps({"status":receipt["status"],"receipt":str(args.output/"execution.json")}))
    return 0 if receipt["status"]=="PASS" else 1

if __name__=="__main__":raise SystemExit(main())
