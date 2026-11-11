#!/usr/bin/env python3
"""Adversarial sibling campaign: valid seed, trusted wrong-name Cluster peer.

Uses the same owned fixture/cleanup/receipt policy as run-owned.py, without
changing its input. Node 1 is intentionally signed by the right CA under the
wrong DNS name. Fixture administration verifies that actual alternate name;
the application must reject discovery, ASK and MOVED to that peer.
"""
import importlib.util
from pathlib import Path
import shutil
import sys
sys.dont_write_bytecode = True
HERE=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location("owned_c12",HERE/"run-owned.py")
base=importlib.util.module_from_spec(spec);spec.loader.exec_module(base)

class BadPeerCampaign(base.Campaign):
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self.receipt["adversarial_runner"]=base.identity(__file__)
        self.receipt["scope"]="Owned actual Sentinel event migration plus trusted-CA wrong-identity discovered/ASK/MOVED TLS Cluster rejection; no global parity certification"
    def certificates(self):
        super().certificates()
        self.command([self.args.openssl,"req","-new","-newkey","rsa:2048","-nodes","-keyout",self.root/"bad-peer.key","-out",self.root/"bad-peer.csr","-subj","/CN=wrong-peer.fixture.test"])
        ext=self.root/"bad-peer.ext";ext.write_text("subjectAltName=DNS:wrong-peer.fixture.test\nextendedKeyUsage=serverAuth,clientAuth\n")
        self.command([self.args.openssl,"x509","-req","-in",self.root/"bad-peer.csr","-CA",self.root/"ca.crt","-CAkey",self.root/"ca.key","-CAcreateserial","-out",self.root/"bad-peer.crt","-days","2","-extfile",ext])
        self.root.joinpath("bad-peer.key").chmod(0o600)
        shutil.copyfile(self.root/"bad-peer.crt",self.args.output/"bad-peer.crt")
        self.receipt["certificates"].append({"name":"bad-peer.crt","sha256":base.sha((self.root/"bad-peer.crt").read_bytes())})
    def config(self,name,text):
        if name=="tls-cluster-1.conf":
            text=text.replace(str(self.root/"server.crt"),str(self.root/"bad-peer.crt")).replace(str(self.root/"server.key"),str(self.root/"bad-peer.key"))
        return super().config(name,text)
    def cli(self,port,*command,tls=False,check=True):
        # Redis fixture orchestration must verify the node's actual identity.
        # No insecure path is used to create or administer the bad-peer cluster.
        if tls:
            import subprocess
            config=self.root/"tls-cluster-1.conf"
            bad=config.exists() and f"tls-port {port}\n" in config.read_text()
            if bad:
                return self.command([self.args.redis_cli,"-h","127.0.0.1","-p",str(port),"--raw","--tls","--sni","wrong-peer.fixture.test","--cacert",self.root/"ca.crt","--cert",self.root/"client.crt","--key",self.root/"client.key",*command],timeout=5,check=check)
        return super().cli(port,*command,tls=tls,check=check)
    def run_test(self,name,env):
        if name.endswith("real_cluster_tls_identity_slots_ask_moved_and_rejections"):
            name="redis_endpoint::cluster_tls_tests::real_cluster_tls_rejects_bad_identity_on_discovery_ask_and_moved"
        return super().run_test(name,env)

# Redis CLI --cluster create uses one SNI for all nodes. The wrong-name fixture
# cannot use that helper without disabling verification. Configure its topology
# through individually verified connections instead.
def bad_cluster(self):
    self.certificates()
    ports=[]
    while len(ports)<3:
        value=base.port()
        if all(value!=other and value+10000!=other and value!=other+10000 for other in ports):ports.append(value)
    for index,number in enumerate(ports):
        name=f"tls-cluster-{index}";directory=self.root/name;directory.mkdir()
        config=self.config(name+".conf",f"bind 127.0.0.1\nport 0\ntls-port {number}\ntls-cert-file {self.root/'server.crt'}\ntls-key-file {self.root/'server.key'}\ntls-ca-cert-file {self.root/'ca.crt'}\ntls-auth-clients yes\ntls-cluster yes\ntls-replication yes\ncluster-enabled yes\ncluster-config-file nodes.conf\ncluster-node-timeout 1000\ncluster-announce-ip 127.0.0.1\ncluster-announce-tls-port {number}\nsave \"\"\nappendonly no\ndir {directory}\n")
        self.start(name,config)
    for number in ports:self.wait(lambda number=number:b"PONG" in self.cli(number,"PING",tls=True).stdout,"individually verified TLS listener")
    for number in ports[1:]:self.cli(ports[0],"CLUSTER","MEET","127.0.0.1",str(number),tls=True)
    self.wait(lambda: all(b"cluster_known_nodes:3" in self.cli(number,"CLUSTER","INFO",tls=True).stdout for number in ports),"TLS cluster peer discovery")
    for number,first,last in zip(ports,[0,5461,10923],[5460,10922,16383]):
        self.cli(number,"CLUSTER","ADDSLOTSRANGE",str(first),str(last),tls=True)
    for number in ports:self.wait(lambda number=number:b"cluster_state:ok" in self.cli(number,"CLUSTER","INFO",tls=True).stdout,"wrong-name peer TLS Cluster slots ready")
    self.run_test("redis_endpoint::cluster_tls_tests::real_cluster_tls_identity_slots_ask_moved_and_rejections",{
        "LIVEKIT_TLS_CLUSTER_NODES":",".join(f"rediss://127.0.0.1:{number}/" for number in ports),"LIVEKIT_TLS_CLUSTER_NAME":"redis.fixture.test","LIVEKIT_TLS_CLUSTER_CA":str(self.root/"ca.crt"),"LIVEKIT_TLS_CLUSTER_CLIENT_CERT":str(self.root/"client.crt"),"LIVEKIT_TLS_CLUSTER_CLIENT_KEY":str(self.root/"client.key"),"LIVEKIT_TLS_CLUSTER_WRONG_CA":str(self.root/"wrong-ca.crt"),
    })
BadPeerCampaign.cluster=bad_cluster
base.Campaign=BadPeerCampaign
if __name__=="__main__":raise SystemExit(base.main())
