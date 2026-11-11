#!/usr/bin/env python3
"""Step matrix for the Twirp (W2-G1) and HTTP route (W5) differential."""
import json, sys

SERVICES = {
    "RoomService": ["CreateRoom", "ListRooms", "DeleteRoom", "ListParticipants", "GetParticipant",
                    "RemoveParticipant", "MutePublishedTrack", "UpdateParticipant", "UpdateSubscriptions",
                    "SendData", "UpdateRoomMetadata", "ForwardParticipant", "MoveParticipant", "PerformRpc"],
    "Egress": ["StartEgress", "UpdateLayout", "UpdateStream", "ListEgress", "StopEgress",
               "StartRoomCompositeEgress", "StartWebEgress", "StartParticipantEgress",
               "StartTrackCompositeEgress", "StartTrackEgress"],
    "Ingress": ["CreateIngress", "UpdateIngress", "ListIngress", "DeleteIngress"],
    "SIP": ["ListSIPTrunk", "CreateSIPInboundTrunk", "CreateSIPOutboundTrunk", "UpdateSIPInboundTrunk",
            "UpdateSIPOutboundTrunk", "GetSIPInboundTrunk", "GetSIPOutboundTrunk", "ListSIPInboundTrunk",
            "ListSIPOutboundTrunk", "DeleteSIPTrunk", "CreateSIPDispatchRule", "UpdateSIPDispatchRule",
            "ListSIPDispatchRule", "DeleteSIPDispatchRule", "CreateSIPParticipant", "TransferSIPParticipant"],
    "AgentDispatchService": ["CreateDispatch", "DeleteDispatch", "ListDispatch"],
}
ROOM = "r1"
FULL = {"video": {"roomAdmin": True, "roomCreate": True, "roomList": True, "roomRecord": True,
                  "ingressAdmin": True, "room": ROOM}, "sip": {"admin": True, "call": True}}
WRONG = {"video": {"canPublish": True, "room": "other"}}
# a representative body per method (fields that reach real validation)
BODIES = {
    "CreateRoom": {"name": ROOM}, "DeleteRoom": {"room": ROOM}, "ListParticipants": {"room": ROOM},
    "GetParticipant": {"room": ROOM, "identity": "nobody"}, "RemoveParticipant": {"room": ROOM, "identity": "nobody"},
    "MutePublishedTrack": {"room": ROOM, "identity": "nobody", "track_sid": "TR_x", "muted": True},
    "UpdateParticipant": {"room": ROOM, "identity": "nobody", "metadata": "m"},
    "UpdateSubscriptions": {"room": ROOM, "identity": "nobody", "track_sids": ["TR_x"], "subscribe": True},
    "SendData": {"room": ROOM, "data": "aGk=", "kind": "RELIABLE"},
    "UpdateRoomMetadata": {"room": ROOM, "metadata": "meta"},
    "ForwardParticipant": {"room": ROOM, "identity": "nobody", "destination_room": "r2"},
    "MoveParticipant": {"room": ROOM, "identity": "nobody", "destination_room": "r2"},
    "PerformRpc": {"room": ROOM, "destination_identity": "nobody", "method": "m", "payload": "p"},
    "StartRoomCompositeEgress": {"room_name": ROOM, "file": {"filepath": "x.mp4"}},
    "StartEgress": {"room_name": ROOM}, "StopEgress": {"egress_id": "EG_x"},
    "UpdateLayout": {"egress_id": "EG_x", "layout": "grid"}, "UpdateStream": {"egress_id": "EG_x"},
    "ListEgress": {"room_name": ROOM}, "StartWebEgress": {"url": "https://example.com"},
    "StartParticipantEgress": {"room_name": ROOM, "identity": "nobody"},
    "StartTrackCompositeEgress": {"room_name": ROOM}, "StartTrackEgress": {"room_name": ROOM, "track_id": "TR_x"},
    "CreateIngress": {"input_type": "RTMP_INPUT", "room_name": ROOM, "participant_identity": "ing"},
    "UpdateIngress": {"ingress_id": "IN_x"}, "ListIngress": {"room_name": ROOM}, "DeleteIngress": {"ingress_id": "IN_x"},
    "CreateSIPInboundTrunk": {"trunk": {"name": "t", "numbers": ["+1555"]}},
    "CreateSIPOutboundTrunk": {"trunk": {"name": "t", "address": "sip.example.com", "numbers": ["+1555"]}},
    "GetSIPInboundTrunk": {"sip_trunk_id": "ST_x"}, "GetSIPOutboundTrunk": {"sip_trunk_id": "ST_x"},
    "DeleteSIPTrunk": {"sip_trunk_id": "ST_x"}, "DeleteSIPDispatchRule": {"sip_dispatch_rule_id": "SDR_x"},
    "CreateSIPDispatchRule": {"dispatch_rule": {"rule": {"dispatch_rule_direct": {"room_name": ROOM}}}},
    "CreateSIPParticipant": {"sip_trunk_id": "ST_x", "sip_call_to": "+1555", "room_name": ROOM},
    "TransferSIPParticipant": {"room_name": ROOM, "participant_identity": "nobody", "transfer_to": "tel:+1555"},
    "CreateDispatch": {"room": ROOM, "agent_name": "a"}, "DeleteDispatch": {"room": ROOM, "dispatch_id": "AD_x"},
    "ListDispatch": {"room": ROOM},
}


def steps():
    out = []
    add = lambda **k: out.append(k)
    for svc, methods in SERVICES.items():
        for m in methods:
            base = f"{svc}.{m}"
            b = BODIES.get(m, {})
            for auth in ("none", "garbage", "bad_secret", "expired"):
                add(id=f"{base}.auth_{auth}", service=svc, method=m, codec="json", auth=auth, body=b)
            add(id=f"{base}.wrong_grant", service=svc, method=m, codec="json", auth=WRONG, body=b)
            add(id=f"{base}.malformed_json", service=svc, method=m, codec="json", auth=FULL, raw="{")
            add(id=f"{base}.bad_proto", service=svc, method=m, codec="proto", auth=FULL, raw_b64="//8=")
            add(id=f"{base}.unknown_field", service=svc, method=m, codec="json", auth=FULL, body={"nope": 1})
            add(id=f"{base}.empty.json", service=svc, method=m, codec="json", auth=FULL, body={})
            add(id=f"{base}.empty.proto", service=svc, method=m, codec="proto", auth=FULL, body={})
    # stateful room scenario (json and proto), run in order
    for codec in ("json", "proto"):
        r = f"room-{codec}"
        g = {"video": {"roomAdmin": True, "roomCreate": True, "roomList": True, "room": r}}
        seq = [
            ("CreateRoom", {"name": r, "empty_timeout": 600, "max_participants": 5, "metadata": "md"}),
            ("CreateRoom", {"name": r}),
            ("ListRooms", {"names": [r]}),
            ("ListParticipants", {"room": r}),
            ("GetParticipant", {"room": r, "identity": "nobody"}),
            ("UpdateRoomMetadata", {"room": r, "metadata": "md2"}),
            ("ListRooms", {"names": [r]}),
            ("SendData", {"room": r, "data": "aGk=", "kind": "RELIABLE", "topic": "t"}),
            ("SendData", {"room": r, "data": "aGk=", "kind": "LOSSY", "destination_identities": ["nobody"]}),
            ("CreateDispatch", {"room": r, "agent_name": "agent-a", "metadata": "x"}),
            ("ListDispatch", {"room": r}),
            ("RemoveParticipant", {"room": r, "identity": "nobody"}),
            ("DeleteRoom", {"room": r}),
            ("DeleteRoom", {"room": r}),
            ("ListRooms", {"names": [r]}),
        ]
        svc_of = {m: s for s, ms in SERVICES.items() for m in ms}
        for i, (m, b) in enumerate(seq):
            add(id=f"scenario.{codec}.{i:02d}.{m}", service=svc_of[m], method=m, codec=codec, auth=g, body=b)
        add(id=f"scenario.{codec}.room_scope", service="RoomService", method="ListParticipants", codec=codec,
            auth={"video": {"roomAdmin": True, "room": "elsewhere"}}, body={"room": r})
    # authorization header forms (W5 JWT handling)
    for form in ("lower_bearer", "no_bearer", "unknown_key"):
        for m in ("ListRooms", "CreateRoom"):
            add(id=f"auth.{form}.{m}", service="RoomService", method=m, codec="json", auth=form,
                body=BODIES.get(m, {}))
    # validation edges
    for name in ["", "a" * 300, "bad/name", "ok-name"]:
        add(id=f"validate.CreateRoom.name.{len(name)}.{name[:6]}", service="RoomService", method="CreateRoom",
            codec="json", auth={"video": {"roomCreate": True}}, body={"name": name})
    add(id="validate.CreateRoom.metadata_large", service="RoomService", method="CreateRoom", codec="json",
        auth={"video": {"roomCreate": True}}, body={"name": "big", "metadata": "x" * 70000})
    add(id="validate.ListRooms.no_list_grant", service="RoomService", method="ListRooms", codec="json",
        auth={"video": {"roomCreate": True}}, body={})
    # twirp routing errors
    for i, (verb, path, ct) in enumerate([
            ("POST", "/twirp/livekit.RoomService/Nope", "application/json"),
            ("POST", "/twirp/livekit.Nope/ListRooms", "application/json"),
            ("GET", "/twirp/livekit.RoomService/ListRooms", None),
            ("POST", "/twirp/livekit.RoomService/ListRooms", "text/plain"),
            ("POST", "/twirp/livekit.RoomService/ListRooms", None),
            ("POST", "/twirp/livekit.RoomService/ListRooms/extra", "application/json")]):
        h = {"Content-Type": ct} if ct else {}
        add(id=f"route.twirp.{i}", kind="http", verb=verb, path=path, auth=FULL, raw="{}", headers=h)
    # W5 HTTP routes
    for i, (verb, path, hdr) in enumerate([
            ("GET", "/", {}), ("HEAD", "/", {}), ("POST", "/", {}), ("GET", "/nope", {}),
            ("GET", "/rtc", {}), ("GET", "/rtc/validate", {}), ("GET", "/rtc/v1", {}), ("GET", "/rtc/v1/validate", {}),
            ("GET", "/rtc/validate?access_token=garbage", {}),
            ("OPTIONS", "/twirp/livekit.RoomService/ListRooms",
             {"Origin": "https://a.example", "Access-Control-Request-Method": "POST",
              "Access-Control-Request-Headers": "authorization,content-type"}),
            ("GET", "/", {"Origin": "https://a.example"}),
            ("POST", "/whip/v1", {}), ("GET", "/whip/v1", {}), ("OPTIONS", "/whip/v1", {"Origin": "https://a.example"}),
            ("GET", "/agent", {}), ("GET", "/debug/pprof/", {}), ("GET", "/metrics", {}),
            ("GET", "//twirp/livekit.RoomService/ListRooms", {})]):
        add(id=f"route.http.{i}.{verb}{path.replace('/', '_')[:30]}", kind="http", verb=verb, path=path,
            auth="none", headers=hdr)
    return out


if __name__ == "__main__":
    s = steps()
    ids = [x["id"] for x in s]
    assert len(ids) == len(set(ids)), "duplicate ids"
    json.dump({"steps": s}, open(sys.argv[1], "w"), indent=1)
    print(len(s), "steps")
