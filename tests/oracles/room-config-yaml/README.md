# Actual fixed-source RoomConfiguration ProtoYAML consumer

Test reference only. Stage from the repository root with:

    /usr/bin/python3 livekit-rs-server/tests/oracles/prepare-locked-server-oracle.py --repository "$PWD" --oracle "$PWD/livekit-rs-server/tests/oracles/room-config-yaml" --out /new/private/stage

In that new directory, execute GOTOOLCHAIN=go1.26.3 GOMAXPROCS=2 go run -mod=readonly . < cases.jsonl > actual.jsonl. It calls the original config.NewConfig consumer with pinned server/protocol module locks. No expected outcome is authored. Source inputs are available through SPEC-0018's exact ProtoYAML0.7.0/YAML3.0.5 archives and licenses.

Original12 and later canonical12 remain separate preserved inputs; cases.jsonl combines these with60 extension cases (84 total). The separately frozen14 tag/float/outer-alias cases are not silently appended to that corpus: run the same staged binary with that public file explicitly on stdin. Compare with the real production auto_egress_config_probe example. Canonical JSON field comparisons ignore object property order and do not prove floating-point sign bits; dedicated bit tests are required.

Current baseline84 has23 canonical-field matches and61 mismatches, including30 admission differences and one accepted-value difference. These remain actual failures. All proposed parser replacements require fresh actual Rust results against unchanged inputs. This oracle does not establish automatic recording, SDK behavior or CLI/API100%.
