# Actual ambient ENV CLI differential

Use the same --official, --rust, --generated-inventory and --output arguments as ../cli-process/compare-cli.py. The632 cases preserve the full158 base prefix and repeat that corpus under three ambient/registered ENV overlays. Comparator normalization is unchanged. On F53 binary4fb912ee actual497PASS/135FAIL. That historical RED remains reproducible. The F60/F61/F72 delivery at334b079 subsequently passed all632 declared cases; see SPEC0029 and f61-f72-functional-delivery.json for its exact binary and source identities. Later candidates must rerun this fixture; the earlier result does not certify new inputs. This finite profile does not close arbitrary raw ENV names, HOME fallback or all CLI/config consumers.


`https-consumer.mjs` verifies the real Run invocation consuming HTTPS ENV after the coordinator starts it: nondefault HTTPS port, normal CA/name verification, exact test-leaf fingerprint, enabled plain HTTP and signed official-SDK ListRooms request. Required HTTPS_CONSUMER_URL/HTTP_URL/CA/CERT/OUTPUT variables identify owned test inputs; no certificate bypass. It does not claim WSS/media or arbitrary raw certificate paths.
