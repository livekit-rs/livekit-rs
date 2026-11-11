# Fixed-source RoomConfiguration agents YAML consumer

Stage with `../prepare-locked-server-oracle.py`. Run Go1.26.3 with GOMAXPROCS=2 against the fixed server module. The ten inputs are passed to actual config.NewConfig; slice nilness, count and null indices are observed from the actual generated message. No service or SDK changes. Acceptance and default-element behavior supplement, rather than replace, full configuration validation.
