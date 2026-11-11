# Actual fixed Pion GCC stage reference

From the repository root, stage the source using `prepare.py --source references/go-modules/pion-interceptor-v0.1.49 --out /new/owned/stage`. The stager verifies all178 original files against SPEC-0018. In that directory run `PION_GCC_SOURCE_INPUT=/absolute/path/to/cases.jsonl PION_GCC_SOURCE_OUTPUT=/new/owned/stage/actual.jsonl GOTOOLCHAIN=go1.26.3 GOMAXPROCS=2 go test -count=1 ./pkg/gcc -run '^TestSourceOracle$'`.

Only wall-clock function references in four files are replaced with an explicit test clock. The driver directly executes the original unexported stage methods; it contains no reimplemented estimator or prefilled numeric expectations. Nine cases contain576 raw events, including reordering, loss and equal-time windows. Results include actual GOOS/GOARCH; out-of-range casts are target-specific. Retained original source and MIT licenses are under references.

Stage acceptance does not prove asynchronous delay-goroutine ordering, RTP/TWCC bridge, browser interoperability, Rust estimator behavior or complete feedback-mode compatibility. These require independent positive consumers.
