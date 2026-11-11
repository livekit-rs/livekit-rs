# Reconstruct fixed-source measurement data

Use `generate.py --root REPO --displaywidth REPO/references/displaywidth-v0.11.0 --output OWNED_DIRECTORY`, then the workspace-pinned `rustfmt` on the generated file before byte-comparing it with `crates/livecloud/src/go_width_data.rs`. Formatting is a separate mechanical step; it does not change trie values. Original source and helper snapshots are identified by SOURCE*.json. Keep MIT notices with redistributed binaries/source.

The original Go observer is run with `go run -mod=readonly main.go` for the 72 explicit contexts and `go run -mod=readonly scalar-main.go` for the raw two-plane scalar result. These are separate entry files and must not be compiled together. Actual scalar output is generated from original library methods; no expected byte array is stored here. Record output/binary identities and then run the Rust ignored comparator with NODE_RUNE_GO_PLANES, which compares every value directly.

Plain fixed-default measurement acceptance is distinct from host locale/tab/editor/terminal policy. The integration does not certify all those host adapters or full CLI compatibility.
