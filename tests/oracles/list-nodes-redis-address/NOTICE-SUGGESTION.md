Suggested paragraph for the existing server NOTICE at coordinator adoption:

The Redis list-nodes address consumer in crates/livecloud/src/redis_address.rs
adapts Go1.26.3 net/ipsock.go, net/port.go, net/lookup.go, net/port_unix.go and
net/cgo_unix.go, Copyright The Go Authors, BSD-3-Clause. The original license
text is preserved in LICENSES/Go-BSD-3-Clause.txt. The original Go observer is
a development-only source consumer, not a production dependency. The existing
libc dependency is used only for the operating system service database boundary.
