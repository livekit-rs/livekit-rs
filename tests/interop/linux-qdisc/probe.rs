//! rustc-only observer of the exact included production source. The runner
//! writes a sibling production.rs from the source whose input SHA it verified.
#[path = "production.rs"]
mod node_tc;
use std::{collections::BTreeSet, fs};
fn fds() -> BTreeSet<String> {
    fs::read_dir("/proc/self/fd")
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect()
}
fn main() {
    let initial = node_tc::sample().expect("real kernel netlink warmup");
    let before = fds();
    for _ in 0..256 {
        assert_eq!(
            node_tc::sample().expect("real repeated netlink dump"),
            initial,
            "traffic must be quiescent during comparison"
        );
        assert_eq!(
            fds(),
            before,
            "production NETLINK sample leaked or replaced an FD"
        );
    }
    println!(
        "{{\"packets\":{},\"drops\":{},\"samples\":257,\"fd_before\":{},\"fd_after\":{}}}",
        initial.0,
        initial.1,
        before.len(),
        fds().len()
    );
}
