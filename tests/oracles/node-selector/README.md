# Fixed Go selector collaborator oracle

Inputs copied byte-for-byte from frozen F45 oracle/selector, originally prepared against LiveKit v1.13.10. No execution claimed. The Rust source module selector_oracle_tests.rs consumes actual process output; it does not generate its own expected result.

Coordinator: build main.go with this go.mod/go.sum and fixed reference source replacements (never a floating selector reimplementation). Feed vectors.json on stdin and save the actual JSON array. Then run the ignored `actual_fixed_go_selector_corpus` with NODE_SELECTOR_VECTORS and NODE_SELECTOR_GO_RESULTS set to the actual files. Preserve any clock_boundary_ambiguous run as insufficient; do not rewrite it to false. The single source nil-stats PANIC remains a separately reported scope gap, not an HTTP error-equivalence proof.

Ported Go PDQsort and BSD license are installed together at crates/lk-compat/src/room_control/selector_go_sort.rs and go-sort.LICENSE by the parent component cut. SOURCE.json binds their source provenance and the exact omitted test/oracle inputs.
