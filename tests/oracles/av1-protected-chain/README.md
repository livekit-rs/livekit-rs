# Fixed original AV1 protected chain control

Source-only, not executed. prepare.py stages the unchanged locked Go source.
Run pkg/sfu/videolayerselector -run '^TestOriginalProtectedChainDropRecoveryAndUnknownReference$'
(no -docker flag). Require exactly8 SOURCE_AV1_CHAIN rows. The real original
selector/FrameChain/SelectorDecisionCache/DD marshaller execute all decisions.

The test-only before-fix overlay asserts that native frame12 cannot be forwarded
when its protected chain references explicitly NotPresent/dropped11, even with
empty FrameDiffs. Recovery13 has a real chain diff0. Actual Go must first prove
this contract, then the unchanged before-fix source must fail that gate.

Native consumes retained target-to-chain mapping, actual chain diffs and active
target transitions before preparing any payload. NotPresent frames also update
chain decisions although they never reach Writer. The source decision cache
retains the original256 entries/80 NACK window, explicit missing/drop/forwarded
states and late expected-reference resolution. Rejected/retired targets do not
create media timing samples; existing successful Writer gate remains unchanged.

The ignored av1::tests::original_go_protected_chain_rows_match_production_observation
reads GO_AV1_CHAIN_RESULTS, requires8 actual rows and compares production chain
observation. This is not a pixel or arbitrary full-DD proof. Output reference
remapping still conservatively requires known forwarded external references;
original Go accepts unknown references pending NACK/reorder. Those output number
and late-repair semantics are explicitly not certified here. Raw-u16 source
unwrap lacks an explicit upstream restart generation and remains a separate gap.
Run the full existing encrypted AV1/VP9/BWE/native suite and decoded SDK campaign
on a new same final source before accepting the combined candidate.
