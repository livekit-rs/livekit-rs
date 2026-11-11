# Actual CLI processes

Run compare-cli.py with --official, --rust, --generated-inventory and --output.
The 158 cases (original84 unchanged plus74 nested/source controls) invoke both real executables with the same controlled argv/ENV.
Only product build identity and order of the complete generated-flag cohort are
normalized. Complete flag row bytes, defaults, multiplicity, errors, whitespace,
exit status and all other ordering remain exact. Actual Go self-repeats prove
map-order variation before the permitted cohort normalization is used.

The inventory is independently extracted from the pinned official
config.GenerateCLIFlags. Historical F33/F47/F51 RED reports remain evidence.
This corpus covers its declared command/help/ENV edges; it does not establish
all payload consumers, arbitrary command paths, mixed product extension namespace or non-UTF-8 OS input parity. The finite fixed command graph is recorded in command-tree.json; nested dispatch is covered only by the declared cases.
