# prost-derive 0.14.4 implicit float presence correction

Published archive/source/licenses retained except SOURCE-listed files. Negative zero must survive scalar implicit-presence wire encoding/length and descriptor serialization. Ordinary equality/NaN behavior otherwise remains. Map float encode/length uses an existing-helper NaN comparison default to encode actual +/-0 without changing absent-value merge default +0 or generic API bounds. This map behavior needs actual Google wire oracle, not authored success.

Isolated NOT_COMPILED/NOT_RUN. Full byte reconstruction is separate evidence. Root must configure patch/exclude, register source archives in references, and run all actual generated/dynamic/outbound/inbound gates.
