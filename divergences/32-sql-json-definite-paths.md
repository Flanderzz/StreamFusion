# Flink SQL/JSON definite paths

Comet's `native/spark-expr/src/string_funcs/get_json_object.rs` separates scalar paths from
column inputs and streams through each document instead of building a DOM on its common
path. StreamFusion follows that structure: the path and policies remain scalar, a definite
path is parsed once per batch, and unescaped selected strings borrow their input span until
appended to the Arrow result. Every field is validated, including fields outside the selected
path. JSON_VALUE and JSON_EXISTS share this parser under the scalar-function registry.

For documents with many short members, a shared `simd-json` reader reuses its input scratch,
structural buffers and tape across rows. It validates the whole document before selecting a
path, and scans all members of each selected object so the last duplicate wins. This differs
from `datafusion-functions-json`'s jiter early lookup, which can stop before an invalid later
field or a duplicate ancestor. The tape's decoded strings are borrowed until the Arrow builder
copies the selected result; no DOM or per-row result String is created.

The SIMD path is deliberately narrower than function admission: it requires fewer than 50,000
input bytes, at most 1000 tape nodes, no Unicode escapes and no floating-point nodes. These
bounds preserve Jackson's string/name/depth constraints and avoid replacing BigDecimal or
surrogate semantics with the SIMD library's conversions. Selected numbers, rejected SIMD
documents and all other inputs use the existing native parser. The first 256 input bytes must
contain at least eight colons before SIMD is attempted; this inexpensive workload heuristic
keeps long padding strings on the streaming path. It does not change accepted input or results.

The semantic reference is Flink **release-2.2.1**, specifically `SqlJsonUtils`,
`JsonValueCallGen`, and its released Jackson 2.18.2/Jayway implementation. Spark's function
does not implement SQL/JSON strict/lax modes, independent EMPTY/ERROR policies, or Flink's
JSON-null behavior. Neither Comet's serde visitor nor StreamFusion's connector JSON decoder
can be substituted without changing results. Connector decoding also has different input
validation and malformed-string contracts, so this change does not alter that parser.

String token validation reuses serde_json's public streaming `IgnoredAny` interface. Its
string scanner validates escapes and control bytes in word-sized chunks without allocating
a decoded String or rejecting lone escaped UTF-16 surrogates. Only the selected string is
unescaped under Flink's rules. The existing serde dependency is therefore also needed by
the connector-free core; no new crate or version is added.

The scanner retains scalar spans and the last matching object member. This reproduces
Jackson's duplicate-member replacement even when a later duplicate ancestor removes an
earlier match. It validates the entire first JSON value, then preserves Jackson's handling
of trailing content. Escaped strings retain UTF-16 surrogate semantics; numbers preserve
integer precision and BigDecimal's scale, zero and exponent formatting. Jackson switches
from the JDK BigDecimal constructor to FastDoubleParser at 500 characters. JDK 17 bounds
the exponent itself to 32 bits; JDK 21/24/25 and FastDoubleParser accept a wider exponent
when the resulting BigDecimal scale fits. The same runtime profile selects that rule.

Root true/false/null token termination calls `Character.isJavaIdentifierPart(char)` in
Jackson. The planner passes the runtime's Unicode version as a scalar literal (JDK 17:
13.0, JDK 21: 15.0, JDK 24/25: 16.0). Native character categories are intersected with that
Unicode age, and supplementary code points are tested as UTF-16 surrogate units, as in
Jackson. This avoids treating a newly assigned character as a token suffix on an older JDK.
Unmapped JDK versions are declined rather than silently using the build machine's rules.

## Resource-limit boundary

Jackson's published limits are 1000 number digits, 1000 nesting levels, 20 million UTF-16
string units and 50,000 member-name units. Those limits are checked even outside the selected
subtree. There is a host implementation quirk beyond the documented number limit:
`ReaderBasedJsonParser._parseNumber2` represents an absent fraction/exponent length as -1,
while its fast path uses 0. This can admit a 1001-digit floating-point token only on the
slow path. Leading zeroes and end-of-input select that path predictably; buffer boundaries
also depend on Jackson's thread-local recycled buffer and prior unrelated parser calls.

Native validation models a fresh 4000-character reader buffer for documents longer than
32768 UTF-16 units. A differential probe found two differences in 132,632 cases, both involving
1001-digit tokens in a 37,502-character document whose host buffer had grown through prior
parses. A 67,096-case subset on JDK 24 has the same two exceptions. Inputs within the documented
limits agreed, including every BMP token suffix on both JDKs and every escaped UTF-16 unit on
JDK 17. Both functions therefore use the existing per-function `allowIncompatible` opt-in;
default planning keeps Flink. This preserves the repository's strict default while making
native execution available to users who accept this specific resource-limit exception.

SQL harness validation uses JDK 17. Direct `SqlJsonUtils` comparisons also run on JDK 24;
the repository's full SQL harness cannot start there because its current Hadoop dependency
calls the removed `Subject.getSubject` API, before any query is executed.

## Initial admission

Only constant definite member/index paths are admitted. Wildcards, recursion, predicates,
slices and dynamic paths remain on Flink. JSON_VALUE initially returns VARCHAR; the boolean,
integer and double RETURNING conversions in Flink are Java object casts outside ON ERROR,
so reusing SQL CAST would be incorrect. Non-null character literal defaults are admitted;
null defaults participate in Flink's generated whole-call null guard and are declined.
JSON_EXISTS supports all four ON ERROR behaviors. Both functions are ordinary scalar
expressions; no operator, converter or JVM callback is added.
