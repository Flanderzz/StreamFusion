# Flink SQL/JSON definite paths

Comet's `native/spark-expr/src/string_funcs/get_json_object.rs` separates scalar paths from
column inputs and streams through each document instead of building a DOM on its common
path. StreamFusion follows that structure: the path and policies remain scalar, a definite
path is parsed once per batch, and unescaped selected strings borrow their input span until
appended to the Arrow result. Every field is validated, including fields outside the selected
path. JSON_VALUE, JSON_EXISTS and IS JSON share this parser under the scalar-function registry.

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

The reader acquires the task thread's actual token buffer from the released shaded Jackson
`JsonFactory`/`BufferRecycler` API before evaluating a batch. This is the same default
thread-local pool used by Flink's `SqlJsonUtils`. Inputs of at most 32768 UTF-16 units grow
the capacity before parsing, including invalid input and rows handled by SIMD; larger inputs
use the existing capacity as `StringReader` does. The native number scanner uses that actual
capacity for boundary checks. At batch completion, including an EMPTY/ERROR policy failure,
the buffer is returned with the capacity reached by the reader. No JSON document or selected
value crosses JNI, and neither the operator nor the row/Arrow converters change.

Comet's `native/spark-expr/src/jvm_udf/mod.rs` and `native/jni-bridge/src/comet_udf_bridge.rs`
provide the reference for calling back on the driving task thread. This use is narrower than
an expression callback: only parser state is exchanged, and a JNI local frame bounds the
Java object's lifetime. The streaming and SIMD kernels continue to execute in Rust.

Both functions are enabled by default. The regression tests compare native JNI evaluation
with `SqlJsonUtils` under initial capacities of 4000, 8000, 16000 and 32768 characters, and
exercise growth within and across batches, Unicode length, malformed input and SIMD input.
They assert the fixture really produces different Flink results under different buffer
histories; matching a fresh-buffer run alone would miss the original bug.

IS JSON uses the same validated first-document result before applying Jayway's path policy.
This is essential for root JSON null: Jackson accepts it as a scalar, while Jayway refuses
to construct a path context from Java null. Its VALUE/OBJECT/ARRAY/SCALAR predicates and
their negations are enabled by default. SQL NULL and invalid documents produce FALSE,
with no SQL NULL result. Neither DataFusion's general JSON readers nor Comet's Spark JSON
extraction implements this SQL predicate contract, so the wrapper reuses our verified parser.

SQL harness validation uses JDK 17. The ten JNI buffer-history tests also pass on JDK 24
with `-Dtest=NativeJsonBufferHistoryTest -Djunit.jupiter.extensions.autodetection.enabled=false`.
That disables the automatic MiniCluster extension, which these direct comparisons do not
need. The full SQL harness cannot start on JDK 24 because the current Hadoop dependency
calls the removed `Subject.getSubject` API before any query is executed.

## Initial admission

Only constant definite member/index paths are admitted. Wildcards, recursion, predicates,
slices and dynamic paths remain on Flink. JSON_VALUE initially returns VARCHAR; the boolean,
integer and double RETURNING conversions in Flink are Java object casts outside ON ERROR,
so reusing SQL CAST would be incorrect. Non-null character literal defaults are admitted;
null defaults participate in Flink's generated whole-call null guard and are declined.
JSON_EXISTS supports all four ON ERROR behaviors. Both functions are ordinary scalar
expressions; no operator or converter changes are required.
