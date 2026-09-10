# Scalar charset and JSON serialization

## UTF-16 encoding

Reference: Flink 2.2.1's ENCODE calls the JDK charset encoder. DataFusion's Spark
`string/encode.rs` provides the same UTF-16 code-unit structure, but its UTF-16
branch initializes every result with a BOM, including empty strings. The JDK writes
the BOM only when input is non-empty. StreamFusion follows the JDK and recognizes
charset aliases once in the planner.

The native encoder writes UTF-16 code units directly into an Arrow binary builder.
It avoids DataFusion Spark's per-row temporary byte vectors while retaining the
existing scalar/array invocation convention and NULL propagation. Only verified
literal charsets are admitted; no incompatible-behavior switch is needed.

## UTF-16 decoding

DECODE follows JDK `sun.nio.cs.UnicodeDecoder`, used by Flink 2.2.1. UTF-16 consumes
an initial BOM and otherwise defaults to big-endian. Explicit BE/LE codecs preserve
the BOM as a character. A high surrogate plus a non-low code unit consumes four bytes
as one malformed sequence; a terminal high surrogate with one trailing byte consumes
all three bytes. Rust's ordinary UTF-16 replacement iterator groups these differently,
so the decoder keeps the JDK byte-consumption rules explicitly.

SQL parity covers all 65,536 single code units, boundary pairs, random byte strings,
NULLs, empty input, and compositions. Encoding/decoding are independent native kernels.

## JSON scalar serialization

Flink 2.2.1's JsonStringCallGen converts scalars to Jackson nodes and serializes them
through SqlJsonUtils.serializeJson. StreamFusion's character, boolean and signed-integer
forms follow the same bytes while writing directly to the Arrow output builder. The shared
string writer also serves the existing native JSON decoder: it copies unescaped spans in
bulk, following Comet's to_json.rs structure, and handles every ASCII control as Jackson
does (including uppercase hex escapes). Flink's separate JSON_QUOTE is not this serializer.

The core writer has no connector-feature dependency. Unlike Comet's general Spark cast
path, unverified floating-point, decimal, binary, temporal and container types remain on
Flink. Direct JSON constructors are excluded from scalar inputs because Flink's code
generator inserts those as raw JSON instead of quoting their character result.

## JSON object construction

Flink's JsonObjectCallGen inserts fields into a Jackson ObjectNode, keeping the last
insertion for each key and skipping NULLs under ABSENT ON NULL. serializeJson converts
that tree to a map and sorts keys using Java's UTF-16 String.compareTo. Sorting UTF-8
bytes would disagree for supplementary characters versus high BMP code points.

The native object UDF groups duplicate literal keys in stable UTF-16 order and escapes
each unique key once per batch. It keeps scalar values as one-row arrays and downcasts
column values once, then selects each key's last applicable value while writing the
final Arrow string buffer. No per-row JSON tree, map, or temporary result string is
constructed. The initial admission is deliberately scalar: direct nested constructors
and other Flink JsonNode conversions remain on Flink until independently verified.

Coverage is listed in [Calc/filter](../docs/operators/calc-filter.md); independent
Flink/native measurements are in [scalar benchmarks](../docs/benchmarks/scalar-functions.md).
