# Flink collection subscripts in columnar expressions

Arroyo keeps expression evaluation in DataFusion over Arrow batches, with scalar UDFs for custom
semantics (`arroyo-planner/src/functions.rs` and `physical.rs`). Collection access uses that same
execution boundary here. No rowwise operator or extra transpose is introduced.

DataFusion's array element kernel counts negative indexes from the end. Flink returns NULL for
runtime indexes below one and rejects such literal indexes during planning. A small Arrow index
kernel enforces Flink's bounds and gathers values with `take`; it retains sliced offsets, parent
validity, nested schemas, decimal values and both timestamp components.

MAP lookup scans each row's offset range with a typed Arrow comparator and gathers the first
matching value. The comparator is built once per batch, not once per entry. Non-NULL literal keys
retain the existing vector equality path. Dynamic floating and collection keys are not admitted
until their equality behavior has independent Flink parity coverage.
Runtime MAP search types must match the declared key type (character widths may differ), because
coercing a wider integer or higher-scale decimal to the stored type can turn a miss into a match.

The released Flink BinaryMap implementation reads primitive stored NULL slots without a null-bit
check when the search key is non-NULL. String/binary slots read empty, integer/decimal/date/time
slots read zero, boolean reads false, and compact timestamps read as epoch. Matching these values,
in entry order, is required by differential tests even
though treating every stored NULL key as unmatchable would be more conventional. NULL search keys
still return NULL. The string case is tracked by
[#125](https://github.com/datafusion-contrib/StreamFusion/issues/125).

Timestamp precision must therefore cross the expression encoding even though Arrow uses the same
component struct for every precision. Nullable timestamp keys above precision 3 and DECIMAL keys
above precision 18 remain on Flink: their variable-width NULL slots have separate read/error
behavior. Maps with those key types declared NOT NULL may use the native comparator.

Flink can also fold a constant NULL subscript into a typed NULL projection. Preserving the type of
that literal is independent of runtime collection access and remains tracked in
[#126](https://github.com/datafusion-contrib/StreamFusion/issues/126).
