# SQL/JSON parsing

JSON_VALUE and JSON_EXISTS use a shared native reader with two parsing paths. The streaming
path borrows selected tokens and validates the first JSON document with Flink/Jackson rules.
The SIMD path uses the existing `simd-json` dependency for documents containing many short
members, where repeatedly scanning individual keys and values costs more than building a tape.

The reader reuses the mutable input buffer, structural scratch and tape across rows in a
batch. Selected decoded strings are copied directly into the Arrow builder. Object lookup
visits all members and retains the last match before descending, including duplicate ancestors.
The entire document is validated even when the requested member appears near the beginning.

A SIMD attempt requires eight colons within the first 256 bytes and fewer than 50,000 total
input bytes. A successful tape must contain at most 1000 nodes, with no floating-point values;
Unicode escapes and selected numbers use the streaming path. Those bounds preserve Jackson's
resource limits, BigDecimal spelling and UTF-16 escape behavior. Invalid SIMD input also goes
through the streaming parser, retaining Flink's first-document and trailing-content behavior.
These are internal parsing choices; the SQL admission and compatibility opt-ins are unchanged.
A document rejected after tape construction is parsed again by the streaming path. The
multi-member measurements use string members; they do not establish an improvement for
workloads dominated by floating-point members or numeric selections.

Long padding strings and documents with many fields are separate benchmark workloads. SIMD
is useful for the latter; a large byte count alone does not predict a benefit. The benchmark
parameter `scalar.json.fields` adds that many short string members without changing the selected
path. Each JSON function is measured independently against Flink, including both transposes.
Final measurements and reproduction commands are on the
[scalar benchmark page](../benchmarks/scalar-functions.md#sqljson-measurements).

Comet's streaming `get_json_object` and `datafusion-functions-json`'s jiter lookup informed
the comparison. Their path selection and validation contracts differ from SQL/JSON in Flink;
see the [semantic note](https://github.com/datafusion-contrib/StreamFusion/blob/main/divergences/32-sql-json-definite-paths.md).
