# Typed NULL literals keep their Arrow schema

Arroyo retains DataFusion's typed `ScalarValue` in its logical expressions. Comet's native
expression planner also constructs typed NULL scalars from the literal's declared Arrow
type, including nested List/Map/Struct types. StreamFusion follows that model: a NULL
literal must have its type before expression coercion, particularly at the top level where
there is no surrounding expression to infer it.

StreamFusion's compact expression protocol previously emitted an untyped NULL for integer,
decimal and nested NULL literals. The native output-type guard correctly rejected those
top-level projections. Disabling that guard or coercing the whole Calc output would mask
unrelated type mismatches, so the encoder now carries the literal's actual type.

The typed-NULL node stores a Base64-encoded, one-field Arrow IPC schema in the existing
string pool. Java derives it with the same Arrow conversion as the row/column boundary;
Rust decodes the standard schema and constructs `ScalarValue::try_from(&DataType)` once
when compiling the expression. This differs from Comet's protobuf datatype encoding
because StreamFusion has no protobuf expression protocol. Reusing Arrow IPC avoids another
recursive type codec and preserves field names, child nullability, decimal parameters,
MAP layout and two-component timestamp fields without duplication. Existing released
Arrow and Base64 dependencies supply the codec; no dependency or JNI signature changes.

The payload contains owned bytes represented as a string, with no borrowed native pointer
or new allocator lifetime. Runtime evaluation uses DataFusion's scalar expansion to Arrow
batches. Existing typed string, binary and temporal literal nodes remain compatible, and
an explicitly untyped SQL NULL still uses the original node. Output-type preflight remains
mandatory. JobManager and TaskManagers must use matching StreamFusion Java/native builds
that understand the new expression node.

SQL tests cover scalar widths and nested types, folded MAP lookups alongside runtime
collection expressions, CASE/arithmetic/boolean compositions, and 8,201 rows across full
and partial batches. A JNI inference test compares the complete Arrow schema, including
quoted/Unicode names and non-null children, against the Java boundary schema.
