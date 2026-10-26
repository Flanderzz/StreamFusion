# Exact builtin certificates

Arroyo delegates regular-expression expressions to DataFusion. DataFusion compiles constant
patterns once and caches runtime patterns, but its grammar and matching behavior do not define
released Flink compatibility. Java-only regular expressions and URL/Formatter rules therefore
retain Flink's generated evaluator.

The existing decimal/cast batch-certificate structure is reused: a successful certificate
computes the entire batch in Rust; failure runs the original generated evaluator. No partial
batch escapes and no exception is discarded. Pattern compilation is shared within a batch.
The initial boundary is ASCII, checked against retained original Java UTF-16 caches. Unsupported
forms remain exact without a new switch or an incompatible-regex default.

Uncertified string arguments retain their sidecars across the synchronous Arrow upcall. The
import owns their buffers for the invocation, following Comet's imported-Arrow lifetime pattern;
no new persistent JNI ownership or global kernel lock is introduced. Retained JVM descriptors
describe a possible route and must not be reported as proof that an upcall executed.

Temporal extraction instead ports Flink's integer calendar arithmetic. LTZ extraction reuses the
JDK-provided finite timezone history used for Unix-time formatting. A separate Rust timezone
database cannot establish parity with the running JDK's history or recurring DST rules.
