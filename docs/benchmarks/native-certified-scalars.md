# Certified scalar functions (2026-10-26)

This cohort measures exact native subsets of regular expressions, LIKE ESCAPE, SIMILAR TO,
PARSE_URL, PRINTF and temporal extraction/arithmetic. It does not establish that every SQL
signature is native or faster. See [Calc coverage](../operators/calc-filter.md) and
[temporal coverage](../operators/temporal-functions.md) for the retained Flink paths.

## Environment and revisions

Apple M4 Pro, macOS arm64, Oracle JDK 17.0.12, released Flink 2.2.1, parallelism one,
Java heap at most 3 GiB, release Rust with mimalloc. The candidate is the implementation
based on canonical main `15e7031dbf087d392bcca881d5ae17a09eab330a` in this change.
Previous Java production classes are from that main revision. The reused previous core
native library was built at `2ebba9b3`; its engine and bridge sources are unchanged at
`15e7031d`. The intervening Parquet-file fix is outside these scalar jobs.

Measured core native SHA-256 values:

- Candidate: `a40912af75baec05c783b6a13e78f19dcd3d4e0431bbae9701682aefab62e816`.
- Previous: `9a06340bb48001f5351019639fa3b2bda5230f6138b931a56f61cf246c916c9c`.

## Prepared-input functionality gate

Each engine processes 131,072 logical rows from prepared, 1,024-row batches. Java starts
on BinaryRowData; Rust starts on Arrow arrays. Setup, initial JNI/import, final export and
perimeter transposes are excluded. The Rust timer is inside Rust and includes the production
batch certificate, scalar adaptation, allocation and result consumption. Java uses released
Flink helpers or the same Pattern/Formatter operations generated stock Calc invokes.
REGEXP_COUNT uses a precompiled Pattern, a conservative control without the released
helper's pattern-cache lookup. LIKE/SIMILAR patterns are compiled before timing, as for
literal generated patterns. URL parsing retains Flink's URL cache. All output checksums agree.
The Rust driver fails if a certificate would need Java; temporal cases invoke production
UDFs directly. This prepared Arrow fixture has no original-Java-string sidecars.

ASCII text is `" abC def_09 "` repeated to 64 or 264 bytes; there are no NULLs in this
performance cohort. Expressions are `REGEXP_COUNT(s,'a')`, `s LIKE '%!_%' ESCAPE '!'`,
`s SIMILAR TO '.*'`, `PARSE_URL(u,'PATH')`, and `PRINTF('n=%020d',n)`. The URL is
`http://example.org/` followed by the text with spaces replaced by `x`; the 1,024-cardinality
control appends the row index modulo 1,024. Integers vary from -512 through 511.
DATEs vary across 1,024 days from 2000-01-01; TIMESTAMP(9) adds 12:34:56.123456789.
Temporal expressions are YEAR(date), FLOOR(timestamp TO MONTH), timestamp + one month,
and GREATEST(timestamp,timestamp). Three warmups precede five measured runs per case;
engines run sequentially, Java then Rust, without concurrent builds or tests.

| Function | Text bytes | URL cardinality | Flink ms, median [min, max] | Rust ms, median [min, max] | Ratio |
| --- | ---: | ---: | ---: | ---: | ---: |
| REGEXP_COUNT | 64 | 1 | 26.160 [17.068, 30.218] | 14.278 [13.751, 15.264] | 1.83x |
| LIKE_ESCAPE | 64 | 1 | 14.491 [13.967, 15.332] | 6.856 [6.853, 6.946] | 2.11x |
| SIMILAR | 64 | 1 | 11.889 [11.673, 13.204] | 10.072 [9.644, 10.233] | 1.18x |
| PARSE_URL | 64 | 1 | 18.223 [17.876, 18.968] | 6.887 [6.592, 6.975] | 2.65x |
| REGEXP_COUNT | 264 | 1 | 90.759 [89.648, 93.148] | 45.165 [44.946, 45.962] | 2.01x |
| LIKE_ESCAPE | 264 | 1 | 17.869 [16.012, 18.840] | 7.705 [7.501, 7.761] | 2.32x |
| SIMILAR | 264 | 1 | 31.747 [31.361, 33.782] | 17.652 [17.469, 17.852] | 1.80x |
| PARSE_URL | 264 | 1 | 63.556 [60.958, 64.637] | 23.289 [22.922, 23.633] | 2.73x |
| PRINTF | 264 | 1 | 31.766 [30.861, 33.476] | 8.039 [7.976, 8.167] | 3.95x |
| YEAR_DATE | 264 | 1 | 3.012 [2.770, 3.050] | 0.552 [0.529, 0.569] | 5.46x |
| FLOOR_MONTH | 264 | 1 | 5.634 [4.801, 5.886] | 2.366 [2.312, 2.383] | 2.38x |
| ADD_MONTH | 264 | 1 | 8.895 [8.300, 9.454] | 3.595 [3.381, 3.666] | 2.47x |
| GREATEST_TIMESTAMP | 264 | 1 | 2.704 [2.671, 2.728] | 1.746 [1.707, 1.754] | 1.55x |
| PARSE_URL | 64 | 1024 | 31.098 [30.423, 35.463] | 20.083 [19.347, 20.613] | 1.55x |
| PARSE_URL | 264 | 1024 | 78.244 [77.384, 79.177] | 48.495 [46.711, 50.323] | 1.61x |

Ratio is stock median / candidate median. Millisecond-scale observations have visible noise;
these figures apply to the stated inputs, not all workloads. Full trials, including warmups
and checksums: [prepared.csv](certified-scalars-2026-10-26/prepared.csv).

Reproduce both drivers separately on the same idle host:

```sh
cargo build --manifest-path native/Cargo.toml -p streamfusion --release \
  --no-default-features --features mimalloc --example certified_scalars
native/target/release/examples/certified_scalars 131072 3 5 264 1
SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench \
  -Dtest=CertifiedScalarBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dcertified.rows=131072 -Dcertified.warmups=3 -Dcertified.runs=5 \
  -Dcertified.bytes=264 -Dcertified.urlCardinality=1
```

Repeat with 64 text bytes and URL cardinality 1,024. The complete raw cohort retains
the repeated temporal/integer controls from all four configurations; their values do not
change with text width or URL cardinality.

## Complete-job diagnostic and previous implementation

Unchanged ScalarFunctionBenchmark queries use 500,000 rows, 264-byte text, row sources,
blackhole sinks, both row/Arrow transposes, two warmups and five measurements.
Stock and candidate alternate execution order; the previous native implementation runs
in a separate process afterwards. The direct launcher retains MiniCluster startup for each
job, so 0.5-0.9 second totals are startup dominated. Do not interpret their ratios as sustained
connector throughput. Different baseline controls and unpaired previous runs expose that noise.
Native queries require Calc admission. Previous calls use generated Flink evaluators; candidate
queries exercise the certified direct-argument path. PARSE_URL's existing whole-job fixture has
composed arguments and is excluded from this direct-kernel comparison.

| Query | Stock seconds, median [min, max] | Previous seconds, median [min, max] | Candidate seconds, median [min, max] | Stock/candidate | Previous/candidate |
| --- | ---: | ---: | ---: | ---: | ---: |
| BASELINE_text | 0.707708 [0.671110, 0.728996] | 0.633683 [0.591771, 0.737010] | 0.609507 [0.593684, 0.652588] | 1.16x | 1.04x |
| BASELINE_bigint | 0.570134 [0.557500, 0.824805] | 0.558285 [0.531343, 0.573337] | 0.560756 [0.518742, 0.574522] | 1.02x | 1.00x |
| LIKE_ESCAPE | 0.715890 [0.673030, 0.797034] | 0.629001 [0.624963, 0.640462] | 0.590582 [0.572885, 0.648843] | 1.21x | 1.07x |
| REGEXP_COUNT | 0.869100 [0.858250, 0.899667] | 0.845544 [0.801341, 0.905506] | 0.736471 [0.717949, 0.888776] | 1.18x | 1.15x |
| PRINTF_BIGINT | 0.783671 [0.726685, 0.810342] | 0.739471 [0.713115, 0.820411] | 0.590059 [0.552479, 0.822823] | 1.33x | 1.25x |

Full observations: [jobs.csv](certified-scalars-2026-10-26/jobs.csv). Reproduce each revision with:

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime -am test -Pbench \
  -Dtest=ScalarFunctionBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dscalar.functions=LIKE_ESCAPE,REGEXP_COUNT,PRINTF_BIGINT \
  -Dscalar.rows=500000 -Dscalar.bytes=264 -Dscalar.warmup=2 -Dscalar.runs=5
```

## Correctness and execution evidence

- 790 Rust core tests pass without default features. Compiled certificates run with no JVM
  captured, including source-cache validation; an unexpected callback fails those tests.
- Another 33 planner, operator-description and UDF-bridge checks pass on both lines.
- Focused SQL suites: 46 pass on Flink 2.2.1/JDK 17; 32 pass and 14 expected capability skips
  on Flink 1.18.1/JDK 11. Runtime sources prevent constant folding.
- Unchanged released upstream SQL invocations: 225 pass on 2.2.1, 140 pass on 1.18.1.
  Positive consumed Calc work appears in 127 and 131 respectively. Some fixtures contain
  an upstream identity UDF or deliberately exercise fallback, so these counts prove Calc work,
  not that every tested function avoided Java. Focused tests inspect complete assignments for
  direct calendar extraction; Rust kernel tests supply separate no-JVM witnesses.
- NULLs, invalid patterns/formats, Java literal replacements and empty regex matches,
  malformed UTF-16, raw URL components/keys, decimal Formatter behavior, negative epoch,
  leap days, historical calendar rules, finite JDK zone history and nanoseconds are covered.

Remaining Java-only pattern syntax, unverified protocols/Formatter conversions, recurring-DST
LTZ extraction, composed interval rewrites and other remaining temporal boundaries stay tracked
by issues #495, #496, #497 and #504. These measurements do not close those broad items.
