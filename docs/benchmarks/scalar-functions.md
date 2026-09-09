# Scalar function benchmarks

Measured on 2026-09-09 against the production implementation in `69122d2b`, using Apple M4 Pro,
JDK 17, UTC, Flink 2.2.1 and DataFusion 54.0.0. The native library uses the standard Maven
`bench` profile: release mode, mimalloc, and the development connector features.

Every query evaluates one function over 2,000,000 rows at parallelism 1. Both engines run
serially in the same JVM for each scenario, alternating which engine runs first on each trial.
Each engine gets two warmups and five measured trials per case; the tables show median elapsed
seconds. Each scenario uses a fresh JVM. No other benchmark or test suite runs concurrently.

Times include SQL planning/execution, a rowwise DataStream source, and a rowwise blackhole
sink. The harness checks that every native function and identity-control plan includes
`NativeCalc`, `RowDataToArrow`, and `ArrowToRowData`. Ratios are Flink time divided by native
time: above 1 means native is faster for that workload; below 1 means it is slower. Small
differences can be run-to-run noise, and these measurements do not isolate kernel cost.

The tables below contain the independently measured cases for the functions admitted by
this revision. Additional cases are introduced together with their function commits.
See [Calc / filter](../operators/calc-filter.md) for the current argument gates.

## Inputs

- Search strings add `row:`/`other:` and `:match`/`:miss` around the padding: total lengths are
  274/275 bytes for the 264-byte case and 18/19 bytes for the 8-byte case. Column needles and
  LOCATE start positions vary independently of the source text.
- String fixtures use a 264-byte payload budget. URL_DECODE and JSON_UNQUOTE repeat complete
  escape groups within that budget; JSON_UNQUOTE adds enclosing quotes. DECODE uses valid
  UTF-8 bytes; malformed input belongs to the semantic tests.
- The Unicode scenario also makes every eighth source value NULL, so Unicode and nullability
  effects are not isolated. Integer, date and timestamp values stay the same across the two
  scenarios; only nullability changes. UNHEX keeps ASCII hex input in both scenarios.
- TO_DATE reads ten-byte date strings. Calendar functions use TIMESTAMP(9) values spanning
  pre-epoch fractions, a leap day and a year boundary. LTRIM/RTRIM use a literal trim set.
- GREATEST/LEAST measure BIGINT inputs. ENCODE/DECODE measure UTF-8. These cases do not claim
  performance for other admitted types or charsets. Source fixtures are in
  `ScalarFunctionBenchmark` and `TextTimeBenchmarkInputs`.

## Reproduce

Use JDK 17 and run one timing process at a time:

```sh
TZ=UTC SF_BENCHMARK=true mvn -pl :streamfusion-runtime test -Pbench \
  '-Dtest=ScalarFunctionBenchmark#individualFunctions' \
  -Dscalar.engine=both -Dscalar.functions=ALL \
  -Dscalar.rows=2000000 -Dscalar.warmup=2 -Dscalar.runs=5 \
  -Dscalar.bytes=264 -Dscalar.unicode=false -Dscalar.nullEvery=0 \
  -Dscalar.output=target/scalar-ascii264.csv
```

Repeat in a fresh JVM with `scalar.unicode=true` and `scalar.nullEvery=8`, using a different
output file. Then run `scalar.functions=SEARCH`, `scalar.bytes=8`, `scalar.unicode=false` and
`scalar.nullEvery=0` in another fresh JVM. A function name such as `STARTSWITH_LITERAL` can
select a single case, but a different selection has a different JIT history from the full run.

## Raw data

The current trials, source-matched identity controls, result medians, workload catalog and
checksums are attached to [PR #44](https://github.com/datafusion-contrib/StreamFusion/pull/44)
as `pr44-flink-native-benchmarks.zip`. The repository keeps benchmark code and final result
tables; generated CSVs are not versioned. Controls are retained for checking the run, and are
not subtracted from function times because their result types and lengths can differ.

## STARTSWITH

`STARTSWITH_LITERAL`: `STARTSWITH(s, 'row:')`; `STARTSWITH_COLUMN`: `STARTSWITH(s, needle)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `STARTSWITH_LITERAL` | ASCII, 264-byte budget | 0.447 | 0.766 | 0.58x |
| `STARTSWITH_COLUMN` | ASCII, 264-byte budget | 0.554 | 0.938 | 0.59x |
| `STARTSWITH_LITERAL` | Unicode, 264-byte budget, NULL/8 | 0.599 | 0.796 | 0.75x |
| `STARTSWITH_COLUMN` | Unicode, 264-byte budget, NULL/8 | 0.700 | 0.981 | 0.71x |
| `STARTSWITH_LITERAL` | ASCII, 8-byte padding | 0.375 | 0.556 | 0.67x |
| `STARTSWITH_COLUMN` | ASCII, 8-byte padding | 0.504 | 0.773 | 0.65x |

## ENDSWITH

`ENDSWITH_LITERAL`: `ENDSWITH(s, ':match')`; `ENDSWITH_COLUMN`: `ENDSWITH(s, needle)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `ENDSWITH_LITERAL` | ASCII, 264-byte budget | 0.445 | 0.759 | 0.59x |
| `ENDSWITH_COLUMN` | ASCII, 264-byte budget | 0.567 | 0.942 | 0.60x |
| `ENDSWITH_LITERAL` | Unicode, 264-byte budget, NULL/8 | 0.579 | 0.832 | 0.70x |
| `ENDSWITH_COLUMN` | Unicode, 264-byte budget, NULL/8 | 0.700 | 1.003 | 0.70x |
| `ENDSWITH_LITERAL` | ASCII, 8-byte padding | 0.380 | 0.552 | 0.69x |
| `ENDSWITH_COLUMN` | ASCII, 8-byte padding | 0.497 | 0.766 | 0.65x |

## INSTR

`INSTR_LITERAL`: `INSTR(s, ':match')`; `INSTR_COLUMN`: `INSTR(s, needle)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `INSTR_LITERAL` | ASCII, 264-byte budget | 1.461 | 0.768 | 1.90x |
| `INSTR_COLUMN` | ASCII, 264-byte budget | 1.622 | 0.945 | 1.72x |
| `INSTR_LITERAL` | Unicode, 264-byte budget, NULL/8 | 0.935 | 0.817 | 1.14x |
| `INSTR_COLUMN` | Unicode, 264-byte budget, NULL/8 | 1.107 | 1.012 | 1.09x |
| `INSTR_LITERAL` | ASCII, 8-byte padding | 0.425 | 0.579 | 0.73x |
| `INSTR_COLUMN` | ASCII, 8-byte padding | 0.556 | 0.793 | 0.70x |

## LOCATE

`LOCATE2_LITERAL`: `LOCATE(':match', s)`; `LOCATE2_COLUMN`: `LOCATE(needle, s)`; `LOCATE3_LITERAL`: `LOCATE(':match', s, start_pos)`; `LOCATE3_COLUMN`: `LOCATE(needle, s, start_pos)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `LOCATE2_LITERAL` | ASCII, 264-byte budget | 1.427 | 0.737 | 1.94x |
| `LOCATE2_COLUMN` | ASCII, 264-byte budget | 1.520 | 0.935 | 1.63x |
| `LOCATE3_LITERAL` | ASCII, 264-byte budget | 1.437 | 0.854 | 1.68x |
| `LOCATE3_COLUMN` | ASCII, 264-byte budget | 1.586 | 1.048 | 1.51x |
| `LOCATE2_LITERAL` | Unicode, 264-byte budget, NULL/8 | 0.963 | 0.839 | 1.15x |
| `LOCATE2_COLUMN` | Unicode, 264-byte budget, NULL/8 | 1.093 | 0.990 | 1.10x |
| `LOCATE3_LITERAL` | Unicode, 264-byte budget, NULL/8 | 1.023 | 1.015 | 1.01x |
| `LOCATE3_COLUMN` | Unicode, 264-byte budget, NULL/8 | 1.151 | 1.222 | 0.94x |
| `LOCATE2_LITERAL` | ASCII, 8-byte padding | 0.440 | 0.607 | 0.72x |
| `LOCATE2_COLUMN` | ASCII, 8-byte padding | 0.544 | 0.790 | 0.69x |
| `LOCATE3_LITERAL` | ASCII, 8-byte padding | 0.453 | 0.653 | 0.69x |
| `LOCATE3_COLUMN` | ASCII, 8-byte padding | 0.586 | 0.860 | 0.68x |

## BIN

`BIN_TINYINT`: `BIN(n)`; `BIN_SMALLINT`: `BIN(n)`; `BIN_INTEGER`: `BIN(n)`; `BIN_BIGINT`: `BIN(n)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `BIN_TINYINT` | non-null | 0.335 | 0.472 | 0.71x |
| `BIN_SMALLINT` | non-null | 0.336 | 0.468 | 0.72x |
| `BIN_INTEGER` | non-null | 0.341 | 0.469 | 0.73x |
| `BIN_BIGINT` | non-null | 0.331 | 0.476 | 0.70x |
| `BIN_TINYINT` | NULL/8 | 0.321 | 0.478 | 0.67x |
| `BIN_SMALLINT` | NULL/8 | 0.324 | 0.468 | 0.69x |
| `BIN_INTEGER` | NULL/8 | 0.332 | 0.464 | 0.71x |
| `BIN_BIGINT` | NULL/8 | 0.325 | 0.468 | 0.69x |

## HEX

`HEX_STRING`: `HEX(s)`; `HEX_TINYINT`: `HEX(n)`; `HEX_SMALLINT`: `HEX(n)`; `HEX_INTEGER`: `HEX(n)`; `HEX_BIGINT`: `HEX(n)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `HEX_TINYINT` | non-null | 0.335 | 0.436 | 0.77x |
| `HEX_SMALLINT` | non-null | 0.338 | 0.432 | 0.78x |
| `HEX_INTEGER` | non-null | 0.344 | 0.430 | 0.80x |
| `HEX_BIGINT` | non-null | 0.332 | 0.440 | 0.75x |
| `HEX_STRING` | ASCII, 264-byte budget | 1.348 | 1.057 | 1.27x |
| `HEX_TINYINT` | NULL/8 | 0.321 | 0.438 | 0.73x |
| `HEX_SMALLINT` | NULL/8 | 0.325 | 0.434 | 0.75x |
| `HEX_INTEGER` | NULL/8 | 0.328 | 0.434 | 0.76x |
| `HEX_BIGINT` | NULL/8 | 0.314 | 0.428 | 0.73x |
| `HEX_STRING` | Unicode, 264-byte budget, NULL/8 | 1.857 | 1.103 | 1.68x |

## TO_BASE64

`TO_BASE64`: `TO_BASE64(s)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `TO_BASE64` | ASCII, 264-byte budget | 0.657 | 0.963 | 0.68x |
| `TO_BASE64` | Unicode, 264-byte budget, NULL/8 | 0.840 | 1.037 | 0.81x |

## UNHEX

`UNHEX`: `UNHEX(s)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `UNHEX` | 528 hex bytes, non-null | 1.203 | 1.295 | 0.93x |
| `UNHEX` | 528 hex bytes, NULL/8 | 1.130 | 1.063 | 1.06x |

## GREATEST

`GREATEST`: `GREATEST(n, m, 17)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `GREATEST` | non-null | 0.308 | 0.471 | 0.65x |
| `GREATEST` | NULL/8 | 0.287 | 0.453 | 0.63x |

## LEAST

`LEAST`: `LEAST(n, m, 17)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `LEAST` | non-null | 0.294 | 0.449 | 0.65x |
| `LEAST` | NULL/8 | 0.276 | 0.453 | 0.61x |

## INITCAP

`INITCAP`: `INITCAP(s)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `INITCAP` | ASCII, 264-byte budget | 1.347 | 1.266 | 1.06x |
| `INITCAP` | Unicode, 264-byte budget, NULL/8 | 1.812 | 1.463 | 1.24x |

## TRANSLATE

`TRANSLATE`: `TRANSLATE(s, 'abcdef', 'ABCDEF')`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `TRANSLATE` | ASCII, 264-byte budget | 2.377 | 1.350 | 1.76x |
| `TRANSLATE` | Unicode, 264-byte budget, NULL/8 | 3.138 | 1.240 | 2.53x |

## BTRIM

`BTRIM`: `BTRIM(s)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `BTRIM` | ASCII, 264-byte budget | 0.566 | 0.868 | 0.65x |
| `BTRIM` | Unicode, 264-byte budget, NULL/8 | 0.728 | 0.937 | 0.78x |

## ELT

`ELT`: `ELT(i, s, t)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `ELT` | ASCII, 264-byte budget | 0.753 | 1.342 | 0.56x |
| `ELT` | Unicode, 264-byte budget, NULL/8 | 1.112 | 1.575 | 0.71x |

## URL_ENCODE

`URL_ENCODE`: `URL_ENCODE(s)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `URL_ENCODE` | ASCII, 264-byte budget | 1.450 | 1.285 | 1.13x |
| `URL_ENCODE` | Unicode, 264-byte budget, NULL/8 | 4.400 | 1.448 | 3.04x |

## OVERLAY

`OVERLAY`: `OVERLAY(s PLACING t FROM i FOR n)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `OVERLAY` | ASCII, 264-byte budget | 0.834 | 1.197 | 0.70x |
| `OVERLAY` | Unicode, 264-byte budget, NULL/8 | 1.201 | 1.415 | 0.85x |

## URL_DECODE

`URL_DECODE`: `URL_DECODE(s)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `URL_DECODE` | ASCII, 264-byte budget | 1.713 | 1.507 | 1.14x |
| `URL_DECODE` | Unicode, 264-byte budget, NULL/8 | 1.521 | 1.257 | 1.21x |

## ENCODE

`ENCODE_UTF8`: `ENCODE(s, 'UTF-8')`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `ENCODE_UTF8` | ASCII, 264-byte budget | 0.454 | 0.755 | 0.60x |
| `ENCODE_UTF8` | Unicode, 264-byte budget, NULL/8 | 0.900 | 0.821 | 1.10x |

## DECODE

`DECODE_UTF8`: `DECODE(b, 'UTF-8')`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `DECODE_UTF8` | ASCII, 264-byte budget | 0.442 | 0.657 | 0.67x |
| `DECODE_UTF8` | Unicode, 264-byte budget, NULL/8 | 0.846 | 0.735 | 1.15x |

## JSON_QUOTE

`JSON_QUOTE`: `JSON_QUOTE(s)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `JSON_QUOTE` | ASCII, 264-byte budget | 1.550 | 1.570 | 0.99x |
| `JSON_QUOTE` | Unicode, 264-byte budget, NULL/8 | 26.048 | 2.972 | 8.77x |

## JSON_UNQUOTE

`JSON_UNQUOTE`: `JSON_UNQUOTE(s)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `JSON_UNQUOTE` | ASCII, 264-byte budget | 1.615 | 1.317 | 1.23x |
| `JSON_UNQUOTE` | Unicode, 264-byte budget, NULL/8 | 1.719 | 1.312 | 1.31x |

## SPLIT

`SPLIT`: `SPLIT(s, '|')`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `SPLIT` | ASCII, 264-byte budget | 3.241 | 6.581 | 0.49x |
| `SPLIT` | Unicode, 264-byte budget, NULL/8 | 2.535 | 4.821 | 0.53x |

## SUBSTRING

`SUBSTRING_DYNAMIC`: `SUBSTRING(s, n, len)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `SUBSTRING_DYNAMIC` | ASCII, 264-byte budget | 0.760 | 0.929 | 0.82x |
| `SUBSTRING_DYNAMIC` | Unicode, 264-byte budget, NULL/8 | 0.937 | 1.005 | 0.93x |

## LEFT

`LEFT_DYNAMIC`: `LEFT(s, n)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `LEFT_DYNAMIC` | ASCII, 264-byte budget | 0.615 | 0.880 | 0.70x |
| `LEFT_DYNAMIC` | Unicode, 264-byte budget, NULL/8 | 0.800 | 0.967 | 0.83x |

## RIGHT

`RIGHT_DYNAMIC`: `RIGHT(s, n)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `RIGHT_DYNAMIC` | ASCII, 264-byte budget | 0.822 | 0.864 | 0.95x |
| `RIGHT_DYNAMIC` | Unicode, 264-byte budget, NULL/8 | 0.983 | 0.912 | 1.08x |

## LPAD

`LPAD_DYNAMIC`: `LPAD(s, n, p)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `LPAD_DYNAMIC` | ASCII, 264-byte budget | 0.878 | 1.123 | 0.78x |
| `LPAD_DYNAMIC` | Unicode, 264-byte budget, NULL/8 | 1.199 | 1.400 | 0.86x |

## RPAD

`RPAD_DYNAMIC`: `RPAD(s, n, p)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `RPAD_DYNAMIC` | ASCII, 264-byte budget | 0.815 | 1.173 | 0.70x |
| `RPAD_DYNAMIC` | Unicode, 264-byte budget, NULL/8 | 1.189 | 1.410 | 0.84x |

## SPLIT_INDEX

`SPLIT_INDEX_DYNAMIC`: `SPLIT_INDEX(s, p, n)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `SPLIT_INDEX_DYNAMIC` | ASCII, 264-byte budget | 2.099 | 1.065 | 1.97x |
| `SPLIT_INDEX_DYNAMIC` | Unicode, 264-byte budget, NULL/8 | 2.029 | 1.121 | 1.81x |

## TO_DATE

`TO_DATE`: `TO_DATE(s)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `TO_DATE` | date strings, non-null | 0.427 | 0.529 | 0.81x |
| `TO_DATE` | date strings, NULL/8 | 0.435 | 0.544 | 0.80x |

## QUARTER

`QUARTER`: `QUARTER(ts)`

| Case | Input | Flink (s) | Native (s) | Flink / native |
|---|---|---:|---:|---:|
| `QUARTER` | TIMESTAMP(9), non-null | 0.368 | 0.434 | 0.85x |
| `QUARTER` | TIMESTAMP(9), NULL/8 | 0.378 | 0.460 | 0.82x |

