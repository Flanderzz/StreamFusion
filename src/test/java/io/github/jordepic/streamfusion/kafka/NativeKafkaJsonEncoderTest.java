package io.github.jordepic.streamfusion.kafka;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.github.jordepic.streamfusion.format.EncodeFormat;
import io.github.jordepic.streamfusion.operator.RowDataArrowConverter;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatOptions;
import org.apache.flink.formats.json.JsonRowDataSerializationSchema;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("streamfusion-kafka")
class NativeKafkaJsonEncoderTest {

  private static final RowType ROW_TYPE =
      RowType.of(
          new LogicalType[] {
            new IntType(),
            new VarCharType(VarCharType.MAX_LENGTH),
            new BooleanType(),
            new DoubleType()
          },
          new String[] {"id", "name", "enabled", "score"});

  @Test
  void matchesFlinkForWholeBatchesWithAndWithoutNullFields() throws Exception {
    GenericRowData first = GenericRowData.of(1, StringData.fromString("quote: \" and 雪"), true, 2.5);
    GenericRowData nulls = GenericRowData.of(2, null, false, null);
    List<RowData> rows = List.of(first, nulls);

    assertMatchesFlink(rows, false);
    assertMatchesFlink(rows, true);
  }

  @Test
  void matchesFlinkTimestampFormatting() throws Exception {
    RowType timestamps =
        RowType.of(
            new LogicalType[] {new TimestampType(3), new TimestampType(9)},
            new String[] {"millis", "nanos"});
    List<RowData> rows =
        List.of(
            GenericRowData.of(
                TimestampData.fromEpochMillis(1_577_934_245_678L),
                TimestampData.fromEpochMillis(1_577_934_245_678L, 123_456)));

    assertMatchesFlink(rows, timestamps, TimestampFormat.SQL, false);
    assertMatchesFlink(rows, timestamps, TimestampFormat.ISO_8601, false);
  }

  /**
   * Flink's timestamp formatters trim the fraction to its shortest spelling (and drop it entirely
   * at .000) for plain TIMESTAMP and TIMESTAMP_LTZ alike, regardless of the declared precision.
   */
  @Test
  void matchesFlinkTimestampFractionTrimming() throws Exception {
    RowType timestamps =
        RowType.of(
            new LogicalType[] {
              new TimestampType(3),
              new TimestampType(9),
              new LocalZonedTimestampType(3),
              new LocalZonedTimestampType(9)
            },
            new String[] {"ts3", "ts9", "ltz3", "ltz9"});
    long base = 1_577_934_245_000L;
    List<RowData> rows =
        List.of(
            timestampRow(TimestampData.fromEpochMillis(base + 500)), // .5, not .500
            timestampRow(TimestampData.fromEpochMillis(base)), // no fraction at all
            timestampRow(TimestampData.fromEpochMillis(base + 123, 456_789)), // .123456789
            timestampRow(TimestampData.fromEpochMillis(base + 120)), // .12
            timestampRow(TimestampData.fromEpochMillis(base + 100, 230_000))); // .10023

    assertMatchesFlink(rows, timestamps, TimestampFormat.SQL, false);
    assertMatchesFlink(rows, timestamps, TimestampFormat.ISO_8601, false);
  }

  private static RowData timestampRow(TimestampData value) {
    return GenericRowData.of(value, value, value, value);
  }

  /**
   * Both of Flink's decimal spellings: the default is {@code stripTrailingZeros().toString()},
   * which turns {@code 100.00} into {@code 1E+2}, while {@code encode.decimal-as-plain-number}
   * keeps the column scale intact ({@code 100.00}). The helper referees each row against both
   * serializer configurations.
   */
  @Test
  void matchesFlinkDecimalSpellingsInBothModes() throws Exception {
    RowType decimals =
        RowType.of(
            new LogicalType[] {new DecimalType(10, 2), new DecimalType(12, 3), new DecimalType(38, 10)},
            new String[] {"low", "mid", "huge"});
    List<RowData> rows =
        List.of(
            row(decimals, "100.00", "123.450", "12345678901234567890123456.7890123456"),
            row(decimals, "1.00", "0.000", "0.0000000010"),
            row(decimals, "0.00", "-0.010", "-9999999999999999999999999999.9999999999"),
            row(decimals, "-0.01", "1000000.000", "0.0000001000"),
            row(decimals, "12345678.90", "-120.000", "1.0000000000"));

    assertMatchesFlink(rows, decimals, TimestampFormat.SQL, false);
  }

  private static RowData row(RowType decimals, String... values) {
    Object[] fields = new Object[values.length];
    for (int i = 0; i < values.length; i++) {
      DecimalType type = (DecimalType) decimals.getTypeAt(i);
      fields[i] =
          DecimalData.fromBigDecimal(
              new BigDecimal(values[i]), type.getPrecision(), type.getScale());
    }
    return GenericRowData.of(fields);
  }

  @Test
  void matchesFlinkForRemainingScalarTypes() throws Exception {
    RowType scalars =
        RowType.of(
            new LogicalType[] {
              new DecimalType(10, 2),
              new VarBinaryType(VarBinaryType.MAX_LENGTH),
              new DateType(),
              new TimeType(3),
              new LocalZonedTimestampType(3)
            },
            new String[] {"amount", "payload", "day", "time", "instant"});
    List<RowData> rows =
        List.of(
            GenericRowData.of(
                DecimalData.fromBigDecimal(new BigDecimal("12345678.90"), 10, 2),
                new byte[] {0, 1, 2, -1},
                (int) LocalDate.of(2020, 2, 29).toEpochDay(),
                45_296_789,
                TimestampData.fromEpochMillis(1_577_934_245_678L)),
            GenericRowData.of(
                DecimalData.fromBigDecimal(new BigDecimal("1.00"), 10, 2),
                new byte[0],
                (int) LocalDate.of(1970, 1, 1).toEpochDay(),
                0,
                TimestampData.fromEpochMillis(0)),
            GenericRowData.of(
                DecimalData.fromBigDecimal(new BigDecimal("-0.01"), 10, 2),
                new byte[] {-128, 127},
                (int) LocalDate.of(1969, 12, 31).toEpochDay(),
                86_399_999,
                TimestampData.fromEpochMillis(-1)));

    assertMatchesFlink(rows, scalars, TimestampFormat.SQL, false);
    assertMatchesFlink(rows, scalars, TimestampFormat.ISO_8601, false);
  }

  /**
   * The upsert key format is its own format instance in Flink, configured solely from {@code
   * key.json.*} (or that format's defaults) — never from the value's settings. The referee builds
   * Flink's key and value serializers the way the upsert-kafka factory would, with deliberately
   * conflicting options, and diffs both byte streams.
   */
  @Test
  void honorsKeyFormatOptionsIndependentlyOfValueOptions() throws Exception {
    RowType rowType =
        RowType.of(
            new LogicalType[] {
              new DecimalType(10, 2), new TimestampType(3), new VarCharType(VarCharType.MAX_LENGTH)
            },
            new String[] {"amount", "ts", "name"});
    RowType keyType =
        RowType.of(
            new LogicalType[] {new DecimalType(10, 2), new TimestampType(3)},
            new String[] {"amount", "ts"});
    DecimalData amount = DecimalData.fromBigDecimal(new BigDecimal("100.00"), 10, 2);
    TimestampData ts = TimestampData.fromEpochMillis(1_577_934_245_500L);
    GenericRowData insert = GenericRowData.of(amount, ts, StringData.fromString("one"));
    GenericRowData delete = GenericRowData.of(amount, ts, StringData.fromString("one"));
    delete.setRowKind(RowKind.DELETE);
    List<RowData> rows = List.of(insert, delete);

    JsonRowDataSerializationSchema flinkKey =
        new JsonRowDataSerializationSchema(
            keyType,
            TimestampFormat.ISO_8601,
            JsonFormatOptions.MapNullKeyMode.LITERAL,
            "null",
            true,
            false);
    flinkKey.open(initializationContext());
    JsonRowDataSerializationSchema flinkValue =
        new JsonRowDataSerializationSchema(
            rowType,
            TimestampFormat.SQL,
            JsonFormatOptions.MapNullKeyMode.LITERAL,
            "null",
            false,
            false);
    flinkValue.open(initializationContext());

    try (BufferAllocator allocator = new RootAllocator();
        CDataDictionaryProvider dictionaries = new CDataDictionaryProvider();
        VectorSchemaRoot root = RowDataArrowConverter.write(rows, rowType, allocator, true);
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(allocator, root, dictionaries, array, schema);
      EncodeFormat valueFormat = EncodeFormat.json(Map.of());
      EncodeFormat keyFormat =
          EncodeFormat.json(
              Map.of(
                  "timestamp-format.standard", "ISO-8601",
                  "encode.decimal-as-plain-number", "true"));
      byte[][][] records =
          NativeKafka.encodeKafkaRecords(
              array.memoryAddress(),
              schema.memoryAddress(),
              valueFormat.format,
              valueFormat.options,
              keyFormat.format,
              keyFormat.options,
              rowType.getChildren().stream().map(Object::toString).toArray(String[]::new),
              rowType.getFieldNames().toArray(String[]::new),
              new int[] {0, 1},
              new int[] {0, 1, 2},
              true);

      byte[] expectedKey = flinkKey.serialize(GenericRowData.of(amount, ts));
      assertArrayEquals(expectedKey, records[0][0]);
      assertArrayEquals(expectedKey, records[0][1]);
      assertArrayEquals(flinkValue.serialize(insert), records[1][0]);
      assertNull(records[1][1]);
    }
  }

  private static void assertMatchesFlink(List<RowData> rows, boolean ignoreNullFields)
      throws Exception {
    assertMatchesFlink(rows, ROW_TYPE, TimestampFormat.SQL, ignoreNullFields, false);
  }

  private static void assertMatchesFlink(
      List<RowData> rows,
      RowType rowType,
      TimestampFormat timestampFormat,
      boolean ignoreNullFields)
      throws Exception {
    assertMatchesFlink(rows, rowType, timestampFormat, ignoreNullFields, false);
    assertMatchesFlink(rows, rowType, timestampFormat, ignoreNullFields, true);
  }

  private static void assertMatchesFlink(
      List<RowData> rows,
      RowType rowType,
      TimestampFormat timestampFormat,
      boolean ignoreNullFields,
      boolean decimalAsPlainNumber)
      throws Exception {
    JsonRowDataSerializationSchema flink =
        new JsonRowDataSerializationSchema(
            rowType,
            timestampFormat,
            JsonFormatOptions.MapNullKeyMode.LITERAL,
            "null",
            decimalAsPlainNumber,
            ignoreNullFields);
    flink.open(initializationContext());

    try (BufferAllocator allocator = new RootAllocator();
        CDataDictionaryProvider dictionaries = new CDataDictionaryProvider();
        VectorSchemaRoot root = RowDataArrowConverter.write(rows, rowType, allocator);
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(allocator, root, dictionaries, array, schema);
      EncodeFormat format =
          EncodeFormat.json(
              Map.of(
                  "timestamp-format.standard",
                  timestampFormat == TimestampFormat.SQL ? "SQL" : "ISO-8601",
                  "encode.ignore-null-fields",
                  String.valueOf(ignoreNullFields),
                  "encode.decimal-as-plain-number",
                  String.valueOf(decimalAsPlainNumber)));
      byte[][] actual =
          NativeKafka.encodeKafkaBatch(
              array.memoryAddress(),
              schema.memoryAddress(),
              format.format,
              format.options,
              rowType.getChildren().stream().map(Object::toString).toArray(String[]::new),
              rowType.getFieldNames().toArray(String[]::new));

      assertEquals(rows.size(), actual.length);
      for (int i = 0; i < rows.size(); i++) {
        byte[] expected = flink.serialize(rows.get(i));
        assertArrayEquals(
            expected,
            actual[i],
            "row "
                + i
                + ": expected "
                + new String(expected, StandardCharsets.UTF_8)
                + ", actual "
                + new String(actual[i], StandardCharsets.UTF_8));
      }
    }
  }

  private static SerializationSchema.InitializationContext initializationContext() {
    return new SerializationSchema.InitializationContext() {
      @Override
      public MetricGroup getMetricGroup() {
        return new UnregisteredMetricsGroup();
      }

      @Override
      public UserCodeClassLoader getUserCodeClassLoader() {
        return SimpleUserCodeClassLoader.create(NativeKafkaJsonEncoderTest.class.getClassLoader());
      }
    };
  }
}
