package io.github.jordepic.streamfusion.format.avro;

import org.apache.flink.formats.avro.AvroToRowDataConverters;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

/**
 * Plan-time admission shared by the native Avro decode providers. Flink's own avro factories build
 * their schema and converter at job submission and throw there for the row types the Avro mapping
 * cannot carry (RAW, TIMESTAMP_LTZ under the legacy mapping, precision beyond the mapping's limit,
 * non-string map keys, and — under the corrected mapping — a nested row holding TIMESTAMP_LTZ,
 * which the converter factory rejects). Running the same two derivations here and declining on
 * failure keeps those tables on Flink, which then fails submission exactly the way vanilla Flink
 * does, instead of the native planner aborting with its own error.
 */
public final class AvroDecodeGate {

  private AvroDecodeGate() {}

  /**
   * Whether the native decode can carry this row type: Flink's own schema/converter derivations
   * accept it, and every column is a type whose arrow-avro decode the native layer reconciles with
   * the Arrow boundary schema. A null type (an options-only probe) passes — the planner gates the
   * concrete scan type before substituting.
   */
  public static boolean supports(RowType rowType, boolean legacyTimestampMapping) {
    if (rowType == null) {
      return true;
    }
    try {
      AvroSchemaConverter.convertToSchema(rowType.copy(false), legacyTimestampMapping);
      AvroToRowDataConverters.createRowConverter(rowType, legacyTimestampMapping);
    } catch (RuntimeException e) {
      return false;
    }
    return rowType.getChildren().stream().allMatch(AvroDecodeGate::decodableColumn);
  }

  private static boolean decodableColumn(LogicalType type) {
    switch (type.getTypeRoot()) {
      case BOOLEAN:
      case INTEGER:
      case BIGINT:
      case FLOAT:
      case DOUBLE:
      case CHAR:
      case VARCHAR:
        return true;
      case ROW:
      case ARRAY:
      case MAP:
        return type.getChildren().stream().allMatch(AvroDecodeGate::decodableColumn);
      default:
        // arrow-avro's Arrow mapping for the remaining types (temporal, decimal, binary,
        // multiset) is not yet reconciled with the boundary schema the operators expect.
        return false;
    }
  }
}
