package io.github.jordepic.streamfusion.format.avroconfluent;

import io.github.jordepic.streamfusion.format.NativeFormatContext;
import io.github.jordepic.streamfusion.format.NativeFormatProvider;
import io.github.jordepic.streamfusion.format.NativeMessageDecoderFactory;
import io.github.jordepic.streamfusion.format.avro.AvroDecodeGate;
import io.github.jordepic.streamfusion.kafka.ConfluentSchemaRegistry;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;

/**
 * Native provider for Flink's {@code debezium-avro-confluent} format: the Debezium changelog
 * envelope with Confluent-framed Avro bodies. The reader schema is derived from the envelope row
 * type {@code ROW<before <physical>.nullable(), after <physical>.nullable(), op STRING>} — the
 * exact derivation Flink's deserializer performs — so the plan-time gate and the runtime registry
 * lookup both operate on the envelope, and the native decode fans the envelope out to changelog
 * rows. Like {@code avro-confluent}, the mapping is hard-wired legacy and the registry options must
 * be a plain URL; an explicit {@code schema} option (Flink validates it against the envelope) also
 * falls back. The format defines no {@code ignore-parse-errors}, so a corrupt message fails the job
 * on both engines.
 */
public final class DebeziumAvroConfluentFormatProvider implements NativeFormatProvider {

  @Override
  public String formatIdentifier() {
    return "debezium-avro-confluent";
  }

  @Override
  public boolean honorsProjection() {
    return false;
  }

  @Override
  public boolean supportsIgnoreParseErrors() {
    return false;
  }

  @Override
  public boolean supports(NativeFormatContext context) {
    return !context.ignoreParseErrors()
        && ConfluentSchemaRegistry.fromOptions(context.options()) != null
        && AvroDecodeGate.supports(envelopeType(context.outputType()), true);
  }

  @Override
  public NativeMessageDecoderFactory createDecoder(NativeFormatContext context) {
    ConfluentSchemaRegistry registry = ConfluentSchemaRegistry.fromOptions(context.options());
    String readerSchema =
        AvroSchemaConverter.convertToSchema(envelopeType(context.outputType()).copy(false))
            .toString();
    return () -> new RegistryAvroDecoder(registry, readerSchema, true);
  }

  /** Flink's Debezium envelope row type over the table's physical row (its own derivation calls). */
  static RowType envelopeType(RowType physical) {
    if (physical == null) {
      return null;
    }
    DataType image = TypeConversions.fromLogicalToDataType(physical).nullable();
    return (RowType)
        DataTypes.ROW(
                DataTypes.FIELD("before", image),
                DataTypes.FIELD("after", image),
                DataTypes.FIELD("op", DataTypes.STRING()))
            .getLogicalType();
  }
}
