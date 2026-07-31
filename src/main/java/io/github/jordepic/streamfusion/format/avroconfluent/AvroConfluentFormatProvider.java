package io.github.jordepic.streamfusion.format.avroconfluent;

import io.github.jordepic.streamfusion.format.NativeFormatContext;
import io.github.jordepic.streamfusion.format.NativeFormatProvider;
import io.github.jordepic.streamfusion.format.NativeMessageDecoderFactory;
import io.github.jordepic.streamfusion.format.avro.AvroDecodeGate;
import io.github.jordepic.streamfusion.kafka.ConfluentSchemaRegistry;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;

/** Native provider for Flink's {@code avro-confluent} format. */
public final class AvroConfluentFormatProvider implements NativeFormatProvider {

  @Override
  public String formatIdentifier() {
    return "avro-confluent";
  }

  @Override
  public boolean honorsProjection() {
    return true;
  }

  @Override
  public boolean supportsIgnoreParseErrors() {
    return false;
  }

  @Override
  public boolean supports(NativeFormatContext context) {
    // Flink's avro-confluent factory has no timestamp-mapping option: it is hard-wired to the
    // legacy mapping, so the gate always checks the legacy derivation.
    return !context.ignoreParseErrors()
        && ConfluentSchemaRegistry.fromOptions(context.options()) != null
        && AvroDecodeGate.supports(context.writerType(), true);
  }

  @Override
  public NativeMessageDecoderFactory createDecoder(NativeFormatContext context) {
    ConfluentSchemaRegistry registry = ConfluentSchemaRegistry.fromOptions(context.options());
    String readerSchema = AvroSchemaConverter.convertToSchema(context.outputType().copy(false)).toString();
    return () -> new RegistryAvroDecoder(registry, readerSchema, false);
  }
}
