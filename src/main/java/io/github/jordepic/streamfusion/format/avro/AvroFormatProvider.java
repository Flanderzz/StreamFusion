package io.github.jordepic.streamfusion.format.avro;

import io.github.jordepic.streamfusion.format.NativeFormatContext;
import io.github.jordepic.streamfusion.format.NativeFormatOptions;
import io.github.jordepic.streamfusion.format.NativeFormatProvider;
import io.github.jordepic.streamfusion.format.NativeMessageDecoderFactory;
import io.github.jordepic.streamfusion.format.NativeSchemaMessageDecoder;
import java.util.Map;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;

/** Native provider for Flink's schema-embedded {@code avro} format. */
public final class AvroFormatProvider implements NativeFormatProvider {

  @Override
  public String formatIdentifier() {
    return "avro";
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
    if (context.ignoreParseErrors()) {
      return false;
    }
    String encoding = NativeFormatOptions.option(context.options(), "encoding");
    if (encoding != null && !"binary".equalsIgnoreCase(encoding)) {
      // Avro's JSON encoding is a different wire format the native decode doesn't read.
      return false;
    }
    if (!legacyTimestampMapping(context.options())) {
      // The corrected mapping changes the derived schema (local-timestamp / TIMESTAMP_LTZ);
      // the native decode only reproduces the legacy mapping so far.
      return false;
    }
    return AvroDecodeGate.supports(context.writerType(), true);
  }

  /** Flink's {@code avro.timestamp_mapping.legacy}, default true. */
  private static boolean legacyTimestampMapping(Map<String, String> options) {
    return !"false".equalsIgnoreCase(NativeFormatOptions.option(options, "timestamp_mapping.legacy"));
  }

  @Override
  public NativeMessageDecoderFactory createDecoder(NativeFormatContext context) {
    String writerSchema = AvroSchemaConverter.convertToSchema(context.writerType().copy(false)).toString();
    String readerSchema =
        context.writerType().equals(context.outputType())
            ? ""
            : AvroSchemaConverter.convertToSchema(context.outputType().copy(false)).toString();
    return () -> new Decoder(writerSchema, readerSchema);
  }

  private static final class Decoder extends NativeSchemaMessageDecoder {
    private final String writerSchema;
    private final String readerSchema;

    private Decoder(String writerSchema, String readerSchema) {
      this.writerSchema = writerSchema;
      this.readerSchema = readerSchema;
    }

    @Override
    protected long createHandle(long schemaArrayAddress, long schemaAddress) {
      return NativeAvroFormat.createDecoder(
          false, writerSchema, readerSchema, schemaArrayAddress, schemaAddress);
    }

    @Override
    public void decodeInto(long inArray, long inSchema, long outArray, long outSchema) {
      NativeAvroFormat.decodeInto(handle, inArray, inSchema, outArray, outSchema);
    }

    @Override
    public long driverInitAddress() {
      return NativeAvroFormat.driverInitAddress();
    }

    @Override
    public void close() {
      if (handle != 0) {
        NativeAvroFormat.closeDecoder(handle);
        handle = 0;
      }
    }
  }
}
