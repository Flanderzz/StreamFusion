package io.github.jordepic.streamfusion.format.raw;

import io.github.jordepic.streamfusion.format.NativeFormatContext;
import io.github.jordepic.streamfusion.format.NativeFormatOptions;
import io.github.jordepic.streamfusion.format.NativeFormatProvider;
import io.github.jordepic.streamfusion.format.NativeMessageDecoderFactory;
import io.github.jordepic.streamfusion.format.NativeSchemaMessageDecoder;
import org.apache.flink.table.types.logical.RowType;

/**
 * Native provider for Flink's raw value format: the whole message is the single physical column's
 * value. Admitted natively: CHAR/VARCHAR (UTF-8 charset only), VARBINARY, BOOLEAN, and the
 * fixed-width numerics with either {@code raw.endianness}. Staying on Flink: multi-column schemas
 * and invalid option values (Flink raises its own ValidationException); {@code RAW<T>} columns
 * (their bytes belong to a Java TypeSerializer); fixed-length BINARY (Flink passes any message
 * length through where Arrow's fixed-size binary enforces the declared one); a non-UTF-8
 * {@code raw.charset} (the native decode has no charset machinery); and {@code ignore-parse-errors}
 * (an option Flink's raw factory doesn't define).
 */
public final class RawFormatProvider implements NativeFormatProvider {

  @Override
  public String formatIdentifier() {
    return "raw";
  }

  /** The single column's admitted roots — kept in a method body (like the sibling providers') so
   * the class links under a Flink-less loader: the extension-JAR probe instantiates providers over
   * the platform classloader, where a static {@code EnumSet<LogicalTypeRoot>} fails resolution. */
  private static boolean supportedType(RowType schema) {
    switch (schema.getTypeAt(0).getTypeRoot()) {
      case CHAR:
      case VARCHAR:
      case VARBINARY:
      case BOOLEAN:
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
      case FLOAT:
      case DOUBLE:
        return true;
      default:
        return false;
    }
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
    RowType schema = context.writerType();
    return !context.ignoreParseErrors()
        && schema.getFieldCount() == 1
        && supportedType(schema)
        && NativeFormatOptions.encode(context.options()) != null;
  }

  @Override
  public NativeMessageDecoderFactory createDecoder(NativeFormatContext context) {
    String formatOptions = NativeFormatOptions.encode(context.options());
    return () -> new Decoder(formatOptions);
  }

  private static final class Decoder extends NativeSchemaMessageDecoder {
    private final String formatOptions;

    private Decoder(String formatOptions) {
      this.formatOptions = formatOptions;
    }

    @Override
    protected long createHandle(long schemaArrayAddress, long schemaAddress) {
      return NativeRawFormat.createDecoder(schemaArrayAddress, schemaAddress, formatOptions);
    }

    @Override
    public void decodeInto(long inArray, long inSchema, long outArray, long outSchema) {
      NativeRawFormat.decodeInto(handle, inArray, inSchema, outArray, outSchema);
    }

    @Override
    public long driverInitAddress() {
      return NativeRawFormat.driverInitAddress();
    }

    @Override
    public void close() {
      if (handle != 0) {
        NativeRawFormat.closeDecoder(handle);
        handle = 0;
      }
    }
  }
}
