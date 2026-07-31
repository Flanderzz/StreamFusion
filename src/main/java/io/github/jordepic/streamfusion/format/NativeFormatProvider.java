package io.github.jordepic.streamfusion.format;

import java.util.Map;
import org.apache.flink.table.types.logical.RowType;

/**
 * A native implementation of one Flink value format. Format artifacts register providers with Java's
 * {@link java.util.ServiceLoader}; connectors use this SPI rather than taking a dependency on every
 * format they may carry.
 */
public interface NativeFormatProvider {

  String formatIdentifier();

  boolean honorsProjection();

  boolean supportsIgnoreParseErrors();

  /** Returns whether this artifact supports the table's exact format options. */
  boolean supports(NativeFormatContext context);

  NativeMessageDecoderFactory createDecoder(NativeFormatContext context);

  /**
   * The sink-side encode format for serializing {@code rowType} under this format instance's
   * prefix-stripped options, or null when this artifact does not natively encode that combination —
   * the planner's fallback gate. Formats without a native serializer keep the default.
   */
  default EncodeFormat encodeFormat(RowType rowType, Map<String, String> options) {
    return null;
  }
}
