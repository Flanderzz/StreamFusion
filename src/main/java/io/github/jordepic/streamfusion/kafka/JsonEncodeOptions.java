package io.github.jordepic.streamfusion.kafka;

import java.io.Serializable;
import java.util.Map;

/**
 * One JSON format instance's encode-affecting options, resolved with the same defaults as Flink's
 * json format factory. Flink configures a Jackson mapper and converter family per format instance,
 * so the native encoder carries the same trio wherever a format instance would exist.
 */
public final class JsonEncodeOptions implements Serializable {

  private static final long serialVersionUID = 1L;

  public final String timestampFormat;
  public final boolean ignoreNullFields;
  public final boolean decimalAsPlainNumber;

  public JsonEncodeOptions(
      String timestampFormat, boolean ignoreNullFields, boolean decimalAsPlainNumber) {
    this.timestampFormat = timestampFormat;
    this.ignoreNullFields = ignoreNullFields;
    this.decimalAsPlainNumber = decimalAsPlainNumber;
  }

  /** Resolves one format instance's options (already stripped of their format prefix). */
  public static JsonEncodeOptions fromFormatOptions(Map<String, String> options) {
    return new JsonEncodeOptions(
        options.getOrDefault("timestamp-format.standard", "SQL"),
        Boolean.parseBoolean(options.getOrDefault("encode.ignore-null-fields", "false")),
        Boolean.parseBoolean(options.getOrDefault("encode.decimal-as-plain-number", "false")));
  }
}
