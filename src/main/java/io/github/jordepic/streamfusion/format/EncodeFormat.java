package io.github.jordepic.streamfusion.format;

import java.io.Serializable;
import java.util.Map;

/**
 * One native sink format instance: the wire-format code and its encode-affecting options rendered
 * as the {@code key=value} lines the native encoder parses — the encode-side counterpart of the
 * decode path's {@link NativeFormatOptions} carrier. The planner resolves a format instance once
 * per sink (value, and upsert key) and every layer below carries this pair instead of
 * format-specific parameters, so additional sink formats plug in at {@link #of} without touching
 * the exec node, operators, or the JNI surface.
 */
public final class EncodeFormat implements Serializable {

  private static final long serialVersionUID = 1L;

  public final int format;
  public final String options;

  private EncodeFormat(int format, String options) {
    this.format = format;
    this.options = options;
  }

  /**
   * Resolves one format instance from its identifier and prefix-stripped table options, or null
   * when the format (or one of its option values) is not natively encoded — the planner's fallback
   * gate. An out-of-range option value also returns null so the query stays on Flink, whose own
   * format factory raises its ValidationException; the native path never runs a validation Flink
   * would have failed.
   */
  public static EncodeFormat of(String identifier, Map<String, String> options) {
    if (FormatCodes.forIdentifier(identifier) != FormatCodes.JSON) {
      return null;
    }
    return json(options);
  }

  /** JSON encode options resolved with Flink's json format factory defaults. */
  public static EncodeFormat json(Map<String, String> options) {
    StringBuilder encoded = new StringBuilder();
    String timestampFormat = options.getOrDefault("timestamp-format.standard", "SQL");
    if ("ISO-8601".equals(timestampFormat)) {
      encoded.append("timestamp-format=ISO-8601\n");
    } else if (!"SQL".equals(timestampFormat)) {
      return null;
    }
    if (!appendBoolean(encoded, "encode.ignore-null-fields", options)
        || !appendBoolean(encoded, "encode.decimal-as-plain-number", options)) {
      return null;
    }
    return new EncodeFormat(FormatCodes.JSON, encoded.toString());
  }

  /**
   * A boolean option the way Flink's configuration reads it: absent or exactly true/false
   * (case-insensitive). Anything else is not appended and fails the resolution.
   */
  private static boolean appendBoolean(
      StringBuilder encoded, String key, Map<String, String> options) {
    String value = options.get(key);
    if (value == null) {
      return true;
    }
    if (!"true".equalsIgnoreCase(value) && !"false".equalsIgnoreCase(value)) {
      return false;
    }
    if (Boolean.parseBoolean(value)) {
      encoded.append(key).append("=true\n");
    }
    return true;
  }
}
