package tech.streamfusion.orc;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Whitelist of verified Java ORC writer options shared by Flink and Paimon. */
public record OrcWriterSettings(Map<String, String> config, String fallbackReason) {
  public static String timestampFallback(org.apache.flink.table.types.logical.LogicalType type) {
    if (java.util.TimeZone.getDefault().hasSameRules(java.util.TimeZone.getTimeZone("UTC")))
      return null;
    if (type instanceof org.apache.flink.table.types.logical.TimestampType
        || type instanceof org.apache.flink.table.types.logical.LocalZonedTimestampType)
      return "native ORC timestamps require a UTC JVM timezone";
    for (var child : type.getChildren()) {
      String reason = timestampFallback(child);
      if (reason != null) return reason;
    }
    return null;
  }

  public String[] keys() {
    return config.keySet().toArray(new String[0]);
  }

  public String[] values() {
    return config.values().toArray(new String[0]);
  }

  private static final Map<String, String> TRANSLATED =
      Map.ofEntries(
          Map.entry("orc.compress", "compression"),
          Map.entry("orc.stripe.size", "stripe.size"),
          Map.entry("orc.compress.size", "compress.size"),
          Map.entry("orc.row.index.stride", "row.index.stride"),
          Map.entry("orc.dictionary.key.threshold", "dictionary.key.threshold"),
          Map.entry("orc.compression.strategy", "compression.strategy"),
          Map.entry("orc.write.format", "write.format"),
          Map.entry("orc.bloom.filter.fpp", "bloom.filter.fpp"));

  public static OrcWriterSettings translate(
      Map<String, String> effective,
      Map<String, String> requested,
      Map<String, String> defaults,
      String bloomColumns) {
    for (var entry : requested.entrySet()) {
      String key = entry.getKey();
      if (TRANSLATED.containsKey(key)
          || key.equals("orc.bloom.filter.columns")
          || key.equals("orc.timestamp-ltz.legacy.type")
          || key.equals("orc.create.index")
          || key.equals("orc.compression.zstd.level")) continue;
      // Unmapped settings are admitted only at the host default, never silently discarded.
      if (!entry.getValue().equalsIgnoreCase(defaults.getOrDefault(key, "\u0000")))
        return fallback("ORC writer option " + key + " requires the stock writer");
    }
    Map<String, String> config = new LinkedHashMap<>();
    for (var entry : TRANSLATED.entrySet()) {
      String value = effective.getOrDefault(entry.getKey(), defaults.get(entry.getKey()));
      if (value != null) config.put(entry.getValue(), value);
    }
    try {
      String compression = config.get("compression").toUpperCase(Locale.ROOT);
      if (!Set.of("NONE", "ZLIB", "SNAPPY", "LZ4", "ZSTD").contains(compression))
        return fallback(
            "ORC compression " + compression + " has not been verified with the Arrow writer");
      config.put("compression", compression);
      config.put("timezone", java.util.TimeZone.getDefault().getID());
      String legacy = effective.getOrDefault("orc.timestamp-ltz.legacy.type", "false");
      if (!Set.of("true", "false").contains(legacy.toLowerCase(Locale.ROOT)))
        return fallback("invalid ORC legacy timestamp setting");
      config.put("legacy.timestamp-ltz", legacy.toLowerCase(Locale.ROOT));
      String strategy = config.get("compression.strategy").toUpperCase(Locale.ROOT);
      if (!Set.of("SPEED", "COMPRESSION").contains(strategy))
        return fallback("invalid ORC compression strategy");
      if (compression.equals("ZSTD") && effective.containsKey("orc.compression.zstd.level")) {
        int level = Integer.parseInt(effective.get("orc.compression.zstd.level"));
        if (level < -131072 || level > 22) return fallback("invalid ORC ZSTD level");
        config.put("compression.zstd.level", Integer.toString(level));
      }
      config.put("compression.strategy", strategy);
      long stripe = Long.parseLong(config.get("stripe.size"));
      long buffer = Long.parseLong(config.get("compress.size"));
      long stride = Long.parseLong(config.get("row.index.stride"));
      if (stripe <= 0
          || buffer <= 0
          || buffer >= (1 << 23)
          || stride < 0
          || stride > Integer.MAX_VALUE
          || (stride > 0 && stride < 1000))
        return fallback("invalid ORC stripe, buffer, or row-index size");
      String indexes = effective.getOrDefault("orc.create.index", "true");
      if (!Set.of("true", "false").contains(indexes.toLowerCase(Locale.ROOT)))
        return fallback("invalid ORC index setting");
      // Both released hosts ignore orc.create.index: their writers use row.index.stride alone.
      double dictionary = Double.parseDouble(config.get("dictionary.key.threshold"));
      double fpp = Double.parseDouble(config.get("bloom.filter.fpp"));
      if (!Double.isFinite(dictionary)
          || dictionary < 0
          || dictionary > 1
          || !Double.isFinite(fpp)
          || fpp <= 0
          || fpp >= 1) return fallback("invalid ORC dictionary threshold or bloom probability");
      if (!Set.of("0.11", "0.12").contains(config.get("write.format")))
        return fallback("unsupported ORC file version");
      config.put("bloom.filter.columns", bloomColumns);
      return new OrcWriterSettings(Map.copyOf(config), null);
    } catch (RuntimeException invalid) {
      return fallback("invalid ORC writer setting: " + invalid.getMessage());
    }
  }

  public static OrcWriterSettings fallback(String reason) {
    return new OrcWriterSettings(Map.of(), reason);
  }
}
