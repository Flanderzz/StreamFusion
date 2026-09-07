package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.paimon.options.Options;
import org.junit.jupiter.api.Test;

class PaimonParquetSettingsTest {

  private static Map<String, String> translated(Options options, String compression) {
    PaimonParquetSettings settings = PaimonParquetSettings.translate(options, compression);
    assertNull(settings.fallbackReason());
    Map<String, String> map = new LinkedHashMap<>();
    for (int i = 0; i < settings.keys().length; i++) {
      map.put(settings.keys()[i], settings.values()[i]);
    }
    return map;
  }

  private static String fallback(Options options, String compression) {
    String reason = PaimonParquetSettings.translate(options, compression).fallbackReason();
    assertTrue(reason != null, "expected a fallback");
    return reason;
  }

  @Test
  void translatesPaimonsDefaultsToTheEncoder() {
    Options options = new Options();
    options.set("parquet.compression.codec.zstd.level", "1");
    Map<String, String> encoder = translated(options, "zstd");
    assertEquals("paimon", encoder.get("schema.shape"));
    assertEquals("ZSTD", encoder.get("compression"));
    assertEquals("1", encoder.get("compression.zstd.level"));
    assertEquals(Integer.toString(128 * 1024 * 1024), encoder.get("block.size"));
    assertEquals(Integer.toString(1024 * 1024), encoder.get("page.size"));
    assertEquals(Integer.toString(1024 * 1024), encoder.get("dictionary.page.size"));
    assertEquals("true", encoder.get("enable.dictionary"));
    assertEquals("1", encoder.get("writer.version"));
    assertEquals("micros", encoder.get("timestamp.unit"));
  }

  @Test
  void honoursExplicitWriterSettings() {
    Options options = new Options();
    options.set("parquet.compression", "gzip");
    options.set("parquet.block.size", "1048576");
    options.set("parquet.page.size", "65536");
    options.set("parquet.dictionary.page.size", "4096");
    options.set("parquet.enable.dictionary", "false");
    options.set("parquet.writer.version", "v2");
    Map<String, String> encoder = translated(options, "zstd");
    assertEquals("GZIP", encoder.get("compression"));
    assertEquals("6", encoder.get("compression.gzip.level"));
    assertEquals("1048576", encoder.get("block.size"));
    assertEquals("65536", encoder.get("page.size"));
    assertEquals("4096", encoder.get("dictionary.page.size"));
    assertEquals("false", encoder.get("enable.dictionary"));
    assertEquals("2", encoder.get("writer.version"));
    assertEquals("UNCOMPRESSED", translated(new Options(), "none").get("compression"));
    assertEquals("UNCOMPRESSED", translated(new Options(), null).get("compression"));
    assertEquals("SNAPPY", translated(new Options(), "snappy").get("compression"));
  }

  @Test
  void declinesWhatTheEncoderCannotReproduce() {
    assertTrue(fallback(new Options(), "lz4").contains("LZ4"));
    Options workers = new Options();
    workers.set("parquet.compression.codec.zstd.workers", "2");
    assertTrue(fallback(workers, "zstd").contains("multithreaded"));
    Options bloom = new Options();
    bloom.set("parquet.bloom.filter.enabled", "true");
    assertTrue(fallback(bloom, "zstd").contains("bloom"));
    Options pageRows = new Options();
    pageRows.set("parquet.page.row.count.limit", "1000");
    assertTrue(fallback(pageRows, "zstd").contains("parquet.page.row.count.limit"));
    Options perColumn = new Options();
    perColumn.set("parquet.compression#id", "snappy");
    assertTrue(fallback(perColumn, "zstd").contains("unrecognized"));
    Options level = new Options();
    level.set("parquet.compression.codec.zstd.level", "99");
    assertTrue(fallback(level, "zstd").contains("ZSTD compression level"));
    Options validation = new Options();
    validation.set("parquet.validation", "true");
    assertTrue(fallback(validation, "zstd").contains("parquet.validation"));
    Options blockSize = new Options();
    blockSize.set("parquet.block.size", "-1");
    assertTrue(fallback(blockSize, "zstd").contains("parquet.block.size"));
  }
}
