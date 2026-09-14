package tech.streamfusion.planner;

import java.util.List;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;

/** Parquet sink rewrite contributed only by streamfusion-parquet. */
public final class ParquetPlannerExtension implements NativePlannerExtension {
  private static final FileSinkMatcher.Format FORMAT =
      new FileSinkMatcher.Format() {
        public boolean changelog(java.util.Map<String, String> options) {
          return "changelog-parquet".equals(options.get("connector"));
        }

        public boolean matches(java.util.Map<String, String> options) {
          return FileSinkMatcher.Format.super.matches(options) || changelog(options);
        }

        public java.util.Map<String, String> writerOptions(java.util.Map<String, String> options) {
          if (!changelog(options)) return options;
          var result = new java.util.LinkedHashMap<>(options);
          result.put("connector", "filesystem");
          result.put("format", "parquet");
          result.putIfAbsent("parquet.write.int64.timestamp", "true");
          result.putIfAbsent("parquet.utc-timezone", "true");
          return result;
        }

        public String name() {
          return "parquet";
        }

        public FileSinkMatcher.Settings settings(
            java.util.Map<String, String> options,
            org.apache.flink.table.types.logical.RowType type,
            List<String> partitions) {
          ParquetSinkTranslator.Result result =
              ParquetSinkTranslator.translate(options, type, partitions);
          return result.fallbackReason == null
              ? new FileSinkMatcher.Settings(result.encoderKeys(), result.encoderValues(), null)
              : new FileSinkMatcher.Settings(null, null, result.fallbackReason);
        }

        public tech.streamfusion.format.ColumnarFileCodec codec(
            org.apache.flink.table.types.logical.RowType type, List<String> partitions) {
          return new tech.streamfusion.parquet.ParquetCodec();
        }
      };

  @Override
  public void addSubstitutions(List<Substitution<?>> entries) {
    entries.add(
        Substitution.of(
                StreamPhysicalSink.class,
                (sink, ctx) -> FileSinkMatcher.substitute(sink, ctx, FORMAT))
            .matching(sink -> FileSinkMatcher.appliesTo(sink, FORMAT))
            .changelogSafe());
  }
}
