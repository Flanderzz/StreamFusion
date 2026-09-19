package tech.streamfusion.compat;

/** Preserves the host's generated SINGLE_VALUE failure type across Flink lines. */
public final class TableErrors {
  private TableErrors() {}

  public static Class<? extends RuntimeException> exceptionType() {
    return org.apache.flink.table.api.TableRuntimeException.class;
  }

  public static void fail(String message) {
    throw new org.apache.flink.table.api.TableRuntimeException(message);
  }
}
